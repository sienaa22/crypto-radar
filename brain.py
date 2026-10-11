#!/usr/bin/env python3
"""
Signal Hub v1

Reads the shared signal ledger that every radar service writes to and:

  1. Prices each signal: the coin price (and BTC) when it fired, 1 day later and
     7 days later. A weekly scorecard shows, per kind of signal, how often the
     coin moved the way the signal said, against BTC. Kinds that do not beat BTC
     are noise; kinds that reliably go the other way are worth fading.
  2. Learns a weight per kind from those results, and sends a CONVERGENCE alert
     when independent signals from different services line up on one coin
     within 48 hours. Kinds that proved useful count more, noise counts less.

Signals without a token (a new project, an audit contest) are counted but
cannot be priced.

Standard library only (plus the optional AI summary, see ai.py). Python 3.8+.

Usage:
  python3 brain.py --env /opt/radar/radar.env              # run forever
  python3 brain.py --env /opt/radar/radar.env --once
  python3 brain.py --env /opt/radar/radar.env --scorecard  # print the scorecard
  python3 brain.py --env /opt/radar/radar.env --coin aptos # signals on one coin
"""

import argparse
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import ai
from common import (
    DAY_MS, HOUR_MS, Telegram, ago, env_float, env_int, env_str, fnum, heartbeat,
    http_json, load_env_file, log, meta_get, meta_set, now_ms, open_db, outbox_add,
    outbox_flush, signals_db, today_str,
)

USER_AGENT = "signal-hub/1.0 (personal research script)"
CG_MARKETS = ("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
              "&order=market_cap_desc&per_page=250&page={}")
COINS = "https://coins.llama.fi"
BTC = "bitcoin"

CFG = {}  # type: Dict[str, Any]

SCHEMA = """
    CREATE TABLE IF NOT EXISTS coins (
        id TEXT PRIMARY KEY, sym TEXT NOT NULL, name TEXT NOT NULL, rank INTEGER NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_coins_sym ON coins (sym, rank);
    CREATE TABLE IF NOT EXISTS convergences (
        coin TEXT NOT NULL, ts INTEGER NOT NULL, n_signals INTEGER NOT NULL, direction INTEGER NOT NULL);
"""

# Kinds that never count toward a convergence: market-wide flows, and trades the
# quality filter rejected (they are recorded only to check the filter)
MARKET_WIDE = {"stablecoin_mint", "stablecoin_burn", "whale_trade_low"}
DIRECTION_WORD = {1: "BULLISH 🟢", -1: "BEARISH 🔴", 0: "ATTENTION ⚪"}


def load_config() -> None:
    CFG["db_path"] = env_str("BRAIN_DB", "/opt/radar/brain.db")
    CFG["window_hours"] = env_float("BRAIN_WINDOW_HOURS", 48)
    CFG["min_signals"] = env_int("BRAIN_MIN_SIGNALS", 3)
    CFG["min_services"] = env_int("BRAIN_MIN_SERVICES", 2)
    CFG["scorecard_weekday"] = env_int("BRAIN_SCORECARD_WEEKDAY", 0)  # Monday
    CFG["scorecard_hour"] = env_int("BRAIN_SCORECARD_HOUR_UTC", 8)


# ---------------------------------------------------------------------------
# Coin directory: ticker -> CoinGecko id
# ---------------------------------------------------------------------------

def refresh_coins(conn: sqlite3.Connection) -> None:
    if now_ms() - int(meta_get(conn, "coins_ms") or 0) < 6 * HOUR_MS:
        return
    rows = []
    for page in range(1, 5):  # top 1000 by market cap
        try:
            data = http_json(CG_MARKETS.format(page), timeout=60, user_agent=USER_AGENT)
        except Exception as e:
            log("Coin list page {} failed: {}".format(page, e))
            break
        rows.extend(data or [])
        time.sleep(3)
    if not rows:
        return
    with conn:
        conn.execute("DELETE FROM coins")
        conn.executemany("INSERT OR REPLACE INTO coins (id, sym, name, rank) VALUES (?, ?, ?, ?)",
                         [(c["id"], (c.get("symbol") or "").upper(), c.get("name") or c["id"], i + 1)
                          for i, c in enumerate(rows) if c.get("id")])
        meta_set(conn, "coins_ms", now_ms())
    log("Coin directory: {} coins".format(len(rows)))


def resolve_symbol(conn: sqlite3.Connection, symbol: str) -> Optional[str]:
    """CoinGecko id of the largest coin using this ticker."""
    sym = symbol.upper().split("/")[0].split("-")[0]
    for suffix in ("USDT", "USDC", "PERP"):
        if sym.endswith(suffix) and len(sym) > len(suffix):
            sym = sym[: -len(suffix)]
    # Hyperliquid and Binance list some small-price coins per 1000 (kPEPE, 1000BONK)
    for prefix in ("1000000", "1000", "K"):
        if sym.startswith(prefix) and len(sym) > len(prefix) + 1:
            row = conn.execute("SELECT id FROM coins WHERE sym = ? ORDER BY rank LIMIT 1",
                               (sym[len(prefix):],)).fetchone()
            if row:
                return row[0]
    row = conn.execute("SELECT id FROM coins WHERE sym = ? ORDER BY rank LIMIT 1", (sym,)).fetchone()
    return row[0] if row else None


def coin_label(conn: sqlite3.Connection, coin: str) -> str:
    row = conn.execute("SELECT name, sym, rank FROM coins WHERE id = ?", (coin,)).fetchone()
    if not row:
        return coin
    return "{} ({}, #{})".format(row[0], row[1], row[2])


# ---------------------------------------------------------------------------
# Pricing
# ---------------------------------------------------------------------------

def prices(coins: List[str], ts_sec: Optional[int] = None) -> Dict[str, float]:
    keys = ",".join("coingecko:" + c for c in sorted(set(coins)))
    if ts_sec is None:
        url = "{}/prices/current/{}".format(COINS, keys)
    else:
        url = "{}/prices/historical/{}/{}".format(COINS, ts_sec, keys)
    data = http_json(url, timeout=60, user_agent=USER_AGENT) or {}
    return {k.split(":", 1)[1]: fnum(v.get("price")) for k, v in (data.get("coins") or {}).items()}


def price_signals(sig: sqlite3.Connection, conn: sqlite3.Connection) -> None:
    """Resolves coins, then fills p0 (now), p1d and p7d (exact historical prices)."""
    with sig:
        for row_id, symbol in sig.execute(
                "SELECT id, symbol FROM signals WHERE coin IS NULL AND symbol IS NOT NULL AND tries < 3"
                " LIMIT 200").fetchall():
            coin = resolve_symbol(conn, symbol)
            sig.execute("UPDATE signals SET coin = ?, tries = tries + 1 WHERE id = ?", (coin, row_id))

    # Price at signal time: current price for fresh signals, history for late ones
    fresh = sig.execute("SELECT id, coin, ts FROM signals WHERE coin IS NOT NULL AND p0 IS NULL"
                        " AND tries < 6 ORDER BY id LIMIT 300").fetchall()
    recent = [r for r in fresh if now_ms() - r[2] < 15 * 60 * 1000]
    if recent:
        try:
            now_prices = prices([r[1] for r in recent] + [BTC])
        except Exception as e:
            log("Current prices failed: {}".format(e))
            now_prices = {}
        with sig:
            for row_id, coin, _ in recent:
                p = now_prices.get(coin)
                if p and now_prices.get(BTC):
                    sig.execute("UPDATE signals SET p0 = ?, btc0 = ? WHERE id = ?",
                                (p, now_prices[BTC], row_id))
                else:
                    sig.execute("UPDATE signals SET tries = tries + 1 WHERE id = ?", (row_id,))
    late = [r for r in fresh if now_ms() - r[2] >= 15 * 60 * 1000][:30]
    for col_p, col_b, days, rows in (
            ("p0", "btc0", 0, late),
            ("p1d", "btc1d", 1, sig.execute(
                "SELECT id, coin, ts FROM signals WHERE p0 > 0 AND p1d IS NULL AND ts <= ?"
                " ORDER BY id LIMIT 40", (now_ms() - DAY_MS,)).fetchall()),
            ("p7d", "btc7d", 7, sig.execute(
                "SELECT id, coin, ts FROM signals WHERE p0 > 0 AND p7d IS NULL AND ts <= ?"
                " ORDER BY id LIMIT 40", (now_ms() - 7 * DAY_MS,)).fetchall())):
        for row_id, coin, ts in rows:
            try:
                p = prices([coin, BTC], int((ts + days * DAY_MS) / 1000))
            except Exception as e:
                log("Historical price failed for {}: {}".format(coin, e))
                continue
            with sig:
                if p.get(coin) and p.get(BTC):
                    sig.execute("UPDATE signals SET {} = ?, {} = ? WHERE id = ?".format(col_p, col_b),
                                (p[coin], p[BTC], row_id))
                else:
                    sig.execute("UPDATE signals SET tries = tries + 1 WHERE id = ?", (row_id,))
            time.sleep(0.2)


# ---------------------------------------------------------------------------
# Scorecard and learned weights
# ---------------------------------------------------------------------------

def edge(coin: str, direction: int, p0: float, p1: float, b0: float, b1: float) -> float:
    """Move in the signal's direction, minus what BTC did (in %).
    Attention-only signals score the size of the move beyond BTC's."""
    r = (p1 / p0 - 1.0) * 100.0
    rb = 0.0 if coin == BTC else (b1 / b0 - 1.0) * 100.0
    if direction == 0:
        return abs(r) - abs(rb)
    return direction * (r - rb)


def scorecard(sig: sqlite3.Connection) -> List[Dict[str, Any]]:
    stats = defaultdict(lambda: {"n": 0, "priced": 0, "e1": [], "e7": []})  # type: Dict[str, Dict[str, Any]]
    for kind, coin, direction, p0, b0, p1, b1, p7, b7 in sig.execute(
            "SELECT kind, coin, direction, p0, btc0, p1d, btc1d, p7d, btc7d FROM signals"):
        s = stats[kind]
        s["n"] += 1
        if p0 and b0:
            s["priced"] += 1
            if p1 and b1:
                s["e1"].append(edge(coin, direction, p0, p1, b0, b1))
            if p7 and b7:
                s["e7"].append(edge(coin, direction, p0, p7, b0, b7))
    out = []
    for kind, s in stats.items():
        row = {"kind": kind, "n": s["n"], "priced": s["priced"], "n1": len(s["e1"]),
               "n7": len(s["e7"]), "e1": None, "e7": None, "win1": None}
        if s["e1"]:
            row["e1"] = sum(s["e1"]) / len(s["e1"])
            row["win1"] = 100.0 * sum(1 for e in s["e1"] if e > 0) / len(s["e1"])
        if s["e7"]:
            row["e7"] = sum(s["e7"]) / len(s["e7"])
        row["verdict"] = verdict(row)
        out.append(row)
    out.sort(key=lambda r: (r["e1"] is None, -(r["e1"] or 0)))
    return out


def verdict(row: Dict[str, Any]) -> str:
    if row["priced"] == 0:
        return "no token to price"
    if row["n1"] < 10:
        return "collecting data ({} of 10)".format(row["n1"])
    e1, win = row["e1"], row["win1"]
    if e1 >= 0.5 and win >= 55:
        return "USEFUL"
    if e1 <= -0.5 and win <= 45:
        return "INVERSE: fade it"
    return "noise"


def learned_weights(sig: sqlite3.Connection) -> Dict[str, float]:
    """Weight per kind: 1.0 until there is data, then up to 2.0 for kinds that beat
    BTC and down to 0.2 for kinds that do not. Few samples stay close to 1.0."""
    weights = {}
    for row in scorecard(sig):
        if row["e1"] is None:
            continue
        confidence = row["n1"] / (row["n1"] + 20.0)
        shift = max(-1.0, min(1.0, row["e1"] / 3.0))
        weights[row["kind"]] = max(0.2, 1.0 + shift * confidence)
    return weights


def format_scorecard(rows: List[Dict[str, Any]]) -> str:
    lines = ["[SCORECARD] Which signals worked - {}".format(today_str()),
             "Edge = move in the signal's direction minus BTC, 1 day and 7 days later.", ""]
    if not rows:
        lines.append("No signals recorded yet.")
    for r in rows:
        if r["e1"] is None:
            lines.append("{}: {} signals | {}".format(r["kind"], r["n"], r["verdict"]))
            continue
        lines.append("{}: {} signals | 1d edge {:+.2f}% (right {:.0f}%) | 7d {} | {}".format(
            r["kind"], r["n"], r["e1"], r["win1"],
            "{:+.2f}%".format(r["e7"]) if r["e7"] is not None else "n/a", r["verdict"]))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Convergence
# ---------------------------------------------------------------------------

def check_convergence(sig: sqlite3.Connection, conn: sqlite3.Connection,
                      weights: Dict[str, float]) -> int:
    """Looks at coins that got new signals since the last check."""
    last_id = int(meta_get(conn, "convergence_last_id") or 0)
    new_coins = [r[0] for r in sig.execute(
        "SELECT DISTINCT coin FROM signals WHERE id > ? AND coin IS NOT NULL AND kind != 'convergence'",
        (last_id,))]
    top = sig.execute("SELECT MAX(id) FROM signals").fetchone()[0] or last_id
    sent = 0
    for coin in new_coins:
        if coin_convergence(sig, conn, coin, weights):
            sent += 1
    with conn:
        meta_set(conn, "convergence_last_id", top)
    return sent


def coin_convergence(sig: sqlite3.Connection, conn: sqlite3.Connection, coin: str,
                     weights: Dict[str, float]) -> bool:
    since = now_ms() - int(CFG["window_hours"] * HOUR_MS)
    rows = sig.execute(
        "SELECT ts, service, kind, direction, strength, title, link FROM signals"
        " WHERE coin = ? AND ts >= ? AND kind != 'convergence' ORDER BY ts", (coin, since)).fetchall()
    latest = {}  # type: Dict[Tuple[str, str], tuple]
    for row in rows:
        if row[2] not in MARKET_WIDE:
            latest[(row[1], row[2])] = row  # one vote per service and kind
    services = {s for s, _ in latest}
    if len(latest) < CFG["min_signals"] or len(services) < CFG["min_services"]:
        return False

    bull = sum(weights.get(k, 1.0) * r[4] for (s, k), r in latest.items() if r[3] > 0)
    bear = sum(weights.get(k, 1.0) * r[4] for (s, k), r in latest.items() if r[3] < 0)
    net = bull - bear
    direction = 1 if net >= 1.5 else -1 if net <= -1.5 else 0
    score = abs(net) + 0.5 * sum(weights.get(k, 1.0) for (s, k), r in latest.items() if r[3] == 0)

    # Once per coin a day, unless more signals arrive or the direction changes
    prev = conn.execute("SELECT n_signals, direction FROM convergences WHERE coin = ? AND ts >= ?"
                        " ORDER BY ts DESC LIMIT 1", (coin, now_ms() - DAY_MS)).fetchone()
    if prev and len(latest) < prev[0] + 2 and direction == prev[1]:
        return False

    label = coin_label(conn, coin)
    lines = ["[CONVERGENCE] {} | {}".format(label, DIRECTION_WORD[direction]),
             "{} independent signals from {} services in {:.0f}h | score {:.1f}".format(
                 len(latest), len(services), CFG["window_hours"], score)]
    if bull and bear:
        lines.append("Mixed: bullish weight {:.1f} vs bearish {:.1f}".format(bull, bear))
    lines.append("")
    for (service, kind), (ts, _, _, d, _, title, link) in sorted(latest.items(), key=lambda kv: kv[1][0]):
        mark = {1: "🟢", -1: "🔴", 0: "⚪"}[d]
        w = weights.get(kind)
        lines.append("{} {} [{}{}] {}".format(mark, ago(ts), kind,
                                             " w{:.1f}".format(w) if w is not None else "", title[:110]))
    lines.append("")
    lines.append("Weights come from the scorecard (1.0 until a kind has a track record).")
    text = "\n".join(lines)
    text += ai.format_brief(ai.brief(conn, text))

    with conn:
        conn.execute("INSERT INTO convergences (coin, ts, n_signals, direction) VALUES (?, ?, ?, ?)",
                     (coin, now_ms(), len(latest), direction))
        outbox_add(conn, text)
    # The convergence itself is a signal, so the scorecard shows whether it works
    with sig:
        sig.execute("INSERT INTO signals (ts, service, kind, coin, direction, strength, title)"
                    " VALUES (?, 'brain', 'convergence', ?, ?, ?, ?)",
                    (now_ms(), coin, direction, min(score, 5.0), "{} signals on {}".format(len(latest), label)))
    return True


def show_coin(sig: sqlite3.Connection, conn: sqlite3.Connection, coin: str) -> None:
    coin = resolve_symbol(conn, coin) or coin
    print("Signals on {} (last 30 days)".format(coin_label(conn, coin)))
    for ts, service, kind, d, title, p0, p1 in sig.execute(
            "SELECT ts, service, kind, direction, title, p0, p1d FROM signals WHERE coin = ? AND ts >= ?"
            " ORDER BY ts", (coin, now_ms() - 30 * DAY_MS)):
        moved = " | 1d {:+.1f}%".format((p1 / p0 - 1) * 100) if p0 and p1 else ""
        print("{} {:>8} {:+d} {:<22} {}{}".format(
            datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%m-%d %H:%M"),
            service, d, kind, title[:80], moved))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def cycle(sig: sqlite3.Connection, conn: sqlite3.Connection, telegram: Telegram) -> None:
    refresh_coins(conn)
    price_signals(sig, conn)
    sent = check_convergence(sig, conn, learned_weights(sig))
    now = datetime.now(timezone.utc)
    if now.weekday() == CFG["scorecard_weekday"] and now.hour >= CFG["scorecard_hour"] \
            and meta_get(conn, "scorecard_day") != today_str():
        with conn:
            meta_set(conn, "scorecard_day", today_str())
            outbox_add(conn, format_scorecard(scorecard(sig)))
    outbox_flush(conn, telegram)
    heartbeat(conn)
    if sent:
        log("Convergence alerts: {}".format(sent))


def main() -> None:
    ap = argparse.ArgumentParser(description="Signal Hub v1")
    ap.add_argument("--env", default="", help="path to env file with tokens")
    ap.add_argument("--once", action="store_true", help="run one cycle and exit")
    ap.add_argument("--scorecard", action="store_true", help="print the scorecard")
    ap.add_argument("--coin", default="", help="list recent signals on one coin (id or ticker)")
    args = ap.parse_args()

    load_env_file(args.env)
    load_config()
    telegram = Telegram.from_env(USER_AGENT)
    conn = open_db(CFG["db_path"], SCHEMA)
    sig = signals_db()

    if args.scorecard:
        print(format_scorecard(scorecard(sig)))
        return
    if args.coin:
        refresh_coins(conn)
        show_coin(sig, conn, args.coin)
        return
    if args.once:
        cycle(sig, conn, telegram)
        return

    log("Running forever, a cycle every 60 seconds (AI summaries: {})".format(
        "on" if ai.enabled() else "off"))
    while True:
        started = time.time()
        try:
            cycle(sig, conn, telegram)
        except Exception as e:
            log("Unexpected error: {}".format(e))
        time.sleep(max(10.0, 60 - (time.time() - started)))


if __name__ == "__main__":
    main()
