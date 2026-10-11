#!/usr/bin/env python3
"""
Flow Watch v1

Follows the money behind price moves, from free public data:

  1. Binance futures positioning (top perps by volume)
       - OI BUILD-UP: open interest jumps within 4 hours. With price up it is a
         long build-up, with price down a short build-up, with price flat a
         coiled spring that often breaks out.
       - CROWDED FUNDING: funding so high or low that one side pays heavily;
         crowded trades tend to unwind (read contrarian).
       - TOP TRADERS VS CROWD: Binance's largest accounts positioned against the
         majority of accounts. This is the smart-money read.
  2. Stablecoin supply, read directly from the chains (USDT on Ethereum and
     Tron, USDC on Ethereum): large mints are new money arriving, large burns
     are money leaving.

Every alert is also written to the shared signal ledger for brain.py.

Standard library only. Python 3.8+.

Usage:
  python3 flows.py --env /opt/radar/radar.env          # run forever
  python3 flows.py --env /opt/radar/radar.env --once
"""

import argparse
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

from common import (
    DAY_MS, HOUR_MS, Telegram, env_float, env_int, env_str, fnum, heartbeat, http_json,
    load_env_file, log, meta_get, meta_set, now_ms, open_db, outbox_add, outbox_flush,
    record_signal, usd,
)

USER_AGENT = "flow-watch/1.0 (personal research script)"
FAPI = "https://fapi.binance.com"
ETH_RPC = "https://ethereum-rpc.publicnode.com"
TRON_API = "https://api.trongrid.io/wallet/triggerconstantcontract"
# (name, chain, contract, decimals)
STABLECOINS = [
    ("USDT", "Ethereum", "0xdAC17F958D2ee523a2206206994597C13D831ec7", 6),
    ("USDC", "Ethereum", "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", 6),
    ("USDT", "Tron", "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", 6),
]
TOTAL_SUPPLY = "0x18160ddd"  # totalSupply()

CFG = {}  # type: Dict[str, Any]

SCHEMA = """
    CREATE TABLE IF NOT EXISTS oi (
        sym TEXT NOT NULL, ts INTEGER NOT NULL, oi REAL NOT NULL, price REAL NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_oi ON oi (sym, ts);
    CREATE TABLE IF NOT EXISTS supply (
        name TEXT NOT NULL, chain TEXT NOT NULL, ts INTEGER NOT NULL, amount REAL NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_supply ON supply (name, chain, ts);
    CREATE TABLE IF NOT EXISTS cooldown (key TEXT PRIMARY KEY, until INTEGER NOT NULL);
"""


def load_config() -> None:
    CFG["db_path"] = env_str("FLOWS_DB", "/opt/radar/flows.db")
    CFG["interval"] = max(120, env_int("FLOWS_INTERVAL_SECONDS", 300))
    CFG["top_n"] = env_int("FLOWS_TOP_PERPS", 120)
    CFG["oi_jump_pct"] = env_float("FLOWS_OI_JUMP_PCT", 20.0)          # OI +20% in 4h
    CFG["oi_min_usd"] = env_float("FLOWS_OI_MIN_USD", 20000000)         # ignore small perps
    CFG["funding_extreme"] = env_float("FLOWS_FUNDING_EXTREME", 0.001)  # 0.1% per 8h
    CFG["top_long_ratio"] = env_float("FLOWS_TOP_LONG_RATIO", 1.5)      # top traders long/short
    CFG["crowd_short_ratio"] = env_float("FLOWS_CROWD_SHORT_RATIO", 0.9)  # accounts long/short
    CFG["stable_move_usd"] = env_float("FLOWS_STABLE_MOVE_USD", 500000000)  # $500M in 24h


def fapi(path: str) -> Any:
    return http_json(FAPI + path, timeout=30, user_agent=USER_AGENT)


def on_cooldown(conn: sqlite3.Connection, key: str, hours: float) -> bool:
    """True if this alert fired recently; otherwise starts its cooldown."""
    row = conn.execute("SELECT until FROM cooldown WHERE key = ?", (key,)).fetchone()
    if row and row[0] > now_ms():
        return True
    with conn:
        conn.execute("INSERT OR REPLACE INTO cooldown (key, until) VALUES (?, ?)",
                     (key, now_ms() + int(hours * HOUR_MS)))
    return False


def alert(conn: sqlite3.Connection, kind: str, sym: str, direction: int, text: str,
          strength: float = 1.0, coin: Optional[str] = None) -> None:
    with conn:
        outbox_add(conn, text)
    record_signal("flows", kind, text.splitlines()[0], direction=direction,
                  coin=coin, symbol=None if coin else sym, strength=strength)


# ---------------------------------------------------------------------------
# Binance futures positioning
# ---------------------------------------------------------------------------

def top_perps() -> Tuple[List[str], Dict[str, dict]]:
    tickers = [t for t in fapi("/fapi/v1/ticker/24hr") or []
               if t.get("symbol", "").endswith("USDT")]
    tickers.sort(key=lambda t: -fnum(t.get("quoteVolume")))
    return [t["symbol"] for t in tickers[: CFG["top_n"]]], {t["symbol"]: t for t in tickers}


def base(sym: str) -> str:
    return sym[:-4] if sym.endswith("USDT") else sym


def check_open_interest(conn: sqlite3.Connection, symbols: List[str], tickers: Dict[str, dict]) -> int:
    def fetch(sym: str) -> Optional[Tuple[str, float]]:
        try:
            return sym, fnum((fapi("/fapi/v1/openInterest?symbol=" + sym) or {}).get("openInterest"))
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = [r for r in pool.map(fetch, symbols) if r and r[1] > 0]
    ts = now_ms()
    with conn:
        conn.executemany("INSERT INTO oi (sym, ts, oi, price) VALUES (?, ?, ?, ?)",
                         [(s, ts, oi, fnum(tickers[s].get("lastPrice"))) for s, oi in results])
        conn.execute("DELETE FROM oi WHERE ts < ?", (ts - 2 * DAY_MS,))

    # Compare with the oldest snapshot between 3.5h and 4.5h ago
    old = {s: (o, p) for s, _, o, p in conn.execute(
        "SELECT sym, MIN(ts), oi, price FROM oi WHERE ts BETWEEN ? AND ? GROUP BY sym",
        (ts - int(4.5 * HOUR_MS), ts - int(3.5 * HOUR_MS)))}
    sent = 0
    for sym, oi_now in results:
        if sym not in old or old[sym][0] <= 0:
            continue
        price = fnum(tickers[sym].get("lastPrice"))
        oi_change = (oi_now / old[sym][0] - 1.0) * 100.0
        price_change = (price / old[sym][1] - 1.0) * 100.0 if old[sym][1] > 0 else 0.0
        notional = oi_now * price
        if oi_change < CFG["oi_jump_pct"] or notional < CFG["oi_min_usd"]:
            continue
        if on_cooldown(conn, "oi:" + sym, 12):
            continue
        if price_change >= 1.0:
            kind, direction, read = "oi_long_buildup", 1, "new longs piling in with price rising"
        elif price_change <= -1.0:
            kind, direction, read = "oi_short_buildup", -1, "new shorts piling in with price falling"
        else:
            kind, direction, read = "oi_coiled", 0, "positions building while price is flat: a big move often follows"
        alert(conn, kind, base(sym), direction, (
            "[OI BUILD-UP] {coin}: open interest {oi:+.0f}% in 4h ({notional}), price {price:+.1f}%\n"
            "Read: {read}\n"
            "24h volume: {vol} | Binance perp {sym}").format(
                coin=base(sym), oi=oi_change, notional=usd(notional), price=price_change, read=read,
                vol=usd(fnum(tickers[sym].get("quoteVolume"))), sym=sym),
            strength=min(2.0, oi_change / CFG["oi_jump_pct"]))
        sent += 1
    return sent


def check_funding(conn: sqlite3.Connection, symbols: List[str]) -> int:
    wanted = set(symbols)
    sent = 0
    for p in fapi("/fapi/v1/premiumIndex") or []:
        sym = p.get("symbol")
        rate = fnum(p.get("lastFundingRate"))
        if sym not in wanted or abs(rate) < CFG["funding_extreme"]:
            continue
        if on_cooldown(conn, "funding:" + sym, 24):
            continue
        crowded_longs = rate > 0
        alert(conn, "funding_crowded_longs" if crowded_longs else "funding_crowded_shorts", base(sym),
              -1 if crowded_longs else 1, (
                  "[CROWDED FUNDING] {coin}: funding {rate:+.3f}% per 8h ({apr:+.0f}% a year)\n"
                  "Read: {read}\n"
                  "Binance perp {sym}").format(
                      coin=base(sym), rate=rate * 100, apr=rate * 3 * 365 * 100, sym=sym,
                      read="longs pay heavily to stay in; crowded longs tend to get flushed"
                      if crowded_longs else
                      "shorts pay heavily to stay in; a short squeeze becomes likely"))
        sent += 1
    return sent


def check_top_traders(conn: sqlite3.Connection, symbols: List[str]) -> int:
    """Binance's top traders (largest accounts by margin) against all accounts."""
    def fetch(sym: str) -> Optional[Tuple[str, float, float]]:
        try:
            top = fapi("/futures/data/topLongShortPositionRatio?period=1h&limit=1&symbol=" + sym)
            crowd = fapi("/futures/data/globalLongShortAccountRatio?period=1h&limit=1&symbol=" + sym)
            return sym, fnum(top[-1]["longShortRatio"]), fnum(crowd[-1]["longShortRatio"])
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=3) as pool:
        rows = [r for r in pool.map(fetch, symbols[:60]) if r]
    sent = 0
    long_top, short_crowd = CFG["top_long_ratio"], CFG["crowd_short_ratio"]
    for sym, top, crowd in rows:
        if top >= long_top and crowd <= short_crowd:
            direction, read = 1, "top traders are long while most accounts are short"
        elif top > 0 and 1.0 / top >= long_top and crowd >= 1.0 / short_crowd:
            direction, read = -1, "top traders are short while most accounts are long"
        else:
            continue
        if on_cooldown(conn, "top:{}:{}".format(sym, direction), 24):
            continue
        alert(conn, "top_traders_long" if direction > 0 else "top_traders_short", base(sym), direction, (
            "[SMART VS CROWD] {coin}: {read}\n"
            "Top traders long/short (by position): {top:.2f} | all accounts: {crowd:.2f}\n"
            "Binance perp {sym}").format(coin=base(sym), read=read, top=top, crowd=crowd, sym=sym))
        sent += 1
    return sent


# ---------------------------------------------------------------------------
# Stablecoin supply
# ---------------------------------------------------------------------------

def total_supply(chain: str, contract: str, decimals: int) -> float:
    if chain == "Ethereum":
        res = http_json(ETH_RPC, body={"jsonrpc": "2.0", "id": 1, "method": "eth_call",
                                       "params": [{"to": contract, "data": TOTAL_SUPPLY}, "latest"]},
                        timeout=30, user_agent=USER_AGENT)
        return int(res["result"], 16) / 10 ** decimals
    res = http_json(TRON_API, body={"owner_address": "T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb",
                                    "contract_address": contract, "function_selector": "totalSupply()",
                                    "visible": True}, timeout=30, user_agent=USER_AGENT)
    return int(res["constant_result"][0], 16) / 10 ** decimals


def check_stablecoins(conn: sqlite3.Connection) -> int:
    ts = now_ms()
    for name, chain, contract, decimals in STABLECOINS:
        try:
            amount = total_supply(chain, contract, decimals)
        except Exception as e:
            log("{} on {} supply failed: {}".format(name, chain, e))
            continue
        with conn:
            conn.execute("INSERT INTO supply (name, chain, ts, amount) VALUES (?, ?, ?, ?)",
                         (name, chain, ts, amount))
    with conn:
        conn.execute("DELETE FROM supply WHERE ts < ?", (ts - 3 * DAY_MS,))

    # Net change of all tracked supply over the last 24 hours
    total_now = total_then = 0.0
    parts = []
    for name, chain, _, _ in STABLECOINS:
        now_row = conn.execute("SELECT amount FROM supply WHERE name = ? AND chain = ? ORDER BY ts DESC"
                               " LIMIT 1", (name, chain)).fetchone()
        then_row = conn.execute("SELECT amount FROM supply WHERE name = ? AND chain = ? AND ts <= ?"
                                " ORDER BY ts DESC LIMIT 1", (name, chain, ts - DAY_MS)).fetchone()
        if not now_row or not then_row:
            return 0  # less than a day of history yet
        total_now += now_row[0]
        total_then += then_row[0]
        if abs(now_row[0] - then_row[0]) >= 1e7:
            parts.append("{} {} {}".format(name, chain, usd(now_row[0] - then_row[0], signed=True)))
    change = total_now - total_then
    if abs(change) < CFG["stable_move_usd"]:
        return 0
    mint = change > 0
    if on_cooldown(conn, "stable:" + ("mint" if mint else "burn"), 12):
        return 0
    alert(conn, "stablecoin_mint" if mint else "stablecoin_burn", "BTC", 1 if mint else -1, (
        "[STABLECOIN {what}] {change} of USDT/USDC in 24h\n"
        "{parts}\n"
        "Tracked supply now {total}\n"
        "Read: {read}").format(
            what="MINT" if mint else "BURN", change=usd(change, signed=True),
            parts=" | ".join(parts) or "spread across chains", total=usd(total_now),
            read="fresh dollars are arriving on-chain, usually ahead of buying"
            if mint else "dollars are being redeemed, liquidity is leaving"),
        coin="bitcoin")
    return 1


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def cycle(conn: sqlite3.Connection, telegram: Telegram) -> None:
    sent = 0
    try:
        symbols, tickers = top_perps()
        sent += check_open_interest(conn, symbols, tickers)
        sent += check_funding(conn, symbols)
        # Positioning ratios change slowly; every 15 minutes is enough
        if now_ms() - int(meta_get(conn, "top_traders_ms") or 0) >= 15 * 60 * 1000:
            sent += check_top_traders(conn, symbols)
            with conn:
                meta_set(conn, "top_traders_ms", now_ms())
    except Exception as e:
        log("Binance checks failed: {}".format(e))
    if now_ms() - int(meta_get(conn, "stable_ms") or 0) >= 30 * 60 * 1000:
        sent += check_stablecoins(conn)
        with conn:
            meta_set(conn, "stable_ms", now_ms())
    outbox_flush(conn, telegram)
    heartbeat(conn)
    log("Cycle done: {} alerts".format(sent))


def main() -> None:
    ap = argparse.ArgumentParser(description="Flow Watch v1")
    ap.add_argument("--env", default="", help="path to env file with tokens")
    ap.add_argument("--once", action="store_true", help="run one cycle and exit")
    args = ap.parse_args()

    load_env_file(args.env)
    load_config()
    telegram = Telegram.from_env(USER_AGENT)
    conn = open_db(CFG["db_path"], SCHEMA)
    if args.once:
        cycle(conn, telegram)
        return
    log("Running forever, a cycle every {} seconds".format(CFG["interval"]))
    while True:
        started = time.time()
        try:
            cycle(conn, telegram)
        except Exception as e:
            log("Unexpected error: {}".format(e))
        time.sleep(max(30.0, CFG["interval"] - (time.time() - started)))


if __name__ == "__main__":
    main()
