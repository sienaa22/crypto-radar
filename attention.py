#!/usr/bin/env python3
"""
Attention Radar v2

A daily "what deserves attention today" report. It scores coins by how much
attention they get from several INDEPENDENT free sources, and boosts anything
the other radar scripts already flagged.

Sources (all free, no key):
  1. CoinGecko /search/trending   -> what people are searching for right now
  2. CoinGecko /coins/markets     -> 24h price change and volume / market cap
  3. Hyperliquid metaAndAssetCtxs -> open interest change over 24h, funding rate
  4. whales.db                    -> smart-money consensus and good big trades
  5. heat.db                      -> protocols with accelerating fees

Coins are matched by CoinGecko id, not by ticker, so two coins that share a
symbol are not mixed up. The point weights are STARTING GUESSES to be tuned.

No trade advice is produced. This is an attention signal only.

Standard library only. Python 3.8+.

Usage:
  python3 attention.py --env /opt/radar/radar.env            # run forever (daily)
  python3 attention.py --env /opt/radar/radar.env --once      # one report now
  python3 attention.py --env /opt/radar/radar.env --test-telegram
"""

import argparse
import sqlite3
import sys
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from common import (
    DAY_MS, HOUR_MS, Telegram, daily_due, env_float, env_int, env_str, fnum,
    heartbeat, http_json, load_env_file, log, now_ms, open_db, open_db_readonly,
    outbox_add, outbox_flush, today_str, usd,
)

USER_AGENT = "attention-radar/2.0 (personal research script)"
CG = "https://api.coingecko.com/api/v3"
HL_INFO = "https://api.hyperliquid.xyz/info"

CFG = {}  # type: Dict[str, Any]

SCHEMA = """
    CREATE TABLE IF NOT EXISTS oi_snapshots (
        sym TEXT NOT NULL, ts INTEGER NOT NULL, oi_usd REAL NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_oi ON oi_snapshots (sym, ts);
"""


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config() -> None:
    CFG["db_path"] = env_str("ATT_DB", "/opt/radar/attention.db")
    CFG["cg_key"] = env_str("COINGECKO_API_KEY")  # optional demo key
    CFG["whales_db"] = env_str("HL_DB", "/opt/radar/whales.db")
    CFG["heat_db"] = env_str("HEAT_DB", "/opt/radar/heat.db")

    CFG["markets_pages"] = env_int("ATT_MARKETS_PAGES", 2)   # 250 coins per page
    CFG["top_n"] = env_int("ATT_TOP_N", 15)

    # Point weights (starting guesses)
    CFG["w_trending"] = env_float("ATT_W_TRENDING", 3.0)
    CFG["w_vol_spike"] = env_float("ATT_W_VOL_SPIKE", 2.0)
    CFG["w_price_24h"] = env_float("ATT_W_PRICE_24H", 1.0)
    CFG["w_oi_jump"] = env_float("ATT_W_OI_JUMP", 2.0)
    CFG["w_funding"] = env_float("ATT_W_FUNDING", 1.0)
    CFG["w_consensus"] = env_float("ATT_W_CONSENSUS", 3.0)
    CFG["w_heat"] = env_float("ATT_W_HEAT", 2.0)

    # Thresholds (starting guesses)
    CFG["vol_mcap_hot"] = env_float("ATT_VOL_MCAP_HOT", 0.15)    # 24h vol >= 15% of mcap
    CFG["price_24h_hot"] = env_float("ATT_PRICE_24H_HOT", 10.0)  # +/-10%
    CFG["funding_hot"] = env_float("ATT_FUNDING_HOT", 0.0003)    # hourly funding magnitude
    CFG["oi_change_hot"] = env_float("ATT_OI_CHANGE_HOT", 30.0)  # OI +30% in 24h
    CFG["oi_min_usd"] = env_float("ATT_OI_MIN_USD", 1000000)     # ignore tiny perps

    CFG["daily_hour_utc"] = env_int("ATT_DAILY_HOUR_UTC", 6)


def sym_norm(s: Optional[str]) -> str:
    return (s or "").strip().upper()


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

def cg_get(path: str) -> Any:
    headers = {"x-cg-demo-api-key": CFG["cg_key"]} if CFG["cg_key"] else None
    return http_json(CG + path, headers=headers, user_agent=USER_AGENT)


def source_markets() -> Dict[str, dict]:
    """{coingecko_id: market row} for the most traded coins."""
    out = {}  # type: Dict[str, dict]
    for page in range(1, CFG["markets_pages"] + 1):
        try:
            data = cg_get("/coins/markets?vs_currency=usd&order=volume_desc&per_page=250"
                          "&page={}&price_change_percentage=24h,7d".format(page))
        except Exception as e:
            log("Markets page {} failed: {}".format(page, e))
            break
        for c in data or []:
            if not c.get("id"):
                continue
            mcap = fnum(c.get("market_cap"))
            out[c["id"]] = {
                "sym": sym_norm(c.get("symbol")),
                "name": c.get("name") or c["id"],
                "price_24h": fnum(c.get("price_change_percentage_24h_in_currency")),
                "vol_mcap": fnum(c.get("total_volume")) / mcap if mcap > 0 else 0.0,
                "mcap": mcap,
            }
        time.sleep(2.0)
    return out


def source_trending() -> Dict[str, dict]:
    """{coingecko_id: {sym, name, rank}}."""
    out = {}  # type: Dict[str, dict]
    try:
        data = cg_get("/search/trending")
    except Exception as e:
        log("Trending fetch failed: {}".format(e))
        return out
    for i, item in enumerate((data or {}).get("coins") or []):
        c = item.get("item") or {}
        if c.get("id"):
            out[c["id"]] = {"sym": sym_norm(c.get("symbol")), "name": c.get("name") or c["id"],
                            "rank": i + 1}
    return out


def source_hyperliquid() -> Dict[str, dict]:
    """{ticker: perp context} with open interest in USD and the hourly funding rate."""
    out = {}  # type: Dict[str, dict]
    try:
        meta, ctxs = http_json(HL_INFO, body={"type": "metaAndAssetCtxs"}, user_agent=USER_AGENT)
    except Exception as e:
        log("Hyperliquid fetch failed: {}".format(e))
        return out
    for asset, ctx in zip((meta or {}).get("universe") or [], ctxs or []):
        sym = sym_norm(asset.get("name"))
        ctx = ctx or {}
        price = fnum(ctx.get("markPx")) or fnum(ctx.get("midPx")) or fnum(ctx.get("prevDayPx"))
        if sym:
            out[sym] = {"oi_usd": fnum(ctx.get("openInterest")) * price,
                        "funding": fnum(ctx.get("funding")), "oi_change": None}
    return out


def add_oi_changes(conn: sqlite3.Connection, hl: Dict[str, dict]) -> None:
    """Stores an OI snapshot and fills each perp's OI change against ~24h ago."""
    ts = now_ms()
    with conn:
        conn.executemany("INSERT INTO oi_snapshots (sym, ts, oi_usd) VALUES (?, ?, ?)",
                         [(s, ts, h["oi_usd"]) for s, h in hl.items()])
        conn.execute("DELETE FROM oi_snapshots WHERE ts < ?", (ts - 3 * DAY_MS,))
    # Oldest snapshot between 20h and 28h ago, per perp
    rows = conn.execute(
        "SELECT sym, MIN(ts), oi_usd FROM oi_snapshots WHERE ts BETWEEN ? AND ? GROUP BY sym",
        (ts - 28 * HOUR_MS, ts - 20 * HOUR_MS)).fetchall()
    for sym, _, old in rows:
        if sym in hl and old > 0:
            hl[sym]["oi_change"] = (hl[sym]["oi_usd"] / old - 1.0) * 100.0


def source_whales() -> Dict[str, List[str]]:
    """{ticker: reasons} from smart-money consensus and well-rated big trades."""
    out = defaultdict(list)  # type: Dict[str, List[str]]
    conn = open_db_readonly(CFG["whales_db"])
    if conn is None:
        return out
    try:
        for coin, side in conn.execute(
                "SELECT DISTINCT coin, side FROM consensus_alerts WHERE ts >= ?",
                (now_ms() - 2 * DAY_MS,)):
            out[sym_norm(coin)].append("whale consensus " + side)
        try:
            rows = conn.execute(
                "SELECT coin, side, COUNT(*), SUM(added_usd) FROM trade_alerts "
                "WHERE ts >= ? AND sent = 1 GROUP BY coin, side", (now_ms() - DAY_MS,)).fetchall()
        except sqlite3.Error:
            rows = []  # whales.py not upgraded yet
        for coin, side, n, total in rows:
            out[sym_norm(coin)].append("{} big {} trade{} ({})".format(
                n, side.lower(), "s" if n > 1 else "", usd(total)))
    except sqlite3.Error as e:
        log("whales.db read failed: {}".format(e))
    finally:
        conn.close()
    return out


def source_heat() -> Dict[str, float]:
    """{coingecko_id: fee growth} for protocols flagged in the last two days."""
    out = {}  # type: Dict[str, float]
    conn = open_db_readonly(CFG["heat_db"])
    if conn is None:
        return out
    try:
        for gecko_id, accel in conn.execute(
                "SELECT gecko_id, MAX(fee_accel) FROM signals WHERE date >= date('now','-1 day')"
                " AND gecko_id IS NOT NULL GROUP BY gecko_id"):
            out[gecko_id] = fnum(accel)
    except sqlite3.Error as e:
        log("heat.db read failed: {}".format(e))
    finally:
        conn.close()
    return out


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def build_report(conn: sqlite3.Connection) -> Tuple[List[dict], Dict[str, int]]:
    log("Fetching sources")
    markets = source_markets()
    trending = source_trending()
    hl = source_hyperliquid()
    add_oi_changes(conn, hl)
    whales = source_whales()
    heat = source_heat()
    counts = {"trending": len(trending), "markets": len(markets), "hl": len(hl),
              "whales": len(whales), "heat": len(heat)}

    # A ticker means the largest coin with that symbol (for perps and whale data)
    by_sym = {}  # type: Dict[str, str]
    for cid, m in sorted(markets.items(), key=lambda kv: kv[1]["mcap"]):
        by_sym[m["sym"]] = cid

    def info_of(cid: str) -> Tuple[str, str]:
        m = markets.get(cid) or trending.get(cid) or {}
        return m.get("sym") or cid.upper(), m.get("name") or cid

    ids = set(markets) | set(trending) | set(heat)
    for sym in set(hl) | set(whales):
        ids.add(by_sym.get(sym, "ticker:" + sym))

    scored = []  # type: List[dict]
    for cid in ids:
        if cid.startswith("ticker:"):
            sym, name = cid.split(":", 1)[1], cid.split(":", 1)[1]
        else:
            sym, name = info_of(cid)
        reasons = []  # type: List[str]
        score = 0.0
        sources_hit = 0

        if cid in trending:
            score += CFG["w_trending"]
            sources_hit += 1
            reasons.append("trending #{}".format(trending[cid]["rank"]))

        m = markets.get(cid)
        if m:
            if m["vol_mcap"] >= CFG["vol_mcap_hot"]:
                score += CFG["w_vol_spike"]
                sources_hit += 1
                reasons.append("vol/mcap {:.0%}".format(m["vol_mcap"]))
            if abs(m["price_24h"]) >= CFG["price_24h_hot"]:
                score += CFG["w_price_24h"]
                reasons.append("24h {:+.0f}%".format(m["price_24h"]))

        # Perp and whale data are per ticker: they belong to the largest coin using it
        owns_ticker = by_sym.get(sym, "ticker:" + sym) == cid
        h = hl.get(sym) if owns_ticker else None
        if h:
            if (h["oi_change"] is not None and h["oi_change"] >= CFG["oi_change_hot"]
                    and h["oi_usd"] >= CFG["oi_min_usd"]):
                score += CFG["w_oi_jump"]
                sources_hit += 1
                reasons.append("OI {:+.0f}% in 24h ({})".format(h["oi_change"], usd(h["oi_usd"])))
            if abs(h["funding"]) >= CFG["funding_hot"]:
                score += CFG["w_funding"]
                reasons.append("funding {} ({:.4f}/h)".format(
                    "longs pay" if h["funding"] > 0 else "shorts pay", h["funding"]))

        if owns_ticker and whales.get(sym):
            score += CFG["w_consensus"]
            sources_hit += 1
            reasons.extend(whales[sym])

        if cid in heat:
            score += CFG["w_heat"]
            sources_hit += 1
            reasons.append("fees {:.1f}x prior month".format(heat[cid]))

        if score > 0:
            scored.append({"sym": sym, "name": name, "score": score,
                           "sources": sources_hit, "reasons": reasons})

    scored.sort(key=lambda r: (r["sources"], r["score"]), reverse=True)
    return scored[: CFG["top_n"]], counts


def format_report(rows: List[dict], counts: Dict[str, int]) -> str:
    lines = ["[ATTENTION RADAR] {}".format(today_str()),
             "Coins getting attention from several independent sources.",
             "Sources: trending {trending}, markets {markets}, HL perps {hl}, "
             "whale signals {whales}, heat {heat}".format(**counts),
             ""]
    if not rows:
        lines.append("Nothing stood out today.")
        return "\n".join(lines)
    for r in rows:
        star = " *" if r["sources"] >= 3 else ""
        lines.append("{} ({}){}  [{} sources]".format(r["sym"], r["name"][:20], star, r["sources"]))
        lines.append("  " + " | ".join(r["reasons"]))
    lines.append("")
    lines.append("* = 3+ independent sources agree. Not trade advice; a starting point to research.")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_once(conn: sqlite3.Connection) -> None:
    try:
        rows, counts = build_report(conn)
    except Exception as e:
        log("Report failed: {}".format(e))
        return
    with conn:
        outbox_add(conn, format_report(rows, counts))
    log("Report queued: {} coins".format(len(rows)))


def snapshot_oi(conn: sqlite3.Connection) -> None:
    """Between reports, keep OI history so the next report can measure 24h change."""
    hl = source_hyperliquid()
    if hl:
        add_oi_changes(conn, hl)


def main() -> None:
    ap = argparse.ArgumentParser(description="Attention Radar v2")
    ap.add_argument("--env", default="", help="path to env file with tokens")
    ap.add_argument("--once", action="store_true", help="one report now")
    ap.add_argument("--test-telegram", action="store_true", help="send a test message")
    args = ap.parse_args()

    load_env_file(args.env)
    load_config()
    telegram = Telegram.from_env(USER_AGENT)

    if args.test_telegram:
        sys.exit(0 if telegram.send("Attention Radar test message. Telegram is working.") else 1)

    conn = open_db(CFG["db_path"], SCHEMA)
    if args.once:
        run_once(conn)
        outbox_flush(conn, telegram)
        return

    log("Running forever; one report per day from hour {} UTC".format(CFG["daily_hour_utc"]))
    while True:
        try:
            if daily_due(conn, "last_daily", CFG["daily_hour_utc"]):
                run_once(conn)
            else:
                snapshot_oi(conn)
            outbox_flush(conn, telegram)
            heartbeat(conn)
        except Exception as e:
            log("Unexpected error: {}".format(e))
        time.sleep(1800)


if __name__ == "__main__":
    main()
