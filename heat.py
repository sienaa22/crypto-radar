#!/usr/bin/env python3
"""
Heat Analyzer v2

Finds DeFi protocols whose fundamentals (fees, revenue) are growing faster than
their token price has moved, using only free DefiLlama endpoints. Sends a short
daily report to Telegram and logs every candidate for later validation.

What it does NOT claim: that a positive gap means the price WILL rise. It is a
hypothesis to be checked against the logged forward returns, not a trade signal.

Core metrics (all computed by code):
  - fee_growth:   fees of the last 30 days / fees of the 30 days before
  - momentum_7d:  fees of the last 7 days / the 7 days before (is it still going?)
  - rev_growth:   same as fee_growth, for revenue
  - retention:    revenue / fees  (low = paid out to LPs or given back as incentives)
  - pf:           market cap / annualized fees;  rel_pf = pf / category median
  - price_30d:    token price change over 30 days
  - price_vs_cat: price_30d minus the median price_30d of the same category
  - gap:          fee growth (%) minus price_vs_cat

Validation: the token price is stored at signal time and again 7 and 30 days
later, next to ETH as a market baseline. --report prints the results.

Standard library only. Python 3.8+.

Usage:
  python3 heat.py --env /opt/radar/radar.env            # run forever (daily)
  python3 heat.py --env /opt/radar/radar.env --once      # one report now
  python3 heat.py --env /opt/radar/radar.env --report    # validation stats
  python3 heat.py --env /opt/radar/radar.env --export heat.csv
  python3 heat.py --env /opt/radar/radar.env --test-telegram
"""

import argparse
import csv
import sqlite3
import sys
import time
from collections import defaultdict
from statistics import median
from typing import Any, Dict, List, Optional, Tuple

from common import (
    DAY_MS, Telegram, daily_due, env_float, env_int, env_str, fnum, heartbeat,
    http_json, load_env_file, log, meta_get, meta_set, now_ms, open_db, outbox_add,
    outbox_flush, pct, today_str, usd,
)

USER_AGENT = "heat-analyzer/2.0 (personal research script)"
BASE = "https://api.llama.fi"
COINS = "https://coins.llama.fi"
BASELINE = "coingecko:ethereum"
PRICE_BATCH = 40

# Protocol categories this fundamentals model does not fit
SKIP_CATEGORIES = {
    "CEX", "Chain", "Canonical Bridge", "Bridge", "Infrastructure",
    "Staking Pool", "Liquid Staking", "Liquid Restaking", "Restaking",
    "Risk Curators", "Onchain Capital Allocator",
}

CFG = {}  # type: Dict[str, Any]

SCHEMA = """
    CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL, date TEXT NOT NULL,
        slug TEXT NOT NULL, name TEXT, category TEXT, gecko_id TEXT,
        fee_accel REAL, rev_accel REAL, retention REAL,
        pf REAL, rel_pf REAL, price_30d REAL, price_vs_cat REAL, gap REAL,
        p0 REAL, p7 REAL, p30 REAL);
    CREATE INDEX IF NOT EXISTS idx_sig_slug ON signals (slug, date);
"""
NEW_COLUMNS = (("momentum_7d", "REAL"), ("mcap", "REAL"),
               ("eth0", "REAL"), ("eth7", "REAL"), ("eth30", "REAL"))


# ---------------------------------------------------------------------------
# Config and database
# ---------------------------------------------------------------------------

def load_config() -> None:
    CFG["db_path"] = env_str("HEAT_DB", "/opt/radar/heat.db")

    # Hard floors (engineering filters, not evidence-based)
    CFG["min_mcap"] = env_float("HEAT_MIN_MCAP", 25000000)       # $25M
    CFG["min_fees_30d"] = env_float("HEAT_MIN_FEES_30D", 100000)  # $100K
    CFG["min_retention"] = env_float("HEAT_MIN_RETENTION", 0.05)  # revenue/fees >= 5%

    # Candidate thresholds (starting guesses, to be tuned from the log)
    CFG["min_fee_accel"] = env_float("HEAT_MIN_FEE_ACCEL", 1.3)    # 30d fees >= 1.3x prior 30d
    CFG["min_momentum"] = env_float("HEAT_MIN_MOMENTUM_7D", 0.9)   # last week not collapsing
    CFG["min_gap"] = env_float("HEAT_MIN_GAP", 20.0)               # fundamentals ahead by 20+
    CFG["top_n"] = env_int("HEAT_TOP_N", 12)
    CFG["daily_hour_utc"] = env_int("HEAT_DAILY_HOUR_UTC", 7)


def db_connect() -> sqlite3.Connection:
    conn = open_db(CFG["db_path"], SCHEMA)
    columns = {row[1] for row in conn.execute("PRAGMA table_info(signals)")}
    with conn:
        for name, kind in NEW_COLUMNS:
            if name not in columns:
                conn.execute("ALTER TABLE signals ADD COLUMN {} {}".format(name, kind))
        if meta_get(conn, "schema") != "2":
            # v1 stored the market cap in p0 by mistake; prices are refilled from history
            conn.execute("UPDATE signals SET mcap = p0, p0 = NULL, p7 = NULL, p30 = NULL")
            meta_set(conn, "schema", "2")
    return conn


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

def fees_overview(data_type: str) -> Dict[str, dict]:
    """{slug: row} from /overview/fees; rows carry total7d, total30d, total60dto30d..."""
    url = "{}/overview/fees?excludeTotalDataChart=true&dataType={}".format(BASE, data_type)
    out = {}  # type: Dict[str, dict]
    for p in (http_json(url, timeout=120, user_agent=USER_AGENT) or {}).get("protocols") or []:
        key = p.get("slug") or p.get("name")
        if key:
            out[str(key)] = p
    return out


def chunks(items: List[str], size: int) -> List[List[str]]:
    return [items[i:i + size] for i in range(0, len(items), size)]


def price_changes_30d(gecko_ids: List[str]) -> Dict[str, float]:
    """{gecko_id: % price change over 30 days}."""
    out = {}  # type: Dict[str, float]
    for batch in chunks(sorted(set(gecko_ids)), PRICE_BATCH):
        url = "{}/percentage/{}?period=30d&lookForward=false".format(
            COINS, ",".join("coingecko:" + g for g in batch))
        try:
            coins = (http_json(url, timeout=60, user_agent=USER_AGENT) or {}).get("coins") or {}
        except Exception as e:
            log("Price change fetch failed: {}".format(e))
            continue
        for key, change in coins.items():
            out[key.split(":", 1)[1]] = fnum(change)
    return out


def prices_at(gecko_ids: List[str], ts_sec: Optional[int]) -> Dict[str, float]:
    """{gecko_id or 'ethereum': USD price} now (ts_sec=None) or at a past time."""
    keys = ["coingecko:" + g for g in gecko_ids] + [BASELINE]
    if ts_sec is None:
        url = "{}/prices/current/{}".format(COINS, ",".join(keys))
    else:
        url = "{}/prices/historical/{}/{}".format(COINS, ts_sec, ",".join(keys))
    coins = (http_json(url, timeout=60, user_agent=USER_AGENT) or {}).get("coins") or {}
    return {k.split(":", 1)[1]: fnum(v.get("price")) for k, v in coins.items()}


# ---------------------------------------------------------------------------
# Core analysis
# ---------------------------------------------------------------------------

def ratio(recent: float, prior: float) -> float:
    return recent / prior if prior > 0 else 0.0


def token_groups(meta: Dict[str, dict], fees: Dict[str, dict], revenue: Dict[str, dict],
                 counts: Dict[str, int]) -> List[dict]:
    """
    Sums fee and revenue windows per token. Protocols that share one token
    (e.g. Aave V2, V3, V4) are one row, because the token is what gets priced.
    """
    groups = {}  # type: Dict[str, dict]
    for slug, f in fees.items():
        m = meta.get(slug)
        if not m:
            continue
        counts["universe"] += 1
        category = m.get("category") or "Other"
        if category in SKIP_CATEGORIES:
            counts["skipped_type"] += 1
            continue
        r = revenue.get(slug) or {}
        # Versions of one protocol share the parent's token, which is usually
        # recorded on just one of them
        key = m.get("parentProtocol") or m.get("gecko_id") or "slug:" + slug
        g = groups.setdefault(key, {
            "slug": slug, "name": m.get("name") or slug, "category": category,
            "gecko_id": m.get("gecko_id"), "mcap": fnum(m.get("mcap")), "members": 0,
            "top_fees": -1.0, "f30": 0.0, "f60": 0.0, "f7": 0.0, "f14": 0.0, "r30": 0.0, "r60": 0.0})
        fees_30d = fnum(f.get("total30d"))
        if fees_30d > g["top_fees"]:
            # The biggest member names the group
            g.update(slug=slug, name=m.get("name") or slug, category=category, top_fees=fees_30d)
        g["members"] += 1
        g["mcap"] = max(g["mcap"], fnum(m.get("mcap")))
        g["gecko_id"] = g["gecko_id"] or m.get("gecko_id")
        for col, src, field in (("f30", f, "total30d"), ("f60", f, "total60dto30d"),
                                ("f7", f, "total7d"), ("f14", f, "total14dto7d"),
                                ("r30", r, "total30d"), ("r60", r, "total60dto30d")):
            g[col] += fnum(src.get(field))

    rows = []  # type: List[dict]
    for g in groups.values():
        if g["f30"] < CFG["min_fees_30d"] or g["mcap"] < CFG["min_mcap"]:
            counts["below_floor"] += 1
            continue
        # Revenue / fees: low means most value is paid out to LPs or given back
        # as incentives, a proxy for "is this real protocol income"
        retention = g["r30"] / g["f30"]
        if retention < CFG["min_retention"]:
            counts["wash_flag"] += 1
            continue
        name = g["name"]
        if g["members"] > 1:
            name += " (+{} more)".format(g["members"] - 1)
        rows.append({
            "slug": g["slug"], "name": name, "category": g["category"],
            "gecko_id": g["gecko_id"], "mcap": g["mcap"],
            "fees_30d": g["f30"], "rev_30d": g["r30"], "retention": retention,
            "fee_accel": ratio(g["f30"], g["f60"]),
            "momentum_7d": ratio(g["f7"], g["f14"]),
            "rev_accel": ratio(g["r30"], g["r60"]),
            "pf": g["mcap"] / (g["f30"] * 365.0 / 30.0),
        })
    return rows


def build_candidates() -> Tuple[List[dict], Dict[str, int]]:
    log("Fetching protocol list and fee / revenue overviews")
    meta = {p["slug"]: p for p in http_json("{}/protocols".format(BASE), timeout=120,
                                            user_agent=USER_AGENT) if p.get("slug")}
    counts = {"universe": 0, "skipped_type": 0, "below_floor": 0,
              "wash_flag": 0, "candidate": 0}
    rows = token_groups(meta, fees_overview("dailyFees"), fees_overview("dailyRevenue"), counts)

    # Token price change, absolute and against the median of its category.
    # Categories with fewer than 3 priced tokens use the median of all tokens.
    changes = price_changes_30d([r["gecko_id"] for r in rows if r["gecko_id"]])
    by_cat_pf = defaultdict(list)  # type: Dict[str, List[float]]
    by_cat_price = defaultdict(list)  # type: Dict[str, List[float]]
    for row in rows:
        row["price_30d"] = changes.get(row["gecko_id"] or "")
        by_cat_pf[row["category"]].append(row["pf"])
        if row["price_30d"] is not None:
            by_cat_price[row["category"]].append(row["price_30d"])
    all_prices = [p for v in by_cat_price.values() for p in v]
    market_median = median(all_prices) if all_prices else 0.0
    med_pf = {c: median(v) for c, v in by_cat_pf.items() if len(v) >= 3}
    med_price = {c: median(v) for c, v in by_cat_price.items() if len(v) >= 3}

    candidates = []  # type: List[dict]
    for row in rows:
        cat = row["category"]
        row["rel_pf"] = row["pf"] / med_pf[cat] if med_pf.get(cat) else 0.0
        if row["price_30d"] is None:
            continue  # no token price: the gap cannot be measured
        row["price_vs_cat"] = row["price_30d"] - med_price.get(cat, market_median)
        row["gap"] = (row["fee_accel"] - 1.0) * 100.0 - row["price_vs_cat"]
        if (row["fee_accel"] >= CFG["min_fee_accel"] and row["momentum_7d"] >= CFG["min_momentum"]
                and row["gap"] >= CFG["min_gap"]):
            candidates.append(row)
            counts["candidate"] += 1

    # Rank: cheaper than peers first (low rel_pf), then stronger growth
    candidates.sort(key=lambda r: (r["rel_pf"] if r["rel_pf"] > 0 else 9.99, -r["fee_accel"]))
    return candidates[: CFG["top_n"]], counts


def log_signals(conn: sqlite3.Connection, candidates: List[dict]) -> None:
    date = today_str()
    with conn:
        for c in candidates:
            if conn.execute("SELECT 1 FROM signals WHERE slug = ? AND date = ?",
                            (c["slug"], date)).fetchone():
                continue
            conn.execute("""
                INSERT INTO signals (ts, date, slug, name, category, gecko_id,
                    fee_accel, rev_accel, retention, pf, rel_pf, price_30d, price_vs_cat,
                    gap, momentum_7d, mcap)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (now_ms(), date, c["slug"], c["name"], c["category"], c["gecko_id"],
                  c["fee_accel"], c["rev_accel"], c["retention"], c["pf"], c["rel_pf"],
                  c["price_30d"], c["price_vs_cat"], c["gap"], c["momentum_7d"], c["mcap"]))


def format_report(candidates: List[dict], counts: Dict[str, int]) -> str:
    lines = ["[HEAT ANALYSIS] {}".format(today_str()),
             "Fees growing faster than the token price moved (vs category median).",
             "Universe {} | candidates {} (type-skip {}, below-floor {}, wash-flag {})".format(
                 counts["universe"], counts["candidate"], counts["skipped_type"],
                 counts["below_floor"], counts["wash_flag"]),
             ""]
    if not candidates:
        lines.append("No candidates passed the filters today.")
        return "\n".join(lines)
    for c in candidates:
        relpf = "{:.2f}x cat".format(c["rel_pf"]) if c["rel_pf"] > 0 else "n/a"
        lines.append(
            "{name} [{cat}]\n"
            "  fees30d {fees} ({growth} vs prior 30d, 7d {mom:.2f}x) | rev/fees {ret:.0%}\n"
            "  price 30d {price} ({vs} vs category) | P/F {pf:.1f} ({relpf}) | gap {gap:+.0f}".format(
                name=c["name"], cat=c["category"], fees=usd(c["fees_30d"]),
                growth=pct((c["fee_accel"] - 1) * 100), mom=c["momentum_7d"], ret=c["retention"],
                price=pct(c["price_30d"]), vs=pct(c["price_vs_cat"]), pf=c["pf"],
                relpf=relpf, gap=c["gap"]))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Validation: prices at signal time and 7 / 30 days later
# ---------------------------------------------------------------------------

def update_forward_prices(conn: sqlite3.Connection) -> None:
    now = now_ms()
    jobs = (("p0", "eth0", 0), ("p7", "eth7", 7), ("p30", "eth30", 30))
    for pcol, ecol, days in jobs:
        rows = conn.execute(
            "SELECT id, gecko_id, ts FROM signals WHERE {} IS NULL AND gecko_id IS NOT NULL"
            " AND ts <= ?".format(pcol), (now - days * DAY_MS,)).fetchall()
        for sid, gid, ts in rows:
            # Exact price at signal time + N days (historical endpoint)
            at = int((ts + days * DAY_MS) / 1000)
            try:
                prices = prices_at([gid], at if at < time.time() - 600 else None)
            except Exception as e:
                log("Price fetch failed for {}: {}".format(gid, e))
                continue
            if prices.get(gid) and prices.get("ethereum"):
                with conn:
                    conn.execute("UPDATE signals SET {} = ?, {} = ? WHERE id = ?".format(
                        pcol, ecol), (prices[gid], prices["ethereum"], sid))
            time.sleep(0.3)


def report_validation(conn: sqlite3.Connection) -> None:
    print("Heat Analyzer validation")
    print("Return = token price change from signal time. ETH = same period, as baseline.")
    for days, pcol, ecol in ((7, "p7", "eth7"), (30, "p30", "eth30")):
        rows = conn.execute(
            "SELECT p0, {}, eth0, {} FROM signals WHERE p0 > 0 AND {} > 0 AND eth0 > 0"
            " AND {} > 0".format(pcol, ecol, pcol, ecol)).fetchall()
        if not rows:
            print("{}d: no completed signals yet".format(days))
            continue
        rets = [(p1 / p0 - 1.0) * 100.0 for p0, p1, _, _ in rows]
        base = [(e1 / e0 - 1.0) * 100.0 for _, _, e0, e1 in rows]
        n = len(rets)
        avg = sum(rets) / n
        avg_base = sum(base) / n
        print("{}d: signals={} | avg return={} | ETH={} | excess={} | win rate={:.0f}%".format(
            days, n, pct(avg), pct(avg_base), pct(avg - avg_base),
            100.0 * sum(1 for r in rets if r > 0) / n))


def export_csv(conn: sqlite3.Connection, path: str) -> None:
    rows = conn.execute(
        "SELECT date, slug, name, category, fee_accel, momentum_7d, retention, pf, rel_pf,"
        " price_30d, gap, p0, p7, p30 FROM signals ORDER BY id").fetchall()
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["date", "slug", "name", "category", "fee_accel", "momentum_7d", "retention",
                    "pf", "rel_pf", "price_30d", "gap", "p0", "p7", "p30", "label", "outcome"])
        for r in rows:
            w.writerow(list(r) + ["", ""])
    log("Exported {} signals to {}".format(len(rows), path))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_once(conn: sqlite3.Connection) -> None:
    try:
        candidates, counts = build_candidates()
    except Exception as e:
        log("Analysis failed: {}".format(e))
        return
    log_signals(conn, candidates)
    with conn:
        outbox_add(conn, format_report(candidates, counts))
    log("Report queued: {} candidates".format(len(candidates)))


def tick(conn: sqlite3.Connection, telegram: Telegram) -> None:
    if daily_due(conn, "last_daily", CFG["daily_hour_utc"]):
        run_once(conn)
    try:
        update_forward_prices(conn)
    except Exception as e:
        log("Forward-price update failed: {}".format(e))
    outbox_flush(conn, telegram)
    heartbeat(conn)


def main() -> None:
    ap = argparse.ArgumentParser(description="Heat Analyzer v2")
    ap.add_argument("--env", default="", help="path to env file with tokens")
    ap.add_argument("--once", action="store_true", help="one report now")
    ap.add_argument("--report", action="store_true", help="validation statistics")
    ap.add_argument("--export", default="", help="export signals to this CSV path")
    ap.add_argument("--test-telegram", action="store_true", help="send a test message")
    args = ap.parse_args()

    load_env_file(args.env)
    load_config()
    telegram = Telegram.from_env(USER_AGENT)

    if args.test_telegram:
        sys.exit(0 if telegram.send("Heat Analyzer test message. Telegram is working.") else 1)

    conn = db_connect()
    if args.export:
        export_csv(conn, args.export)
        return
    if args.report:
        report_validation(conn)
        return
    if args.once:
        run_once(conn)
        update_forward_prices(conn)
        outbox_flush(conn, telegram)
        return

    log("Running forever; one report per day from hour {} UTC".format(CFG["daily_hour_utc"]))
    while True:
        try:
            tick(conn, telegram)
        except Exception as e:
            log("Unexpected error: {}".format(e))
        time.sleep(1800)


if __name__ == "__main__":
    main()
