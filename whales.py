#!/usr/bin/env python3
"""
Whale Watch v2 (Hyperliquid)

What it does:
  1. Once a day, builds two address lists from the public Hyperliquid leaderboard:
       - "whale": the largest accounts by value, WITHOUT market makers
         (accounts that trade far more than their size every month)
       - "smart": traders that pass quality filters on their real trade history
         (min closed trades, profit factor, profitable months, not a bot)
  2. Every few minutes, reads open positions of all tracked addresses and alerts on:
       - BIG TRADE: a new or increased position above a USD threshold
       - ACCUMULATION: a position that grew a lot over two days without one big trade
       - NEAR LIQUIDATION / LIQUIDATED: large positions at their liquidation price
  3. Every BIG TRADE is checked before it is sent:
       - the trader's PnL (all-time, 30d, 7d), the open PnL of the position and
         the trader's record on this coin
       - signs that the trade is NOT a directional bet: market making, a short
         covered by spot holdings, a market-neutral book, a funding-rate farm
     and gets a signal quality (HIGH / MEDIUM / LOW). LOW trades are logged, not sent.
  4. CONSENSUS: several smart traders open fresh positions on the same coin and side.
  5. Paper-trade log: every big trade and consensus signal is priced again after
     24h and 72h, next to BTC as a baseline. --report shows whether they were useful.

Standard library only. Compatible with Python 3.8+.

Usage:
  python3 whales.py --env /opt/radar/radar.env                  # run forever
  python3 whales.py --env /opt/radar/radar.env --once           # one cycle, then exit
  python3 whales.py --env /opt/radar/radar.env --refresh        # rebuild trader lists now
  python3 whales.py --env /opt/radar/radar.env --list           # show smart traders and scores
  python3 whales.py --env /opt/radar/radar.env --trader 0x...   # check one address
  python3 whales.py --env /opt/radar/radar.env --report         # paper-trade statistics
  python3 whales.py --env /opt/radar/radar.env --test-telegram
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from common import (
    DAY_MS, HOUR_MS, Telegram, daily_due, env_float, env_int, env_str, fnum,
    heartbeat, http_json, load_env_file, log, meta_get, meta_set, now_ms,
    open_db, outbox_add, outbox_flush, pct, px, record_signal, short_addr, usd,
)

USER_AGENT = "whale-watch/2.0 (personal research script)"
INFO_URL = "https://api.hyperliquid.xyz/info"
LEADERBOARD_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
EXPLORER_URL = "https://app.hyperliquid.xyz/explorer/address/"

QUALITY_ORDER = {"LOW": 0, "MEDIUM": 1, "HIGH": 2}
PROFILE_CACHE_MS = 30 * 60 * 1000

CFG = {}  # type: Dict[str, Any]

SCHEMA = """
    CREATE TABLE IF NOT EXISTS traders (
        addr TEXT PRIMARY KEY,
        kind TEXT NOT NULL,
        score TEXT,
        scanned INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE IF NOT EXISTS positions (
        addr TEXT NOT NULL, coin TEXT NOT NULL,
        szi REAL, entry REAL, value REAL, liq REAL, lev REAL,
        warned INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (addr, coin));
    CREATE TABLE IF NOT EXISTS opens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL, addr TEXT NOT NULL, coin TEXT NOT NULL,
        side TEXT NOT NULL, notional REAL NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_opens_ts ON opens (ts);
    CREATE TABLE IF NOT EXISTS consensus_alerts (
        coin TEXT NOT NULL, side TEXT NOT NULL, ts INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS pos_snapshots (
        addr TEXT NOT NULL, coin TEXT NOT NULL, ts INTEGER NOT NULL,
        side TEXT NOT NULL, value REAL NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_snap ON pos_snapshots (addr, coin, ts);
    CREATE TABLE IF NOT EXISTS acc_alerts (
        addr TEXT NOT NULL, coin TEXT NOT NULL, side TEXT NOT NULL,
        ts INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL, coin TEXT NOT NULL, side TEXT NOT NULL,
        traders INTEGER, ratio REAL,
        p0 REAL, btc0 REAL, p24 REAL, btc24 REAL, p72 REAL, btc72 REAL);
    CREATE TABLE IF NOT EXISTS trade_alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts INTEGER NOT NULL, addr TEXT NOT NULL, kind TEXT NOT NULL,
        coin TEXT NOT NULL, side TEXT NOT NULL, added_usd REAL NOT NULL,
        quality TEXT NOT NULL, sent INTEGER NOT NULL,
        p0 REAL, btc0 REAL, p24 REAL, btc24 REAL, p72 REAL, btc72 REAL);
    CREATE INDEX IF NOT EXISTS idx_trade_alerts_addr ON trade_alerts (addr, ts);
"""


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config() -> None:
    CFG["db_path"] = env_str("HL_DB", "/opt/radar/whales.db")
    CFG["extra_file"] = env_str("HL_EXTRA_ADDRESSES_FILE", "/opt/radar/extra_whales.txt")

    # Alert thresholds
    CFG["big_trade_usd"] = env_float("HL_BIG_TRADE_USD", 1000000)
    CFG["liq_min_usd"] = env_float("HL_LIQ_MIN_USD", 500000)
    CFG["liq_warn_pct"] = env_float("HL_LIQ_WARN_PCT", 2.0)
    # Big trades below this quality are logged but not sent: LOW, MEDIUM or HIGH
    CFG["min_quality"] = env_str("HL_MIN_QUALITY", "MEDIUM").upper()
    if CFG["min_quality"] not in QUALITY_ORDER:
        CFG["min_quality"] = "MEDIUM"

    # Trader lists
    CFG["whale_count"] = env_int("HL_WHALE_COUNT", 150)
    # Monthly volume / account value above this = market maker, not a directional whale
    CFG["mm_max_turnover"] = env_float("HL_MM_MAX_TURNOVER", 50)
    # Accounts with a public leaderboard name, tracked so their big moves and
    # liquidations are reported even when they are not the very largest
    CFG["named_max"] = env_int("HL_NAMED_MAX", 60)
    CFG["named_min_account"] = env_float("HL_NAMED_MIN_ACCOUNT_USD", 250000)
    CFG["smart_candidates"] = env_int("HL_SMART_CANDIDATES", 200)
    CFG["smart_min_account"] = env_float("HL_SMART_MIN_ACCOUNT_USD", 100000)
    CFG["smart_max"] = env_int("HL_SMART_MAX", 30)
    CFG["smart_min_trades"] = env_int("HL_SMART_MIN_TRADES", 50)
    CFG["smart_min_pf"] = env_float("HL_SMART_MIN_PF", 1.5)
    CFG["smart_min_good_months"] = env_int("HL_SMART_MIN_GOOD_MONTHS", 3)
    CFG["smart_max_fills_per_day"] = env_float("HL_SMART_MAX_FILLS_PER_DAY", 150)

    # Consensus
    CFG["cons_min_traders"] = env_int("HL_CONSENSUS_MIN_TRADERS", 5)
    CFG["cons_min_ratio"] = env_float("HL_CONSENSUS_MIN_RATIO", 0.7)
    CFG["fresh_increase"] = env_float("HL_FRESH_INCREASE", 1.5)

    # Slow accumulation (position grown quietly over a window)
    CFG["acc_window_hours"] = env_float("HL_ACC_WINDOW_HOURS", 48)
    CFG["acc_min_growth_usd"] = env_float("HL_ACC_MIN_GROWTH_USD", 750000)
    CFG["acc_min_ratio"] = env_float("HL_ACC_MIN_RATIO", 2.0)
    CFG["acc_cooldown_hours"] = env_float("HL_ACC_COOLDOWN_HOURS", 48)

    # Timing and load
    CFG["scan_seconds"] = env_int("HL_SCAN_SECONDS", 120)
    CFG["refresh_hours"] = env_float("HL_REFRESH_HOURS", 24)
    CFG["daily_hour_utc"] = env_int("HL_DAILY_SUMMARY_HOUR_UTC", 6)
    CFG["workers"] = max(1, env_int("HL_WORKERS", 4))


def side_of(szi: float) -> str:
    return "LONG" if szi > 0 else "SHORT"


# ---------------------------------------------------------------------------
# Hyperliquid API
# ---------------------------------------------------------------------------

def info(body: dict, timeout: int = 60) -> Any:
    return http_json(INFO_URL, body=body, timeout=timeout, user_agent=USER_AGENT)


def get_state(addr: str) -> Tuple[Dict[str, dict], float]:
    """Open perp positions {coin: position} and the perp account value."""
    st = info({"type": "clearinghouseState", "user": addr}) or {}
    out = {}  # type: Dict[str, dict]
    for ap in st.get("assetPositions") or []:
        p = ap.get("position") or {}
        coin = p.get("coin")
        szi = fnum(p.get("szi"))
        if not coin or szi == 0:
            continue
        out[coin] = {
            "szi": szi,
            "entry": fnum(p.get("entryPx")),
            "value": abs(fnum(p.get("positionValue"))),
            "liq": fnum(p.get("liquidationPx")),
            "lev": fnum((p.get("leverage") or {}).get("value")),
            "upnl": fnum(p.get("unrealizedPnl")),
            "roe": fnum(p.get("returnOnEquity")),
            # Hyperliquid reports funding paid; negative means received
            "funding": fnum((p.get("cumFunding") or {}).get("sinceOpen")),
            "warned": 0,
        }
    account = fnum((st.get("marginSummary") or {}).get("accountValue"))
    return out, account


class Market:
    """Prices for this cycle, plus funding rates loaded only when needed."""

    def __init__(self) -> None:
        self.mids = {k: fnum(v) for k, v in (info({"type": "allMids"}) or {}).items()}
        self._funding = None  # type: Optional[Dict[str, float]]

    def price(self, coin: str) -> float:
        return self.mids.get(coin, 0.0)

    def funding(self, coin: str) -> float:
        """Hourly funding rate; positive = longs pay shorts."""
        if self._funding is None:
            self._funding = {}
            try:
                meta, ctxs = info({"type": "metaAndAssetCtxs"})
                for asset, ctx in zip(meta.get("universe") or [], ctxs or []):
                    self._funding[asset.get("name")] = fnum((ctx or {}).get("funding"))
            except Exception as e:
                log("Funding rates not loaded: {}".format(e))
        return self._funding.get(coin, 0.0)


# ---------------------------------------------------------------------------
# Trader profile and signal quality
# ---------------------------------------------------------------------------

def parse_portfolio(data: Any) -> Dict[str, Dict[str, float]]:
    """{window: {pnl, vlm, value}} from the portfolio endpoint."""
    out = {}  # type: Dict[str, Dict[str, float]]
    for item in data or []:
        if not isinstance(item, list) or len(item) != 2:
            continue
        name, d = item
        pnl_hist = (d or {}).get("pnlHistory") or []
        value_hist = (d or {}).get("accountValueHistory") or []
        out[name] = {
            "pnl": fnum(pnl_hist[-1][1]) if pnl_hist else 0.0,
            "vlm": fnum((d or {}).get("vlm")),
            "value": fnum(value_hist[-1][1]) if value_hist else 0.0,
        }
    return out


def fill_stats(fills: List[dict], coin: str) -> Dict[str, Any]:
    """Closed-trade record overall and on one coin, from recent fills."""
    stats = {"trades": 0, "wins": 0, "net": 0.0, "gross_win": 0.0, "gross_loss": 0.0,
             "coin_trades": 0, "coin_wins": 0, "coin_net": 0.0,
             "fills": len(fills), "maker_share": 0.0, "fills_per_day": 0.0, "span_days": 0.0}
    if not fills:
        return stats
    maker = 0
    times = []
    for f in fills:
        times.append(int(fnum(f.get("time"))))
        if f.get("crossed") is False:
            maker += 1
        closed = fnum(f.get("closedPnl"))
        if closed == 0:
            continue
        net = closed - fnum(f.get("fee"))
        stats["trades"] += 1
        stats["net"] += net
        if net > 0:
            stats["wins"] += 1
            stats["gross_win"] += net
        else:
            stats["gross_loss"] -= net
        if f.get("coin") == coin:
            stats["coin_trades"] += 1
            stats["coin_net"] += net
            if net > 0:
                stats["coin_wins"] += 1
    span_days = max((max(times) - min(times)) / float(DAY_MS), 1.0)
    stats["maker_share"] = maker / float(len(fills))
    stats["fills_per_day"] = len(fills) / span_days
    stats["span_days"] = span_days
    return stats


def spot_holding(spot: Any, coin: str) -> float:
    """Amount of the coin (or its wrapped U-version, e.g. UBTC) held on Hyperliquid spot."""
    names = {coin.upper(), "U" + coin.upper()}
    total = 0.0
    for b in (spot or {}).get("balances") or []:
        if str(b.get("coin", "")).upper() in names:
            total += fnum(b.get("total"))
    return total


class Profiles:
    """Trader profiles, fetched only for traders that trigger an alert, cached 30 min."""

    def __init__(self) -> None:
        self.cache = {}  # type: Dict[str, Tuple[int, dict]]

    def get(self, addr: str) -> dict:
        hit = self.cache.get(addr)
        if hit and now_ms() - hit[0] < PROFILE_CACHE_MS:
            return hit[1]
        profile = {"portfolio": {}, "fills": [], "spot": {}}  # type: Dict[str, Any]
        for key, body in (("portfolio", {"type": "portfolio", "user": addr}),
                          ("fills", {"type": "userFills", "user": addr}),
                          ("spot", {"type": "spotClearinghouseState", "user": addr})):
            try:
                profile[key] = info(body)
            except Exception as e:
                log("Profile {} failed for {}: {}".format(key, short_addr(addr), e))
        profile["portfolio"] = parse_portfolio(profile["portfolio"])
        self.cache[addr] = (now_ms(), profile)
        return profile


def assess(coin: str, pos: dict, positions: Dict[str, dict], profile: dict,
           market: Market) -> Dict[str, Any]:
    """
    Decides how much a big trade looks like a real directional bet by a good trader.
    Returns {quality, good: [...], bad: [...], numbers used in the message}.
    """
    side = side_of(pos["szi"])
    mark = market.price(coin) or (pos["value"] / abs(pos["szi"]))
    port = profile.get("portfolio") or {}
    all_time = port.get("allTime") or {}
    month = port.get("month") or {}
    week = port.get("week") or {}
    account = all_time.get("value") or 0.0
    stats = fill_stats(profile.get("fills") or [], coin)

    good = []  # type: List[str]
    bad = []  # type: List[str]
    points = 0
    hard_no = False  # a reason that makes the trade non-directional

    # 1. Is this trader any good?
    if all_time.get("pnl", 0) > 0:
        points += 1
        good.append("profitable all-time")
    elif all_time:
        points -= 1
        bad.append("losing trader all-time")
    if month.get("pnl", 0) > 0:
        points += 1
        good.append("profitable last 30d")
    elif month:
        bad.append("losing last 30d")
    # Only meaningful when the fills cover some history (a busy bot fills 2000 in hours)
    if stats["coin_trades"] >= 5 and stats["span_days"] >= 7:
        win_rate = stats["coin_wins"] / float(stats["coin_trades"])
        if stats["coin_net"] > 0 and win_rate >= 0.5:
            points += 1
            good.append("wins on {} ({:.0%} of {} closes)".format(coin, win_rate, stats["coin_trades"]))
        elif stats["coin_net"] < 0:
            points -= 1
            bad.append("loses on {}".format(coin))

    # 2. Is it a directional bet at all?
    turnover = month.get("vlm", 0) / account if account > 0 else 0.0
    if turnover > CFG["mm_max_turnover"]:
        hard_no = True
        bad.append("market maker (30d volume {:.0f}x account)".format(turnover))
    elif stats["maker_share"] > 0.9 and stats["fills_per_day"] > 200:
        hard_no = True
        bad.append("bot / market maker ({:.0%} maker orders, {:.0f} fills/day)".format(
            stats["maker_share"], stats["fills_per_day"]))

    hedge_usd = spot_holding(profile.get("spot"), coin) * mark
    if side == "SHORT" and hedge_usd >= 0.5 * pos["value"]:
        hard_no = True
        bad.append("hedged: holds {} of {} spot".format(usd(hedge_usd), coin))

    longs = sum(p["value"] for p in positions.values() if p["szi"] > 0)
    shorts = sum(p["value"] for p in positions.values() if p["szi"] < 0)
    if len(positions) >= 4 and longs + shorts > 0:
        net_share = abs(longs - shorts) / (longs + shorts)
        if net_share < 0.25:
            points -= 2
            bad.append("market-neutral book (longs {} vs shorts {})".format(usd(longs), usd(shorts)))

    rate = market.funding(coin)
    earns = (side == "SHORT" and rate > 0) or (side == "LONG" and rate < 0)
    apr = abs(rate) * 24 * 365 * 100
    if earns and apr >= 10 and 0 < pos["lev"] <= 3:
        points -= 1
        bad.append("possible funding farm (earns {:.0f}% APR at {:.0f}x)".format(apr, pos["lev"]))

    # 3. Conviction: how big is this bet for this trader?
    share = pos["value"] / account if account > 0 else 0.0
    if share >= 0.5:
        points += 1
        good.append("big bet ({:.0%} of account)".format(share))
    elif 0 < share < 0.1:
        points -= 1
        bad.append("small vs account ({:.0%})".format(share))

    if hard_no or points <= 0:
        quality = "LOW"
    elif points >= 3 and not bad:
        quality = "HIGH"
    else:
        quality = "MEDIUM"
    return {"quality": quality, "good": good, "bad": bad, "stats": stats,
            "account": account, "share": share, "turnover": turnover,
            "pnl_all": all_time.get("pnl"), "pnl_month": month.get("pnl"),
            "pnl_week": week.get("pnl"), "funding_apr": apr if earns else -apr}


def past_alert_record(conn: sqlite3.Connection, addr: str) -> str:
    """How this trader's earlier big trades did 24h later."""
    rows = conn.execute(
        "SELECT side, p0, p24 FROM trade_alerts "
        "WHERE addr = ? AND p24 IS NOT NULL AND p0 > 0", (addr,)).fetchall()
    if not rows:
        return "no earlier alerts to judge yet"
    rets = [(1.0 if side == "LONG" else -1.0) * (p24 / p0 - 1.0) * 100.0 for side, p0, p24 in rows]
    right = sum(1 for r in rets if r > 0)
    return "{} judged | right after 24h: {} ({:.0%}) | avg {}".format(
        len(rets), right, right / float(len(rets)), pct(sum(rets) / len(rets)))


def trader_lines(conn: sqlite3.Connection, addr: str, coin: str, pos: dict,
                 a: Dict[str, Any]) -> List[str]:
    """The PnL and quality part of an alert."""
    stats = a["stats"]
    funding = pos.get("funding", 0.0)
    lines = [
        "Position PnL: {} (ROE {}) | funding since open: {} {}".format(
            usd(pos.get("upnl", 0.0), signed=True), pct(pos.get("roe", 0.0) * 100),
            usd(abs(funding)), "received" if funding < 0 else "paid"),
    ]
    if a["pnl_all"] is not None:
        lines.append("Trader PnL: all-time {} | 30d {} | 7d {}".format(
            usd(a["pnl_all"], signed=True), usd(a["pnl_month"] or 0, signed=True),
            usd(a["pnl_week"] or 0, signed=True)))
    if stats["coin_trades"] >= 3 and stats["span_days"] >= 7:
        lines.append("{} record: {} closes, {:.0%} wins, net {}".format(
            coin, stats["coin_trades"], stats["coin_wins"] / float(stats["coin_trades"]),
            usd(stats["coin_net"], signed=True)))
    lines.append("Past radar alerts: " + past_alert_record(conn, addr))
    if a["good"]:
        lines.append("+ " + "; ".join(a["good"]))
    if a["bad"]:
        lines.append("- " + "; ".join(a["bad"]))
    return lines


# ---------------------------------------------------------------------------
# Trader lists
# ---------------------------------------------------------------------------

def window_perf(row: dict, name: str) -> Dict[str, float]:
    for item in row.get("windowPerformances") or []:
        if isinstance(item, list) and len(item) == 2 and item[0] == name:
            d = item[1] or {}
            return {"pnl": fnum(d.get("pnl")), "vlm": fnum(d.get("vlm")), "roi": fnum(d.get("roi"))}
    return {"pnl": 0.0, "vlm": 0.0, "roi": 0.0}


def score_trader(addr: str) -> Optional[dict]:
    """
    Scores a trader from their most recent fills (the API returns up to ~2000).
    Each fill with a non-zero closedPnl counts as one closed trade. Partial closes
    count separately, so this is an approximation of the real trade count.
    """
    fills = info({"type": "userFills", "user": addr})
    if not isinstance(fills, list) or not fills:
        return None
    stats = fill_stats(fills, "")
    monthly = defaultdict(float)  # type: Dict[str, float]
    for f in fills:
        closed = fnum(f.get("closedPnl"))
        if closed:
            month = datetime.fromtimestamp(fnum(f.get("time")) / 1000.0, timezone.utc).strftime("%Y-%m")
            monthly[month] += closed - fnum(f.get("fee"))
    if stats["gross_loss"] > 0:
        pf = stats["gross_win"] / stats["gross_loss"]
    else:
        pf = 99.0 if stats["gross_win"] > 0 else 0.0
    last_months = sorted(monthly)[-4:]
    return {
        "trades": stats["trades"],
        "pf": round(pf, 2),
        "good_months": sum(1 for m in last_months if monthly[m] > 0),
        "months_seen": len(last_months),
        "fills_per_day": round(stats["fills_per_day"], 1),
        "net_pnl": round(stats["net"]),
    }


def passes_filters(s: dict) -> bool:
    return (s["trades"] >= CFG["smart_min_trades"]
            and s["pf"] >= CFG["smart_min_pf"]
            and s["good_months"] >= CFG["smart_min_good_months"]
            and s["fills_per_day"] <= CFG["smart_max_fills_per_day"])


def read_extra_addresses() -> List[str]:
    path = CFG["extra_file"]
    out = []  # type: List[str]
    if not os.path.exists(path):
        return out
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            a = line.split("#", 1)[0].strip().lower()
            if a.startswith("0x") and len(a) == 42:
                out.append(a)
    return out


def refresh_traders(conn: sqlite3.Connection) -> None:
    log("Refreshing trader lists from leaderboard")
    data = http_json(LEADERBOARD_URL, timeout=180, user_agent=USER_AGENT)
    rows = data.get("leaderboardRows") if isinstance(data, dict) else data
    if not isinstance(rows, list) or not rows:
        raise RuntimeError("Leaderboard response has no rows")

    parsed = []  # type: List[dict]
    for r in rows:
        addr = (r.get("ethAddress") or "").lower()
        if not addr.startswith("0x"):
            continue
        value = fnum(r.get("accountValue"))
        month = window_perf(r, "month")
        all_time = window_perf(r, "allTime")
        parsed.append({"addr": addr, "value": value, "month": month, "all": all_time,
                       "name": (r.get("displayName") or "").strip(),
                       "turnover": month["vlm"] / value if value > 0 else 0.0})
    del data, rows
    log("Leaderboard rows parsed: {}".format(len(parsed)))

    new_kinds = {}  # type: Dict[str, Tuple[str, str]]

    # Whales: largest accounts that are not market makers
    market_makers = 0
    for p in sorted(parsed, key=lambda p: p["value"], reverse=True):
        if len(new_kinds) >= CFG["whale_count"]:
            break
        if p["turnover"] > CFG["mm_max_turnover"]:
            market_makers += 1
            continue
        new_kinds[p["addr"]] = ("whale", json.dumps({
            "account_value": round(p["value"]), "pnl_all": round(p["all"]["pnl"]),
            "pnl_month": round(p["month"]["pnl"]), "turnover": round(p["turnover"], 1),
            "name": p["name"]}))

    # Named traders: accounts with a public leaderboard name (known people and funds)
    named = 0
    for p in sorted(parsed, key=lambda p: p["value"], reverse=True):
        if named >= CFG["named_max"]:
            break
        if (not p["name"] or p["addr"] in new_kinds or p["value"] < CFG["named_min_account"]
                or p["turnover"] > CFG["mm_max_turnover"]):
            continue
        named += 1
        new_kinds[p["addr"]] = ("named", json.dumps({
            "account_value": round(p["value"]), "pnl_all": round(p["all"]["pnl"]),
            "pnl_month": round(p["month"]["pnl"]), "name": p["name"]}))

    candidates = [p for p in parsed
                  if p["value"] >= CFG["smart_min_account"] and p["month"]["pnl"] > 0
                  and p["all"]["pnl"] > 0 and p["turnover"] <= CFG["mm_max_turnover"]]
    candidates.sort(key=lambda p: p["month"]["pnl"], reverse=True)
    candidates = candidates[: CFG["smart_candidates"]]
    log("Scoring {} smart-trader candidates (about 1 per second)".format(len(candidates)))

    smart = []  # type: List[Tuple[str, dict]]
    for p in candidates:
        try:
            s = score_trader(p["addr"])
        except Exception as e:
            log("Scoring failed for {}: {}".format(short_addr(p["addr"]), e))
            s = None
        time.sleep(1.0)
        if s and passes_filters(s):
            s["account_value"] = round(p["value"])
            smart.append((p["addr"], s))
    smart.sort(key=lambda x: x[1]["pf"], reverse=True)
    smart = smart[: CFG["smart_max"]]
    for addr, s in smart:
        new_kinds[addr] = ("smart", json.dumps(s))

    for addr in read_extra_addresses():
        if addr not in new_kinds:
            new_kinds[addr] = ("manual", "{}")

    with conn:
        # Upsert, keeping the "scanned" flag of addresses already tracked
        for addr, (kind, score) in new_kinds.items():
            conn.execute("""
                INSERT INTO traders (addr, kind, score, scanned) VALUES (?, ?, ?, 0)
                ON CONFLICT(addr) DO UPDATE SET kind = excluded.kind, score = excluded.score
            """, (addr, kind, score))
        for (addr,) in conn.execute("SELECT addr FROM traders").fetchall():
            if addr not in new_kinds:
                conn.execute("DELETE FROM traders WHERE addr = ?", (addr,))
                conn.execute("DELETE FROM positions WHERE addr = ?", (addr,))
        meta_set(conn, "last_refresh_ms", now_ms())

        n_whale = sum(1 for k, _ in new_kinds.values() if k == "whale")
        n_manual = sum(1 for k, _ in new_kinds.values() if k == "manual")
        msg = ("Whale Watch: trader lists refreshed.\n"
               "Whales (largest accounts): {}\n"
               "Market makers left out: {}\n"
               "Smart traders (passed filters): {} of {} candidates\n"
               "Manual addresses: {}\n"
               "Named traders: {}").format(n_whale, market_makers, len(smart),
                                           len(candidates), n_manual, named)
        if len(smart) < CFG["cons_min_traders"]:
            msg += ("\nWarning: fewer smart traders than the consensus minimum ({}). "
                    "Consensus alerts cannot fire until filters are relaxed.").format(
                        CFG["cons_min_traders"])
        outbox_add(conn, msg)
    log(msg.replace("\n", " | "))


def refresh_due(conn: sqlite3.Connection) -> bool:
    last = meta_get(conn, "last_refresh_ms")
    return last is None or now_ms() - int(last) >= CFG["refresh_hours"] * HOUR_MS


# ---------------------------------------------------------------------------
# Position scanning
# ---------------------------------------------------------------------------

def load_prev(conn: sqlite3.Connection, addr: str) -> Dict[str, dict]:
    prev = {}  # type: Dict[str, dict]
    for coin, szi, entry, value, liq, lev, warned in conn.execute(
            "SELECT coin, szi, entry, value, liq, lev, warned FROM positions WHERE addr = ?",
            (addr,)):
        prev[coin] = {"szi": szi, "entry": entry, "value": value,
                      "liq": liq, "lev": lev, "warned": warned}
    return prev


def save_positions(conn: sqlite3.Connection, addr: str, cur: Dict[str, dict]) -> None:
    conn.execute("DELETE FROM positions WHERE addr = ?", (addr,))
    conn.executemany(
        "INSERT INTO positions (addr, coin, szi, entry, value, liq, lev, warned) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [(addr, coin, c["szi"], c["entry"], c["value"], c["liq"], c["lev"], c["warned"])
         for coin, c in cur.items()])


def liquidation_in_fills(addr: str, coin: str, since_ms: int) -> Optional[bool]:
    """True if a recent fill for this coin is marked as a liquidation. None if unknown."""
    try:
        fills = info({"type": "userFillsByTime", "user": addr,
                      "startTime": int(since_ms - 5 * 60 * 1000)})
    except Exception as e:
        log("Fill check failed for {}: {}".format(short_addr(addr), e))
        return None
    if not isinstance(fills, list):
        return None
    return any(f.get("coin") == coin and f.get("liquidation") for f in fills)


class Scanner:
    def __init__(self, conn: sqlite3.Connection, market: Market, profiles: Profiles) -> None:
        self.conn = conn
        self.market = market
        self.profiles = profiles

    def big_trade(self, addr: str, kind: str, coin: str, c: dict, action: str,
                  added: float, cur: Dict[str, dict]) -> None:
        side = side_of(c["szi"])
        mark = self.market.price(coin) or (c["value"] / abs(c["szi"]))
        a = assess(coin, c, cur, self.profiles.get(addr), self.market)
        send = QUALITY_ORDER[a["quality"]] >= QUALITY_ORDER[CFG["min_quality"]]
        # Every big trade is logged, sent or not, so --report can compare quality levels
        self.conn.execute(
            "INSERT INTO trade_alerts (ts, addr, kind, coin, side, added_usd, quality, sent, p0, btc0)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (now_ms(), addr, kind, coin, side, added, a["quality"], int(send), mark,
             self.market.price("BTC")))
        # LOW trades go to the ledger too, so the scorecard shows whether the filter is right
        record_signal("whales", "whale_trade_" + a["quality"].lower(),
                      "{} {} +{} by {} ({})".format(coin, side, usd(added), short_addr(addr), kind),
                      direction=1 if side == "LONG" else -1, symbol=coin,
                      strength={"HIGH": 1.5, "MEDIUM": 1.0, "LOW": 0.3}[a["quality"]],
                      link=EXPLORER_URL + addr)
        if not send:
            log("Big trade not sent ({}): {} {} {} by {} - {}".format(
                a["quality"], coin, side, usd(added), short_addr(addr), "; ".join(a["bad"])))
            return
        share = " ({:.0%} of account)".format(a["share"]) if a["share"] > 0 else ""
        lines = [
            "[BIG TRADE] {} {} +{} | Signal: {}".format(coin, side, usd(added), a["quality"]),
            "Trader: {} ({})".format(short_addr(addr), kind),
            "Action: {}".format(action),
            "Position now: {} at {:.0f}x{}".format(usd(c["value"]), c["lev"], share),
            "Entry: {} | Mark: {} | Liq: {}".format(
                px(c["entry"]), px(mark), px(c["liq"]) if c["liq"] > 0 else "none"),
        ]
        lines += trader_lines(self.conn, addr, coin, c, a)
        lines.append("Link: " + EXPLORER_URL + addr)
        outbox_add(self.conn, "\n".join(lines))

    def process_changes(self, addr: str, kind: str, prev: Dict[str, dict],
                        cur: Dict[str, dict], since_ms: int) -> int:
        alerts = 0
        who = "{} ({})".format(short_addr(addr), kind)
        link = EXPLORER_URL + addr
        ts = now_ms()

        # New, increased, or near-liquidation positions
        for coin, c in cur.items():
            p = prev.get(coin)
            side = side_of(c["szi"])
            mark = self.market.price(coin) or (c["value"] / abs(c["szi"]))
            same_side = p is not None and (p["szi"] > 0) == (c["szi"] > 0)

            if not same_side:
                added_value = c["value"]
                action = "NEW position"
                fresh = True
            else:
                added_value = max(0.0, abs(c["szi"]) - abs(p["szi"])) * mark
                action = "INCREASED position"
                fresh = abs(c["szi"]) >= CFG["fresh_increase"] * abs(p["szi"])

            if added_value > 0 and fresh:
                self.conn.execute(
                    "INSERT INTO opens (ts, addr, coin, side, notional) VALUES (?, ?, ?, ?, ?)",
                    (ts, addr, coin, side, added_value))

            if added_value >= CFG["big_trade_usd"]:
                self.big_trade(addr, kind, coin, c, action, added_value, cur)
                alerts += 1

            # Near-liquidation warning, sent once until the position moves away again
            c["warned"] = p["warned"] if same_side else 0
            if c["liq"] > 0 and mark > 0 and c["value"] >= CFG["liq_min_usd"]:
                dist = abs(mark - c["liq"]) / mark * 100.0
                if dist <= CFG["liq_warn_pct"] and not c["warned"]:
                    c["warned"] = 1
                    alerts += 1
                    outbox_add(self.conn, (
                        "[NEAR LIQUIDATION] {coin} {side} {total}\n"
                        "Trader: {who}\n"
                        "Mark: {mark} | Liq: {liq} ({dist:.1f}% away)\n"
                        "Leverage: {lev:.0f}x | Entry: {entry} | PnL: {upnl}\n"
                        "Link: {link}").format(
                            coin=coin, side=side, total=usd(c["value"]), who=who,
                            mark=px(mark), liq=px(c["liq"]), dist=dist, lev=c["lev"],
                            entry=px(c["entry"]), upnl=usd(c.get("upnl", 0.0), signed=True),
                            link=link))
                elif dist > 2 * CFG["liq_warn_pct"]:
                    c["warned"] = 0

        # Closed or flipped positions: check for liquidation
        for coin, p in prev.items():
            c = cur.get(coin)
            if c is not None and (c["szi"] > 0) == (p["szi"] > 0):
                continue
            if p["value"] < CFG["liq_min_usd"] or p["liq"] <= 0:
                continue
            mark = self.market.price(coin)
            side = side_of(p["szi"])
            if side == "LONG":
                price_hit = 0 < mark <= p["liq"] * 1.01
            else:
                price_hit = mark > 0 and mark >= p["liq"] * 0.99
            confirmed = liquidation_in_fills(addr, coin, since_ms)
            if confirmed or price_hit:
                alerts += 1
                outbox_add(self.conn, (
                    "[LIQUIDATED] {coin} {side} ~{total} ({how})\n"
                    "Trader: {who}\n"
                    "Entry: {entry} | Liq: {liq} | Mark now: {mark}\n"
                    "Leverage was: {lev:.0f}x\n"
                    "Link: {link}").format(
                        coin=coin, side=side, total=usd(p["value"]),
                        how="confirmed by fills" if confirmed else "likely: price reached liq level",
                        who=who, entry=px(p["entry"]), liq=px(p["liq"]),
                        mark=px(mark) if mark else "?", lev=p["lev"], link=link))
        return alerts

    def check_accumulation(self, addr: str, kind: str, cur: Dict[str, dict],
                           oldest: Dict[Tuple[str, str], Tuple[int, str, float]]) -> int:
        """
        Detects a position that grew a lot over the window WITHOUT any single big trade,
        i.e. quiet scaling-in (TWAP style). Compares current value to the oldest snapshot
        still inside the window.
        """
        alerts = 0
        cooldown = now_ms() - int(CFG["acc_cooldown_hours"] * HOUR_MS)
        for coin, c in cur.items():
            row = oldest.get((addr, coin))
            side = side_of(c["szi"])
            if not row or row[1] != side:
                continue  # no history, or flipped sides inside the window
            old_ts, _, old_value = row
            growth = c["value"] - old_value
            ratio = c["value"] / old_value if old_value > 0 else 0.0
            if growth < CFG["acc_min_growth_usd"] or ratio < CFG["acc_min_ratio"]:
                continue
            if self.conn.execute(
                    "SELECT 1 FROM acc_alerts WHERE addr = ? AND coin = ? AND side = ? AND ts >= ?",
                    (addr, coin, side, cooldown)).fetchone():
                continue
            self.conn.execute("INSERT INTO acc_alerts (addr, coin, side, ts) VALUES (?, ?, ?, ?)",
                              (addr, coin, side, now_ms()))
            a = assess(coin, c, cur, self.profiles.get(addr), self.market)
            mark = self.market.price(coin)
            lines = [
                "[ACCUMULATION] {} {} | Signal: {}".format(coin, side, a["quality"]),
                "Trader: {} ({})".format(short_addr(addr), kind),
                "Position grew: {} -> {} ({:.1f}x) over {:.0f}h".format(
                    usd(old_value), usd(c["value"]), ratio, (now_ms() - old_ts) / float(HOUR_MS)),
                "Added quietly, no single big trade",
                "Entry avg: {} | Mark: {} | {:.0f}x".format(
                    px(c["entry"]), px(mark) if mark else "?", c["lev"]),
            ]
            lines += trader_lines(self.conn, addr, coin, c, a)
            lines.append("Link: " + EXPLORER_URL + addr)
            outbox_add(self.conn, "\n".join(lines))
            record_signal("whales", "whale_accumulation",
                          "{} {} grew {} -> {} by {}".format(coin, side, usd(old_value), usd(c["value"]),
                                                             short_addr(addr)),
                          direction=1 if side == "LONG" else -1, symbol=coin,
                          strength=1.5 if a["quality"] == "HIGH" else 1.0, link=EXPLORER_URL + addr)
            alerts += 1
        return alerts


def trader_label(kind: str, score: Optional[str]) -> str:
    """'whale' or 'named: BobbyBigSize' for alert texts."""
    try:
        name = (json.loads(score or "{}") or {}).get("name") or ""
    except ValueError:
        name = ""
    return "{}: {}".format(kind, name) if name else kind


def scan(conn: sqlite3.Connection, market: Market, profiles: Profiles) -> None:
    started = now_ms()
    since_ms = int(meta_get(conn, "last_scan_ms") or (started - 10 * 60 * 1000))
    traders = [(addr, trader_label(kind, score), scanned) for addr, kind, score, scanned in
               conn.execute("SELECT addr, kind, score, scanned FROM traders").fetchall()]

    # Read all positions in parallel (network bound), then process in order
    def fetch(addr: str) -> Optional[Tuple[Dict[str, dict], float]]:
        try:
            return get_state(addr)
        except Exception as e:
            log("Position read failed for {}: {}".format(short_addr(addr), e))
            return None

    with ThreadPoolExecutor(max_workers=CFG["workers"]) as pool:
        states = list(pool.map(fetch, [t[0] for t in traders]))

    window_from = now_ms() - int(CFG["acc_window_hours"] * HOUR_MS)
    oldest = {(a, c): (int(ts), s, float(v)) for a, c, ts, s, v in conn.execute(
        "SELECT addr, coin, MIN(ts), side, value FROM pos_snapshots WHERE ts >= ? "
        "GROUP BY addr, coin", (window_from,))}
    last_snap = {(a, c): int(ts) for a, c, ts in conn.execute(
        "SELECT addr, coin, MAX(ts) FROM pos_snapshots GROUP BY addr, coin")}

    scanner = Scanner(conn, market, profiles)
    n_alerts = 0
    snap_gap = 2 * HOUR_MS  # one snapshot per position every ~2 hours keeps the table small
    for (addr, kind, scanned), state in zip(traders, states):
        if state is None:
            continue
        cur, _ = state
        with conn:
            if scanned:
                prev = load_prev(conn, addr)
                n_alerts += scanner.process_changes(addr, kind, prev, cur, since_ms)
                n_alerts += scanner.check_accumulation(addr, kind, cur, oldest)
            else:
                # First time this address is seen: store its state silently
                conn.execute("UPDATE traders SET scanned = 1 WHERE addr = ?", (addr,))
            ts = now_ms()
            conn.executemany(
                "INSERT INTO pos_snapshots (addr, coin, ts, side, value) VALUES (?, ?, ?, ?, ?)",
                [(addr, coin, ts, side_of(c["szi"]), c["value"]) for coin, c in cur.items()
                 if ts - last_snap.get((addr, coin), 0) >= snap_gap])
            save_positions(conn, addr, cur)

    keep_ms = int(2 * CFG["acc_window_hours"] * HOUR_MS)
    with conn:
        conn.execute("DELETE FROM pos_snapshots WHERE ts < ?", (now_ms() - keep_ms,))
        conn.execute("DELETE FROM acc_alerts WHERE ts < ?", (now_ms() - keep_ms,))
        meta_set(conn, "last_scan_ms", started)
    log("Scan done: {} traders, {} alerts, {:.0f}s".format(
        len(traders), n_alerts, (now_ms() - started) / 1000.0))


# ---------------------------------------------------------------------------
# Consensus and paper-trade log
# ---------------------------------------------------------------------------

def smart_opens_last_24h(conn: sqlite3.Connection) -> Dict[str, Dict[str, dict]]:
    agg = defaultdict(lambda: {"LONG": {"addrs": set(), "usd": 0.0},
                               "SHORT": {"addrs": set(), "usd": 0.0}})  # type: Dict[str, Dict[str, dict]]
    rows = conn.execute("""
        SELECT o.coin, o.side, o.addr, o.notional
        FROM opens o JOIN traders t ON o.addr = t.addr
        WHERE t.kind = 'smart' AND o.ts >= ?
    """, (now_ms() - DAY_MS,)).fetchall()
    for coin, side, addr, notional in rows:
        agg[coin][side]["addrs"].add(addr)
        agg[coin][side]["usd"] += notional
    return agg


def check_consensus(conn: sqlite3.Connection, market: Market) -> None:
    n_smart = conn.execute("SELECT COUNT(*) FROM traders WHERE kind = 'smart'").fetchone()[0]
    if n_smart == 0:
        return
    cooldown_from = now_ms() - DAY_MS
    for coin, sides in smart_opens_last_24h(conn).items():
        for side, other in (("LONG", "SHORT"), ("SHORT", "LONG")):
            n = len(sides[side]["addrs"])
            a = sides[side]["usd"]
            b = sides[other]["usd"]
            if n < CFG["cons_min_traders"] or a + b <= 0:
                continue
            ratio = (a - b) / (a + b)
            if ratio < CFG["cons_min_ratio"]:
                continue
            if conn.execute("SELECT 1 FROM consensus_alerts WHERE coin = ? AND side = ? AND ts >= ?",
                            (coin, side, cooldown_from)).fetchone():
                continue
            p0 = market.price(coin)
            with conn:
                conn.execute("INSERT INTO consensus_alerts (coin, side, ts) VALUES (?, ?, ?)",
                             (coin, side, now_ms()))
                conn.execute(
                    "INSERT INTO signals (ts, coin, side, traders, ratio, p0, btc0) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (now_ms(), coin, side, n, ratio, p0, market.price("BTC")))
                outbox_add(conn, (
                    "[WHALE CONSENSUS] {coin} - {side}\n"
                    "Smart traders: {n} of {total} opened new {sidel}s in last 24h\n"
                    "Net ratio: {ratio:.2f}  = ({a} - {b}) / ({a} + {b})\n"
                    "Current price: {p0}\n"
                    "Logged for paper-trade test (checked again at 24h and 72h).").format(
                        coin=coin, side=side, n=n, total=n_smart, sidel=side.lower(),
                        ratio=ratio, a=usd(a), b=usd(b), p0=px(p0) if p0 else "?"))
            record_signal("whales", "whale_consensus",
                          "{} of {} smart traders opened {} on {}".format(n, n_smart, side.lower(), coin),
                          direction=1 if side == "LONG" else -1, symbol=coin, strength=1.5)


def update_forward_prices(conn: sqlite3.Connection, market: Market) -> None:
    """Fills 24h and 72h prices for consensus signals and big-trade alerts."""
    btc = market.price("BTC")
    if btc <= 0:
        return
    with conn:
        for table in ("signals", "trade_alerts"):
            for hours, pcol, bcol in ((24, "p24", "btc24"), (72, "p72", "btc72")):
                rows = conn.execute(
                    "SELECT id, coin FROM {} WHERE {} IS NULL AND ts <= ?".format(table, pcol),
                    (now_ms() - hours * HOUR_MS,)).fetchall()
                for row_id, coin in rows:
                    price = market.price(coin)
                    if price > 0:
                        conn.execute("UPDATE {} SET {} = ?, {} = ? WHERE id = ?".format(
                            table, pcol, bcol), (price, btc, row_id))


def performance(rows: List[Tuple[str, float, float, float, float]]) -> Optional[Dict[str, float]]:
    """rows: (side, p0, btc0, p1, btc1). Returns averages in the signal direction."""
    rets = []
    bases = []
    for side, p0, b0, p1, b1 in rows:
        if not (p0 and b0 and p1 and b1):
            continue
        sign = 1.0 if side == "LONG" else -1.0
        rets.append(sign * (p1 / p0 - 1.0) * 100.0)
        bases.append(sign * (b1 / b0 - 1.0) * 100.0)
    if not rets:
        return None
    n = len(rets)
    return {"n": n, "ret": sum(rets) / n, "btc": sum(bases) / n,
            "win": 100.0 * sum(1 for r in rets if r > 0) / n}


def daily_summary(conn: sqlite3.Connection) -> None:
    if not daily_due(conn, "last_daily", CFG["daily_hour_utc"]):
        return
    items = sorted(((s["LONG"]["usd"] + s["SHORT"]["usd"], coin, s)
                    for coin, s in smart_opens_last_24h(conn).items()), reverse=True)
    lines = ["[DAILY] Smart traders - new positions in last 24h"]
    if not items:
        lines.append("No fresh positions from smart traders.")
    for _, coin, sides in items[:7]:
        lines.append("{}: {} long ({}) | {} short ({})".format(
            coin, len(sides["LONG"]["addrs"]), usd(sides["LONG"]["usd"]),
            len(sides["SHORT"]["addrs"]), usd(sides["SHORT"]["usd"])))

    lines.append("")
    lines.append("Big-trade alerts so far, 24h after the alert (all time):")
    any_quality = False
    for quality in ("HIGH", "MEDIUM", "LOW"):
        perf = performance(conn.execute(
            "SELECT side, p0, btc0, p24, btc24 FROM trade_alerts WHERE quality = ?",
            (quality,)).fetchall())
        if perf:
            any_quality = True
            lines.append("{}: {} trades | right {:.0f}% | avg {} (BTC {})".format(
                quality, perf["n"], perf["win"], pct(perf["ret"]), pct(perf["btc"])))
    if not any_quality:
        lines.append("Not enough data yet.")
    with conn:
        outbox_add(conn, "\n".join(lines))


def report(conn: sqlite3.Connection) -> None:
    print("Paper-trade report")
    print("Return = move in the signal direction. BTC = same direction on BTC, as baseline.\n")
    groups = [("Consensus signals", "SELECT side, p0, btc0, {p}, {b} FROM signals", ())]
    for quality in ("HIGH", "MEDIUM", "LOW"):
        groups.append(("Big trades, quality " + quality,
                       "SELECT side, p0, btc0, {p}, {b} FROM trade_alerts WHERE quality = ?",
                       (quality,)))
    for title, sql, params in groups:
        print(title)
        for hours, pcol, bcol in ((24, "p24", "btc24"), (72, "p72", "btc72")):
            perf = performance(conn.execute(sql.format(p=pcol, b=bcol), params).fetchall())
            if not perf:
                print("  {}h: no completed signals yet".format(hours))
                continue
            print("  {}h: n={} | avg return={} | avg BTC={} | excess={} | win rate={:.0f}%".format(
                hours, perf["n"], pct(perf["ret"]), pct(perf["btc"]),
                pct(perf["ret"] - perf["btc"]), perf["win"]))
    print("\nIf LOW does as well as HIGH, the quality filter is not helping: adjust it.")


def list_traders(conn: sqlite3.Connection) -> None:
    parsed = []
    for addr, score in conn.execute("SELECT addr, score FROM traders WHERE kind = 'smart'"):
        try:
            s = json.loads(score or "{}")
        except ValueError:
            s = {}
        parsed.append((s.get("pf", 0), addr, s))
    parsed.sort(reverse=True)
    print("Smart traders: {}".format(len(parsed)))
    for _, addr, s in parsed:
        print("{} PF={} trades={} good_months={}/{} fills/day={} net={} account={}".format(
            addr, s.get("pf"), s.get("trades"), s.get("good_months"), s.get("months_seen"),
            s.get("fills_per_day"), usd(s.get("net_pnl", 0)), usd(s.get("account_value", 0))))
    counts = conn.execute("SELECT kind, COUNT(*) FROM traders GROUP BY kind").fetchall()
    print("All tracked: " + ", ".join("{}={}".format(k, c) for k, c in counts))


def show_trader(conn: sqlite3.Connection, addr: str) -> None:
    """Prints the checks a big trade by this address would get, for every open position."""
    addr = addr.lower()
    market = Market()
    cur, perp_value = get_state(addr)
    profile = Profiles().get(addr)
    print("Trader {} | perp account {} | open positions {}".format(addr, usd(perp_value), len(cur)))
    for coin, c in sorted(cur.items(), key=lambda kv: -kv[1]["value"])[:10]:
        a = assess(coin, c, cur, profile, market)
        print("\n{} {} {} at {:.0f}x | Signal: {}".format(
            coin, side_of(c["szi"]), usd(c["value"]), c["lev"], a["quality"]))
        for line in trader_lines(conn, addr, coin, c, a):
            print("  " + line)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def cycle(conn: sqlite3.Connection, telegram: Telegram, profiles: Profiles,
          force_refresh: bool = False) -> None:
    if force_refresh or refresh_due(conn):
        try:
            refresh_traders(conn)
        except Exception as e:
            log("Trader refresh failed: {}".format(e))
            with conn:
                if conn.execute("SELECT COUNT(*) FROM traders").fetchone()[0] == 0:
                    for addr in read_extra_addresses():
                        conn.execute("INSERT OR IGNORE INTO traders (addr, kind, score) "
                                     "VALUES (?, 'manual', '{}')", (addr,))
                # Retry in 1 hour instead of waiting a full day
                meta_set(conn, "last_refresh_ms",
                         now_ms() - int((CFG["refresh_hours"] - 1) * HOUR_MS))
    market = Market()  # one price snapshot for the whole cycle
    scan(conn, market, profiles)
    check_consensus(conn, market)
    update_forward_prices(conn, market)
    daily_summary(conn)
    with conn:
        conn.execute("DELETE FROM opens WHERE ts < ?", (now_ms() - 7 * DAY_MS,))
    outbox_flush(conn, telegram)
    heartbeat(conn)


def main() -> None:
    parser = argparse.ArgumentParser(description="Whale Watch v2 (Hyperliquid)")
    parser.add_argument("--env", default="", help="path to env file with tokens")
    parser.add_argument("--once", action="store_true", help="run one cycle and exit")
    parser.add_argument("--refresh", action="store_true", help="rebuild trader lists now")
    parser.add_argument("--list", action="store_true", help="show smart traders and scores")
    parser.add_argument("--trader", default="", help="show the checks for one address")
    parser.add_argument("--report", action="store_true", help="paper-trade statistics")
    parser.add_argument("--test-telegram", action="store_true", help="send a test message")
    args = parser.parse_args()

    load_env_file(args.env)
    load_config()
    telegram = Telegram.from_env(USER_AGENT)

    if args.test_telegram:
        sys.exit(0 if telegram.send("Whale Watch test message. Telegram is working.") else 1)

    conn = open_db(CFG["db_path"], SCHEMA)
    profiles = Profiles()

    if args.list:
        list_traders(conn)
        return
    if args.trader:
        show_trader(conn, args.trader)
        return
    if args.report:
        report(conn)
        return
    if args.refresh and not args.once:
        refresh_traders(conn)
        outbox_flush(conn, telegram)
        return
    if args.once:
        cycle(conn, telegram, profiles, force_refresh=args.refresh)
        return

    interval = max(30, CFG["scan_seconds"])
    log("Running forever, a cycle every {} seconds".format(interval))
    while True:
        started = time.time()
        try:
            cycle(conn, telegram, profiles)
        except Exception as e:
            log("Unexpected error: {}".format(e))
        time.sleep(max(10.0, interval - (time.time() - started)))


if __name__ == "__main__":
    main()
