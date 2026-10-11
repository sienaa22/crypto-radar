#!/usr/bin/env python3
"""
News Watch v1

Sends the crypto news that can move a price, and skips the rest.

Sources (all free, no key):
  1. Token unlocks (DefiLlama emission schedules)
       - a large cliff unlock coming in 7 days / 1 day (sell pressure)
       - investor and team vesting about to END (that selling pressure stops)
  2. Exchange listings: Binance, Upbit, Bybit, OKX announcements, and new
     Coinbase markets read directly from its product list
  3. Hacks of $1M or more (DefiLlama)
  4. Headlines from CoinDesk, The Block, Decrypt and Cointelegraph, scored by
     what they are about (hack, listing, ETF decision, unlock, lawsuit, ...),
     which coin they name, and how many outlets carry the same story.
     Opinion, price analysis and recaps lose points.

Every source starts by recording what already exists, so only new items are sent.

Standard library only. Python 3.8+.

Usage:
  python3 news.py --env /opt/radar/radar.env            # run forever
  python3 news.py --env /opt/radar/radar.env --once      # one cycle
  python3 news.py --env /opt/radar/radar.env --unlocks   # upcoming unlocks from the cache
  python3 news.py --env /opt/radar/radar.env --score "Binance will list Aptos"
"""

import argparse
import email.utils
import json
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import namedtuple
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from common import (
    DAY_MS, HOUR_MS, Telegram, env_float, env_int, env_str, fnum, heartbeat,
    http_json, load_env_file, log, meta_get, meta_set, now_ms, open_db, outbox_add,
    outbox_flush, usd,
)

USER_AGENT = "news-watch/1.0 (personal research script)"
EMISSIONS_LIST = "https://defillama-datasets.llama.fi/emissionsProtocolsList"
EMISSION_URL = "https://defillama-datasets.llama.fi/emissions/{}"
COINS_PRICES = "https://coins.llama.fi/prices/current/{}"
HACKS_URL = "https://api.llama.fi/hacks"
CG_MARKETS = ("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
              "&order=market_cap_desc&per_page=250&page=1")
BINANCE_URL = ("https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
               "?type=1&catalogId=48&pageNo=1&pageSize=20")
UPBIT_URL = "https://api-manager.upbit.com/api/v1/announcements?os=web&page=1&per_page=20&category=trade"
BYBIT_URL = "https://api.bybit.com/v5/announcements/index?locale=en-US&type=new_crypto&limit=20"
OKX_URL = "https://www.okx.com/api/v5/support/announcements?annType=announcements-new-listings"
COINBASE_URL = "https://api.exchange.coinbase.com/products"
FEEDS = [
    ("CoinDesk", "https://www.coindesk.com/arc/outboundfeeds/rss/"),
    ("The Block", "https://www.theblock.co/rss.xml"),
    ("Decrypt", "https://decrypt.co/feed"),
    ("Cointelegraph", "https://cointelegraph.com/rss"),
]

# Listings of stocks and other non-crypto products
NOT_CRYPTO_RE = re.compile(r"TradFi|bStocks?|Stocks?\b|Tokenized|Securities|xStocks", re.I)

CFG = {}  # type: Dict[str, Any]

SCHEMA = """
    CREATE TABLE IF NOT EXISTS seen (
        source TEXT NOT NULL, item_key TEXT NOT NULL, ts INTEGER NOT NULL,
        PRIMARY KEY (source, item_key));
    CREATE TABLE IF NOT EXISTS emissions (
        slug TEXT PRIMARY KEY, fetched_ms INTEGER NOT NULL, summary TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS headlines (
        id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, outlet TEXT NOT NULL,
        title TEXT NOT NULL, link TEXT NOT NULL, story TEXT NOT NULL, score REAL NOT NULL,
        sent INTEGER NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_headlines_story ON headlines (story, ts);
"""


def load_config() -> None:
    CFG["db_path"] = env_str("NEWS_DB", "/opt/radar/news.db")
    CFG["interval"] = max(120, env_int("NEWS_INTERVAL_SECONDS", 600))
    CFG["min_score"] = env_float("NEWS_MIN_SCORE", 4)
    CFG["hack_min_usd"] = env_float("NEWS_HACK_MIN_USD", 1000000)
    CFG["unlock_min_pct"] = env_float("NEWS_UNLOCK_MIN_PCT", 1.0)        # of unlocked supply
    CFG["unlock_min_usd"] = env_float("NEWS_UNLOCK_MIN_USD", 5000000)
    CFG["unlock_min_mcap"] = env_float("NEWS_UNLOCK_MIN_MCAP", 100000000)
    CFG["vesting_end_days"] = env_int("NEWS_VESTING_END_DAYS", 14)
    CFG["unlock_fetch_per_cycle"] = env_int("NEWS_UNLOCK_FETCH_PER_CYCLE", 6)


def fetch_text(url: str, timeout: int = 30) -> str:
    for _ in range(5):
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 " + USER_AGENT})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            # Python before 3.11 does not follow 308 redirects by itself
            location = e.headers.get("Location")
            if e.code not in (307, 308) or not location:
                raise
            url = urllib.parse.urljoin(url, location)
    raise RuntimeError("too many redirects for " + url)


def get_json(url: str, timeout: int = 60) -> Any:
    # Some exchange APIs refuse requests without a browser-like agent
    return http_json(url, timeout=timeout, user_agent="Mozilla/5.0 " + USER_AGENT)


def fmt_date(ts_sec: float) -> str:
    return datetime.fromtimestamp(ts_sec, timezone.utc).strftime("%Y-%m-%d")


def fmt_tokens(x: float) -> str:
    if x >= 1e9:
        return "{:.2f}B".format(x / 1e9)
    if x >= 1e6:
        return "{:.1f}M".format(x / 1e6)
    if x >= 1e3:
        return "{:.0f}K".format(x / 1e3)
    return "{:.0f}".format(x)


# ---------------------------------------------------------------------------
# Seen items (each source starts silently)
# ---------------------------------------------------------------------------

Item = namedtuple("Item", "key text")
STARTED = "__started__"  # recorded on a source's first run, even when it had no items


def mark_started(conn: sqlite3.Connection, source: str) -> None:
    conn.execute("INSERT OR IGNORE INTO seen (source, item_key, ts) VALUES (?, ?, ?)",
                 (source, STARTED, now_ms()))


def queue_new(conn: sqlite3.Connection, source: str, items: List[Item],
              bootstrap: bool = True) -> int:
    """
    Queues items not seen before. With bootstrap, the first run of a source only
    records what exists (old listings are not news; upcoming unlocks are).
    """
    first_run = bootstrap and conn.execute("SELECT 1 FROM seen WHERE source = ? LIMIT 1",
                                           (source,)).fetchone() is None
    queued = 0
    with conn:
        if first_run:
            mark_started(conn, source)
        for item in items:
            if conn.execute("SELECT 1 FROM seen WHERE source = ? AND item_key = ?",
                            (source, item.key)).fetchone():
                continue
            conn.execute("INSERT INTO seen (source, item_key, ts) VALUES (?, ?, ?)",
                         (source, item.key, now_ms()))
            if not first_run:
                outbox_add(conn, item.text)
                queued += 1
    return queued


# ---------------------------------------------------------------------------
# Coin directory: names, tickers, 24h change (for matching and context)
# ---------------------------------------------------------------------------

# Tickers that are also common words or acronyms in headlines
TICKER_STOP = {
    "ETF", "SEC", "USD", "CEO", "CFO", "CTO", "FBI", "DOJ", "IRS", "IPO", "API", "NFT", "DAO",
    "DEX", "CEX", "TVL", "APR", "APY", "GDP", "CPI", "FED", "USA", "ONE", "SUN", "GAS",
    "NEAR", "MOVE", "JUST", "FUN", "WIN", "HOT", "KEY", "BOND", "ALL", "NEW", "TOP", "BIG",
    "NOW", "OUT", "AND", "THE", "FOR", "ANY", "TRUMP", "AI", "EU", "UK", "OK", "DOGS", "CAT",
}
NAME_STOP = {"Flow", "Stacks", "Compound", "Maker", "Graph", "Render", "Core", "Gas", "Sun",
             "Bitcoin Cash", "Tether Gold", "Wrapped Bitcoin", "Staked Ether"}


class Coins:
    """Top coins by market cap, refreshed every hour."""

    def __init__(self) -> None:
        self.loaded_ms = 0
        self.rows = []  # type: List[dict]
        self.by_ticker = {}  # type: Dict[str, dict]
        self.name_re = None  # type: Optional[Any]
        self.by_name = {}  # type: Dict[str, dict]

    def refresh(self) -> None:
        if now_ms() - self.loaded_ms < HOUR_MS and self.rows:
            return
        try:
            data = get_json(CG_MARKETS)
        except Exception as e:
            log("Coin list not loaded: {}".format(e))
            return
        self.rows = [{"id": c["id"], "sym": (c.get("symbol") or "").upper(),
                      "name": c.get("name") or c["id"], "rank": i + 1,
                      "change": fnum(c.get("price_change_percentage_24h")),
                      "price": fnum(c.get("current_price"))}
                     for i, c in enumerate(data or []) if c.get("id")]
        self.by_ticker = {}
        self.by_name = {}
        for c in reversed(self.rows):  # higher rank wins
            if len(c["sym"]) >= 3 and c["sym"] not in TICKER_STOP:
                self.by_ticker[c["sym"]] = c
            if len(c["name"]) >= 4 and c["name"] not in NAME_STOP:
                self.by_name[c["name"]] = c
        names = sorted(self.by_name, key=len, reverse=True)
        self.name_re = re.compile(r"\b(" + "|".join(re.escape(n) for n in names) + r")\b") \
            if names else None
        self.loaded_ms = now_ms()

    def find(self, text: str) -> List[dict]:
        """Coins named in a text: by name, $TICKER, or an all-caps ticker."""
        found = {}  # type: Dict[str, dict]
        if self.name_re:
            for m in self.name_re.finditer(text):
                c = self.by_name[m.group(1)]
                found[c["id"]] = c
        for m in re.finditer(r"\$?\b([A-Z][A-Z0-9]{2,9})\b", text):
            c = self.by_ticker.get(m.group(1))
            if c:
                found[c["id"]] = c
        return sorted(found.values(), key=lambda c: c["rank"])

    def context(self, text: str) -> str:
        """'Aptos (APT) +3.2% 24h, rank #30' for the top coins named in a text."""
        parts = ["{} ({}) {:+.1f}% 24h, rank #{}".format(c["name"], c["sym"], c["change"], c["rank"])
                 for c in self.find(text)[:2]]
        return "; ".join(parts)


# ---------------------------------------------------------------------------
# Headline scoring
# ---------------------------------------------------------------------------

# (pattern, points, label). Patterns are matched against the title and summary.
RULES = [
    (r"\b(hack(ed|s|er)?|exploit(ed|s)?|drain(ed|s)?|stolen|(security|data) breach)\b", 4, "hack"),
    (r"\b(approv\w*|greenlight\w*|reject\w*|denie[sd])\b[^.]{0,60}\bETFs?\b"
     r"|\bETFs?\b[^.]{0,60}\b(approv\w*|reject\w*|denie[sd])", 4, "ETF decision"),
    (r"\b(bankrupt\w*|insolven\w*|halts? withdrawals|freez\w* withdrawals|depeg\w*)", 4, "insolvency"),
    (r"\b(will list|to list|lists|listing|adds?|launch\w*)\b[^.]{0,40}\b(Binance|Coinbase|Upbit|"
     r"Robinhood|OKX|Bybit|Kraken)\b|\b(Binance|Coinbase|Upbit|Robinhood|OKX|Bybit|Kraken)\b"
     r"[^.]{0,40}\b(will list|to list|lists|listing|adds)\b", 3, "exchange listing"),
    (r"\bdelist\w*", 3, "delisting"),
    (r"\b(SEC|CFTC|DOJ|regulator\w*)\b[^.]{0,50}\b(sues?|charges?|lawsuit|settle\w*|drops?|"
     r"fine[sd]?|ban\w*)\b|\b(lawsuit|indict\w*|arrest\w*|sanction\w*)\b", 3, "legal"),
    (r"\bunlock\w*|\bvesting\b|\bcliff\b", 2, "token unlock"),
    (r"\b(unlocks?|vesting)\b[^.]{0,60}\b(end|ends|ended|ending|complete\w*|finish\w*)\b", 1,
     "unlocks ending"),
    (r"\b(tokenomics|supply (cut|reduction)|burn\w*|buybacks?)\b", 2, "tokenomics"),
    (r"\bairdrop\w*", 2, "airdrop"),
    (r"\b(acquir\w*|acquisition|merger|treasury (company|firm|strategy)|buys \$?[\d.,]+ ?(million|billion|[MB]))\b",
     2, "deal"),
    (r"\b(BlackRock|Fidelity|JPMorgan|Goldman|Visa|Mastercard|PayPal|Stripe|Nasdaq|CME|"
     r"Federal Reserve|White House)\b", 2, "big institution"),
    (r"\b(mainnet|hard fork|network upgrade|goes live|launch(es|ed)? on)\b", 1, "launch"),
    (r"\bETFs?\b", 1, "ETF"),
    (r"\bpartner(ship|s|ed)?\b", 1, "partnership"),
]
NOISE_RE = re.compile(
    r"\b(price prediction|price analysis|technical analysis|what happened in crypto today|"
    r"podcast|opinion|op-ed|weekly|recap|roundup|here'?s why|explained|how to|guide|"
    r"analyst\w*|could|might|chart\w*|sentiment|newsletter|interview)\b", re.I)
# Court news about an old case is not new market information
OLD_CASE_RE = re.compile(r"\b(convicted|jailed|sentenced|pleads?|pleaded|extradited|"
                         r"guilty|prison)\b", re.I)
COMPILED_RULES = [(re.compile(p, re.I), pts, label) for p, pts, label in RULES]


def score_headline(title: str, summary: str, coins: Coins) -> Tuple[float, List[str], List[dict]]:
    """Returns (score, reasons, coins named)."""
    text = title + ". " + summary[:300]
    score = 0.0
    reasons = []  # type: List[str]
    for pattern, points, label in COMPILED_RULES:
        # The title counts fully, a match only in the summary counts half
        if pattern.search(title):
            score += points
            reasons.append(label)
        elif pattern.search(summary[:300]):
            score += points / 2.0
            reasons.append(label + " (summary)")
    named = coins.find(text)
    if named:
        top = named[0]
        if top["rank"] <= 2:
            score += 1
        elif top["rank"] <= 100:
            score += 2
        else:
            score += 1
        reasons.append("names {} (#{})".format(top["sym"], top["rank"]))
    if NOISE_RE.search(title):
        score -= 3
        reasons.append("analysis/opinion")
    if OLD_CASE_RE.search(title):
        score -= 3
        reasons.append("court follow-up")
    return score, reasons, named


# Capitalized words that say nothing about which story it is
STORY_STOP = {"Crypto", "Bitcoin", "Ethereum", "Here", "What", "This", "That", "With", "After",
              "Over", "Says", "From", "Report", "Reports", "Million", "Billion", "Week", "Today"}


def story_of(reasons: List[str], named: List[dict], title: str) -> Tuple[str, Set[str]]:
    """(kind of event, names that identify the story: coins and proper nouns in the title)."""
    kind = next((r for r in reasons if not r.startswith("names") and "summary" not in r), "other")
    names = {c["id"] for c in named}
    names.update(w for w in re.findall(r"\b[A-Z][A-Za-z0-9]{3,}\b", title) if w not in STORY_STOP)
    return kind, names


def same_story(conn: sqlite3.Connection, kind: str, names: Set[str], outlet: str) -> Tuple[int, bool]:
    """Other outlets with a headline of the same kind sharing a name in the last 12h,
    and whether one of them was already sent."""
    outlets = set()
    sent = False
    for other_outlet, story, was_sent in conn.execute(
            "SELECT outlet, story, sent FROM headlines WHERE ts >= ? AND story LIKE ?",
            (now_ms() - 12 * HOUR_MS, kind + "|%")):
        if names & set(story.split("|", 1)[1].split()):
            if other_outlet != outlet:
                outlets.add(other_outlet)
            sent = sent or bool(was_sent)
    return len(outlets), sent


def parse_feed(xml_text: str) -> List[Dict[str, str]]:
    """Items of an RSS 2.0 or Atom feed: title, link, summary, ts (ms)."""
    out = []  # type: List[Dict[str, Any]]
    root = ET.fromstring(xml_text)
    atom = "{http://www.w3.org/2005/Atom}"
    for item in root.iter("item"):
        out.append({"title": (item.findtext("title") or "").strip(),
                    "link": (item.findtext("link") or "").strip(),
                    "summary": re.sub(r"<[^>]+>", " ", item.findtext("description") or ""),
                    "date": item.findtext("pubDate") or ""})
    for entry in root.iter(atom + "entry"):
        link = entry.find(atom + "link")
        out.append({"title": (entry.findtext(atom + "title") or "").strip(),
                    "link": link.get("href", "") if link is not None else "",
                    "summary": re.sub(r"<[^>]+>", " ", entry.findtext(atom + "summary") or ""),
                    "date": entry.findtext(atom + "updated") or ""})
    for it in out:
        try:
            it["ts"] = int(email.utils.parsedate_to_datetime(it["date"]).timestamp() * 1000)
        except (TypeError, ValueError):
            it["ts"] = now_ms()
    return [it for it in out if it["title"] and it["link"]]


def check_feeds(conn: sqlite3.Connection, coins: Coins) -> int:
    queued = 0
    for outlet, url in FEEDS:
        try:
            items = parse_feed(fetch_text(url))
        except Exception as e:
            log("{} feed failed: {}".format(outlet, e))
            continue
        source = "feed:" + outlet
        first_run = conn.execute("SELECT 1 FROM seen WHERE source = ? LIMIT 1",
                                 (source,)).fetchone() is None
        if first_run:
            with conn:
                mark_started(conn, source)
        for it in sorted(items, key=lambda i: i["ts"]):
            if conn.execute("SELECT 1 FROM seen WHERE source = ? AND item_key = ?",
                            (source, it["link"])).fetchone():
                continue
            score, reasons, named = score_headline(it["title"], it["summary"], coins)
            kind, names = story_of(reasons, named, it["title"])
            with conn:
                conn.execute("INSERT INTO seen (source, item_key, ts) VALUES (?, ?, ?)",
                             (source, it["link"], now_ms()))
                if first_run or now_ms() - it["ts"] > DAY_MS:
                    continue
                n_other, already_sent = same_story(conn, kind, names, outlet)
                if n_other:
                    score += min(2, n_other)
                    reasons.append("{} other outlet{}".format(n_other, "s" if n_other > 1 else ""))
                send = score >= CFG["min_score"] and not already_sent
                conn.execute(
                    "INSERT INTO headlines (ts, outlet, title, link, story, score, sent)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (now_ms(), outlet, it["title"], it["link"],
                     kind + "|" + " ".join(sorted(names)), score, int(send)))
                if not send:
                    continue
                lines = ["[NEWS] " + it["title"],
                         "Source: {}{}".format(outlet, " (+{} more)".format(n_other) if n_other else ""),
                         "Why: " + ", ".join(reasons) + " | score {:.0f}".format(score)]
                ctx = coins.context(it["title"] + ". " + it["summary"][:300])
                if ctx:
                    lines.append("Coin: " + ctx)
                lines.append("Link: " + it["link"])
                outbox_add(conn, "\n".join(lines))
                queued += 1
    with conn:
        conn.execute("DELETE FROM headlines WHERE ts < ?", (now_ms() - 7 * DAY_MS,))
    return queued


# ---------------------------------------------------------------------------
# Exchange listings and hacks
# ---------------------------------------------------------------------------

def listing_text(exchange: str, title: str, link: str, coins: Coins) -> str:
    lines = ["[LISTING] " + exchange, title]
    ctx = coins.context(title)
    if ctx:
        lines.append("Coin: " + ctx)
    lines.append("Link: " + link)
    return "\n".join(lines)


def binance_items(coins: Coins) -> List[Item]:
    data = get_json(BINANCE_URL)
    articles = (((data or {}).get("data") or {}).get("catalogs") or [{}])[0].get("articles") or []
    items = []
    for a in sorted(articles, key=lambda a: a.get("releaseDate") or 0):
        title = a.get("title") or ""
        if not a.get("code") or NOT_CRYPTO_RE.search(title):
            continue
        link = "https://www.binance.com/en/support/announcement/" + a["code"]
        items.append(Item(a["code"], listing_text("Binance", title, link, coins)))
    return items


UPBIT_NEW_RE = re.compile(r"(.+?)\(([A-Z0-9]+)\)\s*(신규 거래지원|.*마켓 디지털 자산 추가)")


def upbit_items(coins: Coins) -> List[Item]:
    data = get_json(UPBIT_URL)
    items = []
    for n in sorted(((data or {}).get("data") or {}).get("notices") or [], key=lambda n: n["id"]):
        m = UPBIT_NEW_RE.match(n.get("title") or "")
        if not m:
            continue  # warnings, delisting notices and other announcements
        sym = m.group(2)
        markets = re.search(r"\(([A-Z, ]+)마켓\)|\(([A-Z, ]+) 마켓\)", n["title"])
        market_text = (markets.group(1) or markets.group(2)).strip() if markets else "?"
        kind = "new listing" if "신규" in m.group(3) else "new markets"
        title = "Upbit {}: {} ({} markets)".format(kind, sym, market_text)
        link = "https://upbit.com/service_center/notice?id={}".format(n["id"])
        text = listing_text("Upbit (Korea)", title, link, coins).replace(
            "\nLink:", "\nOriginal: {}\nLink:".format(n["title"]), 1)
        items.append(Item(str(n["id"]), text))
    return items


def bybit_items(coins: Coins) -> List[Item]:
    data = get_json(BYBIT_URL)
    items = []
    rows = ((data or {}).get("result") or {}).get("list") or []
    for a in sorted(rows, key=lambda a: a.get("dateTimestamp") or 0):
        title = a.get("title") or ""
        if not a.get("url") or NOT_CRYPTO_RE.search(title):
            continue
        items.append(Item(a["url"], listing_text("Bybit", title, a["url"], coins)))
    return items


def okx_items(coins: Coins) -> List[Item]:
    data = get_json(OKX_URL)
    details = (((data or {}).get("data") or [{}])[0] or {}).get("details") or []
    items = []
    for a in sorted(details, key=lambda a: int(fnum(a.get("pTime")))):
        title = a.get("title") or ""
        if not a.get("url") or NOT_CRYPTO_RE.search(title):
            continue
        items.append(Item(a["url"], listing_text("OKX", title, a["url"], coins)))
    return items


def coinbase_items(coins: Coins) -> List[Item]:
    """New base currencies in Coinbase's product list, often before the official post."""
    products = get_json(COINBASE_URL)
    by_base = {}  # type: Dict[str, List[dict]]
    for p in products or []:
        if p.get("base_currency"):
            by_base.setdefault(p["base_currency"], []).append(p)
    items = []
    for base, ps in sorted(by_base.items()):
        state = ", ".join(sorted({("auction" if p.get("auction_mode") else
                                   "limit-only" if p.get("limit_only") else
                                   p.get("status") or "?") for p in ps}))
        title = "Coinbase added {}: {} ({})".format(base, ", ".join(sorted(p["id"] for p in ps)), state)
        link = "https://www.coinbase.com/advanced-trade/spot/{}".format(ps[0]["id"])
        items.append(Item(base, listing_text("Coinbase", title, link, coins)))
    return items


def hack_items() -> List[Item]:
    items = []
    recent = now_ms() / 1000 - 7 * 86400
    for h in sorted(get_json(HACKS_URL, timeout=90) or [], key=lambda h: h.get("date") or 0):
        amount = fnum(h.get("amount"))
        if fnum(h.get("date")) < recent or amount < CFG["hack_min_usd"]:
            continue
        text = ("[HACK] {name}: {amount} lost\n"
                "Date: {date} | Chain: {chain}\n"
                "Type: {kind}{technique}\n"
                "Link: https://defillama.com/hacks").format(
                    name=h.get("name") or "?", amount=usd(amount), date=fmt_date(fnum(h.get("date"))),
                    chain=", ".join(h.get("chain") or []) or "?",
                    kind=h.get("targetType") or h.get("classification") or "?",
                    technique=" - " + h["technique"] if h.get("technique") else "")
        items.append(Item("{}:{}".format(h.get("name"), h.get("date")), text))
    return items


# ---------------------------------------------------------------------------
# Token unlocks
# ---------------------------------------------------------------------------

def render_event(e: dict) -> str:
    text = e.get("description") or ""
    for i, n in enumerate(e.get("noOfTokens") or []):
        text = text.replace("{tokens[%d]}" % i, fmt_tokens(fnum(n)))
    return text.replace("{timestamp}", fmt_date(fnum(e.get("timestamp"))))


def summarize_emission(data: dict) -> dict:
    """The few numbers the alerts need from a (large) emission schedule."""
    now_s = time.time()
    meta = data.get("metadata") or {}
    cats = data.get("categories") or {}
    documented = data.get("documentedData") or {}
    insider_sections = set((cats.get("privateSale") or []) + (cats.get("insiders") or []))

    unlocked_now = 0.0
    insider_end = 0
    insider_left = 0.0
    for section in documented.get("data") or []:
        points = section.get("data") or []
        past = [p for p in points if fnum(p.get("timestamp")) <= now_s]
        if past:
            unlocked_now += fnum(past[-1].get("unlocked"))
        if section.get("label") in insider_sections and points:
            last_increase = 0
            prev = None
            for p in points:
                if prev is not None and fnum(p.get("unlocked")) > prev:
                    last_increase = int(fnum(p.get("timestamp")))
                prev = fnum(p.get("unlocked"))
            insider_end = max(insider_end, last_increase)
            insider_left += fnum(points[-1].get("unlocked")) - (fnum(past[-1].get("unlocked")) if past else 0.0)

    events = []
    for e in meta.get("events") or []:
        ts = fnum(e.get("timestamp"))
        if ts < now_s - DAY_MS / 1000 or ts > now_s + 60 * 86400:
            continue
        cliff = e.get("unlockType") == "cliff"
        events.append({"ts": int(ts), "cliff": cliff, "category": e.get("category") or "",
                       "tokens": sum(fnum(n) for n in e.get("noOfTokens") or []) if cliff else 0.0,
                       "text": render_event(e)})
    progress = (documented.get("tokenAllocation") or {}).get("progress") or {}
    token = meta.get("token") or ""
    gecko = data.get("gecko_id") or (token.split(":", 1)[1] if token.startswith("coingecko:") else "")
    return {"name": data.get("name") or "?", "gecko_id": gecko, "unlocked_now": unlocked_now,
            "insider_end": insider_end, "insider_left": insider_left,
            "progress": {k: progress.get(k) for k in ("privateSale", "insiders") if k in progress},
            "events": events}


def refresh_emissions(conn: sqlite3.Connection) -> int:
    """Downloads a few schedules per cycle, oldest first; each is refreshed weekly."""
    last_list = int(meta_get(conn, "emissions_list_ms") or 0)
    if now_ms() - last_list > DAY_MS:
        slugs = get_json(EMISSIONS_LIST)
        with conn:
            for slug in slugs or []:
                conn.execute("INSERT OR IGNORE INTO emissions (slug, fetched_ms, summary)"
                             " VALUES (?, 0, '{}')", (slug,))
            meta_set(conn, "emissions_list_ms", now_ms())
    due = conn.execute("SELECT slug FROM emissions WHERE fetched_ms < ? ORDER BY fetched_ms LIMIT ?",
                       (now_ms() - 7 * DAY_MS, CFG["unlock_fetch_per_cycle"])).fetchall()
    for (slug,) in due:
        try:
            summary = summarize_emission(get_json(EMISSION_URL.format(slug), timeout=120))
        except Exception as e:
            log("Emission schedule failed for {}: {}".format(slug, e))
            summary = None
        with conn:
            conn.execute("UPDATE emissions SET fetched_ms = ?, summary = COALESCE(?, summary)"
                         " WHERE slug = ?", (now_ms(), json.dumps(summary) if summary else None, slug))
    return len(due)


def current_prices(gecko_ids: List[str]) -> Dict[str, float]:
    out = {}  # type: Dict[str, float]
    ids = sorted(set(g for g in gecko_ids if g))
    for i in range(0, len(ids), 60):
        keys = ",".join("coingecko:" + g for g in ids[i:i + 60])
        try:
            coins = (get_json(COINS_PRICES.format(keys)) or {}).get("coins") or {}
        except Exception as e:
            log("Price fetch failed: {}".format(e))
            continue
        for k, v in coins.items():
            out[k.split(":", 1)[1]] = fnum(v.get("price"))
    return out


def unlock_items(conn: sqlite3.Connection) -> List[Item]:
    summaries = []
    for slug, raw in conn.execute("SELECT slug, summary FROM emissions WHERE fetched_ms > 0"):
        s = json.loads(raw)
        if s.get("gecko_id"):
            summaries.append((slug, s))
    prices = current_prices([s["gecko_id"] for _, s in summaries])
    now_s = time.time()
    items = []  # type: List[Item]
    for slug, s in summaries:
        price = prices.get(s["gecko_id"], 0.0)
        supply = s["unlocked_now"]
        if price <= 0 or supply <= 0 or supply * price < CFG["unlock_min_mcap"]:
            continue
        head = "{} | price ${:,.4g} | unlocked value {}".format(s["name"], price, usd(supply * price))

        # 1. Large cliff unlocks: 7 days and 1 day ahead
        for e in s["events"]:
            days = (e["ts"] - now_s) / 86400.0
            if not e["cliff"] or days < 0 or days > 7:
                continue
            value = e["tokens"] * price
            share = e["tokens"] / supply * 100.0
            if share < CFG["unlock_min_pct"] or value < CFG["unlock_min_usd"]:
                continue
            when = "1d" if days <= 1 else "7d"
            items.append(Item("cliff:{}:{}:{}".format(slug, e["ts"], when), (
                "[UNLOCK] {name}: {tokens} tokens ({value}, {share:.1f}% of unlocked supply) "
                "unlock on {date}{soon}\n"
                "Group: {group}\n"
                "{detail}\n"
                "{head}\n"
                "Large unlocks often bring selling pressure.\n"
                "Link: https://defillama.com/unlocks/{slug}").format(
                    name=s["name"], tokens=fmt_tokens(e["tokens"]), value=usd(value), share=share,
                    date=fmt_date(e["ts"]), soon=" (within 24h)" if when == "1d" else "",
                    group=e["category"] or "?", detail=e["text"], head=head, slug=slug)))

        # 2. Investor and team vesting ends: the selling pressure from insiders stops
        end = s["insider_end"]
        days_left = (end - now_s) / 86400.0 if end else -1
        if 0 <= days_left <= CFG["vesting_end_days"]:
            when = "1d" if days_left <= 1 else "early"
            progress = ", ".join("{} {}%".format({"privateSale": "investors", "insiders": "team"}[k], v)
                                 for k, v in s["progress"].items() if v is not None)
            items.append(Item("end:{}:{}:{}".format(slug, end, when), (
                "[UNLOCK END] {name}: investor and team vesting ends {date} ({left})\n"
                "Still to unlock for them: {left_tokens} tokens ({left_value})\n"
                "Done so far: {progress}\n"
                "{head}\n"
                "After this date no more scheduled insider unlocks: usually a positive change "
                "in supply.\n"
                "Link: https://defillama.com/unlocks/{slug}").format(
                    name=s["name"], date=fmt_date(end),
                    left="within 24h" if when == "1d" else "in {:.0f} days".format(days_left),
                    left_tokens=fmt_tokens(s["insider_left"]), left_value=usd(s["insider_left"] * price),
                    progress=progress or "?", head=head, slug=slug)))
    return items


def list_unlocks(conn: sqlite3.Connection) -> None:
    rows = conn.execute("SELECT COUNT(*), SUM(fetched_ms > 0) FROM emissions").fetchone()
    print("Schedules cached: {} of {}".format(rows[1] or 0, rows[0] or 0))
    for item in sorted(unlock_items(conn), key=lambda i: i.key):
        print("\n" + item.text)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def cycle(conn: sqlite3.Connection, coins: Coins, telegram: Telegram) -> None:
    coins.refresh()
    queued = 0
    sources = [
        ("listing:binance", lambda: binance_items(coins)),
        ("listing:upbit", lambda: upbit_items(coins)),
        ("listing:bybit", lambda: bybit_items(coins)),
        ("listing:okx", lambda: okx_items(coins)),
        ("listing:coinbase", lambda: coinbase_items(coins)),
        ("hacks", hack_items),
    ]
    for source, get_items in sources:
        try:
            queued += queue_new(conn, source, get_items())
        except Exception as e:
            log("{} failed: {}".format(source, e))
    queued += check_feeds(conn, coins)
    try:
        refresh_emissions(conn)
        if now_ms() - int(meta_get(conn, "unlocks_checked_ms") or 0) >= HOUR_MS:
            queued += queue_new(conn, "unlocks", unlock_items(conn), bootstrap=False)
            with conn:
                meta_set(conn, "unlocks_checked_ms", now_ms())
    except Exception as e:
        log("Unlocks failed: {}".format(e))
    sent = outbox_flush(conn, telegram)
    heartbeat(conn)
    log("Cycle done: {} new items, {} sent".format(queued, sent))


def main() -> None:
    ap = argparse.ArgumentParser(description="News Watch v1")
    ap.add_argument("--env", default="", help="path to env file with tokens")
    ap.add_argument("--once", action="store_true", help="run one cycle and exit")
    ap.add_argument("--unlocks", action="store_true", help="show upcoming unlock alerts from the cache")
    ap.add_argument("--score", default="", help="show how a headline would be scored")
    ap.add_argument("--test-telegram", action="store_true", help="send a test message")
    args = ap.parse_args()

    load_env_file(args.env)
    load_config()
    telegram = Telegram.from_env(USER_AGENT)
    if args.test_telegram:
        sys.exit(0 if telegram.send("News Watch test message. Telegram is working.") else 1)

    coins = Coins()
    if args.score:
        coins.refresh()
        score, reasons, _ = score_headline(args.score, "", coins)
        print("score {:.1f} (send at {:.0f}+): {}".format(score, CFG["min_score"], ", ".join(reasons)))
        return

    conn = open_db(CFG["db_path"], SCHEMA)
    if args.unlocks:
        list_unlocks(conn)
        return
    if args.once:
        cycle(conn, coins, telegram)
        return

    log("Running forever, a cycle every {} seconds".format(CFG["interval"]))
    while True:
        started = time.time()
        try:
            cycle(conn, coins, telegram)
        except Exception as e:
            log("Unexpected error: {}".format(e))
        time.sleep(max(30.0, CFG["interval"] - (time.time() - started)))


if __name__ == "__main__":
    main()
