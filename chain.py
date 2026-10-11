#!/usr/bin/env python3
"""
Wallet Watch v1

Follows known wallets on Ethereum and Base, block by block, through free
public RPC nodes, and reports moves that can come before price action:

  - EARLY TOKEN: a fund, market maker or watched person receives a token that
    has no market price yet. Funds receive allocations of a project's token
    before it lists, so this can show an investment before it is announced.
  - FUNDING: a fund sends a large amount of stablecoins or ETH to an address
    that is not an exchange (possibly paying into a deal).
  - TO / FROM EXCHANGE: a watched wallet sends a large amount of a token to an
    exchange (possible sale) or takes it out (accumulation).
  - PRE-UNLOCK FLOW: for tokens with an unlock in the next 14 days (read from
    news.db), large transfers of that token into exchange wallets.

Who is who comes from three places:
  1. a public set of Etherscan labels (exchange wallets, some funds and market
     makers), refreshed weekly,
  2. your own list in WALLETS_FILE (one wallet per line: address, kind, name),
  3. the Hyperliquid whales, named and smart traders from whales.db (kind
     "trader"): the same wallets, followed on Ethereum and Base.
  Kinds: fund, mm (market maker), person, team, trader, exchange.

  python3 chain.py --add 0x... fund "Paradigm"     # add a wallet to your list
  python3 chain.py --list                           # show watched wallets

Coverage depends on the labels: free sources know exchange wallets well and
fund wallets only partly. Add wallets you find (for example on Arkham or
Etherscan) to your list; the alerts get better with every good address.

Standard library only. Python 3.8+.
"""

import argparse
import json
import os
import re
import sqlite3
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from common import (
    DAY_MS, HOUR_MS, Telegram, env_float, env_int, env_str, fnum, heartbeat, http_json,
    load_env_file, log, meta_get, meta_set, now_ms, open_db, open_db_readonly, outbox_add,
    outbox_flush, record_signal, usd,
)

USER_AGENT = "wallet-watch/1.0 (personal research script)"
LABELS_URL = ("https://raw.githubusercontent.com/brianleect/etherscan-labels/main/"
              "data/etherscan/combined/combinedAllLabels.json")
COINS_PRICES = "https://coins.llama.fi/prices/current/{}"
CG_COIN = ("https://api.coingecko.com/api/v3/coins/{}?localization=false&tickers=false"
           "&market_data=false&community_data=false&developer_data=false")

TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
SYMBOL_CALL = "0x95d89b41"
DECIMALS_CALL = "0x313ce567"

# Public nodes that accept log queries filtered by wallet only (tried in order)
CHAINS = {
    "ethereum": {"rpcs": ["https://eth.drpc.org", "https://rpc.mevblocker.io",
                          "https://rpc.flashbots.net", "https://gateway.tenderly.co/public/mainnet"],
                 "explorer": "https://etherscan.io", "llama": "ethereum", "max_blocks": 100,
                 "native": "ETH", "native_id": "ethereum"},
    "base": {"rpcs": ["https://base.drpc.org", "https://gateway.tenderly.co/public/base",
                      "https://mainnet.base.org"],
             "explorer": "https://basescan.org", "llama": "base", "max_blocks": 300,
             "native": "ETH", "native_id": "ethereum"},
}

# Exchange labels in the Etherscan set
EXCHANGE_LABELS = {"binance", "coinbase", "kraken", "okx", "okex", "bybit", "huobi", "kucoin",
                   "gate-io", "bitfinex", "crypto-com", "upbit", "gemini", "bitstamp", "hot-wallet",
                   "bitget", "mexc", "htx"}
# Entities taken from the label set by name (only these, to keep false matches out)
ENTITY_PATTERNS = [
    (r"\bJump Trading\b", "mm"), (r"\bWintermute\b", "mm"), (r"\bAmber Group\b", "mm"),
    (r"\bAuros\b", "mm"), (r"\bFalconX\b", "mm"), (r"\bGSR\b", "mm"), (r"\bCumberland\b", "mm"),
    (r"\bDWF Labs\b", "mm"), (r"\bSpartan Group\b", "fund"), (r"\bDeFiance Capital\b", "fund"),
    (r"^a16z\b", "fund"), (r"^Paradigm\b", "fund"), (r"\bPantera Capital\b", "fund"),
    (r"\bPolychain Capital\b", "fund"), (r"\bDragonfly Capital\b", "fund"),
    (r"\bMulticoin Capital\b", "fund"), (r"\bBinance Labs\b", "fund"), (r"\bGalaxy Digital\b", "fund"),
    (r"\bVitalik Buterin\b", "person"), (r"\bJustin Sun\b", "person"),
]
# Label names that only mention an entity (governance delegates, rate models, its tokens)
ENTITY_NOISE = re.compile(r"Voting|Delegate|Rate Model|Monsters|Token|\(|Deployer|Proxy", re.I)
SPAM_TOKEN = re.compile(r"https?:|www\.|\.com|\.io|\.xyz|\.org|claim|visit|airdrop|reward|voucher|"
                        r"\$|gift|bonus|free", re.I)
STABLES = {"USDT", "USDC", "DAI", "USDE", "FDUSD", "PYUSD", "USDS", "TUSD"}

CFG = {}  # type: Dict[str, Any]

SCHEMA = """
    CREATE TABLE IF NOT EXISTS wallets (
        address TEXT PRIMARY KEY, kind TEXT NOT NULL, name TEXT NOT NULL, source TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS tokens (
        chain TEXT NOT NULL, address TEXT NOT NULL, symbol TEXT, decimals INTEGER,
        price REAL, price_ms INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (chain, address));
    CREATE TABLE IF NOT EXISTS unlock_tokens (
        gecko_id TEXT PRIMARY KEY, contract TEXT, checked_ms INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS seen (key TEXT PRIMARY KEY, ts INTEGER NOT NULL);
"""


def load_config() -> None:
    CFG["db_path"] = env_str("CHAIN_DB", "/opt/radar/chain.db")
    CFG["wallets_file"] = env_str("WALLETS_FILE", "/opt/radar/wallets.txt")
    CFG["news_db"] = env_str("NEWS_DB", "/opt/radar/news.db")
    CFG["whales_db"] = env_str("HL_DB", "/opt/radar/whales.db")
    CFG["interval"] = max(60, env_int("CHAIN_INTERVAL_SECONDS", 120))
    CFG["chains"] = [c.strip() for c in env_str("CHAIN_NETWORKS", "ethereum,base").split(",")
                     if c.strip() in CHAINS]
    CFG["exchange_move_usd"] = env_float("CHAIN_EXCHANGE_MOVE_USD", 1000000)
    CFG["funding_usd"] = env_float("CHAIN_FUNDING_USD", 250000)
    CFG["person_move_usd"] = env_float("CHAIN_PERSON_MOVE_USD", 500000)
    CFG["mm_factor"] = env_float("CHAIN_MM_FACTOR", 5.0)  # market makers move a lot: higher bar
    CFG["unlock_flow_usd"] = env_float("CHAIN_UNLOCK_FLOW_USD", 1000000)
    CFG["early_token_max_per_day"] = env_int("CHAIN_EARLY_TOKEN_MAX_PER_DAY", 8)


# ---------------------------------------------------------------------------
# RPC
# ---------------------------------------------------------------------------

class Rpc:
    def __init__(self, chain: str) -> None:
        self.chain = chain
        self.urls = CHAINS[chain]["rpcs"]
        self.index = 0

    def call(self, method: str, params: list) -> Any:
        """JSON-RPC with failover to the next public node."""
        last = None  # type: Optional[Exception]
        for _ in range(len(self.urls)):
            url = self.urls[self.index]
            try:
                res = http_json(url, body={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                                timeout=30, attempts=1, user_agent="Mozilla/5.0 " + USER_AGENT)
                if "error" in res:
                    raise RuntimeError(str(res["error"])[:200])
                return res.get("result")
            except Exception as e:
                last = e
                self.index = (self.index + 1) % len(self.urls)
        raise RuntimeError("all {} nodes failed: {}".format(self.chain, last))

    def head(self) -> int:
        return int(self.call("eth_blockNumber", []), 16)

    def logs(self, from_block: int, to_block: int, topics: list,
             address: Optional[List[str]] = None) -> List[dict]:
        flt = {"fromBlock": hex(from_block), "toBlock": hex(to_block), "topics": topics}
        if address:
            flt["address"] = address
        return self.call("eth_getLogs", [flt]) or []


def pad(address: str) -> str:
    return "0x" + "0" * 24 + address[2:].lower()


def unpad(topic: str) -> str:
    return "0x" + topic[-40:].lower()


def decode_string(hex_data: str) -> str:
    """ABI string or bytes32 return value."""
    raw = bytes.fromhex(hex_data[2:]) if hex_data and hex_data != "0x" else b""
    if len(raw) >= 64:
        length = int.from_bytes(raw[32:64], "big")
        if 0 < length <= len(raw) - 64:
            return raw[64:64 + length].decode("utf-8", "replace").strip()
    return raw.rstrip(b"\x00").decode("utf-8", "replace").strip()


# ---------------------------------------------------------------------------
# Wallet labels
# ---------------------------------------------------------------------------

def refresh_labels(conn: sqlite3.Connection) -> None:
    """Weekly: exchange wallets and selected entities from the Etherscan label set."""
    if now_ms() - int(meta_get(conn, "labels_ms") or 0) < 7 * DAY_MS:
        return
    try:
        data = http_json(LABELS_URL, timeout=120, user_agent=USER_AGENT)
    except Exception as e:
        log("Label set not loaded: {}".format(e))
        return
    rows = []
    for address, v in (data or {}).items():
        name = (v.get("name") or "").strip()
        labels = set(v.get("labels") or [])
        if not address.startswith("0x") or not name:
            continue
        if labels & EXCHANGE_LABELS or re.search(r"Deposit|Hot Wallet|Cold Wallet", name):
            rows.append((address.lower(), "exchange", name))
            continue
        if ENTITY_NOISE.search(name):
            continue
        for pattern, kind in ENTITY_PATTERNS:
            if re.search(pattern, name):
                rows.append((address.lower(), kind, name))
                break
    with conn:
        conn.execute("DELETE FROM wallets WHERE source = 'labels'")
        conn.executemany("INSERT OR IGNORE INTO wallets (address, kind, name, source) "
                         "VALUES (?, ?, ?, 'labels')", rows)
        meta_set(conn, "labels_ms", now_ms())
    log("Label set: {} wallets".format(len(rows)))


def load_user_wallets(conn: sqlite3.Connection) -> None:
    """Your list wins over the label set. Format: address kind name..."""
    path = CFG["wallets_file"]
    if not os.path.exists(path):
        return
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.split("#", 1)[0].split(None, 2)
            if len(parts) >= 2 and parts[0].lower().startswith("0x") and len(parts[0]) == 42:
                rows.append((parts[0].lower(), parts[1].lower(), parts[2].strip() if len(parts) > 2 else parts[1]))
    with conn:
        conn.execute("DELETE FROM wallets WHERE source = 'user'")
        conn.executemany("INSERT OR REPLACE INTO wallets (address, kind, name, source) VALUES (?, ?, ?, 'user')",
                         rows)


def load_hyperliquid_traders(conn: sqlite3.Connection) -> None:
    """Hyperliquid's tracked whales, named and smart traders use ordinary EVM
    wallets, so the same people can be followed on Ethereum and Base."""
    hl = open_db_readonly(CFG["whales_db"])
    if hl is None:
        return
    rows = []
    try:
        for address, kind, score in hl.execute("SELECT addr, kind, score FROM traders"):
            try:
                name = (json.loads(score or "{}") or {}).get("name") or ""
            except ValueError:
                name = ""
            label = "HL {} {}".format(kind, name or address[:8]).strip()
            rows.append((address.lower(), "trader", label))
    except sqlite3.Error as e:
        log("whales.db read failed: {}".format(e))
    finally:
        hl.close()
    with conn:
        conn.execute("DELETE FROM wallets WHERE source = 'hyperliquid'")
        conn.executemany("INSERT OR IGNORE INTO wallets (address, kind, name, source) "
                         "VALUES (?, ?, ?, 'hyperliquid')", rows)


def wallets(conn: sqlite3.Connection) -> Dict[str, Tuple[str, str]]:
    return {a: (k, n) for a, k, n in conn.execute("SELECT address, kind, name FROM wallets")}


# ---------------------------------------------------------------------------
# Tokens and prices
# ---------------------------------------------------------------------------

def token_info(conn: sqlite3.Connection, rpc: Rpc, address: str) -> Tuple[str, int, Optional[float]]:
    """(symbol, decimals, USD price or None when the token has no market price)."""
    row = conn.execute("SELECT symbol, decimals, price, price_ms FROM tokens WHERE chain = ? AND address = ?",
                       (rpc.chain, address)).fetchone()
    if row and now_ms() - row[3] < 30 * 60 * 1000:
        return row[0], row[1], row[2]
    if row:
        symbol, decimals = row[0], row[1]
    else:
        try:
            symbol = decode_string(rpc.call("eth_call", [{"to": address, "data": SYMBOL_CALL}, "latest"]))[:20]
            decimals = int(rpc.call("eth_call", [{"to": address, "data": DECIMALS_CALL}, "latest"]) or "0x12", 16)
        except Exception:
            symbol, decimals = "?", 18
    price = None
    key = "{}:{}".format(CHAINS[rpc.chain]["llama"], address)
    try:
        coins = (http_json(COINS_PRICES.format(key), timeout=30, user_agent=USER_AGENT) or {}).get("coins") or {}
        price = fnum(coins.get(key, {}).get("price")) or None
    except Exception as e:
        log("Price failed for {}: {}".format(key, e))
        if row:
            price = row[2]
    with conn:
        conn.execute("INSERT OR REPLACE INTO tokens (chain, address, symbol, decimals, price, price_ms)"
                     " VALUES (?, ?, ?, ?, ?, ?)", (rpc.chain, address, symbol, decimals, price, now_ms()))
    return symbol, decimals, price


def native_price(conn: sqlite3.Connection) -> float:
    row = conn.execute("SELECT price, price_ms FROM tokens WHERE chain = 'native' AND address = 'ethereum'").fetchone()
    if row and now_ms() - row[1] < 30 * 60 * 1000 and row[0]:
        return row[0]
    try:
        price = fnum(((http_json(COINS_PRICES.format("coingecko:ethereum"), timeout=30, user_agent=USER_AGENT)
                       or {}).get("coins") or {}).get("coingecko:ethereum", {}).get("price"))
    except Exception:
        return row[0] if row else 0.0
    with conn:
        conn.execute("INSERT OR REPLACE INTO tokens (chain, address, symbol, decimals, price, price_ms)"
                     " VALUES ('native', 'ethereum', 'ETH', 18, ?, ?)", (price, now_ms()))
    return price


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------

def first_time(conn: sqlite3.Connection, key: str) -> bool:
    if conn.execute("SELECT 1 FROM seen WHERE key = ?", (key,)).fetchone():
        return False
    with conn:
        conn.execute("INSERT INTO seen (key, ts) VALUES (?, ?)", (key, now_ms()))
        conn.execute("DELETE FROM seen WHERE ts < ?", (now_ms() - 30 * DAY_MS,))
    return True


def who(address: str, book: Dict[str, Tuple[str, str]]) -> str:
    if address in book:
        kind, name = book[address]
        return "{} [{}]".format(name, kind)
    return address[:8] + "..." + address[-4:]


def send(conn: sqlite3.Connection, text: str, kind: str, direction: int = 0,
         symbol: Optional[str] = None, link: str = "") -> None:
    with conn:
        outbox_add(conn, text)
    record_signal("chain", kind, text.splitlines()[0], direction=direction, symbol=symbol, link=link)


def judge_transfer(conn: sqlite3.Connection, rpc: Rpc, book: Dict[str, Tuple[str, str]],
                   token: Optional[str], sender: str, receiver: str, raw_amount: int, tx: str) -> None:
    """Decides whether one transfer involving a watched wallet is worth an alert."""
    chain = CHAINS[rpc.chain]
    if token is None:
        symbol, decimals, price = chain["native"], 18, native_price(conn)
    else:
        symbol, decimals, price = token_info(conn, rpc, token)
    amount = raw_amount / float(10 ** max(0, min(decimals, 36)))
    value = amount * price if price else 0.0
    s_kind = book.get(sender, ("", ""))[0]
    r_kind = book.get(receiver, ("", ""))[0]
    link = "{}/tx/{}".format(chain["explorer"], tx)
    where = rpc.chain.capitalize()

    # 1. A watched non-exchange wallet receives a token that has no market price yet
    if token and price is None and r_kind in ("fund", "mm", "team") and s_kind != "exchange":
        if SPAM_TOKEN.search(symbol or "") or symbol in ("", "?"):
            return
        day_key = "early_count_" + time.strftime("%Y-%m-%d")
        count = int(meta_get(conn, day_key) or 0)
        if count >= CFG["early_token_max_per_day"] or not first_time(conn, "early:{}:{}".format(token, receiver)):
            return
        with conn:
            meta_set(conn, day_key, count + 1)
        send(conn, (
            "[EARLY TOKEN] {to} received {amount} {sym}, a token with no market price yet ({where})\n"
            "From: {frm}\n"
            "Token contract: {explorer}/token/{token}\n"
            "A fund getting a token before it trades can mean an investment that is not announced yet.\n"
            "Link: {link}").format(to=who(receiver, book), amount="{:,.0f}".format(amount), sym=symbol,
                                   where=where, frm=who(sender, book), explorer=chain["explorer"],
                                   token=token, link=link),
             "early_token", 0, None, link)
        return

    if not value:
        return

    # 2. Funding: a fund sends stablecoins or ETH to a wallet that is not an exchange
    if s_kind == "fund" and r_kind != "exchange" and (symbol.upper() in STABLES or token is None) \
            and value >= CFG["funding_usd"] and first_time(conn, "fund:" + tx):
        send(conn, (
            "[FUNDING?] {frm} sent {value} ({amount} {sym}) to {to} ({where})\n"
            "The receiver is not a known exchange: possibly a deal or a project payment.\n"
            "Link: {link}").format(frm=who(sender, book), value=usd(value), amount="{:,.0f}".format(amount),
                                   sym=symbol, to=who(receiver, book), where=where, link=link),
             "fund_payment", 0, None, link)
        return

    # 3. Watched wallets moving to or from exchanges
    bar = CFG["exchange_move_usd"] * (CFG["mm_factor"] if "mm" in (s_kind, r_kind) else 1.0)
    if r_kind == "exchange" and s_kind in ("fund", "mm", "person", "team", "trader") and value >= bar \
            and first_time(conn, "to-ex:" + tx):
        send(conn, (
            "[TO EXCHANGE] {frm} sent {value} of {sym} to {to} ({where})\n"
            "Read: a deposit to an exchange often comes before selling.\n"
            "Link: {link}").format(frm=who(sender, book), value=usd(value), sym=symbol,
                                   to=who(receiver, book), where=where, link=link),
             "wallet_to_exchange", -1, symbol, link)
        return
    if s_kind == "exchange" and r_kind in ("fund", "mm", "person", "team", "trader") and value >= bar \
            and symbol.upper() not in STABLES and first_time(conn, "from-ex:" + tx):
        send(conn, (
            "[FROM EXCHANGE] {to} withdrew {value} of {sym} from {frm} ({where})\n"
            "Read: taking coins off an exchange usually means holding, not selling.\n"
            "Link: {link}").format(to=who(receiver, book), value=usd(value), sym=symbol,
                                   frm=who(sender, book), where=where, link=link),
             "wallet_from_exchange", 1, symbol, link)
        return

    # 4. Any large move by a watched person
    if ({"person", "trader"} & {s_kind, r_kind}) and value >= CFG["person_move_usd"] and first_time(conn, "person:" + tx):
        send(conn, (
            "[WALLET MOVE] {frm} -> {to}: {value} of {sym} ({where})\n"
            "Link: {link}").format(frm=who(sender, book), to=who(receiver, book), value=usd(value),
                                   sym=symbol, where=where, link=link),
             "person_move", 0, symbol, link)


# ---------------------------------------------------------------------------
# Scanning
# ---------------------------------------------------------------------------

def scan_chain(conn: sqlite3.Connection, rpc: Rpc, book: Dict[str, Tuple[str, str]],
               unlock_contracts: Dict[str, Tuple[str, int]]) -> int:
    head = rpc.head() - 2  # stay a little behind the tip
    key = "last_block_" + rpc.chain
    last = int(meta_get(conn, key) or head - 5)
    start = max(last + 1, head - CHAINS[rpc.chain]["max_blocks"])
    if start > head:
        return 0
    watched = sorted(a for a, (k, _) in book.items() if k != "exchange")
    found = 0
    for i in range(0, len(watched), 50):
        chunk = [pad(a) for a in watched[i:i + 50]]
        for topics in ([TRANSFER, chunk], [TRANSFER, None, chunk]):
            for lg in rpc.logs(start, head, topics):
                if len(lg.get("topics") or []) != 3:
                    continue  # NFTs carry the id as a fourth topic
                found += 1
                judge_transfer(conn, rpc, book, lg["address"].lower(), unpad(lg["topics"][1]),
                               unpad(lg["topics"][2]), int(lg.get("data") or "0x0", 16),
                               lg["transactionHash"])

    if rpc.chain == "ethereum":
        found += scan_native(conn, rpc, book, start, head, set(watched))
        found += scan_unlock_flows(conn, rpc, book, start, head, unlock_contracts)
    with conn:
        meta_set(conn, key, head)
    return found


def scan_native(conn: sqlite3.Connection, rpc: Rpc, book: Dict[str, Tuple[str, str]],
                start: int, head: int, watched: Set[str]) -> int:
    """Plain ETH transfers from or to watched wallets (not visible in token logs)."""
    found = 0
    for number in range(max(start, head - 15), head + 1):
        block = rpc.call("eth_getBlockByNumber", [hex(number), True]) or {}
        for tx in block.get("transactions") or []:
            sender, receiver = (tx.get("from") or "").lower(), (tx.get("to") or "").lower()
            amount = int(tx.get("value") or "0x0", 16)
            if amount and (sender in watched or receiver in watched):
                found += 1
                judge_transfer(conn, rpc, book, None, sender, receiver, amount, tx["hash"])
    return found


def upcoming_unlock_tokens(conn: sqlite3.Connection) -> Dict[str, Tuple[str, int]]:
    """{ethereum contract: (name, unlock time)} for unlocks in the next 14 days."""
    news = open_db_readonly(CFG["news_db"])
    if news is None:
        return {}
    soon = {}  # type: Dict[str, Tuple[str, int]]
    now_s = time.time()
    try:
        for raw, in news.execute("SELECT summary FROM emissions WHERE fetched_ms > 0"):
            s = json.loads(raw)
            times = [e["ts"] for e in s.get("events") or [] if e.get("cliff")] + [s.get("insider_end") or 0]
            upcoming = [t for t in times if now_s <= t <= now_s + 14 * 86400]
            if s.get("gecko_id") and upcoming:
                soon[s["gecko_id"]] = (s.get("name") or s["gecko_id"], min(upcoming))
    except sqlite3.Error as e:
        log("news.db read failed: {}".format(e))
    finally:
        news.close()

    contracts = {}  # type: Dict[str, Tuple[str, int]]
    for gecko_id, info in soon.items():
        row = conn.execute("SELECT contract, checked_ms FROM unlock_tokens WHERE gecko_id = ?",
                           (gecko_id,)).fetchone()
        if not row or now_ms() - row[1] > 7 * DAY_MS:
            try:
                data = http_json(CG_COIN.format(gecko_id), timeout=30, user_agent=USER_AGENT) or {}
                contract = ((data.get("platforms") or {}).get("ethereum") or "").lower() or None
            except Exception:
                contract = row[0] if row else None
            with conn:
                conn.execute("INSERT OR REPLACE INTO unlock_tokens (gecko_id, contract, checked_ms) "
                             "VALUES (?, ?, ?)", (gecko_id, contract, now_ms()))
            time.sleep(2)  # CoinGecko free rate limit
            row = (contract, now_ms())
        if row[0]:
            contracts[row[0]] = info
    return contracts


def scan_unlock_flows(conn: sqlite3.Connection, rpc: Rpc, book: Dict[str, Tuple[str, str]],
                      start: int, head: int, contracts: Dict[str, Tuple[str, int]]) -> int:
    if not contracts:
        return 0
    exchanges = sorted(a for a, (k, _) in book.items() if k == "exchange")
    totals = {}  # type: Dict[Tuple[str, str], List[Any]]
    for i in range(0, len(exchanges), 50):
        chunk = [pad(a) for a in exchanges[i:i + 50]]
        for lg in rpc.logs(start, head, [TRANSFER, None, chunk], address=sorted(contracts)):
            if len(lg.get("topics") or []) != 3:
                continue
            sender = unpad(lg["topics"][1])
            if book.get(sender, ("",))[0] == "exchange":
                continue  # moves between exchange wallets
            token = lg["address"].lower()
            entry = totals.setdefault((token, sender), [0, lg["transactionHash"], unpad(lg["topics"][2])])
            entry[0] += int(lg.get("data") or "0x0", 16)
    sent = 0
    for (token, sender), (raw, tx, receiver) in totals.items():
        symbol, decimals, price = token_info(conn, rpc, token)
        value = raw / float(10 ** decimals) * (price or 0)
        if value < CFG["unlock_flow_usd"] or not first_time(conn, "unlock-flow:{}:{}:{}".format(
                token, sender, int(now_ms() / (6 * HOUR_MS)))):
            continue
        name, unlock_ts = contracts[token]
        days = max(0.0, (unlock_ts - time.time()) / 86400.0)
        send(conn, (
            "[PRE-UNLOCK FLOW] {value} of {name} ({sym}) sent to {to}\n"
            "From: {frm}\n"
            "Unlock in {days:.0f} days ({date})\n"
            "Read: tokens moving to an exchange ahead of an unlock often mean selling.\n"
            "Link: {explorer}/tx/{tx}").format(
                value=usd(value), name=name, sym=symbol, to=who(receiver, book), frm=who(sender, book),
                days=days, date=time.strftime("%Y-%m-%d", time.gmtime(unlock_ts)),
                explorer=CHAINS[rpc.chain]["explorer"], tx=tx),
             "preunlock_exchange_inflow", -1, symbol)
        sent += 1
    return sent


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def cycle(conn: sqlite3.Connection, rpcs: Dict[str, Rpc], telegram: Telegram) -> None:
    refresh_labels(conn)
    load_hyperliquid_traders(conn)
    load_user_wallets(conn)  # last, so your own labels win
    book = wallets(conn)
    unlocks = {}  # type: Dict[str, Tuple[str, int]]
    if now_ms() - int(meta_get(conn, "unlocks_ms") or 0) >= HOUR_MS or not meta_get(conn, "unlocks"):
        unlocks = upcoming_unlock_tokens(conn)
        with conn:
            meta_set(conn, "unlocks", json.dumps(unlocks))
            meta_set(conn, "unlocks_ms", now_ms())
    else:
        unlocks = {k: tuple(v) for k, v in json.loads(meta_get(conn, "unlocks") or "{}").items()}
    total = 0
    for chain, rpc in rpcs.items():
        try:
            total += scan_chain(conn, rpc, book, unlocks)
        except Exception as e:
            log("{} scan failed: {}".format(chain, e))
    outbox_flush(conn, telegram)
    heartbeat(conn)
    log("Cycle done: {} transfers looked at, {} watched wallets, {} unlock tokens".format(
        total, sum(1 for k, _ in book.values() if k != "exchange"), len(unlocks)))


def add_wallet(path: str, address: str, kind: str, name: str) -> None:
    if not re.match(r"^0x[0-9a-fA-F]{40}$", address):
        sys.exit("Not an address: " + address)
    if kind not in ("fund", "mm", "person", "team", "trader", "exchange"):
        sys.exit("Kind must be fund, mm, person, team, trader or exchange")
    with open(path, "a", encoding="utf-8") as f:
        f.write("{} {} {}\n".format(address.lower(), kind, name))
    print("Added {} as {} ({}) to {}".format(address, kind, name, path))


def main() -> None:
    ap = argparse.ArgumentParser(description="Wallet Watch v1")
    ap.add_argument("--env", default="", help="path to env file with tokens")
    ap.add_argument("--once", action="store_true", help="run one cycle and exit")
    ap.add_argument("--list", action="store_true", help="show watched wallets")
    ap.add_argument("--add", nargs=3, metavar=("ADDRESS", "KIND", "NAME"),
                    help="add a wallet to your list (kind: fund, mm, person, team, exchange)")
    args = ap.parse_args()

    load_env_file(args.env)
    load_config()
    if args.add:
        add_wallet(CFG["wallets_file"], *args.add)
        return
    telegram = Telegram.from_env(USER_AGENT)
    conn = open_db(CFG["db_path"], SCHEMA)
    if args.list:
        refresh_labels(conn)
        load_hyperliquid_traders(conn)
        load_user_wallets(conn)
        rows = conn.execute("SELECT kind, COUNT(*) FROM wallets GROUP BY kind").fetchall()
        print("Watched wallets: " + ", ".join("{}={}".format(k, n) for k, n in rows))
        for a, k, n, src in conn.execute("SELECT address, kind, name, source FROM wallets "
                                         "WHERE kind != 'exchange' ORDER BY kind, name"):
            print("{} {:<7} {:<6} {}".format(a, k, src, n))
        return
    rpcs = {c: Rpc(c) for c in CFG["chains"]}
    if args.once:
        cycle(conn, rpcs, telegram)
        return
    log("Running forever, a cycle every {} seconds on {}".format(CFG["interval"], ", ".join(rpcs)))
    while True:
        started = time.time()
        try:
            cycle(conn, rpcs, telegram)
        except Exception as e:
            log("Unexpected error: {}".format(e))
        time.sleep(max(15.0, CFG["interval"] - (time.time() - started)))


if __name__ == "__main__":
    main()
