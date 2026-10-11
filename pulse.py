#!/usr/bin/env python3
"""
Market Pulse v1

Watches for things that happen fast and tells you right away, with context:

  1. PUMP / DUMP: a coin moves far more than it normally does within 5, 15
     or 60 minutes (Binance futures prices, read every minute). Each alert
     lists what the other services saw on that coin in the last hours (news,
     listings, whale trades, flows), so the likely cause is right there.
  2. MARKET MOVE: BTC itself moves sharply.
  3. LIQUIDATIONS: a single large liquidation on Binance futures, and
     liquidation cascades (many in a few minutes on one coin), read live from
     Binance's public liquidation stream.

Liquidations of known Hyperliquid traders are reported by whales.py.

Standard library only. Python 3.8+.

Usage:
  python3 pulse.py --env /opt/radar/radar.env        # run forever
"""

import argparse
import base64
import json
import os
import socket
import ssl
import struct
import sys
import threading
import time
from collections import defaultdict, deque
from typing import Any, Deque, Dict, List, Optional, Tuple

from common import (
    HOUR_MS, Telegram, ago, env_float, env_int, env_str, fnum, heartbeat, http_json,
    load_env_file, log, now_ms, open_db, open_db_readonly, outbox_add, outbox_flush,
    record_signal, signals_db, usd,
)

USER_AGENT = "market-pulse/1.0 (personal research script)"
FAPI = "https://fapi.binance.com"
LIQ_HOST = "fstream.binance.com"
LIQ_PATH = "/market/ws/!forceOrder@arr"  # the old /ws/ path connects but stays silent

CFG = {}  # type: Dict[str, Any]

SCHEMA = """
    CREATE TABLE IF NOT EXISTS cooldown (key TEXT PRIMARY KEY, until INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS spikes (key TEXT PRIMARY KEY, until INTEGER NOT NULL, size REAL NOT NULL);
"""


def load_config() -> None:
    CFG["db_path"] = env_str("PULSE_DB", "/opt/radar/pulse.db")
    CFG["brain_db"] = env_str("BRAIN_DB", "/opt/radar/brain.db")
    CFG["news_db"] = env_str("NEWS_DB", "/opt/radar/news.db")
    CFG["min_volume"] = env_float("PULSE_MIN_VOLUME_USD", 10000000)   # 24h volume floor
    # Minimum moves; the real bar is the larger of these and N times the coin's usual move
    CFG["min_move"] = {5: env_float("PULSE_MIN_MOVE_5M", 4.0), 15: env_float("PULSE_MIN_MOVE_15M", 6.0),
                       60: env_float("PULSE_MIN_MOVE_60M", 10.0)}
    CFG["sigma_mult"] = env_float("PULSE_SIGMA_MULT", 5.0)
    CFG["btc_move"] = env_float("PULSE_BTC_MOVE_15M", 2.0)
    CFG["liq_single_usd"] = env_float("PULSE_LIQ_SINGLE_USD", 2000000)
    CFG["liq_cascade_usd"] = env_float("PULSE_LIQ_CASCADE_USD", 15000000)  # one coin, 5 minutes
    CFG["liq_market_usd"] = env_float("PULSE_LIQ_MARKET_USD", 100000000)   # all coins, 15 minutes


def on_cooldown(conn, key: str, hours: float) -> bool:
    row = conn.execute("SELECT until FROM cooldown WHERE key = ?", (key,)).fetchone()
    if row and row[0] > now_ms():
        return True
    with conn:
        conn.execute("INSERT OR REPLACE INTO cooldown (key, until) VALUES (?, ?)",
                     (key, now_ms() + int(hours * HOUR_MS)))
    return False


def base(sym: str) -> str:
    return sym[:-4] if sym.endswith("USDT") else sym


# ---------------------------------------------------------------------------
# Context: what the other services saw on this coin
# ---------------------------------------------------------------------------

def coin_id_for(symbol: str) -> Optional[str]:
    conn = open_db_readonly(CFG["brain_db"])
    if conn is None:
        return None
    try:
        sym = symbol.upper()
        for prefix in ("1000000", "1000"):
            if sym.startswith(prefix) and len(sym) > len(prefix) + 1:
                sym = sym[len(prefix):]
        row = conn.execute("SELECT id FROM coins WHERE sym = ? ORDER BY rank LIMIT 1", (sym,)).fetchone()
        return row[0] if row else None
    except Exception:
        return None
    finally:
        conn.close()


def context_lines(symbol: str, hours: float = 6) -> List[str]:
    """Recent signals on this coin from every service, newest first."""
    coin = coin_id_for(symbol)
    since = now_ms() - int(hours * HOUR_MS)
    lines = []
    try:
        rows = signals_db().execute(
            "SELECT ts, service, kind, title FROM signals WHERE ts >= ? AND kind NOT IN ('pump', 'dump')"
            " AND (coin = ? OR symbol = ?) ORDER BY ts DESC LIMIT 6",
            (since, coin or "", symbol.upper())).fetchall()
        for ts, service, kind, title in rows:
            lines.append("  {} {}: {}".format(ago(ts), kind, title[:100]))
    except Exception as e:
        log("Context read failed: {}".format(e))
    return lines


# ---------------------------------------------------------------------------
# Price spikes
# ---------------------------------------------------------------------------

class Prices:
    """Minute prices per perp for the last two hours."""

    def __init__(self) -> None:
        self.hist = defaultdict(lambda: deque(maxlen=125))  # type: Dict[str, Deque[Tuple[int, float]]]
        self.volume = {}  # type: Dict[str, float]
        self.volume_ms = 0

    def update(self) -> None:
        ts = now_ms()
        for p in http_json(FAPI + "/fapi/v1/ticker/price", timeout=30, user_agent=USER_AGENT) or []:
            if p.get("symbol", "").endswith("USDT"):
                self.hist[p["symbol"]].append((ts, fnum(p.get("price"))))
        if ts - self.volume_ms > 10 * 60 * 1000:
            for t in http_json(FAPI + "/fapi/v1/ticker/24hr", timeout=30, user_agent=USER_AGENT) or []:
                self.volume[t.get("symbol", "")] = fnum(t.get("quoteVolume"))
            self.volume_ms = ts

    def change(self, sym: str, minutes: int) -> Optional[float]:
        h = self.hist.get(sym)
        if not h or len(h) < 2:
            return None
        target = h[-1][0] - minutes * 60 * 1000
        past = [p for t, p in h if t <= target + 30 * 1000]
        if not past or past[-1] <= 0:
            return None
        return (h[-1][1] / past[-1] - 1.0) * 100.0

    def typical_move(self, sym: str, minutes: int) -> Optional[float]:
        """Median absolute move over `minutes`, from the stored history."""
        h = list(self.hist.get(sym) or [])
        step = max(1, minutes)
        moves = sorted(abs(h[i][1] / h[i - step][1] - 1.0) * 100.0
                       for i in range(step, len(h)) if h[i - step][1] > 0)
        if len(moves) < 20:
            return None
        return moves[len(moves) // 2]


def check_spikes(conn, prices: Prices) -> int:
    sent = 0
    btc = prices.change("BTCUSDT", 15)
    if btc is not None and abs(btc) >= CFG["btc_move"] and not on_cooldown(conn, "market", 1):
        with conn:
            outbox_add(conn, "[MARKET MOVE] BTC {:+.1f}% in 15 minutes (now {:,.0f})".format(
                btc, prices.hist["BTCUSDT"][-1][1]))
        record_signal("pulse", "market_move", "BTC {:+.1f}% in 15m".format(btc),
                      direction=1 if btc > 0 else -1, coin="bitcoin")
        sent += 1

    for sym, h in prices.hist.items():
        if sym == "BTCUSDT" or prices.volume.get(sym, 0) < CFG["min_volume"] or len(h) < 6:
            continue
        for minutes in (5, 15, 60):
            move = prices.change(sym, minutes)
            if move is None:
                continue
            typical = prices.typical_move(sym, minutes)
            bar = max(CFG["min_move"][minutes], CFG["sigma_mult"] * typical if typical else 0)
            if abs(move) < bar:
                continue
            up = move > 0
            # One alert per coin and direction every 2 hours, unless the move doubles
            key = "{}:{}".format(sym, "up" if up else "down")
            row = conn.execute("SELECT until, size FROM spikes WHERE key = ?", (key,)).fetchone()
            if row and row[0] > now_ms() and abs(move) < 2 * row[1]:
                break
            with conn:
                conn.execute("INSERT OR REPLACE INTO spikes (key, until, size) VALUES (?, ?, ?)",
                             (key, now_ms() + 2 * HOUR_MS, abs(move)))
            coin = base(sym)
            hour = prices.change(sym, 60) if len(h) >= 60 else None
            lines = ["[{}] {} {:+.1f}% in {} min (price {:,.6g})".format(
                "PUMP 🚀" if up else "DUMP 📉", coin, move, minutes, h[-1][1]),
                "Usual {}-min move: {} | 24h volume {} | 1h {}".format(
                    minutes, "{:.1f}%".format(typical) if typical else "n/a",
                    usd(prices.volume.get(sym, 0)),
                    "{:+.1f}%".format(hour) if hour is not None else "n/a")]
            ctx = context_lines(coin)
            lines.append("What the radar saw on {} in the last 6h:".format(coin) if ctx else
                         "Nothing on {} in the radar in the last 6h: cause unknown.".format(coin))
            lines.extend(ctx)
            with conn:
                outbox_add(conn, "\n".join(lines))
            record_signal("pulse", "pump" if up else "dump", lines[0], direction=1 if up else -1,
                          symbol=coin)
            sent += 1
            break
    return sent


# ---------------------------------------------------------------------------
# Liquidation stream (minimal WebSocket client)
# ---------------------------------------------------------------------------

class LiquidationStream(threading.Thread):
    """Reads Binance's all-market liquidation stream into a list of events.
    Binance sends at most the largest liquidation per coin each second, so
    the totals are lower bounds."""

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.lock = threading.Lock()
        self.events = []  # type: List[Tuple[int, str, str, float, float]]

    def take(self) -> List[Tuple[int, str, str, float, float]]:
        with self.lock:
            out, self.events = self.events, []
        return out

    def run(self) -> None:
        while True:
            try:
                self.listen()
            except Exception as e:
                log("Liquidation stream dropped: {}".format(e))
            time.sleep(10)

    def listen(self) -> None:
        raw = socket.create_connection((LIQ_HOST, 443), timeout=30)
        sock = ssl.create_default_context().wrap_socket(raw, server_hostname=LIQ_HOST)
        key = base64.b64encode(os.urandom(16)).decode()
        sock.sendall(("GET {} HTTP/1.1\r\nHost: {}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                      "Sec-WebSocket-Key: {}\r\nSec-WebSocket-Version: 13\r\nUser-Agent: {}\r\n\r\n").format(
                          LIQ_PATH, LIQ_HOST, key, USER_AGENT).encode())
        response = b""
        while b"\r\n\r\n" not in response:
            chunk = sock.recv(1024)
            if not chunk:
                raise RuntimeError("closed during handshake")
            response += chunk
        if b" 101 " not in response.split(b"\r\n", 1)[0]:
            raise RuntimeError("handshake refused: " + response[:80].decode("latin-1"))
        buffer = response.split(b"\r\n\r\n", 1)[1]
        sock.settimeout(600)  # Binance pings every few minutes
        log("Liquidation stream connected")

        def read(n: int) -> bytes:
            nonlocal buffer
            while len(buffer) < n:
                chunk = sock.recv(65536)
                if not chunk:
                    raise RuntimeError("stream closed")
                buffer += chunk
            out, buffer = buffer[:n], buffer[n:]
            return out

        message = b""
        while True:
            b1, b2 = read(2)
            opcode, length = b1 & 0x0F, b2 & 0x7F
            if length == 126:
                length = struct.unpack(">H", read(2))[0]
            elif length == 127:
                length = struct.unpack(">Q", read(8))[0]
            mask = read(4) if b2 & 0x80 else b""
            payload = read(length)
            if mask:
                payload = bytes(c ^ mask[i % 4] for i, c in enumerate(payload))
            if opcode == 0x9:  # ping: answer with a masked pong
                self.send(sock, 0xA, payload)
            elif opcode == 0x8:
                raise RuntimeError("server closed the stream")
            elif opcode in (0x1, 0x0):
                message += payload
                if b1 & 0x80:
                    self.handle(message)
                    message = b""

    @staticmethod
    def send(sock: ssl.SSLSocket, opcode: int, payload: bytes) -> None:
        mask = os.urandom(4)
        header = bytes([0x80 | opcode, 0x80 | len(payload)]) + mask
        sock.sendall(header + bytes(c ^ mask[i % 4] for i, c in enumerate(payload)))

    def handle(self, message: bytes) -> None:
        try:
            o = json.loads(message.decode("utf-8")).get("o") or {}
        except ValueError:
            return
        notional = fnum(o.get("z")) * fnum(o.get("ap"))
        if notional <= 0:
            return
        # A SELL order liquidates a long position, a BUY order a short one
        side = "LONG" if o.get("S") == "SELL" else "SHORT"
        with self.lock:
            self.events.append((int(o.get("T") or now_ms()), o.get("s", "?"), side, notional, fnum(o.get("ap"))))


class Liquidations:
    def __init__(self) -> None:
        self.recent = deque()  # type: Deque[Tuple[int, str, str, float, float]]

    def check(self, conn, events: List[Tuple[int, str, str, float, float]]) -> int:
        sent = 0
        self.recent.extend(events)
        cutoff = now_ms() - 15 * 60 * 1000
        while self.recent and self.recent[0][0] < cutoff:
            self.recent.popleft()

        for ts, sym, side, notional, price in events:
            if notional >= CFG["liq_single_usd"]:
                with conn:
                    outbox_add(conn, "[BIG LIQUIDATION] {} {} position of {} liquidated on Binance at {:,.6g}".format(
                        base(sym), side, usd(notional), price))
                record_signal("pulse", "liquidation_" + side.lower(), "{} {} {} liquidated".format(
                    base(sym), side, usd(notional)), direction=-1 if side == "LONG" else 1, symbol=base(sym))
                sent += 1

        five = now_ms() - 5 * 60 * 1000
        per = defaultdict(float)  # type: Dict[Tuple[str, str], float]
        market = defaultdict(float)  # type: Dict[str, float]
        for ts, sym, side, notional, _ in self.recent:
            market[side] += notional
            if ts >= five:
                per[(sym, side)] += notional
        for (sym, side), total in per.items():
            if total >= CFG["liq_cascade_usd"] and not on_cooldown(conn, "cascade:{}:{}".format(sym, side), 1):
                ctx = context_lines(base(sym), 3)
                text = "[LIQUIDATION CASCADE] {} {}s: at least {} liquidated in 5 minutes on Binance".format(
                    base(sym), side.lower(), usd(total))
                if ctx:
                    text += "\nRecent radar signals:\n" + "\n".join(ctx)
                with conn:
                    outbox_add(conn, text)
                record_signal("pulse", "cascade_" + side.lower(), text.splitlines()[0],
                              direction=-1 if side == "LONG" else 1, symbol=base(sym))
                sent += 1
        for side, total in market.items():
            if total >= CFG["liq_market_usd"] and not on_cooldown(conn, "market-liq:" + side, 2):
                with conn:
                    outbox_add(conn, "[MARKET LIQUIDATIONS] at least {} of {}s liquidated on Binance in 15 minutes".format(
                        usd(total), side.lower()))
                sent += 1
        return sent


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Market Pulse v1")
    ap.add_argument("--env", default="", help="path to env file with tokens")
    ap.add_argument("--minutes", type=int, default=0, help="run for this many minutes, then exit (testing)")
    args = ap.parse_args()

    load_env_file(args.env)
    load_config()
    telegram = Telegram.from_env(USER_AGENT)
    conn = open_db(CFG["db_path"], SCHEMA)
    prices = Prices()
    stream = LiquidationStream()
    stream.start()
    liquidations = Liquidations()
    log("Running forever, prices every 60 seconds, liquidations live")
    stop_at = time.time() + args.minutes * 60 if args.minutes else None
    while stop_at is None or time.time() < stop_at:
        started = time.time()
        try:
            prices.update()
            sent = check_spikes(conn, prices) + liquidations.check(conn, stream.take())
            outbox_flush(conn, telegram)
            heartbeat(conn)
            if sent:
                log("{} alerts".format(sent))
        except Exception as e:
            log("Unexpected error: {}".format(e))
        time.sleep(max(5.0, 60 - (time.time() - started)))


if __name__ == "__main__":
    main()
