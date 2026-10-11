"""
Shared helpers for the radar scripts: config, logging, formatting, HTTP,
Telegram delivery through a SQLite outbox, and small key/value state.

Standard library only. Compatible with Python 3.8+.
"""

import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

HOUR_MS = 3600 * 1000
DAY_MS = 24 * HOUR_MS

RETRY_HTTP_CODES = (429, 500, 502, 503, 504)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def parse_env_value(raw: str) -> str:
    value = raw.strip()
    if value[:1] in ("'", '"'):
        end = value.find(value[0], 1)
        if end != -1:
            return value[1:end]
    # Unquoted value: " #" starts a comment
    pos = value.find(" #")
    if pos != -1:
        value = value[:pos]
    return value.strip()


def load_env_file(path: str) -> None:
    if not path:
        return
    if not os.path.exists(path):
        sys.exit("Env file not found: " + path)
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if key.startswith("export "):
                key = key[len("export "):].strip()
            os.environ[key] = parse_env_value(value)


def env_str(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


def env_int(name: str, default: int) -> int:
    try:
        return int(float(os.environ.get(name, default)))
    except ValueError:
        return default


# ---------------------------------------------------------------------------
# Time and logging
# ---------------------------------------------------------------------------

def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print("[{}] {}".format(ts, msg), flush=True)


def now_ms() -> int:
    return int(time.time() * 1000)


def today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def ago(ms: Optional[int]) -> str:
    """Human age of a millisecond timestamp, e.g. '5m ago'."""
    if not ms:
        return "never"
    secs = max(0, (now_ms() - int(ms)) // 1000)
    if secs < 3600:
        return "{}m ago".format(secs // 60)
    if secs < 2 * 86400:
        return "{}h ago".format(secs // 3600)
    return "{}d ago".format(secs // 86400)


# ---------------------------------------------------------------------------
# Numbers and formatting
# ---------------------------------------------------------------------------

def fnum(x: Any, default: float = 0.0) -> float:
    try:
        f = float(x)
    except (TypeError, ValueError):
        return default
    return default if f != f else f  # NaN


def usd(x: float, signed: bool = False) -> str:
    sign = ""
    if signed:
        sign = "+" if x >= 0 else "-"
    x = abs(x)
    if x >= 1e9:
        body = "${:.1f}B".format(x / 1e9)
    elif x >= 1e6:
        body = "${:.1f}M".format(x / 1e6)
    elif x >= 1e3:
        body = "${:.0f}K".format(x / 1e3)
    else:
        body = "${:.0f}".format(x)
    return sign + body


def px(x: float) -> str:
    if x >= 1000:
        return "{:,.1f}".format(x)
    if x >= 1:
        return "{:.3f}".format(x)
    return "{:.6g}".format(x)


def pct(x: float) -> str:
    return "{:+.1f}%".format(x)


def short_addr(a: str) -> str:
    return a[:6] + "..." + a[-4:]


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

class HttpError(RuntimeError):
    def __init__(self, code: int, url: str, detail: str = "") -> None:
        super().__init__("HTTP {}{} for {}".format(code, " " + detail if detail else "", url))
        self.code = code


class NotFound(HttpError):
    pass


def lower_headers(headers: Any) -> Dict[str, str]:
    return {k.lower(): v for k, v in headers.items()}


def http_request(url: str, body: Any = None, headers: Optional[Dict[str, str]] = None,
                 timeout: int = 60, attempts: int = 3,
                 user_agent: str = "radar/2.0") -> Tuple[Any, Dict[str, str]]:
    """
    Sends a GET (or a JSON POST when body is given) and returns
    (parsed_json, response_headers). Header names are lower-case.
    A 304 returns (None, headers).
    Retries 429, 5xx, timeouts and connection errors; other errors raise HttpError.
    """
    h = {"User-Agent": user_agent, "Accept": "application/json"}
    if headers:
        h.update(headers)
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        h["Content-Type"] = "application/json"

    last_error = RuntimeError("no attempt made for " + url)  # type: Exception
    for attempt in range(attempts):
        req = urllib.request.Request(url, data=data, headers=h)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8")), lower_headers(resp.headers)
        except urllib.error.HTTPError as e:
            if e.code == 304:
                return None, lower_headers(e.headers)
            if e.code == 404:
                raise NotFound(404, url)
            remaining = e.headers.get("X-RateLimit-Remaining")
            if e.code == 403 and remaining == "0":
                raise HttpError(403, url, "rate limit, resets at {}".format(
                    e.headers.get("X-RateLimit-Reset", "?")))
            if e.code not in RETRY_HTTP_CODES:
                raise HttpError(e.code, url)
            last_error = HttpError(e.code, url)
            wait = 5.0 * (attempt + 1)
            if e.code == 429:
                wait = min(fnum(e.headers.get("Retry-After"), 15.0 * (attempt + 1)), 120.0)
        except (urllib.error.URLError, OSError, ValueError) as e:
            # Connection errors, timeouts and broken JSON are worth a retry
            last_error = RuntimeError("{} for {}".format(e, url))
            wait = 5.0 * (attempt + 1)
        if attempt < attempts - 1:
            time.sleep(wait)
    raise last_error


def http_json(url: str, body: Any = None, headers: Optional[Dict[str, str]] = None,
              timeout: int = 60, attempts: int = 3, user_agent: str = "radar/2.0") -> Any:
    return http_request(url, body=body, headers=headers, timeout=timeout,
                        attempts=attempts, user_agent=user_agent)[0]


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------

class Telegram:
    def __init__(self, token: str, chat_id: str, user_agent: str = "radar/2.0") -> None:
        self.token = token
        self.chat_id = chat_id
        self.user_agent = user_agent

    @classmethod
    def from_env(cls, user_agent: str = "radar/2.0") -> "Telegram":
        return cls(env_str("TELEGRAM_BOT_TOKEN"), env_str("TELEGRAM_CHAT_ID"), user_agent)

    @property
    def configured(self) -> bool:
        return bool(self.token and self.chat_id)

    @property
    def delay(self) -> float:
        # Telegram allows about 1 message/second per chat, 20/minute in groups
        return 3.1 if self.chat_id.startswith("-") else 1.1

    def send(self, text: str) -> bool:
        if not self.configured:
            log("Telegram not configured. Message would be:\n" + text)
            return False
        url = "https://api.telegram.org/bot{}/sendMessage".format(self.token)
        payload = json.dumps({
            "chat_id": self.chat_id,
            "text": text[:4000],
            "disable_web_page_preview": True,
        }).encode("utf-8")
        req = urllib.request.Request(url, data=payload, headers={
            "Content-Type": "application/json", "User-Agent": self.user_agent})
        for attempt in range(3):
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    resp.read()
                return True
            except urllib.error.HTTPError as e:
                detail = e.read().decode("utf-8", "replace")
                if e.code == 429:
                    try:
                        wait = int(json.loads(detail)["parameters"]["retry_after"])
                    except (ValueError, KeyError, TypeError):
                        wait = 30
                    log("Telegram rate limit, waiting {} seconds".format(wait))
                    time.sleep(min(wait, 300) + 1)
                    continue
                # The URL holds the bot token, so only the status and body are logged
                log("Telegram send failed: HTTP {} {}".format(e.code, detail[:200]))
                return False
            except Exception as e:
                log("Telegram send failed: {}".format(e))
                time.sleep(5 * (attempt + 1))
        return False


# ---------------------------------------------------------------------------
# SQLite: connection, key/value state, outbox
# ---------------------------------------------------------------------------

BASE_SCHEMA = """
    CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT);
    CREATE TABLE IF NOT EXISTS outbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_ms INTEGER NOT NULL,
        text TEXT NOT NULL,
        sent_ms INTEGER);
    CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox (sent_ms);
"""


def open_db(path: str, schema: str = "") -> sqlite3.Connection:
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    # WAL lets the other scripts read this database while it is being written
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(BASE_SCHEMA + schema)
    conn.commit()
    return conn


def open_db_readonly(path: str) -> Optional[sqlite3.Connection]:
    if not os.path.exists(path):
        return None
    try:
        return sqlite3.connect("file:{}?mode=ro".format(path), uri=True, timeout=10)
    except sqlite3.Error:
        return None


# meta_set and outbox_add do not commit, so callers can group them with
# their own writes in "with conn:" (all or nothing).

def meta_get(conn: sqlite3.Connection, key: str, default: Optional[str] = None) -> Optional[str]:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row[0] if row else default


def meta_set(conn: sqlite3.Connection, key: str, value: Any) -> None:
    conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", (key, str(value)))


def heartbeat(conn: sqlite3.Connection) -> None:
    """Records a finished cycle; read by the daily status report in radar.py."""
    with conn:
        meta_set(conn, "heartbeat_ms", now_ms())


def daily_due(conn: sqlite3.Connection, key: str, hour_utc: int) -> bool:
    """True once per UTC day, at or after hour_utc. Marks the day as done."""
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    if now.hour < hour_utc or meta_get(conn, key) == today:
        return False
    with conn:
        meta_set(conn, key, today)
    return True


def outbox_add(conn: sqlite3.Connection, text: str) -> None:
    conn.execute("INSERT INTO outbox (created_ms, text) VALUES (?, ?)", (now_ms(), text))


def outbox_flush(conn: sqlite3.Connection, telegram: Telegram) -> int:
    """
    Sends queued messages in order. A message is marked as sent only after
    Telegram accepts it; on failure the rest stay queued for the next cycle.
    """
    rows = conn.execute(
        "SELECT id, text FROM outbox WHERE sent_ms IS NULL ORDER BY id").fetchall()
    if not rows:
        return 0
    if not telegram.configured:
        for _, text in rows:
            log("Telegram not configured. Queued message:\n" + text)
        log("{} messages stay queued until Telegram is configured".format(len(rows)))
        return 0
    sent = 0
    for msg_id, text in rows:
        if not telegram.send(text):
            log("{} messages left in queue, retrying next cycle".format(len(rows) - sent))
            break
        with conn:
            conn.execute("UPDATE outbox SET sent_ms = ? WHERE id = ?", (now_ms(), msg_id))
        sent += 1
        time.sleep(telegram.delay)
    # Keep 30 days of sent messages for the status report
    with conn:
        conn.execute("DELETE FROM outbox WHERE sent_ms IS NOT NULL AND sent_ms < ?",
                     (now_ms() - 30 * DAY_MS,))
    return sent


def outbox_count_since(conn: sqlite3.Connection, since_ms: int) -> int:
    row = conn.execute("SELECT COUNT(*) FROM outbox WHERE created_ms >= ?", (since_ms,)).fetchone()
    return int(row[0])
