"""
Optional AI reading of news and combined signals, with the official Anthropic SDK.

Turned on only when the `anthropic` package is installed and ANTHROPIC_API_KEY
is set; otherwise every function returns None and the alerts go out without it.

  sudo pip3 install anthropic        # once, on the server
  ANTHROPIC_API_KEY=sk-ant-...       # in /opt/radar/radar.env

Each call returns a short Persian summary and the likely price impact.
AI_MAX_CALLS_PER_DAY caps the daily spend.
"""

import json
import re
import sqlite3
from typing import Dict, Optional

from common import env_int, env_str, log, meta_get, meta_set, today_str

try:
    import anthropic
except ImportError:  # the AI part is optional
    anthropic = None

SYSTEM = (
    "You are a crypto market analyst writing for a Persian-speaking trader. "
    "You get one alert (a news headline, a listing, an unlock, or a set of signals on one coin). "
    "Judge only what the text supports; do not invent facts or prices. "
    "Reply with JSON only, no other text: "
    '{"fa": "<2-3 short sentences in Persian: what happened and why it matters>", '
    '"impact": "bullish" | "bearish" | "neutral", '
    '"coin": "<main ticker affected, or empty>", '
    '"confidence": "low" | "medium" | "high"}'
)

_client = None


def enabled() -> bool:
    return anthropic is not None and bool(env_str("ANTHROPIC_API_KEY"))


def _get_client():
    global _client
    if _client is None:
        _client = anthropic.Anthropic(timeout=60.0, max_retries=2)
    return _client


def _parse(text: str) -> Optional[Dict[str, str]]:
    match = re.search(r"\{.*\}", text, re.S)
    if not match:
        return None
    try:
        data = json.loads(match.group(0))
    except ValueError:
        return None
    if not isinstance(data, dict) or not data.get("fa"):
        return None
    if data.get("impact") not in ("bullish", "bearish", "neutral"):
        data["impact"] = "neutral"
    return {k: str(data.get(k) or "") for k in ("fa", "impact", "coin", "confidence")}


def brief(conn: sqlite3.Connection, alert_text: str) -> Optional[Dict[str, str]]:
    """{fa, impact, coin, confidence} for one alert, or None (off, limit reached, or error)."""
    if not enabled():
        return None
    day_key = "ai_calls_" + today_str()
    used = int(meta_get(conn, day_key) or 0)
    if used >= env_int("AI_MAX_CALLS_PER_DAY", 60):
        return None
    with conn:
        meta_set(conn, day_key, used + 1)
    try:
        response = _get_client().beta.messages.create(
            model=env_str("AI_MODEL", "claude-opus-5-5"),
            max_tokens=1000,
            # A declined request is re-run on Anthropic's recommended fallback model
            betas=["server-side-fallback-2026-07-01"],
            fallbacks="default",
            output_config={"effort": "low"},
            system=SYSTEM,
            messages=[{"role": "user", "content": alert_text[:3000]}],
        )
    except anthropic.APIStatusError as e:
        log("AI request failed: HTTP {} {}".format(e.status_code, str(e)[:200]))
        return None
    except anthropic.APIConnectionError as e:
        log("AI request failed: {}".format(e))
        return None
    if response.stop_reason == "refusal":
        return None
    text = "".join(b.text for b in response.content if b.type == "text")
    return _parse(text)


def format_brief(b: Optional[Dict[str, str]]) -> str:
    """Lines to append to an alert."""
    if not b:
        return ""
    arrow = {"bullish": "🟢 مثبت", "bearish": "🔴 منفی", "neutral": "⚪ خنثی"}[b["impact"]]
    return "\n🤖 {} ({}): {}".format(arrow, b.get("confidence") or "?", b["fa"])


def direction(b: Optional[Dict[str, str]]) -> int:
    if not b:
        return 0
    return {"bullish": 1, "bearish": -1}.get(b["impact"], 0)
