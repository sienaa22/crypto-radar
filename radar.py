#!/usr/bin/env python3
"""
Project Radar v2

Watches public sources for early signs of new crypto projects and sends
Telegram alerts. Also sends one daily status report for all radar services.

Sources:
  - New project folders in DefiLlama/DefiLlama-Adapters PRs (TVL adapters)
  - New project folders in DefiLlama/dimension-adapters PRs (fees/volume adapters)
  - New audit contest repos: Code4rena, Sherlock, CodeHawks, Hats Finance (GitHub)
  - New Cantina competitions and Immunefi programs (public JSON APIs)

GitHub list requests use ETags: an unchanged answer (HTTP 304) does not count
against the rate limit, so checks can run often.

Standard library only. Compatible with Python 3.8+.

Usage:
  python3 radar.py --env /opt/radar/radar.env                 # run forever
  python3 radar.py --env /opt/radar/radar.env --once          # one check, then exit
  python3 radar.py --env /opt/radar/radar.env --status        # send the status report now
  python3 radar.py --env /opt/radar/radar.env --test-telegram # send a test message
  python3 radar.py --env /opt/radar/radar.env --export alerts.csv
"""

import argparse
import csv
import json
import re
import sqlite3
import sys
import time
import urllib.parse
from collections import namedtuple
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from common import (
    DAY_MS, NotFound, Telegram, ago, daily_due, env_int, env_str, heartbeat,
    http_json, http_request, load_env_file, log, meta_get, meta_set, now_ms,
    open_db, open_db_readonly, outbox_add, outbox_count_since, outbox_flush, today_str,
)

USER_AGENT = "project-radar/2.0 (personal research script)"

# (source, repo, label, top-level folders that hold one entry per project)
DEFILLAMA_REPOS = [
    ("defillama_tvl", "DefiLlama/DefiLlama-Adapters", "TVL adapter", ("projects",)),
    ("defillama_dims", "DefiLlama/dimension-adapters", "fees/volume adapter",
     ("fees", "dexs", "aggregators", "aggregator-derivatives", "aggregator-options",
      "bridge-aggregators", "bridges", "options", "open-interest")),
]
SEEN_PROJECTS = "defillama"         # shared by both repos: one alert per project
LEGACY_PR_SOURCE = "defillama_pr"   # v1 stored seen PR numbers here
IGNORED_ENTRY_NAMES = {"helper", "helpers", "config", "utils", "index", "types", "test"}

# GitHub orgs whose contest repos are named "YYYY-MM-project"
AUDIT_ORGS = [
    ("code-423n4", "Code4rena"),
    ("sherlock-audit", "Sherlock"),
    ("CodeHawks-Contests", "CodeHawks"),
]
HATS_ORG = "hats-finance"
CANTINA_URL = "https://cantina.xyz/api/v0/competitions"
IMMUNEFI_URL = "https://immunefi.com/public-api/bounties.json"
LLAMA_PROTOCOLS_URL = "https://api.llama.fi/protocols"
LLAMA_CACHE_SECONDS = 6 * 3600

PR_MAX_PAGES = 10       # 100 PRs per page
REPO_MAX_PAGES = 3      # 100 repos per page
FILES_MAX_PAGES = 30    # GitHub returns at most 3000 files per PR
MAX_PR_FAILURES = 3     # give up on a PR after this many failed reads
HEALTH_ALERT_AFTER = 3  # warn after this many failed checks in a row

# Side repos that belong to the same contest; stripped so one contest = one alert
AUDIT_SUFFIXES = (
    "-findings", "-judging", "-validation", "-bot-reports",
    "-escalations", "-review", "-mitigation",
)

# Not part of a longer hex string, so 32-byte hashes are not taken as addresses
ADDRESS_RE = re.compile(r"(?<![0-9A-Za-z_])0x[0-9a-fA-F]{40}(?![0-9A-Za-z_])")
ZERO_ADDRESS = "0x" + "0" * 40
# Chain guesses, matched against added lines only:
#   base: {            chain: 'base'            chains = ['base', 'arbitrum']
CHAIN_KEY_RE = re.compile(r"^\s*['\"]?([a-z][a-z0-9_]{1,30})['\"]?\s*:\s*\{", re.MULTILINE)
CHAIN_VALUE_RE = re.compile(r"\bchain\s*[:=]\s*['\"`]([a-z][a-z0-9_]{1,30})['\"`]")
CHAIN_LIST_RE = re.compile(r"\bchains\s*[:=]\s*\[([^\]]*)\]")
QUOTED_WORD_RE = re.compile(r"['\"`]([a-z][a-z0-9_]{1,30})['\"`]")
NOT_CHAINS = {
    "tvl", "staking", "pool2", "borrowed", "module", "exports", "config",
    "hallmarks", "methodology", "misrepresentedtokens", "timetravel",
    "start", "options", "params", "headers", "body", "query", "data",
    "tokens", "token", "pools", "pool", "contracts", "contract", "addresses",
    "address", "abi", "abis", "methods", "vaults", "vault", "markets", "market",
    "factory", "factories", "owners", "owner", "target", "targets", "calls",
    "call", "chain", "chains", "fees", "default", "meta", "info", "result",
    "results", "response", "res", "balances", "lps", "lp", "farms", "api",
    "doublecounted", "deadfrom", "owntokens", "blacklistedtokens",
    "permitfailure", "isheavyprotocol", "fetch", "variables", "where",
    "adapter", "breakdown", "dailyfees", "dailyrevenue", "dailyvolume", "version",
}
DATE_PREFIX_RE = re.compile(r"^\d{4}-\d{2}-")
LINK_NEXT_RE = re.compile(r'<([^>]+)>\s*;\s*rel="next"')

SCHEMA = """
    CREATE TABLE IF NOT EXISTS seen (
        source TEXT NOT NULL,
        item_key TEXT NOT NULL,
        first_seen TEXT NOT NULL,
        PRIMARY KEY (source, item_key));
    CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sent_at TEXT NOT NULL,
        source TEXT NOT NULL,
        project TEXT NOT NULL,
        url TEXT NOT NULL,
        details TEXT);
    CREATE TABLE IF NOT EXISTS http_cache (
        url TEXT PRIMARY KEY,
        etag TEXT NOT NULL,
        next_url TEXT,
        body TEXT NOT NULL);
"""

# An alert waiting to be queued: unique key within its source, the message, and
# when the item was created (ISO text from the source, "" if unknown)
Item = namedtuple("Item", "key project link text created")
# Listing items older than this are recorded without an alert: an old contest
# or program that shows up late (e.g. after a paging change) is not news
MAX_ITEM_AGE_DAYS = 30
STARTED = "__started__"  # recorded on a source's first run, even when it had no items


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def github_stamp() -> str:
    """Current time in the format GitHub uses, so the two compare as strings."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", s.lower())


def short_list(items: List[str], limit: int) -> str:
    if not items:
        return "none found"
    shown = items[:limit]
    text = ", ".join(shown)
    if len(items) > limit:
        text += " (+{} more)".format(len(items) - limit)
    return text


def csv_safe(value: Any) -> str:
    """Stops spreadsheet apps from running cell text as a formula."""
    text = "" if value is None else str(value)
    if text[:1] in ("=", "+", "-", "@", "\t", "\r"):
        return "'" + text
    return text


def money_text(amount: Any, currency: str = "") -> str:
    try:
        value = float(amount)
    except (TypeError, ValueError):
        return "unknown"
    if value <= 0:
        return "unknown"
    text = "${:,.0f}".format(value)
    if currency and currency.upper() not in ("USD", "USDC"):
        text = "{:,.0f} {}".format(value, currency)
    return text


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def is_seen(conn: sqlite3.Connection, source: str, key: str) -> bool:
    return conn.execute("SELECT 1 FROM seen WHERE source = ? AND item_key = ?",
                        (source, key)).fetchone() is not None


def source_is_empty(conn: sqlite3.Connection, source: str) -> bool:
    return conn.execute("SELECT 1 FROM seen WHERE source = ? LIMIT 1",
                        (source,)).fetchone() is None


def mark_seen(conn: sqlite3.Connection, source: str, key: str) -> None:
    conn.execute("INSERT OR IGNORE INTO seen (source, item_key, first_seen) VALUES (?, ?, ?)",
                 (source, key, now_iso()))


def queue_alert(conn: sqlite3.Connection, source: str, item: Item) -> None:
    """Marks the item as seen and queues its message, both or neither."""
    with conn:
        mark_seen(conn, source, item.key)
        conn.execute("INSERT INTO alerts (sent_at, source, project, url, details)"
                     " VALUES (?, ?, ?, ?, ?)",
                     (now_iso(), source, item.project, item.link, item.text))
        outbox_add(conn, item.text)


def export_csv(conn: sqlite3.Connection, path: str) -> None:
    rows = conn.execute("SELECT sent_at, source, project, url FROM alerts ORDER BY id").fetchall()
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        # Empty columns are for manual labeling in a spreadsheet
        w.writerow(["sent_at_utc", "source", "project", "url", "label", "next_event", "event_date"])
        for r in rows:
            w.writerow([csv_safe(v) for v in r] + ["", "", ""])
    log("Exported {} alerts to {}".format(len(rows), path))


# ---------------------------------------------------------------------------
# GitHub client with ETag cache
# ---------------------------------------------------------------------------

class GitHub:
    def __init__(self, conn: sqlite3.Connection, token: str) -> None:
        self.conn = conn
        self.token = token
        self.rate_remaining = "?"
        self.requests = 0
        self.cache_hits = 0

    def get(self, url: str, cache: bool = False) -> Tuple[Any, Optional[str]]:
        """Returns (data, next_page_url). With cache=True, unchanged pages cost nothing."""
        headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        cached = None
        if cache:
            cached = self.conn.execute(
                "SELECT etag, next_url, body FROM http_cache WHERE url = ?", (url,)).fetchone()
            if cached:
                headers["If-None-Match"] = cached[0]

        data, resp_headers = http_request(url, headers=headers, timeout=30, user_agent=USER_AGENT)
        self.requests += 1
        self.rate_remaining = resp_headers.get("x-ratelimit-remaining", self.rate_remaining)
        if data is None and cached:
            self.cache_hits += 1
            return json.loads(cached[2]), cached[1]

        match = LINK_NEXT_RE.search(resp_headers.get("link") or "")
        next_url = match.group(1) if match else None
        etag = resp_headers.get("etag")
        if cache and etag:
            with self.conn:
                self.conn.execute(
                    "INSERT OR REPLACE INTO http_cache (url, etag, next_url, body) VALUES (?, ?, ?, ?)",
                    (url, etag, next_url, json.dumps(data)))
        return data, next_url

    def get_all(self, url: str, max_pages: int) -> List[dict]:
        items = []  # type: List[dict]
        next_url = url  # type: Optional[str]
        for _ in range(max_pages):
            if not next_url:
                break
            data, next_url = self.get(next_url)
            items.extend(data)
        return items

    def path_exists(self, repo: str, path: str, ref: Optional[str]) -> Optional[bool]:
        """True if the path exists in repo at ref. None if the check failed."""
        url = "https://api.github.com/repos/{}/contents/{}".format(repo, urllib.parse.quote(path))
        if ref:
            url += "?ref=" + urllib.parse.quote(ref, safe="")
        try:
            self.get(url)
            return True
        except NotFound:
            return False
        except Exception as e:
            log("Contents check failed for {}/{}: {}".format(repo, path, e))
            return None


# ---------------------------------------------------------------------------
# DefiLlama protocol list (to skip projects that are already listed)
# ---------------------------------------------------------------------------

LLAMA_CACHE = {"names": None, "loaded_at": 0.0}  # type: Dict[str, Any]


def llama_names() -> Optional[Set[str]]:
    """Normalized names and slugs of listed protocols. Loaded when needed, cached."""
    cached = LLAMA_CACHE["names"]
    if cached is not None and time.time() - LLAMA_CACHE["loaded_at"] < LLAMA_CACHE_SECONDS:
        return cached
    try:
        data = http_json(LLAMA_PROTOCOLS_URL, timeout=90, user_agent=USER_AGENT)
    except Exception as e:
        log("Could not load DefiLlama protocol list: {}".format(e))
        return cached  # a stale list is better than none
    names = set()  # type: Set[str]
    for p in data:
        for field in ("slug", "name"):
            v = p.get(field)
            if isinstance(v, str) and v:
                names.add(norm(v))
    LLAMA_CACHE.update(names=names, loaded_at=time.time())
    log("Loaded {} DefiLlama protocol names".format(len(names)))
    return names


# ---------------------------------------------------------------------------
# Sources: DefiLlama adapter pull requests
# ---------------------------------------------------------------------------

def new_project_entries(files: List[dict], roots: Tuple[str, ...]) -> Dict[str, str]:
    """
    Returns {project_name: repo_path} for project entries added in a PR.
    Handles both <root>/<name>/index.js and <root>/<name>.js layouts.
    """
    found = {}  # type: Dict[str, str]
    for f in files:
        if f.get("status") != "added":
            continue
        parts = (f.get("filename") or "").split("/")
        if len(parts) < 2 or parts[0] not in roots:
            continue
        if len(parts) >= 3:
            name = parts[1]
            path = parts[0] + "/" + name
        elif parts[1].endswith((".js", ".ts")):
            name = parts[1].rsplit(".", 1)[0]
            path = parts[0] + "/" + parts[1]
        else:
            continue
        if name.lower() in IGNORED_ENTRY_NAMES or name.startswith(".") or not norm(name):
            continue
        found[name] = path
    return found


def added_text(files: List[dict]) -> str:
    """Lines added by the given PR files, without the leading '+'."""
    lines = []  # type: List[str]
    for f in files:
        for line in (f.get("patch") or "").splitlines():
            if line.startswith("+"):
                lines.append(line[1:])
    return "\n".join(lines)


def guess_chains(text: str) -> List[str]:
    found = set(CHAIN_KEY_RE.findall(text)) | set(CHAIN_VALUE_RE.findall(text))
    for group in CHAIN_LIST_RE.findall(text):
        found.update(QUOTED_WORD_RE.findall(group))
    return sorted(c for c in found if c not in NOT_CHAINS)


def files_under(files: List[dict], path: str) -> List[dict]:
    return [f for f in files
            if f.get("filename") == path or (f.get("filename") or "").startswith(path + "/")]


def process_pr(conn: sqlite3.Connection, gh: GitHub, source: str, repo: str, label: str,
               roots: Tuple[str, ...], pr: dict) -> int:
    """Queues alerts for new projects in one PR. Returns how many were queued."""
    number = pr.get("number")
    files = gh.get_all("https://api.github.com/repos/{}/pulls/{}/files?per_page=100".format(
        repo, number), FILES_MAX_PAGES)
    base_sha = (pr.get("base") or {}).get("sha")
    link = pr.get("html_url", "")
    queued = 0

    for name, path in sorted(new_project_entries(files, roots).items()):
        key = norm(name)
        if is_seen(conn, SEEN_PROJECTS, key):
            continue
        # Compare with the repo as it was before this PR, so a PR that was
        # merged between two checks still counts as a new project
        if gh.path_exists(repo, path, base_sha) is True:
            continue
        listed = llama_names()
        if listed is not None and key in listed:
            continue  # already on DefiLlama under the same name

        own_text = added_text(files_under(files, path))
        addresses = sorted(set(a.lower() for a in ADDRESS_RE.findall(own_text)) - {ZERO_ADDRESS})
        text = (
            "[NEW PROJECT] DefiLlama {label} PR\n"
            "Project: {name}\n"
            "Type: {kind}\n"
            "PR #{num}: {title}\n"
            "PR state: {state}\n"
            "On DefiLlama: {listed}\n"
            "Chains (guess): {chains}\n"
            "Contracts: {addrs}\n"
            "Link: {link}"
        ).format(
            label=label, name=name, kind=path.split("/", 1)[0], num=number,
            title=(pr.get("title") or "")[:120],
            state="merged" if pr.get("merged_at") else pr.get("state", "?"),
            listed="unknown (list not loaded)" if listed is None else "no",
            chains=short_list(guess_chains(own_text), 6),
            addrs=short_list(addresses, 4), link=link)
        # Seen under the shared project namespace, logged under this repo's source
        with conn:
            mark_seen(conn, SEEN_PROJECTS, key)
            conn.execute("INSERT INTO alerts (sent_at, source, project, url, details)"
                         " VALUES (?, ?, ?, ?, ?)", (now_iso(), source, name, link, text))
            outbox_add(conn, text)
        queued += 1
    return queued


def check_defillama_repo(conn: sqlite3.Connection, gh: GitHub, source: str, repo: str,
                         label: str, roots: Tuple[str, ...]) -> Tuple[int, int]:
    """
    Reads PRs by last update, so a PR that adds its project files after it was
    opened is still caught. Returns (alerts_queued, items_bootstrapped).
    """
    wm_key, start_key = source + ".watermark", source + ".started_at"
    watermark = meta_get(conn, wm_key)
    started_at = meta_get(conn, start_key)
    url = ("https://api.github.com/repos/{}/pulls"
           "?state=all&sort=updated&direction=desc&per_page=100").format(repo)

    if watermark is None or started_at is None:
        upgrading = source == "defillama_tvl" and not source_is_empty(conn, LEGACY_PR_SOURCE)
        prs = [] if upgrading else gh.get(url, cache=True)[0]
        stamp = github_stamp()
        newest = max((pr.get("updated_at") or "" for pr in prs), default="")
        with conn:
            meta_set(conn, wm_key, newest or stamp)
            meta_set(conn, start_key, stamp)
        if upgrading:
            log("Upgraded {} from v1, watching PRs from now on".format(repo))
            return 0, 0
        # PRs created before this moment are treated as already known
        return 0, len(prs)

    candidates = []  # type: List[dict]
    newest = watermark
    next_url = url  # type: Optional[str]
    reached_end = False
    for page in range(PR_MAX_PAGES):
        prs, next_url = gh.get(next_url, cache=(page == 0))
        for pr in prs:
            updated = pr.get("updated_at") or ""
            if updated < watermark:
                reached_end = True
                break
            newest = max(newest, updated)
            if (pr.get("created_at") or "") >= started_at:
                candidates.append(pr)
        if reached_end or not next_url:
            reached_end = True
            break
    if not reached_end:
        log("Warning: more than {} {} PRs updated since last check, older ones skipped".format(
            PR_MAX_PAGES * 100, repo))

    # The newest PR equals the watermark on every quiet check; it was handled already
    handled = meta_get(conn, source + ".handled", "")
    queued = 0
    retry_from = None  # type: Optional[str]
    for pr in reversed(candidates):  # oldest first, so alerts arrive in order
        version = "{}@{}".format(pr.get("number"), pr.get("updated_at"))
        if version == handled:
            continue
        try:
            queued += process_pr(conn, gh, source, repo, label, roots, pr)
            handled = version
        except Exception as e:
            fail_key = "{}.failures.{}".format(source, pr.get("number"))
            failures = int(meta_get(conn, fail_key) or 0) + 1
            with conn:
                meta_set(conn, fail_key, failures)
            if failures >= MAX_PR_FAILURES:
                log("Giving up on {} PR #{} after {} failures: {}".format(
                    repo, pr.get("number"), failures, e))
                continue
            log("Could not read {} PR #{} (retrying next check): {}".format(
                repo, pr.get("number"), e))
            updated = pr.get("updated_at") or ""
            if retry_from is None or updated < retry_from:
                retry_from = updated

    with conn:
        # After a failure, start the next check from the failed PR again.
        # Already alerted projects are skipped by the seen table.
        meta_set(conn, wm_key, retry_from or newest)
        meta_set(conn, source + ".handled", handled)
    return queued, 0


# ---------------------------------------------------------------------------
# Sources: listings (audit contests, competitions, bounty programs)
# ---------------------------------------------------------------------------

def is_recent(created: str) -> bool:
    """True if an ISO date/time is within MAX_ITEM_AGE_DAYS, or unknown."""
    try:
        when = datetime.fromisoformat(created[:19].replace("Z", "")).replace(tzinfo=timezone.utc)
    except ValueError:
        return True
    return (datetime.now(timezone.utc) - when).days <= MAX_ITEM_AGE_DAYS


def check_listing(conn: sqlite3.Connection, source: str, items: List[Item]) -> Tuple[int, int]:
    """
    Queues an alert for every recent item not seen before. The first run of a
    source only records what exists. Items should be ordered oldest first.
    """
    new_items = [i for i in items if not is_seen(conn, source, i.key)]
    if source_is_empty(conn, source):
        with conn:
            # The marker ends the first run even when the source had no items yet
            mark_seen(conn, source, STARTED)
            for item in new_items:
                mark_seen(conn, source, item.key)
        return 0, len(new_items)
    old = [i for i in new_items if not is_recent(i.created)]
    if old:
        log("{}: {} old items recorded without alert".format(source, len(old)))
        with conn:
            for item in old:
                mark_seen(conn, source, item.key)
        new_items = [i for i in new_items if i not in old]
    for item in new_items:
        queue_alert(conn, source, item)
    return len(new_items), 0


def org_repos(conn: sqlite3.Connection, gh: GitHub, source: str, org: str,
              key_of: Callable[[str], str]) -> List[dict]:
    """Newest repos of an org, paging until it reaches contests already seen."""
    bootstrap = source_is_empty(conn, source)
    next_url = ("https://api.github.com/orgs/{}/repos"
                "?sort=created&direction=desc&per_page=100").format(org)  # type: Optional[str]
    repos = []  # type: List[dict]
    for page in range(1 if bootstrap else REPO_MAX_PAGES):
        data, next_url = gh.get(next_url, cache=(page == 0))
        repos.extend(data)
        if not next_url or any(is_seen(conn, source, key_of(r.get("name", ""))) for r in data):
            break
    return repos


def contest_key(repo_name: str) -> str:
    name = repo_name.lower()
    changed = True
    while changed:
        changed = False
        for suf in AUDIT_SUFFIXES:
            if name.endswith(suf):
                name = name[: -len(suf)]
                changed = True
    return name


def repo_created(r: dict) -> str:
    return (r.get("created_at") or "unknown").replace("T", " ").replace("Z", " UTC")


def check_audit_org(conn: sqlite3.Connection, gh: GitHub, org: str, label: str) -> Tuple[int, int]:
    source = "audit_" + org
    repos = org_repos(conn, gh, source, org, contest_key)

    # Group side repos (-judging, -findings, ...) with their main contest repo
    groups = {}  # type: Dict[str, dict]
    order = []  # type: List[str]
    for r in repos:
        name = r.get("name", "")
        key = contest_key(name)
        if not DATE_PREFIX_RE.match(key):
            continue  # tooling or template repo, not a contest
        if key not in groups:
            groups[key] = r
            order.append(key)
        elif name.lower() == key:
            groups[key] = r  # prefer the main repo over side repos

    items = []  # type: List[Item]
    for key in reversed(order):  # oldest first
        r = groups[key]
        project = DATE_PREFIX_RE.sub("", key)
        link = r.get("html_url") or "https://github.com/{}/{}".format(org, r.get("name", ""))
        text = (
            "[NEW AUDIT] {label}\n"
            "Project: {project}\n"
            "Repo: {org}/{name}\n"
            "Created: {created}\n"
            "Description: {desc}\n"
            "Link: {link}"
        ).format(label=label, project=project, org=org, name=r.get("name", ""),
                 created=repo_created(r), desc=(r.get("description") or "none")[:200], link=link)
        items.append(Item(key, project, link, text, r.get("created_at") or ""))
    return check_listing(conn, source, items)


def hats_key(repo_name: str) -> str:
    return repo_name.lower()


def check_hats(conn: sqlite3.Connection, gh: GitHub) -> Tuple[int, int]:
    """Hats Finance repos are named "<Project>-0x<vault address>"."""
    source = "audit_" + HATS_ORG
    items = []  # type: List[Item]
    for r in reversed(org_repos(conn, gh, source, HATS_ORG, hats_key)):
        name = r.get("name", "")
        if "-0x" not in name or "test" in name.lower():
            continue
        project = name.split("-0x", 1)[0].replace("-", " ")
        link = r.get("html_url") or "https://github.com/{}/{}".format(HATS_ORG, name)
        text = (
            "[NEW AUDIT] Hats Finance\n"
            "Project: {project}\n"
            "Repo: {org}/{name}\n"
            "Created: {created}\n"
            "Link: {link}"
        ).format(project=project, org=HATS_ORG, name=name, created=repo_created(r), link=link)
        items.append(Item(hats_key(name), project, link, text, r.get("created_at") or ""))
    return check_listing(conn, source, items)


def check_cantina(conn: sqlite3.Connection, gh: GitHub) -> Tuple[int, int]:
    data = http_json(CANTINA_URL, timeout=60, user_agent=USER_AGENT)
    items = []  # type: List[Item]
    for c in sorted(data or [], key=lambda c: c.get("createdAt") or ""):
        if not c.get("id"):
            continue
        company = (c.get("company") or {}).get("name") or c.get("name") or "?"
        link = c.get("url") or "https://cantina.xyz/competitions/" + c["id"]
        text = (
            "[NEW AUDIT] Cantina competition\n"
            "Project: {company}\n"
            "Competition: {name}\n"
            "Prize pool: {pot}\n"
            "Status: {status} ({kind})\n"
            "Created: {created}\n"
            "Link: {link}"
        ).format(company=company, name=c.get("name") or "?",
                 pot=money_text(c.get("totalRewardPot"), c.get("currencyCode") or ""),
                 status=c.get("status") or "?", kind=(c.get("kind") or "?").replace("_", " "),
                 created=(c.get("createdAt") or "?")[:10], link=link)
        items.append(Item(c["id"], company, link, text, c.get("createdAt") or ""))
    return check_listing(conn, "cantina", items)


def immunefi_link(slug: str) -> str:
    if slug.startswith("audit-competition"):
        return "https://immunefi.com/audit-competition/{}/".format(slug)
    return "https://immunefi.com/bug-bounty/{}/".format(slug)


def check_immunefi(conn: sqlite3.Connection, gh: GitHub) -> Tuple[int, int]:
    data = http_json(IMMUNEFI_URL, timeout=120, user_agent=USER_AGENT)
    items = []  # type: List[Item]
    for p in sorted(data or [], key=lambda p: p.get("launchDate") or ""):
        slug = p.get("slug")
        if not slug:
            continue
        kind = "Audit competition" if slug.startswith("audit-competition") else "Bug bounty"
        project = p.get("project") or slug
        text = (
            "[NEW PROGRAM] Immunefi {kind}\n"
            "Project: {project}\n"
            "Max bounty: {bounty}\n"
            "Ecosystem: {eco}\n"
            "Scope: {scope}\n"
            "Launch: {launch}\n"
            "Link: {link}"
        ).format(kind=kind.lower(), project=project,
                 bounty=money_text(p.get("maxBounty")),
                 eco=", ".join(p.get("ecosystem") or []) or "unknown",
                 scope=", ".join(p.get("programType") or []) or "unknown",
                 launch=(p.get("launchDate") or "?")[:10], link=immunefi_link(slug))
        items.append(Item(slug, project, immunefi_link(slug), text, p.get("launchDate") or ""))
    return check_listing(conn, "immunefi", items)


# ---------------------------------------------------------------------------
# Source registry and health
# ---------------------------------------------------------------------------

# (name, label, check function, minimum seconds between checks)
Source = namedtuple("Source", "name label check min_interval")


def build_sources() -> List[Source]:
    sources = []  # type: List[Source]
    for name, repo, label, roots in DEFILLAMA_REPOS:
        sources.append(Source(
            name, "DefiLlama " + label + "s",
            (lambda conn, gh, n=name, r=repo, l=label, ro=roots:
             check_defillama_repo(conn, gh, n, r, l, ro)), 0))
    for org, label in AUDIT_ORGS:
        sources.append(Source(
            "audit_" + org, label,
            (lambda conn, gh, o=org, l=label: check_audit_org(conn, gh, o, l)), 0))
    sources.append(Source("audit_" + HATS_ORG, "Hats Finance", check_hats, 0))
    sources.append(Source("cantina", "Cantina", check_cantina, 3600))
    # About 6 MB per download, so not on every check
    sources.append(Source("immunefi", "Immunefi", check_immunefi, 6 * 3600))
    return sources


def record_health(conn: sqlite3.Connection, src: Source, error: Optional[str]) -> None:
    fails_key = "health.{}.fails".format(src.name)
    fails = int(meta_get(conn, fails_key) or 0)
    with conn:
        if error is None:
            if fails >= HEALTH_ALERT_AFTER:
                outbox_add(conn, "[RADAR] {} source is working again.".format(src.label))
            meta_set(conn, fails_key, 0)
            meta_set(conn, "health.{}.last_ok".format(src.name), now_ms())
            return
        fails += 1
        meta_set(conn, fails_key, fails)
        meta_set(conn, "health.{}.error".format(src.name), error[:300])
        if fails == HEALTH_ALERT_AFTER:
            outbox_add(conn, "[RADAR WARNING] {} source failed {} checks in a row.\n"
                             "Last error: {}".format(src.label, fails, error[:300]))


# ---------------------------------------------------------------------------
# Daily status report for all services
# ---------------------------------------------------------------------------

def other_services() -> List[Tuple[str, str, int]]:
    """(label, database path, longest normal gap between cycles in ms)."""
    hl_gap = max(30, env_int("HL_SCAN_SECONDS", 120)) * 1000
    return [
        ("Whale Watch", env_str("HL_DB", "/opt/radar/whales.db"), max(15 * 60 * 1000, 5 * hl_gap)),
        ("Heat Analyzer", env_str("HEAT_DB", "/opt/radar/heat.db"), 2 * 3600 * 1000),
        ("Attention Radar", env_str("ATT_DB", "/opt/radar/attention.db"), 2 * 3600 * 1000),
        ("News Watch", env_str("NEWS_DB", "/opt/radar/news.db"),
         3 * max(120, env_int("NEWS_INTERVAL_SECONDS", 600)) * 1000),
    ]


def service_line(label: str, conn: Optional[sqlite3.Connection], max_gap_ms: int) -> str:
    if conn is None:
        return "➖ {}: no database yet".format(label)
    try:
        beat = meta_get(conn, "heartbeat_ms")
        alerts = outbox_count_since(conn, now_ms() - DAY_MS)
    except sqlite3.Error:
        return "➖ {}: not upgraded yet (no status data)".format(label)
    if beat is None:
        return "➖ {}: no cycle finished yet".format(label)
    late = now_ms() - int(beat) > max_gap_ms
    return "{} {}: last cycle {}{} | {} messages in 24h".format(
        "⚠️" if late else "✅", label, ago(int(beat)), " (LATE)" if late else "", alerts)


def build_status(conn: sqlite3.Connection, gh: GitHub, sources: List[Source],
                 interval: int) -> str:
    lines = ["[STATUS] Daily health check {}".format(today_str())]
    lines.append(service_line("Project Radar", conn, 3 * interval * 1000))
    failing = []
    for src in sources:
        fails = int(meta_get(conn, "health.{}.fails".format(src.name)) or 0)
        if fails:
            failing.append("{} ({}x)".format(src.label, fails))
    lines.append("   Sources failing: {}".format(", ".join(failing) or "none"))
    for label, path, gap in other_services():
        other = open_db_readonly(path)
        try:
            lines.append(service_line(label, other, gap))
        finally:
            if other is not None:
                other.close()
    lines.append("GitHub API: {} requests left this hour".format(gh.rate_remaining))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

class Radar:
    def __init__(self, conn: sqlite3.Connection, interval: int) -> None:
        self.conn = conn
        self.interval = interval
        self.gh = GitHub(conn, env_str("GITHUB_TOKEN"))
        self.telegram = Telegram.from_env(USER_AGENT)
        self.sources = build_sources()
        self.status_hour = env_int("RADAR_STATUS_HOUR_UTC", 8)

    def source_due(self, src: Source) -> bool:
        if not src.min_interval:
            return True
        last = int(meta_get(self.conn, "health.{}.last_try".format(src.name)) or 0)
        return now_ms() - last >= src.min_interval * 1000

    def run_once(self) -> None:
        log("Check started")
        queued = boot_total = 0
        requests_before = self.gh.requests
        for src in self.sources:
            if not self.source_due(src):
                continue
            with self.conn:
                meta_set(self.conn, "health.{}.last_try".format(src.name), now_ms())
            try:
                a, b = src.check(self.conn, self.gh)
                queued += a
                boot_total += b
                record_health(self.conn, src, None)
            except Exception as e:
                log("{} source failed: {}".format(src.label, e))
                record_health(self.conn, src, str(e))

        if boot_total:
            with self.conn:
                outbox_add(self.conn,
                           "Project Radar started.\n"
                           "Marked {} existing items as already seen.\n"
                           "From now on you will only get NEW projects.".format(boot_total))
        if daily_due(self.conn, "last_status", self.status_hour):
            self.send_status()

        sent = outbox_flush(self.conn, self.telegram)
        heartbeat(self.conn)
        log("Check finished: {} new alerts, {} sent, {} bootstrapped, "
            "{} GitHub requests ({} unchanged), {} left".format(
                queued, sent, boot_total, self.gh.requests - requests_before,
                self.gh.cache_hits, self.gh.rate_remaining))
        self.gh.cache_hits = 0

    def send_status(self) -> None:
        with self.conn:
            outbox_add(self.conn, build_status(self.conn, self.gh, self.sources, self.interval))


def main() -> None:
    parser = argparse.ArgumentParser(description="Project Radar v2")
    parser.add_argument("--env", default="", help="path to env file with tokens")
    parser.add_argument("--once", action="store_true", help="run one check and exit")
    parser.add_argument("--status", action="store_true", help="send the status report now")
    parser.add_argument("--test-telegram", action="store_true", help="send a test message")
    parser.add_argument("--export", default="", help="export alerts to this CSV path")
    args = parser.parse_args()

    load_env_file(args.env)
    if not env_str("GITHUB_TOKEN"):
        log("Warning: GITHUB_TOKEN is empty. Limit is only 60 requests/hour.")

    if args.test_telegram:
        ok = Telegram.from_env(USER_AGENT).send("Project Radar test message. Telegram is working.")
        sys.exit(0 if ok else 1)

    conn = open_db(env_str("RADAR_DB", "/opt/radar/radar.db"), SCHEMA)
    interval = max(120, env_int("RADAR_INTERVAL_SECONDS", 900))
    radar = Radar(conn, interval)

    if args.export:
        export_csv(conn, args.export)
        return
    if args.status:
        radar.send_status()
        outbox_flush(conn, radar.telegram)
        return
    if args.once:
        radar.run_once()
        return

    log("Running forever, every {} seconds".format(interval))
    while True:
        started = time.time()
        try:
            radar.run_once()
        except Exception as e:
            log("Unexpected error: {}".format(e))
        time.sleep(max(30.0, interval - (time.time() - started)))


if __name__ == "__main__":
    main()
