import os
import sqlite3
from datetime import datetime, timezone
import unittest
import urllib.parse

from helpers import FakeTelegram, LocalServer, temp_db_path

import common
import radar


def pr(n, created, updated, merged=False):
    return {"number": n, "created_at": created, "updated_at": updated,
            "merged_at": "x" if merged else None, "state": "closed" if merged else "open",
            "title": "Add %d" % n, "html_url": "https://gh/pr/%d" % n,
            "base": {"sha": "base%d" % n}}


RECENT = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def repo(name, created=RECENT):
    return {"name": name, "created_at": created, "html_url": "https://gh/" + name,
            "description": None}


class World:
    """Fake GitHub / Cantina / Immunefi / DefiLlama state."""

    def __init__(self):
        self.prs = {"DefiLlama/DefiLlama-Adapters": [], "DefiLlama/dimension-adapters": []}
        self.files = {}
        self.fail_files = set()
        self.base_paths = set()
        self.orgs = {org: [] for org, _ in radar.AUDIT_ORGS}
        self.orgs[radar.HATS_ORG] = []
        self.cantina = []
        self.immunefi = []
        self.llama = [{"name": "Aave", "slug": "aave"}]
        self.json_calls = []

    def github_get(self, url):
        if "/pulls?" in url:
            return self.prs[url.split("/repos/")[1].split("/pulls")[0]], None
        if "/files" in url:
            n = int(url.split("/pulls/")[1].split("/")[0])
            if n in self.fail_files:
                raise RuntimeError("boom")
            return self.files.get(n, []), None
        if "/contents/" in url:
            path = urllib.parse.unquote(url.split("/contents/")[1].split("?")[0])
            if "?ref=base" in url and path in self.base_paths:
                return {}, None
            raise common.NotFound(404, url)
        if "/orgs/" in url:
            return self.orgs[url.split("/orgs/")[1].split("/")[0]], None
        raise AssertionError(url)

    def http_json(self, url, **kwargs):
        self.json_calls.append(url)
        if url == radar.CANTINA_URL:
            return self.cantina
        if url == radar.IMMUNEFI_URL:
            return self.immunefi
        if url == radar.LLAMA_PROTOCOLS_URL:
            return self.llama
        raise AssertionError(url)


class RadarFlowTest(unittest.TestCase):
    def setUp(self):
        self.world = World()
        self.conn = common.open_db(temp_db_path(), radar.SCHEMA)
        self.radar = radar.Radar(self.conn, 900)
        self.radar.telegram = self.tg = FakeTelegram()
        self.radar.status_hour = 99  # no status report in these tests
        self.radar.gh.get = lambda url, cache=False: self.world.github_get(url)
        self._orig_json = radar.http_json
        radar.http_json = self.world.http_json
        radar.LLAMA_CACHE.update(names=None, loaded_at=0.0)

    def tearDown(self):
        radar.http_json = self._orig_json

    def run_check(self):
        self.radar.run_once()
        sent, self.tg.sent = self.tg.sent, []
        return sent

    def test_full_flow(self):
        w = self.world
        tvl = w.prs["DefiLlama/DefiLlama-Adapters"]
        dims = w.prs["DefiLlama/dimension-adapters"]
        tvl.append(pr(1, "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"))
        w.orgs["code-423n4"] = [repo("2026-01-old-findings"), repo("2026-01-old"), repo("tooling")]
        w.orgs["sherlock-audit"] = [repo("2026-01-sold-judging")]
        w.cantina = [{"id": "c1", "name": "Old", "createdAt": "2026-01-01"}]
        w.immunefi = [{"slug": "old", "project": "Old", "launchDate": "2026-01-01"}]

        # Run 1: everything that exists is recorded silently
        sent = self.run_check()
        self.assertEqual(len(sent), 1)
        self.assertIn("Marked 5 existing items", sent[0])
        self.assertNotIn(radar.LLAMA_PROTOCOLS_URL, w.json_calls, "llama list loaded on bootstrap")

        # Make the stored start time early enough for the fake PRs below
        with self.conn:
            for src in ("defillama_tvl", "defillama_dims"):
                common.meta_set(self.conn, src + ".started_at", "2026-01-01T00:00:00Z")
                common.meta_set(self.conn, src + ".watermark", "2026-01-01T00:00:00Z")
            common.meta_set(self.conn, "health.cantina.last_try", 0)  # due again

        # Run 2: a PR merged between checks, an existing project, an old project,
        # a fees adapter for a project already alerted, and new listings
        topic = "0x" + "ab" * 32
        addr = "0x" + "12" * 20
        tvl.insert(0, pr(3, "2026-02-01T00:00:00Z", "2026-02-02T00:00:00Z", merged=True))
        tvl.insert(1, pr(2, "2026-02-01T00:00:00Z", "2026-02-01T00:00:00Z"))
        w.files[3] = [
            {"status": "added", "filename": "projects/newproj/index.js",
             "patch": "+const chains = ['base', 'arbitrum']\n+  ethereum: {\n+  tokens: {\n"
                      "+ topic: '%s'\n+ x: '%s'\n- y: '0x%s'\n+ z: '0x%s'"
                      % (topic, addr, "34" * 20, "0" * 40)},
            {"status": "added", "filename": "projects/aave/v9.js", "patch": "+x"},
            {"status": "modified", "filename": "projects/other/index.js",
             "patch": "+ z: '0x%s'" % ("56" * 20)},
        ]
        w.files[2] = [{"status": "added", "filename": "projects/oldproj/v2.js", "patch": "+x"}]
        w.base_paths = {"projects/oldproj"}
        dims.append(pr(7, "2026-02-03T00:00:00Z", "2026-02-03T00:00:00Z"))
        w.files[7] = [{"status": "added", "filename": "fees/newproj.ts", "patch": "+x"},
                      {"status": "added", "filename": "dexs/swapper/index.ts", "patch": "+x"}]
        w.orgs["code-423n4"] = [repo("2026-03-fresh-findings"), repo("2026-03-fresh")] + \
            w.orgs["code-423n4"]
        w.cantina.append({"id": "c2", "name": "Fresh Comp", "company": {"name": "FreshCo"},
                          "totalRewardPot": "50000", "currencyCode": "USDC",
                          "status": "live", "kind": "public_contest", "createdAt": RECENT,
                          "url": "https://cantina.xyz/competitions/c2"})
        self.tg.ok = False
        self.run_check()
        pending = self.conn.execute(
            "SELECT text FROM outbox WHERE sent_ms IS NULL ORDER BY id").fetchall()
        self.assertEqual(len(pending), 4, pending)

        # Run 3: Telegram is back, the queue drains in order
        self.tg.ok = True
        sent = self.run_check()
        self.assertEqual([s.splitlines()[1] for s in sent],
                         ["Project: newproj", "Project: swapper", "Project: fresh", "Project: FreshCo"])
        msg = sent[0]
        self.assertIn(addr, msg)
        for bad in (topic[:42], "34" * 20, "56" * 20, "0" * 40):
            self.assertNotIn(bad, msg)
        self.assertIn("Chains (guess): arbitrum, base, ethereum", msg)
        self.assertIn("Repo: code-423n4/2026-03-fresh\n", sent[2])
        self.assertIn("Prize pool: $50,000", sent[3])
        # Cantina and Immunefi wait for their minimum interval
        self.assertEqual(w.json_calls.count(radar.IMMUNEFI_URL), 1)

    def test_pr_failures_and_late_files(self):
        w = self.world
        tvl = w.prs["DefiLlama/DefiLlama-Adapters"]
        self.run_check()
        with self.conn:
            common.meta_set(self.conn, "defillama_tvl.started_at", "2026-01-01T00:00:00Z")
            common.meta_set(self.conn, "defillama_tvl.watermark", "2026-04-01T00:00:00Z")
        tvl[:] = [pr(5, "2026-04-01T00:00:00Z", "2026-04-03T00:00:00Z"),
                  pr(4, "2026-04-01T00:00:00Z", "2026-04-02T00:00:00Z")]
        w.files[4] = [{"status": "added", "filename": "projects/four.js", "patch": "+x"}]
        w.files[5] = [{"status": "added", "filename": "README.md"}]
        w.fail_files = {4}
        self.run_check()
        self.assertEqual(common.meta_get(self.conn, "defillama_tvl.watermark"), "2026-04-02T00:00:00Z")

        w.fail_files = set()
        tvl[0] = pr(5, "2026-04-01T00:00:00Z", "2026-04-05T00:00:00Z")
        w.files[5] = [{"status": "added", "filename": "projects/five/index.js", "patch": "+x"}]
        sent = self.run_check()
        self.assertEqual([s.splitlines()[1] for s in sent], ["Project: four", "Project: five"])
        self.assertEqual(self.run_check(), [], "duplicate alert")

    def test_health_warning_after_repeated_failures(self):
        def broken(url):
            raise RuntimeError("down")
        self.radar.gh.get = lambda url, cache=False: broken(url)
        for _ in range(radar.HEALTH_ALERT_AFTER - 1):
            self.assertFalse(any("WARNING" in s for s in self.run_check()))
        warnings = [s for s in self.run_check() if "WARNING" in s]
        self.assertTrue(any("Code4rena" in s for s in warnings), warnings)

    def test_status_report(self):
        for name in ("HL_DB", "HEAT_DB", "ATT_DB"):
            os.environ[name] = temp_db_path(name)  # missing files
        common.heartbeat(self.conn)
        text = radar.build_status(self.conn, self.radar.gh, self.radar.sources, 900)
        self.assertIn("✅ Project Radar", text)
        self.assertIn("➖ Whale Watch: no database yet", text)


class OldItemsTest(unittest.TestCase):
    def test_old_contests_found_late_are_not_alerted(self):
        conn = common.open_db(temp_db_path(), radar.SCHEMA)
        gh = radar.GitHub(conn, "")
        org = {"code-423n4": [repo("2026-01-first")]}
        gh.get = lambda url, cache=False: (org["code-423n4"], None)
        self.assertEqual(radar.check_audit_org(conn, gh, "code-423n4", "Code4rena"), (0, 1))
        # A wider page now shows older contests that were never stored, and one new one
        org["code-423n4"] = [repo("2026-10-fresh"), repo("2026-01-first"),
                             repo("2025-01-initia-modules", "2025-01-03T23:44:27Z"),
                             repo("2025-02-blend", "2025-02-06T18:07:05Z")]
        self.assertEqual(radar.check_audit_org(conn, gh, "code-423n4", "Code4rena"), (1, 0))
        (text,) = [r[0] for r in conn.execute("SELECT text FROM outbox")]
        self.assertIn("Project: fresh", text)
        self.assertEqual(radar.check_audit_org(conn, gh, "code-423n4", "Code4rena"), (0, 0))


class UpgradeTest(unittest.TestCase):
    def test_v1_database(self):
        path = temp_db_path()
        c = sqlite3.connect(path)
        c.executescript("""
            CREATE TABLE seen (source TEXT NOT NULL, item_key TEXT NOT NULL,
                first_seen TEXT NOT NULL, PRIMARY KEY (source, item_key));
            CREATE TABLE alerts (id INTEGER PRIMARY KEY AUTOINCREMENT, sent_at TEXT NOT NULL,
                source TEXT NOT NULL, project TEXT NOT NULL, url TEXT NOT NULL, details TEXT);
            INSERT INTO seen VALUES ('defillama_pr', '1', 'x');
            INSERT INTO alerts (sent_at, source, project, url, details)
                VALUES ('t', 's', '=evil', 'u', 'd');
        """)
        c.commit()
        c.close()
        conn = common.open_db(path, radar.SCHEMA)
        gh = radar.GitHub(conn, "")
        gh.get = lambda url, cache=False: (_ for _ in ()).throw(AssertionError("no fetch on upgrade"))
        self.assertEqual(radar.check_defillama_repo(conn, gh, "defillama_tvl",
                                                    "DefiLlama/DefiLlama-Adapters",
                                                    "TVL adapter", ("projects",)), (0, 0))
        csv_path = temp_db_path("a.csv")
        radar.export_csv(conn, csv_path)
        with open(csv_path) as f:
            self.assertIn("'=evil", f.read())


class EtagTest(unittest.TestCase):
    def test_unchanged_page_uses_cache(self):
        def handler(path, headers):
            if headers.get("if-none-match") == '"v1"':
                return 304, {"ETag": '"v1"', "X-RateLimit-Remaining": "4999"}, b""
            return 200, {"ETag": '"v1"', "X-RateLimit-Remaining": "4998",
                         "Link": '<http://next>; rel="next"'}, [{"id": 1}]

        server = LocalServer(handler)
        try:
            conn = common.open_db(temp_db_path(), radar.SCHEMA)
            gh = radar.GitHub(conn, "tok")
            first = gh.get(server.url + "/list", cache=True)
            second = gh.get(server.url + "/list", cache=True)
            self.assertEqual(first, ([{"id": 1}], "http://next"))
            self.assertEqual(second, first)
            self.assertEqual(gh.cache_hits, 1)
            self.assertEqual(gh.rate_remaining, "4999")
            self.assertEqual(server.requests[0][1]["authorization"], "Bearer tok")
        finally:
            server.close()


class ParsingTest(unittest.TestCase):
    def test_entries_and_chains(self):
        files = [{"status": "added", "filename": "projects/foo/index.js"},
                 {"status": "added", "filename": "projects/bar.js"},
                 {"status": "added", "filename": "projects/helper/x.js"},
                 {"status": "added", "filename": "projects/README.md"},
                 {"status": "added", "filename": "fees/baz.ts"}]
        self.assertEqual(radar.new_project_entries(files, ("projects",)),
                         {"foo": "projects/foo", "bar": "projects/bar.js"})
        self.assertEqual(radar.guess_chains("chain: 'sonic'\n  base: {\n  tokens: {"),
                         ["base", "sonic"])

    def test_contest_and_hats_names(self):
        self.assertEqual(radar.contest_key("2024-01-Foo-judging"), "2024-01-foo")
        self.assertEqual(radar.contest_key("2024-03-foo-mitigation-findings"), "2024-03-foo")
        self.assertEqual(radar.immunefi_link("audit-competition-ens"),
                         "https://immunefi.com/audit-competition/audit-competition-ens/")


if __name__ == "__main__":
    unittest.main()
