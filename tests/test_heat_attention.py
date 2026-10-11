import sqlite3
import unittest

from helpers import temp_db_path

import attention
import common
import heat


def fee_row(slug, f30, f60, f7=7.0, f14=7.0):
    return {"slug": slug, "total30d": f30, "total60dto30d": f60, "total7d": f7, "total14dto7d": f14}


class HeatTest(unittest.TestCase):
    def setUp(self):
        heat.load_config()
        self.orig = heat.http_json

    def tearDown(self):
        heat.http_json = self.orig

    def test_candidates_use_prior_month_and_price_vs_category(self):
        protocols = [
            {"slug": "fast", "name": "Fast", "category": "Dexs", "mcap": 1e8, "gecko_id": "fast"},
            {"slug": "flat", "name": "Flat", "category": "Dexs", "mcap": 1e8, "gecko_id": "flat"},
            {"slug": "pumped", "name": "Pumped", "category": "Dexs", "mcap": 1e8, "gecko_id": "pump"},
            {"slug": "cex", "name": "Cex", "category": "CEX", "mcap": 1e9, "gecko_id": "cex"},
        ]
        fees = [fee_row("fast", 2e6, 1e6), fee_row("flat", 1e6, 1e6),
                fee_row("pumped", 2e6, 1e6), fee_row("cex", 9e6, 1e6)]
        revenue = [dict(r, total30d=r["total30d"] * 0.5, total60dto30d=r["total60dto30d"] * 0.5)
                   for r in fees]
        changes = {"coingecko:fast": 5.0, "coingecko:flat": 0.0, "coingecko:pump": 150.0}

        def fake(url, **kwargs):
            if url.endswith("/protocols"):
                return protocols
            if "dataType=dailyFees" in url:
                return {"protocols": fees}
            if "dataType=dailyRevenue" in url:
                return {"protocols": revenue}
            if "/percentage/" in url:
                return {"coins": changes}
            raise AssertionError(url)

        heat.http_json = fake
        candidates, counts = heat.build_candidates()
        self.assertEqual([c["slug"] for c in candidates], ["fast"])
        fast = candidates[0]
        self.assertAlmostEqual(fast["fee_accel"], 2.0)
        self.assertAlmostEqual(fast["price_vs_cat"], 0.0)  # category median is +5%
        self.assertAlmostEqual(fast["gap"], 100.0)
        self.assertEqual(counts["skipped_type"], 1)
        self.assertIn("price 30d +5.0%", heat.format_report(candidates, counts))

    def test_protocols_sharing_a_token_are_one_row(self):
        meta = {"aave-v2": {"name": "Aave V2", "category": "Lending", "mcap": 4e9,
                            "gecko_id": "aave", "parentProtocol": "parent#aave"},
                "aave-v3": {"name": "Aave V3", "category": "Lending", "mcap": None,
                            "gecko_id": None, "parentProtocol": "parent#aave"}}
        fees = {"aave-v2": fee_row("aave-v2", 2e5, 1e5), "aave-v3": fee_row("aave-v3", 9e7, 6e7)}
        revenue = {k: dict(v) for k, v in fees.items()}
        counts = {"universe": 0, "skipped_type": 0, "below_floor": 0, "wash_flag": 0, "candidate": 0}
        (row,) = heat.token_groups(meta, fees, revenue, counts)
        self.assertEqual(row["name"], "Aave V3 (+1 more)")
        self.assertEqual(row["gecko_id"], "aave")
        self.assertAlmostEqual(row["fees_30d"], 9.02e7)
        self.assertAlmostEqual(row["pf"], 4e9 / (9.02e7 * 365 / 30))

    def test_v1_market_cap_in_p0_is_moved(self):
        path = temp_db_path()
        c = sqlite3.connect(path)
        c.executescript(heat.SCHEMA + """
            INSERT INTO signals (ts, date, slug, gecko_id, p0) VALUES (1, 'd', 'venice', 'v', 1.08e9);
        """)
        c.commit()
        c.close()
        heat.CFG["db_path"] = path
        conn = heat.db_connect()
        self.assertEqual(conn.execute("SELECT p0, mcap FROM signals").fetchone(), (None, 1.08e9))
        conn.close()
        conn = heat.db_connect()  # second start must not touch the data again
        self.assertEqual(conn.execute("SELECT mcap FROM signals").fetchone(), (1.08e9,))


class AttentionTest(unittest.TestCase):
    def setUp(self):
        attention.load_config()
        attention.CFG["whales_db"] = temp_db_path("w.db")
        attention.CFG["heat_db"] = temp_db_path("h.db")
        self.saved = {n: getattr(attention, n) for n in
                      ("source_markets", "source_trending", "source_hyperliquid")}

    def tearDown(self):
        for n, f in self.saved.items():
            setattr(attention, n, f)

    def test_matching_by_id_and_oi_change(self):
        attention.source_markets = lambda: {
            "uniswap": {"sym": "UNI", "name": "Uniswap", "price_24h": 1.0, "vol_mcap": 0.2, "mcap": 5e9},
            "unicorn": {"sym": "UNI", "name": "Unicorn", "price_24h": 1.0, "vol_mcap": 0.0, "mcap": 1e6},
        }
        attention.source_trending = lambda: {"unicorn": {"sym": "UNI", "name": "Unicorn", "rank": 1}}
        attention.source_hyperliquid = lambda: {"UNI": {"oi_usd": 2e7, "funding": 0.0, "oi_change": None}}

        conn = common.open_db(temp_db_path(), attention.SCHEMA)
        with conn:
            conn.execute("INSERT INTO oi_snapshots VALUES ('UNI', ?, 1e7)",
                         (common.now_ms() - 24 * common.HOUR_MS,))
        h = common.open_db(attention.CFG["heat_db"], heat.SCHEMA)
        with h:
            h.execute("INSERT INTO signals (ts, date, slug, gecko_id, fee_accel)"
                      " VALUES (1, date('now'), 'uniswap', 'uniswap', 1.8)")
        h.close()

        rows, counts = attention.build_report(conn)
        by_name = {r["name"]: r for r in rows}
        uni = by_name["Uniswap"]
        self.assertIn("vol/mcap 20%", uni["reasons"])
        self.assertIn("OI +100% in 24h ($20.0M)", uni["reasons"])
        self.assertIn("fees 1.8x prior month", uni["reasons"])
        self.assertEqual(uni["sources"], 3)
        # The trending coin that shares the ticker is scored on its own
        self.assertEqual(by_name["Unicorn"]["reasons"], ["trending #1"])
        self.assertIn("UNI (Uniswap) *", attention.format_report(rows, counts))


if __name__ == "__main__":
    unittest.main()
