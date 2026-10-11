import unittest

from helpers import temp_db_path

import ai
import brain
import common
import flows

DAY = common.DAY_MS
HOUR = common.HOUR_MS


def clear_ledger():
    sig = common.signals_db()
    with sig:
        sig.execute("DELETE FROM signals")
    return sig


def brain_db():
    brain.load_config()
    conn = common.open_db(temp_db_path(), brain.SCHEMA)
    with conn:
        conn.executemany("INSERT INTO coins VALUES (?, ?, ?, ?)", [
            ("bitcoin", "BTC", "Bitcoin", 1), ("aptos", "APT", "Aptos", 30),
            ("pepe", "PEPE", "Pepe", 40), ("aptos-fake", "APT", "Fake Aptos", 900)])
    return conn


class LedgerTest(unittest.TestCase):
    def setUp(self):
        self.sig = clear_ledger()
        self.conn = brain_db()
        self.orig = brain.prices
        self.calls = []

        def fake_prices(coins, ts_sec=None):
            self.calls.append(ts_sec)
            later = ts_sec is not None and ts_sec * 1000 > common.now_ms() - 5 * DAY
            return {c: (110.0 if later and c == "aptos" else 100.0) for c in coins}
        brain.prices = fake_prices

    def tearDown(self):
        brain.prices = self.orig

    def test_symbols_resolve_to_the_largest_coin(self):
        self.assertEqual(brain.resolve_symbol(self.conn, "APT"), "aptos")
        self.assertEqual(brain.resolve_symbol(self.conn, "kPEPE"), "pepe")
        self.assertEqual(brain.resolve_symbol(self.conn, "1000PEPEUSDT"), "pepe")
        self.assertIsNone(brain.resolve_symbol(self.conn, "NOPE"))

    def test_pricing_and_scorecard(self):
        common.record_signal("news", "listing_binance", "Binance lists APT", 1, symbol="APT")
        with self.sig:
            # Ten older signals that already went the right way, to give the kind a record
            for i in range(10):
                self.sig.execute(
                    "INSERT INTO signals (ts, service, kind, coin, direction, title, p0, btc0, p1d, btc1d)"
                    " VALUES (?, 'flows', 'oi_long_buildup', 'aptos', 1, 't', 100, 100, 103, 100)",
                    (common.now_ms() - 3 * DAY,))
        brain.price_signals(self.sig, self.conn)
        row = self.sig.execute("SELECT coin, p0, btc0 FROM signals WHERE kind = 'listing_binance'").fetchone()
        self.assertEqual(row, ("aptos", 100.0, 100.0))

        card = {r["kind"]: r for r in brain.scorecard(self.sig)}
        self.assertAlmostEqual(card["oi_long_buildup"]["e1"], 3.0)
        self.assertEqual(card["oi_long_buildup"]["verdict"], "USEFUL")
        self.assertIn("collecting data", card["listing_binance"]["verdict"])
        weights = brain.learned_weights(self.sig)
        self.assertGreater(weights["oi_long_buildup"], 1.0)
        self.assertIn("oi_long_buildup", brain.format_scorecard(brain.scorecard(self.sig)))

    def test_edge(self):
        self.assertAlmostEqual(brain.edge("aptos", 1, 100, 110, 100, 105), 5.0)
        self.assertAlmostEqual(brain.edge("aptos", -1, 100, 110, 100, 105), -5.0)
        self.assertAlmostEqual(brain.edge("bitcoin", 1, 100, 110, 100, 110), 10.0)  # no BTC baseline vs itself
        self.assertAlmostEqual(brain.edge("aptos", 0, 100, 90, 100, 102), 8.0)


class ConvergenceTest(unittest.TestCase):
    def setUp(self):
        self.sig = clear_ledger()
        self.conn = brain_db()

    def add(self, service, kind, direction, coin="aptos"):
        common.record_signal(service, kind, "{} {}".format(service, kind), direction, coin=coin)

    def outbox(self):
        return [r[0] for r in self.conn.execute("SELECT text FROM outbox ORDER BY id")]

    def test_independent_signals_line_up(self):
        self.add("news", "unlock_vesting_end", 1)
        self.add("whales", "whale_trade_low", 1)  # rejected by the quality filter: no vote
        self.assertEqual(brain.check_convergence(self.sig, self.conn, {}), 0)
        self.add("whales", "whale_accumulation", 1)
        self.add("flows", "top_traders_long", 1)
        self.assertEqual(brain.check_convergence(self.sig, self.conn, {}), 1)
        (text,) = self.outbox()
        self.assertIn("[CONVERGENCE] Aptos (APT, #30) | BULLISH", text)
        self.assertIn("3 independent signals from 3 services", text)
        self.assertNotIn("whale_trade_low", text)
        # The convergence is measured like any other signal
        self.assertEqual(self.sig.execute("SELECT COUNT(*) FROM signals WHERE kind = 'convergence'")
                         .fetchone()[0], 1)
        # Same picture again: no repeat
        self.add("news", "unlock_vesting_end", 1)
        self.assertEqual(brain.check_convergence(self.sig, self.conn, {}), 0)

    def test_one_service_alone_is_not_enough(self):
        for kind in ("whale_accumulation", "whale_consensus", "whale_trade_high"):
            self.add("whales", kind, 1)
        self.assertEqual(brain.check_convergence(self.sig, self.conn, {}), 0)

    def test_mixed_signals_are_attention(self):
        self.add("news", "hack", -1)
        self.add("flows", "oi_long_buildup", 1)
        self.add("whales", "whale_accumulation", 1)
        weights = {"hack": 2.0}
        self.assertEqual(brain.check_convergence(self.sig, self.conn, weights), 1)
        self.assertIn("ATTENTION", self.outbox()[0])
        self.assertIn("Mixed", self.outbox()[0])


class FlowsTest(unittest.TestCase):
    def setUp(self):
        clear_ledger()
        flows.load_config()
        self.conn = common.open_db(temp_db_path(), flows.SCHEMA)
        self.orig = flows.fapi
        self.responses = {}
        flows.fapi = lambda path: self.responses[path.split("?")[0] + ("?" + path.split("symbol=")[-1]
                                                                       if "symbol=" in path else "")]

    def tearDown(self):
        flows.fapi = self.orig

    def test_oi_build_up_with_price_up(self):
        ts = common.now_ms() - 4 * HOUR
        with self.conn:
            self.conn.execute("INSERT INTO oi VALUES ('APTUSDT', ?, 2000000, 10.0)", (ts,))
        self.responses["/fapi/v1/openInterest?APTUSDT"] = {"openInterest": "2600000"}
        tickers = {"APTUSDT": {"lastPrice": "10.5", "quoteVolume": "5e8"}}
        self.assertEqual(flows.check_open_interest(self.conn, ["APTUSDT"], tickers), 1)
        text = self.conn.execute("SELECT text FROM outbox").fetchone()[0]
        self.assertIn("[OI BUILD-UP] APT: open interest +30% in 4h ($27.3M), price +5.0%", text)
        self.assertIn("new longs", text)
        # Cooldown: no second alert
        self.assertEqual(flows.check_open_interest(self.conn, ["APTUSDT"], tickers), 0)
        row = common.signals_db().execute("SELECT kind, symbol, direction FROM signals").fetchone()
        self.assertEqual(row, ("oi_long_buildup", "APT", 1))

    def test_crowded_funding_is_contrarian(self):
        self.responses["/fapi/v1/premiumIndex"] = [
            {"symbol": "APTUSDT", "lastFundingRate": "0.0015"},
            {"symbol": "BTCUSDT", "lastFundingRate": "0.0001"}]
        self.assertEqual(flows.check_funding(self.conn, ["APTUSDT", "BTCUSDT"]), 1)
        row = common.signals_db().execute("SELECT kind, direction FROM signals").fetchone()
        self.assertEqual(row, ("funding_crowded_longs", -1))

    def test_top_traders_against_crowd(self):
        self.responses["/futures/data/topLongShortPositionRatio?APTUSDT"] = [{"longShortRatio": "1.8"}]
        self.responses["/futures/data/globalLongShortAccountRatio?APTUSDT"] = [{"longShortRatio": "0.7"}]
        self.responses["/futures/data/topLongShortPositionRatio?BTCUSDT"] = [{"longShortRatio": "1.1"}]
        self.responses["/futures/data/globalLongShortAccountRatio?BTCUSDT"] = [{"longShortRatio": "1.0"}]
        self.assertEqual(flows.check_top_traders(self.conn, ["APTUSDT", "BTCUSDT"]), 1)
        self.assertIn("top traders are long while most accounts are short",
                      self.conn.execute("SELECT text FROM outbox").fetchone()[0])

    def test_stablecoin_mint(self):
        orig = flows.total_supply
        flows.total_supply = lambda chain, contract, decimals: {
            "0xdAC17F958D2ee523a2206206994597C13D831ec7": 89.0e9}.get(contract, 50.0e9)
        try:
            with self.conn:
                for name, chain, _, _ in flows.STABLECOINS:
                    self.conn.execute("INSERT INTO supply VALUES (?, ?, ?, ?)",
                                      (name, chain, common.now_ms() - 25 * HOUR,
                                       88.0e9 if (name, chain) == ("USDT", "Ethereum") else 50.0e9))
            self.assertEqual(flows.check_stablecoins(self.conn), 1)
        finally:
            flows.total_supply = orig
        text = self.conn.execute("SELECT text FROM outbox").fetchone()[0]
        self.assertIn("[STABLECOIN MINT] +$1.0B of USDT/USDC in 24h", text)
        self.assertIn("USDT Ethereum +$1.0B", text)


class AiTest(unittest.TestCase):
    def test_off_without_key(self):
        conn = common.open_db(temp_db_path())
        self.assertFalse(ai.enabled())
        self.assertIsNone(ai.brief(conn, "anything"))
        self.assertEqual(ai.format_brief(None), "")
        self.assertEqual(ai.direction(None), 0)

    def test_parse_reply(self):
        reply = 'Here: {"fa": "خبر مهم", "impact": "bullish", "coin": "APT", "confidence": "high"}'
        b = ai._parse(reply)
        self.assertEqual(b["impact"], "bullish")
        self.assertEqual(ai.direction(b), 1)
        self.assertIn("🟢", ai.format_brief(b))
        self.assertIsNone(ai._parse("no json here"))
        self.assertEqual(ai._parse('{"fa": "x", "impact": "moon"}')["impact"], "neutral")


if __name__ == "__main__":
    unittest.main()
