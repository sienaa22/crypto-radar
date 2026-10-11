import unittest

from helpers import temp_db_path

import common
import whales

DAY = common.DAY_MS


class FakeMarket:
    def __init__(self, mids, funding=None):
        self.mids = mids
        self.rates = funding or {}

    def price(self, coin):
        return self.mids.get(coin, 0.0)

    def funding(self, coin):
        return self.rates.get(coin, 0.0)


def position(szi, value, lev=5.0, upnl=0.0, liq=0.0, entry=None):
    return {"szi": szi, "entry": entry or value / abs(szi), "value": value, "liq": liq,
            "lev": lev, "upnl": upnl, "roe": 0.1, "funding": -100.0, "warned": 0}


def profile(pnl_all, pnl_month, account, vlm_month=0.0, fills=None, spot=None):
    return {
        "portfolio": {
            "allTime": {"pnl": pnl_all, "vlm": 0.0, "value": account},
            "month": {"pnl": pnl_month, "vlm": vlm_month, "value": account},
            "week": {"pnl": 1.0, "vlm": 0.0, "value": account},
        },
        "fills": fills or [],
        "spot": spot or {"balances": []},
    }


def taker_fills(coin, n, days, pnl):
    """n closing fills over `days` days, each with the given closed PnL."""
    step = int(days * DAY / max(n - 1, 1))
    return [{"coin": coin, "time": i * step, "closedPnl": str(pnl), "fee": "0",
             "crossed": True} for i in range(n)]


class AssessTest(unittest.TestCase):
    def setUp(self):
        whales.load_config()
        self.market = FakeMarket({"ETH": 2500.0, "BTC": 80000.0})

    def test_profitable_directional_big_bet_is_high(self):
        pos = position(-4000, 10_000_000)
        a = whales.assess("ETH", pos, {"ETH": pos}, profile(5e6, 2e5, 12e6,
                          fills=taker_fills("ETH", 20, 30, 1000)), self.market)
        self.assertEqual(a["quality"], "HIGH", a)
        self.assertTrue(any("wins on ETH" in g for g in a["good"]))

    def test_market_maker_is_low(self):
        pos = position(-9000, 23_000_000, lev=15)
        a = whales.assess("ETH", pos, {"ETH": pos},
                          profile(2e8, 1e6, 1.2e8, vlm_month=1.3e10), self.market)
        self.assertEqual(a["quality"], "LOW")
        self.assertTrue(any("market maker" in b for b in a["bad"]))

    def test_short_covered_by_spot_is_low(self):
        pos = position(-1000, 2_500_000)
        spot = {"balances": [{"coin": "UETH", "total": "900"}]}
        a = whales.assess("ETH", pos, {"ETH": pos}, profile(5e6, 2e5, 4e6, spot=spot), self.market)
        self.assertEqual(a["quality"], "LOW")
        self.assertTrue(any("hedged" in b for b in a["bad"]))

    def test_neutral_book_and_funding_farm(self):
        book = {"ETH": position(-1000, 2_500_000, lev=2), "BTC": position(30, 2_400_000),
                "SOL": position(-100, 500_000), "HYPE": position(100, 600_000)}
        market = FakeMarket(self.market.mids, {"ETH": 0.0001})  # shorts earn ~88% APR
        a = whales.assess("ETH", book["ETH"], book, profile(5e6, 2e5, 4e6), market)
        self.assertEqual(a["quality"], "LOW")
        joined = " ".join(a["bad"])
        self.assertIn("market-neutral", joined)
        self.assertIn("funding farm", joined)

    def test_bot_record_on_coin_is_ignored(self):
        # 2000 fills in a few hours: per-coin record means nothing
        fills = taker_fills("ETH", 50, 0.1, -10)
        pos = position(-4000, 10_000_000)
        a = whales.assess("ETH", pos, {"ETH": pos}, profile(5e6, 2e5, 12e6, fills=fills),
                          self.market)
        self.assertFalse(any("ETH" in b for b in a["bad"]))


class ParsingTest(unittest.TestCase):
    def test_portfolio(self):
        data = [["month", {"pnlHistory": [[1, "0"], [2, "-3.5"]], "vlm": "10",
                           "accountValueHistory": [[2, "100"]]}],
                ["bad"]]
        self.assertEqual(whales.parse_portfolio(data),
                         {"month": {"pnl": -3.5, "vlm": 10.0, "value": 100.0}})

    def test_performance(self):
        rows = [("LONG", 100, 10, 110, 10), ("SHORT", 100, 10, 110, 11), ("LONG", 0, 1, 1, 1)]
        perf = whales.performance(rows)
        self.assertEqual(perf["n"], 2)
        self.assertAlmostEqual(perf["ret"], 0.0)
        self.assertAlmostEqual(perf["btc"], -5.0)
        self.assertEqual(perf["win"], 50.0)


class ScannerTest(unittest.TestCase):
    def setUp(self):
        whales.load_config()
        whales.CFG["min_quality"] = "MEDIUM"
        self.conn = common.open_db(temp_db_path(), whales.SCHEMA)
        self.market = FakeMarket({"ETH": 2500.0, "BTC": 80000.0}, {})

    def scanner(self, prof):
        class P:
            def get(self, addr):
                return prof
        return whales.Scanner(self.conn, self.market, P())

    def outbox(self):
        return [r[0] for r in self.conn.execute("SELECT text FROM outbox ORDER BY id")]

    def test_good_trade_sent_with_pnl(self):
        s = self.scanner(profile(5e6, 2e5, 12e6))
        cur = {"ETH": position(-4000, 10_000_000, upnl=-12_300)}
        with self.conn:
            s.process_changes("0x" + "ab" * 20, "whale", {}, cur, 0)
        (text,) = self.outbox()
        self.assertIn("[BIG TRADE] ETH SHORT +$10.0M | Signal: HIGH", text)
        self.assertIn("Position PnL: -$12K", text)
        self.assertIn("Trader PnL: all-time +$5.0M | 30d +$200K", text)
        self.assertIn("Past radar alerts: no earlier alerts", text)
        row = self.conn.execute("SELECT quality, sent, p0, btc0 FROM trade_alerts").fetchone()
        self.assertEqual(row, ("HIGH", 1, 2500.0, 80000.0))

    def test_low_trade_logged_not_sent(self):
        s = self.scanner(profile(-1e6, -2e5, 12e6))
        cur = {"ETH": position(-4000, 10_000_000)}
        with self.conn:
            s.process_changes("0x" + "cd" * 20, "whale", {}, cur, 0)
        self.assertEqual(self.outbox(), [])
        self.assertEqual(self.conn.execute("SELECT quality, sent FROM trade_alerts").fetchone(),
                         ("LOW", 0))

    def test_forward_prices_and_record(self):
        addr = "0x" + "ef" * 20
        old = common.now_ms() - 3 * DAY
        with self.conn:
            self.conn.execute(
                "INSERT INTO trade_alerts (ts, addr, kind, coin, side, added_usd, quality, sent,"
                " p0, btc0) VALUES (?, ?, 'whale', 'ETH', 'SHORT', 1, 'HIGH', 1, 2600, 80000)",
                (old, addr))
        whales.update_forward_prices(self.conn, self.market)
        row = self.conn.execute("SELECT p24, p72 FROM trade_alerts").fetchone()
        self.assertEqual(row, (2500.0, 2500.0))
        self.assertIn("right after 24h: 1 (100%)", whales.past_alert_record(self.conn, addr))


if __name__ == "__main__":
    unittest.main()
