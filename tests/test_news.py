import json
import time
import unittest

from helpers import temp_db_path

import common
import news

DAY_S = 86400


def fake_coins():
    coins = news.Coins()
    coins.rows = []
    data = [("bitcoin", "BTC", "Bitcoin"), ("ethereum", "ETH", "Ethereum"),
            ("solana", "SOL", "Solana"), ("aptos", "APT", "Aptos"), ("flow", "FLOW", "Flow")]
    for i, (cid, sym, name) in enumerate(data):
        coins.rows.append({"id": cid, "sym": sym, "name": name, "rank": [1, 2, 7, 90, 150][i],
                           "change": 3.2, "price": 1.0})
    coins.loaded_ms = common.now_ms()
    # Same index building as refresh(), without the network
    coins.by_ticker = {c["sym"]: c for c in coins.rows if c["sym"] not in news.TICKER_STOP}
    coins.by_name = {c["name"]: c for c in coins.rows if c["name"] not in news.NAME_STOP}
    coins.name_re = news.re.compile(r"\b(" + "|".join(sorted(coins.by_name, key=len, reverse=True)) + r")\b")
    return coins


class ScoringTest(unittest.TestCase):
    def setUp(self):
        news.load_config()
        self.coins = fake_coins()

    def score(self, title, summary=""):
        return news.score_headline(title, summary, self.coins)[0]

    def test_important_headlines_pass(self):
        for title in ("SEC approves spot Solana ETFs",
                      "Aptos token unlocks for private investors and core contributors end today",
                      "Binance will list Aptos (APT)",
                      "DeFi lender drained of $40 million in exploit"):
            self.assertGreaterEqual(self.score(title), news.CFG["min_score"], title)

    def test_noise_does_not_pass(self):
        for title in ("Bitcoin price prediction: analysts see $150K by December",
                      "Uranium Finance hacker convicted over $50M theft",
                      "Here's what happened in crypto today",
                      "Fund flows into Bitcoin ETFs slow down"):
            self.assertLess(self.score(title), news.CFG["min_score"], title)

    def test_coin_matching(self):
        found = self.coins.find("Fund flows rise as $APT and Solana rally, ETF news")
        self.assertEqual([c["id"] for c in found], ["solana", "aptos"])  # not "Flow", not "ETF"


class FeedTest(unittest.TestCase):
    RSS = """<?xml version="1.0"?><rss><channel>
      <item><title>{t}</title><link>{l}</link><description>&lt;p&gt;x&lt;/p&gt;</description>
      <pubDate>{d}</pubDate></item></channel></rss>"""

    def setUp(self):
        news.load_config()
        self.conn = common.open_db(temp_db_path(), news.SCHEMA)
        self.coins = fake_coins()
        self.feeds = {}
        self.orig = news.fetch_text
        news.fetch_text = lambda url, timeout=30: self.feeds.get(url, "<rss><channel/></rss>")

    def tearDown(self):
        news.fetch_text = self.orig

    def set_feed(self, outlet, title, link):
        url = dict(news.FEEDS)[outlet]
        date = time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.gmtime())
        self.feeds[url] = self.RSS.format(t=title, l=link, d=date)

    def outbox(self):
        return [r[0] for r in self.conn.execute("SELECT text FROM outbox ORDER BY id")]

    def test_bootstrap_then_one_message_per_story(self):
        news.check_feeds(self.conn, self.coins)  # first run: empty feeds, marks the start
        self.set_feed("CoinDesk", "DeFi lender Ledgerly drained of $40 million in exploit", "https://a/1")
        self.assertEqual(news.check_feeds(self.conn, self.coins), 1)
        self.set_feed("Decrypt", "Ledgerly exploit: $40 million stolen from lending pools", "https://b/1")
        self.assertEqual(news.check_feeds(self.conn, self.coins), 0)  # same story, other outlet
        (text,) = self.outbox()
        self.assertIn("[NEWS] DeFi lender Ledgerly drained", text)
        self.assertIn("Link: https://a/1", text)


class QueueTest(unittest.TestCase):
    def test_empty_first_run_still_ends_bootstrap(self):
        conn = common.open_db(temp_db_path(), news.SCHEMA)
        self.assertEqual(news.queue_new(conn, "hacks", []), 0)
        self.assertEqual(news.queue_new(conn, "hacks", [news.Item("k", "[HACK] x")]), 1)

    def test_upbit_title_parsing(self):
        orig = news.get_json
        news.get_json = lambda url, timeout=60: {"data": {"notices": [
            {"id": 7, "title": "카이아(KAIA) 신규 거래지원 안내 (KRW, BTC, USDT 마켓)"},
            {"id": 8, "title": "유의 종목 지정 안내"}]}}
        try:
            items = news.upbit_items(fake_coins())
        finally:
            news.get_json = orig
        self.assertEqual(len(items), 1)
        self.assertIn("Upbit new listing: KAIA (KRW, BTC, USDT markets)", items[0].text)


class UnlockTest(unittest.TestCase):
    def schedule(self, now_s):
        day = lambda d: int(now_s + d * DAY_S)  # noqa: E731
        return {
            "name": "Testcoin", "gecko_id": "testcoin",
            "categories": {"privateSale": ["Investors"], "insiders": ["Team"],
                           "noncirculating": ["Community"]},
            "documentedData": {
                "data": [
                    {"label": "Investors", "data": [{"timestamp": day(-30), "unlocked": 50e6},
                                                    {"timestamp": day(-1), "unlocked": 90e6},
                                                    {"timestamp": day(5), "unlocked": 100e6},
                                                    {"timestamp": day(20), "unlocked": 100e6}]},
                    {"label": "Team", "data": [{"timestamp": day(-1), "unlocked": 100e6},
                                               {"timestamp": day(10), "unlocked": 100e6}]},
                    {"label": "Community", "data": [{"timestamp": day(-1), "unlocked": 810e6},
                                                    {"timestamp": day(300), "unlocked": 900e6}]},
                ],
                "tokenAllocation": {"progress": {"privateSale": 90.0, "insiders": 100.0}},
            },
            "metadata": {"events": [
                {"description": "A cliff of {tokens[0]} tokens was unlocked from Investors on {timestamp}",
                 "timestamp": day(5), "noOfTokens": [10e6], "category": "privateSale",
                 "unlockType": "cliff"}]},
        }

    def test_cliff_and_vesting_end(self):
        news.load_config()
        conn = common.open_db(temp_db_path(), news.SCHEMA)
        summary = news.summarize_emission(self.schedule(time.time()))
        self.assertEqual(summary["unlocked_now"], 1000e6)
        self.assertAlmostEqual(summary["insider_left"], 10e6)
        with conn:
            conn.execute("INSERT INTO emissions VALUES ('testcoin', 1, ?)", (json.dumps(summary),))
        orig = news.current_prices
        news.current_prices = lambda ids: {"testcoin": 2.0}
        try:
            items = news.unlock_items(conn)
        finally:
            news.current_prices = orig
        texts = {i.key.split(":")[0]: i.text for i in items}
        self.assertIn("10.0M tokens ($20.0M, 1.0% of unlocked supply)", texts["cliff"])
        self.assertIn("investor and team vesting ends", texts["end"])
        self.assertIn("in 5 days", texts["end"])
        # Upcoming unlocks are news: sent even on the first run
        self.assertEqual(news.queue_new(conn, "unlocks", items, bootstrap=False), 2)
        self.assertEqual(news.queue_new(conn, "unlocks", items, bootstrap=False), 0)


if __name__ == "__main__":
    unittest.main()
