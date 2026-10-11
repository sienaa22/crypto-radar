import json
import random
import struct
import unittest

from helpers import temp_db_path

import chain
import common
import news
import pulse
from test_news import fake_coins

MIN = 60 * 1000
FUND = "0x" + "f1" * 20
TRADER = "0x" + "a1" * 20
EXCHANGE = "0x" + "e1" * 20
STRANGER = "0x" + "99" * 20
USDC = "0x" + "c0" * 20
NEWTOKEN = "0x" + "77" * 20


def ledger_rows():
    return common.signals_db().execute("SELECT kind, direction, symbol FROM signals ORDER BY id").fetchall()


class PrelistingTest(unittest.TestCase):
    def setUp(self):
        news.load_config()
        self.orig = news.get_json

    def tearDown(self):
        news.get_json = self.orig

    def test_filters(self):
        responses = {
            news.BINANCE_FUTURES_INFO: {"symbols": [
                {"symbol": "NEWUSDT", "baseAsset": "NEW", "status": "PENDING_TRADING", "underlyingType": "COIN"},
                {"symbol": "TSLAUSDT", "baseAsset": "TSLA", "status": "PENDING_TRADING", "underlyingType": "EQUITY"},
                {"symbol": "BTCUSDT", "baseAsset": "BTC", "status": "TRADING", "underlyingType": "COIN"}]},
            news.BYBIT_PRELAUNCH.format("spot"): {"result": {"list": [
                {"symbol": "BTCUSDT", "baseCoin": "BTC", "status": "Trading"},
                {"symbol": "ABCUSDT", "baseCoin": "ABC", "status": "PreLaunch", "launchTime": "1791800000000"}]}},
            news.BYBIT_PRELAUNCH.format("linear"): {"result": {"list": []}},
            news.OKX_INSTRUMENTS.format("SPOT"): {"data": [
                {"instId": "XYZ-USDT", "state": "preopen", "instCategory": "1", "baseCcy": "XYZ"},
                {"instId": "XFLY-USDT", "state": "preopen", "instCategory": "", "baseCcy": ""},
                {"instId": "ETH-USDT", "state": "live", "instCategory": "1", "baseCcy": "ETH"}]},
            news.OKX_INSTRUMENTS.format("SWAP"): {"data": []},
        }
        news.get_json = lambda url, timeout=60: responses[url]
        coins = fake_coins()
        self.assertEqual([i.key for i in news.binance_futures_prelist(coins)], ["NEWUSDT"])
        self.assertEqual([i.key for i in news.bybit_prelaunch(coins)], ["spot:ABCUSDT"])
        okx = news.okx_preopen(coins)
        self.assertEqual([i.key for i in okx], ["XYZ-USDT"])
        self.assertIn("[PRE-LISTING] OKX: XYZ market created before trading opens", okx[0].text)
        self.assertEqual(okx[0].signal, ("prelisting_okx", 1, None, "XYZ"))


class WalletTest(unittest.TestCase):
    def setUp(self):
        with common.signals_db() as sig:
            sig.execute("DELETE FROM signals")
        chain.load_config()
        self.conn = common.open_db(temp_db_path(), chain.SCHEMA)
        self.book = {FUND: ("fund", "Some Capital"), TRADER: ("trader", "HL whale"),
                     EXCHANGE: ("exchange", "Binance 14")}
        self.rpc = chain.Rpc("ethereum")
        tokens = {USDC: ("USDC", 6, 1.0), NEWTOKEN: ("ZETA", 18, None)}
        self.orig = chain.token_info, chain.native_price
        chain.token_info = lambda conn, rpc, address: tokens[address]
        chain.native_price = lambda conn: 2500.0

    def tearDown(self):
        chain.token_info, chain.native_price = self.orig

    def judge(self, token, sender, receiver, amount, tx):
        chain.judge_transfer(self.conn, self.rpc, self.book, token, sender, receiver, amount, tx)

    def outbox(self):
        return [r[0] for r in self.conn.execute("SELECT text FROM outbox ORDER BY id")]

    def test_rules(self):
        self.judge(NEWTOKEN, STRANGER, FUND, 5_000_000 * 10 ** 18, "0x01")       # early token
        self.judge(NEWTOKEN, STRANGER, FUND, 1 * 10 ** 18, "0x02")               # same token again: no
        self.judge(USDC, FUND, STRANGER, 400_000 * 10 ** 6, "0x03")              # funding
        self.judge(USDC, FUND, EXCHANGE, 2_000_000 * 10 ** 6, "0x04")            # to exchange
        self.judge(None, EXCHANGE, TRADER, 800 * 10 ** 18, "0x05")               # ETH out of exchange, $2M
        self.judge(USDC, FUND, STRANGER, 1_000 * 10 ** 6, "0x06")                # too small
        texts = self.outbox()
        self.assertEqual(len(texts), 4, texts)
        self.assertIn("[EARLY TOKEN] Some Capital [fund] received 5,000,000 ZETA", texts[0])
        self.assertIn("[FUNDING?] Some Capital [fund] sent $400K", texts[1])
        self.assertIn("[TO EXCHANGE] Some Capital [fund] sent $2.0M of USDC to Binance 14", texts[2])
        self.assertIn("[FROM EXCHANGE] HL whale [trader] withdrew $2.0M of ETH", texts[3])
        self.assertEqual([r[:2] for r in ledger_rows()],
                         [("early_token", 0), ("fund_payment", 0), ("wallet_to_exchange", -1),
                          ("wallet_from_exchange", 1)])

    def test_spam_tokens_are_ignored(self):
        chain.token_info = lambda conn, rpc, address: ("visit-claim.com", 18, None)
        self.judge(NEWTOKEN, STRANGER, FUND, 10 ** 24, "0x07")
        self.assertEqual(self.outbox(), [])

    def test_decode_and_user_file(self):
        encoded = "0x" + "20".rjust(64, "0") + "4".rjust(64, "0") + "5553444300".ljust(64, "0")
        self.assertEqual(chain.decode_string(encoded), "USDC")
        self.assertEqual(chain.decode_string("0x" + "4d4b52".ljust(64, "0")), "MKR")
        path = temp_db_path("wallets.txt")
        chain.CFG["wallets_file"] = path
        chain.add_wallet(path, "0x" + "AB" * 20, "fund", "Paradigm")
        chain.load_user_wallets(self.conn)
        self.assertEqual(chain.wallets(self.conn)["0x" + "ab" * 20], ("fund", "Paradigm"))


class PulseTest(unittest.TestCase):
    def setUp(self):
        with common.signals_db() as sig:
            sig.execute("DELETE FROM signals")
        pulse.load_config()
        pulse.CFG["brain_db"] = temp_db_path("missing.db")
        self.conn = common.open_db(temp_db_path(), pulse.SCHEMA)

    def prices(self, path):
        p = pulse.Prices()
        start = common.now_ms() - len(path) * MIN
        for i, price in enumerate(path):
            p.hist["ABCUSDT"].append((start + i * MIN, price))
            p.hist["BTCUSDT"].append((start + i * MIN, 80000.0))
        p.volume = {"ABCUSDT": 5e7, "BTCUSDT": 1e10}
        p.volume_ms = common.now_ms()
        return p

    def test_pump_with_context(self):
        common.record_signal("news", "listing_upbit", "Upbit lists ABC", 1, symbol="ABC")
        calm = [100.0 + (0.05 if i % 2 else -0.05) for i in range(60)]
        p = self.prices(calm + [101, 104, 108, 110, 112])
        self.assertEqual(pulse.check_spikes(self.conn, p), 1)
        text = self.conn.execute("SELECT text FROM outbox").fetchone()[0]
        self.assertIn("[PUMP 🚀] ABC +11.9% in 5 min", text)
        self.assertIn("listing_upbit: Upbit lists ABC", text)
        self.assertEqual(pulse.check_spikes(self.conn, p), 0)  # cooldown
        self.assertIn(("pump", 1, "ABC"), ledger_rows())

    def test_noisy_coin_needs_a_bigger_move(self):
        # A coin that swings about 3% a minute: a 6% jump is normal for it
        rng = random.Random(7)
        wild = [100.0]
        for _ in range(90):
            wild.append(wild[-1] * (1 + rng.gauss(0, 0.03)))
        p = self.prices(wild + [wild[-1] * 1.06])
        self.assertEqual(pulse.check_spikes(self.conn, p), 0)

    def test_liquidations(self):
        liq = pulse.Liquidations()
        now = common.now_ms()
        events = [(now, "BTCUSDT", "LONG", 3_000_000, 80000.0)] + \
                 [(now, "ETHUSDT", "SHORT", 1_000_000, 2500.0) for _ in range(16)]
        self.assertEqual(liq.check(self.conn, events), 2)
        texts = [r[0] for r in self.conn.execute("SELECT text FROM outbox ORDER BY id")]
        self.assertIn("[BIG LIQUIDATION] BTC LONG position of $3.0M", texts[0])
        self.assertIn("[LIQUIDATION CASCADE] ETH shorts: at least $16.0M", texts[1])

    def test_websocket_frame_parsing(self):
        stream = pulse.LiquidationStream()
        msg = json.dumps({"e": "forceOrder", "o": {"s": "SOLUSDT", "S": "BUY", "z": "1000",
                                                   "ap": "150", "T": 1}}).encode()
        stream.handle(msg)
        self.assertEqual(stream.take(), [(1, "SOLUSDT", "SHORT", 150000.0, 150.0)])
        # Length encoding used by the reader for medium frames
        self.assertEqual(struct.unpack(">H", struct.pack(">H", 300))[0], 300)


if __name__ == "__main__":
    unittest.main()
