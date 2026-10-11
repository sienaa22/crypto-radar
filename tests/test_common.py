import os
import unittest

from helpers import FakeTelegram, LocalServer, temp_db_path

import common


class EnvTest(unittest.TestCase):
    def test_parse_values(self) -> None:
        path = temp_db_path("e.env")
        with open(path, "w") as f:
            f.write("A=val # comment\nB=\"x # y\" # c\nexport C=1\nD='q'\nE=a#b\n# skipped\n")
        common.load_env_file(path)
        self.assertEqual([os.environ[k] for k in "ABCDE"], ["val", "x # y", "1", "q", "a#b"])

    def test_numbers(self) -> None:
        os.environ["T_NUM"] = "abc"
        self.assertEqual(common.env_float("T_NUM", 2.5), 2.5)
        os.environ["T_NUM"] = "3.9"
        self.assertEqual(common.env_int("T_NUM", 0), 3)


class FormatTest(unittest.TestCase):
    def test_usd(self) -> None:
        self.assertEqual(common.usd(1_300_000), "$1.3M")
        self.assertEqual(common.usd(-12_300, signed=True), "-$12K")
        self.assertEqual(common.usd(2.5e9, signed=True), "+$2.5B")

    def test_fnum_nan(self) -> None:
        self.assertEqual(common.fnum(float("nan"), 7.0), 7.0)
        self.assertEqual(common.fnum(None), 0.0)


class OutboxTest(unittest.TestCase):
    def test_sends_in_order_and_keeps_failures(self) -> None:
        conn = common.open_db(temp_db_path())
        with conn:
            for text in ("one", "two", "three"):
                common.outbox_add(conn, text)
        down = FakeTelegram(ok=False)
        self.assertEqual(common.outbox_flush(conn, down), 0)
        up = FakeTelegram()
        self.assertEqual(common.outbox_flush(conn, up), 3)
        self.assertEqual(up.sent, ["one", "two", "three"])
        self.assertEqual(common.outbox_flush(conn, up), 0)
        self.assertEqual(common.outbox_count_since(conn, 0), 3)

    def test_daily_due_once_per_day(self) -> None:
        conn = common.open_db(temp_db_path())
        self.assertTrue(common.daily_due(conn, "k", 0))
        self.assertFalse(common.daily_due(conn, "k", 0))
        self.assertFalse(common.daily_due(conn, "other", 24))


class HttpTest(unittest.TestCase):
    def test_retry_then_success_and_404(self) -> None:
        calls = {"n": 0}

        def handler(path, headers):
            if path == "/missing":
                return 404, {}, {}
            calls["n"] += 1
            if calls["n"] == 1:
                return 503, {}, {}
            return 200, {"X-Thing": "yes"}, {"ok": True}

        server = LocalServer(handler)
        try:
            data, headers = common.http_request(server.url + "/flaky")
            self.assertEqual(data, {"ok": True})
            self.assertEqual(headers["x-thing"], "yes")
            self.assertEqual(calls["n"], 2)
            with self.assertRaises(common.NotFound):
                common.http_json(server.url + "/missing")
        finally:
            server.close()


if __name__ == "__main__":
    unittest.main()
