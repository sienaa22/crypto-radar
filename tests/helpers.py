"""Shared test helpers: import path, temp databases, fake Telegram, local HTTP server."""

import json
import os
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Callable, Dict, List, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.dont_write_bytecode = True

# Every test run writes its signal ledger to a temporary file, never /opt/radar
os.environ["SIGNALS_DB"] = os.path.join(tempfile.mkdtemp(), "signals.db")
os.environ.pop("ANTHROPIC_API_KEY", None)

import common  # noqa: E402

# Tests never wait for retries or Telegram pacing
common.time.sleep = lambda s: None


def temp_db_path(name: str = "test.db") -> str:
    return os.path.join(tempfile.mkdtemp(), name)


class FakeTelegram(common.Telegram):
    def __init__(self, ok: bool = True) -> None:
        super().__init__("token", "1")
        self.ok = ok
        self.sent = []  # type: List[str]

    def send(self, text: str) -> bool:
        if self.ok:
            self.sent.append(text)
        return self.ok


# A handler returns (status, headers, body); body is JSON-encoded unless bytes
Handler = Callable[[str, Dict[str, str]], Tuple[int, Dict[str, str], object]]


class LocalServer:
    def __init__(self, handler: Handler) -> None:
        outer = self
        self.requests = []  # type: List[Tuple[str, Dict[str, str]]]

        class H(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                headers = {k.lower(): v for k, v in self.headers.items()}
                outer.requests.append((self.path, headers))
                status, resp_headers, body = handler(self.path, headers)
                data = body if isinstance(body, bytes) else json.dumps(body).encode()
                self.send_response(status)
                for k, v in resp_headers.items():
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(data) if status != 304 else 0))
                self.end_headers()
                if status != 304:
                    self.wfile.write(data)

            def log_message(self, *args: object) -> None:
                pass

        self.httpd = HTTPServer(("127.0.0.1", 0), H)
        self.url = "http://127.0.0.1:{}".format(self.httpd.server_port)
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
