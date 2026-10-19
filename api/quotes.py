"""Vercel route: /api/quotes"""

from __future__ import annotations

import json
import sys
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from yahoo_finance import yahoo_book


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        query = parse_qs(urlsplit(self.path).query)
        source = query.get("source", ["yahoo"])[0].strip().lower()
        if source == "ibkr":
            self._send(
                400,
                {
                    "error": "IBKR quotes stay on the computer that runs Trader Workstation. Choose Yahoo on this site."
                },
            )
            return
        try:
            fresh = query.get("fresh", ["0"])[0] == "1"
            self._send(200, yahoo_book(fresh=fresh))
        except Exception as exc:
            self._send(502, {"error": str(exc)})

    def _send(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)
