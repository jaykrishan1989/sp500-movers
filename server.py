"""S&P 500 movers page. Quotes come from IBKR. Run: python server.py"""

from __future__ import annotations

import csv
import json
import queue
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from datetime import datetime
from zoneinfo import ZoneInfo
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from ibkr import connectToIBKR
from yahoo_finance import RANGE_PERIODS, getDataFromYahooFinance, return_deviations, yahoo_book

try:
    from ib_async import Contract, Stock
except ImportError:
    from ib_insync import Contract, Stock

ROOT = Path(__file__).resolve().parent
CONSTITUENTS = ROOT / "data" / "constituents.csv"
CONTRACTS = ROOT / "data" / "ibkr-contracts.json"
CSV_URL = (
    "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/"
    "main/data/constituents.csv"
)
USER_AGENT = "Mozilla/5.0"
HOST = "127.0.0.1"
PORT = 8780
EASTERN = ZoneInfo("America/New_York")
CLIENT_ID = 17
CACHE_SECONDS = 60
# Stay under the ~100 market-data line cap even when cancels are still in flight.
STREAM_WINDOW = 10
INFO_CODES = {
    300,
    2103,
    2104,
    2105,
    2106,
    2107,
    2108,
    2119,
    2157,
    2158,
    10089,
    10090,
    10167,
}

_cv = threading.Condition()
_cache: dict = {
    "at": 0.0,
    "payload": None,
    "complete": False,
    "building": False,
    "generation": 0,
    "error": None,
}
_market_data_type: dict = {"value": 0}


def http_get(url: str, timeout: float = 20) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def refresh_constituents() -> None:
    age = time.time() - CONSTITUENTS.stat().st_mtime if CONSTITUENTS.exists() else 10**12
    if age < 7 * 24 * 3600:
        return
    try:
        data = http_get(CSV_URL, timeout=20)
    except (urllib.error.URLError, TimeoutError, OSError):
        return
    first = data.splitlines()[0] if data else b""
    if b"Symbol" not in first:
        return
    CONSTITUENTS.parent.mkdir(parents=True, exist_ok=True)
    CONSTITUENTS.write_bytes(data)


def load_constituents() -> list[dict]:
    refresh_constituents()
    with CONSTITUENTS.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def load_contract_cache() -> dict:
    if not CONTRACTS.exists():
        return {}
    try:
        return json.loads(CONTRACTS.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def save_contract_cache(cache: dict) -> None:
    CONTRACTS.write_text(json.dumps(cache, indent=2), encoding="utf-8")


def price(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number <= 0:
        return None
    return number


def _bar_day(value) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    return str(value)[:10]


def calculateSD(
    ib,
    symbol: str,
    periods: tuple[str, ...] = ("7 day", "1 month", "3 month"),
) -> dict:
    """Sample standard deviation of daily simple returns for each period.

    One history request covers every period. Each window is the last N
    close-to-close returns, and the deviation divides by N - 1.
    """
    symbol = symbol.strip().upper()
    unknown = [name for name in periods if name not in RANGE_PERIODS]
    if unknown:
        raise ValueError("Unknown period: " + ", ".join(unknown))

    cache = load_contract_cache()
    saved = cache.get(symbol) or {}
    con_id = saved.get("conId")
    if con_id:
        contract = Contract(conId=int(con_id), exchange="SMART")
    else:
        contract = Stock(symbol.replace(".", " "), "SMART", "USD")
        qualified = ib.qualifyContracts(contract)
        if not qualified or not contract.conId:
            raise ValueError(f"No IBKR contract for {symbol}")
        cache[symbol] = {"conId": int(contract.conId), "symbol": contract.symbol}
        save_contract_cache(cache)
        contract = Contract(conId=int(contract.conId), exchange="SMART")

    needed = max(RANGE_PERIODS[name] for name in periods)
    duration = "6 M" if needed > 21 else "2 M" if needed > 7 else "21 D"
    bars = ib.reqHistoricalData(
        contract,
        endDateTime="",
        durationStr=duration,
        barSizeSetting="1 day",
        whatToShow="TRADES",
        useRTH=True,
        formatDate=1,
        timeout=30,
    )
    closes = [price(bar.close) for bar in bars or []]
    results = return_deviations(closes, periods)
    if not any("standardDeviation" in item for item in results):
        raise RuntimeError(f"IBKR returned {len(closes)} daily bars for {symbol}.")
    return {"symbol": symbol, "periods": results}


def choose_market_data(ib) -> int:
    """Use live prices when the API session has them, otherwise delayed."""
    chosen = _market_data_type["value"]
    if chosen:
        ib.reqMarketDataType(chosen)
        return chosen

    blocked = {"value": False}

    def on_error(req_id, code, message, contract):
        if code in (354, 10089, 10167):
            blocked["value"] = True

    ib.errorEvent += on_error
    contract = None
    try:
        ib.reqMarketDataType(1)
        qualified = ib.qualifyContracts(Stock("AAPL", "SMART", "USD"))
        contract = qualified[0] if qualified else Stock("AAPL", "SMART", "USD")
        ticker = ib.reqMktData(contract, "", False, False)
        ticker.last = float("nan")
        ticker.close = float("nan")
        deadline = time.monotonic() + 1.5
        while time.monotonic() < deadline and not blocked["value"]:
            ib.sleep(0.05)
            if price(ticker.last) is not None and price(ticker.close) is not None:
                break
        live = (
            not blocked["value"]
            and price(ticker.last) is not None
            and price(ticker.close) is not None
        )
    finally:
        if contract is not None and getattr(contract, "conId", 0):
            ib.cancelMktData(contract)
        ib.errorEvent -= on_error

    chosen = 1 if live else 3
    ib.reqMarketDataType(chosen)
    _market_data_type["value"] = chosen
    label = "live" if chosen == 1 else "delayed"
    print(f"IBKR market data: {label}", flush=True)
    return chosen


def resolve_contracts(ib, members: list[dict]) -> tuple[list[tuple[dict, object]], int]:
    cache = load_contract_cache()
    paired: list[tuple[dict, object]] = []
    missing: list[tuple[dict, object]] = []
    for row in members:
        saved = cache.get(row["Symbol"]) or {}
        con_id = saved.get("conId")
        if con_id:
            paired.append((row, Contract(conId=int(con_id), exchange="SMART")))
        else:
            missing.append((row, Stock(row["Symbol"].replace(".", " "), "SMART", "USD")))

    qualified = 0
    if missing:
        for start in range(0, len(missing), 50):
            chunk = missing[start : start + 50]
            ib.qualifyContracts(*[contract for _, contract in chunk])
            for row, contract in chunk:
                if not contract.conId:
                    continue
                qualified += 1
                cache[row["Symbol"]] = {
                    "conId": int(contract.conId),
                    "symbol": contract.symbol,
                }
                paired.append((row, Contract(conId=int(contract.conId), exchange="SMART")))
        save_contract_cache(cache)
    return paired, qualified


def build_book(ib, on_partial=None) -> dict:
    started = time.perf_counter()
    members = load_constituents()
    paired, qualified = resolve_contracts(ib, members)
    contracts_done = time.perf_counter()
    market_data = choose_market_data(ib)
    market_done = time.perf_counter()
    quotes, latest, snapshot_hits, streamed = collect_quotes(
        ib, paired, market_data, len(members), on_partial
    )
    finished = time.perf_counter()
    print(
        f"IBKR profile contracts={contracts_done - started:.3f}s "
        f"qualified={qualified} marketData={market_done - contracts_done:.3f}s "
        f"collect={finished - market_done:.3f}s total={finished - started:.3f}s "
        f"snapshot={snapshot_hits} streamed={streamed} "
        f"quoted={len(quotes)}/{len(members)}",
        flush=True,
    )
    if not quotes:
        raise RuntimeError(
            "IBKR returned no prices. Delayed US stock data must be enabled "
            "for the API, or the session needs a US equity market data subscription."
        )
    return _payload(quotes, latest, len(members), market_data, complete=True)


def _payload(quotes: dict, latest: datetime | None, universe: int, market_data: int, complete: bool) -> dict:
    items = sorted(quotes.values(), key=lambda item: item["symbol"])
    return {
        "asOf": latest.isoformat() if latest else None,
        "universe": universe,
        "quoted": len(items),
        "source": "IBKR",
        "marketData": "live" if market_data == 1 else "delayed",
        "quotes": items,
        "complete": complete,
    }


def collect_quotes(ib, paired, market_data: int, universe: int, on_partial) -> tuple[dict, datetime | None, int, int]:
    """Request every name, record (last - close) / close, and free lines quickly.

    Snapshots do not sit on a market-data line, so the whole universe goes out
    at the API message pace. Names still missing after that pass are streamed
    and cancelled as soon as last and close are both valid.
    """
    quotes: dict = {}
    latest: datetime | None = None
    started = time.perf_counter()
    last_publish = 0.0
    published_count = -1

    def remember(row, ticker) -> bool:
        nonlocal latest
        last = price(ticker.last)
        close = price(ticker.close)
        if last is None or close is None:
            return False
        if ticker.time is not None and (latest is None or ticker.time > latest):
            latest = ticker.time
        quotes[row["Symbol"]] = {
            "symbol": row["Symbol"],
            "name": row["Security"],
            "price": last,
            "changePercent": (last - close) / close * 100,
        }
        return True

    def publish(force: bool = False) -> None:
        nonlocal last_publish, published_count
        if on_partial is None or not quotes:
            return
        now = time.monotonic()
        if not force and len(quotes) == published_count and now - last_publish < 0.25:
            return
        last_publish = now
        published_count = len(quotes)
        on_partial(_payload(quotes, latest, universe, market_data, complete=False))

    mode = _session_mode(ib, paired[0][1] if paired else None)
    snapshot_hits = 0
    streamed = 0
    if mode == "last":
        snapshot_hits = _collect_snapshots(ib, paired, remember, publish, started)
        missing = [(row, contract) for row, contract in paired if row["Symbol"] not in quotes]
        if missing:
            before = len(quotes)
            _collect_stream(ib, missing, quotes, remember, publish, started)
            streamed = len(quotes) - before
    else:
        print("Market is closed; using the last two daily closes", flush=True)

        def note(row, px, change, when) -> None:
            nonlocal latest
            if when is not None and (latest is None or when > latest):
                latest = when
            quotes[row["Symbol"]] = {
                "symbol": row["Symbol"],
                "name": row["Security"],
                "price": px,
                "changePercent": change,
            }

        snapshot_hits = _collect_history(ib, paired, note, publish)
    publish(force=True)
    print(
        f"IBKR quotes {len(quotes)} / {len(paired)} ({time.perf_counter() - started:.1f}s)",
        flush=True,
    )
    return quotes, latest, snapshot_hits, streamed


def _session_mode(ib, contract) -> str:
    """Live and open sessions report a last trade. A closed session reports last as 0."""
    if contract is None:
        return "history"
    ticker = ib.reqMktData(contract, "", False, False)
    deadline = time.monotonic() + 2.0
    mode = "history"
    while time.monotonic() < deadline:
        ib.sleep(0.1)
        if price(ticker.last) is not None:
            mode = "last"
            break
        if ticker.last == 0 and price(ticker.close) is not None:
            break
    ib.cancelMktData(contract)
    return mode


def _bar_time(value) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(hour=16, minute=0, second=0, microsecond=0, tzinfo=EASTERN)
        return value
    if hasattr(value, "year"):
        return datetime(value.year, value.month, value.day, 16, 0, tzinfo=EASTERN)
    return None


def _collect_history(ib, paired, note, publish) -> int:
    """Session change from the last two regular daily closes. Used when the market is shut."""
    hits = 0
    total = len(paired)
    for row, contract in paired:
        _serve_pending_ranges(ib)
        bars = None
        for _attempt in range(3):
            try:
                bars = ib.reqHistoricalData(
                    contract,
                    endDateTime="",
                    durationStr="4 D",
                    barSizeSetting="1 day",
                    whatToShow="TRADES",
                    useRTH=True,
                    formatDate=1,
                    timeout=8,
                )
                break
            except Exception as exc:
                message = str(exc).lower()
                if "pacing" in message or "162" in message:
                    ib.sleep(2.0)
                    continue
                bars = None
                break
        if not bars or len(bars) < 2:
            continue
        previous, latest_bar = bars[-2], bars[-1]
        if previous.close is None or latest_bar.close is None:
            continue
        if previous.close <= 0 or latest_bar.close <= 0:
            continue
        change = (latest_bar.close - previous.close) / previous.close * 100
        note(row, float(latest_bar.close), change, _bar_time(latest_bar.date))
        hits += 1
        if hits % 25 == 0:
            publish()
            print(f"IBKR daily bars {hits} / {total}", flush=True)
    return hits


def _collect_snapshots(ib, paired, remember, publish, started) -> int:
    """Take snapshots in small batches so they finish and release their lines."""
    hits = 0
    batch_size = 40
    ib.RequestTimeout = 20
    start = 0
    while start < len(paired):
        if time.perf_counter() - started > 80:
            break
        _serve_pending_ranges(ib)
        chunk = paired[start : start + batch_size]
        try:
            tickers = list(ib.reqTickers(*[contract for _, contract in chunk]))
        except TimeoutError:
            if batch_size == 40:
                batch_size = 15
                print("IBKR snapshot batch timed out; using groups of 15", flush=True)
                continue
            print("IBKR snapshot group timed out; skipping it", flush=True)
            start += batch_size
            continue
        for (row, _), ticker in zip(chunk, tickers):
            if remember(row, ticker):
                hits += 1
        start += batch_size
        publish()
    return hits


def _collect_stream(ib, missing, quotes, remember, publish, started) -> None:
    pending = deque((row, contract, 1) for row, contract in missing)
    active: dict = {}
    window = STREAM_WINDOW
    pause_until = 0.0

    def on_error(req_id, code, message, contract):
        nonlocal window, pause_until
        if code in INFO_CODES:
            return
        if code == 101:
            window = max(15, window - 10)
            pause_until = time.monotonic() + 0.5
            print(f"IBKR market-data line cap, stream window {window}", flush=True)
            con_id = getattr(contract, "conId", 0) if contract is not None else 0
            if con_id in active:
                row, held, ticker, t0, attempt = active.pop(con_id)
                pending.appendleft((row, held, attempt))
                ib.cancelMktData(held)
        elif code == 100:
            pause_until = time.monotonic() + 1.0
            print("IBKR pacing; slowing stream requests", flush=True)

    ib.errorEvent += on_error
    try:
        while pending or active:
            if time.perf_counter() - started > 90:
                print("IBKR stream stopped after the collection budget", flush=True)
                break
            _serve_pending_ranges(ib)
            if time.monotonic() < pause_until:
                ib.sleep(0.05)
                _harvest_stream(ib, active, pending, quotes, remember)
                publish()
                continue
            while pending and len(active) < window:
                row, contract, attempt = pending.popleft()
                if row["Symbol"] in quotes:
                    continue
                ticker = ib.reqMktData(contract, "", False, False)
                active[contract.conId] = (row, contract, ticker, time.monotonic(), attempt)
            ib.sleep(0.02)
            _harvest_stream(ib, active, pending, quotes, remember)
            publish()
    finally:
        for row, contract, ticker, t0, attempt in list(active.values()):
            ib.cancelMktData(contract)
        active.clear()
        ib.errorEvent -= on_error


def _harvest_stream(ib, active: dict, pending, quotes: dict, remember) -> None:
    now = time.monotonic()
    finished = []
    for con_id, (row, contract, ticker, t0, attempt) in active.items():
        if remember(row, ticker):
            finished.append((con_id, False))
        elif now - t0 > 6:
            finished.append((con_id, attempt < 2 and row["Symbol"] not in quotes))
    for con_id, retry in finished:
        row, contract, ticker, t0, attempt = active.pop(con_id)
        ib.cancelMktData(contract)
        if retry:
            pending.append((row, contract, attempt + 1))


class QuoteService:
    """One IBKR session, owned by a single thread. Uses connectToIBKR()."""

    def __init__(self) -> None:
        self._jobs: queue.Queue = queue.Queue()
        self._ready = threading.Event()
        self._error: Exception | None = None
        self._thread = threading.Thread(target=self._loop, name="ibkr", daemon=True)

    def start(self) -> None:
        with _cv:
            _cache["building"] = True
        self._thread.start()

    def wait_until_ready(self, timeout: float = 180) -> None:
        if not self._ready.wait(timeout):
            raise TimeoutError("IBKR quotes are still loading.")
        with _cv:
            if _cache["payload"] is None and (self._error is not None or _cache["error"] is not None):
                raise self._error or _cache["error"]

    def book_fresh(self) -> dict:
        target = self.kick()
        deadline = time.time() + 180
        with _cv:
            while not (
                _cache["generation"] >= target
                and _cache["complete"]
                and _cache["payload"] is not None
            ):
                if _cache["error"] is not None and not _cache["building"]:
                    if _cache["payload"] is not None and _cache["complete"]:
                        break
                    raise _cache["error"]
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise TimeoutError("IBKR quotes are still loading.")
                _cv.wait(remaining)
            return _view()

    def kick(self) -> int:
        with _cv:
            fresh = (
                _cache["payload"] is not None
                and _cache["complete"]
                and not _cache["building"]
                and time.time() - _cache["at"] < CACHE_SECONDS
            )
            if fresh:
                return _cache["generation"]
            target = _cache["generation"] + 1
            if not _cache["building"]:
                _cache["building"] = True
                _cache["error"] = None
                self._error = None
                self._jobs.put("refresh")
            return target

    def drain_ranges(self, ib) -> None:
        deferred = []
        while True:
            try:
                item = self._jobs.get_nowait()
            except queue.Empty:
                break
            if isinstance(item, tuple) and item and item[0] == "range":
                _, symbol, reply = item
                try:
                    reply.put(("ok", calculateSD(ib, symbol)))
                    print(f"IBKR range {symbol}", flush=True)
                except Exception as exc:
                    reply.put(("err", exc))
                    print(f"IBKR range {symbol} failed: {exc}", flush=True)
            else:
                deferred.append(item)
        for item in deferred:
            self._jobs.put(item)

    def range_for(self, symbol: str) -> dict:
        reply: queue.Queue = queue.Queue(1)
        self._jobs.put(("range", symbol, reply))
        try:
            status, value = reply.get(timeout=150)
        except queue.Empty as exc:
            raise TimeoutError(f"IBKR range for {symbol} timed out.") from exc
        if status == "err":
            raise value
        return value

    def stop(self) -> None:
        if not self._thread.is_alive():
            return
        self._jobs.put("stop")
        self._thread.join(timeout=5)

    def _on_partial(self, payload: dict) -> None:
        with _cv:
            if _cache["complete"] and _cache["payload"] is not None:
                return
            _cache["payload"] = payload
            _cache["complete"] = False
            _cv.notify_all()
        self._ready.set()

    def _fail(self, exc: Exception) -> None:
        print(f"IBKR quote load failed: {exc}", flush=True)
        self._error = exc
        with _cv:
            _cache["error"] = exc
            _cache["building"] = False
            _cv.notify_all()
        self._ready.set()

    def _build(self, ib) -> None:
        with _cv:
            _cache["building"] = True
            _cache["error"] = None
        self._error = None
        payload = build_book(ib, self._on_partial)
        with _cv:
            _cache["payload"] = payload
            _cache["complete"] = True
            _cache["at"] = time.time()
            _cache["building"] = False
            _cache["generation"] += 1
            _cv.notify_all()
        self._ready.set()
        print(
            f"IBKR book ready: {payload['quoted']} of {payload['universe']} "
            f"({payload['marketData']})",
            flush=True,
        )

    def _loop(self) -> None:
        ib = None
        try:
            print("Connecting to IBKR…", flush=True)
            ib = connectToIBKR(client_id=CLIENT_ID, readonly=True)
            self._build(ib)
        except Exception as exc:
            self._fail(exc)

        if ib is None:
            return

        while True:
            item = self._jobs.get()
            if item == "stop":
                break
            if isinstance(item, tuple) and item[0] == "range":
                _, symbol, reply = item
                try:
                    if not ib.isConnected():
                        ib = connectToIBKR(client_id=CLIENT_ID, readonly=True)
                    reply.put(("ok", calculateSD(ib, symbol)))
                except Exception as exc:
                    reply.put(("err", exc))
                continue
            try:
                if not ib.isConnected():
                    ib = connectToIBKR(client_id=CLIENT_ID, readonly=True)
                self._build(ib)
            except Exception as exc:
                self._fail(exc)

        if ib.isConnected():
            ib.disconnect()


def _view() -> dict:
    payload = _cache["payload"]
    body = dict(payload)
    body["complete"] = bool(_cache["complete"])
    body["refreshing"] = bool(_cache["building"])
    body["cachedAt"] = _cache["at"] if _cache["complete"] else 0
    return body


service = QuoteService()


def _serve_pending_ranges(ib) -> None:
    """Run queued ticker-range jobs without waiting for the whole book."""
    service.drain_ranges(ib)


def get_book(fresh: bool = False) -> dict:
    service.wait_until_ready()
    if fresh:
        return service.book_fresh()
    with _cv:
        fresh_enough = (
            _cache["payload"] is not None
            and _cache["complete"]
            and time.time() - _cache["at"] < CACHE_SECONDS
        )
        if fresh_enough:
            return _view()
        has_payload = _cache["payload"] is not None
    if has_payload:
        service.kick()
        with _cv:
            return _view()
    return service.book_fresh()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt: str, *args) -> None:
        print(f"{self.address_string()} {fmt % args}", flush=True)

    def do_GET(self) -> None:
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path in ("/", "/index.html"):
            body = (ROOT / "index.html").read_bytes()
            self._send(200, "text/html; charset=utf-8", body)
            return
        query = urllib.parse.parse_qs(parsed.query)
        source = query.get("source", ["ibkr"])[0].strip().lower()
        if parsed.path == "/api/quotes":
            fresh = query.get("fresh", ["0"])[0] == "1"
            try:
                payload = yahoo_book(fresh=fresh) if source == "yahoo" else get_book(fresh=fresh)
                body = json.dumps(payload).encode("utf-8")
            except Exception as exc:
                body = json.dumps({"error": str(exc)}).encode("utf-8")
                self._send(502, "application/json; charset=utf-8", body)
                return
            self._send(200, "application/json; charset=utf-8", body)
            return
        if parsed.path == "/api/range":
            symbol = query.get("symbol", [""])[0].strip().upper()
            if not symbol:
                self._send(400, "application/json; charset=utf-8", b'{"error":"Missing symbol."}')
                return
            try:
                payload = (
                    getDataFromYahooFinance(symbol)
                    if source == "yahoo"
                    else service.range_for(symbol)
                )
                body = json.dumps(payload).encode("utf-8")
            except Exception as exc:
                body = json.dumps({"error": str(exc)}).encode("utf-8")
                self._send(502, "application/json; charset=utf-8", body)
                return
            self._send(200, "application/json; charset=utf-8", body)
            return
        self._send(404, "text/plain; charset=utf-8", b"Not found")

    def _send(self, status: int, content_type: str, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    service.start()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"S&P 500 movers at http://{HOST}:{PORT}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.", flush=True)
    finally:
        service.stop()
        server.server_close()


if __name__ == "__main__":
    main()
