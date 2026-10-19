"""Yahoo Finance quotes and range windows. Stdlib only, safe to run on Vercel."""

from __future__ import annotations

import csv
import json
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent
CONSTITUENTS = ROOT / "data" / "constituents.csv"
USER_AGENT = "Mozilla/5.0"
EASTERN = ZoneInfo("America/New_York")
CACHE_SECONDS = 60
RANGE_PERIODS = {
    "7 day": 7,
    "1 month": 21,
    "3 month": 63,
}

_yahoo_lock = threading.Lock()
_yahoo_cache: dict = {"at": 0.0, "payload": None}


def price(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number <= 0:
        return None
    return number


def _range_deviation(ranges: list[float]) -> tuple[float, float]:
    count = len(ranges)
    average = sum(ranges) / count
    variance = sum((value - average) ** 2 for value in ranges) / count
    return average, variance ** 0.5


def load_constituents() -> list[dict]:
    with CONSTITUENTS.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _yahoo_get(url: str) -> dict:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def _yahoo_windows(ranges: list[float], periods: tuple[str, ...]) -> list[dict]:
    results = []
    for name in periods:
        count = RANGE_PERIODS[name]
        window = ranges[-count:]
        if len(window) < count:
            results.append(
                {
                    "period": name,
                    "days": len(window),
                    "error": f"Need {count} trading days, got {len(window)}.",
                }
            )
            continue
        average, deviation = _range_deviation(window)
        results.append(
            {
                "period": name,
                "days": count,
                "averageRange": average,
                "standardDeviation": deviation,
            }
        )
    return results


def _yahoo_quote_batch(symbols: list[str]) -> list[dict]:
    query = urllib.parse.urlencode(
        {"symbols": ",".join(symbols), "range": "1d", "interval": "1d"}
    )
    payload = _yahoo_get("https://query1.finance.yahoo.com/v7/finance/spark?" + query)
    spark = payload.get("spark") or {}
    if spark.get("error"):
        detail = spark["error"].get("description") or "Yahoo quote error"
        raise RuntimeError(detail)
    rows = []
    for item in spark.get("result") or []:
        meta = item["response"][0]["meta"]
        rows.append(meta)
    return rows


def _yahoo_ranges(symbol: str, periods: tuple[str, ...]) -> dict:
    yahoo = symbol.replace(".", "-")
    url = (
        "https://query1.finance.yahoo.com/v8/finance/chart/"
        + urllib.parse.quote(yahoo)
        + "?interval=1d&range=6mo"
    )
    payload = _yahoo_get(url)
    chart = payload.get("chart") or {}
    if chart.get("error"):
        detail = chart["error"].get("description") or f"No Yahoo history for {symbol}"
        raise RuntimeError(detail)
    result = (chart.get("result") or [None])[0]
    if not result:
        raise RuntimeError(f"No Yahoo history for {symbol}")
    quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    highs = quote.get("high") or []
    lows = quote.get("low") or []
    opens = quote.get("open") or []
    closes = quote.get("close") or []
    count = max(len(highs), len(lows), len(opens), len(closes))
    ranges = []
    session_open = None
    last_close = None
    for index in range(count):
        high_price = price(highs[index] if index < len(highs) else None)
        low_price = price(lows[index] if index < len(lows) else None)
        opened = price(opens[index] if index < len(opens) else None)
        closed = price(closes[index] if index < len(closes) else None)
        if opened is not None:
            session_open = opened
        if closed is not None:
            last_close = closed
        if high_price is None or low_price is None or high_price < low_price:
            continue
        ranges.append(high_price - low_price)
    windows = _yahoo_windows(ranges, periods)
    if not any("standardDeviation" in item for item in windows):
        raise RuntimeError(f"Yahoo returned {len(ranges)} daily bars for {symbol}.")
    last_price = price((result.get("meta") or {}).get("regularMarketPrice")) or last_close
    return {
        "symbol": symbol,
        "open": session_open,
        "last": last_price,
        "periods": windows,
        "source": "Yahoo",
    }


def _yahoo_book() -> dict:
    members = load_constituents()
    by_yahoo = {}
    for row in members:
        by_yahoo[row["Symbol"].replace(".", "-")] = row
    symbols = list(by_yahoo)
    batches = [symbols[index : index + 20] for index in range(0, len(symbols), 20)]
    metas = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(_yahoo_quote_batch, batch) for batch in batches]
        for future in as_completed(futures):
            metas.extend(future.result())

    quotes = []
    latest = 0
    for meta in metas:
        member = by_yahoo.get(meta.get("symbol"))
        last = price(meta.get("regularMarketPrice"))
        change = meta.get("regularMarketChangePercent")
        try:
            change = float(change)
        except (TypeError, ValueError):
            change = None
        if member is None or last is None or change is None or change != change:
            continue
        latest = max(latest, int(meta.get("regularMarketTime") or 0))
        quotes.append(
            {
                "symbol": member["Symbol"],
                "name": member["Security"],
                "price": last,
                "changePercent": change,
            }
        )
    if not quotes:
        raise RuntimeError("Yahoo returned no prices.")
    quotes.sort(key=lambda item: item["symbol"])
    as_of = None
    if latest:
        as_of = datetime.fromtimestamp(latest, EASTERN).isoformat(timespec="seconds")
    return {
        "asOf": as_of,
        "universe": len(members),
        "quoted": len(quotes),
        "source": "Yahoo",
        "marketData": "yahoo",
        "quotes": quotes,
        "complete": True,
    }


def getDataFromYahooFinance(
    symbol: str | None = None,
    periods: tuple[str, ...] = ("7 day", "1 month", "3 month"),
) -> dict:
    """Same mover book, or one stock's range windows, from Yahoo Finance.

    Call it with no symbol for the S&P 500 quote book. Pass a symbol for that
    name's 7-day, 1-month, and 3-month range standard deviations.
    """
    unknown = [name for name in periods if name not in RANGE_PERIODS]
    if unknown:
        raise ValueError("Unknown period: " + ", ".join(unknown))
    if symbol:
        return _yahoo_ranges(symbol.strip().upper(), periods)
    return _yahoo_book()


def yahoo_book(fresh: bool = False) -> dict:
    now = time.time()
    with _yahoo_lock:
        cached = _yahoo_cache["payload"]
        if cached is not None and not fresh and now - _yahoo_cache["at"] < CACHE_SECONDS:
            return cached
    payload = getDataFromYahooFinance()
    payload["cachedAt"] = time.time()
    with _yahoo_lock:
        _yahoo_cache["at"] = payload["cachedAt"]
        _yahoo_cache["payload"] = payload
    return payload
