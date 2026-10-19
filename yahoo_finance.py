"""Yahoo Finance quotes and return standard deviations. Stdlib only, safe to run on Vercel."""

from __future__ import annotations

import csv
import json
import statistics
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
# Trading sessions, not calendar days. One month is about 21 sessions.
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


def _simple_returns(closes: list[float | None]) -> list[float]:
    """Close-to-close simple returns. A missing close breaks the chain."""
    returns = []
    previous = None
    for close in closes:
        if close is None:
            previous = None
            continue
        if previous is not None:
            returns.append((close - previous) / previous)
        previous = close
    return returns


def return_deviations(closes: list[float | None], periods: tuple[str, ...]) -> list[dict]:
    """Sample standard deviation of simple returns for each named period.

    A period of N trading days is the last N close-to-close returns. The
    deviation is the sample standard deviation, so it divides by N - 1.
    """
    unknown = [name for name in periods if name not in RANGE_PERIODS]
    if unknown:
        raise ValueError("Unknown period: " + ", ".join(unknown))
    returns = _simple_returns(closes)
    results = []
    for name in periods:
        count = RANGE_PERIODS[name]
        window = returns[-count:]
        if len(window) < count:
            results.append(
                {
                    "period": name,
                    "days": len(window),
                    "error": f"Need {count} daily returns, got {len(window)}.",
                }
            )
            continue
        results.append(
            {
                "period": name,
                "days": count,
                "standardDeviation": statistics.stdev(window),
            }
        )
    return results


def load_constituents() -> list[dict]:
    with CONSTITUENTS.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _yahoo_get(url: str) -> dict:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


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
    closes = [price(value) for value in (quote.get("close") or [])]
    windows = return_deviations(closes, periods)
    if not any("standardDeviation" in item for item in windows):
        raise RuntimeError(f"Yahoo returned {len(closes)} daily bars for {symbol}.")
    return {
        "symbol": symbol,
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
    """Same mover book, or one stock's return standard deviations, from Yahoo Finance.

    Call it with no symbol for the S&P 500 quote book. Pass a symbol for that
    name's 7-day, 1-month, and 3-month sample standard deviations of returns.
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
