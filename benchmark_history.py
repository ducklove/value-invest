"""Daily benchmark close-price history (KOSPI / SP500 / GOLD / ...).

Before this module, `/api/portfolio/benchmark-history` hit yfinance on every
request. That worked until yfinance/Yahoo had a bad day and certain tickers
silently returned empty DataFrames — the chart then quietly dropped those
series with no user-visible error (see routes/portfolio.py).

We now persist daily closes in the `benchmark_daily` table keyed by
(code, date). Two entry points feed the table:

* `backfill_benchmark(code, start)` — lazy backfill when the DB doesn't yet
  have enough history for a given `start`. Called from the API route on
  demand; runs exactly once per (code, start) the first time it's wider
  than what we have.

* `update_benchmark_today(codes)` — nightly incremental. Called at the end
  of `snapshot_nav.run_all_snapshots()` so daily closes flow in for free on
  the 22:00 KST timer. Best-effort: a yfinance outage just means that one
  day's row arrives later, never breaks the snapshot itself.

The yfinance call is kept in an executor so asyncio isn't blocked for the
several hundred ms the HTTP round-trip takes.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta

import cache

logger = logging.getLogger(__name__)

# Maps our internal benchmark code → the Yahoo ticker yfinance understands.
# Extend here to support new chips on the NAV chart.
YF_TICKER: dict[str, str] = {
    "KOSPI": "^KS11",
    "SP500": "^GSPC",
    "GOLD":  "GC=F",
}

# How far back lazy backfill will try to reach on first request. Ten years
# is plenty for any period button on the NAV chart and still < 3000 rows
# per code.
_MAX_BACKFILL_YEARS = 10


def _download_sync(ticker: str, start: str, end: str) -> list[dict]:
    """Blocking yfinance download, run in executor. Returns [{date, close}]
    sorted ascending. Silently returns [] on empty DataFrame so callers can
    distinguish 'no data' from 'exception'."""
    import yfinance as yf

    df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
    if df.empty:
        return []
    close = df["Close"]
    # auto_adjust=True still yields a MultiIndex for single-ticker frames in
    # recent yfinance versions — unwrap if so.
    if hasattr(close, "columns"):
        close = close.iloc[:, 0]
    out: list[dict] = []
    for d, v in close.items():
        # Filter NaN (non-trading days, pre-listing dates).
        if v != v:  # NaN != NaN
            continue
        out.append({"date": d.strftime("%Y-%m-%d"), "close": round(float(v), 4)})
    return out


async def _download(ticker: str, start: str, end: str) -> list[dict]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _download_sync, ticker, start, end)


async def backfill_benchmark(code: str, start: str) -> int:
    """Ensure the DB has data back to at least `start` for `code`.

    Returns number of rows newly written (0 if DB was already wide enough
    or the ticker is unknown). Exceptions from yfinance are swallowed and
    logged — the API route treats the empty DB as a degraded response
    rather than a 502.
    """
    ticker = YF_TICKER.get(code.upper())
    if not ticker:
        logger.warning("Unknown benchmark code for backfill: %s", code)
        return 0

    earliest = await cache.get_benchmark_earliest_date(code)
    today = date.today()

    # Cap the backfill window so a pathological `start` (e.g. 1990-01-01)
    # doesn't pull decades of data we'll never show.
    min_allowed = (today - timedelta(days=_MAX_BACKFILL_YEARS * 365 + 5)).isoformat()
    fetch_from = max(start, min_allowed)

    # Already have data reaching back to or past fetch_from → nothing to do.
    if earliest and earliest <= fetch_from:
        return 0

    # If we have some data but it starts later than fetch_from, just pull
    # the missing prefix [fetch_from, earliest). Otherwise pull the full
    # window [fetch_from, today].
    end_date = earliest if earliest else today.isoformat()
    try:
        rows = await _download(ticker, fetch_from, end_date)
    except Exception as exc:
        logger.warning("Benchmark backfill failed (%s %s..%s): %s", code, fetch_from, end_date, exc)
        return 0

    if not rows:
        logger.info("Benchmark backfill returned no rows (%s %s..%s)", code, fetch_from, end_date)
        return 0

    return await cache.save_benchmark_rows(code, rows)


async def update_benchmark_today(codes: list[str] | None = None) -> dict[str, int]:
    """Nightly increment — append yesterday/today rows for each code.

    Fetches from (last_stored_date + 1) through today for each code.
    Returns {code: rows_written}. Never raises — a failure for one code
    must not break the snapshot pipeline or sister codes.
    """
    if codes is None:
        codes = list(YF_TICKER.keys())

    written: dict[str, int] = {}
    today = date.today()
    for code in codes:
        ticker = YF_TICKER.get(code.upper())
        if not ticker:
            continue

        last = await cache.get_benchmark_last_date(code)
        if last:
            # Start the fetch window one day after last stored date.
            try:
                start_d = date.fromisoformat(last) + timedelta(days=1)
            except ValueError:
                logger.warning("Malformed benchmark_daily.date (%s) for %s; skipping increment", last, code)
                written[code] = 0
                continue
            if start_d > today:
                # Already up to date.
                written[code] = 0
                continue
            start = start_d.isoformat()
        else:
            # Empty table for this code — fall back to the same lazy path
            # backfill_benchmark would take. 10-year window.
            start = (today - timedelta(days=_MAX_BACKFILL_YEARS * 365)).isoformat()

        # yfinance end is exclusive; use today+1 so today's close (if settled)
        # is included.
        end = (today + timedelta(days=1)).isoformat()
        try:
            rows = await _download(ticker, start, end)
        except Exception as exc:
            logger.warning("Benchmark increment failed (%s %s..%s): %s", code, start, end, exc)
            written[code] = 0
            continue

        if rows:
            n = await cache.save_benchmark_rows(code, rows)
            written[code] = n
            logger.info("Benchmark increment %s: +%d rows (%s..%s)", code, n, rows[0]["date"], rows[-1]["date"])
        else:
            written[code] = 0

    # Observability: aggregate tick so dashboard shows last benchmark
    # refresh. Keep the details payload compact — one counts dict.
    try:
        import observability
        await observability.record_event(
            "benchmark_history",
            "increment_ok" if any(written.values()) else "increment_noop",
            level="info",
            details={"written": written},
        )
    except Exception:
        pass
    return written


__all__ = [
    "YF_TICKER",
    "backfill_benchmark",
    "update_benchmark_today",
]
