"""Tests for benchmark_history + /api/portfolio/benchmark-history.

The big behavioral change here: the API used to hit yfinance on every
request, so a yfinance outage silently dropped S&P500/GOLD series on the
NAV chart. Now the API serves from the `benchmark_daily` table and only
touches yfinance via `backfill_benchmark` the first time a code is seen
(or when the requested `start` predates stored history).

These tests pin that behavior without going to the network.
"""
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

import benchmark_history
import cache
from routes import portfolio as pf


class BenchmarkCacheTests(unittest.IsolatedAsyncioTestCase):
    """Pure cache.py helpers — no yfinance, no route."""

    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(cache, "DB_PATH", Path(self.tmp.name) / "cache.db")
        self.db_patch.start()
        await cache.close_db()
        await cache.init_db()

    async def asyncTearDown(self):
        await cache.close_db()
        self.db_patch.stop()
        self.tmp.cleanup()

    async def test_save_and_read_rows(self):
        rows = [
            {"date": "2026-04-10", "close": 100.0},
            {"date": "2026-04-11", "close": 101.5},
            {"date": "2026-04-12", "close": 99.8},
        ]
        n = await cache.save_benchmark_rows("KOSPI", rows)
        self.assertEqual(n, 3)

        got = await cache.get_benchmark_rows("KOSPI")
        self.assertEqual([r["date"] for r in got], ["2026-04-10", "2026-04-11", "2026-04-12"])
        self.assertAlmostEqual(got[1]["close"], 101.5)

    async def test_upsert_overwrites_same_date(self):
        await cache.save_benchmark_rows("KOSPI", [{"date": "2026-04-10", "close": 100.0}])
        # Second save with a different close on the same date should overwrite,
        # not duplicate — the NAV chart shouldn't see two 2026-04-10 points.
        await cache.save_benchmark_rows("KOSPI", [{"date": "2026-04-10", "close": 123.0}])
        rows = await cache.get_benchmark_rows("KOSPI")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["close"], 123.0)

    async def test_start_filter(self):
        await cache.save_benchmark_rows("KOSPI", [
            {"date": "2026-04-01", "close": 1},
            {"date": "2026-04-05", "close": 2},
            {"date": "2026-04-10", "close": 3},
        ])
        rows = await cache.get_benchmark_rows("KOSPI", start="2026-04-05")
        self.assertEqual([r["date"] for r in rows], ["2026-04-05", "2026-04-10"])

    async def test_scoped_to_code(self):
        await cache.save_benchmark_rows("KOSPI", [{"date": "2026-04-01", "close": 1}])
        await cache.save_benchmark_rows("SP500", [{"date": "2026-04-01", "close": 5000}])
        k = await cache.get_benchmark_rows("KOSPI")
        s = await cache.get_benchmark_rows("SP500")
        self.assertEqual(len(k), 1)
        self.assertEqual(len(s), 1)
        self.assertEqual(k[0]["close"], 1)
        self.assertEqual(s[0]["close"], 5000)

    async def test_last_and_earliest(self):
        self.assertIsNone(await cache.get_benchmark_last_date("GOLD"))
        self.assertIsNone(await cache.get_benchmark_earliest_date("GOLD"))
        await cache.save_benchmark_rows("GOLD", [
            {"date": "2026-03-30", "close": 2200},
            {"date": "2026-04-05", "close": 2300},
            {"date": "2026-04-02", "close": 2250},
        ])
        self.assertEqual(await cache.get_benchmark_last_date("GOLD"), "2026-04-05")
        self.assertEqual(await cache.get_benchmark_earliest_date("GOLD"), "2026-03-30")

    async def test_save_skips_null_close(self):
        # yfinance occasionally returns NaN rows — `_download_sync` drops
        # them, but defense in depth: the helper also filters None.
        await cache.save_benchmark_rows("KOSPI", [
            {"date": "2026-04-10", "close": None},
            {"date": "2026-04-11", "close": 100.0},
        ])
        rows = await cache.get_benchmark_rows("KOSPI")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["date"], "2026-04-11")


class BackfillTests(unittest.IsolatedAsyncioTestCase):
    """backfill_benchmark — lazy full-history pull."""

    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(cache, "DB_PATH", Path(self.tmp.name) / "cache.db")
        self.db_patch.start()
        await cache.close_db()
        await cache.init_db()

    async def asyncTearDown(self):
        await cache.close_db()
        self.db_patch.stop()
        self.tmp.cleanup()

    async def test_rejects_unknown_code(self):
        n = await benchmark_history.backfill_benchmark("UNKNOWN", "2026-01-01")
        self.assertEqual(n, 0)
        # Nothing written.
        self.assertEqual(await cache.get_benchmark_rows("UNKNOWN"), [])

    async def test_empty_db_triggers_download(self):
        fake_rows = [
            {"date": "2026-04-01", "close": 3000.0},
            {"date": "2026-04-02", "close": 3010.0},
        ]
        calls = []

        async def fake_download(ticker, start, end):
            calls.append((ticker, start, end))
            return fake_rows

        with patch.object(benchmark_history, "_download", new=fake_download):
            n = await benchmark_history.backfill_benchmark("SP500", "2026-03-01")

        self.assertEqual(n, 2)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], "^GSPC")
        saved = await cache.get_benchmark_rows("SP500")
        self.assertEqual(len(saved), 2)

    async def test_no_download_when_db_already_covers(self):
        # Seed DB with data reaching back past the requested start.
        await cache.save_benchmark_rows("KOSPI", [
            {"date": "2026-01-01", "close": 2500.0},
            {"date": "2026-04-01", "close": 2700.0},
        ])
        calls = []

        async def fake_download(ticker, start, end):
            calls.append((ticker, start, end))
            return []

        with patch.object(benchmark_history, "_download", new=fake_download):
            n = await benchmark_history.backfill_benchmark("KOSPI", "2026-02-01")

        self.assertEqual(n, 0)
        self.assertEqual(calls, [])  # short-circuit before yfinance.

    async def test_download_exception_is_swallowed(self):
        async def failing_download(ticker, start, end):
            raise RuntimeError("yfinance 500")

        with patch.object(benchmark_history, "_download", new=failing_download):
            n = await benchmark_history.backfill_benchmark("GOLD", "2026-01-01")
        # Swallow + log, don't raise. Endpoint can still serve empty rows.
        self.assertEqual(n, 0)

    async def test_backfill_caps_at_ten_years(self):
        # A 20-year-ago start should get clamped — we don't want the first
        # request after deploy pulling 5000 rows of irrelevant history.
        calls = []

        async def capture(ticker, start, end):
            calls.append((ticker, start, end))
            return []

        start_20y = (date.today() - timedelta(days=365 * 20)).isoformat()
        with patch.object(benchmark_history, "_download", new=capture):
            await benchmark_history.backfill_benchmark("KOSPI", start_20y)

        self.assertEqual(len(calls), 1)
        actual_start = date.fromisoformat(calls[0][1])
        ten_years_ago = date.today() - timedelta(days=365 * 10 + 10)
        # actual_start should be within a week of the 10-year cap.
        self.assertGreaterEqual(actual_start, ten_years_ago)


class UpdateTodayTests(unittest.IsolatedAsyncioTestCase):
    """update_benchmark_today — nightly incremental from snapshot_nav."""

    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(cache, "DB_PATH", Path(self.tmp.name) / "cache.db")
        self.db_patch.start()
        await cache.close_db()
        await cache.init_db()

    async def asyncTearDown(self):
        await cache.close_db()
        self.db_patch.stop()
        self.tmp.cleanup()

    async def test_starts_day_after_last_stored(self):
        await cache.save_benchmark_rows("KOSPI", [{"date": "2026-04-10", "close": 2700.0}])

        captured = []

        async def capture(ticker, start, end):
            captured.append((ticker, start, end))
            return [{"date": "2026-04-11", "close": 2710.0}]

        with patch.object(benchmark_history, "_download", new=capture):
            written = await benchmark_history.update_benchmark_today(codes=["KOSPI"])

        self.assertEqual(written, {"KOSPI": 1})
        # Should have fetched starting 2026-04-11, not 2026-04-10 (no overlap).
        self.assertEqual(captured[0][1], "2026-04-11")

    async def test_skips_when_already_up_to_date(self):
        future = (date.today() + timedelta(days=1)).isoformat()
        await cache.save_benchmark_rows("KOSPI", [{"date": future, "close": 2700.0}])

        calls = []

        async def capture(ticker, start, end):
            calls.append((ticker, start, end))
            return []

        with patch.object(benchmark_history, "_download", new=capture):
            written = await benchmark_history.update_benchmark_today(codes=["KOSPI"])

        self.assertEqual(written, {"KOSPI": 0})
        self.assertEqual(calls, [])

    async def test_one_code_failure_does_not_break_others(self):
        async def selective_download(ticker, start, end):
            if ticker == "^GSPC":
                raise RuntimeError("Yahoo 502")
            return [{"date": "2026-04-11", "close": 1.0}]

        with patch.object(benchmark_history, "_download", new=selective_download):
            written = await benchmark_history.update_benchmark_today(codes=["SP500", "KOSPI"])

        self.assertEqual(written["SP500"], 0)
        self.assertEqual(written["KOSPI"], 1)
        self.assertEqual(len(await cache.get_benchmark_rows("KOSPI")), 1)
        self.assertEqual(len(await cache.get_benchmark_rows("SP500")), 0)

    async def test_defaults_to_all_codes(self):
        async def no_data(ticker, start, end):
            return []

        with patch.object(benchmark_history, "_download", new=no_data):
            written = await benchmark_history.update_benchmark_today()

        self.assertEqual(set(written.keys()), set(benchmark_history.YF_TICKER.keys()))


class RouteTests(unittest.IsolatedAsyncioTestCase):
    """get_benchmark_history endpoint — DB-first behavior."""

    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(cache, "DB_PATH", Path(self.tmp.name) / "cache.db")
        self.db_patch.start()
        await cache.close_db()
        await cache.init_db()

    async def asyncTearDown(self):
        await cache.close_db()
        self.db_patch.stop()
        self.tmp.cleanup()

    async def test_rejects_unknown_code(self):
        from fastapi import HTTPException
        with self.assertRaises(HTTPException) as exc_info:
            await pf.get_benchmark_history(code="UNKNOWN", start="2026-01-01")
        self.assertEqual(exc_info.exception.status_code, 400)

    async def test_serves_from_db_without_network(self):
        await cache.save_benchmark_rows("KOSPI", [
            {"date": "2026-04-01", "close": 2700.0},
            {"date": "2026-04-02", "close": 2710.0},
        ])

        async def must_not_call(*a, **kw):
            raise AssertionError("backfill should not be needed when DB covers `start`")

        # DB already covers 2026-03-01 (start) since earliest 2026-04-01 < start?
        # No: start=2026-03-01, earliest=2026-04-01 → backfill WOULD trigger.
        # For this test we want the opposite path, so request start=2026-04-01
        # which equals earliest — backfill short-circuits.
        with patch.object(benchmark_history, "_download", side_effect=must_not_call):
            rows = await pf.get_benchmark_history(code="KOSPI", start="2026-04-01")

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["date"], "2026-04-01")

    async def test_triggers_backfill_when_db_empty(self):
        fake = [
            {"date": "2026-04-01", "close": 5000.0},
            {"date": "2026-04-02", "close": 5010.0},
        ]
        call_count = 0

        async def fake_download(ticker, start, end):
            nonlocal call_count
            call_count += 1
            return fake

        with patch.object(benchmark_history, "_download", new=fake_download):
            rows = await pf.get_benchmark_history(code="SP500", start="2026-03-01")

        self.assertEqual(call_count, 1)
        self.assertEqual([r["date"] for r in rows], ["2026-04-01", "2026-04-02"])

    async def test_returns_empty_list_on_download_failure(self):
        # yfinance blows up → endpoint still returns (empty) list rather than
        # 502. Frontend treats empty identically to success-with-no-rows, so
        # the chart just shows no series until the nightly increment lands.
        async def boom(ticker, start, end):
            raise RuntimeError("network down")

        with patch.object(benchmark_history, "_download", new=boom):
            rows = await pf.get_benchmark_history(code="GOLD", start="2026-01-01")

        self.assertEqual(rows, [])


if __name__ == "__main__":
    unittest.main()
