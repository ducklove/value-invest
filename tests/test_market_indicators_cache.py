import time
import unittest
from unittest.mock import patch

import market_indicators


class MarketIndicatorsCacheTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        market_indicators._indicators_cache.clear()
        market_indicators._indicator_item_cache.clear()

    async def test_subset_request_reuses_item_cache(self):
        calls: list[str] = []

        async def fake_fetch_json(client, codes: list[str]) -> dict:
            calls.extend(codes)
            return {code: {
                "value": "100.00",
                "change": "1.00",
                "change_pct": "1.00%",
                "direction": "up",
            } for code in codes}

        with patch.object(market_indicators.naver_indicators, "fetch_indicators", side_effect=fake_fetch_json):
            first = await market_indicators.fetch_indicators(["KOSPI", "KOSDAQ"])

        self.assertEqual(set(first.keys()), {"KOSPI", "KOSDAQ"})
        self.assertEqual(calls, ["KOSPI", "KOSDAQ"])

        with patch.object(market_indicators.naver_indicators, "fetch_indicators", side_effect=AssertionError("cache missed")):
            second = await market_indicators.fetch_indicators(["KOSPI"])

        self.assertEqual(second["KOSPI"]["change_pct"], "1.00%")

    async def test_refresh_failure_keeps_stale_indicator_value(self):
        stale_ts = time.monotonic() - market_indicators._INDICATORS_TTL - 1
        market_indicators._indicator_item_cache["SPX"] = (
            stale_ts,
            {
                "value": "6,000.00",
                "change": "12.00",
                "change_pct": "0.20%",
                "direction": "up",
            },
        )

        async def empty_json(client, codes: list[str]) -> dict:
            return {}

        with patch.object(market_indicators.naver_indicators, "fetch_indicators", side_effect=empty_json):
            result = await market_indicators.fetch_indicators(["SPX"])

        self.assertEqual(result["SPX"]["value"], "6,000.00")
        self.assertEqual(result["SPX"]["change_pct"], "0.20%")
        self.assertTrue(result["SPX"]["_stale"])

        market_indicators._indicators_cache.clear()
        market_indicators._indicator_item_cache["SPX"] = (stale_ts, result["SPX"])
        fresh = {"value": "6,001.00", "change": "13.00", "change_pct": "0.22%", "direction": "up", "as_of": "2026-09-11T10:00:00+09:00"}
        with patch.object(market_indicators.naver_indicators, "fetch_indicators", return_value={"SPX": fresh}):
            recovered = await market_indicators.fetch_indicators(["SPX"])
        self.assertEqual(recovered["SPX"], fresh)
        self.assertNotIn("_stale", recovered["SPX"])
