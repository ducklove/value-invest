import unittest
from unittest.mock import patch

import market_indicators


class MarketIndicatorsCacheTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        market_indicators._indicators_cache.clear()
        market_indicators._indicator_item_cache.clear()

    async def test_subset_request_reuses_item_cache(self):
        calls: list[str] = []

        async def fake_fetch_kr_index(client, naver_code: str) -> dict:
            calls.append(naver_code)
            return {
                "value": "100.00",
                "change": "1.00",
                "change_pct": "1.00%",
                "direction": "up",
            }

        with patch.object(market_indicators, "_fetch_kr_index", side_effect=fake_fetch_kr_index):
            first = await market_indicators.fetch_indicators(["KOSPI", "KOSDAQ"])

        self.assertEqual(set(first.keys()), {"KOSPI", "KOSDAQ"})
        self.assertEqual(calls, ["KOSPI", "KOSDAQ"])

        with patch.object(market_indicators, "_fetch_kr_index", side_effect=AssertionError("cache missed")):
            second = await market_indicators.fetch_indicators(["KOSPI"])

        self.assertEqual(second["KOSPI"]["change_pct"], "1.00%")

