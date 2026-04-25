import unittest
from unittest.mock import AsyncMock, patch

from routes import portfolio as pf


class PortfolioAssetInsightTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        pf._asset_history_cache.clear()

    async def asyncTearDown(self):
        pf._asset_history_cache.clear()

    async def test_korean_stock_history_uses_kis_daily_rows(self):
        calls = []

        async def fake_get_history(symbol, **kwargs):
            calls.append((symbol, kwargs))
            return {
                "items": [
                    {"stck_bsop_date": "20250103", "stck_clpr": "71,000"},
                    {"stck_bsop_date": "20250102", "stck_clpr": "70000"},
                    {"stck_bsop_date": "bad", "stck_clpr": "1"},
                ]
            }

        with patch.object(pf.kis_proxy_client, "get_history", new=fake_get_history):
            payload = await pf._asset_history_for_insight("005930", {"currency": "KRW"})

        self.assertEqual(payload["currency"], "KRW")
        self.assertEqual(
            payload["rows"],
            [
                {"date": "2025-01-02", "close": 70000.0},
                {"date": "2025-01-03", "close": 71000.0},
            ],
        )
        self.assertEqual(calls[0][0], "005930")
        self.assertEqual(calls[0][1]["period"], "D")
        self.assertTrue(calls[0][1]["adjusted"])

    async def test_korean_benchmark_history_uses_kis_history(self):
        rows = [{"date": "2025-01-02", "close": 70000.0}]
        downloader = AsyncMock(return_value={"rows": rows, "currency": "KRW"})

        with patch.object(pf, "_download_korean_history", new=downloader):
            result = await pf._benchmark_history_for_insight("005930")

        self.assertEqual(result, rows)
        downloader.assert_awaited_once_with("005930")

    async def test_korean_history_failure_does_not_poison_cache(self):
        calls = 0

        async def flaky_get_history(symbol, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("temporary upstream hiccup")
            return {"items": [{"stck_bsop_date": "20250102", "stck_clpr": "70000"}]}

        with patch.object(pf.kis_proxy_client, "get_history", new=flaky_get_history):
            first = await pf._asset_history_for_insight("005930", {"currency": "KRW"})
            second = await pf._asset_history_for_insight("005930", {"currency": "KRW"})

        self.assertEqual(first["rows"], [])
        self.assertEqual(second["rows"], [{"date": "2025-01-02", "close": 70000.0}])
        self.assertEqual(calls, 2)

    async def test_korean_stock_macro_codes_include_domestic_market(self):
        codes = pf._macro_codes_for_asset({"assetClass": "korean_stock"}, "KRW")

        self.assertEqual(codes, ["USD_KRW", "KOSPI", "KOSDAQ", "KR3Y"])
