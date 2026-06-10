import unittest
from unittest.mock import AsyncMock, patch

import close_price_client
import kis_proxy_client
from routes import portfolio as pf
from services.portfolio import foreign
from services.portfolio import insights


class PortfolioAssetInsightTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        insights.asset_history_cache.clear()
        foreign._failed_yf_cache.clear()

    async def asyncTearDown(self):
        insights.asset_history_cache.clear()
        foreign._failed_yf_cache.clear()

    async def test_static_foreign_etfs_skip_ticker_discovery(self):
        with patch.object(foreign, "yfinance_find_ticker", new=AsyncMock(side_effect=AssertionError("no probe"))), \
             patch.object(foreign, "fetch_naver_world_stock", new=AsyncMock(side_effect=AssertionError("no naver"))):
            self.assertEqual(await foreign.resolve_foreign_reuters("A200"), "A200.AX")
            self.assertEqual(await foreign.resolve_foreign_reuters("EUN2.DE"), "EUN2.DE")
            self.assertEqual(await foreign.resolve_foreign_name("A200.AX"), "BetaShares Australia 200 ETF")
            self.assertEqual(await foreign.detect_currency("EUN2.DE"), "EUR")

            resolved = await pf.resolve_name(code="A200")

        self.assertEqual(resolved["stock_code"], "A200.AX")
        self.assertEqual(resolved["stock_name"], "BetaShares Australia 200 ETF")

    async def test_static_foreign_etf_quote_uses_fast_yahoo_path_only(self):
        fast = AsyncMock(return_value={"price": 12345, "change": 10, "change_pct": 0.1})
        legacy = AsyncMock(side_effect=AssertionError("legacy yfinance should not run"))
        kis = AsyncMock(side_effect=AssertionError("KIS should not run for static non-KIS ETF"))

        with patch.object(foreign, "yfinance_fetch_quote_fast", new=fast), \
             patch.object(foreign, "yfinance_fetch_quote", new=legacy), \
             patch.object(foreign, "kis_fetch_foreign_quote", new=kis):
            quote = await foreign.fetch_foreign_quote("EUN2")

        self.assertEqual(quote["price"], 12345)
        fast.assert_awaited_once_with("EUN2.DE")
        legacy.assert_not_awaited()
        kis.assert_not_awaited()

    async def test_alphanumeric_krx_etf_code_stays_on_domestic_path(self):
        self.assertTrue(pf._is_korean_stock("0074K0"))
        self.assertTrue(pf._is_korean_stock("0074k0"))
        self.assertFalse(pf._is_preferred_stock("0074K0"))
        self.assertTrue(pf._is_preferred_stock("005935"))
        self.assertTrue(pf._is_preferred_stock("00088K"))
        self.assertFalse(pf._is_preferred_stock("005930"))

        resolver = AsyncMock(return_value="KRX 알파뉴메릭 ETF")
        with patch.object(foreign, "resolve_name", new=resolver):
            resolved = await pf.resolve_name(code="0074k0")

        self.assertEqual(resolved["stock_code"], "0074K0")
        self.assertEqual(resolved["stock_name"], "KRX 알파뉴메릭 ETF")
        resolver.assert_awaited_once_with("0074K0")

    async def test_korean_stock_history_falls_back_to_kis_daily_rows(self):
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

        with patch.object(close_price_client, "get_daily_closes", new=AsyncMock(return_value=[])), \
             patch.object(kis_proxy_client, "get_history", new=fake_get_history):
            payload = await insights.asset_history_for_insight("005930", {"currency": "KRW"})

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

        with patch.object(insights, "download_korean_history", new=downloader):
            result = await insights.benchmark_history_for_insight("005930")

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

        with patch.object(close_price_client, "get_daily_closes", new=AsyncMock(return_value=[])), \
             patch.object(kis_proxy_client, "get_history", new=flaky_get_history):
            first = await insights.asset_history_for_insight("005930", {"currency": "KRW"})
            second = await insights.asset_history_for_insight("005930", {"currency": "KRW"})

        self.assertEqual(first["rows"], [])
        self.assertEqual(second["rows"], [{"date": "2025-01-02", "close": 70000.0}])
        self.assertEqual(calls, 2)

    async def test_korean_stock_macro_codes_include_domestic_market(self):
        codes = insights.macro_codes_for_asset({"assetClass": "korean_stock"}, "KRW")

        self.assertEqual(codes, ["USD_KRW", "KOSPI", "KOSDAQ", "KR3Y"])

    async def test_holding_context_marks_holding_value_dashboard(self):
        config = {
            "holdingValue": {
                "baseUrl": "https://ducklove.github.io/holding_value",
                "meta": {
                    "002380": {
                        "totalShares": 1000,
                        "treasuryShares": 100,
                        "subsidiaries": [{"code": "028260", "sharesHeld": 10}],
                    }
                },
            }
        }

        with patch.object(pf.integrations, "build_public_integrations", return_value=config):
            holding = insights.holding_context_for_asset("002380")

        self.assertEqual(holding["code"], "002380")
        self.assertEqual(holding["subsidiaryCount"], 1)
        self.assertEqual(holding["url"], "https://ducklove.github.io/holding_value/?code=002380")
