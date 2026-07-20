"""hover 캔들 툴팁용 일봉 API — services/stock_candles + /api/stocks/{code}/daily-candles."""
import unittest
from unittest.mock import AsyncMock, patch

import kis_proxy_client
from services import stock_candles
from services.portfolio import history as portfolio_history


class StockCandlesServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        stock_candles._candle_cache.clear()

    async def asyncTearDown(self):
        stock_candles._candle_cache.clear()

    async def test_korean_candles_parse_ohlc_from_kis_history(self):
        payload = {"items": [
            # 순서를 뒤집어 넣어도 날짜 오름차순으로 정렬돼 나온다.
            {"stck_bsop_date": "20260717", "stck_clpr": "1300", "stck_oprc": "1210",
             "stck_hgpr": "1330", "stck_lwpr": "1190"},
            {"stck_bsop_date": "20260716", "stck_clpr": "1200", "stck_oprc": "1100",
             "stck_hgpr": "1250", "stck_lwpr": "1080", "acml_vol": "1000"},
        ]}
        with patch.object(kis_proxy_client, "get_history", new=AsyncMock(return_value=payload)) as kis:
            result = await stock_candles.get_daily_candles("005930", days=60)

        kis.assert_awaited_once()
        self.assertTrue(result["supported"])
        self.assertEqual(result["currency"], "KRW")
        self.assertEqual(result["source"], "kis")
        self.assertEqual(result["candles"], [
            {"date": "2026-07-16", "close": 1200.0, "open": 1100.0, "high": 1250.0,
             "low": 1080.0, "volume": 1000.0},
            {"date": "2026-07-17", "close": 1300.0, "open": 1210.0, "high": 1330.0,
             "low": 1190.0},
        ])

    async def test_korean_candles_keep_only_last_days(self):
        items = [
            {"stck_bsop_date": f"2026{month:02d}{day:02d}", "stck_clpr": "100"}
            for month in range(1, 4)
            for day in range(1, 29)
        ]
        with patch.object(kis_proxy_client, "get_history", new=AsyncMock(return_value={"items": items})):
            result = await stock_candles.get_daily_candles("005930", days=20)
        self.assertEqual(len(result["candles"]), 20)
        self.assertEqual(result["candles"][-1]["date"], "2026-03-28")

    async def test_candles_are_cached_per_code_and_days(self):
        payload = {"items": [
            {"stck_bsop_date": "20260716", "stck_clpr": "1200"},
            {"stck_bsop_date": "20260717", "stck_clpr": "1300"},
        ]}
        with patch.object(kis_proxy_client, "get_history", new=AsyncMock(return_value=payload)) as kis:
            await stock_candles.get_daily_candles("005930", days=60)
            await stock_candles.get_daily_candles("005930", days=60)
        kis.assert_awaited_once()

    async def test_special_assets_are_unsupported_without_upstream_calls(self):
        with (
            patch.object(kis_proxy_client, "get_history", new=AsyncMock()) as kis,
            patch.object(portfolio_history, "download_yfinance_history", new=AsyncMock()) as yf,
        ):
            for code in ("CASH_KRW", "KRX_GOLD", "CRYPTO_BTC"):
                result = await stock_candles.get_daily_candles(code)
                self.assertFalse(result["supported"])
                self.assertEqual(result["candles"], [])
        kis.assert_not_awaited()
        yf.assert_not_awaited()

    async def test_foreign_candles_use_yahoo_ohlc_rows(self):
        rows = [
            {"date": "2026-07-16", "close": 10.0, "open": 9.5, "high": 10.2, "low": 9.4},
            {"date": "2026-07-17", "close": 10.5, "open": 10.0, "high": 10.6, "low": 9.9},
        ]
        with (
            patch.object(portfolio_history, "download_yfinance_history",
                         new=AsyncMock(return_value={"rows": rows, "currency": "USD"})) as yf,
            patch.object(stock_candles.foreign, "ensure_ticker_map", new=AsyncMock()),
        ):
            result = await stock_candles.get_daily_candles("AAPL", days=60)

        yf.assert_awaited_once()
        self.assertEqual(yf.await_args.args[0], "AAPL")
        self.assertEqual(result["currency"], "USD")
        self.assertEqual(result["source"], "yahoo")
        self.assertEqual(len(result["candles"]), 2)
        self.assertEqual(result["candles"][0]["open"], 9.5)

    async def test_upstream_failure_returns_empty_and_is_not_cached(self):
        ok_payload = {"items": [{"stck_bsop_date": "20260717", "stck_clpr": "1300"}]}
        with patch.object(kis_proxy_client, "get_history", new=AsyncMock(side_effect=RuntimeError("down"))):
            result = await stock_candles.get_daily_candles("005930")
        self.assertEqual(result["candles"], [])

        # 실패는 캐시되지 않는다 — 다음 호출은 즉시 업스트림 재시도.
        with patch.object(kis_proxy_client, "get_history", new=AsyncMock(return_value=ok_payload)) as kis:
            result = await stock_candles.get_daily_candles("005930")
        kis.assert_awaited_once()
        self.assertEqual(len(result["candles"]), 1)


class StockCandlesRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_route_strips_code_and_delegates_to_service(self):
        from routes import stocks as stocks_routes

        with patch.object(stock_candles, "get_daily_candles",
                          new=AsyncMock(return_value={"code": "005930", "candles": []})) as svc:
            result = await stocks_routes.get_stock_daily_candles(" 005930 ", days=40)

        svc.assert_awaited_once_with("005930", days=40)
        self.assertEqual(result["code"], "005930")
