import unittest
import time
from unittest.mock import AsyncMock, patch

import stock_price


class StockPriceFallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_market_data_uses_financial_data_when_kis_financials_fail(self):
        with patch("stock_price._get_yfinance_aux", return_value=(None, None, None, None)), \
             patch("stock_price._group_close_by_year_series", return_value={2024: 1000.0}), \
             patch("stock_price._group_last_by_year_series", return_value={}), \
             patch("stock_price._group_sum_by_year_series", return_value={}), \
             patch("stock_price.kis_proxy_client.get_history", new=AsyncMock(side_effect=RuntimeError("history down"))), \
             patch("stock_price.kis_proxy_client.get_dividends", new=AsyncMock(side_effect=RuntimeError("dividend down"))), \
             patch("stock_price.kis_proxy_client.get_financials", new=AsyncMock(side_effect=RuntimeError("financial down"))), \
             patch("stock_price.kis_proxy_client.get_quote", new=AsyncMock(return_value={"summary": {"listed_shares": "100"}})):
            result = await stock_price.fetch_market_data(
                "005930",
                financial_data=[{"year": 2024, "net_income": 50000.0, "total_equity": 100000.0}],
                start_year=2024,
                end_year=2024,
            )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["year"], 2024)
        self.assertEqual(result[0]["close_price"], 1000.0)
        self.assertEqual(result[0]["eps"], 500.0)
        self.assertEqual(result[0]["bps"], 1000.0)
        self.assertEqual(result[0]["market_cap"], 100000.0)

    async def test_fetch_market_data_prefers_dart_dividend_per_share(self):
        with patch("stock_price._get_yfinance_aux", return_value=(None, None, None, None)), \
             patch("stock_price._group_close_by_year_series", return_value={}), \
             patch("stock_price._group_last_by_year_series", return_value={}), \
             patch("stock_price._group_sum_by_year_series", return_value={2025: 10000.0}), \
             patch("stock_price._group_close_by_year", return_value={2025: 420500.0}), \
             patch("stock_price._group_dividends_by_year", return_value={2025: 10000.0}), \
             patch("stock_price.kis_proxy_client.get_history", new=AsyncMock(return_value={"items": []})), \
             patch("stock_price.kis_proxy_client.get_dividends", new=AsyncMock(return_value={"items": []})), \
             patch("stock_price.kis_proxy_client.get_financials", new=AsyncMock(return_value={})), \
             patch("stock_price.kis_proxy_client.get_quote", new=AsyncMock(return_value={"summary": {"listed_shares": "100"}})), \
             patch("stock_price.dart_client.fetch_dividend_per_share_by_year", new=AsyncMock(return_value={2025: 15000.0})):
            result = await stock_price.fetch_market_data(
                "002380",
                start_year=2025,
                end_year=2025,
                corp_code="00105271",
            )

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["year"], 2025)
        self.assertEqual(result[0]["dividend_per_share"], 15000.0)
        self.assertEqual(result[0]["dividend_yield"], 3.57)

    async def test_fetch_quote_snapshot_can_bypass_stale_ws_cache(self):
        with patch("stock_price.kis_ws_manager.get_cached_quote", return_value={"price": 1000}), \
             patch("stock_price.kis_ws_manager.active_market_code", return_value="J"), \
             patch("stock_price.kis_proxy_client.get_quote", new=AsyncMock(return_value={
                 "summary": {
                     "current_price": "2000",
                     "previous_close": "1980",
                     "change": "20",
                     "change_rate": "1.01",
                 }
             })) as get_quote:
            result = await stock_price.fetch_quote_snapshot("005930", use_ws_cache=False)

        get_quote.assert_awaited_once()
        self.assertEqual(result["price"], 2000.0)
        self.assertEqual(result["previous_close"], 1980.0)

    async def test_fetch_quote_snapshot_prefers_ws_cache_by_default(self):
        with patch("stock_price.kis_ws_manager.get_cached_quote", return_value={
            "date": "20260518",
            "price": 1000,
            "change": 10,
            "change_pct": 1.0,
            "ts": time.time(),
        }), patch("stock_price.kis_ws_manager.ws_cache_matches_rest_market", return_value=True), \
             patch("stock_price.kis_proxy_client.get_quote", new=AsyncMock()) as get_quote:
            result = await stock_price.fetch_quote_snapshot("005930")

        get_quote.assert_not_awaited()
        self.assertEqual(result["price"], 1000)

    async def test_fetch_quote_snapshot_ignores_old_ws_cache_by_default(self):
        with patch("stock_price.kis_ws_manager.get_cached_quote", return_value={
            "date": "20260518",
            "price": 1000,
            "change": 10,
            "change_pct": 1.0,
            "ts": time.time() - 120,
        }), patch("stock_price.kis_ws_manager.ws_cache_matches_rest_market", return_value=True), \
             patch("stock_price.kis_ws_manager.active_market_code", return_value="J"), \
             patch("stock_price.kis_proxy_client.get_quote", new=AsyncMock(return_value={
                 "summary": {
                     "current_price": "2000",
                     "previous_close": "1980",
                     "change": "20",
                     "change_rate": "1.01",
                 }
             })) as get_quote:
            result = await stock_price.fetch_quote_snapshot("005930")

        get_quote.assert_awaited_once()
        self.assertEqual(result["price"], 2000.0)

    async def test_fetch_quote_snapshot_ignores_ws_cache_when_rest_market_differs(self):
        with patch("stock_price.kis_ws_manager.get_cached_quote", return_value={
            "date": "20260520",
            "price": 1745000,
            "change": 0,
            "change_pct": 0.0,
            "ts": time.time(),
        }), patch("stock_price.kis_ws_manager.ws_cache_matches_rest_market", return_value=False), \
             patch("stock_price.kis_ws_manager.active_market_code", return_value="NX"), \
             patch("stock_price.kis_proxy_client.get_quote", new=AsyncMock(return_value={
                 "summary": {
                     "current_price": "1786000",
                     "previous_close": "1745000",
                     "change": "41000",
                     "change_rate": "2.35",
                 }
             })) as get_quote:
            result = await stock_price.fetch_quote_snapshot("000660")

        get_quote.assert_awaited_once_with("000660", market="NX")
        self.assertEqual(result["price"], 1786000.0)
        self.assertEqual(result["change_pct"], 2.35)

    async def test_fetch_quote_snapshot_does_not_mark_nxt_unsupported_on_error(self):
        get_quote = AsyncMock(side_effect=[
            stock_price.kis_proxy_client.KISProxyError("temporary NX failure"),
            {
                "summary": {
                    "current_price": "1745000",
                    "previous_close": "1745000",
                    "change": "0",
                    "change_rate": "0",
                }
            },
        ])
        with patch("stock_price.kis_ws_manager.get_cached_quote", return_value=None), \
             patch("stock_price.kis_ws_manager.active_market_code", return_value="NX"), \
             patch("stock_price.kis_ws_manager.mark_nxt_unsupported") as mark_unsupported, \
             patch("stock_price.kis_proxy_client.get_quote", new=get_quote):
            result = await stock_price.fetch_quote_snapshot("000660")

        mark_unsupported.assert_not_called()
        self.assertEqual(get_quote.await_args_list[0].kwargs, {"market": "NX"})
        self.assertEqual(get_quote.await_args_list[1].kwargs, {"market": None})
        self.assertEqual(result["price"], 1745000.0)
        self.assertEqual(result["_stale"], True)

    async def test_fetch_quote_snapshot_falls_back_to_history_when_quote_fails(self):
        with patch("stock_price.kis_ws_manager.get_cached_quote", return_value=None), \
             patch("stock_price.kis_ws_manager.active_market_code", return_value="J"), \
             patch("stock_price.kis_proxy_client.get_quote", new=AsyncMock(side_effect=RuntimeError("quote down"))), \
             patch("stock_price.kis_proxy_client.get_history", new=AsyncMock(return_value={
                 "items": [
                     {"stck_bsop_date": "20260515", "stck_clpr": "1980"},
                     {"stck_bsop_date": "20260518", "stck_clpr": "2000", "acml_tr_pbmn": "123456"},
                 ]
             })) as get_history:
            result = await stock_price.fetch_quote_snapshot("005930")

        get_history.assert_awaited_once()
        self.assertEqual(result["date"], "2026-05-18")
        self.assertEqual(result["price"], 2000.0)
        self.assertEqual(result["previous_close"], 1980.0)
        self.assertEqual(result["change"], 20.0)
        self.assertEqual(result["trade_value"], 123456.0)
