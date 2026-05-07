import unittest
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
