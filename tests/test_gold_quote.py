from copy import deepcopy
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

import httpx

from services.portfolio import special_assets


class GoldQuoteTests(IsolatedAsyncioTestCase):
    def setUp(self):
        self.payload = {
            "reutersCode": "M04020000", "stockExchangeType": {"code": "KRX"}, "unit": "원/g",
            "closePrice": "195,310", "fluctuations": "3,340", "fluctuationsType": {"code": "2"},
            "localTradedAt": "2026-09-18T15:19:15+09:00",
        }

    async def fetch(self, payload, status=200):
        response = httpx.Response(status, json=payload, request=httpx.Request("GET", "https://example.test"))
        client = AsyncMock()
        client.get.return_value = response
        with patch.object(special_assets, "get_http_client", AsyncMock(return_value=client)):
            result = await special_assets.fetch_krx_gold_quote()
        self.assertEqual(client.get.call_args.args[0], "https://api.stock.naver.com/marketindex/metals/M04020000")
        return result

    async def test_gold_price_change_and_original_timestamp(self):
        result = await self.fetch(self.payload)
        self.assertEqual((result["price"], result["change"], result["change_pct"]), (195310, 3340, 1.74))
        self.assertEqual((result["source"], result["as_of"]), ("naver_json", self.payload["localTradedAt"]))
        for code, raw, expected in [("5", "3,340", -3340), ("5", "-3,340", -3340), ("3", "0", 0)]:
            with self.subTest(code=code, raw=raw):
                result = await self.fetch({**self.payload, "fluctuationsType": {"code": code}, "fluctuations": raw})
                self.assertEqual(result["change"], expected)
                self.assertEqual(result["change_pct"], round(expected / (195310 - expected) * 100, 2))

    async def test_rejects_other_gold_products_and_invalid_quotes(self):
        for fields in [
            {"reutersCode": "CMDT_GD"}, {"reutersCode": "M04020100"},
            {"stockExchangeType": {"code": "SHINHAN"}}, {"stockExchangeType": None}, {"unit": "USD/oz"},
            {"closePrice": "0"}, {"closePrice": "NaN"}, {"closePrice": True}, {"closePrice": None},
            {"fluctuations": "Infinity"}, {"fluctuations": "200,000"},
            {"fluctuationsType": {"code": "3"}}, {"fluctuationsType": {"code": "unknown"}},
            {"localTradedAt": "bad"}, {"localTradedAt": "2026-09-18T15:19:15"},
        ]:
            with self.subTest(fields=fields):
                self.assertEqual(await self.fetch({**self.payload, **fields}), {})
        missing = deepcopy(self.payload)
        del missing["fluctuations"]
        for payload in (missing, [], None):
            self.assertEqual(await self.fetch(payload), {})
        self.assertEqual(await self.fetch(self.payload, status=410), {})
