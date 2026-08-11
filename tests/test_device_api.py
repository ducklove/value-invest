"""GET /api/device/portfolio — ePaper 기기용 읽기 전용 요약 엔드포인트."""

import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException, Request

from routes import device
from services.portfolio import device_summary


def _request(headers: dict[str, str] | None = None) -> Request:
    encoded = [
        (k.lower().encode("latin-1"), v.encode("latin-1"))
        for k, v in (headers or {}).items()
    ]
    return Request({
        "type": "http",
        "method": "GET",
        "path": "/api/device/portfolio",
        "headers": encoded,
        "query_string": b"",
        "client": ("192.168.68.90", 12345),
        "server": ("testserver", 80),
    })


def _items() -> list[dict]:
    return [
        {"stock_code": "A", "stock_name": "가나전자", "quantity": 10,
         "avg_price": 1000, "avg_price_currency": "KRW", "currency": "KRW"},
        # 해외 종목: 원금은 USD 로 입력돼 있지만 시세는 이미 원화로 온다.
        {"stock_code": "B", "stock_name": "Beta Corp", "quantity": 2,
         "avg_price": 100, "avg_price_currency": "USD", "currency": "USD"},
        {"stock_code": "C", "stock_name": "시세없음", "quantity": 5,
         "avg_price": 500, "avg_price_currency": "KRW", "currency": "KRW"},
    ]


def _enriched(items: list[dict]) -> list[dict]:
    quotes = {
        "A": {"price": 1200, "change": 20, "change_pct": 1.69},
        # price/change 는 foreign.fetch_foreign_quote 가 fx_to_krw 로 환산한 값.
        "B": {"price": 200_000, "change": 2_000, "change_pct": 1.01},
        "C": {},  # 시세 미확보 — 합계에서 빠져야 한다
    }
    for item in items:
        item["quote"] = quotes[item["stock_code"]]
    return items


def _fx_rate(currency):
    return 1300.0 if (currency or "KRW").upper() == "USD" else 1.0


class DeviceTokenTests(unittest.IsolatedAsyncioTestCase):
    async def test_disabled_when_token_not_configured(self):
        # internal.py 와 달리 loopback 예외가 없다 — 미설정이면 그냥 꺼진 것.
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(HTTPException) as ctx:
                await device.device_portfolio(_request(), top=10)
        self.assertEqual(ctx.exception.status_code, 503)

    async def test_rejects_wrong_token(self):
        env = {"DEVICE_API_TOKEN": "right", "DEVICE_USER_EMAIL": "u@example.test"}
        with patch.dict("os.environ", env, clear=True):
            with self.assertRaises(HTTPException) as ctx:
                await device.device_portfolio(_request({"X-Device-Token": "wrong"}), top=10)
        self.assertEqual(ctx.exception.status_code, 403)

    async def test_rejects_missing_token_header(self):
        env = {"DEVICE_API_TOKEN": "right", "DEVICE_USER_EMAIL": "u@example.test"}
        with patch.dict("os.environ", env, clear=True):
            with self.assertRaises(HTTPException) as ctx:
                await device.device_portfolio(_request(), top=10)
        self.assertEqual(ctx.exception.status_code, 403)

    async def test_requires_device_user_email(self):
        with patch.dict("os.environ", {"DEVICE_API_TOKEN": "right"}, clear=True):
            with self.assertRaises(HTTPException) as ctx:
                await device.device_portfolio(_request({"X-Device-Token": "right"}), top=10)
        self.assertEqual(ctx.exception.status_code, 503)

    async def test_unknown_device_user_is_404(self):
        env = {"DEVICE_API_TOKEN": "right", "DEVICE_USER_EMAIL": "nobody@example.test"}
        with patch.dict("os.environ", env, clear=True), \
             patch("repositories.users.get_user_by_email", new=AsyncMock(return_value=None)):
            with self.assertRaises(HTTPException) as ctx:
                await device.device_portfolio(_request({"X-Device-Token": "right"}), top=10)
        self.assertEqual(ctx.exception.status_code, 404)

    async def test_valid_token_returns_summary_with_timestamp(self):
        env = {"DEVICE_API_TOKEN": "right", "DEVICE_USER_EMAIL": "u@example.test"}
        summary = {"total_value": 1, "holdings": []}
        with patch.dict("os.environ", env, clear=True), \
             patch("repositories.users.get_user_by_email",
                   new=AsyncMock(return_value={"google_sub": "sub-1"})), \
             patch("services.portfolio.device_summary.build_summary",
                   new=AsyncMock(return_value=summary)) as build:
            result = await device.device_portfolio(_request({"X-Device-Token": "right"}), top=5)

        build.assert_awaited_once_with("sub-1", top_n=5)
        self.assertEqual(result["total_value"], 1)
        self.assertIn("generated_at", result)


class DeviceSummaryTests(unittest.IsolatedAsyncioTestCase):
    async def _build(self, items, *, top_n=10):
        with patch("repositories.portfolio.get_portfolio", new=AsyncMock(return_value=items)), \
             patch("services.portfolio.quote_service.enrich_with_cached_quotes",
                   new=AsyncMock(side_effect=_enriched)), \
             patch("services.portfolio.fx.fx_rate_for_currency",
                   new=AsyncMock(side_effect=_fx_rate)):
            return await device_summary.build_summary("sub-1", top_n=top_n)

    async def test_empty_portfolio_returns_zeroed_summary(self):
        with patch("repositories.portfolio.get_portfolio", new=AsyncMock(return_value=[])):
            result = await device_summary.build_summary("sub-1")
        self.assertEqual(result["total_value"], 0.0)
        self.assertEqual(result["holdings"], [])
        self.assertIsNone(result["total_pnl_pct"])

    async def test_market_value_does_not_reapply_fx_to_the_quote(self):
        """회귀 방지: 시세는 이미 원화다.

        여기에 환율을 한 번 더 곱하면 해외 종목이 환율 배수만큼 부풀어
        총액이 조 단위로, 수익률이 수만 %로 튄다 (실제로 그렇게 겪었다).
        """
        result = await self._build(_items())
        # B 평가액 = 2주 x 200,000원 = 400,000. 1300 을 곱하면 5.2억이 된다.
        beta = next(row for row in result["holdings"] if row["code"] == "B")
        self.assertEqual(beta["value"], 400_000)

    async def test_totals_use_krw_quotes_and_fx_converted_cost(self):
        result = await self._build(_items())
        # A: 10주 x 1,200원 = 12,000 / B: 2주 x 200,000원 = 400,000
        self.assertEqual(result["total_value"], 412_000)
        # 원금만 통화 환산: A 10 x 1,000 = 10,000 / B 2 x ($100 x 1300) = 260,000
        self.assertEqual(result["total_invested"], 270_000)
        self.assertEqual(result["total_pnl"], 142_000)
        self.assertEqual(result["total_pnl_pct"], 52.59)

    async def test_day_pnl_uses_per_share_change_in_krw(self):
        result = await self._build(_items())
        # A: 10 x 20 = +200 / B: 2 x 2,000 = +4,000
        self.assertEqual(result["day_pnl"], 4_200)
        self.assertEqual(result["day_pnl_pct"], 1.03)

    async def test_unpriced_holding_is_excluded_and_counted(self):
        result = await self._build(_items())
        # C 는 시세가 없어 평가액에도 원금에도 들어가지 않는다.
        self.assertEqual(result["unpriced"], 1)
        self.assertEqual(result["holdings_count"], 2)
        self.assertNotIn("C", [row["code"] for row in result["holdings"]])

    async def test_holdings_sorted_by_value_with_weights(self):
        result = await self._build(_items())
        self.assertEqual([row["code"] for row in result["holdings"]], ["B", "A"])
        self.assertEqual(result["holdings"][0]["weight_pct"], 97.09)
        self.assertEqual(result["holdings"][1]["weight_pct"], 2.91)
        self.assertEqual(result["holdings"][0]["pnl_pct"], 53.85)
        self.assertEqual(result["holdings"][1]["pnl_pct"], 20.0)

    async def test_top_n_truncates_holdings_but_not_the_count(self):
        result = await self._build(_items(), top_n=1)
        self.assertEqual(len(result["holdings"]), 1)
        self.assertEqual(result["holdings"][0]["code"], "B")
        self.assertEqual(result["holdings_count"], 2)


if __name__ == "__main__":
    unittest.main()
