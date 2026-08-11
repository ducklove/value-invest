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


# 서비스에서 정한 순서대로. 평가액 순이라면 AAPL 이 먼저 와야 한다.
# 국내 6자리 코드는 벌크 경로로, 그 외는 개별 경로로 빠진다.
KR = "005930"
US = "AAPL"
ITEMS = [
    {"stock_code": KR, "stock_name": "삼성전자", "quantity": 10},
    {"stock_code": US, "stock_name": "Apple Inc.", "quantity": 2},
    {"stock_code": "IDX_KOSPI", "stock_name": "코스피", "quantity": 0},
    {"stock_code": "NOQUOTE", "stock_name": "시세없음", "quantity": 5},
]

# price/change_pct 는 시세 계층이 이미 원화로 환산해 내려준 값이다.
BULK = {KR: {"price": 1200, "change_pct": 1.69}}
PER_CODE = {US: {"price": 200_000, "change_pct": -0.5}}

LATEST = {"date": "2026-08-10", "nav": 4000.0, "total_units": 100.0, "total_value": 400_000}
BASELINE = {"date": "2026-08-10", "total_value": 400_000, "nav": 4000.0}
YEAR_START = {"date": "2025-12-30", "total_value": 300_000, "nav": 3000.0}


class DeviceSummaryTests(unittest.IsolatedAsyncioTestCase):
    async def _build(self, *, items=None, cashflows=(), latest=LATEST,
                     baseline=BASELINE, year_start=YEAR_START, top_n=10,
                     stale_quote=None):
        """stale_quote 를 주면 개별 조회까지 실패한 코드가 낡은 캐시로 채워진다."""
        async def fetch_quote(code, **kwargs):
            return PER_CODE.get(code, {})

        with patch("repositories.portfolio.get_portfolio",
                   new=AsyncMock(return_value=list(items if items is not None else ITEMS))), \
             patch("services.stock_quotes.get_bulk_quote_snapshots",
                   new=AsyncMock(return_value=dict(BULK))), \
             patch("services.portfolio.quote_service.fetch_quote", new=fetch_quote), \
             patch("services.stock_quotes.get_stock_cached",
                   return_value=object() if stale_quote else None), \
             patch("services.stock_quotes.stock_to_quote",
                   return_value=dict(stale_quote) if stale_quote else {}), \
             patch("repositories.snapshots.get_snapshot_on_or_before",
                   new=AsyncMock(return_value=baseline)), \
             patch("repositories.snapshots.get_cashflows_created_after",
                   new=AsyncMock(return_value=list(cashflows))), \
             patch("repositories.snapshots.get_latest_snapshot",
                   new=AsyncMock(return_value=latest)), \
             patch("repositories.snapshots.get_year_start_snapshot",
                   new=AsyncMock(return_value=year_start)):
            return await device_summary.build_summary("sub-1", top_n=top_n)

    async def test_empty_portfolio_returns_zeroed_summary(self):
        with patch("repositories.portfolio.get_portfolio", new=AsyncMock(return_value=[])):
            result = await device_summary.build_summary("sub-1")
        self.assertEqual(result["total_value"], 0.0)
        self.assertEqual(result["holdings"], [])
        self.assertIsNone(result["ytd_pnl_pct"])

    async def test_holdings_keep_the_order_set_in_the_service(self):
        """평가액 순으로 다시 정렬하지 않는다 — 사용자가 정한 순서가 화면 순서다."""
        result = await self._build()
        self.assertEqual([row["code"] for row in result["holdings"]], [KR, US])

    async def test_quotes_missing_from_bulk_are_fetched_individually(self):
        """캐시만 보고 포기하면 그 종목이 총액에서 빠져 화면이 작아진다."""
        result = await self._build()
        beta = next(row for row in result["holdings"] if row["code"] == US)
        self.assertEqual(beta["value"], 400_000)
        self.assertEqual(result["total_value"], 412_000)

    async def test_reference_rows_are_not_counted_as_unpriced(self):
        """IDX_/FX_ 는 보유가 아니라 참조행이라 시세가 없는 게 정상이다."""
        result = await self._build()
        self.assertEqual(result["unpriced"], 1)          # NOQUOTE 만
        self.assertNotIn("IDX_KOSPI", [r["code"] for r in result["holdings"]])

    async def test_stale_fallback_keeps_the_holding_in_the_total(self):
        result = await self._build(
            items=[{"stock_code": "NOQUOTE", "stock_name": "낡은시세", "quantity": 5}],
            stale_quote={"price": 1000},
        )
        self.assertEqual(result["total_value"], 5000)
        self.assertEqual(result["unpriced"], 0)
        self.assertTrue(result["stale"])

    async def test_day_return_is_nav_based(self):
        # 현재 NAV = 412,000 / 100좌 = 4,120 ; 기준 NAV = 4,000
        result = await self._build()
        self.assertEqual(result["day_pnl_pct"], 3.0)
        self.assertEqual(result["day_pnl"], 12_000)

    async def test_ytd_return_is_nav_based_not_cost_based(self):
        """매입가 대비가 아니라 연초 NAV 대비다 — 입출금이 성과로 잡히면 안 된다."""
        # 4,120 / 3,000 - 1 = 37.33%
        result = await self._build()
        self.assertEqual(result["ytd_pnl_pct"], 37.33)
        self.assertEqual(result["ytd_pnl"], 112_000)

    async def test_pending_deposit_is_removed_from_today_return(self):
        """오늘 입금 10만원은 수익이 아니다 — 손익에서 빼고 NAV 좌수로 흡수한다."""
        cashflows = [{"id": 1, "type": "deposit", "amount": 100_000,
                      "nav_at_time": 4000.0, "units_change": 25.0, "created_at": "x"}]
        result = await self._build(cashflows=cashflows)
        # 손익 = 412,000 - 400,000 - 100,000
        self.assertEqual(result["day_pnl"], -88_000)
        # NAV = 412,000 / (100 + 25) = 3,296 → (3296/4000 - 1) = -17.6%
        self.assertEqual(result["day_pnl_pct"], -17.6)

    async def test_cashflow_without_units_falls_back_to_value_adjustment(self):
        cashflows = [{"id": 1, "type": "deposit", "amount": 12_000,
                      "nav_at_time": None, "units_change": None, "created_at": "x"}]
        result = await self._build(cashflows=cashflows)
        # 좌수 정보가 없으면 평가액에서 빼고 기존 좌수로 나눈다: 400,000/100 = 4,000
        self.assertEqual(result["day_pnl_pct"], 0.0)

    async def test_missing_snapshots_leave_returns_blank_not_zero(self):
        result = await self._build(baseline=None, year_start=None, latest=None)
        self.assertIsNone(result["day_pnl"])
        self.assertIsNone(result["day_pnl_pct"])
        self.assertIsNone(result["ytd_pnl"])
        self.assertIsNone(result["ytd_pnl_pct"])
        self.assertEqual(result["total_value"], 412_000)

    async def test_weights_are_relative_to_the_whole_portfolio(self):
        result = await self._build()
        weights = {row["code"]: row["weight_pct"] for row in result["holdings"]}
        self.assertEqual(weights[KR], 2.91)
        self.assertEqual(weights[US], 97.09)

    async def test_top_n_truncates_holdings_but_not_the_count(self):
        result = await self._build(top_n=1)
        self.assertEqual([row["code"] for row in result["holdings"]], [KR])
        self.assertEqual(result["holdings_count"], 2)


if __name__ == "__main__":
    unittest.main()
