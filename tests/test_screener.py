"""밸류 스크리너 테스트.

repository(인메모리 필터), service(검증·정규화·스냅샷 캐시·finance-pi 호출),
route(엔드포인트) 세 계층을 검증한다. 더 이상 로컬 DB 시드를 쓰지 않고
finance-pi 스냅샷 응답(fixture 리스트)을 mock 한다 — 실제 동작과 동일.
"""

from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from core.app_factory import create_app
from repositories import screener as screener_repo
from services import screener as screener_service


def _snapshot_fixture() -> list[dict]:
    """finance-pi 가 반환할 가상의 전 유니버스 스냅샷."""
    return [
        # 두산: 저평가 고품질 (P/E 8, P/B 0.8, ROE 15%, 배당 3%)
        {
            "ticker": "000001", "name": "두산", "market": "KOSPI",
            "close": 50000, "per": 8.0, "pbr": 0.8, "roe": 15.0,
            "market_cap": 100000, "dividend_yield": 3.0,
            "operating_margin": 10.0, "debt_ratio": 50.0,
            "revenue": 20000, "operating_profit": 2000,
            "net_income": 1500, "equity": 10000, "as_of": "2026-04-29",
        },
        # KT: 보통 (P/E 20, P/B 1.5, ROE 7.5%)
        {
            "ticker": "000002", "name": "KT", "market": "KOSPI",
            "close": 40000, "per": 20.0, "pbr": 1.5, "roe": 7.5,
            "market_cap": 50000, "dividend_yield": 0.0,
            "operating_margin": 5.0, "debt_ratio": 100.0,
            "revenue": 20000, "operating_profit": 1000,
            "net_income": 750, "equity": 10000, "as_of": "2026-04-29",
        },
        # 신성비: 재무 데이터 없음 (PBR/ROE/equity null) — 시세/PER(naver)만
        {
            "ticker": "000003", "name": "신성비", "market": "KOSDAQ",
            "close": 10000, "per": 25.0, "pbr": None, "roe": None,
            "market_cap": 5000, "dividend_yield": None,
            "operating_margin": None, "debt_ratio": None,
            "revenue": None, "operating_profit": None,
            "net_income": None, "equity": None, "as_of": "2026-04-29",
        },
    ]


class ScreenerRepoTests(unittest.TestCase):
    """repositories/screener.py — 인메모리 필터/정렬/페이징 (순수 함수)."""

    def setUp(self):
        self.snapshot = _snapshot_fixture()

    def test_per_max_filter_excludes_expensive(self):
        rows, total = screener_repo.screen_snapshot(
            self.snapshot, {"per": ("max", 15)}
        )
        self.assertEqual(total, 1)
        self.assertEqual(rows[0]["ticker"], "000001")

    def test_roe_min_filter(self):
        rows, total = screener_repo.screen_snapshot(
            self.snapshot, {"roe": ("min", 10)}
        )
        self.assertEqual(total, 1)
        self.assertEqual(rows[0]["ticker"], "000001")

    def test_combined_filters_intersect(self):
        rows, total = screener_repo.screen_snapshot(
            self.snapshot,
            {"per": ("max", 15), "roe": ("min", 10), "dividend_yield": ("min", 2)},
        )
        self.assertEqual(total, 1)
        self.assertEqual(rows[0]["ticker"], "000001")

    def test_filter_excludes_null_metric_rows(self):
        # 신성비는 pbr/roe 가 null → roe 필터에 걸리지 않는다.
        rows, total = screener_repo.screen_snapshot(
            self.snapshot, {"roe": ("min", 1)}
        )
        tickers = {r["ticker"] for r in rows}
        self.assertNotIn("000003", tickers)

    def test_filter_with_no_match_returns_zero(self):
        rows, total = screener_repo.screen_snapshot(
            self.snapshot, {"per": ("max", 1)}
        )
        self.assertEqual(total, 0)
        self.assertEqual(rows, [])

    def test_sort_by_market_cap_desc(self):
        rows, _ = screener_repo.screen_snapshot(
            self.snapshot, {"per": ("max", 100)}
        )
        # 두산(100000) > KT(50000) > 신성비(5000) → 내림차순
        self.assertEqual([r["ticker"] for r in rows], ["000001", "000002", "000003"])

    def test_pagination_respects_offset(self):
        rows, total = screener_repo.screen_snapshot(
            self.snapshot, {"per": ("max", 100)}, limit=1, offset=1
        )
        self.assertEqual(total, 3)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["ticker"], "000002")

    def test_zero_limit_returns_empty(self):
        rows, total = screener_repo.screen_snapshot(
            self.snapshot, {"per": ("max", 15)}, limit=0
        )
        self.assertEqual(total, 0)
        self.assertEqual(rows, [])

    def test_result_row_has_friendly_aliases(self):
        rows, _ = screener_repo.screen_snapshot(
            self.snapshot, {"per": ("max", 15)}
        )
        # ticker → stock_code, name → corp_name, close → close_price alias 추가
        self.assertEqual(rows[0]["stock_code"], "000001")
        self.assertEqual(rows[0]["corp_name"], "두산")
        self.assertEqual(rows[0]["close_price"], 50000)

    def test_snapshot_coverage(self):
        cov = screener_repo.snapshot_coverage(self.snapshot)
        self.assertEqual(cov["universe"], 3)
        self.assertEqual(cov["valued"], 3)  # 전부 close 있음
        self.assertEqual(cov["fundamentals"], 2)  # 신성비만 equity 없음


class ScreenerServiceTests(unittest.IsolatedAsyncioTestCase):
    """services/screener.py — 검증 + finance-pi 호출 mock."""

    async def test_normalize_filters_rejects_unknown_metric(self):
        with self.assertRaises(screener_service.ScreenerError):
            screener_service.normalize_filters({"nonexistent": {"min": 1}})

    async def test_normalize_filters_rejects_non_number(self):
        with self.assertRaises(screener_service.ScreenerError):
            screener_service.normalize_filters({"per": {"min": "abc"}})

    async def test_normalize_filters_rejects_out_of_range(self):
        with self.assertRaises(screener_service.ScreenerError):
            screener_service.normalize_filters({"per": {"min": 99999}})

    async def test_normalize_filters_drops_empty_bounds(self):
        out = screener_service.normalize_filters({"per": {}})
        self.assertEqual(out, {})

    async def test_normalize_filters_rejects_non_dict_bounds(self):
        with self.assertRaises(screener_service.ScreenerError):
            screener_service.normalize_filters({"per": 10})

    async def test_run_screen_rejects_empty_filters(self):
        with self.assertRaises(screener_service.ScreenerError):
            await screener_service.run_screen({}, use_cache=False)

    async def test_run_screen_rejects_bad_sort(self):
        with self.assertRaises(screener_service.ScreenerError):
            await screener_service.run_screen({}, sort_by="nonexistent", use_cache=False)

    async def test_run_screen_rejects_bad_limit(self):
        with self.assertRaises(screener_service.ScreenerError):
            await screener_service.run_screen({}, limit=0, use_cache=False)

    async def test_run_screen_returns_shaped_response(self):
        with patch.object(screener_service, "get_screener_snapshot", new=AsyncMock(
            return_value={"as_of": "2026-04-29", "count": 3, "rows": _snapshot_fixture()}
        )):
            result = await screener_service.run_screen(
                {"per": {"max": 15}}, use_cache=False
            )
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["rows"][0]["stock_code"], "000001")
        self.assertEqual(result["filters"]["per"], {"op": "max", "value": 15.0})

    async def test_get_filter_specs_includes_coverage(self):
        with patch.object(screener_service, "get_screener_snapshot", new=AsyncMock(
            return_value={"as_of": "2026-04-29", "count": 3, "rows": _snapshot_fixture()}
        )):
            specs = await screener_service.get_filter_specs()
        self.assertIn("filters", specs)
        self.assertEqual(specs["coverage"]["universe"], 3)
        self.assertEqual(specs["coverage"]["fundamentals"], 2)

    async def test_run_screen_maps_finance_pi_failure_to_502(self):
        import cache as cache_module
        from close_price_client import ClosePriceClientError
        from core.errors import ExternalServiceError
        # 스냅샷 캐시를 비워 _get_snapshot 이 실제 get_screener_snapshot 을 호출하게 한다.
        await cache_module.delete_cache_value(
            screener_service._SNAPSHOT_CACHE_NS, "latest"
        )
        with patch.object(screener_service, "get_screener_snapshot", new=AsyncMock(
            side_effect=ClosePriceClientError("boom")
        )):
            with self.assertRaises(ExternalServiceError):
                await screener_service.run_screen({"per": {"max": 15}}, use_cache=False)


class ScreenerRouteTests(unittest.TestCase):
    """TestClient 기반 라우트 엔드포인트 테스트."""

    @classmethod
    def setUpClass(cls):
        cls.app = create_app()
        cls.client = TestClient(cls.app)

    def test_spec_endpoint_returns_filter_definitions(self):
        # spec 은 coverage 를 finance-pi 에서 가져오지만, 캐시가 비어있으면
        # 빈 스냅샷으로 폴백한다 — 여기서는 필터 정의가 반환되는지만 검증.
        resp = self.client.get("/api/screener/spec")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("per", data["filters"])
        self.assertIn("roe", data["filters"])
        self.assertIn("coverage", data)

    def test_run_endpoint_with_empty_filters_is_rejected(self):
        resp = self.client.post("/api/screener/run?limit=50", json={"filters": {}})
        self.assertEqual(resp.status_code, 400)

    def test_run_endpoint_rejects_bad_filter_metric(self):
        resp = self.client.post(
            "/api/screener/run?limit=50",
            json={"filters": {"nonexistent": {"min": 1}}},
        )
        self.assertEqual(resp.status_code, 400)

    def test_run_endpoint_rejects_bad_limit_query(self):
        resp = self.client.post("/api/screener/run?limit=0", json={"filters": {}})
        self.assertEqual(resp.status_code, 422)
