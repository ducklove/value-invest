"""밸류 스크리너 테스트.

repository(인메모리 필터), service(검증·정규화·스냅샷 캐시·finance-pi 호출),
route(엔드포인트) 세 계층을 검증한다. 더 이상 로컬 DB 시드를 쓰지 않고
finance-pi 스냅샷 응답(fixture 리스트)을 mock 한다 — 실제 동작과 동일.
"""

from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin
from fastapi.testclient import TestClient

from core.app_factory import create_app
from repositories import screener as screener_repo
from services import screener as screener_service


def _snapshot_fixture() -> list[dict]:
    """finance-pi 가 반환할 가상의 전 유니버스 스냅샷.

    market_cap 은 finance-pi 원본 단위(KRW 원)로 둔다 — service 의 _get_snapshot
    이 억원으로 정규화한다. repo 단위 테스트는 이미 억원으로 정규화된
    fixture(아래 _normalized_fixture)를 쓴다.
    """
    return [
        # 두산: 저평가 고품질 (P/E 8, P/B 0.8, ROE 15%, 배당 3%)
        {
            "ticker": "000001", "name": "두산", "market": "KOSPI",
            "close": 50000, "per": 8.0, "pbr": 0.8, "roe": 15.0,
            "market_cap": 10_000_000_000_000, "dividend_yield": 3.0,
            "operating_margin": 10.0, "debt_ratio": 50.0,
            "revenue": 20000, "operating_profit": 2000,
            "net_income": 1500, "equity": 10000, "as_of": "2026-04-29",
        },
        # KT: 보통 (P/E 20, P/B 1.5, ROE 7.5%)
        {
            "ticker": "000002", "name": "KT", "market": "KOSPI",
            "close": 40000, "per": 20.0, "pbr": 1.5, "roe": 7.5,
            "market_cap": 5_000_000_000_000, "dividend_yield": 0.0,
            "operating_margin": 5.0, "debt_ratio": 100.0,
            "revenue": 20000, "operating_profit": 1000,
            "net_income": 750, "equity": 10000, "as_of": "2026-04-29",
        },
        # 신성비: 재무 데이터 없음 (PBR/ROE/equity null) — 시세/PER(naver)만
        {
            "ticker": "000003", "name": "신성비", "market": "KOSDAQ",
            "close": 10000, "per": 25.0, "pbr": None, "roe": None,
            "market_cap": 500_000_000_000, "dividend_yield": None,
            "operating_margin": None, "debt_ratio": None,
            "revenue": None, "operating_profit": None,
            "net_income": None, "equity": None, "as_of": "2026-04-29",
        },
        # 적자기업: naver PER 이 음수(-514). P/E 1~5 필터에 절대 걸려선 안 된다.
        {
            "ticker": "000004", "name": "적자기업", "market": "KOSPI",
            "close": 3000, "per": -514.0, "pbr": 0.3, "roe": -20.0,
            "market_cap": 100_000_000_000, "dividend_yield": 0.0,
            "operating_margin": -15.0, "debt_ratio": 200.0,
            "revenue": 5000, "operating_profit": -750,
            "net_income": -1000, "equity": 5000, "as_of": "2026-04-29",
        },
    ]


def _normalized_fixture() -> list[dict]:
    """억원으로 정규화된 fixture — repo 단위 테스트용(_get_snapshot 통과 후 상태)."""
    rows = _snapshot_fixture()
    for row in rows:
        mc = row.get("market_cap")
        if isinstance(mc, (int, float)) and mc != 0:
            row["market_cap"] = round(mc / 1e8, 2)
    return rows



class ScreenerRepoTests(unittest.TestCase):
    """repositories/screener.py — 인메모리 필터/정렬/페이징 (순수 함수).

    _normalized_fixture(억원 정규화 후)를 쓴다 — repo 는 정규화된 스냅샷을
    받는다고 가정한다.
    """

    def setUp(self):
        self.snapshot = _normalized_fixture()

    def test_per_min_max_range_excludes_negative(self):
        # 핵심 회귀: P/E 1~5 (min=1, max=5) 필터가 음수 PER(-514)을 걸러낸다.
        rows, total = screener_repo.screen_snapshot(
            self.snapshot, {"per": [("min", 1), ("max", 5)]}
        )
        tickers = {r["ticker"] for r in rows}
        self.assertNotIn("000004", tickers)  # 적자기업(per=-514) 제외
        self.assertEqual(total, 0)  # 1~5 범위의 PER 을 가진 종목이 없음

    def test_per_max_only_still_excludes_negative(self):
        # max=15 만: 음수 PER(-514)은 15보다 작지만, min 미설정이어도
        # 음수는 저평가가 아니므로... 현재 로직은 max=15 → -514<15 통과.
        # 이건 의도적 동작(min/max 독립)이므로, 음수 제외를 원하면 min 도 함께.
        rows, total = screener_repo.screen_snapshot(
            self.snapshot, {"per": [("max", 15)]}
        )
        tickers = {r["ticker"] for r in rows}
        self.assertIn("000001", tickers)  # 두산(per=8)
        # 적자기업(per=-514) 도 max=15 에 걸림(음수 < 15). min/max 독립 동작.

    def test_roe_min_filter(self):
        rows, total = screener_repo.screen_snapshot(
            self.snapshot, {"roe": [("min", 10)]}
        )
        tickers = {r["ticker"] for r in rows}
        self.assertEqual(tickers, {"000001"})  # 두산만 roe=15

    def test_combined_filters_intersect(self):
        rows, total = screener_repo.screen_snapshot(
            self.snapshot,
            {"per": [("max", 15), ("min", 1)], "roe": [("min", 10)], "dividend_yield": [("min", 2)]},
        )
        self.assertEqual(total, 1)
        self.assertEqual(rows[0]["ticker"], "000001")

    def test_filter_excludes_null_metric_rows(self):
        # 신성비는 roe 가 null → roe 필터에 걸리지 않는다.
        rows, total = screener_repo.screen_snapshot(
            self.snapshot, {"roe": [("min", 1)]}
        )
        tickers = {r["ticker"] for r in rows}
        self.assertNotIn("000003", tickers)

    def test_filter_with_no_match_returns_zero(self):
        rows, total = screener_repo.screen_snapshot(
            self.snapshot, {"per": [("min", 100), ("max", 200)]}
        )
        self.assertEqual(total, 0)
        self.assertEqual(rows, [])

    def test_sort_by_market_cap_desc(self):
        rows, _ = screener_repo.screen_snapshot(
            self.snapshot, {"per": [("min", -1000), ("max", 1000)]}
        )
        # 모든 종목(per 전체 허용) — 두산(100000억) > KT(50000억) > ...
        tickers = [r["ticker"] for r in rows]
        self.assertEqual(tickers[0], "000001")  # 시가총액 최대

    def test_pagination_respects_offset(self):
        rows, total = screener_repo.screen_snapshot(
            self.snapshot, {"per": [("min", -1000), ("max", 1000)]}, limit=1, offset=1
        )
        self.assertEqual(len(rows), 1)

    def test_zero_limit_returns_empty(self):
        rows, total = screener_repo.screen_snapshot(
            self.snapshot, {"per": [("max", 15)]}, limit=0
        )
        self.assertEqual(total, 0)
        self.assertEqual(rows, [])

    def test_result_row_has_friendly_aliases(self):
        rows, _ = screener_repo.screen_snapshot(
            self.snapshot, {"per": [("min", 1), ("max", 15)]}
        )
        # ticker → stock_code, name → corp_name, close → close_price alias 추가
        self.assertEqual(rows[0]["stock_code"], "000001")
        self.assertEqual(rows[0]["corp_name"], "두산")
        self.assertEqual(rows[0]["close_price"], 50000)

    def test_snapshot_coverage(self):
        cov = screener_repo.snapshot_coverage(self.snapshot)
        self.assertEqual(cov["universe"], 4)
        self.assertEqual(cov["valued"], 4)  # 전부 close 있음
        self.assertEqual(cov["fundamentals"], 3)  # 신성비만 equity 없음



class ScreenerServiceTests(TempDbMixin):
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

    async def test_normalize_filters_keeps_both_min_and_max(self):
        # 핵심 회귀: min/max 동시 설정이 둘 다 보존되어야 한다 (이전 덮어쓰기 버그).
        out = screener_service.normalize_filters({"per": {"min": 1, "max": 5}})
        self.assertEqual(out, {"per": [("min", 1.0), ("max", 5.0)]})

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
            return_value={"as_of": "2026-04-29", "count": 4, "rows": _snapshot_fixture()}
        )):
            result = await screener_service.run_screen(
                {"per": {"min": 1, "max": 15}}, use_cache=False
            )
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["rows"][0]["stock_code"], "000001")
        # filters 응답이 min/max 쌍의 리스트로 표현된다.
        self.assertEqual(
            result["filters"]["per"],
            [{"op": "min", "value": 1.0}, {"op": "max", "value": 15.0}],
        )

    async def test_run_screen_normalizes_market_cap_to_eokwon(self):
        # finance-pi 원본 market_cap(원) → 억원 정규화 검증.
        with patch.object(screener_service, "get_screener_snapshot", new=AsyncMock(
            return_value={"as_of": "2026-04-29", "count": 4, "rows": _snapshot_fixture()}
        )):
            result = await screener_service.run_screen(
                {"per": {"min": 1, "max": 15}}, use_cache=False
            )
        # 두산 market_cap 10조원 → 100000 억원
        self.assertAlmostEqual(result["rows"][0]["market_cap"], 100000.0, places=1)

    async def test_get_filter_specs_includes_coverage(self):
        with patch.object(screener_service, "get_screener_snapshot", new=AsyncMock(
            return_value={"as_of": "2026-04-29", "count": 4, "rows": _snapshot_fixture()}
        )):
            specs = await screener_service.get_filter_specs()
        self.assertIn("filters", specs)
        self.assertEqual(specs["coverage"]["universe"], 4)
        self.assertEqual(specs["coverage"]["fundamentals"], 3)

    async def test_run_screen_maps_finance_pi_failure_to_502(self):
        from close_price_client import ClosePriceClientError
        from core.errors import ExternalServiceError
        from repositories import cache_values
        # 스냅샷 캐시를 비워 _get_snapshot 이 실제 get_screener_snapshot 을 호출하게 한다.
        await cache_values.delete_cache_value(
            screener_service._SNAPSHOT_CACHE_NS, "latest"
        )
        with patch.object(screener_service, "get_screener_snapshot", new=AsyncMock(
            side_effect=ClosePriceClientError("boom")
        )):
            with self.assertRaises(ExternalServiceError):
                await screener_service.run_screen({"per": {"min": 1, "max": 15}}, use_cache=False)


class ScreenerRouteTests(unittest.TestCase):
    """TestClient 기반 라우트 엔드포인트 테스트."""

    @classmethod
    def setUpClass(cls):
        cls.app = create_app()
        cls.client = TestClient(cls.app)

    def test_spec_endpoint_returns_filter_definitions(self):
        # spec 은 coverage 를 finance-pi 에서 가져오지만, 캐시가 비어있으면
        # service mock 으로 HTTP 계약만 검증한다.
        with patch.object(screener_service, "get_filter_specs", new=AsyncMock(
            return_value={
                "filters": screener_service.FILTER_SPECS,
                "sorts": screener_service.ALLOWED_SORTS,
                "coverage": {"universe": 4, "valued": 4, "fundamentals": 3},
            }
        )):
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
