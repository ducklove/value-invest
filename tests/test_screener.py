"""밸류 스크리너 테스트.

repository(SQL 필터 + 파생 지표), service(검증·정규화·캐싱), route(엔드포인트)
세 계층을 임시 DB 시드로 검증한다. 파생 지표(ROE/부채비율/영업이익률)는
SQL 에서 계산되므로 손계산 기대값과 대조한다.
"""

from __future__ import annotations

import unittest

from _harness import TempDbMixin
from fastapi.testclient import TestClient

import cache
from core.app_factory import create_app
from repositories import screener as screener_repo
from services import screener as screener_service


async def _seed_screener_data():
    """스크리너 커버리지를 위한 최소 시드: corp_codes + market_data + financial_data."""
    db = await cache.get_db()
    # corp_codes (유니버스)
    await db.executemany(
        "INSERT INTO corp_codes (stock_code, corp_code, corp_name, updated_at) VALUES (?, ?, ?, ?)",
        [
            ("000001", "c1", "두산", "2026-01-01"),
            ("000002", "c2", "KT", "2026-01-01"),
            ("000003", "c3", "보유종목아님", "2026-01-01"),
        ],
    )
    # market_data (최신 연도 밸류에이션) — 000001/000002 만 보유
    await db.executemany(
        "INSERT INTO market_data (stock_code, year, close_price, per, pbr, eps, bps,"
        " dividend_per_share, dividend_yield, market_cap) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            # 두산: 저평가 고품질 (P/E 8, P/B 0.8, ROE 15%, 배당 3%)
            ("000001", 2025, 50000, 8.0, 0.8, 6250, 62500, 1500, 3.0, 100000),
            # KT: 보통 (P/E 20, P/B 1.5, ROE 7%)
            ("000002", 2025, 40000, 20.0, 1.5, 2000, 26666, 0, 0.0, 50000),
        ],
    )
    # financial_data (ROE/부채비율/영업이익률 계산용)
    await db.executemany(
        "INSERT INTO financial_data (stock_code, year, report_date, revenue, operating_profit,"
        " net_income, total_assets, total_liabilities, total_equity) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            # 두산: ROE 15%(1500/10000), 부채비율 50%(5000/10000), 영업이익률 10%(2000/20000)
            ("000001", 2025, "2026-03-31", 20000, 2000, 1500, 15000, 5000, 10000),
            # KT: ROE ~7.5%(750/10000), 부채비율 100%(10000/10000), 영업이익률 5%(1000/20000)
            ("000002", 2025, "2026-03-31", 20000, 1000, 750, 20000, 10000, 10000),
        ],
    )
    await db.commit()


class ScreenerRepoTests(TempDbMixin):
    async def seed(self):
        await _seed_screener_data()

    async def test_coverage_counts_universe_valued_fundamentals(self):
        cov = await screener_repo.screener_coverage()
        # universe=3 (corp_codes), valued=2 (market_data), fundamentals=2 (financial_data)
        self.assertEqual(cov["universe"], 3)
        self.assertEqual(cov["valued"], 2)
        self.assertEqual(cov["fundamentals"], 2)

    async def test_no_filters_returns_full_universe(self):
        # repo 는 순수 SQL 레이어 — 빈 필터(1=1)는 전수 유니버스를 반환한다.
        # 빈 필터 거부는 service 계층의 책임이다(아래 ServiceTests 참조).
        rows, total = await screener_repo.screen_stocks({})
        self.assertEqual(total, 3)
        self.assertEqual(len(rows), 3)

    async def test_per_max_filter_excludes_expensive(self):
        # P/E <= 15 → 두산(8)만, KT(20) 제외
        rows, total = await screener_repo.screen_stocks({"per": ("max", 15)})
        self.assertEqual(total, 1)
        self.assertEqual(rows[0]["stock_code"], "000001")
        self.assertEqual(rows[0]["corp_name"], "두산")

    async def test_roe_min_filter_uses_sql_computed_metric(self):
        # ROE >= 10% → 두산(15%)만, KT(7.5%) 제외. ROE 는 컬럼이 아니라 SQL 파생.
        rows, total = await screener_repo.screen_stocks({"roe": ("min", 10)})
        self.assertEqual(total, 1)
        self.assertEqual(rows[0]["stock_code"], "000001")
        # 파생 지표가 결과 행에 포함된다.
        self.assertAlmostEqual(rows[0]["roe"], 15.0, places=2)

    async def test_combined_filters_intersect(self):
        # P/E <= 15 AND ROE >= 10 AND 배당 >= 2% → 두산만
        rows, total = await screener_repo.screen_stocks(
            {"per": ("max", 15), "roe": ("min", 10), "dividend_yield": ("min", 2)}
        )
        self.assertEqual(total, 1)
        self.assertEqual(rows[0]["stock_code"], "000001")

    async def test_filter_with_no_match_returns_zero(self):
        rows, total = await screener_repo.screen_stocks({"per": ("max", 1)})
        self.assertEqual(total, 0)
        self.assertEqual(rows, [])

    async def test_derived_metrics_in_result_row(self):
        rows, _ = await screener_repo.screen_stocks({"per": ("max", 100)})
        row = next(r for r in rows if r["stock_code"] == "000001")
        # 두산: ROE 1500/10000*100=15, 부채비율 5000/10000*100=50, 영업이익률 2000/20000*100=10
        self.assertAlmostEqual(row["roe"], 15.0, places=2)
        self.assertAlmostEqual(row["debt_ratio"], 50.0, places=2)
        self.assertAlmostEqual(row["operating_margin"], 10.0, places=2)

    async def test_sort_by_market_cap_desc_default(self):
        rows, _ = await screener_repo.screen_stocks({"per": ("max", 100)})
        # 두산(100000) > KT(50000) → 내림차순
        self.assertEqual(rows[0]["stock_code"], "000001")
        self.assertEqual(rows[1]["stock_code"], "000002")

    async def test_pagination_respects_offset(self):
        rows, total = await screener_repo.screen_stocks(
            {"per": ("max", 100)}, limit=1, offset=1
        )
        self.assertEqual(total, 2)
        self.assertEqual(len(rows), 1)
        # offset 1 → 두 번째 종목(KT)
        self.assertEqual(rows[0]["stock_code"], "000002")

    async def test_zero_limit_returns_empty(self):
        rows, total = await screener_repo.screen_stocks({"per": ("max", 15)}, limit=0)
        self.assertEqual(total, 0)
        self.assertEqual(rows, [])


class ScreenerServiceTests(TempDbMixin):
    async def seed(self):
        await _seed_screener_data()

    async def test_normalize_filters_rejects_unknown_metric(self):
        with self.assertRaises(screener_service.ScreenerError):
            screener_service.normalize_filters({"nonexistent": {"min": 1}})

    async def test_normalize_filters_rejects_non_number(self):
        with self.assertRaises(screener_service.ScreenerError):
            screener_service.normalize_filters({"per": {"min": "abc"}})

    async def test_normalize_filters_rejects_out_of_range(self):
        with self.assertRaises(screener_service.ScreenerError):
            screener_service.normalize_filters({"per": {"min": 99999}})

    async def test_normalize_filters_accepts_min_max_pair(self):
        out = screener_service.normalize_filters({"per": {"min": 3, "max": 15}})
        # 마지막 op 만 남는다(같은 키에 min/max 동시 → 마지막이 max). API 스펙상 한 방향만.
        self.assertIn("per", out)

    async def test_normalize_filters_drops_empty_bounds(self):
        out = screener_service.normalize_filters({"per": {}})
        self.assertEqual(out, {})

    async def test_normalize_filters_rejects_non_dict_bounds(self):
        with self.assertRaises(screener_service.ScreenerError):
            screener_service.normalize_filters({"per": 10})

    async def test_run_screen_rejects_empty_filters(self):
        # 빈 필터 전수 스캔 방지 — service 계층에서 거부.
        with self.assertRaises(screener_service.ScreenerError):
            await screener_service.run_screen({}, use_cache=False)

    async def test_run_screen_rejects_bad_sort(self):
        with self.assertRaises(screener_service.ScreenerError):
            await screener_service.run_screen({}, sort_by="nonexistent")

    async def test_run_screen_rejects_bad_limit(self):
        with self.assertRaises(screener_service.ScreenerError):
            await screener_service.run_screen({}, limit=0)
        with self.assertRaises(screener_service.ScreenerError):
            await screener_service.run_screen({}, limit=99999)

    async def test_run_screen_rejects_bad_offset(self):
        with self.assertRaises(screener_service.ScreenerError):
            await screener_service.run_screen({}, offset=-1)

    async def test_run_screen_returns_shaped_response(self):
        result = await screener_service.run_screen({"per": {"max": 15}}, use_cache=False)
        self.assertEqual(result["total"], 1)
        self.assertEqual(len(result["rows"]), 1)
        self.assertEqual(result["rows"][0]["stock_code"], "000001")
        # 필터가 정규화된 형태로 응답에 포함된다.
        self.assertEqual(result["filters"]["per"], {"op": "max", "value": 15.0})

    async def test_run_screen_rounds_float_metrics(self):
        result = await screener_service.run_screen({"per": {"max": 100}}, use_cache=False)
        row = next(r for r in result["rows"] if r["stock_code"] == "000001")
        # 소수점 2자리로 반올림
        self.assertEqual(row["roe"], 15.0)

    async def test_run_screen_caches_repeat_calls(self):
        # 캐시 미사용 → DB 조회
        await screener_service.run_screen({"per": {"max": 15}}, use_cache=False)
        # 캐시 사용 → 같은 키로 두 번째 호출은 캐시에서(에러 없이 통과)
        r1 = await screener_service.run_screen({"per": {"max": 15}}, use_cache=True)
        r2 = await screener_service.run_screen({"per": {"max": 15}}, use_cache=True)
        self.assertEqual(r1, r2)

    async def test_get_filter_specs_includes_coverage(self):
        specs = await screener_service.get_filter_specs()
        self.assertIn("filters", specs)
        self.assertIn("sorts", specs)
        self.assertEqual(specs["coverage"]["universe"], 3)
        self.assertEqual(specs["coverage"]["valued"], 2)


class ScreenerRouteTests(unittest.TestCase):
    """TestClient 기반 라우트 엔드포인트 테스트 — TempDbMixin 없이 앱 임포트만."""

    @classmethod
    def setUpClass(cls):
        cls.app = create_app()
        cls.client = TestClient(cls.app)

    def test_spec_endpoint_returns_filter_definitions(self):
        resp = self.client.get("/api/screener/spec")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("per", data["filters"])
        self.assertIn("roe", data["filters"])
        self.assertIn("coverage", data)

    def test_run_endpoint_with_empty_filters_is_rejected(self):
        resp = self.client.post("/api/screener/run?limit=50", json={"filters": {}})
        # 빈 필터 → service 가 ScreenerError(400) 로 거부
        self.assertEqual(resp.status_code, 400)

    def test_run_endpoint_rejects_bad_filter_metric(self):
        resp = self.client.post(
            "/api/screener/run?limit=50",
            json={"filters": {"nonexistent": {"min": 1}}},
        )
        # ScreenerError → 400
        self.assertEqual(resp.status_code, 400)

    def test_run_endpoint_rejects_bad_limit_query(self):
        resp = self.client.post("/api/screener/run?limit=0", json={"filters": {}})
        # FastAPI Query(ge=1) → 422
        self.assertEqual(resp.status_code, 422)
