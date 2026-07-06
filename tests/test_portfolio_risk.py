"""services/portfolio/risk.py + routes/portfolio_risk.py 테스트.

수학 파트는 손으로 계산한 기대값(소수 4자리 반올림 규약)을 고정해 둔다 —
수식 규약(단순수익률, stdev×sqrt(252), 365.25일 연환산, Cov/Var 베타)이
바뀌면 여기 기대값도 의도적으로 함께 바꿔야 한다.
"""

import unittest
from datetime import date, timedelta
from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin, seed_user
from fastapi import HTTPException
from starlette.requests import Request

from repositories import benchmark_daily as benchmark_daily_repo
from repositories import db as db_repo
from routes import portfolio_risk as portfolio_risk_route
from services.portfolio import risk
from services.portfolio.time_windows import today_kst_date


def _series(rows):
    """[(date, nav)] → get_nav_history 모양의 [{date, nav, total_value}]."""
    return [{"date": d, "nav": v, "total_value": (v or 0) * 10} for d, v in rows]


# 손계산 기준 시리즈: 수익률 +10%, -10%, +10%
SERIES_A = _series([
    ("2026-06-01", 100.0),
    ("2026-06-02", 110.0),
    ("2026-06-03", 99.0),
    ("2026-06-04", 108.9),
])
# 벤치마크 수익률은 정확히 절반(+5%, -5%, +5%) → beta=2, correlation=1
BENCH_A = [
    {"date": "2026-06-01", "close": 100.0},
    {"date": "2026-06-02", "close": 105.0},
    {"date": "2026-06-03", "close": 99.75},
    {"date": "2026-06-04", "close": 104.7375},
]
ANCHOR_A = date(2026, 6, 9)


class RiskMathTests(unittest.TestCase):
    def test_cumulative_drawdown_best_worst(self):
        out = risk.compute_risk_metrics(SERIES_A, "ALL", today=ANCHOR_A)
        m = out["metrics"]
        # 누적: 108.9/100 - 1 = +8.9%
        self.assertAlmostEqual(m["cumulative_return_pct"], 8.9, places=3)
        # MDD: 고점 110(06-02) → 저점 99(06-03) = -10%
        self.assertAlmostEqual(m["max_drawdown_pct"], -10.0, places=3)
        self.assertEqual(m["max_drawdown_peak_date"], "2026-06-02")
        self.assertEqual(m["max_drawdown_trough_date"], "2026-06-03")
        # 현재 낙폭: 108.9/110 - 1 = -1%
        self.assertAlmostEqual(m["current_drawdown_pct"], -1.0, places=3)
        self.assertEqual(m["best_day"], {"date": "2026-06-02", "return_pct": 10.0})
        self.assertEqual(m["worst_day"], {"date": "2026-06-03", "return_pct": -10.0})
        self.assertEqual(out["points"], 4)
        self.assertEqual(out["start_date"], "2026-06-01")
        self.assertEqual(out["end_date"], "2026-06-04")
        self.assertFalse(out["insufficient"])
        self.assertIsNone(out["benchmark"])

    def test_volatility_and_sharpe(self):
        # stdev([0.1, -0.1, 0.1]) = 0.1154701 → ×sqrt(252)×100 = 183.3030%
        # sharpe(rf=0) = (0.0333333×252) / (0.1154701×sqrt(252)) = 4.5826
        out = risk.compute_risk_metrics(SERIES_A, "ALL", today=ANCHOR_A)
        self.assertAlmostEqual(out["metrics"]["annualized_volatility_pct"], 183.303, places=2)
        self.assertAlmostEqual(out["metrics"]["sharpe_ratio"], 4.5826, places=3)

        # rf=3.5%: (8.4 - 0.035) / 1.83303 = 4.5635
        out_rf = risk.compute_risk_metrics(SERIES_A, "ALL", risk_free_rate_pct=3.5, today=ANCHOR_A)
        self.assertAlmostEqual(out_rf["metrics"]["sharpe_ratio"], 4.5635, places=3)

    def test_annualized_return_calendar_days(self):
        # 100 → 121, 365일 경과: (1.21)^(365.25/365) - 1 = 21.0158%
        series = _series([("2025-06-09", 100.0), ("2026-06-09", 121.0)])
        out = risk.compute_risk_metrics(series, "ALL", today=date(2026, 6, 9))
        self.assertAlmostEqual(out["metrics"]["cumulative_return_pct"], 21.0, places=3)
        self.assertAlmostEqual(out["metrics"]["annualized_return_pct"], 21.0158, places=3)

    def test_beta_and_correlation_hand_computed(self):
        out = risk.compute_risk_metrics(
            SERIES_A, "ALL", benchmark_rows=BENCH_A, today=ANCHOR_A, min_benchmark_returns=2
        )
        self.assertEqual(out["benchmark"]["overlap_returns"], 3)
        self.assertAlmostEqual(out["benchmark"]["beta"], 2.0, places=4)
        self.assertAlmostEqual(out["benchmark"]["correlation"], 1.0, places=4)

    def test_beta_null_below_default_minimum_overlap(self):
        # 기본 하한(20개 수익률) 미만이면 beta/correlation 은 null
        out = risk.compute_risk_metrics(SERIES_A, "ALL", benchmark_rows=BENCH_A, today=ANCHOR_A)
        self.assertIsNotNone(out["benchmark"])
        self.assertIsNone(out["benchmark"]["beta"])
        self.assertIsNone(out["benchmark"]["correlation"])
        self.assertEqual(out["benchmark"]["overlap_returns"], 3)

    def test_window_slicing_base_point_semantics(self):
        series = _series([
            ("2025-01-02", 100.0),
            ("2025-12-30", 120.0),
            ("2026-01-05", 118.0),
            ("2026-05-09", 130.0),
            ("2026-05-15", 128.0),
            ("2026-06-01", 132.0),
        ])
        today = date(2026, 6, 9)

        # 1M 경계 2026-05-09 — base 는 경계일 이하 마지막 포인트(05-09 자신)
        out_1m = risk.compute_risk_metrics(series, "1M", today=today)
        self.assertEqual(out_1m["start_date"], "2026-05-09")
        self.assertEqual(out_1m["points"], 3)
        self.assertFalse(out_1m["insufficient"])

        # YTD base = 전년도 마지막 스냅샷(2025-12-30)
        out_ytd = risk.compute_risk_metrics(series, "YTD", today=today)
        self.assertEqual(out_ytd["start_date"], "2025-12-30")
        self.assertEqual(out_ytd["points"], 5)
        self.assertAlmostEqual(out_ytd["metrics"]["cumulative_return_pct"], 10.0, places=3)

        # 3M 경계 2026-03-09 → base = 2026-01-05
        out_3m = risk.compute_risk_metrics(series, "3M", today=today)
        self.assertEqual(out_3m["start_date"], "2026-01-05")
        self.assertEqual(out_3m["points"], 4)

        # 1Y 경계 2025-06-09 → base = 2025-01-02 (전체 포함)
        out_1y = risk.compute_risk_metrics(series, "1Y", today=today)
        self.assertEqual(out_1y["start_date"], "2025-01-02")
        self.assertEqual(out_1y["points"], 6)
        self.assertFalse(out_1y["insufficient"])

    def test_month_end_clamp(self):
        self.assertEqual(risk._shift_months(date(2026, 3, 31), 1), date(2026, 2, 28))
        self.assertEqual(risk._shift_months(date(2026, 1, 15), 12), date(2025, 1, 15))

    def test_short_series_flags_insufficient_but_computes(self):
        out = risk.compute_risk_metrics(SERIES_A, "1Y", today=ANCHOR_A)
        self.assertTrue(out["insufficient"])
        # 짧아도 가능한 구간은 계산한다
        self.assertAlmostEqual(out["metrics"]["cumulative_return_pct"], 8.9, places=3)
        self.assertEqual(out["points"], 4)

    def test_invalid_nav_rows_are_skipped(self):
        series = _series([
            ("2026-06-01", 100.0),
            ("2026-06-02", None),   # 결측
            ("2026-06-03", 0.0),    # 0 NAV 가드
            ("2026-06-04", 110.0),
        ])
        out = risk.compute_risk_metrics(series, "ALL", today=ANCHOR_A)
        self.assertEqual(out["points"], 2)
        self.assertAlmostEqual(out["metrics"]["cumulative_return_pct"], 10.0, places=3)

    def test_empty_series(self):
        out = risk.compute_risk_metrics([], "1Y", today=ANCHOR_A)
        self.assertTrue(out["insufficient"])
        self.assertEqual(out["points"], 0)
        self.assertIsNone(out["start_date"])
        self.assertIsNone(out["end_date"])
        self.assertIsNone(out["metrics"]["cumulative_return_pct"])
        self.assertIsNone(out["benchmark"])

    def test_unknown_window_raises(self):
        with self.assertRaises(ValueError):
            risk.compute_risk_metrics(SERIES_A, "2W", today=ANCHOR_A)


def _request(path: str = "/api/portfolio/risk") -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "headers": [],
        "query_string": b"",
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
        "scheme": "http",
    }
    return Request(scope)


class PortfolioRiskRouteTests(TempDbMixin):
    async def seed(self):
        portfolio_risk_route._risk_cache.clear()

        await seed_user()
        db = await db_repo.get_db()

        # 오늘(KST) 기준 과거 100일치 일별 스냅샷 — 포트폴리오 수익률은
        # +2% / -1% 교대, 벤치마크는 정확히 그 절반 → beta=2, corr=1.
        today = today_kst_date()
        nav, close = 1000.0, 300.0
        snap_rows, bench_rows = [], []
        for i in range(100):
            d = (today - timedelta(days=100 - i)).isoformat()
            snap_rows.append(("u1", d, nav * 10, nav * 10, nav, 10.0))
            bench_rows.append({"date": d, "close": close})
            r = 0.02 if i % 2 == 0 else -0.01
            nav *= 1 + r
            close *= 1 + r / 2
        await db.executemany(
            "INSERT INTO portfolio_snapshots (google_sub, date, total_value, total_invested, nav, total_units) VALUES (?, ?, ?, ?, ?, ?)",
            snap_rows,
        )
        await db.commit()
        await benchmark_daily_repo.save_benchmark_rows("IDX_KOSPI", bench_rows)

    async def asyncTearDown(self):
        portfolio_risk_route._risk_cache.clear()
        await super().asyncTearDown()

    async def test_risk_endpoint_payload_contract(self):
        user = {"google_sub": "u1", "email": "user@example.com"}
        with patch("routes.portfolio_risk.get_current_user", AsyncMock(return_value=user)):
            out = await portfolio_risk_route.get_portfolio_risk(_request(), window="3m", benchmark=None)

        self.assertEqual(out["window"], "3M")
        self.assertFalse(out["insufficient"])
        self.assertGreater(out["points"], 20)
        self.assertIsNotNone(out["start_date"])
        self.assertIsNotNone(out["end_date"])
        m = out["metrics"]
        for key in (
            "cumulative_return_pct",
            "annualized_return_pct",
            "annualized_volatility_pct",
            "max_drawdown_pct",
            "current_drawdown_pct",
            "sharpe_ratio",
        ):
            self.assertIsNotNone(m[key], key)
        self.assertLessEqual(m["max_drawdown_pct"], 0)
        self.assertEqual(m["best_day"]["return_pct"], 2.0)
        self.assertEqual(m["worst_day"]["return_pct"], -1.0)

        bench = out["benchmark"]
        self.assertEqual(bench["code"], "IDX_KOSPI")
        self.assertEqual(bench["name"], "코스피")
        self.assertAlmostEqual(bench["beta"], 2.0, places=3)
        self.assertAlmostEqual(bench["correlation"], 1.0, places=3)

    async def test_benchmark_without_rows_is_null(self):
        user = {"google_sub": "u1"}
        with patch("routes.portfolio_risk.get_current_user", AsyncMock(return_value=user)):
            out = await portfolio_risk_route.get_portfolio_risk(
                _request(), window="3M", benchmark="IDX_KOSDAQ"
            )
        self.assertIsNone(out["benchmark"])

    async def test_result_is_cached_per_user_window_benchmark(self):
        user = {"google_sub": "u1"}
        with patch("routes.portfolio_risk.get_current_user", AsyncMock(return_value=user)):
            first = await portfolio_risk_route.get_portfolio_risk(_request(), window="1M", benchmark=None)
            # 스냅샷을 모두 지워도 TTL 캐시가 같은 결과를 돌려준다
            db = await db_repo.get_db()
            await db.execute("DELETE FROM portfolio_snapshots")
            await db.commit()
            second = await portfolio_risk_route.get_portfolio_risk(_request(), window="1M", benchmark=None)
            other_window = await portfolio_risk_route.get_portfolio_risk(_request(), window="ALL", benchmark=None)

        self.assertEqual(first, second)
        self.assertEqual(other_window["points"], 0)  # 다른 키 → DB 재조회

    async def test_requires_login(self):
        with patch("routes.portfolio_risk.get_current_user", AsyncMock(return_value=None)):
            with self.assertRaises(HTTPException) as exc:
                await portfolio_risk_route.get_portfolio_risk(_request(), window="1Y", benchmark=None)
        self.assertEqual(exc.exception.status_code, 401)

    async def test_rejects_bad_window_and_benchmark(self):
        user = {"google_sub": "u1"}
        with patch("routes.portfolio_risk.get_current_user", AsyncMock(return_value=user)):
            with self.assertRaises(HTTPException) as exc:
                await portfolio_risk_route.get_portfolio_risk(_request(), window="2W", benchmark=None)
            self.assertEqual(exc.exception.status_code, 400)

            with self.assertRaises(HTTPException) as exc:
                await portfolio_risk_route.get_portfolio_risk(
                    _request(), window="1Y", benchmark="bad code!"
                )
            self.assertEqual(exc.exception.status_code, 400)


if __name__ == "__main__":
    unittest.main()
