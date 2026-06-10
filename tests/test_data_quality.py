"""Tests for the scheduled data-quality monitor (services/data_quality.py).

Covers the freshness check functions against a seeded temp DB (weekend
edge cases included), system_events recording via run_all_checks, the
loopback-guarded internal endpoint, and the admin event-summary exposure
the '데이터 품질' panel reads.
"""
import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from starlette.requests import Request

import cache
from _harness import TempDbMixin
from repositories import benchmark_daily as benchmark_repo
from repositories import system_events as system_events_repo
from repositories import snapshots as snapshots_repo
import observability
from routes import admin as admin_route
from routes import internal as internal_route
from services import data_quality


# 고정 기준 시각들 — 2026-06-10 은 수요일, 06-13/14 는 주말.
WED_LATE = datetime(2026, 6, 10, 22, 30)    # 평일, 22:00 스냅샷 이후
WED_MORNING = datetime(2026, 6, 10, 9, 0)   # 평일, 스냅샷 전·장 시작 직후
SAT_LATE = datetime(2026, 6, 13, 22, 30)    # 토요일
SUN_LATE = datetime(2026, 6, 14, 22, 30)    # 일요일
MON_MORNING = datetime(2026, 6, 8, 9, 0)    # 월요일 아침


def _request(path: str = "/", *, method: str = "POST", headers: dict[str, str] | None = None,
             client_host: str = "127.0.0.1") -> Request:
    encoded_headers = [
        (key.lower().encode("latin-1"), value.encode("latin-1"))
        for key, value in (headers or {}).items()
    ]
    scope = {
        "type": "http",
        "method": method,
        "path": path,
        "headers": encoded_headers,
        "query_string": b"",
        "client": (client_host, 12345),
        "server": ("testserver", 80),
        "scheme": "http",
    }
    return Request(scope)


class TradingDayHelperTests(unittest.TestCase):
    def test_weekday_after_settle_expects_same_day(self):
        self.assertEqual(
            data_quality.last_expected_trading_day(WED_LATE, settled_minutes=data_quality.SETTLED_MINUTES),
            date(2026, 6, 10),
        )

    def test_weekday_before_settle_expects_previous_weekday(self):
        self.assertEqual(
            data_quality.last_expected_trading_day(WED_MORNING, settled_minutes=data_quality.SETTLED_MINUTES),
            date(2026, 6, 9),
        )

    def test_monday_before_settle_expects_previous_friday(self):
        self.assertEqual(
            data_quality.last_expected_trading_day(MON_MORNING, settled_minutes=data_quality.SETTLED_MINUTES),
            date(2026, 6, 5),
        )

    def test_weekend_expects_friday(self):
        self.assertEqual(
            data_quality.last_expected_trading_day(SAT_LATE, settled_minutes=data_quality.SETTLED_MINUTES),
            date(2026, 6, 12),
        )
        self.assertEqual(
            data_quality.last_expected_trading_day(SUN_LATE, settled_minutes=data_quality.SETTLED_MINUTES),
            date(2026, 6, 12),
        )

    def test_no_settle_threshold_expects_same_weekday_any_time(self):
        self.assertEqual(data_quality.last_expected_trading_day(WED_MORNING), date(2026, 6, 10))

    def test_trading_day_gap_skips_weekend(self):
        # 월요일 기대, 금요일 데이터 → 빠진 거래일은 월요일 하루뿐.
        self.assertEqual(data_quality.trading_day_gap(date(2026, 6, 8), date(2026, 6, 5)), 1)
        # 수요일 기대, 전주 목요일 데이터 → 금/월/화/수 = 4 거래일.
        self.assertEqual(data_quality.trading_day_gap(date(2026, 6, 10), date(2026, 6, 4)), 4)
        # 최신이 기대 이상이면 0.
        self.assertEqual(data_quality.trading_day_gap(date(2026, 6, 10), date(2026, 6, 10)), 0)
        self.assertEqual(data_quality.trading_day_gap(date(2026, 6, 10), date(2026, 6, 12)), 0)


class _SeededDbTestCase(TempDbMixin):
    async def _seed_user_with_holdings(self, sub: str = "u1"):
        db = await cache.get_db()
        await db.execute(
            "INSERT INTO users (google_sub, email, name, picture, email_verified, created_at, last_login_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (sub, "a@b.c", "A", None, 1, "2026-01-01", "2026-01-01"),
        )
        await db.execute(
            "INSERT INTO user_portfolio (google_sub, stock_code, stock_name, avg_price, quantity, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (sub, "005930", "삼성전자", 60000.0, 10, "2026-01-01", "2026-01-01"),
        )
        await db.commit()

    async def _seed_snapshot(self, snap_date: str, sub: str = "u1"):
        db = await cache.get_db()
        await db.execute(
            "INSERT OR REPLACE INTO portfolio_snapshots (google_sub, date, total_value, total_invested, nav, total_units) VALUES (?, ?, ?, ?, ?, ?)",
            (sub, snap_date, 1_000_000, 900_000, 1100.0, 1000.0),
        )
        await db.commit()


class NavSnapshotFreshnessTests(_SeededDbTestCase):
    async def test_fresh_snapshot_is_ok(self):
        await self._seed_user_with_holdings()
        await self._seed_snapshot("2026-06-10")
        result = await data_quality.check_nav_snapshot_freshness(now=WED_LATE)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["value"], 0)

    async def test_one_trading_day_behind_is_warn(self):
        await self._seed_user_with_holdings()
        await self._seed_snapshot("2026-06-09")
        result = await data_quality.check_nav_snapshot_freshness(now=WED_LATE)
        self.assertEqual(result["status"], "warn")
        self.assertEqual(result["value"], 1)

    async def test_many_trading_days_behind_is_error(self):
        await self._seed_user_with_holdings()
        await self._seed_snapshot("2026-06-04")  # 목요일 — 4 거래일 지연
        result = await data_quality.check_nav_snapshot_freshness(now=WED_LATE)
        self.assertEqual(result["status"], "error")
        self.assertEqual(result["value"], 4)

    async def test_weekend_run_accepts_friday_snapshot(self):
        # 토요일 22:30 점검 — 금요일 스냅샷이면 신선(주말은 거래일 아님).
        await self._seed_user_with_holdings()
        await self._seed_snapshot("2026-06-12")
        result = await data_quality.check_nav_snapshot_freshness(now=SAT_LATE)
        self.assertEqual(result["status"], "ok")

    async def test_before_settle_time_expects_previous_trading_day(self):
        # 수요일 아침 수동 실행 — 화요일 스냅샷까지만 기대 (거짓 경보 방지).
        await self._seed_user_with_holdings()
        await self._seed_snapshot("2026-06-09")
        result = await data_quality.check_nav_snapshot_freshness(now=WED_MORNING)
        self.assertEqual(result["status"], "ok")

    async def test_missing_snapshots_with_holdings_is_error(self):
        await self._seed_user_with_holdings()
        result = await data_quality.check_nav_snapshot_freshness(now=WED_LATE)
        self.assertEqual(result["status"], "error")

    async def test_no_portfolio_users_skips(self):
        result = await data_quality.check_nav_snapshot_freshness(now=WED_LATE)
        self.assertEqual(result["status"], "ok")
        self.assertIn("생략", result["detail"])


class IntradayPointsTests(_SeededDbTestCase):
    async def test_weekend_skips(self):
        result = await data_quality.check_intraday_points(now=SAT_LATE)
        self.assertEqual(result["status"], "ok")
        self.assertIn("주말", result["detail"])

    async def test_before_market_open_skips(self):
        await self._seed_user_with_holdings()
        result = await data_quality.check_intraday_points(now=WED_MORNING)
        self.assertEqual(result["status"], "ok")
        self.assertIn("장 시작 전", result["detail"])

    async def test_no_users_skips(self):
        result = await data_quality.check_intraday_points(now=WED_LATE)
        self.assertEqual(result["status"], "ok")

    async def test_zero_points_on_trading_day_is_warn(self):
        await self._seed_user_with_holdings()
        result = await data_quality.check_intraday_points(now=WED_LATE)
        self.assertEqual(result["status"], "warn")
        self.assertEqual(result["value"], 0)

    async def test_points_present_is_ok(self):
        await self._seed_user_with_holdings()
        await snapshots_repo.save_intraday_snapshot("u1", "2026-06-10T10:00", 1_000_000)
        await snapshots_repo.save_intraday_snapshot("u1", "2026-06-10T10:30", 1_010_000)
        result = await data_quality.check_intraday_points(now=WED_LATE)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["value"], 2)


class BenchmarkFreshnessTests(_SeededDbTestCase):
    async def test_covers_every_tracked_code(self):
        import benchmark_history
        results = await data_quality.check_benchmark_freshness(now=WED_LATE)
        checks = {r["check"] for r in results}
        for code in benchmark_history.YF_TICKER:
            self.assertIn(f"benchmark_freshness_{code}", checks)

    async def test_missing_code_is_warn(self):
        results = await data_quality.check_benchmark_freshness(now=WED_LATE)
        self.assertTrue(all(r["status"] == "warn" for r in results))

    async def test_fresh_and_slightly_lagged_codes_are_ok(self):
        # 미국장 시차 감안 — 2 거래일 이내 지연은 ok.
        await benchmark_repo.save_benchmark_rows("KOSPI", [{"date": "2026-06-10", "close": 2800.0}])
        await benchmark_repo.save_benchmark_rows("SP500", [{"date": "2026-06-08", "close": 6000.0}])
        results = {r["check"]: r for r in await data_quality.check_benchmark_freshness(now=WED_LATE)}
        self.assertEqual(results["benchmark_freshness_KOSPI"]["status"], "ok")
        self.assertEqual(results["benchmark_freshness_SP500"]["status"], "ok")

    async def test_stale_code_escalates_to_error(self):
        # 2026-05-20(수) → 06-10 기준 거래일 15일 지연 — error.
        await benchmark_repo.save_benchmark_rows("GOLD", [{"date": "2026-05-20", "close": 2400.0}])
        results = {r["check"]: r for r in await data_quality.check_benchmark_freshness(now=WED_LATE)}
        self.assertEqual(results["benchmark_freshness_GOLD"]["status"], "error")


class SystemEventsErrorRateTests(_SeededDbTestCase):
    async def test_no_errors_is_ok(self):
        await system_events_repo.insert_system_event(level="info", source="snapshot_nav", kind="tick_ok")
        result = await data_quality.check_system_events_error_rate(now=datetime.now())
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["value"], 0)

    async def test_some_errors_is_warn(self):
        for _ in range(3):
            await system_events_repo.insert_system_event(level="error", source="kis_ws", kind="reconnect_failed")
        result = await data_quality.check_system_events_error_rate(now=datetime.now())
        self.assertEqual(result["status"], "warn")
        self.assertEqual(result["value"], 3)
        self.assertIn("kis_ws", result["detail"])

    async def test_error_flood_escalates(self):
        for _ in range(12):
            await system_events_repo.insert_system_event(level="error", source="http", kind="error")
        result = await data_quality.check_system_events_error_rate(now=datetime.now())
        self.assertEqual(result["status"], "error")

    async def test_own_events_are_excluded(self):
        # 자기증폭 방지 — data_quality 자신의 error 는 세지 않는다.
        await system_events_repo.insert_system_event(level="error", source="data_quality", kind="nav_snapshot_freshness")
        result = await data_quality.check_system_events_error_rate(now=datetime.now())
        self.assertEqual(result["status"], "ok")


class RunAllChecksTests(_SeededDbTestCase):
    async def test_records_non_ok_checks_and_summary(self):
        # 빈 DB: NAV/장중은 생략(ok), 벤치마크 3종은 데이터 없음(warn).
        out = await data_quality.run_all_checks(now=WED_LATE)
        self.assertEqual(out["counts"]["error"], 0)
        self.assertGreaterEqual(out["counts"]["warn"], 3)

        rows = await system_events_repo.get_system_events(source="data_quality")
        kinds = {r["kind"] for r in rows}
        self.assertIn("check_summary", kinds)
        self.assertIn("benchmark_freshness_KOSPI", kinds)
        # 요약 이벤트 level 은 warn 존재 → warning.
        summary_row = await system_events_repo.get_latest_event("data_quality", "check_summary")
        self.assertEqual(summary_row["level"], "warning")
        self.assertIn('"results"', summary_row["details"])

    async def test_all_ok_records_only_info_summary(self):
        await benchmark_repo.save_benchmark_rows("KOSPI", [{"date": "2026-06-10", "close": 2800.0}])
        await benchmark_repo.save_benchmark_rows("SP500", [{"date": "2026-06-09", "close": 6000.0}])
        await benchmark_repo.save_benchmark_rows("GOLD", [{"date": "2026-06-09", "close": 2400.0}])
        out = await data_quality.run_all_checks(now=WED_LATE)
        self.assertEqual(out["counts"]["warn"], 0)
        self.assertEqual(out["counts"]["error"], 0)

        rows = await system_events_repo.get_system_events(source="data_quality")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["kind"], "check_summary")
        self.assertEqual(rows[0]["level"], "info")

    async def test_error_check_sets_summary_level_error(self):
        await self._seed_user_with_holdings()  # 스냅샷 없음 → NAV error
        out = await data_quality.run_all_checks(now=WED_LATE)
        self.assertGreaterEqual(out["counts"]["error"], 1)
        summary_row = await system_events_repo.get_latest_event("data_quality", "check_summary")
        self.assertEqual(summary_row["level"], "error")
        # 실패 점검 개별 이벤트도 level=error 로 남는다.
        errs = await system_events_repo.get_system_events(source="data_quality", level="error")
        self.assertTrue(any(r["kind"] == "nav_snapshot_freshness" for r in errs))

    async def test_crashing_check_becomes_error_result_not_exception(self):
        with patch.object(data_quality, "check_nav_snapshot_freshness", AsyncMock(side_effect=RuntimeError("boom"))):
            out = await data_quality.run_all_checks(now=WED_LATE, record=False)
        crashed = [r for r in out["results"] if r["check"] == "nav_snapshot_freshness"]
        self.assertEqual(len(crashed), 1)
        self.assertEqual(crashed[0]["status"], "error")
        self.assertIn("boom", crashed[0]["detail"])

    async def test_record_false_writes_no_events(self):
        await data_quality.run_all_checks(now=WED_LATE, record=False)
        rows = await system_events_repo.get_system_events(source="data_quality")
        self.assertEqual(rows, [])


class InternalEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_rejects_non_loopback_client(self):
        request = _request("/api/internal/data-quality/check", client_host="203.0.113.5")
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(HTTPException) as exc_info:
                await internal_route.run_data_quality_check(request)
        self.assertEqual(exc_info.exception.status_code, 403)

    async def test_rejects_forwarded_loopback_without_token(self):
        request = _request(
            "/api/internal/data-quality/check",
            headers={"X-Forwarded-For": "203.0.113.10"},
        )
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(HTTPException) as exc_info:
                await internal_route.run_data_quality_check(request)
        self.assertEqual(exc_info.exception.status_code, 403)

    async def test_happy_path_returns_results_json(self):
        fake = {"counts": {"ok": 5, "warn": 1, "error": 0}, "results": [{"check": "x", "status": "warn", "detail": "d", "value": 1}]}
        request = _request("/api/internal/data-quality/check")
        with patch.dict("os.environ", {}, clear=True), \
             patch("services.data_quality.run_all_checks", AsyncMock(return_value=fake)):
            out = await internal_route.run_data_quality_check(request)
        self.assertTrue(out["ok"])
        self.assertEqual(out["counts"], fake["counts"])
        self.assertEqual(out["results"], fake["results"])

    async def test_failure_maps_to_500(self):
        request = _request("/api/internal/data-quality/check")
        with patch.dict("os.environ", {}, clear=True), \
             patch("services.data_quality.run_all_checks", AsyncMock(side_effect=RuntimeError("db locked"))):
            with self.assertRaises(HTTPException) as exc_info:
                await internal_route.run_data_quality_check(request)
        self.assertEqual(exc_info.exception.status_code, 500)


class AdminEventSummaryDataQualityTests(_SeededDbTestCase):
    """관리자 '데이터 품질' 카드의 데이터 계약 — event-summary 가 최신
    check_summary 이벤트(파싱된 details_obj 포함)를 내려준다."""

    def _admin_request(self) -> Request:
        return _request("/api/admin/event-summary", method="GET")

    async def _mk_admin(self) -> dict:
        return {"google_sub": "u1", "email": "a@b.c", "is_admin": True}

    async def test_summary_exposes_latest_data_quality_run(self):
        await observability.record_event(
            "data_quality", "check_summary",
            level="warning",
            details={"counts": {"ok": 4, "warn": 2, "error": 0},
                     "results": [{"check": "benchmark_freshness_GOLD", "status": "warn", "detail": "d", "value": 3}]},
            wait=True,
        )
        with patch("routes.admin.get_current_user", AsyncMock(return_value=await self._mk_admin())):
            out = await admin_route.event_summary(self._admin_request(), hours=24)
        dq = out["data_quality"]
        self.assertIsNotNone(dq)
        self.assertEqual(dq["details_obj"]["counts"]["warn"], 2)
        self.assertEqual(dq["details_obj"]["results"][0]["check"], "benchmark_freshness_GOLD")

    async def test_summary_data_quality_none_before_first_run(self):
        with patch("routes.admin.get_current_user", AsyncMock(return_value=await self._mk_admin())):
            out = await admin_route.event_summary(self._admin_request(), hours=24)
        self.assertIsNone(out["data_quality"])


if __name__ == "__main__":
    unittest.main()
