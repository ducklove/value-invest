"""domain.market_calendar coverage + CSAT (수능) special-session handling (R8)."""

import json
import os
import unittest
from datetime import date, datetime, timedelta
from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin

from core.errors import AppError
from domain import market_calendar
from domain.market_calendar import MarketCalendarUnknown, closing_at
from routes import internal
from services import data_quality
from services.portfolio.time_windows import KST

CSAT_2026 = "2026-11-19"
_NO_OVERRIDES = {k: v for k, v in os.environ.items() if k != market_calendar.SESSIONS_ENV}


def _env(overrides: dict | None = None) -> dict:
    env = dict(_NO_OVERRIDES)
    if overrides is not None:
        env[market_calendar.SESSIONS_ENV] = json.dumps(overrides)
    return env


class HolidayTableCoverageTests(unittest.TestCase):
    def test_holiday_table_covers_today_plus_60_days(self):
        # Canary: fails ~2 months before the calendar runs out so the next
        # year's reviewed KRX holidays get added before settlement starts
        # raising "거래일 달력을 확인해야 합니다" every day.
        today = date.today()
        horizon = today + timedelta(days=60)
        missing = [y for y in range(today.year, horizon.year + 1) if y not in market_calendar.HOLIDAYS]
        self.assertEqual(missing, [], f"domain/market_calendar.py HOLIDAYS 에 {missing}년 KRX 휴장일을 추가하세요.")
        self.assertEqual(market_calendar.missing_holiday_years(today, 60), [])

    def test_missing_year_detection(self):
        self.assertEqual(market_calendar.missing_holiday_years(date(2027, 11, 15), 60), [2028])
        self.assertEqual(market_calendar.missing_holiday_years(date(2026, 9, 30), 60), [])

    def test_unknown_year_raises_calendar_unknown(self):
        with patch.dict(os.environ, _env(), clear=True):
            with self.assertRaises(MarketCalendarUnknown):
                closing_at("2028-01-04")


class CsatSessionTests(unittest.TestCase):
    def test_csat_2026_raises_without_override_and_is_value_error(self):
        with patch.dict(os.environ, _env(), clear=True):
            with self.assertRaises(MarketCalendarUnknown) as ctx:
                closing_at(CSAT_2026)
        self.assertIsInstance(ctx.exception, ValueError)  # 기존 except ValueError 경로 유지
        self.assertIn("수능일", str(ctx.exception))

    def test_override_sets_close_or_holiday(self):
        with patch.dict(os.environ, _env({CSAT_2026: "16:30"}), clear=True):
            close = closing_at(CSAT_2026)
        self.assertEqual((close.hour, close.minute), (16, 30))
        self.assertEqual(close.tzinfo, KST)
        with patch.dict(os.environ, _env({CSAT_2026: None}), clear=True):
            self.assertIsNone(closing_at(CSAT_2026))

    def test_regular_days_unchanged(self):
        with patch.dict(os.environ, _env(), clear=True):
            self.assertEqual(closing_at("2026-11-18").hour, 15)
            self.assertEqual(closing_at("2026-11-12").hour, 15)  # 둘째 목요일은 후보 아님
            self.assertIsNone(closing_at("2026-10-05"))
            self.assertIsNone(closing_at("2026-11-21"))

    def test_candidate_rule(self):
        self.assertTrue(market_calendar.is_csat_candidate(date(2026, 11, 19)))
        self.assertTrue(market_calendar.is_csat_candidate(date(2027, 11, 18)))
        self.assertFalse(market_calendar.is_csat_candidate(date(2026, 11, 12)))
        self.assertFalse(market_calendar.is_csat_candidate(date(2026, 11, 20)))

    def test_unconfigured_special_sessions_window(self):
        self.assertEqual(market_calendar.unconfigured_special_sessions(date(2026, 10, 20), 30, {}), [date(2026, 11, 19)])
        self.assertEqual(market_calendar.unconfigured_special_sessions(date(2026, 11, 19), 30, {}), [date(2026, 11, 19)])
        self.assertEqual(market_calendar.unconfigured_special_sessions(date(2026, 9, 30), 30, {}), [])
        self.assertEqual(market_calendar.unconfigured_special_sessions(date(2026, 11, 20), 30, {}), [])
        self.assertEqual(
            market_calendar.unconfigured_special_sessions(date(2026, 10, 20), 30, {CSAT_2026: "16:30"}), [])


class CalendarCoverageCheckTests(unittest.IsolatedAsyncioTestCase):
    async def _check(self, now, overrides=None, raw=None):
        env = _env(overrides)
        if raw is not None:
            env[market_calendar.SESSIONS_ENV] = raw
        with patch.dict(os.environ, env, clear=True):
            return await data_quality.check_market_calendar_coverage(now=now)

    async def test_warns_30_days_before_unconfigured_csat(self):
        result = await self._check(datetime(2026, 10, 20, 20, 30))
        self.assertEqual(result["check"], "market_calendar_coverage")
        self.assertEqual(result["status"], "warn")
        self.assertIn(CSAT_2026, result["detail"])
        self.assertIn("PORTFOLIO_MARKET_SESSIONS", result["detail"])
        self.assertEqual(result["value"], [CSAT_2026])

    async def test_ok_when_override_set_or_out_of_window(self):
        self.assertEqual((await self._check(datetime(2026, 10, 20, 20, 30), {CSAT_2026: "16:30"}))["status"], "ok")
        self.assertEqual((await self._check(datetime(2026, 9, 30, 20, 30)))["status"], "ok")

    async def test_warns_about_missing_year(self):
        result = await self._check(datetime(2027, 11, 5, 20, 30), {"2027-11-18": "16:30"})
        self.assertEqual(result["status"], "warn")
        self.assertIn("2028", result["detail"])
        self.assertEqual(result["value"], ["2028"])

    async def test_malformed_override_json_is_error(self):
        result = await self._check(datetime(2026, 10, 20, 20, 30), raw="{not json")
        self.assertEqual(result["status"], "error")


def _post(path: str):
    from starlette.requests import Request

    return Request({"type": "http", "method": "POST", "path": path, "headers": [], "query_string": b"",
                    "client": ("127.0.0.1", 1), "server": ("127.0.0.1", 3691), "scheme": "https"})


CSAT_EVENING = datetime(2026, 11, 19, 20, 10, tzinfo=KST)
CSAT_BRIEFING = datetime(2026, 11, 19, 15, 50, tzinfo=KST)


class CsatDayScheduledJobsTests(TempDbMixin):
    """On an unconfigured CSAT day each timer job fails as a logged error (500),
    never an unhandled exception that could take the process down."""

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.env = patch.dict(os.environ, _env(), clear=True)
        self.env.start()

    async def asyncTearDown(self):
        self.env.stop()
        await super().asyncTearDown()

    async def _assert_logged_failure(self, coro_factory, kind):
        with self.assertLogs("routes.internal", level="ERROR") as logs:
            with self.assertRaises(AppError) as ctx:
                await coro_factory()
        self.assertEqual(ctx.exception.status_code, 500)
        self.assertIn("수능일", str(ctx.exception))
        self.assertTrue(any(f"{kind} failed" in line for line in logs.output), logs.output)

    async def test_nav_snapshot_timer(self):
        with patch("services.portfolio.time_windows.now_kst", return_value=CSAT_EVENING):
            await self._assert_logged_failure(lambda: internal.run_nav_snapshot(_post("/api/internal/snapshot/nav")),
                                              "nav snapshot")

    async def test_after_close_timer(self):
        with patch("services.portfolio.time_windows.now_kst", return_value=CSAT_EVENING):
            await self._assert_logged_failure(
                lambda: internal.run_after_close_snapshot(_post("/api/internal/snapshot/after-close")),
                "after close snapshot")

    async def test_market_close_briefing_timer(self):
        request = _post("/api/internal/daily-briefing/send")
        request.scope["query_string"] = b"kind=market_close"
        with patch("services.portfolio.time_windows.now_kst", return_value=CSAT_BRIEFING), \
             patch("services.daily_briefing.opted_in_users", new=AsyncMock(return_value=["u1"])):
            await self._assert_logged_failure(lambda: internal.run_daily_briefing_send(request),
                                              "daily briefing send")

    async def test_data_quality_timer_reports_error_results_not_exception(self):
        with patch("services.market.indicator_health.summary",
                   new=AsyncMock(return_value={"check": "market_indicators", "status": "ok", "detail": "", "value": 0})):
            out = await data_quality.run_all_checks(now=CSAT_EVENING.replace(tzinfo=None), record=False)
        by_check = {r["check"]: r for r in out["results"]}
        self.assertEqual(by_check["market_calendar_coverage"]["status"], "warn")
        self.assertIn(CSAT_2026, by_check["market_calendar_coverage"]["detail"])
        self.assertEqual(by_check["nav_snapshot_freshness"]["status"], "error")
        self.assertIn("수능일", by_check["nav_snapshot_freshness"]["detail"])


if __name__ == "__main__":
    unittest.main()
