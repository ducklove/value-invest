"""services/dividend_calendar.py + routes/dividend_calendar.py 테스트.

서비스 수학(환산·월별 합계·추정 휴리스틱)은 시드한 임시 DB
(repositories.db.DB_PATH 패치) + 고정 환율 모킹으로 손계산 기대값을 검증한다.
추정 일정 규약(국내 4/15 연 1회, USD 분기 15일, 기타 반기 15일)이 바뀌면
여기 기대값도 의도적으로 함께 바꿔야 한다.
"""

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from starlette.requests import Request

import cache
import repositories.db
from routes import dividend_calendar as dividend_calendar_route
from services import dividend_calendar

FIXED_TODAY = date(2026, 6, 10)
FX_USD = 1400.0


class WindowHelperTests(unittest.TestCase):
    def test_shift_month_across_year_boundaries(self):
        self.assertEqual(dividend_calendar._shift_month(2026, 1, -2), (2025, 11))
        self.assertEqual(dividend_calendar._shift_month(2026, 12, 1), (2027, 1))
        self.assertEqual(dividend_calendar._shift_month(2026, 6, 0), (2026, 6))

    def test_window_months_inclusive_range(self):
        months = dividend_calendar.window_months(FIXED_TODAY, 2, 10)
        self.assertEqual(len(months), 13)
        self.assertEqual(months[0], (2026, 4))
        self.assertEqual(months[-1], (2027, 4))


class EstimatedEventHeuristicTests(unittest.TestCase):
    """순수 함수 — 통화별 추정 케이던스(연/분기/반기)와 금액 분할."""

    MONTHS = dividend_calendar.window_months(FIXED_TODAY, 2, 10)

    def test_korean_stock_annual_april(self):
        item = {"stock_code": "005930", "stock_name": "삼성전자", "quantity": 10}
        events = dividend_calendar.estimated_events_for_holding(item, 1500.0, None, self.MONTHS)
        self.assertEqual([e["date"] for e in events], ["2026-04-15", "2027-04-15"])
        for e in events:
            self.assertEqual(e["type"], "estimated")
            self.assertFalse(e["confirmed"])
            self.assertEqual(e["currency"], "KRW")
            self.assertEqual(e["amount_per_share"], 1500.0)
            self.assertEqual(e["expected_amount_krw"], 15000)
            self.assertEqual(e["label"], "연간 배당 (예상)")

    def test_usd_foreign_quarterly_split(self):
        item = {"stock_code": "AAPL", "stock_name": "Apple", "quantity": 5}
        frow = {"stock_code": "AAPL", "dps_native": 1.0, "currency": "USD", "dps_krw": 1300.0}
        events = dividend_calendar.estimated_events_for_holding(item, 1400.0, frow, self.MONTHS)
        self.assertEqual(
            [e["date"] for e in events],
            ["2026-06-15", "2026-09-15", "2026-12-15", "2027-03-15"],
        )
        for e in events:
            self.assertEqual(e["currency"], "USD")
            self.assertEqual(e["amount_per_share"], 0.25)  # 연간 1.0 의 1/4
            self.assertEqual(e["expected_amount_krw"], 1750)  # 1400/4 × 5주
            self.assertEqual(e["label"], "분기 배당 (예상)")

    def test_non_usd_foreign_semiannual_split(self):
        item = {"stock_code": "0005.HK", "stock_name": "HSBC", "quantity": 100}
        frow = {"stock_code": "0005.HK", "dps_native": 4.0, "currency": "HKD", "dps_krw": 700.0}
        events = dividend_calendar.estimated_events_for_holding(item, 720.0, frow, self.MONTHS)
        self.assertEqual([e["date"] for e in events], ["2026-06-15", "2026-12-15"])
        self.assertEqual(events[0]["amount_per_share"], 2.0)
        self.assertEqual(events[0]["expected_amount_krw"], 36000)  # 720/2 × 100주
        self.assertEqual(events[0]["label"], "반기 배당 (예상)")

    def test_manual_krw_override_falls_back_to_krw_display(self):
        # 관리자 수동 override 행은 dps_native 가 없다 — KRW 로 표시 + 반기.
        item = {"stock_code": "SCHP", "stock_name": "SCHP", "quantity": 2}
        frow = {"stock_code": "SCHP", "dps_native": None, "currency": "KRW", "dps_krw": 800.0}
        events = dividend_calendar.estimated_events_for_holding(item, 800.0, frow, self.MONTHS)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0]["currency"], "KRW")
        self.assertEqual(events[0]["amount_per_share"], 400.0)

    def test_no_events_for_zero_dps_cash_or_empty_quantity(self):
        months = self.MONTHS
        zero = dividend_calendar.estimated_events_for_holding(
            {"stock_code": "000660", "quantity": 10}, 0.0, None, months)
        none = dividend_calendar.estimated_events_for_holding(
            {"stock_code": "000660", "quantity": 10}, None, None, months)
        cash = dividend_calendar.estimated_events_for_holding(
            {"stock_code": "CASH_KRW", "quantity": 100}, 100.0, None, months)
        sold = dividend_calendar.estimated_events_for_holding(
            {"stock_code": "005930", "quantity": 0}, 1500.0, None, months)
        self.assertEqual([zero, none, cash, sold], [[], [], [], []])


class _SeededDbTestCase(unittest.IsolatedAsyncioTestCase):
    """임시 DB 시드: 국내/우선주/해외/무배당/현금 보유 + 배당 테이블."""

    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(repositories.db, "DB_PATH", Path(self.tmp.name) / "cache.db")
        self.db_patch.start()
        await cache.close_db()
        await cache.init_db()
        dividend_calendar_route._calendar_cache.clear()

        db = await cache.get_db()
        await db.execute(
            "INSERT INTO users (google_sub, email, name, picture, email_verified, created_at, last_login_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("u1", "user@example.com", "User", "", 1, "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
        )
        holdings = [
            ("u1", "005930", "삼성전자", 10, 70000.0),
            ("u1", "005935", "삼성전자우", 3, 60000.0),
            ("u1", "AAPL", "Apple", 5, 180.0),
            ("u1", "000660", "SK하이닉스", 7, 200000.0),  # dps=0 → 이벤트 없음
            ("u1", "CASH_KRW", "원화", 1000000, 1.0),      # 현금 → 제외
        ]
        await db.executemany(
            "INSERT INTO user_portfolio (google_sub, stock_code, stock_name, quantity, avg_price, created_at, updated_at) VALUES (?, ?, ?, ?, ?, '2026-01-01', '2026-01-01')",
            holdings,
        )
        # 국내 연간 DPS — 전년도(2025) 행이 trailing 값으로 쓰인다.
        await db.executemany(
            "INSERT INTO market_data (stock_code, year, dividend_per_share) VALUES (?, ?, ?)",
            [("005930", 2025, 1500.0), ("000660", 2025, 0.0)],
        )
        # 우선주 — 수기 시트 값이 보통주 fallback 보다 우선.
        await db.execute(
            "INSERT INTO preferred_dividends (stock_code, dividend_per_share, source_name, common_code, sheet_year, fetched_at) VALUES (?, ?, ?, ?, ?, ?)",
            ("005935", 1600.0, "삼성전자우", "005930", 2025, "2026-06-01T00:00:00"),
        )
        # 해외 — yfinance trailing 연간 DPS (native + 수집 시점 KRW 환산).
        await db.execute(
            "INSERT INTO foreign_dividends (stock_code, dps_native, currency, dps_krw, source, manual_note, fetched_at) VALUES (?, ?, ?, ?, 'yfinance', NULL, ?)",
            ("AAPL", 1.0, "USD", 1300.0, "2026-06-01T00:00:00"),
        )
        await db.commit()

    async def asyncTearDown(self):
        dividend_calendar_route._calendar_cache.clear()
        await cache.close_db()
        self.db_patch.stop()
        self.tmp.cleanup()


class BuildCalendarTests(_SeededDbTestCase):
    async def _build(self, fx_rate=FX_USD, **kwargs):
        with patch(
            "services.portfolio.fx.fx_rate_for_currency",
            AsyncMock(return_value=fx_rate),
        ):
            return await dividend_calendar.build_calendar("u1", today=FIXED_TODAY, **kwargs)

    async def test_events_fx_conversion_and_monthly_aggregation(self):
        out = await self._build()

        self.assertEqual(out["as_of"], "2026-06-10")
        self.assertEqual(out["start_month"], "2026-04")
        self.assertEqual(out["end_month"], "2027-04")

        events = out["events"]
        # 국내 2종목 × 2회(4월) + AAPL 분기 4회 = 8. 무배당/현금은 0.
        self.assertEqual(len(events), 8)
        self.assertTrue(all(ev["type"] == "estimated" and not ev["confirmed"] for ev in events))
        codes = {ev["stock_code"] for ev in events}
        self.assertEqual(codes, {"005930", "005935", "AAPL"})

        by_key = {(ev["stock_code"], ev["date"]): ev for ev in events}
        # 국내: 1500 × 10주, 우선주 시트: 1600 × 3주.
        self.assertEqual(by_key[("005930", "2026-04-15")]["expected_amount_krw"], 15000)
        self.assertEqual(by_key[("005935", "2026-04-15")]["expected_amount_krw"], 4800)
        # AAPL: 실시간 환율(1400) 우선 — 연간 $1.00 → 분기 $0.25 × 1400 × 5주.
        aapl = by_key[("AAPL", "2026-06-15")]
        self.assertEqual(aapl["currency"], "USD")
        self.assertEqual(aapl["amount_per_share"], 0.25)
        self.assertEqual(aapl["expected_amount_krw"], 1750)

        # 월별 합계: 윈도 13개월 전부 (빈 달 포함, 0 으로).
        monthly = {row["month"]: row for row in out["monthly"]}
        self.assertEqual(len(out["monthly"]), 13)
        self.assertEqual(monthly["2026-04"], {"month": "2026-04", "total_krw": 19800, "count": 2})
        self.assertEqual(monthly["2026-06"], {"month": "2026-06", "total_krw": 1750, "count": 1})
        self.assertEqual(monthly["2026-05"], {"month": "2026-05", "total_krw": 0, "count": 0})

        # 요약: 4월 19800×2 + AAPL 1750×4 = 46600.
        self.assertEqual(out["summary"]["total_expected_krw"], 46600)
        self.assertEqual(out["summary"]["estimated_count"], 8)
        self.assertEqual(out["summary"]["confirmed_count"], 0)

    async def test_fx_unknown_falls_back_to_stored_dps_krw(self):
        # fx_rate_for_currency 의 1.0 은 '환율 모름' 센티널 → 수집 시점
        # 환산값(dps_krw=1300)으로 폴백한다.
        out = await self._build(fx_rate=1.0)
        aapl = [ev for ev in out["events"] if ev["stock_code"] == "AAPL"][0]
        self.assertEqual(aapl["expected_amount_krw"], 1625)  # 1300/4 × 5주
        self.assertEqual(aapl["amount_per_share"], 0.25)     # native 표시는 그대로

    async def test_confirmed_brief_events_excluded_from_monthly_totals(self):
        db = await cache.get_db()
        payload = {
            "upcoming_events": [
                # 보유 중 + 윈도 내 → 확정(ex_date) 이벤트로 노출.
                {"stock_code": "005930", "stock_name": "삼성전자", "type": "배당기준일",
                 "date": "2026-06-26", "amount": 361},
                # 미보유 종목 → 제외.
                {"stock_code": "999999", "stock_name": "유령", "type": "배당기준일",
                 "date": "2026-06-26", "amount": 100},
                # 윈도 밖 → 제외.
                {"stock_code": "005930", "type": "배당기준일", "date": "2030-01-01", "amount": 361},
            ]
        }
        await db.execute(
            "INSERT INTO daily_market_briefs (google_sub, brief_date, source_hash, payload_json, markdown, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("u1", "2026-06-10", "h", json.dumps(payload, ensure_ascii=False), "md",
             "2026-06-10T08:00:00", "2026-06-10T08:00:00"),
        )
        await db.commit()

        out = await self._build()
        confirmed = [ev for ev in out["events"] if ev["confirmed"]]
        self.assertEqual(len(confirmed), 1)
        ev = confirmed[0]
        self.assertEqual(ev["type"], "ex_date")
        self.assertEqual(ev["date"], "2026-06-26")
        self.assertEqual(ev["label"], "배당기준일 (확정)")
        self.assertEqual(ev["expected_amount_krw"], 3610)  # 361 × 10주

        # 기준일은 현금 유입이 아님 — 6월 합계는 AAPL 추정분만, count 는 2.
        monthly = {row["month"]: row for row in out["monthly"]}
        self.assertEqual(monthly["2026-06"]["total_krw"], 1750)
        self.assertEqual(monthly["2026-06"]["count"], 2)
        self.assertEqual(out["summary"]["confirmed_count"], 1)

    async def test_empty_portfolio_returns_empty_events_with_full_month_grid(self):
        db = await cache.get_db()
        await db.execute("DELETE FROM user_portfolio")
        await db.commit()
        out = await self._build()
        self.assertEqual(out["events"], [])
        self.assertEqual(len(out["monthly"]), 13)
        self.assertEqual(out["summary"]["total_expected_krw"], 0)


def _request(path: str = "/api/portfolio/dividend-calendar") -> Request:
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


class DividendCalendarRouteTests(_SeededDbTestCase):
    async def _get(self, months=12, user={"google_sub": "u1"}):
        with patch(
            "routes.dividend_calendar.get_current_user", AsyncMock(return_value=user)
        ), patch(
            "services.portfolio.fx.fx_rate_for_currency", AsyncMock(return_value=FX_USD)
        ):
            return await dividend_calendar_route.get_dividend_calendar(_request(), months=months)

    async def test_requires_login(self):
        with patch("routes.dividend_calendar.get_current_user", AsyncMock(return_value=None)):
            with self.assertRaises(HTTPException) as exc:
                await dividend_calendar_route.get_dividend_calendar(_request(), months=12)
        self.assertEqual(exc.exception.status_code, 401)

    async def test_rejects_out_of_range_months(self):
        for bad in (2, 25, "abc"):
            with self.assertRaises(HTTPException) as exc:
                await self._get(months=bad)
            self.assertEqual(exc.exception.status_code, 400)

    async def test_payload_contract(self):
        out = await self._get(months=12)
        for key in ("as_of", "start_month", "end_month", "events", "monthly", "summary"):
            self.assertIn(key, out)
        # months=12 → back 2 고정 + forward 10 → 현재 달 포함 13개 월 행.
        self.assertEqual(len(out["monthly"]), 13)
        self.assertGreater(len(out["events"]), 0)
        first = out["events"][0]
        for key in ("date", "stock_code", "stock_name", "label", "type",
                    "amount_per_share", "currency", "shares",
                    "expected_amount_krw", "confirmed"):
            self.assertIn(key, first)
        # 날짜 오름차순 정렬.
        dates = [ev["date"] for ev in out["events"]]
        self.assertEqual(dates, sorted(dates))

    async def test_result_is_cached_per_user_and_months(self):
        first = await self._get(months=12)
        # 보유 종목을 모두 지워도 TTL 캐시가 같은 결과를 돌려준다.
        db = await cache.get_db()
        await db.execute("DELETE FROM user_portfolio")
        await db.commit()
        second = await self._get(months=12)
        other = await self._get(months=6)  # 다른 키 → DB 재조회

        self.assertEqual(first, second)
        self.assertEqual(other["events"], [])


if __name__ == "__main__":
    unittest.main()
