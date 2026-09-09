"""공시 지급일·권리일·예상 일정의 의미와 합계, 수취 연결을 검증한다."""

import copy
import json
import unittest
from datetime import date
from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin, seed_user
from fastapi import HTTPException
from starlette.requests import Request

from domain.dividend_schedule import calendar_event, frequency_of, project_events
from repositories import portfolio
from repositories.db import transaction
from routes import dividend_calendar as route
from routes import dividend_receipts as receipts
from services import dividend_calendar as cal
from services.portfolio.fx import FXUnavailableError

TODAY = date(2026, 9, 10)


def payment(day, amount=0.12, **extra):
    return {"pay_date": day, "ex_date": None, "record_date": None, "currency": "USD", "amount_per_share": amount, **extra}


def monthly_history():
    return [payment(f"{y}-{m:02d}-10") for y in (2025, 2026) for m in range(1, 13) if (y, m) <= (2026, 9)]


def feed(events, **extra):
    return {"events": events, "status": "fresh", "official": True, "fetched_at": "2026-09-10T00:00:00+00:00", **extra}


def request():
    return Request({"type": "http", "method": "GET", "path": "/api/portfolio/dividend-calendar", "headers": [], "query_string": b""})


class PatternTests(unittest.TestCase):
    def test_windows_cross_years(self):
        self.assertEqual(cal._shift_month(2026, 1, -2), (2025, 11))
        self.assertEqual(cal.window_months(TODAY, 2, 10)[-1], (2027, 7))

    def test_frequency_comes_from_history_not_currency(self):
        for currency in ("USD", "HKD", "KRW"):
            self.assertEqual(frequency_of([{**e, "currency": currency} for e in monthly_history()], TODAY), "monthly")
        quarterly = [payment(f"{y}-{m:02d}-20") for y in (2025, 2026) for m in (2, 5, 8)]
        self.assertEqual(frequency_of(quarterly, TODAY), "quarterly")
        self.assertEqual(frequency_of([payment("2026-09-01")], TODAY), "irregular")
        self.assertEqual(frequency_of([payment("2025-04-20"), payment("2026-04-17")], TODAY), "annual")

    def test_monthly_has_twelve_future_payments_without_past_fabrication(self):
        result = project_events(monthly_history(), TODAY, date(2027, 10, 1), "monthly")
        self.assertEqual(len(result), 12)
        self.assertEqual(len({e["pay_date"][:7] for e in result}), 12)
        self.assertTrue(all(e["pay_date"] > TODAY.isoformat() and e["estimated"] for e in result))

    def test_domestic_quarterly_uses_record_period_despite_delayed_annual_payment(self):
        history = [payment(p, record_date=r) for r, p in (("2024-12-31", "2025-04-18"), ("2025-03-31", "2025-05-20"),
                  ("2025-06-30", "2025-08-20"), ("2025-09-30", "2025-11-19"), ("2025-12-31", "2026-04-17"),
                  ("2026-03-31", "2026-05-20"), ("2026-06-30", "2026-08-20"))]
        self.assertEqual(frequency_of(history, TODAY), "quarterly")

    def test_official_payment_replaces_nearby_projection(self):
        history = monthly_history() + [payment("2026-10-09", ex_date="2026-09-30")]
        result = project_events(history, TODAY, date(2026, 12, 1), "monthly")
        self.assertEqual([e["pay_date"] for e in result], ["2026-11-10"])

    def test_schp_omitted_months_and_two_december_payments_are_preserved(self):
        history = [payment(f"{y}-{m:02d}-07", 0.09) for y in (2025, 2026) for m in range(3, 13) if (y, m) <= (2026, 9)]
        history.append(payment("2025-12-26", 0.17))
        result = project_events(history, TODAY, date(2027, 4, 1), "monthly")
        self.assertEqual(sum(e["pay_date"].startswith("2026-12") for e in result), 2)
        self.assertFalse(any(e["pay_date"].startswith(("2027-01", "2027-02")) for e in result))

    def test_moving_annual_boundary_does_not_duplicate_quarter(self):
        history = [payment(d) for d in ("2024-08-10", "2024-11-10", "2025-02-10", "2025-05-10", "2025-08-11", "2025-11-10", "2026-02-10", "2026-05-10", "2026-08-10")]
        result = project_events(history, TODAY, date(2027, 9, 10), "quarterly")
        self.assertEqual(len(result), 4)

    def test_insufficient_irregular_or_suspended_history_is_not_projected(self):
        for history, freq in ((monthly_history()[:4], "monthly"), (monthly_history(), "irregular"), (monthly_history()[-2:], "monthly")):
            self.assertEqual(project_events(history, TODAY, date(2027, 9, 1), freq), [])

    def test_receipt_identity_survives_pay_date_enrichment(self):
        holding = {"stock_code": "AGNC", "quantity": 10}
        raw = payment(None, ex_date="2026-08-31")
        before = calendar_event(holding, raw, 1400, "monthly", feed([raw], official=False))
        after = calendar_event(holding, {**raw, "pay_date": "2026-09-10"}, 1400, "monthly", feed([raw]))
        self.assertEqual(before["source_key"], after["source_key"])
        self.assertFalse(before["cashflow"])
        self.assertTrue(after["cashflow"])


class CalendarTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        for code, qty in (("AGNC", 10), ("SCHP", 20), ("005930", 3), ("CASH_USD", 100)):
            await portfolio.save_portfolio_item("u1", code, code, qty, 1)
        self.data = {
            "AGNC": feed(monthly_history() + [payment("2026-10-09", ex_date="2026-09-30")]),
            "SCHP": feed([payment("2026-09-08", 0.0773, ex_date="2026-09-01", record_date="2026-09-01")], frequency_hint="monthly"),
            "005930": feed([payment(None, 374, ex_date="2026-06-29", currency="KRW")], official=False),
        }
        self.history_patch = patch("services.dividend_sources.get_histories", AsyncMock(side_effect=lambda codes: copy.deepcopy({k: v for k, v in self.data.items() if k in codes})))
        self.history_patch.start()
        self.addCleanup(self.history_patch.stop)
        self.fx_patch = patch("services.portfolio.fx.fx_rate_for_currency", AsyncMock(side_effect=lambda c: 1 if c == "KRW" else 1400))
        self.fx_patch.start()
        self.addCleanup(self.fx_patch.stop)
        self.today_patch = patch.object(cal, "today_kst_date", return_value=TODAY)
        self.today_patch.start()
        self.addCleanup(self.today_patch.stop)

    async def build(self, **kw):
        return await cal.build_calendar("u1", today=TODAY, **kw)

    async def test_official_dates_and_cash_totals(self):
        result = await self.build(months_back=4, months_forward=3)
        sept = next(m for m in result["monthly"] if m["month"] == "2026-09")
        self.assertEqual(sept["total_krw"], 1680 + 2164)
        self.assertEqual(sept["announced_krw"], sept["total_krw"])
        june = next(m for m in result["monthly"] if m["month"] == "2026-06")
        self.assertEqual(june["total_krw"], 1680)  # 삼성 배당락일 금액 제외
        schp = next(e for e in result["events"] if e["stock_code"] == "SCHP")
        self.assertEqual((schp["date"], schp["ex_date"], schp["frequency"]), ("2026-09-08", "2026-09-01", "monthly"))
        self.assertEqual(result["summary"]["unknown_payment_count"], 1)
        self.assertFalse(any(e["stock_code"] == "CASH_USD" for e in result["events"]))

    async def test_missing_fx_keeps_native_and_reports_unconverted_amount(self):
        with patch("services.portfolio.fx.fx_rate_for_currency", AsyncMock(side_effect=FXUnavailableError("USD"))):
            result = await self.build()
        sept = next(m for m in result["monthly"] if m["month"] == "2026-09")
        self.assertEqual((sept["total_krw"], sept["unconverted_count"]), (0, 2))
        self.assertEqual(next(e for e in result["events"] if e["stock_code"] == "SCHP")["amount_per_share"], 0.0773)

    async def test_stored_fx_fallback_is_marked(self):
        async with transaction() as db:
            await db.execute("INSERT INTO foreign_dividends(stock_code,dps_native,currency,dps_krw,source,fetched_at) VALUES ('SCHP',1,'USD',1300,'yfinance','2026-09-01')")
        with patch("services.portfolio.fx.fx_rate_for_currency", AsyncMock(side_effect=FXUnavailableError("USD"))):
            result = await self.build()
        event = next(e for e in result["events"] if e["stock_code"] == "SCHP")
        self.assertEqual((event["fx_source"], event["expected_amount_krw"]), ("stored", 2010))

    async def test_stale_history_is_visible_but_not_projected(self):
        self.data["AGNC"]["status"] = "stale"
        result = await self.build()
        self.assertFalse(any(e["type"] == "estimated" and e["stock_code"] == "AGNC" for e in result["events"]))
        self.assertEqual(result["summary"]["stale_count"], 1)

    async def test_brief_record_date_and_exclusive_end(self):
        self.data["005930"]["official"] = True
        async with transaction() as db:
            await db.execute("INSERT INTO daily_market_briefs(google_sub,brief_date,source_hash,payload_json,markdown,created_at,updated_at) VALUES ('u1','2026-09-10','h',?,'','','')",
                             (json.dumps({"upcoming_events": [{"stock_code": "005930", "date": d, "type": "배당기준일", "amount": 374} for d in ("2026-09-30", "2026-10-01")]}),))
        result = await self.build(months_back=0, months_forward=0)
        records = [e for e in result["events"] if e["type"] == "record_date"]
        self.assertEqual([e["date"] for e in records], ["2026-09-30"])
        self.assertFalse(records[0]["cashflow"])
        self.assertFalse(records[0]["confirmed"])

    async def test_missing_history_keeps_coverage_without_fabricated_dates(self):
        self.data = {}
        result = await self.build()
        self.assertEqual(result["events"], [])
        self.assertEqual(len(result["coverage"]), 3)

    async def test_route_auth_validation_and_current_quantity(self):
        with patch.object(route, "get_current_user", AsyncMock(return_value=None)):
            with self.assertRaises(HTTPException) as exc:
                await route.get_dividend_calendar(request(), months=12)
            self.assertEqual(exc.exception.status_code, 401)
        with patch.object(route, "get_current_user", AsyncMock(return_value={"google_sub": "u1"})):
            for bad in (2, 25, "abc"):
                with self.assertRaises(HTTPException):
                    await route.get_dividend_calendar(request(), months=bad)
            before = await route.get_dividend_calendar(request(), months=12)
            await portfolio.save_portfolio_item("u1", "AGNC", "AGNC", 20, 1)
            after = await route.get_dividend_calendar(request(), months=12)
        first = next(e for e in before["events"] if e["stock_code"] == "AGNC")
        changed = next(e for e in after["events"] if e["stock_code"] == "AGNC" and e["date"] == first["date"])
        self.assertEqual(changed["expected_amount_krw"], first["expected_amount_krw"] * 2)

    async def test_receipt_candidates_exclude_predictions_and_detect_legacy(self):
        with patch.object(receipts, "get_current_user", AsyncMock(return_value={"google_sub": "u1"})), patch.object(receipts.repo, "received_source_keys", AsyncMock(return_value={"SCHP:estimated:2026-09-15"})):
            result = await receipts.candidates(request())
        self.assertFalse(any(e["type"] == "estimated" for e in result["events"]))
        schp = next(e for e in result["events"] if e["stock_code"] == "SCHP")
        self.assertTrue(schp["received"])
        self.assertTrue(schp["legacy_receipt_match"])
        self.assertEqual(schp["source_key"], "SCHP:ex_date:2026-09-01")
