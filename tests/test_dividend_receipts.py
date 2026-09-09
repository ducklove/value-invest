import asyncio
from datetime import timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import httpx
from _harness import TempDbMixin, seed_user
from fastapi import FastAPI
from pydantic import ValidationError

import snapshot_nav
from core.errors import DBError, register_exception_handlers
from domain.dividend_receipts import DividendCreate, DividendInput, receipt_today
from domain.portfolio_distributions import DistributionCreate, DistributionInput
from domain.portfolio_trades import TradeConflict, TradeError
from repositories import dividend_receipts, investment_insights, portfolio, portfolio_distributions, snapshots
from repositories.db import get_db, transaction
from routes import dividend_receipts as receipt_routes
from routes import portfolio_distributions as distribution_routes
from services.notifications import engine as notification_engine
from services.portfolio import attribution, period_reports, risk, snapshot_views


class DividendReceiptTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await seed_user("u2", "second@example.com")
        await portfolio.save_portfolio_item("u1", "005930", "삼성전자", 10, 100)
        await portfolio.save_portfolio_item("u1", "CASH_KRW", "원화", 10000, 1)
        self.today = receipt_today().isoformat()
        self.yesterday = (receipt_today() - timedelta(days=1)).isoformat()
        await snapshots.save_snapshot("u1", self.yesterday, 11000, 11000, 1100, 10)

    async def prepared(self, **changes):
        payload = {"stock_code": "005930", "stock_name": "삼성전자", "country": "KR", "currency": "KRW",
                   "received_date": self.today, "gross_amount": 1000, **changes}
        preview = await dividend_receipts.preview_dividend("u1", DividendInput(**payload))
        return DividendCreate(**payload, request_id=uuid4(), expected_revision=preview["revision"])

    async def receive(self, **changes):
        return await dividend_receipts.record_dividend("u1", await self.prepared(**changes))

    async def distribution(self, amount=846, **changes):
        payload = DistributionInput(amount=amount, **changes)
        preview = await portfolio_distributions.preview_distribution("u1", payload)
        return DistributionCreate(**payload.model_dump(), request_id=uuid4(), expected_revision=preview["revision"])

    async def settle(self, value, day=None):
        with patch.object(snapshot_nav, "_fetch_total_value", AsyncMock(return_value=(value, 11000, []))):
            await snapshot_nav.take_snapshot("u1", day or self.today)
        return await snapshots.get_latest_snapshot("u1")

    async def test_domestic_receipt_updates_cash_and_income_but_not_units_or_external_flow(self):
        stock = await portfolio.get_portfolio_item("u1", "005930")
        result = await self.receive()
        self.assertEqual((result["gross_amount"], result["tax_amount"], result["net_amount"]), (1000, 154, 846))
        self.assertEqual(result["cash_after"], 10846)
        self.assertEqual(stock, await portfolio.get_portfolio_item("u1", "005930"))
        self.assertEqual(await snapshots.get_cashflows("u1"), [])
        income = await investment_insights.income_events("u1", self.today, self.today)
        self.assertEqual((len(income), income[0]["kind"], income[0]["amount_krw"], income[0]["from_receipt"]), (1, "dividend", 846, 1))
        with self.assertRaises(TradeConflict):
            await investment_insights.delete_income("u1", income[0]["id"])
        snap = await self.settle(11846)
        self.assertEqual((snap["total_units"], snap["nav"], snap["return_nav"]), (10, 1184.6, 1184.6))

    async def test_presets_foreign_fx_and_manual_tax_override(self):
        for country, code, currency, rate, tax, net in [
            ("US", "AAPL", "USD", 15, 15, 85), ("CN", "600519.SS", "CNY", 14.4, 14.4, 85.6),
            ("HK", "0700.HK", "HKD", 15.4, 15.4, 84.6), ("OTHER", "TEST", "EUR", 0, 0, 100),
        ]:
            with self.subTest(country=country):
                result = await self.receive(stock_code=code, currency=currency, country=country, gross_amount=100, fx_rate=1000)
                self.assertEqual((result["tax_rate"], result["tax_amount"], result["net_amount"]), (rate, tax, net))
                self.assertEqual(result["amount_krw"], net * 1000)
                self.assertEqual((await portfolio.get_portfolio_item("u1", f"CASH_{currency}"))["quantity"], net)
        manual = await self.receive(gross_amount=1000, tax_rate=0, tax_amount=123)
        self.assertEqual(manual["net_amount"], 877)
        exempt = await self.receive(gross_amount=1000, tax_rate=0)
        self.assertEqual(exempt["net_amount"], 1000)

    async def test_per_share_uses_decimal_and_currency_rounding(self):
        result = await self.receive(gross_amount=None, amount_per_share="361.25", quantity="0.5")
        self.assertEqual((result["gross_amount"], result["tax_amount"], result["net_amount"]), (181, 27, 154))
        result = await self.receive(stock_code="AAPL", country="US", currency="USD", gross_amount=None,
                                    quantity="1.5", amount_per_share="0.25", fx_rate=1400)
        self.assertEqual((result["gross_amount"], result["tax_amount"], result["net_amount"]), (0.38, 0.05, 0.33))

    async def test_invalid_receipts_and_future_dates_do_not_change_cash(self):
        before = await portfolio.get_portfolio("u1")
        invalid = [{"stock_code": "CASH_KRW"}, {"gross_amount": "0.1"}, {"tax_amount": 1001},
                   {"gross_amount": None}, {"currency": "USD"}, {"quantity": 2},
                   {"source_key": "AAPL:estimated:2026-01-15"}, {"fx_rate": True}, {"tax_rate": 101},
                   {"tax_amount": "Infinity"}, {"received_date": (receipt_today() + timedelta(days=1)).isoformat()}]
        for changes in invalid:
            with self.subTest(changes=changes), self.assertRaises((TradeError, ValidationError)):
                await self.prepared(**changes)
        self.assertEqual(before, await portfolio.get_portfolio("u1"))

    async def test_receipt_source_and_request_deduplication(self):
        source = f"005930:estimated:{self.today}"
        request = await self.prepared(source_key=source)
        results = await asyncio.gather(*[dividend_receipts.record_dividend("u1", request) for _ in range(3)])
        self.assertEqual(sum(not result["replayed"] for result in results), 1)
        self.assertEqual((await portfolio.get_portfolio_item("u1", "CASH_KRW"))["quantity"], 10846)
        with self.assertRaises(TradeConflict):
            await self.prepared(source_key=source)
        with self.assertRaises(TradeConflict):
            await dividend_receipts.record_dividend("u1", request.model_copy(update={"memo": "다른 내용"}))

    async def test_receipt_stale_preview_and_concurrent_records(self):
        first, second = await self.prepared(), await self.prepared()
        results = await asyncio.gather(dividend_receipts.record_dividend("u1", first), dividend_receipts.record_dividend("u1", second), return_exceptions=True)
        self.assertEqual(sum(isinstance(result, TradeConflict) for result in results), 1)
        self.assertEqual(len(await dividend_receipts.list_receipts("u1")), 1)

    async def test_receipt_ledger_failure_rolls_back_cash_and_classification(self):
        request = await self.prepared()
        async with transaction() as db:
            await db.execute("CREATE TRIGGER fail_receipt BEFORE INSERT ON portfolio_dividend_receipts BEGIN SELECT RAISE(ABORT, 'receipt failed'); END")
        with self.assertRaises(DBError):
            await dividend_receipts.record_dividend("u1", request)
        self.assertEqual((await portfolio.get_portfolio_item("u1", "CASH_KRW"))["quantity"], 10000)
        self.assertEqual(await investment_insights.income_events("u1", self.today, self.today), [])

    async def test_past_receipt_classification_follows_cash_application_date(self):
        result = await self.receive(received_date=self.yesterday)
        self.assertEqual((result["received_date"], result["applied_date"]), (self.yesterday, self.today))
        self.assertEqual(await investment_insights.income_events("u1", self.yesterday, self.yesterday), [])

    async def test_receipt_after_settlement_is_classified_on_next_snapshot_once(self):
        await self.settle(11000)
        await self.receive()
        pending = await attribution.build_attribution("u1", self.yesterday, self.today)
        self.assertEqual(next(row["amount"] for row in pending["components"] if row["key"] == "dividend"), 0)
        tomorrow = (receipt_today() + timedelta(days=1)).isoformat()
        await self.settle(11846, tomorrow)
        self.assertEqual(await investment_insights.income_events("u1", self.today, self.today), [])
        row = (await investment_insights.income_events("u1", tomorrow, tomorrow))[0]
        self.assertEqual((row["amount_krw"], row["receipt_settled_date"]), (846, tomorrow))

    async def test_distribution_retains_units_and_total_return_after_full_dividend_payout(self):
        await self.receive()
        request = await self.distribution()
        result = await portfolio_distributions.record_distribution("u1", request)
        self.assertEqual((result["cash_after"], result["available_after"], result["units_change"]), (10000, 0, 0))
        self.assertEqual(await notification_engine._net_cashflow_since_settlement("u1", self.yesterday), -846)
        snap = await self.settle(11000)
        self.assertEqual((snap["nav"], snap["total_units"], snap["distribution_per_unit"]), (1100, 10, 84.6))
        self.assertAlmostEqual(snap["return_nav"], 1184.6)
        rerun = await self.settle(11000)
        self.assertEqual(snap, rerun)
        next_day = (receipt_today() + timedelta(days=1)).isoformat()
        later = await self.settle(12100, next_day)
        self.assertAlmostEqual(later["return_nav"], 1184.6 * 1.1)
        self.assertEqual(later["distribution_per_unit"], 0)
        history = await snapshots.get_nav_history("u1")
        self.assertAlmostEqual(risk.clean_series(history)[1][1], 1184.6)
        self.assertAlmostEqual(period_reports._nav_points(history, receipt_today())[1]["nav"], 1184.6)

    async def test_distribution_same_day_rerun_and_external_deposit_do_not_issue_distribution_units(self):
        await self.receive()
        await portfolio_distributions.record_distribution("u1", await self.distribution(423))
        first = await self.settle(11423)
        self.assertAlmostEqual(first["return_nav"], 1184.6)
        await portfolio_distributions.record_distribution("u1", await self.distribution(423))
        await snapshots.add_cashflow_and_sync_cash("u1", self.today, "deposit", 1100, None, None, None)
        final = await self.settle(12100)
        self.assertEqual(final["total_units"], 11)
        self.assertEqual(final["nav"], 1100)
        self.assertAlmostEqual(final["return_nav"], 1184.6)
        self.assertEqual([row["type"] for row in await snapshots.get_cashflows("u1")].count("distribution"), 2)

    async def test_distribution_is_separate_from_external_flows_in_attribution(self):
        await self.receive()
        await portfolio_distributions.record_distribution("u1", await self.distribution())
        await self.settle(11000)
        report = await attribution.build_attribution("u1", self.yesterday, self.today)
        parts = {row["key"]: row["amount"] for row in report["components"]}
        self.assertEqual((parts["external_flow"], parts["distribution"], parts["dividend"]), (0, -846, 846))
        self.assertEqual((report["investment_pnl"], report["reconciliation_error"]), (846, 0))
        cash = period_reports._cashflow_summary(await snapshots.get_cashflows("u1"), self.today, self.today)
        self.assertEqual((cash["net_cashflow"], cash["total_distribution"], cash["distribution_count"]), (0, 846, 1))

    async def test_distribution_rejects_excess_balance_and_preserves_foreign_cash(self):
        await self.receive()
        with self.assertRaisesRegex(TradeError, "누적액"):
            await self.distribution(847)
        await portfolio.save_portfolio_item("u1", "CASH_KRW", "원화", 800, 1)
        with self.assertRaisesRegex(TradeError, "부족"):
            await self.distribution(846)
        await self.receive(stock_code="AAPL", country="US", currency="USD", gross_amount=100, fx_rate=1400)
        result = await portfolio_distributions.record_distribution("u1", await self.distribution(85, currency="USD", fx_rate=1400))
        self.assertEqual((result["cash_after"], result["amount_krw"]), (0, 119000))
        self.assertEqual((await portfolio.get_portfolio_item("u1", "CASH_KRW"))["quantity"], 800)
        views = await snapshot_views.previous_day("u1", self.yesterday)
        self.assertEqual(views["today_cashflows_by_stock"], {"CASH_USD": -119000})
        self.assertEqual(views["today_cashflows"][0]["units_change"], 0)

    async def test_distribution_duplicate_and_concurrent_requests_cannot_double_pay(self):
        await self.receive()
        first, second = await self.distribution(), await self.distribution()
        results = await asyncio.gather(portfolio_distributions.record_distribution("u1", first), portfolio_distributions.record_distribution("u1", second), return_exceptions=True)
        self.assertEqual(sum(isinstance(result, TradeConflict) for result in results), 1)
        self.assertTrue((await portfolio_distributions.record_distribution("u1", first))["replayed"])
        with self.assertRaises(TradeConflict):
            await portfolio_distributions.record_distribution("u1", first.model_copy(update={"amount": Decimal(1)}))

    async def test_distribution_failure_rolls_back_cash_and_remaining_dividend(self):
        await self.receive()
        request = await self.distribution()
        async with transaction() as db:
            await db.execute("CREATE TRIGGER fail_distribution BEFORE INSERT ON portfolio_distributions BEGIN SELECT RAISE(ABORT, 'distribution failed'); END")
        with self.assertRaises(DBError):
            await portfolio_distributions.record_distribution("u1", request)
        self.assertEqual((await portfolio.get_portfolio_item("u1", "CASH_KRW"))["quantity"], 10846)
        self.assertEqual((await portfolio_distributions.balances("u1"))[0]["available_amount"], 846)

    async def test_snapshot_failure_rolls_back_distribution_settlement_marker(self):
        await self.receive()
        await portfolio_distributions.record_distribution("u1", await self.distribution())
        async with transaction() as db:
            await db.execute("CREATE TRIGGER fail_snapshot BEFORE INSERT ON portfolio_snapshots BEGIN SELECT RAISE(ABORT, 'snapshot failed'); END")
        with self.assertRaises(DBError):
            await self.settle(11000)
        self.assertIsNone((await snapshots.get_distribution_flows("u1"))[0]["applied_snapshot_date"])
        self.assertEqual((await snapshots.get_latest_snapshot("u1"))["date"], self.yesterday)

    async def test_routes_auth_isolation_and_schedule_candidates(self):
        app = FastAPI()
        app.include_router(receipt_routes.router)
        app.include_router(distribution_routes.router)
        register_exception_handlers(app)
        source = f"005930:estimated:{self.today}"
        await self.receive(source_key=source)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            with patch.object(receipt_routes, "get_current_user", AsyncMock(return_value=None)):
                for path in ['/api/portfolio/dividend-receipts', '/api/portfolio/distributions', '/api/portfolio/distributions/balances']:
                    self.assertEqual((await client.get(path)).status_code, 401)
            with patch.object(receipt_routes, "get_current_user", AsyncMock(return_value={"google_sub": "u2"})):
                self.assertEqual((await client.get('/api/portfolio/distributions/balances')).json(), [])
                self.assertEqual((await client.get('/api/portfolio/dividend-receipts')).json(), [])
            with patch.object(receipt_routes, "get_current_user", AsyncMock(return_value={"google_sub": "u1"})), patch.object(
                receipt_routes.dividend_calendar, "build_calendar", AsyncMock(return_value={"as_of": self.today, "events": [{"stock_code": "005930", "type": "estimated", "date": self.today}]}),
            ):
                result = (await client.get('/api/portfolio/dividend-receipts/candidates')).json()
                self.assertEqual((result["events"][0]["source_key"], result["events"][0]["received"]), (source, True))
                request = await self.distribution()
                response = await client.post('/api/portfolio/distributions', json=request.model_dump(mode="json"))
                self.assertEqual(response.status_code, 200, response.text)
        db = await get_db()
        self.assertEqual((await (await db.execute("SELECT COUNT(*) FROM portfolio_cashflows")).fetchone())[0], 0)
