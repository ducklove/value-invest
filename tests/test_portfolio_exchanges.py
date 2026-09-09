import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import httpx
from _harness import TempDbMixin, seed_user
from fastapi import FastAPI
from pydantic import ValidationError

from core.errors import DBError, register_exception_handlers
from domain.portfolio_exchanges import ExchangeCreate, ExchangeInput
from domain.portfolio_trades import TradeConflict, TradeCreate, TradeError, TradeInput
from repositories import portfolio, snapshots
from repositories import portfolio_exchanges as exchanges
from repositories import portfolio_trades as trades
from repositories.db import transaction
from routes import portfolio_trades as routes


class PortfolioExchangeTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await seed_user("u2", "second@example.com")
        await portfolio.save_portfolio_item("u1", "CASH_KRW", "원화", 2000000, 1, memo="원화 설정")
        await portfolio.save_portfolio_item("u1", "CASH_USD", "달러", 1000, 1, "USD", avg_price_currency="USD", memo="달러 설정")
        await portfolio.save_portfolio_item("u1", "005930", "삼성전자", 10, 100)

    async def prepared(self, **changes):
        payload = ExchangeInput(side="exchange", **{"from_currency": "KRW", "to_currency": "USD", "amount": 140000,
                                "rate": 1400, "rate_basis": "from_per_to", "fees": 1000, **changes})
        preview = await exchanges.preview_exchange("u1", payload)
        return ExchangeCreate(**payload.model_dump(), request_id=uuid4(), expected_revision=preview["revision"])

    async def test_krw_to_usd_changes_both_cash_balances_and_preserves_metadata_units(self):
        await snapshots.save_snapshot("u1", "2026-09-09", 3401000, 3401000, 3401, 1000)
        baseline = await snapshots.get_latest_snapshot("u1")
        before = await portfolio.get_portfolio("u1")
        request = await self.prepared()
        self.assertEqual(await portfolio.get_portfolio("u1"), before)
        result = await exchanges.record_exchange("u1", request)
        self.assertEqual((result["source_debit"], result["received_amount"], result["from_after"], result["to_after"]), (141000, 100, 1859000, 1100))
        rows = {row["stock_code"]: row for row in await portfolio.get_portfolio("u1")}
        self.assertEqual((rows["CASH_KRW"]["quantity"], rows["CASH_USD"]["quantity"]), (1859000, 1100))
        self.assertEqual((rows["CASH_KRW"]["memo"], rows["CASH_USD"]["memo"]), ("원화 설정", "달러 설정"))
        self.assertEqual(rows["005930"]["quantity"], 10)
        self.assertEqual(await snapshots.get_latest_snapshot("u1"), baseline)
        self.assertEqual(await snapshots.get_cashflows("u1"), [])
        self.assertEqual((await trades.list_trades("u1"))[0]["side"], "exchange")

    async def test_usd_to_krw_and_cross_currency_rounding_create_target_cash(self):
        request = await self.prepared(from_currency="USD", to_currency="KRW", amount=100, fees="0.5", rate_basis="to_per_from")
        result = await exchanges.record_exchange("u1", request)
        self.assertEqual((result["from_after"], result["to_after"]), (899.5, 2140000))
        request = await self.prepared(from_currency="USD", to_currency="EUR", amount=100, rate="0.92345", rate_basis="to_per_from", fees=0)
        result = await exchanges.record_exchange("u1", request)
        self.assertEqual(result["received_amount"], 92.35)
        target = await portfolio.get_portfolio_item("u1", "CASH_EUR")
        self.assertEqual((target["quantity"], target["currency"], target["avg_price_currency"]), (92.35, "EUR", "EUR"))
        request = await self.prepared(from_currency="USD", to_currency="JPY", amount=10, rate="145.55", rate_basis="to_per_from", fees=0)
        result = await exchanges.record_exchange("u1", request)
        self.assertEqual(result["received_amount"], 1456)

    async def test_received_override_and_full_exchange_retain_zero_cash(self):
        request = await self.prepared(from_currency="USD", to_currency="EUR", amount=1000, rate="0.92", rate_basis="to_per_from", fees=0, received_amount="919.99")
        result = await exchanges.record_exchange("u1", request)
        self.assertEqual((result["received_amount"], result["received_override"], result["from_after"]), (919.99, True, 0))
        self.assertIsNotNone(await portfolio.get_portfolio_item("u1", "CASH_USD"))

    async def test_invalid_amount_rates_currencies_and_insufficient_cash_never_write(self):
        before = await portfolio.get_portfolio("u1")
        invalid = [{"to_currency": "KRW"}, {"amount": 2000000}, {"amount": "1.1"}, {"fees": "0.01"},
                   {"received_amount": "0.001"}, {"received_amount": 0}, {"amount": 0}, {"rate": 0},
                   {"rate": "NaN"}, {"fees": -1}, {"rate": True}, {"to_currency": "XYZ"},
                   {"from_currency": "EUR", "to_currency": "USD"}, {"rate": "0.00000001", "amount": 1000000000},
                   {"rate": 1000000000000, "amount": 1, "fees": 0}]
        for changes in invalid:
            with self.subTest(changes=changes), self.assertRaises((TradeError, ValidationError)):
                await self.prepared(**changes)
        self.assertEqual(await portfolio.get_portfolio("u1"), before)
        self.assertEqual(await trades.list_trades("u1"), [])

    async def test_either_currency_changed_after_preview_blocks_write(self):
        for code, currency in [("CASH_KRW", "KRW"), ("CASH_USD", "USD")]:
            request = await self.prepared()
            item = await portfolio.get_portfolio_item("u1", code)
            await portfolio.save_portfolio_item("u1", code, code, item["quantity"] + 100, 1, currency)
            with self.assertRaises(TradeConflict):
                await exchanges.record_exchange("u1", request)
        self.assertEqual(await trades.list_trades("u1"), [])

    async def test_concurrent_retries_write_once_and_changed_payload_conflicts(self):
        request = await self.prepared()
        results = await asyncio.gather(*[exchanges.record_exchange("u1", request) for _ in range(3)])
        self.assertEqual(sum(not row["replayed"] for row in results), 1)
        self.assertEqual((await portfolio.get_portfolio_item("u1", "CASH_USD"))["quantity"], 1100)
        with self.assertRaises(TradeConflict):
            await exchanges.record_exchange("u1", request.model_copy(update={"amount": Decimal(10000)}))

    async def test_concurrent_distinct_exchanges_cannot_double_spend(self):
        first, second = await self.prepared(amount=1500000), await self.prepared(amount=1500000)
        results = await asyncio.gather(exchanges.record_exchange("u1", first), exchanges.record_exchange("u1", second), return_exceptions=True)
        self.assertEqual(sum(isinstance(result, TradeConflict) for result in results), 1)
        self.assertEqual(len(await trades.list_trades("u1")), 1)

    async def test_target_update_failure_rolls_back_source_debit(self):
        request = await self.prepared()
        before = await portfolio.get_portfolio("u1")
        async with transaction() as db:
            await db.execute("CREATE TRIGGER fail_exchange BEFORE UPDATE ON user_portfolio WHEN NEW.stock_code='CASH_USD' BEGIN SELECT RAISE(ABORT,'target failed'); END")
        with self.assertRaises(DBError):
            await exchanges.record_exchange("u1", request)
        self.assertEqual(await portfolio.get_portfolio("u1"), before)
        self.assertEqual(await trades.list_trades("u1"), [])

    async def test_ledger_failure_rolls_back_new_target_cash(self):
        request = await self.prepared(to_currency="EUR")
        before = await portfolio.get_portfolio("u1")
        async with transaction() as db:
            await db.execute("CREATE TRIGGER fail_exchange BEFORE INSERT ON portfolio_trades BEGIN SELECT RAISE(ABORT,'ledger failed'); END")
        with self.assertRaises(DBError):
            await exchanges.record_exchange("u1", request)
        self.assertEqual(await portfolio.get_portfolio("u1"), before)

    async def test_other_task_cannot_observe_only_debited_cash(self):
        request = await self.prepared(to_currency="EUR")
        original = portfolio.save_portfolio_item
        observed = []
        async def paused(*args, **kwargs):
            result = await original(*args, **kwargs)
            observed.extend(await asyncio.create_task(portfolio.get_portfolio("u1")))
            return result
        with patch.object(portfolio, "save_portfolio_item", paused):
            await exchanges.record_exchange("u1", request)
        rows = {row["stock_code"]: row for row in observed}
        self.assertEqual(rows["CASH_KRW"]["quantity"], 2000000)
        self.assertNotIn("CASH_EUR", rows)

    async def test_shared_trade_api_auth_validation_and_mixed_history(self):
        app = FastAPI()
        app.include_router(routes.router)
        register_exception_handlers(app)
        request = await self.prepared()
        trade = TradeInput(stock_code="005930", stock_name="삼성전자", side="buy", quantity=1, price=100)
        preview = await trades.preview_trade("u1", trade)
        await trades.record_trade("u1", TradeCreate(**trade.model_dump(), request_id=uuid4(), expected_revision=preview["revision"]))
        request = await self.prepared()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            with patch.object(routes, "get_current_user", AsyncMock(return_value=None)):
                self.assertEqual((await client.post('/api/portfolio/trades', json=request.model_dump(mode="json"))).status_code, 401)
            with patch.object(routes, "get_current_user", AsyncMock(return_value={"google_sub": "u1"})):
                self.assertEqual((await client.post('/api/portfolio/trades/preview', json=request.model_dump(mode="json", exclude={"request_id", "expected_revision"}))).status_code, 200)
                self.assertEqual((await client.post('/api/portfolio/trades', json=request.model_dump(mode="json"))).status_code, 200)
                history = (await client.get('/api/portfolio/trades')).json()
                self.assertEqual([row["side"] for row in history], ["exchange", "buy"])
                self.assertEqual((await client.post('/api/portfolio/trades', json={**request.model_dump(mode="json"), "rate": True})).status_code, 422)
            with patch.object(routes, "get_current_user", AsyncMock(return_value={"google_sub": "u2"})):
                self.assertEqual((await client.get('/api/portfolio/trades')).json(), [])
                self.assertEqual((await client.post('/api/portfolio/trades', json=request.model_dump(mode="json"))).status_code, 409)
