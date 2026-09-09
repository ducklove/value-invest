import asyncio
import json
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import httpx
from _harness import TempDbMixin, seed_user
from fastapi import FastAPI

from core.errors import DBError, register_exception_handlers
from domain.portfolio_trades import TradeConflict, TradeCreate, TradeError, TradeInput
from repositories import portfolio, portfolio_trades, snapshots
from repositories.db import get_db, transaction
from routes import portfolio_trades as routes


class PortfolioTradeTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await seed_user("u2", "second@example.com")
        await portfolio.save_portfolio_item("u1", "005930", "삼성전자", 10, 100, target_price=180, memo="보존")
        await portfolio.save_portfolio_item("u1", "CASH_KRW", "원화", 2000, 1)

    async def prepared(self, user="u1", **changes):
        payload = {"stock_code": "005930", "stock_name": "삼성전자", "side": "buy",
                   "quantity": 2, "price": 150, "fees": 10, "currency": "KRW", **changes}
        preview = await portfolio_trades.preview_trade(user, TradeInput(**payload))
        return TradeCreate(**payload, expected_revision=preview["revision"], request_id=uuid4())

    async def test_buy_updates_weighted_cost_and_cash_without_external_flow(self):
        before = await portfolio.get_portfolio("u1")
        request = await self.prepared()
        self.assertEqual(before, await portfolio.get_portfolio("u1"))
        result = await portfolio_trades.record_trade("u1", request)
        stock = await portfolio.get_portfolio_item("u1", "005930")
        cash = await portfolio.get_portfolio_item("u1", "CASH_KRW")
        self.assertEqual(stock["quantity"], 12)
        self.assertAlmostEqual(stock["avg_price"], 1310 / 12)
        self.assertEqual(cash["quantity"], 1690)
        self.assertEqual(result["cash_change"], -310)
        full = next(i for i in await portfolio.get_portfolio("u1") if i["stock_code"] == "005930")
        self.assertEqual((full["target_price"], full["memo"]), (180, "보존"))
        self.assertEqual(await snapshots.get_cashflows("u1"), [])
        self.assertEqual(len(await portfolio_trades.list_trades("u1")), 1)

    async def test_partial_and_full_sale(self):
        request = await self.prepared(side="sell", quantity=4, price=200, fees=5)
        result = await portfolio_trades.record_trade("u1", request)
        self.assertEqual((result["quantity_after"], result["cash_after"], result["avg_price_after"]), (6, 2795, 100))
        await portfolio.set_portfolio_tags("u1", "005930", ["보유"])
        await portfolio_trades.record_trade("u1", await self.prepared(side="sell", quantity=6, price=200, fees=0))
        self.assertIsNone(await portfolio.get_portfolio_item("u1", "005930"))
        self.assertEqual(await portfolio.get_portfolio_tags("u1", "005930"), [])
        self.assertEqual((await portfolio.get_portfolio_item("u1", "CASH_KRW"))["quantity"], 3995)
        self.assertEqual(len(await portfolio_trades.list_trades("u1")), 2)

    async def test_new_stock_and_fractional_foreign_trade(self):
        await portfolio.save_portfolio_item("u1", "CASH_USD", "달러", 100, 1, "USD")
        request = await self.prepared(stock_code="AAPL", stock_name="Apple", currency="USD", quantity="0.25", price="123.45", fees="0.10")
        result = await portfolio_trades.record_trade("u1", request)
        self.assertEqual(result["gross_amount"], 30.86)
        self.assertEqual(result["cash_after"], 69.04)
        self.assertEqual(result["avg_price_currency"], "USD")
        self.assertEqual(result["avg_price_after"], 123.84)
        self.assertEqual((await portfolio.get_portfolio_item("u1", "CASH_KRW"))["quantity"], 2000)

    async def test_sale_tax_is_added_to_commission_and_can_be_overridden(self):
        request = await self.prepared(side="sell", quantity=4, price=200, fees=5, tax_rate="0.2")
        result = await portfolio_trades.record_trade("u1", request)
        self.assertEqual((result["commission"], result["tax_amount"], result["fees"], result["cash_change"]), (5, 1, 6, 794))
        exempt = await self.prepared(side="sell", quantity=1, price=200, fees=5, tax_rate=0)
        result = await portfolio_trades.record_trade("u1", exempt)
        self.assertEqual(result["tax_amount"], 0)
        manual = await self.prepared(side="sell", quantity=1, price=200, fees=5, tax_rate="0.2", tax_amount=3)
        self.assertEqual((await portfolio_trades.record_trade("u1", manual))["fees"], 8)

    async def test_old_request_fingerprint_still_replays_after_tax_fields_added(self):
        request = await self.prepared()
        await portfolio_trades.record_trade("u1", request)
        payload = request.model_dump(mode="json", exclude={"request_id", "tax_rate", "tax_amount"})
        db = await get_db()
        row = await (await db.execute("SELECT fingerprint FROM portfolio_trades")).fetchone()
        self.assertEqual(row["fingerprint"], portfolio_trades._digest(payload))
        self.assertTrue((await portfolio_trades.record_trade("u1", request))["replayed"])

    async def test_foreign_sale_creates_missing_currency_cash(self):
        await portfolio.save_portfolio_item("u1", "AAPL", "Apple", 3, 120000, "USD")
        result = await portfolio_trades.record_trade("u1", await self.prepared(stock_code="AAPL", side="sell", quantity=1, price=150, fees=1, currency="USD"))
        self.assertEqual((result["cash_after"], result["quantity_after"], result["avg_price_after"]), (149, 2, 120000))
        self.assertEqual((await portfolio.get_portfolio_item("u1", "CASH_USD"))["currency"], "USD")

    async def test_mixed_cost_currency_requires_explicit_execution_fx(self):
        await portfolio.save_portfolio_item("u1", "AAPL", "Apple", 2, 100000, "USD")
        await portfolio.save_portfolio_item("u1", "CASH_USD", "달러", 1000, 1, "USD")
        with self.assertRaisesRegex(TradeError, "체결 환율"):
            await self.prepared(stock_code="AAPL", quantity=1, price=100, fees=1, currency="USD")
        result = await portfolio_trades.record_trade("u1", await self.prepared(stock_code="AAPL", quantity=1, price=100, fees=1, currency="USD", cost_fx_rate=1400))
        self.assertEqual(result["cash_after"], 899)
        self.assertAlmostEqual(result["avg_price_after"], (200000+101*1400)/3)
        self.assertEqual(result["avg_price_currency"], "KRW")
        with self.assertRaisesRegex(TradeError, "체결 환율"):
            await self.prepared(stock_code="AAPL", quantity=1, price=100, fees=0, currency="USD", cost_fx_rate=1_000_000_000_000)

    async def test_fractional_trade_cannot_silently_lose_shares_in_real_column(self):
        await portfolio.save_portfolio_item("u1", "AAPL", "Apple", 999999999, 1, "USD", avg_price_currency="USD")
        await portfolio.save_portfolio_item("u1", "CASH_USD", "달러", 100, 1, "USD")
        with self.assertRaisesRegex(TradeError, "정밀도"):
            await self.prepared(stock_code="AAPL", quantity="0.00000001", price="1000000", fees=0, currency="USD")

    async def test_invalid_balances_and_instruments_do_not_write(self):
        before = await portfolio.get_portfolio("u1")
        for changes in [{"quantity": 100}, {"side": "sell", "quantity": 11},
                        {"side": "sell", "quantity": 1, "fees": 200},
                        {"currency": "USD"}, {"stock_code": "CASH_KRW"}, {"fees": "0.1"}]:
            with self.subTest(changes=changes), self.assertRaises(TradeError):
                await self.prepared(**changes)
        self.assertEqual(before, await portfolio.get_portfolio("u1"))
        self.assertEqual(await portfolio_trades.list_trades("u1"), [])

    async def test_short_and_paired_positions_are_not_cash_settled(self):
        await portfolio.save_portfolio_item("u1", "000660", "헤지", -2, 100)
        with self.assertRaisesRegex(TradeError, "롱숏"):
            await self.prepared(stock_code="000660", side="buy", quantity=1)
        await portfolio.set_portfolio_pair("u1", "000660", "005930")
        with self.assertRaisesRegex(TradeError, "롱숏"):
            await self.prepared()

    async def test_cash_and_cost_updates_roll_back_on_cash_failure(self):
        request = await self.prepared()
        before = await portfolio.get_portfolio("u1")
        async with transaction() as db:
            await db.execute("CREATE TRIGGER fail_cash BEFORE UPDATE ON user_portfolio WHEN NEW.stock_code = 'CASH_KRW' BEGIN SELECT RAISE(ABORT, 'cash failed'); END")
        with self.assertRaises(DBError):
            await portfolio_trades.record_trade("u1", request)
        self.assertEqual(await portfolio.get_portfolio("u1"), before)
        self.assertEqual(await portfolio_trades.list_trades("u1"), [])

    async def test_stale_preview_blocks_lost_update(self):
        request = await self.prepared()
        await portfolio.save_portfolio_item("u1", "CASH_KRW", "원화", 3000, 1)
        with self.assertRaises(TradeConflict):
            await portfolio_trades.record_trade("u1", request)
        self.assertEqual((await portfolio.get_portfolio_item("u1", "005930"))["quantity"], 10)
        self.assertEqual(await portfolio_trades.list_trades("u1"), [])

    async def test_duplicate_requests_are_applied_once_even_concurrently(self):
        request = await self.prepared()
        results = await asyncio.gather(*[portfolio_trades.record_trade("u1", request) for _ in range(3)])
        self.assertEqual(sum(not r["replayed"] for r in results), 1)
        self.assertEqual((await portfolio.get_portfolio_item("u1", "CASH_KRW"))["quantity"], 1690)
        with self.assertRaises(TradeConflict):
            await portfolio_trades.record_trade("u1", request.model_copy(update={"memo": "다른 내용"}))

    async def test_concurrent_distinct_trades_cannot_overspend(self):
        a = await self.prepared(quantity=10, price=150, fees=0)
        b = await self.prepared(quantity=10, price=150, fees=0)
        results = await asyncio.gather(portfolio_trades.record_trade("u1", a), portfolio_trades.record_trade("u1", b), return_exceptions=True)
        self.assertEqual(sum(isinstance(r, TradeConflict) for r in results), 1)
        self.assertEqual((await portfolio.get_portfolio_item("u1", "CASH_KRW"))["quantity"], 500)

    async def test_ledger_failure_rolls_back_stock_and_cash(self):
        request = await self.prepared()
        before = await portfolio.get_portfolio("u1")
        async with transaction() as db:
            await db.execute("CREATE TRIGGER fail_trade BEFORE INSERT ON portfolio_trades BEGIN SELECT RAISE(ABORT, 'ledger failed'); END")
        with self.assertRaises(DBError):
            await portfolio_trades.record_trade("u1", request)
        self.assertEqual(await portfolio.get_portfolio("u1"), before)
        self.assertEqual(await portfolio_trades.list_trades("u1"), [])

    async def test_no_partially_updated_holdings_visible_to_other_task(self):
        request = await self.prepared()
        original = portfolio.save_portfolio_item
        observations = []
        async def paused(*args, **kwargs):
            result = await original(*args, **kwargs)
            async def read_elsewhere():
                return await portfolio.get_portfolio("u1")
            observations.extend(await asyncio.create_task(read_elsewhere()))
            return result
        with patch.object(portfolio, "save_portfolio_item", paused):
            await portfolio_trades.record_trade("u1", request)
        self.assertEqual({i["stock_code"]: i["quantity"] for i in observations}, {"005930": 10, "CASH_KRW": 2000})

    async def test_routes_auth_validation_and_user_isolation(self):
        app = FastAPI()
        app.include_router(routes.router)
        register_exception_handlers(app)
        request = await self.prepared()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            with patch.object(routes, "get_current_user", AsyncMock(return_value=None)):
                self.assertEqual((await client.get('/api/portfolio/trades')).status_code, 401)
            with patch.object(routes, "get_current_user", AsyncMock(return_value={"google_sub": "u1"})):
                for field, value in [("quantity", True), ("quantity", 0), ("price", "NaN"), ("fees", -1), ("currency", "XYZ")]:
                    payload = request.model_dump(mode="json")
                    payload[field] = value
                    self.assertEqual((await client.post('/api/portfolio/trades', json=payload)).status_code, 422)
                self.assertEqual((await client.post('/api/portfolio/trades', json=request.model_dump(mode="json"))).status_code, 200)
            with patch.object(routes, "get_current_user", AsyncMock(return_value={"google_sub": "u2"})):
                self.assertEqual((await client.get('/api/portfolio/trades')).json(), [])
                response = await client.post('/api/portfolio/trades', json=request.model_dump(mode="json"))
                self.assertEqual(response.status_code, 409)
        db = await get_db()
        self.assertEqual(json.loads((await (await db.execute("SELECT result_json FROM portfolio_trades")).fetchone())[0])["quantity_after"], 12)
