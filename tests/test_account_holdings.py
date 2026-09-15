from datetime import datetime
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from _harness import TempDbMixin, seed_user

from domain.portfolio_trades import TradeCreate, TradeInput
from repositories import account_holdings, accounts, bootstrap, portfolio, portfolio_trades, snapshots
from repositories.db import get_db
from services.portfolio import fx


class AccountHoldingsTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await seed_user("u2", "second@example.com")
        await portfolio.save_portfolio_item("u1", "005930", "삼성전자", 10, 100)
        self.default = await accounts.get_default_account_id("u1")
        self.second = (await accounts.create_account("u1", name="NH 일반"))["account_id"]

    async def test_same_stock_in_two_accounts_aggregates_and_scoped_edit_preserves_other(self):
        await portfolio.save_portfolio_item("u1", "005930", "삼성전자", 20, 200, account_id=self.second)
        combined = (await portfolio.get_portfolio("u1"))[0]
        self.assertEqual(combined["quantity"], 30)
        self.assertAlmostEqual(combined["avg_price"], 5000 / 30)
        self.assertEqual(combined["account_count"], 2)
        await portfolio.save_portfolio_item("u1", "005930", "삼성전자", 5, 300, account_id=self.second)
        self.assertEqual((await portfolio.get_portfolio("u1", self.default))[0]["quantity"], 10)
        self.assertEqual((await portfolio.get_portfolio("u1", self.second))[0]["quantity"], 5)
        self.assertEqual((await portfolio.get_portfolio("u1"))[0]["quantity"], 15)
        with self.assertRaises(accounts.AccountError):
            await portfolio.delete_portfolio_item("u1", "005930")
        await portfolio.delete_portfolio_item("u1", "005930", self.second)
        self.assertEqual((await portfolio.get_portfolio("u1"))[0]["quantity"], 10)

    async def test_account_ownership_and_nonempty_delete(self):
        with self.assertRaises(accounts.AccountError):
            await portfolio.save_portfolio_item("u2", "005930", "삼성전자", 2, 3, account_id=self.second)
        await portfolio.save_portfolio_item("u1", "005930", "삼성전자", 2, 3, account_id=self.second)
        with self.assertRaises(accounts.AccountError):
            await accounts.delete_account("u1", self.second)
        with self.assertRaises(accounts.AccountError):
            await portfolio.get_portfolio("u2", self.second)

    async def test_trade_uses_selected_account_cash_and_idempotency(self):
        await portfolio.save_portfolio_item("u1", "CASH_KRW", "현금", 10000, 1, account_id=self.default)
        await portfolio.save_portfolio_item("u1", "CASH_KRW", "현금", 1000, 1, account_id=self.second)
        values = dict(account_id=self.second, stock_code="005930", stock_name="삼성전자", side="buy", quantity=2, price=100)
        preview = await portfolio_trades.preview_trade("u1", TradeInput(**values))
        self.assertEqual(preview["cash_before"], 1000)
        self.assertEqual(preview["quantity_before"], 0)
        request = TradeCreate(**values, request_id=uuid4(), expected_revision=preview["revision"])
        await portfolio_trades.record_trade("u1", request)
        self.assertTrue((await portfolio_trades.record_trade("u1", request))["replayed"])
        self.assertEqual((await account_holdings.get_position("u1", "CASH_KRW", self.second))["quantity"], 800)
        self.assertEqual((await account_holdings.get_position("u1", "CASH_KRW", self.default))["quantity"], 10000)
        self.assertEqual((await portfolio.get_portfolio_item("u1", "005930"))["quantity"], 12)

    async def test_mixed_purchase_currency_keeps_each_cost_component(self):
        await portfolio.save_portfolio_item("u1", "AAPL", "Apple", 1, 200000, "USD", avg_price_currency="KRW", account_id=self.default)
        await portfolio.save_portfolio_item("u1", "AAPL", "Apple", 2, 100, "USD", avg_price_currency="USD", account_id=self.second)
        rows = await portfolio.get_portfolio("u1")
        with patch.object(fx, "fx_rate_for_currency", AsyncMock(return_value=1500)):
            await fx.annotate_avg_price_krw(rows)
        apple = next(row for row in rows if row["stock_code"] == "AAPL")
        self.assertAlmostEqual(apple["avg_price_krw"] * apple["quantity"], 500000)
        self.assertTrue(apple["mixed_cost_currency"])
        self.assertEqual((await account_holdings.get_position("u1", "AAPL", self.second))["avg_price"], 100)

    async def test_migration_is_idempotent_and_projection_matches(self):
        await portfolio.save_portfolio_item("u1", "005930", "삼성전자", 20, 200, account_id=self.second)
        await bootstrap.init_db()
        await bootstrap.init_db()
        rows = await account_holdings.list_positions("u1")
        self.assertEqual(sum(row["quantity"] for row in rows), 30)
        db = await get_db()
        raw = await (await db.execute("SELECT quantity FROM user_portfolio WHERE google_sub='u1' AND stock_code='005930'")).fetchone()
        self.assertEqual(raw["quantity"], 30)

    async def test_projection_failure_rolls_back_account_ledger(self):
        before = await account_holdings.list_positions("u1")
        with patch.object(portfolio, "_save_portfolio_projection", AsyncMock(side_effect=ValueError("저장 실패"))):
            with self.assertRaises(ValueError):
                await portfolio.save_portfolio_item("u1", "005930", "삼성전자", 20, 200, account_id=self.second)
        self.assertEqual(await account_holdings.list_positions("u1"), before)
        self.assertEqual((await portfolio.get_portfolio("u1"))[0]["quantity"], 10)

    async def test_group_change_preserves_every_account_position(self):
        await portfolio.save_portfolio_item("u1", "005930", "삼성전자", 20, 200, account_id=self.second)
        before = await account_holdings.list_positions("u1")
        await portfolio.set_holding_group("u1", "005930", "관심그룹")
        self.assertEqual(await account_holdings.list_positions("u1"), before)
        self.assertEqual((await portfolio.get_portfolio("u1"))[0]["group_name"], "관심그룹")

    async def test_deposit_and_cancel_return_to_original_account(self):
        await portfolio.save_portfolio_item("u1", "CASH_KRW", "현금", 1000, 1, account_id=self.default)
        flow = await snapshots.add_cashflow_and_sync_cash("u1", datetime.now().date().isoformat(), "deposit", 200, None, None, None, self.second)
        self.assertEqual((await account_holdings.get_position("u1", "CASH_KRW", self.second))["quantity"], 200)
        await snapshots.delete_cashflow_and_sync_cash("u1", flow["id"])
        self.assertEqual((await account_holdings.get_position("u1", "CASH_KRW", self.second))["quantity"], 0)
        self.assertEqual((await account_holdings.get_position("u1", "CASH_KRW", self.default))["quantity"], 1000)
