from datetime import date
from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin, seed_user
from pydantic import ValidationError

import snapshot_nav
from domain.portfolio_inputs import CashflowInput
from repositories import db as db_repo
from repositories import portfolio as portfolio_repo
from repositories import snapshots as repo
from routes import portfolio as routes


class NavCashflowAccountingTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await repo.add_cashflow_and_sync_cash("u1", "2026-08-31", "deposit", 10000, None, None, None)
        async with db_repo.transaction() as db:
            await db.execute(
                "UPDATE portfolio_cashflows SET applied_snapshot_date='2026-08-31', created_at='2026-08-31T19:00:00'",
            )
        await repo.save_snapshot("u1", "2026-08-31", 10000, 10000, 1000, 10)

    async def settle(self, day, value):
        with patch.object(snapshot_nav, "_fetch_total_value", AsyncMock(return_value=(
            value, value, [{"stock_code": "CASH_KRW", "market_value": value}],
        ))):
            await snapshot_nav.take_snapshot("u1", day)
        return await repo.get_snapshot_by_date("u1", day)

    async def test_unsettled_withdrawal_is_neutralized_and_applied_once(self):
        await repo.add_cashflow_and_sync_cash("u1", "2026-09-04", "withdrawal", 2000, None, None, None)
        snap = await self.settle("2026-09-07", 8000)
        self.assertAlmostEqual(snap["nav"], 1000)
        self.assertAlmostEqual(snap["total_units"], 8)
        snap = await self.settle("2026-09-07", 8000)
        self.assertAlmostEqual(snap["nav"], 1000)
        self.assertEqual(await repo.get_pending_cashflows("u1", "2026-09-07"), [])

    async def test_concurrent_deposit_retries_before_issuing_units(self):
        calls = 0

        async def value(*args):
            nonlocal calls
            calls += 1
            cash = await portfolio_repo.get_portfolio_item("u1", "CASH_KRW")
            amount = cash["quantity"]
            if calls == 1:
                await repo.add_cashflow_and_sync_cash("u1", "2026-09-07", "deposit", 2000, None, None, None)
            return amount, amount, []

        with patch.object(snapshot_nav, "_fetch_total_value", side_effect=value):
            await snapshot_nav.take_snapshot("u1", "2026-09-07")
        self.assertEqual(calls, 2)
        snap = await repo.get_latest_snapshot("u1")
        self.assertAlmostEqual(snap["nav"], 1000)
        self.assertAlmostEqual(snap["total_units"], 12)
        self.assertEqual(snap["total_value"], 12000)

    async def test_stock_failure_rolls_back_nav_and_cashflow_markers(self):
        cf = await repo.add_cashflow_and_sync_cash("u1", "2026-09-07", "deposit", 2000, None, None, None)
        with patch.object(repo, "_refresh_group_snapshots", side_effect=RuntimeError("aggregate failed")):
            with self.assertRaises(RuntimeError):
                await self.settle("2026-09-07", 12000)
        self.assertIsNone(await repo.get_snapshot_by_date("u1", "2026-09-07"))
        flow = next(row for row in await repo.get_cashflows("u1") if row["id"] == cf["id"])
        self.assertIsNone(flow["applied_snapshot_date"])
        self.assertIsNone(flow["units_change"])

    async def test_settled_cancellation_keeps_history_and_reverses_once(self):
        cf = await repo.add_cashflow_and_sync_cash("u1", "2026-09-01", "deposit", 2000, None, None, None)
        before = await self.settle("2026-09-01", 12000)
        for _ in range(2):
            self.assertTrue(await repo.delete_cashflow_and_sync_cash("u1", cf["id"]))
        self.assertEqual(before, await repo.get_snapshot_by_date("u1", "2026-09-01"))
        flows = await repo.get_cashflows("u1")
        reversal = [row for row in flows if row["reversal_of_id"] == cf["id"]]
        self.assertEqual(len(reversal), 1)
        self.assertEqual(reversal[0]["type"], "withdrawal")
        self.assertEqual((await portfolio_repo.get_portfolio_item("u1", "CASH_KRW"))["quantity"], 10000)
        today = date.today().isoformat()
        after = await self.settle(today, 10000)
        self.assertAlmostEqual(after["nav"], 1000)
        with self.assertRaises(repo.CashflowCancellationError):
            await repo.delete_cashflow_and_sync_cash("u1", reversal[0]["id"])

    async def test_cancellation_rejects_short_balance_without_changing_history(self):
        cf = await repo.add_cashflow_and_sync_cash("u1", "2026-09-01", "deposit", 20000, None, None, None)
        before = await self.settle("2026-09-01", 30000)
        await repo.add_cashflow_and_sync_cash("u1", "2026-09-02", "withdrawal", 25000, None, None, None)
        with self.assertRaises(repo.CashflowBalanceError):
            await repo.delete_cashflow_and_sync_cash("u1", cf["id"])
        self.assertEqual(before, await repo.get_snapshot_by_date("u1", "2026-09-01"))
        self.assertFalse(any(row["reversal_of_id"] for row in await repo.get_cashflows("u1")))

    async def test_post_20h_settled_cashflow_is_not_counted_twice(self):
        cf = await repo.add_cashflow_and_sync_cash("u1", "2026-09-07", "deposit", 2000, None, None, None)
        async with db_repo.transaction() as db:
            await db.execute("UPDATE portfolio_cashflows SET created_at='2026-09-07T20:02:00' WHERE id=?", (cf["id"],))
        await self.settle("2026-09-07", 12000)
        self.assertEqual(await repo.get_cashflows_created_after("u1", "2026-09-07T20:00:00"), [])
        with patch.object(routes, "get_current_user", AsyncMock(return_value={"google_sub": "u1"})), \
             patch.object(routes, "_portfolio_today_baseline_date", return_value="2026-09-07"):
            response = await routes.get_prev_day_snapshot(object())
        self.assertEqual(response["today_net_cashflow"], 0)

    async def test_backdated_cashflow_counts_in_month_when_cash_actually_arrives(self):
        await repo.add_cashflow_and_sync_cash("u1", "2026-08-25", "deposit", 2000, None, None, None)
        net, by_stock = await routes._net_cashflow_since_snapshot("u1", "2026-08-31")
        self.assertEqual(net, 2000)
        self.assertEqual(by_stock, {"CASH_KRW": 2000})
        await self.settle("2026-09-07", 12000)
        net, _ = await routes._net_cashflow_since_snapshot("u1", "2026-08-31")
        self.assertEqual(net, 2000)

    async def test_future_dated_deposit_is_rejected_before_cash_changes(self):
        with self.assertRaises(ValidationError):
            CashflowInput(type="deposit", amount=2000, date="2999-09-09")
        with self.assertRaises(repo.CashflowCancellationError):
            await repo.add_cashflow_and_sync_cash("u1", "2999-09-09", "deposit", 2000, None, None, None)
        self.assertEqual((await portfolio_repo.get_portfolio_item("u1", "CASH_KRW"))["quantity"], 10000)

    async def test_full_withdrawal_and_reentry_preserve_nav(self):
        await repo.add_cashflow_and_sync_cash("u1", "2026-09-01", "withdrawal", 10000, None, None, None)
        empty = await self.settle("2026-09-01", 0)
        self.assertEqual(empty["total_units"], 0)
        self.assertEqual(empty["nav"], 1000)
        await repo.add_cashflow_and_sync_cash("u1", "2026-09-02", "deposit", 5000, None, None, None)
        snap = await self.settle("2026-09-02", 5000)
        self.assertEqual(snap["nav"], 1000)
        self.assertEqual(snap["total_units"], 5)

    async def test_full_withdrawal_roundoff_does_not_reset_nav_to_zero(self):
        units = 6911405.723465628
        async with db_repo.transaction() as db:
            await db.execute("UPDATE user_portfolio SET quantity=10000.1 WHERE google_sub='u1' AND stock_code='CASH_KRW'")
        await repo.save_snapshot("u1", "2026-08-31", 10000.1, 10000.1, 10000.1 / units, units)
        await repo.add_cashflow_and_sync_cash("u1", "2026-09-01", "withdrawal", 10000.1, None, None, None)
        empty = await self.settle("2026-09-01", 0)
        self.assertEqual(empty["total_units"], 0)
        self.assertAlmostEqual(empty["nav"], 10000.1 / units)
