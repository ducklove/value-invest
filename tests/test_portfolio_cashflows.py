from _harness import TempDbMixin, seed_user
from repositories import portfolio as portfolio_repo
from repositories import snapshots as snapshots_repo


class PortfolioCashflowTransactionTests(TempDbMixin):
    async def seed(self):
        await seed_user()

    async def test_add_cashflow_and_sync_cash_creates_cash_position_atomically(self):
        result = await snapshots_repo.add_cashflow_and_sync_cash(
            "u1",
            "2026-05-28",
            "deposit",
            1500,
            "입금",
            1000,
            1.5,
        )

        cashflows = await snapshots_repo.get_cashflows("u1")
        cash_item = await portfolio_repo.get_portfolio_item("u1", "CASH_KRW")

        self.assertEqual(cashflows[0]["id"], result["id"])
        self.assertEqual(cashflows[0]["amount"], 1500)
        self.assertEqual(cash_item["quantity"], 1500)
        self.assertEqual(cash_item["avg_price"], 1.0)
        self.assertEqual(cash_item["currency"], "KRW")

    async def test_withdrawal_rejects_without_cashflow_write_when_cash_is_short(self):
        with self.assertRaises(snapshots_repo.CashflowBalanceError):
            await snapshots_repo.add_cashflow_and_sync_cash(
                "u1",
                "2026-05-28",
                "withdrawal",
                1500,
                None,
                1000,
                -1.5,
            )

        self.assertEqual(await snapshots_repo.get_cashflows("u1"), [])
        self.assertIsNone(await portfolio_repo.get_portfolio_item("u1", "CASH_KRW"))

    async def test_delete_cashflow_and_sync_cash_reverses_cash_position_atomically(self):
        result = await snapshots_repo.add_cashflow_and_sync_cash(
            "u1",
            "2026-05-28",
            "deposit",
            1500,
            None,
            1000,
            1.5,
        )

        deleted = await snapshots_repo.delete_cashflow_and_sync_cash("u1", result["id"])
        cash_item = await portfolio_repo.get_portfolio_item("u1", "CASH_KRW")

        self.assertTrue(deleted)
        self.assertEqual(await snapshots_repo.get_cashflows("u1"), [])
        self.assertEqual(cash_item["quantity"], 0)
