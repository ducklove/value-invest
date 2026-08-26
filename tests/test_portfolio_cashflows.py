from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin, seed_user

import snapshot_nav
from repositories import db as db_repo
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


class NavUnitsLifecycleTests(TempDbMixin):
    """입출금 유닛의 전체 수명주기 — 정산 반영, 재실행 멱등, 삭제 회수.

    과거에는 '정산일 == date 정확 일치 + 그날 첫 정산'만 유닛을 반영해
    주말/정산 후/소급 입력분이 영구 유실됐다 (다음 정산에서 NAV 가
    입금액만큼 가짜 상승). applied_snapshot_date 마킹이 이를 막는다.
    """

    async def seed(self):
        await seed_user()

    async def _run_settlement(self, snap_date: str, total_value: float):
        with patch.object(
            snapshot_nav,
            "_fetch_total_value",
            new=AsyncMock(return_value=(
                total_value, 8000, [{"stock_code": "005930", "market_value": total_value}],
            )),
        ):
            await snapshot_nav.take_snapshot("u1", snap_date)

    async def test_weekend_deposit_units_survive_to_monday_settlement(self):
        # 금요일 정산: 10,000 = 10 units @ NAV 1000
        await snapshots_repo.save_snapshot("u1", "2026-05-15", 10000, 8000, 1000.0, 10.0, None)
        # 토요일 입금 1,100 (신규 라우트 방식 — units 미정)
        await snapshots_repo.add_cashflow_and_sync_cash(
            "u1", "2026-05-16", "deposit", 1100, None, None, None,
        )
        # 월요일 정산: 평가액 12,100 (입금 1,100 포함, 시장 +1,000)
        await self._run_settlement("2026-05-18", 12100)

        snap = await snapshots_repo.get_snapshot_by_date("u1", "2026-05-18")
        # 발행 NAV = (12100-1100)/10 = 1100 → 입금 유닛 1.0, 총 11 units
        self.assertAlmostEqual(snap["total_units"], 11.0)
        self.assertAlmostEqual(snap["nav"], 1100.0)

        cashflows = await snapshots_repo.get_cashflows("u1")
        self.assertEqual(cashflows[0]["applied_snapshot_date"], "2026-05-18")
        self.assertAlmostEqual(cashflows[0]["units_change"], 1.0)
        self.assertAlmostEqual(cashflows[0]["nav_at_time"], 1100.0)

    async def test_settlement_rerun_does_not_double_apply_units(self):
        await snapshots_repo.save_snapshot("u1", "2026-05-15", 10000, 8000, 1000.0, 10.0, None)
        await snapshots_repo.add_cashflow_and_sync_cash(
            "u1", "2026-05-16", "deposit", 1100, None, None, None,
        )
        await self._run_settlement("2026-05-18", 12100)
        # 재실행 (시세 소폭 변동) — 이미 반영된 유닛이 다시 더해지면 안 된다.
        await self._run_settlement("2026-05-18", 12210)

        snap = await snapshots_repo.get_snapshot_by_date("u1", "2026-05-18")
        self.assertAlmostEqual(snap["total_units"], 11.0)
        self.assertAlmostEqual(snap["nav"], 12210 / 11.0)

    async def test_post_settlement_deposit_lands_on_next_settlement(self):
        """20:05 정산 뒤 입력한 당일자 입금 — 다음 정산이 집어간다.
        (구현 전에는 date 정확 일치 조회라 영원히 유실되던 케이스.)"""
        await self._run_settlement("2026-05-18", 10000)  # 첫 스냅샷: 10 units
        await snapshots_repo.add_cashflow_and_sync_cash(
            "u1", "2026-05-18", "deposit", 1100, None, None, None,
        )
        await self._run_settlement("2026-05-19", 11100)  # 시장 변동 0 + 입금

        snap = await snapshots_repo.get_snapshot_by_date("u1", "2026-05-19")
        # 발행 NAV = (11100-1100)/10 = 1000 → 1.1 units, 총 11.1
        self.assertAlmostEqual(snap["total_units"], 11.1)
        self.assertAlmostEqual(snap["nav"], 1000.0)

    async def test_delete_applied_cashflow_claws_back_units_from_snapshots(self):
        await snapshots_repo.save_snapshot("u1", "2026-05-15", 10000, 8000, 1000.0, 10.0, None)
        result = await snapshots_repo.add_cashflow_and_sync_cash(
            "u1", "2026-05-16", "deposit", 1100, None, None, None,
        )
        await self._run_settlement("2026-05-18", 12100)  # 11 units @ 1100

        deleted = await snapshots_repo.delete_cashflow_and_sync_cash("u1", result["id"])
        self.assertTrue(deleted)

        snap = await snapshots_repo.get_snapshot_by_date("u1", "2026-05-18")
        # 유닛 회수: 11 - 1 = 10, NAV 재계산 12100/10 = 1210. 회수하지 않으면
        # 평가액(CASH_KRW)만 빠지고 유닛이 남아 NAV 가 영구 하락한다.
        self.assertAlmostEqual(snap["total_units"], 10.0)
        self.assertAlmostEqual(snap["nav"], 1210.0)
        # 정산 전 스냅샷은 건드리지 않는다.
        before = await snapshots_repo.get_snapshot_by_date("u1", "2026-05-15")
        self.assertAlmostEqual(before["total_units"], 10.0)
        self.assertAlmostEqual(before["nav"], 1000.0)

    async def test_pending_query_excludes_applied_and_future_dates(self):
        await snapshots_repo.add_cashflow_and_sync_cash("u1", "2026-05-16", "deposit", 100, None, None, None)
        await snapshots_repo.add_cashflow_and_sync_cash("u1", "2026-05-18", "deposit", 200, None, None, None)
        await snapshots_repo.add_cashflow_and_sync_cash("u1", "2026-05-20", "deposit", 300, None, None, None)
        async with db_repo.transaction() as tdb:
            await tdb.execute(
                "UPDATE portfolio_cashflows SET applied_snapshot_date = '2026-05-16' WHERE amount = 100",
            )

        pending = await snapshots_repo.get_pending_cashflows("u1", "2026-05-18")
        self.assertEqual([cf["amount"] for cf in pending], [200])
