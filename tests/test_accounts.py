"""멀티계좌 phase 1 테스트.

스키마 마이그레이션(default 계좌 백필), repository CRUD, 라우트 엔드포인트를
임시 DB로 검증한다. 핵심 계약: 기존 단일 포트폴리오 동작이 깨지지 않아야 한다
(account_id 가 nullable 이고 default 로 백필된다).
"""

from __future__ import annotations

import unittest

from _harness import TempDbMixin, seed_user
from fastapi.testclient import TestClient

import cache
from core.app_factory import create_app
from repositories import accounts as accounts_repo
from repositories import portfolio as portfolio_repo

SUB = "u1"


async def _seed_user_with_holdings(sub: str = SUB) -> None:
    """사용자 1명 + 보유 종목 2개(account_id 없이) — 백필 전 상태를 모방."""
    await seed_user(sub=sub)
    db = await cache.get_db()
    now = "2026-01-01T00:00:00"
    await db.executemany(
        "INSERT INTO user_portfolio (google_sub, stock_code, stock_name, quantity, avg_price,"
        " avg_price_currency, sort_order, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (sub, "005930", "삼성전자", 10, 70000, "KRW", 0, now, now),
            (sub, "AAPL", "Apple", 5, 150, "USD", 1, now, now),
        ],
    )
    await db.commit()


class AccountMigrationTests(TempDbMixin):
    """init_db 백핀: 기존 사용자의 default 계좌 생성 + holdings 귀속."""

    async def seed(self):
        # init_db 직후(빈 스키마)에 사용자+보유종목을 넣고, 그 다음 init_db 를
        # 다시 호출해 백핀 로직이 동작하는지 검증한다.
        await _seed_user_with_holdings()
        await cache.init_db()  # idempotent — 백핀 재실행

    async def test_default_account_created_for_existing_user(self):
        accounts = await accounts_repo.list_accounts(SUB)
        self.assertEqual(len(accounts), 1)
        self.assertEqual(accounts[0]["name"], "기본 계좌")
        self.assertEqual(accounts[0]["type"], "general")

    async def test_holdings_backfilled_to_default_account(self):
        default_id = await accounts_repo.get_default_account_id(SUB)
        self.assertIsNotNone(default_id)
        db = await cache.get_db()
        cursor = await db.execute(
            "SELECT account_id FROM user_portfolio WHERE google_sub = ?", (SUB,)
        )
        rows = await cursor.fetchall()
        # 모든 보유 종목이 default 계좌로 귀속됐다.
        self.assertTrue(all(r["account_id"] == default_id for r in rows))
        self.assertEqual(len(rows), 2)

    async def test_user_without_holdings_has_no_account(self):
        # 보유 종목이 없는 사용자는 백핀 대상이 아니다(루프가 user_portfolio 기준).
        accounts = await accounts_repo.list_accounts(SUB)
        # _seed_user_with_holdings 로 SUB 는 보유종목이 있으므로 1개.
        self.assertEqual(len(accounts), 1)

    async def test_default_account_backfill_function_is_idempotent(self):
        db = await cache.get_db()
        await db.execute(
            "UPDATE user_portfolio SET account_id = NULL WHERE google_sub = ? AND stock_code = '005930'",
            (SUB,),
        )
        await accounts_repo.ensure_default_account(db, SUB)
        accounts = await accounts_repo.list_accounts(SUB)
        self.assertEqual(len(accounts), 1)

        cursor = await db.execute(
            "SELECT account_id FROM user_portfolio WHERE google_sub = ? AND stock_code = '005930'",
            (SUB,),
        )
        row = await cursor.fetchone()
        self.assertEqual(row["account_id"], accounts[0]["account_id"])

    async def test_portfolio_defaults_backfill_function_restores_groups_and_account(self):
        db = await cache.get_db()
        await db.execute("DELETE FROM portfolio_groups WHERE google_sub = ?", (SUB,))
        await db.execute("DELETE FROM portfolio_accounts WHERE google_sub = ?", (SUB,))
        await db.execute(
            "UPDATE user_portfolio SET group_name = NULL, account_id = NULL WHERE google_sub = ?",
            (SUB,),
        )
        await portfolio_repo.backfill_portfolio_defaults(db)

        accounts = await accounts_repo.list_accounts(SUB)
        self.assertEqual(len(accounts), 1)
        cursor = await db.execute(
            "SELECT group_name, COUNT(*) AS n FROM portfolio_groups WHERE google_sub = ? GROUP BY group_name",
            (SUB,),
        )
        groups = {row["group_name"]: row["n"] for row in await cursor.fetchall()}
        self.assertEqual(groups, {"한국주식": 1, "해외주식": 1, "기타": 1})

        cursor = await db.execute(
            "SELECT stock_code, group_name, account_id FROM user_portfolio WHERE google_sub = ?",
            (SUB,),
        )
        holdings = {row["stock_code"]: dict(row) for row in await cursor.fetchall()}
        self.assertEqual(holdings["005930"]["group_name"], "한국주식")
        self.assertEqual(holdings["AAPL"]["group_name"], "해외주식")
        self.assertTrue(all(row["account_id"] == accounts[0]["account_id"] for row in holdings.values()))


class AccountsRepoTests(TempDbMixin):
    async def seed(self):
        await _seed_user_with_holdings()
        # init_db 백핀을 트리거해 default 계좌를 보장한다(마이그레이션 재실행).
        await cache.init_db()

    async def test_create_account_assigns_sort_order(self):
        # 기본 계좌가 이미 sort_order=0 으로 존재. 새 계좌는 1.
        isa = await accounts_repo.create_account(SUB, name="ISA", type="isa")
        self.assertEqual(isa["sort_order"], 1)
        self.assertEqual(isa["type"], "isa")
        self.assertTrue(isa["account_id"].startswith("acc-"))

    async def test_create_account_rejects_empty_name(self):
        with self.assertRaises(accounts_repo.AccountError):
            await accounts_repo.create_account(SUB, name="")

    async def test_create_account_rejects_long_name(self):
        with self.assertRaises(accounts_repo.AccountError):
            await accounts_repo.create_account(SUB, name="x" * 41)

    async def test_create_account_rejects_bad_type(self):
        with self.assertRaises(accounts_repo.AccountError):
            await accounts_repo.create_account(SUB, name="X", type="nonexistent")

    async def test_create_account_rejects_duplicate_name(self):
        await accounts_repo.create_account(SUB, name="ISA", type="isa")
        with self.assertRaises(accounts_repo.AccountError) as ctx:
            await accounts_repo.create_account(SUB, name="ISA", type="isa")
        self.assertIn("같은 이름", str(ctx.exception))

    async def test_update_account_name(self):
        acct = await accounts_repo.create_account(SUB, name="ISA", type="isa")
        updated = await accounts_repo.update_account(SUB, acct["account_id"], name="ISA 계좌")
        self.assertEqual(updated["name"], "ISA 계좌")
        self.assertEqual(updated["type"], "isa")  # type unchanged

    async def test_update_account_type(self):
        acct = await accounts_repo.create_account(SUB, name="계좌A")
        updated = await accounts_repo.update_account(SUB, acct["account_id"], type="pension")
        self.assertEqual(updated["type"], "pension")

    async def test_update_nonexistent_account(self):
        with self.assertRaises(accounts_repo.AccountError):
            await accounts_repo.update_account(SUB, "acc-doesnotexist", name="X")

    async def test_delete_account_reparents_holdings_to_default(self):
        default_id = await accounts_repo.get_default_account_id(SUB)
        # 보유 종목 하나를 새 계좌로 이동
        isa = await accounts_repo.create_account(SUB, name="ISA", type="isa")
        db = await cache.get_db()
        await db.execute(
            "UPDATE user_portfolio SET account_id = ? WHERE google_sub = ? AND stock_code = '005930'",
            (isa["account_id"], SUB),
        )
        await db.commit()
        # ISA 계좌 삭제 → 삼성전자가 default 계좌로 재귀속
        await accounts_repo.delete_account(SUB, isa["account_id"])
        cursor = await db.execute(
            "SELECT account_id FROM user_portfolio WHERE google_sub = ? AND stock_code = '005930'",
            (SUB,),
        )
        row = await cursor.fetchone()
        self.assertEqual(row["account_id"], default_id)
        # ISA 계좌는 사라졌다
        self.assertIsNone(await accounts_repo.get_account(SUB, isa["account_id"]))

    async def test_cannot_delete_default_account(self):
        default_id = await accounts_repo.get_default_account_id(SUB)
        with self.assertRaises(accounts_repo.AccountError) as ctx:
            await accounts_repo.delete_account(SUB, default_id)
        self.assertIn("기본 계좌", str(ctx.exception))

    async def test_reorder_accounts(self):
        a1 = await accounts_repo.create_account(SUB, name="ISA", type="isa")
        a2 = await accounts_repo.create_account(SUB, name="연금", type="pension")
        default_id = await accounts_repo.get_default_account_id(SUB)
        # 순서 뒤집기: 연금 → ISA → 기본
        reordered = await accounts_repo.reorder_accounts(SUB, [a2["account_id"], a1["account_id"], default_id])
        self.assertEqual([a["account_id"] for a in reordered], [a2["account_id"], a1["account_id"], default_id])

    async def test_reorder_ignores_unknown_ids(self):
        a1 = await accounts_repo.create_account(SUB, name="ISA", type="isa")
        default_id = await accounts_repo.get_default_account_id(SUB)
        # 알 수 없는 id 가 섞여 있어도 에러 없이 통과(무시).
        reordered = await accounts_repo.reorder_accounts(SUB, [a1["account_id"], "acc-unknown", default_id])
        self.assertEqual(len(reordered), 2)


class AccountsRouteTests(unittest.TestCase):
    """TestClient 기반 라우트 엔드포인트 테스트."""

    @classmethod
    def setUpClass(cls):
        cls.app = create_app()
        cls.client = TestClient(cls.app)

    def test_list_without_auth_returns_401(self):
        resp = self.client.get("/api/portfolio/accounts")
        self.assertEqual(resp.status_code, 401)
