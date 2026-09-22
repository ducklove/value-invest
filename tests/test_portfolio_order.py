import json
from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin, seed_user

from domain.portfolio_order import initial_order_key
from repositories import account_holdings, accounts, bootstrap, brokers, portfolio, portfolio_order
from repositories.broker_secrets import BrokerError
from repositories.db import get_db
from services.brokers import sync


def test_initial_order_classifies_codes_without_using_display_name_or_group():
    codes = ["CASH_USD", "FUEVFVND.HM", "AAPL", "KRX_GOLD", "0074K0", "000660", "02826K", "CASH_KRW"]
    assert sorted(codes, key=initial_order_key) == [
        "000660", "0074K0", "02826K", "KRX_GOLD", "AAPL", "FUEVFVND.HM", "CASH_KRW", "CASH_USD",
    ]
    assert initial_order_key("0074K0.KS")[0] == 0
    assert initial_order_key("83188.HK")[0] == 1
    assert initial_order_key("CMA_RP_KRW")[0] == 2
    assert initial_order_key("FUTURES_BASE_KRW", "krfuture")[0] == 0
    assert initial_order_key("FUTURES_BASE_KRW", "gbfuture")[0] == 1


class PortfolioOrderTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await seed_user("u2", "second@example.com")
        await account_holdings.ensure("u1")
        self.manual = await accounts.get_default_account_id("u1")
        self.aid = (await accounts.create_account("u1", name="NH"))["account_id"]
        cid = await brokers.store_credential("u1", "order-test-key", "order-test-secret")
        await brokers.link_account("u1", self.aid, cid, "12345678901", "live", True)
        self.rows = [dict(stock_code=code, stock_name=code, quantity=10, avg_price=100,
                          avg_price_currency="KRW", currency="KRW")
                     for code in ["CASH_USD", "MSFT", "005930", "CASH_KRW", "AAPL", "000660"]]
        self.default_order = ["000660", "005930", "AAPL", "MSFT", "CASH_KRW", "CASH_USD"]

    async def resync(self):
        with patch.object(sync, "fetch_snapshot", AsyncMock(return_value=(self.rows, {}))):
            return await sync.sync_account("u1", self.aid)

    async def codes(self, aid=None):
        return [row["stock_code"] for row in await portfolio.get_portfolio("u1", aid)]

    async def test_initial_sync_and_empty_aggregate_have_deterministic_default_order(self):
        await self.resync()
        self.assertEqual(await self.codes(self.aid), self.default_order)
        self.assertEqual(await self.codes(), self.default_order)
        self.rows.reverse()
        await self.resync()
        self.assertEqual(await self.codes(self.aid), self.default_order)

    async def test_custom_order_survives_balance_updates_and_new_symbols(self):
        await self.resync()
        manual_order = list(reversed(self.default_order))
        await portfolio_order.save("u1", self.aid, manual_order)
        self.rows[2]["quantity"] = 7
        self.rows.extend([dict(self.rows[2], stock_code=code) for code in ["ZZZ", "010140"]])
        self.rows.reverse()
        await self.resync()
        self.assertEqual(await self.codes(self.aid), manual_order + ["010140", "ZZZ"])
        self.assertEqual((await account_holdings.get_position("u1", "005930", self.aid))["quantity"], 7)

    async def test_reset_preserves_other_accounts_aggregate_metadata_and_ledger(self):
        await self.resync()
        for code in ["005930", "000660", "CASH_KRW"]:
            await portfolio.save_portfolio_item("u1", code, code, 3, 100, account_id=self.manual)
        await portfolio_order.save("u1", self.manual, ["CASH_KRW", "005930", "000660"])
        from repositories import portfolio_metadata
        await portfolio_metadata.save("u1", "005930", custom_name="사용자 이름", memo="유지할 메모", target_price=1000)
        await portfolio_order.save("u1", self.aid, list(reversed(self.default_order)))
        before = await account_holdings.list_positions("u1")
        aggregate = await portfolio.get_portfolio("u1")
        await portfolio_order.reset("u1", self.aid)
        self.assertEqual(await self.codes(self.aid), self.default_order)
        self.assertEqual(await self.codes(self.manual), ["CASH_KRW", "005930", "000660"])
        self.assertEqual(await portfolio.get_portfolio("u1"), aggregate)
        self.assertEqual(await account_holdings.list_positions("u1"), before)
        item = next(row for row in await portfolio.get_portfolio("u1", self.aid) if row["stock_code"] == "005930")
        self.assertEqual((item["stock_name"], item["memo"], item["target_price"]), ("사용자 이름", "유지할 메모", 1000))
        # 전체 합산에서의 수동 순서 변경도 두 계좌의 명시적 순서는 바꾸지 않는다.
        await portfolio.save_portfolio_order("u1", list(reversed(await self.codes())))
        self.assertEqual(await self.codes(self.aid), self.default_order)
        self.assertEqual(await self.codes(self.manual), ["CASH_KRW", "005930", "000660"])

    async def test_failed_initial_sync_leaves_order_unset_and_retry_initializes_it(self):
        with patch.object(sync, "fetch_snapshot", AsyncMock(side_effect=BrokerError("일시 오류"))):
            with self.assertRaises(BrokerError):
                await sync.sync_account("u1", self.aid)
        db = await get_db()
        row = await (await db.execute("SELECT holding_order_json FROM portfolio_accounts WHERE account_id=?", (self.aid,))).fetchone()
        self.assertIsNone(row["holding_order_json"])
        await self.resync()
        self.assertEqual(await self.codes(self.aid), self.default_order)

    async def test_legacy_accounts_keep_existing_order_through_migration_and_sync(self):
        await self.resync()
        from repositories.db import transaction
        async with transaction() as db:
            await db.execute("UPDATE portfolio_accounts SET holding_order_json=NULL WHERE account_id=?", (self.aid,))
        custom_order = list(reversed(self.default_order))
        await portfolio.save_portfolio_order("u1", custom_order)
        await bootstrap.init_db()
        await bootstrap.init_db()
        await self.resync()
        self.assertEqual(await self.codes(self.aid), custom_order)

    async def test_order_write_rejects_partial_duplicate_unknown_and_other_user(self):
        await self.resync()
        for codes in (self.default_order[:-1], ["UNKNOWN"] + self.default_order[1:],
                      [self.default_order[0]] + self.default_order[:-1]):
            with self.subTest(codes=codes), self.assertRaises(accounts.AccountError):
                await portfolio_order.save("u1", self.aid, codes)
        with self.assertRaises(accounts.AccountError):
            await portfolio_order.reset("u2", self.aid)
        with self.assertRaises(accounts.AccountError):
            await portfolio_order.save("u2", self.aid, self.default_order)
        self.assertEqual(await self.codes(self.aid), self.default_order)
        db = await get_db()
        row = await (await db.execute("SELECT holding_order_json FROM portfolio_accounts WHERE account_id=?", (self.aid,))).fetchone()
        self.assertEqual(json.loads(row["holding_order_json"]), self.default_order)
