from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin, seed_user

from repositories import account_holdings, accounts, brokers, futures_pairs, portfolio
from repositories.db import get_db, transaction
from routes import portfolio as portfolio_routes
from services.brokers import futures_contracts, futures_underlyings, sync

NOV = "KRFUT_KA486B000"
DEC = "KRFUT_KA486C000"
JAN = "KRFUT_KA4871000"
AMORE = "KRFUT_KAC66C000"
MAPPING = {NOV[6:]: "006800", DEC[6:]: "006800", JAN[6:]: "006800", AMORE[6:]: "090430"}


class FuturesPairTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await seed_user("u2", "other@example.com")
        await portfolio.save_portfolio_item("u1", "00680K", "미래에셋2우B", 100, 100, group_name="롱숏")
        await portfolio.save_portfolio_item("u1", "090435", "아모레퍼시픽우", 100, 100)
        self.aid = (await accounts.create_account("u1", name="선물"))["account_id"]
        cid = await brokers.store_credential("u1", "test-futures-pair", "test-secret")
        await brokers.link_account("u1", self.aid, cid, "12345678901", "live", product="krfuture")

    async def holding(self, code, qty=-10):
        await portfolio.save_portfolio_item("u1", code, "선물", qty, 100)

    async def run_sync(self, codes):
        rows = [{"stock_code": code, "stock_name": "미래에셋 F 202612", "quantity": -10,
                 "avg_price": 100, "avg_price_currency": "KRW", "currency": "KRW"} for code in codes]
        with patch.object(sync, "fetch_snapshot", AsyncMock(return_value=(rows, {}))), \
             patch.object(futures_underlyings, "underlying_codes", AsyncMock(return_value=MAPPING)):
            await sync.sync_account("u1", self.aid)

    async def test_partial_rollover_links_existing_december_and_new_preferred_hedge(self):
        # 운영처럼 기존 월물은 청산 도중 양수 수량이어도 연결 근거로 보존한다.
        await self.holding(NOV, 20)
        await portfolio.set_portfolio_pair("u1", NOV, "00680K")
        await self.holding(DEC)
        await self.holding(AMORE)
        await portfolio.set_portfolio_tags("u1", DEC, ["제거할 태그"])
        await self.run_sync([DEC, AMORE])
        items = {r["stock_code"]: r for r in await portfolio.get_portfolio("u1")}
        self.assertEqual(items[DEC]["pair_long_code"], "00680K")
        self.assertEqual(items[DEC]["group_name"], "롱숏")
        self.assertEqual(items[DEC]["tags"], [])
        self.assertEqual(items[AMORE]["pair_long_code"], "090435")
        ordered = list(items)
        self.assertEqual(ordered[ordered.index("00680K") + 1:ordered.index("00680K") + 3], [DEC, NOV])

    async def test_full_rollover_remembers_link_even_across_empty_balance_and_restart(self):
        # 원장에만 있는 기존 월물이 새 월물보다 먼저 삭제되는 경우도 보존한다.
        async with transaction() as db:
            await account_holdings.initialize(db, "u1")
            await db.execute("INSERT INTO account_holdings VALUES (?,?,?,?,?,?,?,?,?,?)",
                             ("u1", self.aid, NOV, "선물", -10, 100, "KRW", "KRW", "2026-11-01", "2026-11-01"))
            await account_holdings.rebuild("u1", NOV)
        await portfolio.set_portfolio_pair("u1", NOV, "00680K")
        await self.run_sync([])
        self.assertIsNone(await portfolio.get_portfolio_item("u1", NOV))
        await self.run_sync([DEC])
        self.assertEqual((await portfolio.get_portfolio_item("u1", DEC))["pair_long_code"], "00680K")
        await self.run_sync([JAN])
        self.assertEqual((await portfolio.get_portfolio_item("u1", JAN))["pair_long_code"], "00680K")

    async def test_manual_unlink_is_not_reversed_by_sync(self):
        await self.run_sync([DEC])
        await self.holding(JAN)
        with patch.object(portfolio_routes, "get_current_user", AsyncMock(return_value={"google_sub": "u1"})), \
             patch.object(futures_underlyings, "underlying_codes", AsyncMock(return_value={})):
            await portfolio_routes.update_portfolio_pair(DEC, None, {"long_code": None})
            await portfolio_routes.update_portfolio_pair(JAN, None, {"long_code": None})
        await self.run_sync([DEC, JAN])
        self.assertIsNone((await portfolio.get_portfolio_item("u1", DEC))["pair_long_code"])
        self.assertIsNone((await portfolio.get_portfolio_item("u1", JAN))["pair_long_code"])

    async def test_ambiguous_missing_master_positive_and_other_user_are_not_paired(self):
        await portfolio.save_portfolio_item("u1", "006805", "미래에셋우", 100, 100)
        await self.holding(DEC)
        await self.holding(JAN, 10)
        await portfolio.save_portfolio_item("u2", NOV, "선물", -10, 100)
        await portfolio.save_portfolio_item("u2", "006805", "우선주", 100, 100)
        await portfolio.set_portfolio_pair("u2", NOV, "006805")
        await futures_pairs.capture("u2", MAPPING)
        await futures_pairs.apply("u1", MAPPING)
        self.assertIsNone((await portfolio.get_portfolio_item("u1", DEC))["pair_long_code"])
        await portfolio.delete_portfolio_item("u1", "006805")
        await futures_pairs.apply("u1", {})
        self.assertIsNone((await portfolio.get_portfolio_item("u1", DEC))["pair_long_code"])
        await futures_pairs.apply("u1", MAPPING)
        self.assertEqual((await portfolio.get_portfolio_item("u1", DEC))["pair_long_code"], "00680K")
        self.assertIsNone((await portfolio.get_portfolio_item("u1", JAN))["pair_long_code"])
        db = await get_db()
        self.assertEqual((await (await db.execute("SELECT long_code FROM futures_pair_choices WHERE google_sub='u2'")).fetchone())[0], "006805")


def test_display_name_removes_only_trailing_multiplier():
    assert futures_contracts.display_name("미래에셋증 F 202612 (  10)") == "미래에셋증 F 202612"
    assert futures_contracts.display_name("한화 F 202612 (10)") == "한화 F 202612"
    assert futures_contracts.display_name("선물 (이름) 202612") == "선물 (이름) 202612"
