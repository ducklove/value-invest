from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin, seed_user
from fastapi import HTTPException
from starlette.requests import Request

from repositories import account_holdings, accounts, brokers, portfolio
from routes import portfolio as route
from services.brokers import sync


class PortfolioMetadataTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await seed_user("u2", "other@example.com")
        await portfolio.save_portfolio_item("u1", "000660", "SK하이닉스", 2, 100)
        await portfolio.get_portfolio_groups("u1")
        self.aid = (await accounts.create_account("u1", name="NH"))["account_id"]
        cid = await brokers.store_credential("u1", "metadata-key", "metadata-secret")
        await brokers.link_account("u1", self.aid, cid, "12345678901", "live", False)
        self.rows = [
            dict(stock_code="005930", stock_name="삼성전자", quantity=3, avg_price=80000,
                 avg_price_currency="KRW", currency="KRW"),
            dict(stock_code="CASH_KRW", stock_name="원화", quantity=5000, avg_price=1,
                 avg_price_currency="KRW", currency="KRW"),
        ]
        await self.resync()
        auth = patch.object(route, "get_current_user", AsyncMock(return_value={"google_sub": "u1"}))
        auth.start()
        self.addCleanup(auth.stop)

    def request(self, aid=None):
        return Request({"type": "http", "headers": [(b"x-portfolio-account", aid.encode())] if aid else []})

    async def resync(self):
        with patch.object(sync, "fetch_snapshot", AsyncMock(return_value=(self.rows, {}))):
            await sync.sync_account("u1", self.aid)

    async def test_metadata_survives_sync_in_both_views_without_changing_ledger(self):
        before = await account_holdings.list_positions("u1")
        group = (await portfolio.get_portfolio_groups("u1"))[0]["group_name"]
        # 이름·메모 편집은 환율/시세 API가 멈춰도 저장한다.
        with patch.object(route.fx, "price_to_krw", AsyncMock(side_effect=AssertionError("환율 조회 금지"))):
            saved = await route.save_holding_metadata("005930", self.request(self.aid), {
                "stock_name": "내 삼성전자", "group_name": group, "target_price": 120000,
                "memo": "장기 보유", "created_at": "2020-02-20",
            })
        await portfolio.update_portfolio_benchmark("u1", "005930", "IDX_SP500")
        self.assertNotIn("quantity", saved)
        self.assertEqual(await account_holdings.list_positions("u1"), before)
        await portfolio.save_portfolio_order("u1", ["005930", "000660", "CASH_KRW"])
        self.rows[0].update(stock_name="증권사 새 종목명", quantity=5, avg_price=90000)
        await self.resync()
        for aid in (None, self.aid):
            item = next(x for x in await portfolio.get_portfolio("u1", aid) if x["stock_code"] == "005930")
            self.assertEqual(item["stock_name"], "내 삼성전자")
            self.assertEqual(item["quantity"], 5)
            self.assertEqual(item["avg_price"], 90000)
            self.assertEqual(item["group_name"], group)
            self.assertEqual(item["memo"], "장기 보유")
            self.assertEqual(item["target_price"], 120000)
            self.assertEqual(item["benchmark_code"], "IDX_SP500")
            self.assertEqual(item["created_at"], "2020-02-20T00:00:00")
            self.assertEqual(item["sort_order"], 0)
        self.assertEqual((await account_holdings.get_position("u1", "005930", self.aid))["stock_name"], "증권사 새 종목명")

    async def test_metadata_refuses_balance_fields_invalid_values_and_other_user(self):
        before = await account_holdings.list_positions("u1")
        for payload in ({"quantity": 1}, {"avg_price": 1}, {"currency": "USD"},
                        {"stock_name": " "}, {"stock_name": None}, {"stock_name": "가" * 81},
                        {"target_price": float("nan")}, {"target_price": True},
                        {"target_price_disabled": "false"}, {"created_at": "2026-02-30"},
                        {"memo": "가" * 501}, {"group_name": "없는 그룹"}):
            with self.subTest(payload=payload), self.assertRaises(HTTPException) as error:
                await route.save_holding_metadata("005930", self.request(self.aid), payload)
            self.assertEqual(error.exception.status_code, 400)
        with patch.object(route, "get_current_user", AsyncMock(return_value={"google_sub": "u2"})):
            with self.assertRaises(HTTPException) as error:
                await route.save_holding_metadata("005930", self.request(), {"memo": "변경"})
            self.assertEqual(error.exception.status_code, 404)
            with self.assertRaises(accounts.AccountError):
                await route.save_holding_metadata("005930", self.request(self.aid), {"memo": "변경"})
        with self.assertRaises(accounts.AccountError):
            await portfolio.save_portfolio_item("u1", "005930", "삼성전자", 1, 1, account_id=self.aid)
        with self.assertRaises(accounts.AccountError):
            await portfolio.delete_portfolio_item("u1", "005930", self.aid)
        self.assertEqual(await account_holdings.list_positions("u1"), before)

    async def test_target_formula_clear_and_memo_clear_are_metadata_only(self):
        before = await account_holdings.list_positions("u1")
        with patch.object(route, "_resolve_target_formula_price", AsyncMock(return_value=160000)) as resolve:
            saved = await route.save_holding_metadata("005930", self.request(self.aid), {
                "target_price_formula": "매입가*2", "memo": "설정",
            })
        resolve.assert_awaited_once_with("005930", "매입가*2", 80000)
        self.assertEqual(saved["target_price"], 160000)
        saved = await route.save_holding_metadata("005930", self.request(self.aid), {
            "target_price": None, "target_price_formula": None, "target_price_disabled": True, "memo": "",
        })
        self.assertTrue(saved["target_price_disabled"])
        self.assertIsNone(saved["target_price"])
        self.assertIsNone(saved["target_price_formula"])
        self.assertIsNone(saved["memo"])
        self.assertEqual(await account_holdings.list_positions("u1"), before)

    async def test_scoped_reorder_keeps_other_account_slots_and_survives_sync(self):
        await portfolio.save_portfolio_order("u1", ["005930", "000660", "CASH_KRW"])
        before = await account_holdings.list_positions("u1")
        result = await route.save_portfolio_order(self.request(self.aid), {"stock_codes": ["CASH_KRW", "005930"]})
        self.assertEqual(result["count"], 2)
        self.assertEqual([x["stock_code"] for x in await portfolio.get_portfolio("u1")], ["CASH_KRW", "000660", "005930"])
        self.assertEqual(await account_holdings.list_positions("u1"), before)
        await self.resync()
        self.assertEqual([x["stock_code"] for x in await portfolio.get_portfolio("u1", self.aid)], ["CASH_KRW", "005930"])
        for codes in (["005930"], ["CASH_KRW", "000660"], ["UNKNOWN", "005930"]):
            with self.subTest(codes=codes), self.assertRaises(HTTPException):
                await route.save_portfolio_order(self.request(self.aid), {"stock_codes": codes})
