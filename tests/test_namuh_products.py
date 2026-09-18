from datetime import datetime
from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin, seed_user

from repositories import account_holdings, accounts, brokers, portfolio
from repositories.broker_secrets import BrokerError
from services.brokers import derivatives, namuh, sync
from services.portfolio import quote_service


class NamuhProductsTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await account_holdings.ensure("u1")
        self.aid = (await accounts.create_account("u1", name="NH 전용계좌"))["account_id"]
        self.cid = await brokers.store_credential("u1", "test-products-key", "test-products-secret")

    def link_data(self, product):
        return {"credential_id": self.cid, "account_no": "12345678901", "environment": "live", "product": product}

    def owned(self):
        return patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "12345678901", "environment": "live"}]))

    async def link(self, product):
        await brokers.link_account("u1", self.aid, self.cid, "12345678901", "live", product=product)

    @staticmethod
    def domestic(pnl=-100, equity=900):
        return [{"Output_0": [
            {"iem_cd": "101V9000", "iem_nm": "코스피200 선물", "sby_dit_nm": "매수", "tdy_ny_stl_qty": 2,
             "avg_pr": 350, "now_pr": 351, "eal_pls_amt": 50},
            {"iem_cd": "101V9000", "iem_nm": "코스피200 선물", "sby_dit_nm": "매도", "tdy_ny_stl_qty": 1,
             "avg_pr": 352, "now_pr": 351, "eal_pls_amt": -150}],
            "Output_1": {"nas_tal": equity, "tot_eal_pls": pnl, "dsg_csh": 1000, "dsg_sba_amt": 0}}]

    async def test_gold_uses_dedicated_balance_without_stock_listing_filter(self):
        page = {"Output_0": {"dca": 1000, "nxt_dd_dca": 900, "nxt2_dd_dca": 800, "drn_pbl_amt": 700},
                "Output_1": [{"iem_cd": "M04020000", "iem_nm": "금 99.99K", "itg_bnc_qty": 12, "phs_pr": 120000}]}
        with self.owned(), patch.object(namuh, "pages", AsyncMock(return_value=[page])) as api:
            rows, _ = await sync.fetch_snapshot("u1", self.link_data("gold"))
        api.assert_awaited_once_with("u1", self.cid, "/krgold/inquiry/v1/goldDepositAndBalance", {"act_no": "12345678901"}, "live")
        self.assertEqual({row["stock_code"]: row["quantity"] for row in rows}, {"KRX_GOLD": 12, "CASH_KRW": 800})
        self.assertEqual(rows[0]["avg_price"], 120000)

    async def test_domestic_long_short_contracts_and_equity_are_separate_and_persist_after_disconnect(self):
        await self.link("krfuture")
        for hour, suffix in ((10, "balance"), (20, "nightBalance"), (3, "nightBalance")):
            with self.subTest(hour=hour), self.owned(), \
                 patch.object(derivatives, "now_kst", return_value=datetime(2026, 9, 18, hour, tzinfo=derivatives._KST)), \
                 patch.object(namuh, "pages", AsyncMock(return_value=self.domestic())) as api:
                await sync.sync_account("u1", self.aid)
            self.assertTrue(api.await_args.args[2].endswith("/" + suffix))
        positions = await account_holdings.list_positions("u1", self.aid)
        self.assertEqual({p["stock_code"]: p["quantity"] for p in positions}, {"FUTURES_BASE_KRW": 1000, "FUTURES_PNL_KRW": -100})
        values = [p["quantity"] * (await quote_service.fetch_quote(p["stock_code"]))["price"] for p in positions]
        self.assertEqual(sum(values), 900)
        import snapshot_nav
        with patch.object(snapshot_nav.asyncio, "sleep", AsyncMock()):
            total_value, invested, stocks = await snapshot_nav._fetch_total_value("u1", "2026-09-18")
        self.assertEqual((total_value, invested), (900, 1000))
        self.assertEqual(sum(row["market_value"] for row in stocks), 900)
        item = next(a for a in await accounts.list_accounts("u1") if a["account_id"] == self.aid)
        self.assertEqual(item["connection"]["product"], "krfuture")
        self.assertEqual([p["side"] for p in item["broker_snapshot"]["positions"]], ["매수", "매도"])
        self.assertEqual([p["quantity"] for p in item["broker_snapshot"]["positions"]], [2, 1])
        self.assertEqual(await accounts.list_accounts("another-user"), [])
        await brokers.disconnect("u1", self.aid)
        detached = next(a for a in await accounts.list_accounts("u1") if a["account_id"] == self.aid)
        self.assertIsNone(detached["broker"])
        self.assertEqual(detached["broker_snapshot"], item["broker_snapshot"])

    async def test_overseas_imports_both_directions_and_uses_broker_won_total_once(self):
        calls = []

        async def pages(_user, _cid, path, body, _env):
            calls.append((path, body))
            if path.endswith("/deposit"):
                return [{"Output_0": {"cur_cd": "TKR", "fdv_dsg_aet_tot_eal_amt": 1490, "fdv_ny_stl_eal_pls": 90,
                                       "fdv_dsg_amt": 1400, "fdv_wrw_pbl_amt": 1000}}]
            return [{"Output_0": {"fdv_brg_wtm": 800}, "Output_1": [
                {"iem_cd": "ESU26", "cur_cd": "USD", "byn_ny_stl_bnc_qty": 2, "sll_ny_stl_bnc_qty": 1},
                {"iem_cd": "HSIU26", "cur_cd": "HKD", "byn_ny_stl_bnc_qty": 0, "sll_ny_stl_bnc_qty": 3},
                {"iem_cd": "ORDER_ONLY", "cur_cd": "USD", "byn_ny_stl_bnc_qty": 0, "sll_ny_stl_bnc_qty": 0}]}]

        with self.owned(), patch.object(namuh, "pages", side_effect=pages), \
             patch.object(derivatives, "now_kst", return_value=datetime(2026, 9, 18, 10, tzinfo=derivatives._KST)):
            rows, balances = await sync.fetch_snapshot("u1", self.link_data("gbfuture"))
        self.assertEqual([body["cur_cd"] for _, body in calls], ["TKR", "KRW"])
        self.assertTrue(all(body["sls_dt"] == "20260918" for _, body in calls))
        self.assertEqual(sum(r["quantity"] for r in rows), 1490)
        self.assertEqual(sum(r["quantity"] * r["avg_price"] for r in rows), 1400)
        contracts = balances["_snapshot"]["positions"]
        self.assertEqual([(p["code"], p["side"], p["quantity"], p["currency"]) for p in contracts],
                         [("ESU26", "매수", 2, "USD"), ("ESU26", "매도", 1, "USD"), ("HSIU26", "매도", 3, "HKD")])
        self.assertTrue(all(p["average_price"] is None and p["pnl"] is None for p in contracts))

    async def test_failed_futures_parse_preserves_entire_snapshot(self):
        await self.link("krfuture")
        with self.owned(), patch.object(namuh, "pages", AsyncMock(return_value=self.domestic())):
            await sync.sync_account("u1", self.aid)
        before = await account_holdings.list_positions("u1", self.aid)
        old_snapshot = next(a for a in await accounts.list_accounts("u1") if a["account_id"] == self.aid)["broker_snapshot"]
        for page in ({}, {"Output_1": {"nas_tal": "NaN", "tot_eal_pls": 0}},
                     {"Output_1": {"nas_tal": 1000, "tot_eal_pls": -10}}):
            with self.subTest(page=page), self.owned(), patch.object(namuh, "pages", AsyncMock(return_value=[page])):
                with self.assertRaises(BrokerError):
                    await sync.sync_account("u1", self.aid)
            self.assertEqual(await account_holdings.list_positions("u1", self.aid), before)
            after = next(a for a in await accounts.list_accounts("u1") if a["account_id"] == self.aid)
            self.assertEqual(after["broker_snapshot"], old_snapshot)

    async def test_opposite_account_pnl_and_zero_equity_aggregate_without_stock_direction_conflict(self):
        await self.link("krfuture")
        with self.owned(), patch.object(namuh, "pages", AsyncMock(return_value=self.domestic(pnl=-100, equity=0))):
            await sync.sync_account("u1", self.aid)
        other = (await accounts.create_account("u1", name="두 번째 선물"))["account_id"]
        await brokers.link_account("u1", other, self.cid, "22222222222", "live", product="krfuture")
        with patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "22222222222", "environment": "live"}])), \
             patch.object(namuh, "pages", AsyncMock(return_value=self.domestic(pnl=100, equity=1000))):
            await sync.sync_account("u1", other)
        total = await portfolio.get_portfolio("u1")
        self.assertEqual(sum(r["quantity"] for r in total), 1000)
        self.assertEqual(next(r["quantity"] for r in total if r["stock_code"] == "FUTURES_PNL_KRW"), 0)
        self.assertTrue(all(r["group_name"] == "기타" for r in total))

    async def test_future_and_gold_order_paths_remain_forbidden(self):
        for path in ("/krgold/order/v1/goldBuy", "/krfuture/order/v1/order", "/gbfuture/order/v1/buy"):
            with self.subTest(path=path), self.assertRaises(BrokerError):
                await namuh.pages("u1", self.cid, path, {})
