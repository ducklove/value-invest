from datetime import datetime
from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin, seed_user

from domain.timeutil import KST
from repositories import account_holdings, accounts, brokers, portfolio
from repositories.broker_secrets import BrokerError
from routes import broker_accounts
from services.brokers import derivatives, futures_contracts, namuh, sync
from services.portfolio import quote_service


class NamuhProductsTests(TempDbMixin):
    async def seed(self):
        futures_contracts._expiries.clear()
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
    def domestic(pnl=30, equity=1030, *, night=False):
        page = {"Output_0": [
            {"iem_cd": "KA486B000", "iem_nm": "미래에셋증 F 202611 (  10)", "sby_dit_nm": "매수",
             "bf_dd_ny_stl_qty": 3, "bnc_ind_qty": -1, "lqd_pbl_qty": 1,
             "avg_pr": 350, "now_pr": 351, "eal_pls_amt": 20},
            {"iem_cd": "KAC96B000", "iem_nm": "한화 F 202611 (  10)", "sby_dit_nm": "매도",
             "bf_dd_ny_stl_qty": 0, "bnc_ind_qty": 1, "lqd_pbl_qty": 0,
             "avg_pr": 352, "now_pr": 351, "eal_pls_amt": 10}],
            "Output_1": {"nas_tal": equity, "tot_eal_pls": pnl, "dsg_csh": 1000, "dsg_sba_amt": 0}}
        if night:
            for row, qty in zip(page["Output_0"], (2, 1)):
                row["fno_iem_cd"] = row.pop("iem_cd")
                row.pop("bf_dd_ny_stl_qty")
                row.pop("bnc_ind_qty")
                row["tdy_ny_stl_qty"] = qty
        return [page]

    @staticmethod
    def domestic_api(balance):
        async def pages(_user, _cid, path, body, _env):
            if "/quote/" in path:
                return [{"Output_0": {"iem_cd": body["iem_cd"].removeprefix("K"), "last_tr_date": "20261112"}}]
            return balance
        return AsyncMock(side_effect=pages)

    async def test_gold_uses_dedicated_balance_without_stock_listing_filter(self):
        page = {"Output_0": {"dca": 1000, "nxt_dd_dca": 900, "nxt2_dd_dca": 800, "drn_pbl_amt": 700},
                "Output_1": [{"iem_cd": "M04020000", "iem_nm": "금 99.99K", "itg_bnc_qty": 12, "rsdl_qty": 12, "phs_pr": 120000}]}
        with self.owned(), patch.object(namuh, "pages", AsyncMock(return_value=[page])) as api:
            rows, _ = await sync.fetch_snapshot("u1", self.link_data("gold"))
        api.assert_awaited_once_with("u1", self.cid, "/krgold/inquiry/v1/goldDepositAndBalance", {"act_no": "12345678901"}, "live")
        self.assertEqual({row["stock_code"]: row["quantity"] for row in rows}, {"KRX_GOLD": 12, "CASH_KRW": 800})
        self.assertEqual(rows[0]["avg_price"], 120000)

    async def test_gold_live_response_without_withdrawable_cash_still_imports_d2_and_grams(self):
        # 운영 응답은 문서와 달리 출금가능금액을 생략한다. 주문가능액으로 대체하지 않는다.
        page = {"rsp_cd": "00166", "Output_0": {
            "dca": 1000, "nxt_dd_dca": 900, "nxt2_dd_dca": 800, "orr_pbl_amt4": 700},
            "Output_1": [{"iem_cd": "M04020000", "iem_nm": "금 99.99K", "itg_bnc_qty": 12.0, "rsdl_qty": 12.0, "phs_pr": 120000}]}
        await self.link("gold")
        with self.owned(), patch.object(namuh, "pages", AsyncMock(return_value=[page])):
            result = await sync.sync_account("u1", self.aid)
        positions = {r["stock_code"]: r for r in await account_holdings.list_positions("u1", self.aid)}
        self.assertEqual(positions["KRX_GOLD"]["quantity"], 12)
        self.assertEqual(positions["KRX_GOLD"]["avg_price"], 120000)
        self.assertEqual(positions["CASH_KRW"]["quantity"], 800)
        self.assertNotIn("drn_pbl_amt", result["balances"]["KRW"])
        del page["Output_0"]["nxt2_dd_dca"]
        with self.owned(), patch.object(namuh, "pages", AsyncMock(return_value=[page])):
            with self.assertRaises(BrokerError):
                await sync.sync_account("u1", self.aid)
        self.assertEqual({r["stock_code"]: r for r in await account_holdings.list_positions("u1", self.aid)}, positions)

    async def test_gold_uses_execution_quantity_without_adding_unsettled_twice(self):
        page = {"Output_0": {"dca": 1000, "nxt_dd_dca": 900, "nxt2_dd_dca": 800},
                "Output_1": [{"iem_cd": "M04020000", "itg_bnc_qty": 12, "ny_stl_qty": -3,
                              "rsdl_qty": 9, "phs_pr": 120000}]}
        with self.owned(), patch.object(namuh, "pages", AsyncMock(return_value=[page])):
            rows, _ = await sync.fetch_snapshot("u1", self.link_data("gold"))
        self.assertEqual({row["stock_code"]: row["quantity"] for row in rows}, {"KRX_GOLD": 9, "CASH_KRW": 800})

    async def test_domestic_signed_share_holdings_and_adjusted_cash_persist_after_disconnect(self):
        await self.link("krfuture")
        for hour, suffix in ((10, "balance"), (20, "nightBalance"), (3, "nightBalance")):
            with self.subTest(hour=hour), self.owned(), \
                 patch.object(derivatives, "now_kst", return_value=datetime(2026, 9, 18, hour, tzinfo=KST)), \
                 patch.object(namuh, "pages", self.domestic_api(self.domestic(night=hour != 10))) as api:
                await sync.sync_account("u1", self.aid)
            body = {"act_no": "12345678901"}
            if hour != 10:
                body.update({"ost_dit_cd": "9", "ost_dit_cd1": "1"})
            api.assert_any_await("u1", self.cid, "/krfuture/inquiry/v1/" + suffix, body, "live")
            item = next(a for a in await accounts.list_accounts("u1") if a["account_id"] == self.aid)
            self.assertEqual([p["quantity"] for p in item["broker_snapshot"]["positions"]], [2, 1])
            self.assertEqual([p["code"] for p in item["broker_snapshot"]["positions"]], ["KA486B000", "KAC96B000"])
        positions = await account_holdings.list_positions("u1", self.aid)
        self.assertEqual({p["stock_code"]: p["quantity"] for p in positions}, {"KRFUT_KA486B000": 20, "KRFUT_KAC96B000": -10, "CASH_KRW": -2480})
        values = [p["quantity"] * (await quote_service.fetch_quote(p["stock_code"]))["price"] for p in positions]
        self.assertEqual(sum(values), 1030)
        from services.portfolio import nav_snapshot as snapshot_nav
        with patch.object(snapshot_nav.asyncio, "sleep", AsyncMock()):
            total_value, invested, stocks = await snapshot_nav._fetch_total_value("u1", "2026-09-18")
        self.assertEqual((total_value, invested), (1030, 1000))
        self.assertEqual(sum(row["market_value"] for row in stocks), 1030)
        item = next(a for a in await accounts.list_accounts("u1") if a["account_id"] == self.aid)
        self.assertEqual(item["connection"]["product"], "krfuture")
        self.assertEqual([p["side"] for p in item["broker_snapshot"]["positions"]], ["매수", "매도"])
        self.assertEqual([p["quantity"] for p in item["broker_snapshot"]["positions"]], [2, 1])
        self.assertEqual(await accounts.list_accounts("another-user"), [])
        await brokers.disconnect("u1", self.aid)
        detached = next(a for a in await accounts.list_accounts("u1") if a["account_id"] == self.aid)
        self.assertIsNone(detached["broker"])
        self.assertEqual(detached["broker_snapshot"], item["broker_snapshot"])

    async def test_domestic_day_preview_reads_all_pages_and_does_not_link_or_write_holdings(self):
        first, = self.domestic()
        second = {"Output_0": [first["Output_0"].pop()], "Output_1": first.pop("Output_1")}
        # 전량 청산된 행은 가격·코드가 생략되어도 보유 계약으로 가져오지 않는다.
        first["Output_0"].append({"bf_dd_ny_stl_qty": 3, "bnc_ind_qty": -3})
        before = await account_holdings.list_positions("u1")
        with self.owned(), patch.object(broker_accounts, "user_id", AsyncMock(return_value="u1")), \
             patch.object(derivatives, "now_kst", return_value=datetime(2026, 10, 6, 10, tzinfo=KST)), \
             patch.object(namuh, "pages", self.domestic_api([first, second])) as api:
            registered = await broker_accounts.register_broker("namuh", None, {
                "app_key": "test-products-key", "app_secret": "test-products-secret"})
            result = await broker_accounts.preview(self.aid, "namuh", None, {
                "selection": registered["accounts"][0]["selection"], "product": "krfuture"})
        api.assert_any_await("u1", self.cid, "/krfuture/inquiry/v1/balance", {"act_no": "12345678901"}, "live")
        snapshot = result["broker_snapshot"]
        self.assertEqual([(p["side"], p["quantity"]) for p in snapshot["positions"]], [("매수", 2), ("매도", 1)])
        self.assertEqual(snapshot["equity"], 1030)
        self.assertEqual(snapshot["pnl"], 30)
        self.assertEqual(sum(r["quantity"] * (1 if r["stock_code"] == "CASH_KRW" else 351) for r in result["items"]), 1030)
        self.assertEqual(await account_holdings.list_positions("u1"), before)
        account = next(a for a in await accounts.list_accounts("u1") if a["account_id"] == self.aid)
        self.assertIsNone(account["broker"])
        self.assertFalse(account["broker_snapshot"])

    async def test_invalid_domestic_contract_fields_preserve_entire_snapshot(self):
        await self.link("krfuture")
        day = datetime(2026, 10, 6, 10, tzinfo=KST)
        with self.owned(), patch.object(derivatives, "now_kst", return_value=day), \
             patch.object(namuh, "pages", self.domestic_api(self.domestic())):
            await sync.sync_account("u1", self.aid)
        before = await account_holdings.list_positions("u1", self.aid)
        snapshot = next(a for a in await accounts.list_accounts("u1") if a["account_id"] == self.aid)["broker_snapshot"]
        cases = [
            (False, "bf_dd_ny_stl_qty", None), (False, "bf_dd_ny_stl_qty", -1),
            (False, "bnc_ind_qty", None), (False, "bnc_ind_qty", ""),
            (False, "bnc_ind_qty", "NaN"), (False, "bnc_ind_qty", True),
            (False, "bnc_ind_qty", -4), (False, "bnc_ind_qty", 0.5),
            (True, "tdy_ny_stl_qty", None), (True, "tdy_ny_stl_qty", -1),
            (True, "tdy_ny_stl_qty", 0.5), (True, "fno_iem_cd", ""),
        ]
        for night, key, value in cases:
            pages = self.domestic(night=night)
            if value is None:
                del pages[0]["Output_0"][0][key]
            else:
                pages[0]["Output_0"][0][key] = value
            with self.subTest(night=night, field=key, value=value), self.owned(), \
                 patch.object(derivatives, "now_kst", return_value=day.replace(hour=20) if night else day), \
                 patch.object(namuh, "pages", self.domestic_api(pages)):
                with self.assertRaises(BrokerError):
                    await sync.sync_account("u1", self.aid)
            self.assertEqual(await account_holdings.list_positions("u1", self.aid), before)
            after = next(a for a in await accounts.list_accounts("u1") if a["account_id"] == self.aid)
            self.assertEqual(after["broker_snapshot"], snapshot)

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
             patch.object(derivatives, "now_kst", return_value=datetime(2026, 9, 18, 10, tzinfo=KST)):
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
        with self.owned(), patch.object(derivatives, "now_kst", return_value=datetime(2026, 10, 6, 10, tzinfo=KST)), \
             patch.object(namuh, "pages", self.domestic_api(self.domestic())):
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
        with self.owned(), patch.object(derivatives, "now_kst", return_value=datetime(2026, 10, 6, 10, tzinfo=KST)), \
             patch.object(namuh, "pages", self.domestic_api(self.domestic(pnl=-100, equity=0))):
            await sync.sync_account("u1", self.aid)
        other = (await accounts.create_account("u1", name="두 번째 선물"))["account_id"]
        await brokers.link_account("u1", other, self.cid, "22222222222", "live", product="krfuture")
        with patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "22222222222", "environment": "live"}])), \
             patch.object(derivatives, "now_kst", return_value=datetime(2026, 10, 6, 10, tzinfo=KST)), \
             patch.object(namuh, "pages", self.domestic_api(self.domestic(pnl=100, equity=1000))):
            await sync.sync_account("u1", other)
        total = await portfolio.get_portfolio("u1")
        self.assertEqual(sum(r["quantity"] * (1 if r["stock_code"] == "CASH_KRW" else 351) for r in total), 1000)
        self.assertFalse(any(r["stock_code"].startswith("FUTURES_") for r in total))
        self.assertTrue(all(r["group_name"] == "기타" for r in total))

    async def test_future_and_gold_order_paths_remain_forbidden(self):
        for path in ("/krgold/order/v1/goldBuy", "/krfuture/order/v1/order", "/gbfuture/order/v1/buy"):
            with self.subTest(path=path), self.assertRaises(BrokerError):
                await namuh.pages("u1", self.cid, path, {})


    async def test_four_stock_futures_shorts_replace_value_rows_and_preserve_memo_and_quotes(self):
        from repositories import broker_quotes
        from repositories.db import transaction
        from services import stock_quotes
        await self.link("krfuture")
        # 이전 배포의 국내선물 합산 항목을 동기화 한 번으로 교체한다.
        async with transaction() as db:
            for row in derivatives.valuation_rows(900, -100):
                await db.execute("INSERT INTO account_holdings VALUES (?,?,?,?,?,?,?,?,?,?)", (
                    "u1", self.aid, row["stock_code"], row["stock_name"], row["quantity"], row["avg_price"],
                    "KRW", "KRW", "2026-10-06", "2026-10-06"))
                await account_holdings.rebuild("u1", row["stock_code"])
        page = {"Output_0": [], "Output_1": {"nas_tal": 10000000, "tot_eal_pls": 440000}}
        for code, name, qty, avg, current in [
            ("KA486B000", "미래에셋증", 400, 33200, 32650),
            ("KAC96B000", "한화", 100, 128800, 128600),
            ("KAG56B000", "한진칼", 40, 132550, 133200),
            ("KAGT6B000", "두산", 10, 1456000, 1473000),
        ]:
            page["Output_0"].append({"iem_cd": code, "iem_nm": name + " F 202611 (  10)",
                "sby_dit_nm": "매도", "bf_dd_ny_stl_qty": 0, "bnc_ind_qty": qty,
                "avg_pr": avg, "now_pr": current, "eal_pls_amt": qty * 10 * (avg-current)})
        with self.owned(), patch.object(derivatives, "now_kst", return_value=datetime(2026, 10, 6, 10, tzinfo=KST)), \
             patch.object(namuh, "pages", self.domestic_api([page])):
            await sync.sync_account("u1", self.aid)
        items = {r["stock_code"]: r for r in await portfolio.get_portfolio("u1", self.aid)}
        shorts = [r for code, r in items.items() if code.startswith("KRFUT_")]
        self.assertEqual(sorted(r["quantity"] for r in shorts), [-4000, -1000, -400, -100])
        self.assertTrue(all(r["memo"] == "만기일 2026-11-12" for r in shorts))
        self.assertFalse(any(code.startswith("FUTURES_") for code in items))
        self.assertEqual(items["CASH_KRW"]["quantity"], 469780000)
        self.assertEqual(sum(items["KRFUT_"+r["iem_cd"]]["quantity"] * r["now_pr"] for r in page["Output_0"])
                         + items["CASH_KRW"]["quantity"], 10000000)
        self.assertEqual(sum(r["quantity"] * r["avg_price"] for r in items.values()), 9560000)
        # 모든 메모 동기화에서 사용자 메모·공통 분류·등록일을 보존한다.
        code = shorts[0]["stock_code"]
        await portfolio._save_portfolio_projection("u1", code, shorts[0]["stock_name"], shorts[0]["quantity"], shorts[0]["avg_price"], memo="만기일 2026-11-12\n헤지 목적")
        with self.owned(), patch.object(derivatives, "now_kst", return_value=datetime(2026, 10, 6, 10, tzinfo=KST)), \
             patch.object(namuh, "pages", self.domestic_api([page])):
            await sync.sync_account("u1", self.aid)
        self.assertEqual(next(r for r in await portfolio.get_portfolio("u1") if r["stock_code"] == code)["memo"],
                         "만기일 2026-11-12\n헤지 목적")
        # 프로세스 메모리 없이도 가격 복구. 해외 시세 서비스로 보내지 않는다.
        stock_quotes._stock_cache.clear()
        stock_quotes._last_known.clear()
        stock_quotes._dead_stock_cache.clear()
        for row in shorts:
            q = await broker_quotes.futures_quote(row["stock_code"])
            self.assertEqual(q["source"], "namuh_balance")
            with patch.object(quote_service.foreign, "fetch_foreign_quote", AsyncMock()) as foreign:
                self.assertEqual((await quote_service.fetch_quote(row["stock_code"]))["price"], q["price"])
            foreign.assert_not_awaited()
        # 가격과 예수금이 같은 스냅샷으로 바뀐다. 0 계약은 제거한다.
        page["Output_0"][0].update({"now_pr": 33000, "eal_pls_amt": 800000})
        page["Output_0"][1]["bnc_ind_qty"] = 0
        page["Output_1"].update({"nas_tal": 10000000, "tot_eal_pls": 0})
        with self.owned(), patch.object(derivatives, "now_kst", return_value=datetime(2026, 10, 6, 10, tzinfo=KST)), \
             patch.object(namuh, "pages", self.domestic_api([page])):
            await sync.sync_account("u1", self.aid)
        items = await portfolio.get_portfolio("u1", self.aid)
        self.assertNotIn("KRFUT_KAC96B000", [r["stock_code"] for r in items])
        values = [r["quantity"] * (await quote_service.fetch_quote(r["stock_code"]))["price"] for r in items]
        self.assertEqual(sum(values), 10000000)

    async def test_invalid_expiry_or_multiplier_preserves_holdings(self):
        await self.link("krfuture")
        day = datetime(2026, 10, 6, 10, tzinfo=KST)
        with self.owned(), patch.object(derivatives, "now_kst", return_value=day), \
             patch.object(namuh, "pages", self.domestic_api(self.domestic())):
            await sync.sync_account("u1", self.aid)
        before = await account_holdings.list_positions("u1", self.aid)
        for field, value in [("last_tr_date", "20260230"), ("last_tr_date", ""), ("iem_cd", "ACXX0000")]:
            futures_contracts._expiries.clear()
            async def pages(_user, _cid, path, body, _env):
                if "/quote/" not in path:
                    return self.domestic()
                quote = {"iem_cd": body["iem_cd"].removeprefix("K"), "last_tr_date": "20261112"}
                quote[field] = value
                return [{"Output_0": quote}]
            with self.subTest(field=field, value=value), self.owned(), patch.object(derivatives, "now_kst", return_value=day), \
                 patch.object(namuh, "pages", side_effect=pages), self.assertRaises(BrokerError):
                await sync.sync_account("u1", self.aid)
            self.assertEqual(await account_holdings.list_positions("u1", self.aid), before)
        pages = self.domestic()
        pages[0]["Output_0"][0]["iem_nm"] = "선물 단위 미제공"
        with self.owned(), patch.object(derivatives, "now_kst", return_value=day), \
             patch.object(namuh, "pages", self.domestic_api(pages)), self.assertRaises(BrokerError):
            await sync.sync_account("u1", self.aid)
        self.assertEqual(await account_holdings.list_positions("u1", self.aid), before)
