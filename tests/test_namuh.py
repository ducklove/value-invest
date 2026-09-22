import asyncio
import json
import logging
from datetime import datetime
from unittest.mock import AsyncMock, patch

import httpx
from _harness import TempDbMixin, seed_user

from repositories import account_holdings, accounts, bootstrap, brokers, portfolio, portfolio_order, snapshots
from repositories.broker_secrets import BrokerError
from repositories.db import get_db
from services.brokers import namuh, namuh_listing, overseas_realtime, realtime, sync


class NamuhTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await seed_user("u2", "second@example.com")
        await account_holdings.ensure("u1")
        self.aid = (await accounts.create_account("u1", name="NH"))["account_id"]
        self.cid = await brokers.store_credential("u1", "test-namuh-app-key", "test-namuh-secret")
        namuh._locks.clear()
        namuh._last_call.clear()
        listing = patch.object(namuh_listing, "listed_codes", AsyncMock(return_value={"005930", "000660", "02826K", "03473K", "0203K0"}))
        self.listing = listing.start()
        self.addCleanup(listing.stop)

    async def link(self):
        await brokers.link_account("u1", self.aid, self.cid, "12345678901", "live", False)

    async def test_secrets_encrypted_and_user_scope(self):
        db = await get_db()
        row = dict(await (await db.execute("SELECT * FROM broker_credentials")).fetchone())
        self.assertNotIn("test-namuh-app-key", json.dumps(row))
        self.assertNotIn("test-namuh-secret", json.dumps(row))
        with self.assertRaises(BrokerError):
            await brokers.get_credential("u2", self.cid)
        with self.assertRaises(BrokerError):
            await brokers.store_credential("u2", "test-namuh-app-key", "test-namuh-secret")
        await self.link()
        linked = next(row for row in await accounts.list_accounts("u1") if row["account_id"] == self.aid)
        self.assertEqual(linked["connection"]["account_no"], "12345678901")
        self.assertNotIn("account_ciphertext", linked["connection"])
        self.assertEqual(await accounts.list_accounts("u2"), [])
        stored = dict(await (await db.execute("SELECT * FROM broker_account_links")).fetchone())
        self.assertNotIn("12345678901", json.dumps(stored))

    async def test_token_single_flight_persistent_cache_and_log_redaction(self):
        calls = []
        def respond(request):
            calls.append(request.url.path)
            return httpx.Response(200, json={"access_token": "private-token", "expires_in": 86400})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            with patch.object(namuh, "get_http_client", AsyncMock(return_value=client)):
                tokens = await asyncio.gather(*(namuh.token("u1", self.cid) for _ in range(4)))
        self.assertEqual(tokens, ["private-token"] * 4)
        self.assertEqual(calls, ["/oauth2/token"])
        record = logging.LogRecord("httpx", 20, "", 1, 'POST https://api.nhplug.com:8443/oauth2/token?appkey=PRIVATE&appsecretkey=SECRET "200"', (), None)
        namuh._HideTokenQuery().filter(record)
        self.assertNotIn("PRIVATE", record.getMessage())

    async def test_sync_replaces_only_linked_account_and_failure_preserves_snapshot(self):
        await portfolio.save_portfolio_item("u1", "005930", "삼성전자", 10, 100)
        await self.link()
        rows = [{"stock_code": "005930", "stock_name": "삼성전자", "quantity": 20, "avg_price": 200, "avg_price_currency": "KRW", "currency": "KRW"}]
        with patch.object(sync, "fetch_snapshot", AsyncMock(return_value=(rows, {"KRW": {"dca": 1000}}))):
            await sync.sync_account("u1", self.aid)
            await sync.sync_account("u1", self.aid)
        self.assertEqual((await portfolio.get_portfolio_item("u1", "005930"))["quantity"], 30)
        with patch.object(sync, "fetch_snapshot", AsyncMock(side_effect=BrokerError("조회 실패"))):
            with self.assertRaises(BrokerError):
                await sync.sync_account("u1", self.aid)
        self.assertEqual((await portfolio.get_portfolio_item("u1", "005930"))["quantity"], 30)
        with self.assertRaises(accounts.AccountError):
            await portfolio.save_portfolio_item("u1", "005930", "삼성전자", 1, 2, account_id=self.aid)
        await brokers.disconnect("u1", self.aid)
        self.assertEqual((await portfolio.get_portfolio_item("u1", "005930"))["quantity"], 30)

    async def test_no_order_paths_and_pagination_failure(self):
        with self.assertRaises(BrokerError):
            await namuh.pages("u1", self.cid, "/krstock/order/v1/cashBuy", {})
        with self.assertRaises(BrokerError):
            namuh.continuation({}, {"cts_flag": "Y"})
        self.assertEqual(namuh.continuation({"Output_0": {"ctsz20": "next"}}, {}), "next")

    async def test_rmb_currency_backfill_allows_sync_and_preserves_original_cost(self):
        await portfolio.save_portfolio_item("u1", "83188.HK", "위안화 ETF", 2, 100000, "HKD", avg_price_currency="KRW")
        await portfolio.save_portfolio_item("u1", "83199.HK", "위안화 채권 ETF", 3, 150, "HKD", avg_price_currency="HKD")
        await portfolio.save_portfolio_item("u1", "08388.HK", "홍콩달러 종목", 1, 20, "HKD", avg_price_currency="HKD")
        before = await account_holdings.list_positions("u1")
        await self.link()
        rows = [{"stock_code": "83188.HK", "stock_name": "위안화 ETF", "quantity": 4,
                 "avg_price": 50, "avg_price_currency": "CNY", "currency": "CNY"}]
        with patch.object(sync, "fetch_snapshot", AsyncMock(return_value=(rows, {}))):
            with self.assertRaisesRegex(BrokerError, r"83188.HK.*HKD.*CNY"):
                await sync.sync_account("u1", self.aid)
            self.assertEqual(await account_holdings.list_positions("u1"), before)
            await bootstrap.init_db()
            corrected = await account_holdings.list_positions("u1")
            for old, new in zip(before, corrected):
                self.assertEqual(new, {**old, "currency": "CNY" if old["stock_code"] in {"83188.HK", "83199.HK"} else "HKD"})
            await bootstrap.init_db()
            self.assertEqual(await account_holdings.list_positions("u1"), corrected)
            await sync.sync_account("u1", self.aid)
            await sync.sync_account("u1", self.aid)
        item = next(row for row in await portfolio.get_portfolio("u1") if row["stock_code"] == "83188.HK")
        self.assertEqual((item["currency"], item["quantity"]), ("CNY", 6))
        from services.portfolio import fx
        with patch.object(fx, "fx_rate_for_currency", AsyncMock(return_value=200)):
            await fx.annotate_avg_price_krw([item])
        self.assertAlmostEqual(item["avg_price_krw"] * item["quantity"], 240000)
        self.assertIsNone((await brokers.get_link("u1", self.aid))["sync_error"])

    async def test_actual_currency_conflict_still_preserves_entire_account(self):
        await portfolio.save_portfolio_item("u1", "AAPL", "Apple", 1, 100, "EUR")
        await self.link()
        before = await account_holdings.list_positions("u1")
        rows = [{"stock_code": "AAPL", "stock_name": "Apple", "quantity": 2,
                 "avg_price": 200, "avg_price_currency": "USD", "currency": "USD"}]
        await bootstrap.init_db()
        with patch.object(sync, "fetch_snapshot", AsyncMock(return_value=(rows, {}))):
            with self.assertRaisesRegex(BrokerError, "AAPL.*EUR.*USD"):
                await sync.sync_account("u1", self.aid)
        self.assertEqual(await account_holdings.list_positions("u1"), before)

    async def test_mock_account_reads_market_data_from_live_but_balance_from_mock(self):
        requests = []

        def respond(request):
            requests.append(request)
            return httpx.Response(200, json={"rsp_cd": "00000", "Output_0": {}})

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            with patch.object(namuh, "get_http_client", AsyncMock(return_value=client)), \
                 patch.object(namuh, "token", AsyncMock(return_value="test-only")), \
                 patch.object(namuh, "MIN_CALL_INTERVAL", 0):
                for path in ("/krfuture/quote/v1/day", "/krstock/quote/v1/currentPrice", "/krstock/inquiry/v1/balance"):
                    await namuh.pages("u1", self.cid, path, {}, "mock")
                with self.assertRaises(BrokerError):
                    await namuh.pages("u1", self.cid, "/krfuture/order/v1/order", {}, "mock")
        self.assertEqual([r.url.host for r in requests], ["api.nhplug.com", "api.nhplug.com", "moapi.nhplug.com"])
        self.assertTrue(all("/order/" not in r.url.path for r in requests))

    async def test_balance_parser_distinguishes_settlement_cash_and_combines_pages(self):
        summary = {key: str(value) for key, value in zip(("dca", "nxt_dd_dca", "nxt2_dd_dca", "orr_pbl_amt", "drn_pbl_amt"), (1000, 900, 800, 700, 600))}
        domestic = [{"Output_0": summary, "Output_1": [{"iem_cd": "KR7005930003", "iem_nm": "삼성전자", "itg_bnc_qty": "10", "rsdl_qty": "10", "phs_pr": "100"}]},
                    {"Output_1": [{"iem_cd": "A000660", "iem_nm": "SK하이닉스", "itg_bnc_qty": "2", "rsdl_qty": "2", "phs_pr": "200"}]}]
        with patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "12345678901", "environment": "live"}])), \
             patch.object(namuh, "pages", AsyncMock(side_effect=[domestic, [{"Output_0": []}]])):
            rows, balances = await sync.fetch_snapshot("u1", {"credential_id": self.cid, "account_no": "12345678901", "environment": "live", "include_overseas": False})
        self.assertEqual({r["stock_code"]: r["quantity"] for r in rows}, {"005930": 10, "000660": 2, "CASH_KRW": 800})
        self.assertEqual(balances["KRW"]["drn_pbl_amt"], 600)
        with self.assertRaises(BrokerError):
            sync.summary({"Output_0": {}})

    async def test_mmw_combines_with_settlement_cash_once_and_preserves_account_settings(self):
        await portfolio.save_portfolio_item("u1", "CASH_KRW", "수동 원화", 500, 1)
        await self.link()
        total = {"dca": 0, "nxt_dd_dca": 65593681, "nxt2_dd_dca": 43906154}
        first = {"iem_cd": "MMW1003", "iem_nm": "한국증권금융 예치", "rsdl_qty": 21499426, "eal_amt": 21499426}
        second = {**first, "rsdl_qty": 20200000, "eal_amt": 20200000}
        domestic = [{"Output_0": total, "Output_1": [
            {"iem_cd": "", "iem_nm": "", "rsdl_qty": 0, "eal_amt": 0, "ny_stl_qty": -2},
            {"iem_cd": "000660", "iem_nm": "SK하이닉스", "rsdl_qty": 80, "phs_pr": 100}, first,
        ]}, {"Output_1": [second]}]

        async def pages(_user, _cid, path, _body, _env):
            if path == "/krstock/inquiry/v1/balance":
                return domestic
            self.assertEqual(path, "/gbstock/inquiry/v1/margin")
            return [{"Output_0": [{"cur_cd": "USD", "fc_dca": 100, "stl_af_fc_dca": 80, "fc_drn_pbl_amt": 50}]}]

        with patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "12345678901", "environment": "live"}])), \
             patch.object(namuh, "pages", side_effect=pages):
            result = await sync.sync_account("u1", self.aid)
            self.assertEqual(result["balances"]["KRW"]["nxt2_dd_dca"], 43906154)
            self.assertEqual(result["balances"]["KRW"]["mmw_eal_amt"], 41699426)
            order = ["CASH_USD", "CASH_KRW", "000660"]
            await portfolio_order.save("u1", self.aid, order)
            for _ in range(2):
                await sync.sync_account("u1", self.aid)
            positions = {row["stock_code"]: row["quantity"] for row in await account_holdings.list_positions("u1", self.aid)}
            self.assertEqual(positions, {"000660": 80, "CASH_KRW": 85605580, "CASH_USD": 80})
            self.assertEqual((await portfolio.get_portfolio_item("u1", "CASH_KRW"))["quantity"], 85606080)
            self.assertEqual([row["stock_code"] for row in await portfolio.get_portfolio("u1", self.aid)], order)
            # MMW 인출로 현금만 늘어도 합계는 유지한다. 누적 가산/별도 입출금 생성은 없다.
            total["nxt2_dd_dca"] += first["eal_amt"] + second["eal_amt"]
            first.update(rsdl_qty=0, eal_amt=0)
            second.update(rsdl_qty=0, eal_amt=0)
            await sync.sync_account("u1", self.aid)
            self.assertEqual((await portfolio.get_portfolio_item("u1", "CASH_KRW"))["quantity"], 85606080)
        self.assertEqual(await snapshots.get_cashflows("u1"), [])

    async def test_ambiguous_empty_rows_and_invalid_mmw_preserve_previous_snapshot(self):
        await self.link()
        old = [{"stock_code": "CASH_KRW", "stock_name": "원화", "quantity": 500,
                "avg_price": 1, "avg_price_currency": "KRW", "currency": "KRW"}]
        with patch.object(sync, "fetch_snapshot", AsyncMock(return_value=(old, {}))):
            await sync.sync_account("u1", self.aid)
        before = await account_holdings.list_positions("u1", self.aid)
        for code, qty, value in (("", 1, 1), ("", 0, 1), ("", 0, None), ("", None, 0),
                                 ("MMW1003", 1, None), ("MMW1003", 1, "NaN"), ("MMW1003", -1, 1),
                                 ("MMW1003", 1, -1), ("MMW1003", 0, 1), ("MMW1003", 1, 0),
                                 ("MMW9999", 1, 1)):
            row = {"iem_cd": code, "iem_nm": "", "rsdl_qty": qty, "eal_amt": value}
            domestic = [{"Output_0": {"dca": 0, "nxt_dd_dca": 100, "nxt2_dd_dca": 80}, "Output_1": [row]}]
            with self.subTest(code=code, qty=qty, value=value), \
                 patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "12345678901", "environment": "live"}])), \
                 patch.object(namuh, "pages", AsyncMock(return_value=domestic)):
                with self.assertRaises(BrokerError):
                    await sync.sync_account("u1", self.aid)
            self.assertEqual(await account_holdings.list_positions("u1", self.aid), before)

    async def test_unsettled_buy_partial_sale_and_full_sale_use_execution_balance(self):
        await portfolio.save_portfolio_item("u1", "430500", "수동 보유분", 7, 100)
        await self.link()
        self.listing.return_value.add("430500")
        old = [{"stock_code": code, "stock_name": code, "quantity": qty, "avg_price": 100,
                "avg_price_currency": "KRW", "currency": "KRW"}
               for code, qty in (("430500", 2800), ("000660", 10))]
        with patch.object(sync, "fetch_snapshot", AsyncMock(return_value=(old, {}))):
            await sync.sync_account("u1", self.aid)
        total = {"dca": 1000, "nxt_dd_dca": 1200, "nxt2_dd_dca": 1500}
        unsettled = [
            {"iem_cd": "430500", "itg_bnc_qty": 2800, "ny_stl_qty": -1300, "rsdl_qty": 1500, "phs_pr": 100},
            {"iem_cd": "005930", "itg_bnc_qty": 0, "ny_stl_qty": 10000, "rsdl_qty": 10000, "phs_pr": 200},
            {"iem_cd": "000660", "itg_bnc_qty": 10, "ny_stl_qty": -10, "rsdl_qty": 0, "phs_pr": ""},
        ]
        expected = {"430500": 1500, "005930": 10000, "CASH_KRW": 1500}
        # 같은 체결을 반복 조회하거나 결제가 완료되어도 수량·예수금을 이중 반영하지 않는다.
        settled = [{**row, "itg_bnc_qty": row["rsdl_qty"], "ny_stl_qty": 0} for row in unsettled]
        for domestic_rows in (unsettled, unsettled, settled):
            pages = [[{"Output_0": total, "Output_1": domestic_rows}], [{"Output_0": []}]]
            with patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "12345678901", "environment": "live"}])), \
                 patch.object(namuh, "pages", AsyncMock(side_effect=pages)) as api:
                await sync.sync_account("u1", self.aid)
            self.assertEqual(api.await_args_list[0].args[3]["bnc_bse_cd"], "1")
            rows = await account_holdings.list_positions("u1", self.aid)
            self.assertEqual({r["stock_code"]: r["quantity"] for r in rows}, expected)
            self.assertEqual((await portfolio.get_portfolio_item("u1", "430500"))["quantity"], 1507)
            self.assertIsNone(await portfolio.get_portfolio_item("u1", "000660"))

    async def test_invalid_execution_quantity_preserves_snapshot_without_settlement_fallback(self):
        await self.link()
        old = [{"stock_code": "005930", "stock_name": "삼성전자", "quantity": 10, "avg_price": 100,
                "avg_price_currency": "KRW", "currency": "KRW"}]
        with patch.object(sync, "fetch_snapshot", AsyncMock(return_value=(old, {}))):
            await sync.sync_account("u1", self.aid)
        before = await account_holdings.list_positions("u1", self.aid)
        for value in (None, "", "NaN", True, -1):
            row = {"iem_cd": "005930", "itg_bnc_qty": 10, "ny_stl_qty": -5, "rsdl_qty": value, "phs_pr": 100}
            if value is None:
                del row["rsdl_qty"]
            pages = [[{"Output_0": {"dca": 100, "nxt_dd_dca": 100, "nxt2_dd_dca": 80}, "Output_1": [row]}],
                     [{"Output_0": []}]]
            with self.subTest(value=value), \
                 patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "12345678901", "environment": "live"}])), \
                 patch.object(namuh, "pages", AsyncMock(side_effect=pages)):
                with self.assertRaises(BrokerError):
                    await sync.sync_account("u1", self.aid)
            self.assertEqual(await account_holdings.list_positions("u1", self.aid), before)

    async def test_listed_only_sync_removes_old_unlisted_positions_and_keeps_manual_account(self):
        await portfolio.save_portfolio_item("u1", "900180", "완리", 3, 100)
        await self.link()
        old = [{"stock_code": code, "stock_name": name, "quantity": 2, "avg_price": 100,
                "avg_price_currency": "KRW", "currency": "KRW"}
               for code, name in (("900180", "완리"), ("072610", "티맥스소프트"))]
        with patch.object(sync, "fetch_snapshot", AsyncMock(return_value=(old, {}))):
            await sync.sync_account("u1", self.aid)
        total = {"dca": 1000, "nxt_dd_dca": 900, "nxt2_dd_dca": 800, "drn_pbl_amt": 600}
        listed = [{"iem_cd": "A005930", "iem_nm": "상장 주식", "itg_bnc_qty": 5, "rsdl_qty": 5, "phs_pr": 100, "now_pr": 0}]
        # 운영 응답처럼 상장종목 조회에도 비상장이 섞인다. 제외 종목 숫자는 파싱하지 않는다.
        excluded = [{"iem_cd": row["stock_code"] if row["stock_code"] == "072610" else "A" + row["stock_code"],
                     "iem_nm": row["stock_name"], "itg_bnc_qty": "", "rsdl_qty": "", "phs_pr": ""} for row in old]
        calls = []

        async def pages(_user, _cid, path, body, _environment):
            calls.append((path, body))
            if path == "/krstock/inquiry/v1/balance":
                return [{"Output_0": total, "Output_1": listed + excluded}]
            if path == "/gbstock/inquiry/v1/margin":
                return [{"Output_0": [{"cur_cd": "USD", "fc_dca": 100, "stl_af_fc_dca": 80, "fc_drn_pbl_amt": 60},
                                       {"cur_cd": "JPY", "fc_dca": 2000, "stl_af_fc_dca": 1500, "fc_drn_pbl_amt": 1000},
                                       {"cur_cd": "<원화환산합계>", "stl_af_dca": 999999}]}]
            self.fail("해외주식 제외 상태에서 주식 잔고를 조회했습니다.")

        with patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "12345678901", "environment": "live"}])), \
             patch.object(namuh, "pages", side_effect=pages):
            await sync.sync_account("u1", self.aid)
            result = await sync.sync_account("u1", self.aid)
        self.assertEqual(result["balances"]["_excluded"], ["072610", "900180"])
        self.assertEqual([body["ltg_aot_dit_cd"] for path, body in calls if path.startswith("/krstock/")], ["1", "1"])
        positions = {row["stock_code"]: row["quantity"] for row in await account_holdings.list_positions("u1", self.aid)}
        self.assertEqual(positions, {"005930": 5, "CASH_KRW": 800, "CASH_USD": 80, "CASH_JPY": 1500})
        self.assertEqual((await portfolio.get_portfolio_item("u1", "900180"))["quantity"], 3)
        self.assertIsNone(await portfolio.get_portfolio_item("u1", "072610"))
        self.assertIsNone((await brokers.get_link("u1", self.aid))["sync_error"])

    async def test_listing_failure_preserves_holdings_and_cash(self):
        await self.link()
        rows = [{"stock_code": "005930", "stock_name": "삼성전자", "quantity": 2, "avg_price": 100,
                 "avg_price_currency": "KRW", "currency": "KRW"},
                {"stock_code": "CASH_KRW", "stock_name": "원화 현금", "quantity": 500, "avg_price": 1,
                 "avg_price_currency": "KRW", "currency": "KRW"}]
        with patch.object(sync, "fetch_snapshot", AsyncMock(return_value=(rows, {}))):
            await sync.sync_account("u1", self.aid)
        before = await account_holdings.list_positions("u1", self.aid)
        self.listing.side_effect = BrokerError("상장 목록 조회 실패")
        domestic = [{"Output_0": {"dca": 100, "nxt_dd_dca": 100, "nxt2_dd_dca": 80},
                     "Output_1": [{"iem_cd": "A005930", "itg_bnc_qty": 3, "rsdl_qty": 3, "phs_pr": 100}]}]
        with patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "12345678901", "environment": "live"}])), \
             patch.object(namuh, "pages", AsyncMock(return_value=domestic)):
            with self.assertRaisesRegex(BrokerError, "상장 목록"):
                await sync.sync_account("u1", self.aid)
        self.assertEqual(await account_holdings.list_positions("u1", self.aid), before)

    async def test_alphanumeric_and_konex_listings_are_preserved(self):
        total = {"dca": 100, "nxt_dd_dca": 100, "nxt2_dd_dca": 80}
        domestic = [{"Output_0": total, "Output_1": [
            {"iem_cd": code, "itg_bnc_qty": 2, "rsdl_qty": 2, "phs_pr": 100}
            for code in ("A02826K", "KR703473K016", "0203K0")]}]
        with patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "12345678901", "environment": "live"}])), \
             patch.object(namuh, "pages", AsyncMock(side_effect=[domestic, [{"Output_0": []}]])):
            rows, _ = await sync.fetch_snapshot("u1", {"credential_id": self.cid, "account_no": "12345678901", "environment": "live", "include_overseas": False})
        self.assertEqual({r["stock_code"] for r in rows}, {"02826K", "03473K", "0203K0", "CASH_KRW"})

    async def test_foreign_cash_failure_preserves_snapshot_when_overseas_stocks_disabled(self):
        await self.link()
        old = [{"stock_code": "CASH_USD", "stock_name": "USD 현금", "quantity": 80, "avg_price": 1,
                "avg_price_currency": "USD", "currency": "USD"}]
        with patch.object(sync, "fetch_snapshot", AsyncMock(return_value=(old, {}))):
            await sync.sync_account("u1", self.aid)
        before = await account_holdings.list_positions("u1", self.aid)
        domestic = [{"Output_0": {"dca": 1000, "nxt_dd_dca": 900, "nxt2_dd_dca": 800, "drn_pbl_amt": 600}}]
        for margin in (BrokerError("외화 조회 실패"), {}, {"Output_0": [None]},
                       {"Output_0": [{"cur_cd": "USD", "fc_dca": 100, "fc_drn_pbl_amt": 60}]},
                       {"Output_0": [{"cur_cd": "USD", "fc_dca": 100, "fc_drn_pbl_amt": 60, "stl_af_fc_dca": "NaN"}]}):
            with self.subTest(margin=margin), \
                 patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "12345678901", "environment": "live"}])), \
                 patch.object(namuh, "pages", AsyncMock(side_effect=[domestic, margin if isinstance(margin, BrokerError) else [margin]])):
                with self.assertRaises(BrokerError):
                    await sync.sync_account("u1", self.aid)
            self.assertEqual(await account_holdings.list_positions("u1", self.aid), before)

    async def test_listed_only_response_still_rejects_invalid_stock_values(self):
        await self.link()
        domestic = [{"Output_0": {"dca": 1000, "nxt_dd_dca": 900, "nxt2_dd_dca": 800, "drn_pbl_amt": 600},
                     "Output_1": [{"iem_cd": "A005930", "iem_nm": "삼성전자", "itg_bnc_qty": 1, "rsdl_qty": 1, "phs_pr": ""}]}]
        with patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "12345678901", "environment": "live"}])), \
             patch.object(namuh, "pages", AsyncMock(return_value=domestic)):
            with self.assertRaises(BrokerError):
                await sync.sync_account("u1", self.aid)
        self.assertEqual(await account_holdings.list_positions("u1", self.aid), [])

    async def test_balance_accepts_rate_specific_order_limits_and_uses_final_summary(self):
        total = {"dca": 1000, "nxt_dd_dca": 900, "nxt2_dd_dca": 800, "drn_pbl_amt": 600,
                 "orr_pbl_amt1": "4000", "orr_pbl_amt2": 3000, "orr_pbl_amt3": 2000, "orr_pbl_amt4": 700}
        domestic = [{"Output_0": {**total, "nxt2_dd_dca": 0}, "Output_1": [
            {"iem_cd": "A005930", "iem_nm": "삼성전자", "itg_bnc_qty": 10, "rsdl_qty": 10, "phs_pr": 100}]},
            {"Output_0": total, "Output_1": [
                {"iem_cd": "A000660", "iem_nm": "SK하이닉스", "itg_bnc_qty": 2, "rsdl_qty": 2, "phs_pr": 200}]}]
        with patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "12345678901", "environment": "live"}])), \
             patch.object(namuh, "pages", AsyncMock(side_effect=[domestic, [{"Output_0": []}]])):
            rows, balances = await sync.fetch_snapshot("u1", {"credential_id": self.cid, "account_no": "12345678901", "environment": "live", "include_overseas": False})
        self.assertEqual({r["stock_code"]: r["quantity"] for r in rows}, {"005930": 10, "000660": 2, "CASH_KRW": 800})
        self.assertNotIn("orr_pbl_amt", balances["KRW"])
        self.assertEqual(balances["KRW"]["orr_pbl_amt1"], 4000)
        self.assertEqual(balances["KRW"]["orr_pbl_amt4"], 700)

    async def test_currency_conversion_total_is_not_a_cash_position(self):
        domestic = [{"Output_0": {"dca": 1000, "nxt_dd_dca": 900, "nxt2_dd_dca": 800, "drn_pbl_amt": 600}}]
        foreign = [{"Output_0": {"fc_aet_amt": 0}, "Output_1": []}]
        usd = {"cur_cd": "USD", "fc_dca": 100, "stl_af_fc_dca": 80, "fc_drn_pbl_amt": 60}
        vnd = {"cur_cd": "VND", "fc_dca": 100000, "stl_af_fc_dca": 80000, "fc_drn_pbl_amt": 60000}
        converted = {"cur_cd": "<원화환산합계>", "dca": 140000, "stl_af_dca": 112000,
                     "fc_dca": 0, "stl_af_fc_dca": 0, "fc_drn_pbl_amt": 0}
        for margin_rows, expected in (([usd, converted], {"CASH_KRW": 800, "CASH_USD": 80}),
                                      ([vnd, converted], {"CASH_KRW": 800, "CASH_VND": 80000}),
                                      ([converted], {"CASH_KRW": 800})):
            with self.subTest(currencies=len(margin_rows)):
                async def pages(_user, _cid, path, _body, _environment):
                    if path.startswith("/krstock/"):
                        return domestic
                    if path.endswith("/margin"):
                        return [{"Output_0": margin_rows}]
                    return foreign
                with patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "12345678901", "environment": "live"}])), \
                     patch.object(namuh, "pages", side_effect=pages):
                    rows, balances = await sync.fetch_snapshot("u1", {"credential_id": self.cid, "account_no": "12345678901", "environment": "live"})
                self.assertEqual({r["stock_code"]: r["quantity"] for r in rows}, expected)
                self.assertNotIn("<원화환산합계>", balances)

    async def test_all_country_balance_imports_vietnam_australia_germany_and_original_currencies(self):
        await brokers.link_account("u1", self.aid, self.cid, "12345678901", "live", True)
        domestic = [{"Output_0": {"dca": 100, "nxt_dd_dca": 90, "nxt2_dd_dca": 80}}]
        def position(code, country, currency, qty):
            return {"iem_cd": code, "fc_sec_trd_nat_cd": country, "cur_cd": currency,
                    "cns_bse_bnc_qty": qty, "fc_phs_uit_pr": 30000 if currency == "VND" else 100,
                    "byn_cns_qty": 10, "sll_cns_qty": 5}  # 체결기준 수량에 다시 가감하면 안 된다.
        foreign = [{"Output_0": {"fc_aet_amt": 1000}, "Output_1": [
            position("AAA", "020", "AUD", 10), position("AAA", "200", "USD", 20),
            position("EUN2", "050", "EUR", 5)]}, {"Output_1": [
            position("FUEVFVND", "660", "VND", 100), position("83199", "120", "CNY", 2),
            {"iem_cd": "SOLD", "cns_bse_bnc_qty": 0}]}]
        cash = [{"Output_0": [{"cur_cd": "VND", "fc_dca": 6000, "stl_af_fc_dca": 5000, "fc_drn_pbl_amt": 4000}]}]
        mapping = {("AUSAAA", "AUD"): "AAA.AX", ("DEUEUN2", "EUR"): "EUN2.DE", ("VNMFUEVFVND", "VND"): "FUEVFVND.HM"}
        with patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "12345678901", "environment": "live"}])), \
             patch.object(namuh, "pages", AsyncMock(side_effect=[domestic, foreign, cash])) as api, \
             patch.object(overseas_realtime, "ensure_master", AsyncMock()), \
             patch.object(overseas_realtime, "code_for_balance", side_effect=lambda gic, currency: mapping.get((gic, currency))):
            await sync.sync_account("u1", self.aid)
        self.assertEqual(api.await_args_list[1].args[3]["fc_sec_trd_nat_cd"], "000")
        self.assertEqual(api.await_count, 3)
        rows = {r["stock_code"]: r for r in await account_holdings.list_positions("u1", self.aid)}
        self.assertEqual({code: r["quantity"] for code, r in rows.items()}, {
            "CASH_KRW": 80, "AAA.AX": 10, "AAA": 20, "EUN2.DE": 5, "FUEVFVND.HM": 100, "83199.HK": 2, "CASH_VND": 5000})
        self.assertEqual((rows["FUEVFVND.HM"]["currency"], rows["FUEVFVND.HM"]["avg_price_currency"], rows["FUEVFVND.HM"]["avg_price"]), ("VND", "VND", 30000))

    async def test_unknown_foreign_market_or_master_failure_preserves_whole_snapshot(self):
        await brokers.link_account("u1", self.aid, self.cid, "12345678901", "live", True)
        old = [{"stock_code": "FUEVFVND.HM", "stock_name": "ETF", "quantity": 100, "avg_price": 30000,
                "avg_price_currency": "VND", "currency": "VND"}]
        with patch.object(sync, "fetch_snapshot", AsyncMock(return_value=(old, {}))):
            await sync.sync_account("u1", self.aid)
        before = await account_holdings.list_positions("u1", self.aid)
        for country in (None, "999", "660"):
            domestic = [{"Output_0": {"dca": 100, "nxt_dd_dca": 90, "nxt2_dd_dca": 80}}]
            foreign = [{"Output_0": {"fc_aet_amt": 1}, "Output_1": [{"iem_cd": "FUEVFVND", "fc_sec_trd_nat_cd": country,
                "cur_cd": "VND", "cns_bse_bnc_qty": 50, "fc_phs_uit_pr": 30000}]}]
            with self.subTest(country=country), \
                 patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "12345678901", "environment": "live"}])), \
                 patch.object(namuh, "pages", AsyncMock(side_effect=[domestic, foreign])), \
                 patch.object(overseas_realtime, "ensure_master", AsyncMock()), \
                 patch.object(overseas_realtime, "code_for_balance", return_value=None):
                with self.assertRaises(BrokerError):
                    await sync.sync_account("u1", self.aid)
            self.assertEqual(await account_holdings.list_positions("u1", self.aid), before)

    async def test_gold_balance_uses_existing_gold_asset_without_changing_units(self):
        domestic = [{"Output_0": {"dca": 100, "nxt_dd_dca": 100, "nxt2_dd_dca": 100, "drn_pbl_amt": 100},
                     "Output_1": [{"iem_cd": "M04020000", "iem_nm": "금 99.99K", "itg_bnc_qty": 3, "rsdl_qty": 3, "phs_pr": 120000}]}]
        with patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "12345678901", "environment": "live"}])), \
             patch.object(namuh, "pages", AsyncMock(side_effect=[domestic, [{"Output_0": []}]])):
            rows, _ = await sync.fetch_snapshot("u1", {"credential_id": self.cid, "account_no": "12345678901", "environment": "live", "include_overseas": False})
        self.assertEqual(rows[0]["stock_code"], "KRX_GOLD")
        self.assertEqual(rows[0]["quantity"], 3)
        self.assertEqual(rows[0]["avg_price"], 120000)
        self.listing.assert_not_awaited()

    async def test_cma_rp_preserves_broker_valuation_and_cost_separately_from_cash(self):
        from services.portfolio import foreign, quote_service

        total = {"dca": 100, "nxt_dd_dca": 100, "nxt2_dd_dca": 80, "drn_pbl_amt": 60}
        domestic = [{"Output_0": total, "Output_1": [
            {"iem_cd": "RKRW221", "iem_nm": "CMA 원화RP", "itg_bnc_qty": 1000, "rsdl_qty": 1000,
             "phs_pr": 0, "now_pr": 0, "eal_amt": 1010, "eal_pls_amt": 10}]},
            {"Output_0": total, "Output_1": [
                {"iem_cd": "RKRW221", "iem_nm": "CMA 원화RP", "itg_bnc_qty": 2000, "rsdl_qty": 2000,
                 "phs_pr": 0, "now_pr": 0, "eal_amt": 2040, "eal_pls_amt": 40}]}]
        await self.link()
        with patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "12345678901", "environment": "live"}])), \
             patch.object(namuh, "pages", AsyncMock(side_effect=[domestic, [{"Output_0": []}]])):
            await sync.sync_account("u1", self.aid)
        positions = {row["stock_code"]: row for row in await portfolio.get_portfolio("u1")}
        rp = positions["CMA_RP_KRW"]
        self.assertEqual(rp["quantity"], 3050)
        self.assertAlmostEqual(rp["quantity"] * rp["avg_price"], 3000)
        self.assertEqual(positions["CASH_KRW"]["quantity"], 80)
        self.assertEqual(rp["group_name"], "기타")
        self.listing.assert_not_awaited()
        with patch.object(foreign, "fetch_foreign_quote", AsyncMock()) as foreign_quote:
            quote = await quote_service.fetch_external_quote_for_stock_service("CMA_RP_KRW")
        foreign_quote.assert_not_awaited()
        self.assertEqual(rp["quantity"] * quote["price"], 3050)
        self.assertAlmostEqual(rp["quantity"] * (quote["price"] - rp["avg_price"]), 50)

    async def test_required_cash_stays_strict_and_failed_parse_preserves_holdings(self):
        await portfolio.save_portfolio_item("u1", "005930", "삼성전자", 10, 100)
        await self.link()
        for raw in (None, "", "NaN", "invalid-private-value"):
            with self.subTest(raw=raw):
                total = {"dca": 1000, "nxt_dd_dca": 900, "nxt2_dd_dca": raw, "drn_pbl_amt": 600}
                if raw is None:
                    del total["nxt2_dd_dca"]
                with patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "12345678901", "environment": "live"}])), \
                     patch.object(namuh, "pages", AsyncMock(return_value=[{"Output_0": total}])), \
                     self.assertLogs("services.brokers.sync", level="WARNING") as logs:
                    with self.assertRaises(BrokerError):
                        await sync.sync_account("u1", self.aid)
                self.assertIn("nxt2_dd_dca", logs.output[0])
                self.assertNotIn("invalid-private-value", logs.output[0])
                self.assertEqual((await portfolio.get_portfolio_item("u1", "005930"))["quantity"], 10)
                self.assertEqual(await account_holdings.list_positions("u1", self.aid), [])

    async def test_pagination_sends_both_continuation_headers_and_collects_final_page(self):
        calls = []
        def respond(request):
            index = len(calls)
            calls.append(request)
            if index:
                self.assertEqual(request.headers["cts"], f"page-{index}")
                self.assertEqual(request.headers["cts_flag"], "Y")
            else:
                self.assertNotIn("cts", request.headers)
                self.assertNotIn("cts_flag", request.headers)
            return httpx.Response(200, json={"rsp_cd": "00166" if index == 3 else "00218",
                "rsp_msg": "조회완료" if index == 3 else "계속조회", "Output_1": [{"page": index}]},
                headers={"cts": f"page-{index + 1}", "cts_flag": "N" if index == 3 else "Y"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            with patch.object(namuh, "get_http_client", AsyncMock(return_value=client)), \
                 patch.object(namuh, "token", AsyncMock(return_value="test-token")):
                pages = await namuh.pages("u1", self.cid, "/krstock/inquiry/v1/balance", {})
        self.assertEqual([page["Output_1"][0]["page"] for page in pages], [0, 1, 2, 3])

    async def test_continuation_ignores_terminal_header_and_keeps_incomplete_guard(self):
        self.assertIsNone(namuh.continuation({"rsp_cd": "00166"}, {"cts": "last-page"}))
        self.assertEqual(namuh.continuation({"rsp_cd": "00218"}, {"cts": " next-page "}), "next-page")
        self.assertIsNone(namuh.continuation({}, {"cts": "last-page", "cts_flag": " N "}))
        with self.assertRaises(BrokerError):
            namuh.continuation({}, {"cts": "  ", "cts_flag": " Y "})

    async def test_rate_limit_retries_same_page_then_resumes_without_duplicate_holdings(self):
        calls = []
        def respond(request):
            calls.append(request)
            if len(calls) == 1:
                return httpx.Response(200, json={"Output_1": [{"page": 1}]}, headers={"cts": "next", "cts_flag": "Y"})
            self.assertEqual(request.headers["cts"], "next")
            self.assertEqual(request.headers["cts_flag"], "Y")
            if len(calls) == 2:
                return httpx.Response(429, headers={"Retry-After": "3"})
            return httpx.Response(200, json={"Output_1": [{"page": 2}]}, headers={"cts_flag": "N"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            with patch.object(namuh, "get_http_client", AsyncMock(return_value=client)), \
                 patch.object(namuh, "token", AsyncMock(return_value="test-token")), \
                 patch.object(namuh.asyncio, "sleep", AsyncMock()) as sleep:
                pages = await namuh.pages("u1", self.cid, "/krstock/inquiry/v1/balance", {})
        self.assertEqual([p["Output_1"][0]["page"] for p in pages], [1, 2])
        self.assertEqual(len(calls), 3)
        self.assertGreater(sleep.await_args_list[-1].args[0], 2.8)

    async def test_persistent_rate_limit_has_bounded_retries_and_returns_no_partial_snapshot(self):
        calls = []
        def respond(request):
            calls.append(request)
            return httpx.Response(429, headers={"Retry-After": "NaN"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            with patch.object(namuh, "get_http_client", AsyncMock(return_value=client)), \
                 patch.object(namuh, "token", AsyncMock(return_value="test-token")), \
                 patch.object(namuh.asyncio, "sleep", AsyncMock()):
                with self.assertRaisesRegex(BrokerError, "조회 한도"):
                    await namuh.pages("u1", self.cid, "/krstock/inquiry/v1/balance", {})
        self.assertEqual(len(calls), 3)

    async def test_active_key_cannot_be_overwritten_by_failed_secret_entry(self):
        await self.link()
        with self.assertRaises(BrokerError):
            await brokers.store_credential("u1", "test-namuh-app-key", "wrong-secret")
        self.assertEqual((await brokers.get_credential("u1", self.cid))["app_secret"], "test-namuh-secret")

    async def test_same_account_cannot_be_linked_twice_using_different_keys(self):
        await self.link()
        link = await brokers.get_link("u1", self.aid)
        self.assertNotEqual(link["account_fingerprint"], brokers.fingerprint("12345678901"))
        second = (await accounts.create_account("u1", name="중복 계좌"))["account_id"]
        cid = await brokers.store_credential("u1", "another-namuh-key", "another-secret")
        with self.assertRaises(BrokerError):
            await brokers.link_account("u1", second, cid, "12345678901", "live", False)

    async def test_foreign_code_uses_existing_quote_provider_symbols(self):
        self.assertEqual(sync.foreign_code("00700", "120"), "0700.HK")
        self.assertEqual(sync.foreign_code("BRK.B", "200"), "BRK-B")
        self.assertEqual(sync.foreign_code("285A", "070"), "285A.T")

    async def test_tick_is_not_ack_and_rejects_stale_or_future_ticks(self):
        now = datetime(2026, 9, 15, 10, 0, 0, tzinfo=realtime._KST)
        message = {"header": {"tr_cd": "mc"}, "body": {"code": "005930", "time": "09:59:59", "price": "100", "change": "2", "sign": "5"}}
        tick = realtime.normalize(message, now)
        self.assertEqual(tick["previous_close"], 102)
        self.assertEqual(tick["market"], "UN")
        self.assertIsNone(realtime.normalize({**message, "header": {"tr_cd": "mc", "rsp_cd": "00000"}}, now))
        message["body"]["time"] = "09:00:00"
        self.assertIsNone(realtime.normalize(message, now))
        message["body"]["time"] = "10:01:00"
        self.assertIsNone(realtime.normalize(message, now))
