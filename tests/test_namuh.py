import asyncio
import json
import logging
from datetime import datetime
from unittest.mock import AsyncMock, patch

import httpx
from _harness import TempDbMixin, seed_user

from repositories import account_holdings, accounts, brokers, portfolio
from repositories.broker_secrets import BrokerError
from repositories.db import get_db
from services.brokers import namuh, realtime, sync


class NamuhTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await seed_user("u2", "second@example.com")
        await account_holdings.ensure("u1")
        self.aid = (await accounts.create_account("u1", name="NH"))["account_id"]
        self.cid = await brokers.store_credential("u1", "test-namuh-app-key", "test-namuh-secret")
        namuh._locks.clear()
        namuh._last_call.clear()

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

    async def test_balance_parser_distinguishes_settlement_cash_and_combines_pages(self):
        summary = {key: str(value) for key, value in zip(("dca", "nxt_dd_dca", "nxt2_dd_dca", "orr_pbl_amt", "drn_pbl_amt"), (1000, 900, 800, 700, 600))}
        domestic = [{"Output_0": summary, "Output_1": [{"iem_cd": "KR7005930003", "iem_nm": "삼성전자", "itg_bnc_qty": "10", "phs_pr": "100"}]},
                    {"Output_1": [{"iem_cd": "A000660", "iem_nm": "SK하이닉스", "itg_bnc_qty": "2", "phs_pr": "200"}]}]
        with patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "12345678901", "environment": "live"}])), \
             patch.object(namuh, "pages", AsyncMock(return_value=domestic)):
            rows, balances = await sync.fetch_snapshot("u1", {"credential_id": self.cid, "account_no": "12345678901", "environment": "live", "include_overseas": False})
        self.assertEqual({r["stock_code"]: r["quantity"] for r in rows}, {"005930": 10, "000660": 2, "CASH_KRW": 800})
        self.assertEqual(balances["KRW"]["drn_pbl_amt"], 600)
        with self.assertRaises(BrokerError):
            sync.summary({"Output_0": {}})

    async def test_balance_accepts_rate_specific_order_limits_and_uses_final_summary(self):
        total = {"dca": 1000, "nxt_dd_dca": 900, "nxt2_dd_dca": 800, "drn_pbl_amt": 600,
                 "orr_pbl_amt1": "4000", "orr_pbl_amt2": 3000, "orr_pbl_amt3": 2000, "orr_pbl_amt4": 700}
        domestic = [{"Output_0": {**total, "nxt2_dd_dca": 0}, "Output_1": [
            {"iem_cd": "A005930", "iem_nm": "삼성전자", "itg_bnc_qty": 10, "phs_pr": 100}]},
            {"Output_0": total, "Output_1": [
                {"iem_cd": "A000660", "iem_nm": "SK하이닉스", "itg_bnc_qty": 2, "phs_pr": 200}]}]
        with patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "12345678901", "environment": "live"}])), \
             patch.object(namuh, "pages", AsyncMock(return_value=domestic)):
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

    async def test_gold_balance_uses_existing_gold_asset_without_changing_units(self):
        domestic = [{"Output_0": {"dca": 100, "nxt_dd_dca": 100, "nxt2_dd_dca": 100, "drn_pbl_amt": 100},
                     "Output_1": [{"iem_cd": "M04020000", "iem_nm": "금 99.99K", "itg_bnc_qty": 3, "phs_pr": 120000}]}]
        with patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "12345678901", "environment": "live"}])), \
             patch.object(namuh, "pages", AsyncMock(return_value=domestic)):
            rows, _ = await sync.fetch_snapshot("u1", {"credential_id": self.cid, "account_no": "12345678901", "environment": "live", "include_overseas": False})
        self.assertEqual(rows[0]["stock_code"], "KRX_GOLD")
        self.assertEqual(rows[0]["quantity"], 3)
        self.assertEqual(rows[0]["avg_price"], 120000)

    async def test_cma_rp_preserves_broker_valuation_and_cost_separately_from_cash(self):
        from services.portfolio import foreign, quote_service

        total = {"dca": 100, "nxt_dd_dca": 100, "nxt2_dd_dca": 80, "drn_pbl_amt": 60}
        domestic = [{"Output_0": total, "Output_1": [
            {"iem_cd": "RKRW221", "iem_nm": "CMA 원화RP", "itg_bnc_qty": 1000,
             "phs_pr": 0, "now_pr": 0, "eal_amt": 1010, "eal_pls_amt": 10}]},
            {"Output_0": total, "Output_1": [
                {"iem_cd": "RKRW221", "iem_nm": "CMA 원화RP", "itg_bnc_qty": 2000,
                 "phs_pr": 0, "now_pr": 0, "eal_amt": 2040, "eal_pls_amt": 40}]}]
        await self.link()
        with patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": "12345678901", "environment": "live"}])), \
             patch.object(namuh, "pages", AsyncMock(return_value=domestic)):
            await sync.sync_account("u1", self.aid)
        positions = {row["stock_code"]: row for row in await portfolio.get_portfolio("u1")}
        rp = positions["CMA_RP_KRW"]
        self.assertEqual(rp["quantity"], 3050)
        self.assertAlmostEqual(rp["quantity"] * rp["avg_price"], 3000)
        self.assertEqual(positions["CASH_KRW"]["quantity"], 80)
        self.assertEqual(rp["group_name"], "기타")
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
