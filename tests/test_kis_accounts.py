import asyncio
import base64
import json
import logging
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from _harness import TempDbMixin, seed_user
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from repositories import account_holdings, accounts, broker_activity, brokers, portfolio
from repositories.broker_secrets import BrokerError, encrypt
from repositories.db import get_db
from routes import broker_accounts
from services.brokers import kis, kis_balance, kis_realtime, namuh, sync


def domestic():
    return {"output1": [{"pdno": "005930", "prdt_name": "삼성전자", "hldg_qty": "3", "pchs_avg_pric": "70000", "loan_amt": "0"}],
            "output2": [{"dnca_tot_amt": "10000", "nxdy_excc_amt": "9000", "prvs_rcdl_excc_amt": "8000", "tot_loan_amt": "0", "cma_evlu_amt": "0"}]}


def present():
    return {"output1": [{"pdno": "AAPL", "ovrs_excg_cd": "NASD", "ccld_qty_smtl1": "2", "buy_crcy_cd": "USD"}],
            "output2": [{"crcy_cd": "USD", "frcr_dncl_amt_2": "1000", "frcr_buy_amt_smtl": "200", "frcr_sll_amt_smtl": "50"}],
            "output3": {"tot_loan_amt": "0"}}


async def snapshot_pages(user, cid, path, params, env="live"):
    if path == kis.DOMESTIC:
        return [domestic()]
    if path == kis.STOCK_INFO:
        return [{"output": {"scts_mket_lstg_dt": "19750611", "scts_mket_lstg_abol_dt": "", "kosdaq_mket_lstg_dt": "", "lstg_abol_dt": ""}}]
    if path == kis.PRESENT:
        return [present()]
    return [{"output1": [{"ovrs_pdno": "AAPL", "ovrs_cblc_qty": "2", "pchs_avg_pric": "150", "tr_crcy_cd": "USD"}]
             if params["OVRS_EXCG_CD"] == "NASD" else []}]


class KisAccountsTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await seed_user("u2", "second@example.com")
        await account_holdings.ensure("u1")
        self.aid = (await accounts.create_account("u1", name="한국투자"))["account_id"]
        self.cid = await brokers.store_credential("u1", "test-kis-key", "test-kis-secret", provider="kis", hts_id="test_hts")
        self.link = {"account_id": self.aid, "credential_id": self.cid, "account_no": "1234567801", "environment": "live", "provider": "kis", "product": "stocks"}
        kis._locks.clear()
        kis._token_locks.clear()
        kis._last_call.clear()
        kis_balance._listing.clear()

    async def test_provider_and_environment_isolation_encryption_and_authenticated_account_number(self):
        await brokers.link_account("u1", self.aid, self.cid, "1234567801", "live", provider="kis")
        row = next(r for r in await accounts.list_accounts("u1") if r["account_id"] == self.aid)
        self.assertEqual(row["broker"], "kis")
        self.assertEqual(row["connection"]["account_no"], "1234567801")
        self.assertNotIn("ciphertext", json.dumps(row))
        self.assertNotIn("test_hts", json.dumps(row))
        self.assertFalse(await brokers.has_link("u1"))  # 나무 시세 우선권으로 오인하지 않는다.
        db = await get_db()
        stored = str(dict(await (await db.execute("SELECT * FROM broker_credentials WHERE credential_id=?", (self.cid,))).fetchone()))
        for private in ("test-kis-key", "test-kis-secret", "test_hts"):
            self.assertNotIn(private, stored)
        with self.assertRaises(BrokerError):
            await brokers.get_link("u2", self.aid)
        with self.assertRaises(BrokerError):
            await brokers.store_credential("u2", "test-kis-key", "test-kis-secret", provider="kis")
        with self.assertRaises(BrokerError):
            await kis.credential("u1", self.cid, "mock")
        with self.assertRaises(BrokerError):
            await namuh.token("u1", self.cid)
        with self.assertRaises(BrokerError):
            await broker_accounts.require_provider("u1", self.aid, "namuh")

    async def test_token_singleflight_persistent_cache_and_mock_host(self):
        calls = []
        async def respond(request):
            calls.append(request)
            return httpx.Response(200, json={"access_token": "private-access", "expires_in": 86400})
        cid = await brokers.store_credential("u1", "test-mock-key", "test-mock-secret", provider="kis", environment="mock")
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            with patch.object(kis, "get_http_client", AsyncMock(return_value=client)):
                result = await asyncio.gather(*(kis.token("u1", cid, "mock") for _ in range(4)))
                self.assertEqual(result, ["private-access"] * 4)
                self.assertEqual(len(calls), 1)
                self.assertEqual(calls[0].url.host, "openapivts.koreainvestment.com")
                self.assertEqual(calls[0].url.path, "/oauth2/tokenP")
        stored = (await brokers.get_credential("u1", cid))["token_ciphertext"]
        self.assertNotIn("private-access", stored)

    async def test_pagination_queries_and_failure_never_return_partial_snapshot(self):
        requests = []
        def respond(request):
            requests.append(request)
            if len(requests) == 1:
                return httpx.Response(200, headers={"tr_cont": "M"}, json={"rt_cd": "0", "output1": [{"pdno": "005930"}], "ctx_area_fk100": "fk", "ctx_area_nk100": "nk"})
            return httpx.Response(200, json={"rt_cd": "1", "msg_cd": "TEST0001", "msg1": "계좌 1234567801 실패"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            with patch.object(kis, "get_http_client", AsyncMock(return_value=client)), patch.object(kis, "token", AsyncMock(return_value="access")), patch.object(kis, "MIN_INTERVAL", 0):
                with self.assertRaisesRegex(BrokerError, "TEST0001") as error:
                    await kis.pages("u1", self.cid, kis.DOMESTIC, {"CANO": "12345678"})
                self.assertNotIn("12345678", str(error.exception))
        self.assertEqual(requests[1].url.params["CTX_AREA_NK100"], "nk")
        self.assertEqual(requests[1].headers["tr_cont"], "N")
        self.assertEqual(requests[0].headers["tr_id"], "TTTC8434R")
        with self.assertRaises(BrokerError):
            await kis.pages("u1", self.cid, "/uapi/domestic-stock/v1/trading/order-cash", {})

    async def test_rates_and_expiry_retry_without_exposing_or_reissuing_valid_token(self):
        responses = [httpx.Response(500, json={"rt_cd": "1", "msg_cd": "EGW00123"}),
                     httpx.Response(500, json={"rt_cd": "1", "msg_cd": "EGW00201"}), httpx.Response(200, json={"rt_cd": "0", "output1": []})]
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: responses.pop(0))) as client:
            with patch.object(kis, "get_http_client", AsyncMock(return_value=client)), patch.object(kis, "token", AsyncMock(side_effect=["old", "new"])) as tokens, \
                 patch.object(kis, "MIN_INTERVAL", 0), patch.object(kis.asyncio, "sleep", AsyncMock()):
                await kis.pages("u1", self.cid, kis.DOMESTIC, {})
        self.assertEqual(tokens.await_count, 2)
        self.assertEqual(tokens.await_args_list[1].kwargs, {"expired": "old"})

    async def test_balances_cash_and_other_accounts_atomic_idempotent(self):
        await portfolio.save_portfolio_item("u1", "005930", "삼성전자", 10, 50000)
        await brokers.link_account("u1", self.aid, self.cid, "1234567801", "live", provider="kis")
        with patch.object(kis, "pages", side_effect=snapshot_pages), patch.object(sync.activity, "fetch", AsyncMock()) as nh_activity:
            for _ in range(2):
                await sync.sync_account("u1", self.aid, include_activity=True)
            nh_activity.assert_not_awaited()
        held = {r["stock_code"]: r for r in await account_holdings.list_positions("u1", self.aid)}
        self.assertEqual(held["CASH_KRW"]["quantity"], 8000)
        self.assertEqual(held["CASH_USD"]["quantity"], 850)
        self.assertEqual(held["AAPL"]["avg_price_currency"], "USD")
        self.assertEqual((await portfolio.get_portfolio_item("u1", "005930"))["quantity"], 13)
        self.assertIsNone(await broker_activity.state("u1", self.aid))
        before = await account_holdings.list_positions("u1", self.aid)
        with patch.object(kis, "pages", AsyncMock(side_effect=BrokerError("누락"))):
            with self.assertRaises(BrokerError):
                await sync.sync_account("u1", self.aid)
        self.assertEqual(await account_holdings.list_positions("u1", self.aid), before)
        await brokers.disconnect("u1", self.aid)
        self.assertEqual(await account_holdings.list_positions("u1", self.aid), before)

    async def test_exclude_delisted_and_unlisted_but_include_halted_listed(self):
        for abolished, listed_at, expected in [("20250101", "20000101", False), ("", "", False), ("", "20000101", True)]:
            kis_balance._listing.clear()
            data = {"scts_mket_lstg_dt": listed_at, "scts_mket_lstg_abol_dt": abolished, "kosdaq_mket_lstg_dt": "", "lstg_abol_dt": abolished, "tr_stop_yn": "Y"}
            with patch.object(kis, "pages", AsyncMock(return_value=[{"output": data}])):
                self.assertEqual(await kis_balance.listed("u1", self.link, "005930"), expected)

    async def test_foreign_cash_even_when_positions_excluded_and_missing_or_duplicate_values_fail(self):
        with patch.object(kis, "pages", side_effect=snapshot_pages):
            rows, _ = await kis.fetch_snapshot("u1", {**self.link, "include_overseas": False})
        self.assertIn("CASH_USD", {r["stock_code"] for r in rows})
        self.assertNotIn("AAPL", {r["stock_code"] for r in rows})
        for bad in (None, "", "nan", "inf", True):
            data = domestic()
            data["output2"][0]["prvs_rcdl_excc_amt"] = bad
            with patch.object(kis, "pages", AsyncMock(return_value=[data])):
                with self.assertRaises(BrokerError):
                    await kis.fetch_snapshot("u1", self.link)
        data = present()
        data["output2"] *= 2
        with patch.object(kis, "pages", AsyncMock(side_effect=[[domestic()], [data]])), patch.object(kis_balance, "listed", AsyncMock(return_value=True)):
            with self.assertRaisesRegex(BrokerError, "중복"):
                await kis.fetch_snapshot("u1", self.link)

    async def test_signed_selection_cannot_cross_user_or_provider(self):
        selection = encrypt(json.dumps({"user": "u1", "expires_at": 9999999999, "provider": "kis"}))
        payload = {"selection": selection}
        self.assertEqual(broker_accounts.selection("u1", payload, "kis")["provider"], "kis")
        with self.assertRaises(BrokerError):
            broker_accounts.selection("u2", payload, "kis")
        with self.assertRaises(BrokerError):
            broker_accounts.selection("u1", payload)
        with patch.object(broker_accounts, "user_id", AsyncMock(return_value="u1")):
            with self.assertRaises(BrokerError) as error:
                await broker_accounts.register_kis(None, {"app_key": "secret-private-input"})
            self.assertNotIn("secret-private-input", str(error.exception))

    async def test_nh_gold_and_kis_stocks_coexist_and_merge_without_overwriting_other_account(self):
        nh_aid = (await accounts.create_account("u1", name="NH 금"))["account_id"]
        nh_cid = await brokers.store_credential("u1", "test-namuh-key", "test-namuh-secret")
        await brokers.link_account("u1", nh_aid, nh_cid, "12345678901", "live", product="gold")
        gold = kis_balance.position("KRX_GOLD", "KRX 금현물", 10, 150000, "KRW")
        with patch.object(sync, "fetch_snapshot", AsyncMock(return_value=([gold], {}))):
            await sync.sync_account("u1", nh_aid)
        await brokers.link_account("u1", self.aid, self.cid, "1234567801", "live", provider="kis")
        with patch.object(kis, "pages", side_effect=snapshot_pages):
            await sync.sync_account("u1", self.aid)
        self.assertEqual((await portfolio.get_portfolio_item("u1", "KRX_GOLD"))["quantity"], 10)
        self.assertEqual((await portfolio.get_portfolio_item("u1", "005930"))["quantity"], 3)
        linked = [r for r in await accounts.list_accounts("u1") if r["broker"]]
        self.assertEqual({r["broker"] for r in linked}, {"namuh", "kis"})
        await brokers.disconnect("u1", self.aid)
        self.assertTrue(await brokers.has_link("u1"))
        self.assertEqual((await brokers.get_credential("u1", nh_cid))["app_key"], "test-namuh-key")

    async def test_unsupported_product_cma_and_incomplete_foreign_snapshot_preserve_previous_holdings(self):
        await brokers.link_account("u1", self.aid, self.cid, "1234567801", "live", provider="kis")
        with patch.object(kis, "pages", side_effect=snapshot_pages):
            await sync.sync_account("u1", self.aid)
        before = await account_holdings.list_positions("u1", self.aid)
        data = domestic()
        data["output2"][0]["cma_evlu_amt"] = "1000"
        with patch.object(kis, "pages", AsyncMock(return_value=[data])):
            with self.assertRaisesRegex(BrokerError, "CMA"):
                await sync.sync_account("u1", self.aid)
        self.assertEqual(await account_holdings.list_positions("u1", self.aid), before)
        with self.assertRaisesRegex(BrokerError, "주식 계좌"):
            await kis.fetch_snapshot("u1", {**self.link, "product": "gold"})
        async def incomplete(user, cid, path, params, env="live"):
            return [{"output1": []}] if path == kis.OVERSEAS else await snapshot_pages(user, cid, path, params, env)
        with patch.object(kis, "pages", side_effect=incomplete):
            with self.assertRaisesRegex(BrokerError, "해외 잔고를 모두"):
                await sync.sync_account("u1", self.aid)
        self.assertEqual(await account_holdings.list_positions("u1", self.aid), before)


def encrypted_notice(account="1234567801", channel="H0STCNI0"):
    key, iv = "x" * 32, "y" * 16
    fields = [""] * (26 if channel.startswith("H0ST") else 25)
    fields[0], fields[1] = "customer", account
    pad = padding.PKCS7(128).padder()
    raw = "^".join(fields).encode()
    raw = pad.update(raw) + pad.finalize()
    enc = Cipher(algorithms.AES(key.encode()), modes.CBC(iv.encode())).encryptor()
    value = base64.b64encode(enc.update(raw) + enc.finalize()).decode()
    return f"1|{channel}|001|{value}", {channel: (key, iv)}


def test_aes_notifications_validate_framing_and_no_plain_account_logs():
    for channel in ("H0STCNI0", "H0STCNI9", "H0GSCNI0"):
        frame, secret = encrypted_notice(channel=channel)
        assert kis_realtime.decode_accounts(frame, secret) == {"1234567801"}
        assert kis_realtime.decode_accounts(frame.replace("|001|", "|002|"), secret) == set()
        assert kis_realtime.decode_accounts(frame, {}) == set()
    record = logging.LogRecord("httpx", 20, "", 1, 'GET https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-balance?CANO=12345678&ACNT_PRDT_CD=01 "200"', (), None)
    kis._HideAccountQuery().filter(record)
    assert "12345678" not in record.getMessage()


@pytest.mark.asyncio
async def test_server_polling_runs_without_browser_and_only_for_kis():
    stop = asyncio.Event()
    links = [{"google_sub": "u1", "account_id": "kis-a", "credential_id": "kis-key", "environment": "live", "provider": "kis"},
             {"google_sub": "u1", "account_id": "nh-a", "credential_id": "nh-key", "environment": "live", "provider": "namuh"}]
    async def synced(*args):
        stop.set()
    with patch.object(brokers, "list_links", AsyncMock(return_value=links)), patch.object(kis_realtime, "sync_account", side_effect=synced) as sync_mock, \
         patch.object(kis_realtime, "stream", AsyncMock()) as ws:
        await asyncio.wait_for(kis_realtime.run(stop), 5)
    sync_mock.assert_awaited_once_with("u1", "kis-a")
    assert ws.await_args.args[:3] == ("u1", "kis-key", "live")


@pytest.mark.asyncio
async def test_notification_ack_does_not_change_cash_and_other_account_notice_is_ignored():
    changed = []
    frame, secret = encrypted_notice()
    key, iv = secret["H0STCNI0"]
    class Socket:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass
        async def send(self, value):
            pass
        async def __aiter__(self):
            yield json.dumps({"header": {"tr_id": "H0STCNI0"}, "body": {"rt_cd": "0", "output": {"key": key, "iv": iv}}})
            yield encrypted_notice("9999999901")[0]
            yield frame
            raise asyncio.CancelledError
    rows = [{"google_sub": "u1", "credential_id": "cid", "environment": "live", "provider": "kis", "account_id": "a"},
            {"google_sub": "u2", "credential_id": "cid2", "environment": "live", "provider": "kis", "account_id": "b"}]
    with patch.object(kis, "credential", AsyncMock(return_value={"hts_id": "hts"})), patch.object(kis, "approval", AsyncMock(return_value="approval")), \
         patch.object(kis_realtime.websockets, "connect", return_value=Socket()), patch.object(brokers, "list_links", AsyncMock(return_value=rows)), \
         patch.object(brokers, "get_link", AsyncMock(return_value={"account_no": "1234567801"})) as get_link:
        with pytest.raises(asyncio.CancelledError):
            await kis_realtime.stream("u1", "cid", "live", changed.append)
    assert changed == [None, "a"]
    assert all(call.args == ("u1", "a") for call in get_link.await_args_list)
    kis_realtime._states.clear()
