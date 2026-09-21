"""공식 명세 필드로 계좌 격리·결제 현금·연속조회·통보의 공통 계약을 검증한다."""

import asyncio
import copy
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from _harness import TempDbMixin, seed_user

from domain.broker_catalog import BROKERS
from repositories import account_holdings, accounts, brokers
from repositories.broker_secrets import BrokerError
from routes import broker_accounts
from services.brokers import push, rest, runtime, sync
from services.brokers.kiwoom import KiwoomAdapter
from services.brokers.ls import LsAdapter
from services.brokers.registry import catalog, get_adapter


def kiwoom_responses():
    return {
        "ka00001": [{"acctNo": "1234567801"}],
        "kt00018": [{"tot_loan_amt": 0, "tot_crd_loan_amt": 0, "tot_crd_ls_amt": 0,
                     "acnt_evlt_remn_indv_tot": [{"stk_cd": "A005930", "stk_nm": "삼성전자", "rmnd_qty": "3", "pur_pric": "70000"},
                                               {"stk_cd": "A900180", "stk_nm": "비상장", "rmnd_qty": "4", "pur_pric": "100"}]}],
        "kt00001": [{"entr": 10000, "d1_entra": 9500, "d2_entra": 9000, "loan_sum": 0, "ls_sum": 0,
                     "ch_uncla_tot": 0, "etc_loan_tot": 0, "dpst_grntl_remn": 0, "sell_grntl_remn": 0,
                     "stk_entr_prst": [{"crnc_cd": "USD", "fx_entr": 999}]}],
        "ust21160": [{"won_dfr_amt": 0, "won_etc_loana": 0, "d0_usd_fx_entr": 1000, "d2_usd_fx_entr": 950, "d4_usd_fx_entr": 800.5}],
        "kt50020": [{"gold_acnt_evlt_prst": [{"stk_cd": "M04020000", "stk_nm": "금 99.99_1Kg", "real_qty": "12", "avg_prc": "150000"}]}],
        "kt50021": [{"entra": 20000, "prsm_entra": 10000, "etc_loan_tot": 0, "dly_amt": 0}],
    }


async def kiwoom_pages(user, cid, env, tr, inputs):
    if tr == "ka10100":
        code = inputs["stk_cd"]
        return [{"code": code, "marketCode": "0" if code == "005930" else "30", "state": "거래정지"}]
    if tr == "ust21070":
        return [{"result_list": [{"stk_cd": "BRK.B", "frgn_stk_nm": "버크셔", "poss_qty": "1.5", "crnc_code": "USD", "frgn_stk_book_uv": "450.25"}]
                 if inputs["stex_tp"] == "NY" else []}]
    return copy.deepcopy(kiwoom_responses()[tr])


def ls_responses():
    return {
        "CSPAQ12200": [{"CSPAQ12200OutBlock1": {"AcntNo": "12345678901"}, "CSPAQ12200OutBlock2": {
            "Dps": 10000, "D1Dps": 9500, "D2Dps": 9000, "MloanAmt": 0, "DpspdgLoanAmt": 0, "MnyrclAmt": 0, "EtclndAmt": 0, "RcvblAmt": 0}}],
        "t0424": [{"t0424OutBlock": {"cts_expcode": ""}, "t0424OutBlock1": [
            {"expcode": "005930", "hname": "삼성전자", "janqty": 3, "pamt": 70000, "sinamt": 0},
            {"expcode": "900180", "hname": "비상장", "janqty": 2, "pamt": 100, "sinamt": 0},
            {"expcode": "CMARP", "janqty": 1000, "mamt": 1000, "appamt": 1020, "sinamt": 0}]}],
        "t8436": [{"t8436OutBlock": [{"shcode": "005930"}]}],
        "COSOQ02701": [{"COSOQ02701OutBlock2": [{"CrcyCode": "USD", "PrsmptFcurrDps2": 950, "PrsmptFcurrDps4": 800.5},
                                              {"CrcyCode": "JPY", "PrsmptFcurrDps2": 10, "PrsmptFcurrDps4": 10}]}],
        "COSOQ00201": [{"COSOQ00201OutBlock2": {"LoanAmt": 0}, "COSOQ00201OutBlock4": [
            {"ShtnIsuNo": "BRK.B", "JpnMktHanglIsuNm": "버크셔", "AstkBalQty": 1.5, "AstkSettQty": 1,
             "FcstckUprc": 450.25, "CrcyCode": "USD", "MktTpNm": "NYSE", "LoanAmt": 0, "AstkBalTpCode": "10"}]}],
    }


async def ls_pages(user, cid, env, tr, inputs):
    return copy.deepcopy(ls_responses()[tr])


def test_registry_and_ui_catalog_share_capabilities():
    assert set(BROKERS) == {row["id"] for row in catalog()} == {"namuh", "kis", "kiwoom", "ls"}
    assert json.loads(json.dumps(catalog())) == json.loads((Path(__file__).parent / "fixtures/broker-catalog.json").read_text(encoding="utf-8"))
    assert get_adapter("namuh").definition.activity
    assert not get_adapter("ls").definition.activity
    with pytest.raises(BrokerError):
        get_adapter("unknown")


class BrokerAdaptersTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await seed_user("u2", "other@example.com")
        await account_holdings.ensure("u1")
        self.aid = (await accounts.create_account("u1", name="확장 계좌"))["account_id"]
        self.cid = await brokers.store_credential("u1", "new-test-key", "new-test-secret", provider="kiwoom")

    async def test_credentials_are_isolated_and_key_account_cannot_be_linked_twice(self):
        with self.assertRaises(BrokerError):
            await brokers.store_credential("u2", "new-test-key", "anything", provider="kiwoom")
        # 같은 문자열 키도 다른 증권사 인증·계좌 지문과 충돌하지 않는다.
        other = await brokers.store_credential("u1", "new-test-key", "new-test-secret", provider="ls")
        self.assertNotEqual(other, self.cid)
        with self.assertRaises(BrokerError):
            await brokers.link_account("u1", self.aid, self.cid, "1234567801", "live", provider="ls")
        await brokers.link_account("u1", self.aid, self.cid, "1234567801", "live", provider="kiwoom")
        second = (await accounts.create_account("u1", name="중복 연결"))["account_id"]
        with self.assertRaises(BrokerError):
            await brokers.link_account("u1", second, self.cid, "9999999901", "live", provider="kiwoom")
        with self.assertRaises(BrokerError):
            await brokers.get_link("u2", self.aid)
        response = json.dumps(await accounts.list_accounts("u1"))
        self.assertIn("1234567801", response)
        self.assertNotIn("new-test-secret", response)
        self.assertNotIn("ciphertext", response)

    async def test_kiwoom_full_sync_settlement_native_currency_idempotency_and_preservation(self):
        adapter = get_adapter("kiwoom")
        adapter._listing.clear()
        await brokers.link_account("u1", self.aid, self.cid, "1234567801", "live", provider="kiwoom")
        with patch.object(adapter, "pages", side_effect=kiwoom_pages):
            result = await sync.sync_account("u1", self.aid)
            await sync.sync_account("u1", self.aid)
        rows = {r["stock_code"]: r for r in await account_holdings.list_positions("u1", self.aid)}
        self.assertEqual(rows["005930"]["quantity"], 3)
        self.assertNotIn("900180", rows)
        self.assertEqual(rows["BRK-B"]["quantity"], 1.5)
        self.assertEqual(rows["CASH_KRW"]["quantity"], 9000)
        self.assertEqual(rows["CASH_USD"]["quantity"], 800.5)
        self.assertEqual(result["balances"]["_excluded"], ["900180"])
        before = await account_holdings.list_positions("u1", self.aid)
        async def incomplete(*args):
            page = await kiwoom_pages(*args)
            if args[3] == "ust21160":
                del page[0]["d4_usd_fx_entr"]
            return page
        with patch.object(adapter, "pages", side_effect=incomplete), self.assertRaises(BrokerError):
            await sync.sync_account("u1", self.aid)
        self.assertEqual(await account_holdings.list_positions("u1", self.aid), before)

    async def test_ls_cma_cash_separation_and_incomplete_listing_keeps_holdings(self):
        adapter = get_adapter("ls")
        adapter._listing.clear()
        cid = await brokers.store_credential("u1", "ls-test-key", "ls-test-secret", provider="ls")
        await brokers.link_account("u1", self.aid, cid, "12345678901", "live", provider="ls")
        with patch.object(adapter, "pages", side_effect=ls_pages):
            await sync.sync_account("u1", self.aid)
        rows = {r["stock_code"]: r for r in await account_holdings.list_positions("u1", self.aid)}
        self.assertEqual(rows["CMA_RP_KRW"]["quantity"], 1020)
        self.assertAlmostEqual(rows["CMA_RP_KRW"]["avg_price"] * 1020, 1000)
        self.assertEqual(rows["CASH_KRW"]["quantity"], 9000)
        self.assertEqual(rows["CASH_USD"]["quantity"], 800.5)
        self.assertEqual(rows["CASH_JPY"]["quantity"], 10)
        self.assertEqual(rows["BRK-B"]["quantity"], 1.5)
        adapter._listing.clear()
        before = await account_holdings.list_positions("u1", self.aid)
        async def incomplete(*args):
            return [{"t8436OutBlock": []}] if args[3] == "t8436" else await ls_pages(*args)
        with patch.object(adapter, "pages", side_effect=incomplete), self.assertRaises(BrokerError):
            await sync.sync_account("u1", self.aid)
        self.assertEqual(await account_holdings.list_positions("u1", self.aid), before)

    async def test_registration_provider_choice_and_unsupported_product_are_bound(self):
        adapter = get_adapter("kiwoom")
        with patch.object(broker_accounts, "user_id", AsyncMock(return_value="u1")), patch.object(adapter, "pages", side_effect=kiwoom_pages):
            response = await broker_accounts.register_broker("kiwoom", None, {"app_key": "new-test-key", "app_secret": "new-test-secret"})
            self.assertNotIn("new-test", json.dumps(response))
            choice = {"selection": response["accounts"][0]["selection"], "product": "gold"}
            self.assertEqual(broker_accounts.selection("u1", choice, "kiwoom")["account_no"], "1234567801")
            for user, provider, product in [("u2", "kiwoom", "gold"), ("u1", "ls", "gold"), ("u1", "kiwoom", "krfuture")]:
                with self.assertRaises(BrokerError):
                    broker_accounts.selection(user, {**choice, "product": product}, provider)
            with self.assertRaises(BrokerError) as error:
                await broker_accounts.register_broker("ls", None, {"app_key": "secret-marker"})
            self.assertNotIn("secret-marker", str(error.exception))
        self.assertFalse((await accounts.list_accounts("u1"))[-1]["broker"])

    async def test_transport_caches_encrypted_token_and_rejects_provider_and_environment(self):
        adapter = KiwoomAdapter()
        hits = []
        def handler(request):
            hits.append(request)
            return httpx.Response(200, json={"return_code": 0, "token": "private-token", "expires_dt": "20990101000000"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with patch.object(rest, "get_http_client", AsyncMock(return_value=client)):
                result = await asyncio.gather(*(adapter.token("u1", self.cid, "live") for _ in range(3)))
                self.assertEqual(result, ["private-token"] * 3)
                self.assertEqual(len(hits), 1)
                self.assertNotIn("new-test-key", str(hits[0].url))
                for source, env in [(adapter, "mock"), (LsAdapter(), "live")]:
                    with self.assertRaises(BrokerError):
                        await source.token("u1", self.cid, env)
                with self.assertRaises(BrokerError):
                    await adapter.pages("u1", self.cid, "live", "kt10000", {})  # 주문은 허용 목록에 없다.
        stored = await brokers.get_credential("u1", self.cid)
        self.assertEqual(stored["token"], "private-token")
        self.assertNotIn("private-token", stored["token_ciphertext"])


@pytest.mark.parametrize("adapter,pages,tr", [(KiwoomAdapter, kiwoom_pages, "kt00001"), (LsAdapter, ls_pages, "CSPAQ12200")])
async def test_identity_change_and_missing_money_do_not_return_partial_snapshot(adapter, pages, tr):
    instance = adapter()
    link = {"credential_id": "cid", "environment": "live", "account_no": "wrong", "include_overseas": True}
    with patch.object(instance, "pages", side_effect=pages), pytest.raises(BrokerError, match="연결 계좌"):
        await instance.fetch_snapshot("u1", link)
    link["account_no"] = "1234567801" if adapter is KiwoomAdapter else "12345678901"
    async def broken(*args):
        result = await pages(*args)
        if args[3] == tr:
            if adapter is KiwoomAdapter:
                result[0]["d2_entra"] = "NaN"
            else:
                result[0]["CSPAQ12200OutBlock2"]["D2Dps"] = None
        return result
    with patch.object(instance, "pages", side_effect=broken), pytest.raises(BrokerError):
        await instance.fetch_snapshot("u1", link)


async def test_gold_uses_grams_and_settlement_cash_and_rejects_other_gold():
    instance = KiwoomAdapter()
    link = {"credential_id": "cid", "environment": "live", "account_no": "1234567801", "product": "gold"}
    with patch.object(instance, "pages", side_effect=kiwoom_pages):
        rows, _ = await instance.fetch_snapshot("u1", link)
        assert [(r["stock_code"], r["quantity"]) for r in rows] == [("KRX_GOLD", 12), ("CASH_KRW", 10000)]
    async def mini(*args):
        result = await kiwoom_pages(*args)
        if args[3] == "kt50020":
            result[0]["gold_acnt_evlt_prst"][0]["stk_cd"] = "M04020100"
        return result
    with patch.object(instance, "pages", side_effect=mini), pytest.raises(BrokerError, match="미니금"):
        await instance.fetch_snapshot("u1", link)


@pytest.mark.parametrize("factory,tr,body", [(KiwoomAdapter, "kt00018", {"return_code": 0}), (LsAdapter, "CSPAQ12200", {"rsp_cd": "00136"})])
async def test_pagination_and_error_envelopes_preserve_complete_read(factory, tr, body):
    adapter = factory()
    adapter.interval = 0
    requests = []
    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={**body, "page": len(requests)}, headers={adapter.flag_header: "Y", adapter.cursor_header: "next"})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with patch.object(adapter, "token", AsyncMock(return_value="token")), patch.object(rest, "get_http_client", AsyncMock(return_value=client)):
            with pytest.raises(BrokerError, match="반복"):
                await adapter.pages("u1", "cid", "live", tr, {})
            assert len(requests) == 2
            assert requests[1].headers[adapter.cursor_header] == "next"
            client._transport = httpx.MockTransport(lambda _: httpx.Response(200, json=body, headers={adapter.flag_header: "Y"}))
            with pytest.raises(BrokerError, match="누락"):
                await adapter.pages("u1", "cid", "live", tr, {})
            client._transport = httpx.MockTransport(lambda _: httpx.Response(200, json={adapter.result_field: "FAIL", "message": "secret-account"}))
            with pytest.raises(BrokerError) as error:
                await adapter.pages("u1", "cid", "live", tr, {})
            assert "secret-account" not in str(error.value)


async def test_ls_body_cursor_and_documented_success_codes():
    adapter, requests = LsAdapter(), []
    adapter.interval = 0
    def handler(request):
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"rsp_cd": "00000", "t0424OutBlock": {"cts_expcode": "005930" if len(requests) == 1 else ""}, "t0424OutBlock1": []})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with patch.object(adapter, "token", AsyncMock(return_value="token")), patch.object(rest, "get_http_client", AsyncMock(return_value=client)):
            pages = await adapter.pages("u1", "cid", "live", "t0424", {"cts_expcode": ""})
    assert len(pages) == 2
    assert requests[1]["t0424InBlock"]["cts_expcode"] == "005930"


async def test_kiwoom_embedded_expiry_refreshes_once_without_exposing_message():
    adapter, count = KiwoomAdapter(), 0
    adapter.interval = 0
    def handler(request):
        nonlocal count
        count += 1
        return httpx.Response(200, json={"return_code": 3, "return_msg": "[8005:Token이 유효하지 않습니다]"} if count == 1 else {"return_code": 0})
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with patch.object(adapter, "token", AsyncMock(side_effect=["old", "new"])) as token, patch.object(rest, "get_http_client", AsyncMock(return_value=client)):
            await adapter.pages("u1", "cid", "live", "ka00001", {})
    assert token.call_args.kwargs == {"expired": "old"}
    assert count == 2


@pytest.mark.parametrize("protocol", [push.KiwoomProtocol, push.LsProtocol])
async def test_notification_ack_is_not_balance_and_other_accounts_are_ignored(protocol):
    provider = "kiwoom" if protocol is push.KiwoomProtocol else "ls"
    account_no = "1234567801" if provider == "kiwoom" else "12345678901"
    if provider == "kiwoom":
        messages = [{"trnm": "LOGIN", "return_code": 0}, {"trnm": "REG", "return_code": 0}]
        messages += [{"trnm": "REAL", "data": [{"type": "04", "values": {"9201": number}}]} for number in ["9999999901", account_no]]
    else:
        messages = [{"header": {"tr_cd": tr, "rsp_cd": "00000"}} for tr in ("SC1", "AS1")]
        messages += [{"header": {"tr_cd": "SC1"}, "body": {"ordacntno": number}} for number in ["99999999999", account_no]]
    class Socket:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            return False
        send = AsyncMock()
        async def __aiter__(self):
            for message in messages:
                yield json.dumps(message)
            raise asyncio.CancelledError
    adapter = get_adapter(provider)
    links = [{"provider": provider, "google_sub": owner, "credential_id": cid, "environment": "live", "account_id": aid}
             for owner, cid, aid in [("u1", "cid", "correct"), ("u2", "cid", "other-user"), ("u1", "other", "other-key")]]
    changed = []
    with patch.object(adapter, "token", AsyncMock(return_value="token")), patch.object(push.websockets, "connect", return_value=Socket()), \
         patch.object(brokers, "list_links", AsyncMock(return_value=links)), patch.object(brokers, "get_link", AsyncMock(return_value={"account_no": account_no})):
        with pytest.raises(asyncio.CancelledError):
            await push._stream(adapter, protocol, "u1", "cid", "live", changed.append)
    assert changed == [None, "correct"]
    push.forget(provider, "u1", "cid")


async def test_common_runtime_polls_without_browser_and_ignores_other_providers():
    adapter, stop = get_adapter("ls"), asyncio.Event()
    links = [{"google_sub": "u1", "credential_id": provider, "account_id": provider, "environment": "live", "provider": provider} for provider in BROKERS]
    async def refreshed(user, aid, **kwargs):
        assert (user, aid) == ("u1", "ls")
        stop.set()
    with patch.object(brokers, "list_links", AsyncMock(return_value=links)), patch.object(adapter, "stream", AsyncMock()) as stream:
        await asyncio.wait_for(runtime.run_accounts(adapter, stop, synchronize=refreshed), 5)
    stream.assert_awaited_once()
