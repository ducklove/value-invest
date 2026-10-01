"""NH 배당 전용 자동 반영과 '미확인' 판정.

픽스처는 2026-10 운영 탐사의 마스킹된 응답 형태(필드·관계·단위)만 재현한다. 실제 계좌·금액이 아니다.
"""

import copy
import json
from datetime import date, datetime, timedelta
from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin, seed_user

from domain.dividend_receipts import DividendInput
from domain.dividend_verification import NH_CONFIRMED, NH_PARTIAL, UNCONFIRMED, annotate_calendar, annotate_receipts
from domain.portfolio_trades import TradeConflict
from repositories import (
    account_holdings,
    accounts,
    broker_activity,
    brokers,
    dividend_receipts,
    investment_insights,
    portfolio,
    snapshots,
)
from repositories.broker_secrets import BrokerError
from repositories.db import get_db, transaction
from services import dividend_calendar as cal
from services.brokers import activity, namuh, sync

ACCOUNT = "12345678901"
TOTAL = "/common/inquiry/v1/totalTransaction"
TODAY = date.today()


def ymd(day: date) -> str:
    return day.strftime("%Y%m%d")


def total_row(label="배당금", day=None, **values):
    """종합거래내역 국내 행: 통화 빈 문자열, 6자리 종목, 세전 trd_amt, 원화 세금 tax_sum."""
    return {"act_no": ACCOUNT, "trd_dt": ymd(day or TODAY - timedelta(days=10)), "cur_cd": "", "sps_cd_krl_anm": label,
            "iem_cd": "005930", "iem_nm": "삼성전자", "trd_amt": "10000", "tax_sum": "1540", "trd_orn_fee": "0",
            "trd_af_dca": "18460", "int_amt": "0", "opi_cus_fnm": "private-person", "cli_pe_fnm": "private-client", **values}


def daily_row(serial=7, day=None, **values):
    """해외주식 일별거래내역 배당 행: fc_trd_amt=현지세 후 순입금, fc_tax_sum=현지세, tax_sum=국내세(원)."""
    return {"trd_dt": ymd(day or TODAY - timedelta(days=12)), "trd_sno": serial, "act_trd_tp_nm": "입금",
            "sps_cd_nm": "외화배당금입금", "iem_krl_nm": "애플", "iem_cd": "AAPL US", "cur_cd_nm": "USD",
            "aly_xcg_rt": 1400.5, "fc_trd_amt": 8.5, "fc_tax_sum": 1.5, "fc_icm_tax": 1.5, "fc_rsd_tax": 0,
            "tax_sum": 0, "icm_tax": 0, "rsd_tax": 0, "krw_sas_amt": 14005, "trd_bf_fc_dca": 100.0, "trd_af_fc_dca": 108.5,
            "trd_bf_dca": 0, "trd_af_dca": 0, "fc_amt": 0, "krw_amt": 0, "krw_trd_amt": 0, "fc_sas_amt": 0,
            "ral_trd_dt": ymd(day or TODAY - timedelta(days=12)), **values}


def cny_row(serial=3, **values):
    # 홍콩 상장·위안 지급: 현지세 0, 국내 15.4%가 원화 예수금에서 빠져 음수가 될 수 있다.
    return daily_row(serial, iem_cd="700 HK", iem_krl_nm="텐센트", cur_cd_nm="CNY", aly_xcg_rt=210.0, fc_trd_amt=100,
                     fc_tax_sum=0, fc_icm_tax=0, tax_sum=3234, icm_tax=2940, rsd_tax=294, krw_sas_amt=21000,
                     trd_bf_fc_dca=0, trd_af_fc_dca=100, trd_bf_dca=0, trd_af_dca=-3234, **values)


class FakeNH:
    def __init__(self, total=None, daily=None):
        self.total = total if total is not None else []
        self.daily = daily
        self.calls = []

    async def __call__(self, user, cid, path, body, environment="live"):
        assert path in namuh.READ_PATHS
        self.calls.append((path, body))
        if path == TOTAL:
            start, end = body["iqr_sta_dt"], body["iqr_end_dt"]
            return [{"rsp_cd": "00000", "Output_0": [r for r in self.total if start <= r["trd_dt"] <= end]}]
        if path == activity.GB_DAILY:
            if self.daily is None:
                return [{"rsp_cd": "13578", "rsp_msg": "조회할 내역이 없습니다."}]
            return [{"rsp_cd": "00000", "Output_0": [r for r in self.daily if body["iqr_sta_dt"] <= r["trd_dt"] <= body["iqr_end_dt"]],
                     "Output_1": {"rpm_tal": 0}}]
        raise AssertionError(path)


AAPL = {"stock_code": "AAPL", "stock_name": "Apple", "quantity": 10, "avg_price": 1, "avg_price_currency": "USD", "currency": "USD"}
SAMSUNG = {"stock_code": "005930", "stock_name": "삼성전자", "quantity": 10, "avg_price": 1, "avg_price_currency": "KRW", "currency": "KRW"}
CASH = [{"stock_code": "CASH_KRW", "stock_name": "원화", "quantity": 18460, "avg_price": 1, "avg_price_currency": "KRW", "currency": "KRW"},
        {"stock_code": "CASH_USD", "stock_name": "USD 현금", "quantity": 108.5, "avg_price": 1, "avg_price_currency": "USD", "currency": "USD"}]


class NHDividendTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await seed_user("u2", "second@example.com")
        await account_holdings.ensure("u1")
        self.aid = (await accounts.create_account("u1", name="NH"))["account_id"]
        self.cid = await brokers.store_credential("u1", "test-nh-div-key", "test-nh-div-secret")
        await brokers.link_account("u1", self.aid, self.cid, ACCOUNT, "live")
        self.link = await brokers.get_link("u1", self.aid)

    async def sync(self, fake, snapshot=None, **kwargs):
        rows = copy.deepcopy(CASH) + copy.deepcopy(snapshot or [])
        with patch.object(namuh, "pages", fake), patch.object(sync, "fetch_snapshot", AsyncMock(return_value=(rows, {}))):
            return await sync.sync_account("u1", self.aid, include_activity=True, **kwargs)

    async def rows(self):
        db = await get_db()
        return [dict(r) for r in await (await db.execute("SELECT * FROM broker_transactions ORDER BY id")).fetchall()]

    # ---- 국내 ---------------------------------------------------------------
    async def test_domestic_amounts_key_and_privacy(self):
        previous = total_row("이체입금", trd_amt="10000", tax_sum="0", trd_af_dca="10000", iem_cd="", iem_nm="")
        rows = [previous, total_row(), total_row("예탁금이용료", trd_amt="12", tax_sum="2", trd_af_dca="18470")]
        with patch.object(namuh, "pages", FakeNH(rows)):
            entries = await activity.fetch("u1", self.link, TODAY - timedelta(days=30), TODAY)
        self.assertEqual(len(entries), 1)  # 이자·입출금은 보류 유지
        data = entries[0]
        self.assertEqual((data["gross_amount"], data["tax_amount"], data["net_amount"], data["income_amount"]), (10000, 1540, 8460, 8460))
        self.assertEqual((data["currency"], data["stock_code"], data["auto_kind"], data["verification"]), ("KRW", "005930", "dividend", "nh_confirmed"))
        self.assertEqual(data["balance_check"], "matched")
        text = json.dumps(entries, ensure_ascii=False)
        self.assertNotIn(ACCOUNT, text)
        self.assertNotIn("private-", text)
        with patch.object(namuh, "pages", FakeNH([total_row(trd_af_dca="99999")])):
            alone = (await activity.fetch("u1", self.link, TODAY - timedelta(days=30), TODAY))[0]
        # 앞 행이 없어 검산하지 못해도 같은 거래·같은 내용 버전이다.
        self.assertEqual(alone["balance_check"], "no_previous")
        self.assertEqual((alone["source_key"], alone["source_revision"]), (data["source_key"], data["source_revision"]))

    async def test_domestic_identical_rows_get_ordinals_and_invalid_amounts_need_review(self):
        rows = [total_row(), total_row(), total_row(iem_cd="000660", trd_amt="500", tax_sum="900")]
        with patch.object(namuh, "pages", FakeNH(rows)):
            entries = await activity.fetch("u1", self.link, TODAY - timedelta(days=30), TODAY)
        self.assertEqual(len({e["source_key"] for e in entries}), 3)
        invalid = next(e for e in entries if e["stock_code"] == "000660")
        self.assertEqual((invalid["auto_kind"], invalid["verification"], invalid["income_amount"]), ("review", "needs_review", None))
        for row in (total_row(cur_cd="USD"), total_row("외화배당금입금"), total_row("주식배당"), total_row("배당금 취소"), total_row(iem_cd="US0378331005")):
            with patch.object(namuh, "pages", FakeNH([row])):
                self.assertEqual(await activity.fetch("u1", self.link, TODAY - timedelta(days=30), TODAY), [], row)

    async def test_sync_imports_12_months_idempotent_overlap_without_cash_or_nav(self):
        old = TODAY - timedelta(days=300)
        fake = FakeNH([total_row(day=old, trd_af_dca="8460"), total_row()], [daily_row(), cny_row()])
        result = await self.sync(fake)
        self.assertIsNone(result["activity_error"])
        first_total = next(body for path, body in fake.calls if path == TOTAL)
        self.assertEqual(first_total["iqr_sta_dt"], ymd(TODAY - timedelta(days=365)))
        daily = next(body for path, body in fake.calls if path == activity.GB_DAILY)
        self.assertEqual((daily["act_trd_cfc_cd"], daily["iem_mlf_cd"], daily["iqr_sta_dt"]), ("01", "00001", ymd(TODAY - timedelta(days=365))))
        self.assertEqual(len(await self.rows()), 4)
        # 같은 60초 동기화는 재조회하지 않는다(잔고만 갱신).
        calls = len(fake.calls)
        await self.sync(fake)
        self.assertEqual(len(fake.calls), calls)
        with patch.object(sync, "ACTIVITY_MIN_INTERVAL", 0):
            await self.sync(fake)
        overlap = [body for path, body in fake.calls[calls:] if path == TOTAL][0]
        self.assertEqual(overlap["iqr_sta_dt"], ymd(TODAY - timedelta(days=sync.ACTIVITY_OVERLAP_DAYS)))
        await self.sync(fake, start=TODAY - timedelta(days=365), end=TODAY)
        rows = await self.rows()
        self.assertEqual(len(rows), 4)
        self.assertTrue(all(r["revision"] == 1 and r["kind"] == "dividend" for r in rows))
        # 현금은 증권사 잔고 스냅샷 그대로이고 NAV 입출금·좌수는 만들지 않는다.
        positions = {p["stock_code"]: p["quantity"] for p in await account_holdings.list_positions("u1", self.aid)}
        self.assertEqual(positions, {"CASH_KRW": 18460, "CASH_USD": 108.5})
        self.assertEqual(await snapshots.get_cashflows("u1"), [])
        events = await investment_insights.income_events("u1", "2000-01-01", "2100-01-01")
        self.assertEqual(sorted(e["amount_krw"] for e in events), [8460, 8460, 11904, 17766])
        self.assertTrue(all(e["kind"] == "dividend" and e["from_broker"] for e in events))
        history = await broker_activity.history("u1", self.aid)
        usd = next(i for i in history["items"] if i["currency"] == "USD")
        self.assertEqual((usd["gross_amount"], usd["tax_amount"], usd["net_amount"], usd["fx_rate"], usd["gross_krw"], usd["net_krw"]),
                         (10.0, 1.5, 8.5, 1400.5, 14005, 11904))
        self.assertEqual(usd["verification"], NH_CONFIRMED)
        cny = next(i for i in history["items"] if i["currency"] == "CNY")
        self.assertEqual((cny["stock_code"], cny["domestic_tax_krw"], cny["net_krw"]), ("0700.HK", 3234, 17766))
        krw = next(t for t in history["totals"] if t["currency"] == "KRW")
        self.assertEqual((krw["amount"], krw["amount_krw"]), (16920, 16920))
        with self.assertRaises(accounts.AccountError):
            await broker_activity.history("u2", self.aid)

    async def test_back_dated_rows_and_corrections(self):
        booked = TODAY - timedelta(days=3)
        fake = FakeNH([total_row(day=booked, ral_trd_dt=ymd(booked - timedelta(days=5)))],
                      [daily_row(day=booked, ral_trd_dt=ymd(booked - timedelta(days=20)))])
        await self.sync(fake)
        items = (await broker_activity.history("u1", self.aid))["items"]
        self.assertTrue(all(i["booked_date"] == booked.isoformat() for i in items))
        self.assertEqual(sorted(i["date"] for i in items), [(booked - timedelta(days=20)).isoformat(), (booked - timedelta(days=5)).isoformat()])
        # 같은 키의 금액 정정은 새 거래가 아니라 '확인 필요'와 버전 증가로 남는다.
        fake.total = [total_row(day=booked, ral_trd_dt=ymd(booked - timedelta(days=5)), trd_amt="11000", tax_sum="1694")]
        fake.daily = [daily_row(day=booked, ral_trd_dt=ymd(booked - timedelta(days=20)), fc_trd_amt=9.0, trd_af_fc_dca=109.0)]
        await self.sync(fake, start=booked - timedelta(days=30), end=TODAY)
        rows = await self.rows()
        self.assertEqual(len(rows), 2)
        self.assertEqual({(r["kind"], r["revision"]) for r in rows}, {("review", 2)})
        history = await broker_activity.history("u1", self.aid)
        self.assertEqual({i["verification"] for i in history["items"]}, {"needs_review"})
        self.assertEqual(await investment_insights.income_events("u1", "2000-01-01", "2100-01-01"), [])

    # ---- 해외 ---------------------------------------------------------------
    async def test_overseas_fx_unit_self_check_and_needs_review(self):
        link = self.link
        usd = activity.overseas_dividend(daily_row(), link, "AAPL")
        self.assertEqual((usd["fx_rate"], usd["auto_kind"], usd["nh_dividend"]), (1400.5, "dividend", True))
        # 100단위 표기 환율(JPY 등)은 원화 과세표준으로 확인될 때만 /100 을 적용한다.
        jpy = activity.overseas_dividend(daily_row(cur_cd_nm="JPY", iem_cd="7203 JP", aly_xcg_rt=950.0, fc_trd_amt=1000, fc_tax_sum=0,
                                                   krw_sas_amt=9500, trd_bf_fc_dca=0, trd_af_fc_dca=1000), link)
        self.assertEqual((jpy["fx_rate"], jpy["net_krw"], jpy["verification"]), (9.5, 9500, NH_CONFIRMED))
        for broken in (daily_row(aly_xcg_rt=None), daily_row(aly_xcg_rt=1.0), daily_row(krw_sas_amt=0), daily_row(fc_tax_sum=None),
                       daily_row(trd_af_fc_dca=200.0)):
            data = activity.overseas_dividend(broken, link)
            self.assertEqual((data["auto_kind"], data["verification"], data["fx_rate"], data["net_krw"]), ("review", "needs_review", None, None))
            self.assertEqual((data["currency"], data["net_amount"]), ("USD", 8.5))
        await broker_activity.store("u1", link, [activity.overseas_dividend(daily_row(aly_xcg_rt=None), link)])
        self.assertEqual(await investment_insights.income_events("u1", "2000-01-01", "2100-01-01"), [])
        for bad in (daily_row(trd_sno=None), daily_row(trd_sno=""), daily_row(trd_sno=True), daily_row(cur_cd_nm=""), daily_row(trd_dt="")):
            with self.assertRaises(BrokerError):
                activity.overseas_dividend(bad, link)
        self.assertIsNone(activity.overseas_dividend(daily_row(sps_cd_nm="외화증권매도"), link))
        same_day = [daily_row(1), daily_row(2), daily_row(1, day=TODAY - timedelta(days=11))]
        self.assertEqual(len({activity.overseas_dividend(r, link)["source_key"] for r in same_day}), 3)

    async def test_tax_refund_is_dividend_tax_adjustment_not_new_dividend(self):
        refund = daily_row(9, sps_cd_nm="외화제세금환급", fc_trd_amt=1.5, fc_tax_sum=10.0, fc_icm_tax=10.0, tax_sum=2156,
                           icm_tax=1960, rsd_tax=196, krw_sas_amt=14005, trd_bf_fc_dca=108.5, trd_af_fc_dca=110.0,
                           trd_bf_dca=0, trd_af_dca=-2156)
        data = activity.overseas_dividend(refund, self.link, "AAPL")
        self.assertEqual((data["adjustment"], data["nh_dividend"], data["gross_amount"], data["refund_base_gross"]), ("tax_refund", False, None, 10.0))
        await broker_activity.store("u1", self.link, [data])
        events = await investment_insights.income_events("u1", "2000-01-01", "2100-01-01")
        self.assertEqual([e["amount_krw"] for e in events], [2101 - 2156])
        self.assertEqual(await broker_activity.dividend_records("u1"), [])

    async def test_empty_and_incomplete_daily_responses(self):
        result = await self.sync(FakeNH([total_row()], None))
        self.assertIsNone(result["activity_error"])
        self.assertEqual(len(await self.rows()), 1)

        async def missing(user, cid, path, body, environment="live"):
            return [{"rsp_cd": "00000", "Output_0": []}] if path == TOTAL else [{"rsp_cd": "00000"}]
        with patch.object(namuh, "pages", missing):
            with self.assertRaises(BrokerError):
                await activity.fetch("u1", self.link, TODAY - timedelta(days=5), TODAY)

    async def test_empty_domestic_period_is_zero_rows_not_failure(self):
        # NH 는 내역이 없는 기간에 Output 블록 없이 13578 만 돌려준다(해외 일별거래내역과 같은 규약).
        class EmptyAware(FakeNH):
            async def __call__(self, user, cid, path, body, environment="live"):
                pages = await super().__call__(user, cid, path, body, environment)
                if path == TOTAL and not pages[0]["Output_0"]:
                    return [{"rsp_cd": "13578", "rsp_msg": "조회할 내역이 없습니다."}]
                return pages
        fake = EmptyAware([], [daily_row()])
        result = await self.sync(fake)
        self.assertIsNone(result["activity_error"])
        self.assertEqual([json.loads(r["data_json"])["currency"] for r in await self.rows()], ["USD"])
        # 최초 365일은 종합거래내역 한 번(운영 확인 범위)으로 읽는다.
        totals = [body for path, body in fake.calls if path == TOTAL]
        self.assertEqual([(b["iqr_sta_dt"], b["iqr_end_dt"]) for b in totals], [(ymd(TODAY - timedelta(days=365)), ymd(TODAY))])
        # 이후 조용한 계좌의 겹침 재조회도 빈 기간을 실패로 보지 않는다.
        fake.total = [total_row()]
        with patch.object(sync, "ACTIVITY_MIN_INTERVAL", 0):
            self.assertIsNone((await self.sync(fake))["activity_error"])
        self.assertEqual(len(await self.rows()), 2)
        # 13578 이 아닌 응답에서 Output_0 가 빠지면 계속 보류한다.
        async def missing(user, cid, path, body, environment="live"):
            return [{"rsp_cd": "00000"}]
        with patch.object(namuh, "pages", missing), self.assertRaises(BrokerError):
            await activity.fetch("u1", self.link, TODAY - timedelta(days=5), TODAY)

    async def test_failed_import_backs_off_instead_of_rereading_every_sync(self):
        class Failing(FakeNH):
            async def __call__(self, user, cid, path, body, environment="live"):
                await super().__call__(user, cid, path, body, environment)
                if path == activity.GB_DAILY:
                    raise BrokerError("일시 오류")
                return [{"rsp_cd": "00000", "Output_0": []}]
        fake = Failing()
        self.assertEqual((await self.sync(fake))["activity_error"], "일시 오류")
        first = len(fake.calls)
        self.assertEqual(first, 2)
        await self.sync(fake)  # 60초 뒤 자동 동기화: 잔고만 갱신
        self.assertEqual(len(fake.calls), first)
        state = await broker_activity.state("u1", self.aid)
        self.assertEqual((state["failed_attempts"], state["last_import_at"]), (1, None))
        db = await get_db()

        async def attempted(minutes, failures):
            stamp_text = (datetime.now(activity.KST).replace(tzinfo=None) - timedelta(minutes=minutes)).isoformat()
            await db.execute("UPDATE broker_activity_state SET attempted_at=?,failed_attempts=? WHERE account_id=?", (stamp_text, failures, self.aid))
            await db.commit()
        await attempted(15, 3)  # 3회 연속 실패 → 20분 대기
        await self.sync(fake)
        self.assertEqual(len(fake.calls), first)
        await attempted(25, 3)
        await self.sync(fake)
        self.assertEqual(len(fake.calls), first + 2)
        self.assertEqual((await broker_activity.state("u1", self.aid))["failed_attempts"], 4)
        # 기간 지정 가져오기는 백오프와 무관하고, 성공하면 실패 횟수를 지운다.
        await self.sync(FakeNH([total_row()], None), start=TODAY - timedelta(days=30), end=TODAY)
        state = await broker_activity.state("u1", self.aid)
        self.assertEqual((state["failed_attempts"], state["error"]), (0, None))

    async def test_history_totals_pair_same_rows_and_separate_tax_settlement(self):
        refund = daily_row(9, sps_cd_nm="외화제세금환급", fc_trd_amt=1.5, fc_tax_sum=10.0, fc_icm_tax=10.0, tax_sum=2156,
                           krw_sas_amt=14005, trd_bf_fc_dca=108.5, trd_af_fc_dca=110.0, trd_bf_dca=0, trd_af_dca=-2156)
        await self.receipt(n=1, account_id=None)  # 국내 배당은 수동 수취로 이미 분류
        await self.sync(FakeNH([total_row()], [daily_row(), refund]))
        totals = (await broker_activity.history("u1", self.aid))["totals"]
        self.assertEqual(totals, [{"kind": "dividend", "currency": "USD", "amount": 8.5, "amount_krw": 11904.0, "adjustment_krw": -55.0}])

    async def test_one_receipt_offsets_only_one_nh_dividend(self):
        # 수취 하나(계좌 미지정)가 같은 종목·날짜·금액의 NH 배당 두 건과 맞아도 한 건만 상쇄한다.
        await self.receipt(n=1, account_id=None)
        await self.sync(FakeNH([total_row(), total_row()], None))
        self.assertTrue(all(r["income_krw"] == 8460 for r in await self.rows()))
        events = await investment_insights.income_events("u1", "2000-01-01", "2100-01-01")
        self.assertEqual([e["amount_krw"] for e in events if e.get("from_broker")], [8460])

    async def test_server_refuses_manual_receipt_for_nh_only_dividend(self):
        await self.sync(FakeNH([total_row()], None), snapshot=[SAMSUNG])
        payload = {"stock_code": "005930", "stock_name": "삼성전자", "country": "KR", "currency": "KRW",
                   "received_date": (TODAY - timedelta(days=9)).isoformat(), "gross_amount": 10000}
        with self.assertRaises(TradeConflict):
            await dividend_receipts.preview_dividend("u1", DividendInput(**payload))
        # 다른 날짜의 배당(NH 입금 없음)은 막지 않는다.
        far = {**payload, "received_date": (TODAY - timedelta(days=60)).isoformat()}
        self.assertEqual((await dividend_receipts.preview_dividend("u1", DividendInput(**far)))["net_amount"], 8460)
        # 수동 계좌에도 보유하면 그 몫의 수취는 기록할 수 있다.
        await portfolio.save_portfolio_item("u1", "005930", "삼성전자", 5, 1, account_id=await accounts.get_default_account_id("u1"))
        self.assertEqual((await dividend_receipts.preview_dividend("u1", DividendInput(**payload)))["net_amount"], 8460)

    def test_read_paths_include_only_read_inquiries(self):
        self.assertIn(activity.GB_DAILY, namuh.READ_PATHS)
        self.assertFalse(any("order" in path.lower() for path in namuh.READ_PATHS))

    # ---- 미확인 --------------------------------------------------------------
    async def receipt(self, **values):
        result = {"account_id": self.aid, "stock_code": "005930", "stock_name": "삼성전자", "currency": "KRW", "country": "KR",
                  "received_date": (TODAY - timedelta(days=10)).isoformat(), "applied_date": TODAY.isoformat(), "gross_amount": 10000,
                  "quantity": None, "amount_per_share": None, "tax_rate": 15.4, "tax_amount": 1540, "net_amount": 8460, "fx_rate": 1,
                  "amount_krw": 8460, "cash_code": "CASH_KRW", "cash_before": 0, "cash_after": 8460, "source_key": None, "memo": "",
                  "request_id": f"r{values.get('n', 0)}", "created_at": TODAY.isoformat(), "income_event_id": None, "replayed": False}
        result.update({k: v for k, v in values.items() if k != "n"})
        async with transaction() as db:
            await db.execute("INSERT INTO portfolio_dividend_receipts (google_sub,request_id,stock_code,fingerprint,result_json,created_at) VALUES (?,?,?,?,?,?)",
                             ("u1", result["request_id"], result["stock_code"], "f", json.dumps(result, ensure_ascii=False), result["created_at"]))

    async def test_manual_receipts_are_confirmed_or_unconfirmed(self):
        await self.sync(FakeNH([total_row()], [cny_row()]))
        ago = lambda days: (TODAY - timedelta(days=days)).isoformat()  # noqa: E731
        await self.receipt(n=1, net_amount=8450)                         # 절사 차이 허용
        await self.receipt(n=2, received_date=ago(10), net_amount=8460, account_id="other")  # 다른 계좌
        await self.receipt(n=3, received_date=ago(25))                   # 날짜 범위 밖
        await self.receipt(n=4, stock_code="700.HK", currency="CNY", country="HK", received_date=ago(9), net_amount=84.6)
        await self.receipt(n=5, stock_code="000660", net_amount=8460)    # 다른 종목
        rows = {r["request_id"]: r for r in await dividend_receipts.list_receipts("u1")}
        self.assertEqual(rows["r1"]["verification"], NH_CONFIRMED)
        self.assertEqual(rows["r1"]["nh_match"]["net_amount"], 8460)
        self.assertEqual(rows["r4"]["verification"], UNCONFIRMED)  # 종목코드 형식이 다르면 확인하지 않는다
        for key in ("r2", "r3", "r5"):
            self.assertEqual((rows[key]["verification"], rows[key]["nh_match"]), (UNCONFIRMED, None), key)
        # 수동 수취가 이미 분류한 배당은 NH 배당을 다시 집계하지 않는다(한 수취 = 한 NH 기록).
        events = await investment_insights.income_events("u1", "2000-01-01", "2100-01-01")
        self.assertEqual([e["amount_krw"] for e in events], [17766])

    def test_receipt_pairing_tolerance_and_foreign_economic_net(self):
        nh = [{"id": 1, "account_id": "a", "date": "2026-09-01", "booked_date": "2026-09-03", "stock_code": "0700.HK", "symbol": "700 HK",
               "currency": "CNY", "net_amount": 100.0, "gross_amount": 100.0, "fx_rate": 210.0, "domestic_tax_krw": 3234},
              {"id": 2, "account_id": "a", "date": "2026-09-01", "booked_date": "2026-09-01", "stock_code": "", "symbol": "BRK.B US",
               "currency": "USD", "net_amount": 8.5, "gross_amount": 10.0, "fx_rate": 1400.0, "domestic_tax_krw": 0}]
        receipts = [{"stock_code": "0700.HK", "currency": "CNY", "received_date": "2026-09-07", "net_amount": 84.6},
                    {"stock_code": "BRK-B", "currency": "USD", "received_date": "2026-08-30", "net_amount": 8.51, "account_id": "a"},
                    {"stock_code": "BRK-B", "currency": "USD", "received_date": "2026-08-30", "net_amount": 8.51, "account_id": "a"},
                    {"stock_code": "0700.HK", "currency": "HKD", "received_date": "2026-09-01", "net_amount": 100}]
        result = annotate_receipts(receipts, nh)
        self.assertEqual([r["verification"] for r in result], [NH_CONFIRMED, NH_CONFIRMED, UNCONFIRMED, UNCONFIRMED])

    def test_calendar_past_payments_are_confirmed_or_unconfirmed(self):
        today = date(2026, 9, 10)
        events = [{"stock_code": "AGNC", "date": "2026-08-10", "date_kind": "payment", "type": "payment", "source_key": "AGNC:ex_date:2026-07-31"},
                  {"stock_code": "AGNC", "date": "2026-09-08", "date_kind": "payment", "type": "payment", "source_key": "AGNC:ex_date:2026-08-29"},
                  {"stock_code": "005930", "date": "2026-07-20", "date_kind": "payment", "type": "payment", "source_key": "005930:ex_date:2026-06-29"},
                  {"stock_code": "AGNC", "date": "2026-09-30", "date_kind": "ex_date", "type": "ex_date"},
                  {"stock_code": "AGNC", "date": "2026-09-01", "date_kind": "payment", "type": "estimated"},
                  {"stock_code": "AGNC", "date": "2026-10-10", "date_kind": "payment", "type": "payment"}]
        nh = [{"id": 1, "account_id": "a", "date": "2026-08-11", "stock_code": "AGNC", "currency": "USD", "net_amount": 1.0}]
        receipts = [{"stock_code": "005930", "currency": "KRW", "received_date": "2026-07-20", "net_amount": 846,
                     "source_key": "005930:ex_date:2026-06-29", "account_id": "a"}]
        nh.append({"id": 2, "account_id": "a", "date": "2026-07-21", "stock_code": "005930", "currency": "KRW", "net_amount": 846.0})
        result = annotate_calendar(events, nh, receipts, today)
        self.assertEqual([e["verification"] for e in result], [NH_CONFIRMED, UNCONFIRMED, NH_CONFIRMED, None, None, None])
        self.assertEqual(result[0]["nh_match"]["date"], "2026-08-11")
        # NH 기록 없이 수동 수취만 있으면 확인으로 보지 않는다.
        alone = annotate_calendar(events[2:3], [], receipts, today)
        self.assertEqual(alone[0]["verification"], UNCONFIRMED)

    async def test_calendar_api_marks_unconfirmed_past_payment(self):
        today = TODAY
        paid, missed = today - timedelta(days=12), today - timedelta(days=40)
        history = {"AAPL": {"events": [{"pay_date": d.isoformat(), "ex_date": None, "record_date": None, "currency": "USD", "amount_per_share": 0.25}
                                       for d in (missed, paid)], "status": "fresh", "official": True, "fetched_at": today.isoformat()}}
        await self.sync(FakeNH([], [daily_row(day=paid)]), snapshot=[AAPL])

        async def calendar():
            with patch("services.dividend_sources.get_histories", AsyncMock(return_value=history)), \
                 patch("services.portfolio.fx.fx_rate_for_currency", AsyncMock(return_value=1400)):
                result = await cal.build_calendar("u1", months_back=2, months_forward=1, today=today)
            return result, {e["date"]: e["verification"] for e in result["events"]
                            if e["stock_code"] == "AAPL" and e["date_kind"] == "payment" and e["type"] != "estimated"}
        result, status = await calendar()
        self.assertEqual(status[paid.isoformat()], NH_CONFIRMED)
        self.assertEqual(status[missed.isoformat()], UNCONFIRMED)
        self.assertGreaterEqual(result["summary"]["unconfirmed_count"], 1)
        # 같은 종목을 수동 계좌에도 보유하면 NH 입금은 NH 계좌 몫만 확인한다(수동 몫은 수취 입력 대상).
        manual = await accounts.get_default_account_id("u1")
        await portfolio.save_portfolio_item("u1", "AAPL", "Apple", 100, 1, "USD", account_id=manual)
        result, status = await calendar()
        self.assertEqual(status[paid.isoformat()], NH_PARTIAL)
        self.assertEqual(status[missed.isoformat()], UNCONFIRMED)
        self.assertEqual(result["summary"]["nh_partial_count"], 1)

    def test_calendar_mixed_account_holdings_are_partial(self):
        today = date(2026, 9, 10)
        events = [{"stock_code": "005930", "date": "2026-08-20", "date_kind": "payment", "type": "payment", "shares": 110,
                   "source_key": "005930:ex_date:2026-06-29"}]
        nh = [{"id": 1, "account_id": "nh", "date": "2026-08-20", "stock_code": "005930", "currency": "KRW", "net_amount": 3384.0}]
        self.assertEqual(annotate_calendar(events, nh, [], today, {"005930"})[0]["verification"], NH_CONFIRMED)
        mixed = annotate_calendar(events, nh, [], today, set())[0]
        self.assertEqual((mixed["verification"], mixed["nh_match"]["date"]), (NH_PARTIAL, "2026-08-20"))
        self.assertEqual(annotate_calendar(events, [], [], today, set())[0]["verification"], UNCONFIRMED)
