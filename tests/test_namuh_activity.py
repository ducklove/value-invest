import json
from datetime import date, timedelta
from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin, seed_user

from repositories import account_holdings, accounts, broker_activity, brokers, investment_insights, snapshots
from repositories.broker_secrets import BrokerError
from repositories.db import get_db
from services.brokers import activity, notifications, sync
from services.portfolio.attribution import decompose


def cash_row(serial="1", label="현금배당 입금", **values):
    return {"act_no": "12345678901", "trd_dt": activity.stamp()[:10].replace("-", ""), "trd_sno": serial,
            "cur_cd": "KRW", "sps_cd_krl_anm": label, "iem_cd": "005930", "iem_nm": "삼성전자",
            "trd_bf_dca": "10000", "trd_af_dca": "10846", "trd_amt": "1000", "tax_sum": "154",
            "trd_orn_fee": "0", "int_amt": "0", **values}


class ActivityTests(TempDbMixin):
    async def seed(self):
        await seed_user()
        await seed_user("u2", "second@example.com")
        await account_holdings.ensure("u1")
        self.aid = (await accounts.create_account("u1", name="NH"))["account_id"]
        self.cid = await brokers.store_credential("u1", "test-nh-income-key", "test-nh-income-secret")
        await brokers.link_account("u1", self.aid, self.cid, "12345678901", "live")
        self.link = await brokers.get_link("u1", self.aid)

    def normalized(self, *args, **kwargs):
        return activity.normalize(cash_row(*args, **kwargs), self.link)

    async def test_income_idempotency_reason_and_cash_are_independent(self):
        records = [self.normalized(), self.normalized("2", "예탁금이용료"), self.normalized("3", "대여수수료 입금")]
        await broker_activity.store("u1", self.link, records)
        await broker_activity.store("u1", self.link, records)
        data = await broker_activity.history("u1", self.aid)
        self.assertEqual(len(data["items"]), 3)
        self.assertEqual({r["kind"] for r in data["totals"]}, {"dividend", "interest", "other_income"})
        self.assertEqual(await account_holdings.list_positions("u1", self.aid), [])
        row = data["items"][0]
        await broker_activity.annotate("u1", self.aid, row["id"], revision=1, kind=row["kind"], reason="장기 보유 대여 수입")
        await broker_activity.store("u1", self.link, records)
        self.assertEqual((await broker_activity.history("u1", self.aid))["items"][0]["reason"], "장기 보유 대여 수입")
        with self.assertRaises(BrokerError):
            await broker_activity.annotate("u1", self.aid, row["id"], revision=1, kind=row["kind"], reason="오래된 편집")
        events = await investment_insights.income_events("u1", "2000-01-01", "2100-01-01")
        self.assertEqual(len(events), 3)
        self.assertTrue(all(e["from_broker"] for e in events))

    async def test_external_flow_only_after_initial_import_and_no_cash_mutation(self):
        await broker_activity.store("u1", self.link, [self.normalized("1", "이체입금")])
        self.assertEqual(await snapshots.get_cashflows("u1"), [])
        current = self.normalized("2", "이체입금")
        await broker_activity.store("u1", self.link, [current])
        await broker_activity.store("u1", self.link, [current])
        flows = await snapshots.get_cashflows("u1")
        self.assertEqual(len(flows), 1)
        self.assertEqual((flows[0]["type"], flows[0]["amount"]), ("deposit", 846))
        self.assertTrue(flows[0]["from_broker"])
        with self.assertRaises(snapshots.CashflowCancellationError):
            await snapshots.delete_cashflow_and_sync_cash("u1", flows[0]["id"])
        row = (await broker_activity.history("u1", self.aid))["items"][0]
        await broker_activity.annotate("u1", self.aid, row["id"], revision=1, kind="other_income", reason="이벤트 지원금 확인", income_amount=846)
        self.assertEqual(len(await snapshots.get_cashflows("u1")), 2)
        self.assertEqual(await account_holdings.list_positions("u1", self.aid), [])
        before = await snapshots.get_cashflows("u1")
        await broker_activity.annotate("u1", self.aid, row["id"], revision=2, kind="other_income", reason="지원금 사유 수정")
        self.assertEqual(await snapshots.get_cashflows("u1"), before)

    async def test_principal_rp_interest_unknown_fx_and_correction(self):
        rp = self.normalized("1", "CMA RP매도", trd_bf_dca="0", trd_af_dca="1000846", int_amt="1000", trd_amt="1001000")
        self.assertEqual((rp["auto_kind"], rp["income_amount"]), ("interest", 846))
        foreign = self.normalized("2", cur_cd="USD", trd_bf_fc_dca="1.00", trd_af_fc_dca="9.46", aly_xcg_rt="1300")
        self.assertEqual(foreign["net_amount"], 8.46)
        self.assertIsNone(foreign["fx_rate"])
        await broker_activity.store("u1", self.link, [rp, foreign])
        events = await investment_insights.income_events("u1", "2000-01-01", "2100-01-01")
        self.assertEqual([e["amount_krw"] for e in events], [846])
        row = (await broker_activity.history("u1", self.aid))["items"][0]
        await broker_activity.annotate("u1", self.aid, row["id"], revision=1, kind="dividend", reason="실제 환율 확인", fx_rate=1300)
        await broker_activity.store("u1", self.link, [foreign])
        self.assertEqual((await broker_activity.history("u1", self.aid))["items"][0]["fx_rate"], 1300)
        corrected = self.normalized("2", "배당 정정", cur_cd="USD", trd_bf_fc_dca="1", trd_af_fc_dca="8")
        await broker_activity.store("u1", self.link, [corrected])
        self.assertEqual((await broker_activity.history("u1", self.aid))["items"][0]["kind"], "review")

    async def test_unknown_principal_and_income_cancellation_require_explicit_amount(self):
        await broker_activity.store("u1", self.link, [self.normalized("1", "RP상환", trd_bf_dca="0", trd_af_dca="1000846")])
        row = (await broker_activity.history("u1", self.aid))["items"][0]
        with self.assertRaises(BrokerError):
            await broker_activity.annotate("u1", self.aid, row["id"], revision=1, kind="interest", reason="이자 확인")
        await broker_activity.annotate("u1", self.aid, row["id"], revision=1, kind="interest", reason="상환 원금 제외", income_amount=846)
        await broker_activity.store("u1", self.link, [self.normalized("2", "이자 취소", trd_bf_dca="1000", trd_af_dca="154")])
        row = (await broker_activity.history("u1", self.aid))["items"][0]
        await broker_activity.annotate("u1", self.aid, row["id"], revision=1, kind="interest", reason="이자 지급 취소 확인", income_amount=-846)
        events = await investment_insights.income_events("u1", "2000-01-01", "2100-01-01")
        self.assertEqual(sum(event["amount_krw"] for event in events), 0)
        self.assertEqual((await broker_activity.history("u1", self.aid))["totals"][0]["amount"], 0)

    async def test_late_snapshot_validation_rolls_back_income_and_cashflow(self):
        other = (await accounts.list_accounts("u1"))[0]["account_id"]
        from repositories import portfolio
        await portfolio.save_portfolio_item("u1", "005930", "삼성전자", 1, 100, account_id=other)
        await broker_activity.store("u1", self.link, [])
        rows = [{"stock_code": "005930", "stock_name": "잘못된 통화", "quantity": 1,
                 "avg_price": 1, "avg_price_currency": "USD", "currency": "USD"}]
        with patch.object(activity, "fetch", AsyncMock(return_value=[self.normalized("1", "이체입금")])), \
             patch.object(sync, "fetch_snapshot", AsyncMock(return_value=(rows, {}))):
            with self.assertRaises(BrokerError):
                await sync.sync_account("u1", self.aid, include_activity=True)
        self.assertEqual((await broker_activity.history("u1", self.aid))["items"], [])
        self.assertEqual(await snapshots.get_cashflows("u1"), [])

    async def test_partial_fetch_invalid_payloads_and_account_isolation(self):
        for data in (cash_row(trd_sno=""), cash_row(trd_af_dca="nan"), cash_row(act_no="99999999999")):
            with self.assertRaises(BrokerError):
                activity.normalize(data, self.link)
        with self.assertRaises(accounts.AccountError):
            await broker_activity.history("u2", self.aid)
        with patch.object(activity.namuh, "pages", AsyncMock(return_value=[{"message": {"usr_msg": "조회 실패"}}])):
            # 누락된 응답을 빈 거래내역으로 간주하여 진행 시각을 기록하지 않는다.
            with self.assertRaises(BrokerError):
                await activity.fetch("u1", self.link, date.today(), date.today())
        self.assertIsNone(await broker_activity.state("u1", self.aid))

    async def test_snapshot_and_ledger_commit_together(self):
        data = self.normalized()
        rows = [{"stock_code": "CASH_KRW", "stock_name": "원화", "quantity": 10846,
                 "avg_price": 1, "avg_price_currency": "KRW", "currency": "KRW"}]
        with patch.object(activity, "fetch", AsyncMock(return_value=[data])), patch.object(sync, "fetch_snapshot", AsyncMock(return_value=(rows, {}))):
            await sync.sync_account("u1", self.aid, include_activity=True)
        self.assertEqual((await account_holdings.list_positions("u1", self.aid))[0]["quantity"], 10846)
        with patch.object(activity, "fetch", AsyncMock(return_value=[self.normalized("2")])), patch.object(sync, "fetch_snapshot", AsyncMock(side_effect=BrokerError("조회 실패"))):
            with self.assertRaises(BrokerError):
                await sync.sync_account("u1", self.aid, include_activity=True)
        self.assertEqual(len((await broker_activity.history("u1", self.aid))["items"]), 1)
        self.assertEqual((await account_holdings.list_positions("u1", self.aid))[0]["quantity"], 10846)

    async def test_account_notifications_only_match_connected_owner_credential(self):
        message = {"header": {"tr_cd": "d2"}, "body": {"accountno": "12345678901"}}
        self.assertEqual(await notifications.account_for_message("u1", self.cid, "live", message), self.aid)
        for owner, cid, env in [("u2", self.cid, "live"), ("u1", "other", "live"), ("u1", self.cid, "mock")]:
            self.assertIsNone(await notifications.account_for_message(owner, cid, env, message))
        self.assertIsNone(await notifications.account_for_message("u1", self.cid, "live", {**message, "header": {"tr_cd": "d2", "rsp_cd": "00000"}}))
        await brokers.disconnect("u1", self.aid)
        self.assertIsNone(await notifications.account_for_message("u1", self.cid, "live", message))

    async def test_no_private_fields_stored_and_backfill_does_not_create_units(self):
        data = self.normalized("1", "이체입금", cus_fnm="private-person", ata_opi_act_no="private-counterparty")
        await broker_activity.store("u1", self.link, [])
        past = (date.today() - timedelta(days=30)).strftime("%Y%m%d")
        await broker_activity.store("u1", self.link, [self.normalized("2", "이체입금", trd_dt=past)])
        self.assertEqual(await snapshots.get_cashflows("u1"), [])
        self.assertNotIn("private-", json.dumps(data))
        self.assertNotIn("12345678901", json.dumps(data))
        db = await get_db()
        self.assertEqual((await (await db.execute("SELECT COUNT(*) FROM broker_transactions")).fetchone())[0], 1)


def test_interest_and_other_income_reconcile_without_becoming_external_flows():
    nav = [{"date": "2026-09-20", "total_value": 10000}, {"date": "2026-09-21", "total_value": 11000}]
    result = decompose(nav, [], [], [{"date": "2026-09-21", "kind": "interest", "amount_krw": 600},
                                    {"date": "2026-09-21", "kind": "other_income", "amount_krw": 400}])
    parts = {r["key"]: r["amount"] for r in result["components"]}
    assert parts["interest"] == 600 and parts["other_income"] == 400
    assert parts["external_flow"] == 0 and parts["unclassified"] == 0
