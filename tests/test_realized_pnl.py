"""NH 기간 손익 수집·원장·세금 요약. NH 응답은 공개 명세 형태의 합성 자료이며 실제 계좌·금액이 아니다."""

import asyncio
import copy
import json
from datetime import date, datetime, timedelta
from unittest.mock import AsyncMock, patch

import httpx
from _harness import TempDbMixin, seed_user

from core.app_factory import create_app
from domain import realized_tax as rules
from domain.timeutil import KST
from repositories import account_holdings, accounts, brokers, snapshots
from repositories import realized_pnl as ledger
from repositories.broker_secrets import BrokerError
from repositories.db import get_db, transaction
from routes import investment_insights as insights_route
from services.brokers import namuh, realized_pnl
from services.portfolio import action_board
from services.portfolio import realized_tax as service

ACCOUNT = "12345678901"
EMPTY = {"rsp_cd": "13578", "rsp_msg": "조회할 내역이 없습니다."}
NOW = datetime(2026, 11, 8, 20, 0)


def gb(day, sell, trading, charges, realized, *, qty=10, cur="USD", nat="200", **values):
    return {"orr_dt": day, "fc_sec_trd_nat_cd": nat, "fc_sec_trd_nat_nm": "미국", "trd_cur_cd": cur, "byn_qty": 0, "byn_uit_pr": 0,
            "fc_byn_amt1": 0, "sll_qty": qty, "fc_sll_amt": sell, "fc_sby_pls": trading, "fc_sdr_xps": charges, "fc_rzt_pls": realized, **values}


def stock(symbol, name, sell, trading, charges, realized, *, qty=5):
    return {"iem_cd": symbol, "iem_nm": name, "byn_qty": 0, "byn_uit_pr": 0, "fc_byn_amt1": 0, "sll_qty": qty, "fc_sll_amt": sell,
            "fc_sby_pls": trading, "fc_sdr_xps": charges, "fc_rzt_pls": realized}


class FakeNH:
    """2026-03-10(애플 이익·테슬라 손실)과 2026-06-15(엔비디아 이익) 매도, 2026-04-02는 매수만."""

    def __init__(self):
        self.calls = []
        self.fail = {}
        self.period = {
            "1": [gb("20260310", 10_000, 2_000, 10, 1_990), gb("20260402", 0, 0, 0, 0, qty=0), gb("20260615", 5_000, 5_000, 5, 4_995)],
            "2": [gb("20260310", 14_000_000, 2_800_000, 14_000, 2_786_000), gb("20260402", 0, 0, 0, 0, qty=0), gb("20260615", 7_000_000, 7_000_000, 7_000, 6_993_000)],
        }
        self.detail = {
            ("20260310", "1"): [stock("AAPL", "애플", 6_000, 3_000, 6, 2_994), stock("TSLA", "테슬라", 4_000, -1_000, 4, -1_004)],
            ("20260310", "2"): [stock("AAPL", "애플", 8_400_000, 4_200_000, 8_400, 4_191_600), stock("TSLA", "테슬라", 5_600_000, -1_400_000, 5_600, -1_405_600)],
            ("20260615", "1"): [stock("NVDA", "엔비디아", 5_000, 5_000, 5, 4_995)],
            ("20260615", "2"): [stock("NVDA", "엔비디아", 7_000_000, 7_000_000, 7_000, 6_993_000)],
        }
        self.trading = [
            {"iem_cd": "005930", "iem_nm": "삼성전자", "sll_qty": 10, "sll_amt": 700_000, "sll_abk_amt": 600_000, "pls_amt": 100_000},
            {"iem_cd": "A000660", "iem_nm": "SK하이닉스", "sll_qty": 1, "sll_amt": 200_000, "sll_abk_amt": 230_000, "pls_amt": -30_000},
            {"iem_cd": "035720", "iem_nm": "카카오", "byn_qty": 5, "byn_amt": 250_000, "sll_qty": 0, "sll_amt": 0, "pls_amt": 0},
        ]
        self.daily = {"005930": [
            {"orr_dt": "20260203", "sby_dit_cd_nm": "매도", "cns_qty": 6, "cns_amt": 420_000, "phs_amt": 360_000, "pls_amt": 60_000},
            {"orr_dt": "20260115", "sby_dit_cd_nm": "매수", "cns_qty": 10, "cns_amt": 600_000, "phs_amt": 0, "pls_amt": 0},
            {"orr_dt": "20260504", "sby_dit_cd_nm": "매도", "cns_qty": 4, "cns_amt": 280_000, "phs_amt": 240_000, "pls_amt": 40_000},
        ]}

    def count(self, path):
        return sum(1 for called, _ in self.calls if called == path)

    async def __call__(self, user, cid, path, body, environment="live"):
        assert path in namuh.READ_PATHS, path
        assert environment == "live"
        assert body["act_no"] == ACCOUNT
        self.calls.append((path, dict(body)))
        if path in self.fail:
            raise BrokerError(self.fail[path])
        if path == realized_pnl.KR_TRADING:
            if not body["iqr_sta_dt"].startswith("2026"):
                return [dict(EMPTY)]
            return [{"rsp_cd": "00000", "Output_0": {"pls_amt": sum(row["pls_amt"] for row in self.trading)}, "Output_1": copy.deepcopy(self.trading)}]
        if path == realized_pnl.KR_DAILY:
            rows = self.daily.get(body["iem_cd"])
            return [{"rsp_cd": "00000", "Output_0": copy.deepcopy(rows)}] if rows else [dict(EMPTY)]
        if path == realized_pnl.GB_PERIOD:
            rows = [row for row in self.period[body["iqr_dit"]] if body["sta_orr_dt"] <= row["orr_dt"] <= body["end_orr_dt"]]
            if not rows:
                return [dict(EMPTY)]
            total = {"fc_rzt_pls_sum": sum(row["fc_rzt_pls"] for row in rows)}
            return [{"rsp_cd": "00000", "Output_0": total, "Output_1": copy.deepcopy(rows)}]
        if path == realized_pnl.GB_DETAIL:
            rows = self.detail.get((body["orr_dt"], body["iqr_dit"]))
            return [{"rsp_cd": "00000", "Output_0": copy.deepcopy(rows)}] if rows else [dict(EMPTY)]
        raise AssertionError(path)


class CollectionBase(TempDbMixin):
    async def seed(self):
        await seed_user()
        await seed_user("u2", "second@example.com")
        await account_holdings.ensure("u1")
        self.aid = (await accounts.create_account("u1", name="NH 위탁"))["account_id"]
        self.cid = await brokers.store_credential("u1", "test-nh-pnl-key", "test-nh-pnl-secret")
        await brokers.link_account("u1", self.aid, self.cid, ACCOUNT, "live")
        self.link = await brokers.get_link("u1", self.aid)

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.clock = NOW
        self.nh = FakeNH()
        self.income_sync = AsyncMock(return_value={"ok": True, "activity_error": None})
        patches = [patch.object(ledger, "stamp", lambda: self.clock.isoformat(timespec="microseconds")),
                   patch.object(realized_pnl.namuh, "pages", self.nh),
                   patch.object(realized_pnl, "sync_account", self.income_sync),
                   patch.object(realized_pnl, "_fx_reference", AsyncMock(return_value=1400.0))]
        for item in patches:
            item.start()
            self.addCleanup(item.stop)
        service._upcoming_cache.clear()

    async def collect(self, *, seconds=0, interval=realized_pnl.DEFAULT_INTERVAL_SECONDS):
        self.clock += timedelta(seconds=seconds)
        await realized_pnl.collect_account("u1", self.aid, now=self.clock.replace(tzinfo=KST), interval_seconds=interval)

    async def ledger_rows(self):
        return await ledger.rows("u1")

    async def overseas_total(self):
        return sum(row["realized_krw"] or 0 for row in await self.ledger_rows() if row["market"] == "overseas")


class RealizedCollectionTests(CollectionBase):
    async def test_requests_follow_spec_and_only_use_read_paths(self):
        await self.collect()
        self.assertFalse(any("order" in path.lower() for path in namuh.READ_PATHS))
        for path in (realized_pnl.GB_PERIOD, realized_pnl.GB_DETAIL, realized_pnl.KR_TRADING, realized_pnl.KR_DAILY):
            self.assertIn(path, namuh.READ_PATHS)
        periods = [body for path, body in self.nh.calls if path == realized_pnl.GB_PERIOD and body["sta_orr_dt"] == "20260101"]
        self.assertEqual({body["iqr_dit"] for body in periods}, {"1", "2"})
        self.assertTrue(all(body["end_orr_dt"] == "20261108" and body["fc_sec_trd_nat_cd"] == "000" for body in periods))
        details = [body for path, body in self.nh.calls if path == realized_pnl.GB_DETAIL]
        self.assertEqual(details[0], {"act_no": ACCOUNT, "orr_dt": "20260310", "fc_sec_trd_nat_cd": "200", "trd_cur_cd": "USD", "iqr_dit": "1"})
        trading = next(body for path, body in self.nh.calls if path == realized_pnl.KR_TRADING and body["iqr_sta_dt"] == "20260101")
        self.assertEqual(trading, {"act_no": ACCOUNT, "iqr_sta_dt": "20260101", "iqr_end_dt": "20261108"})
        daily = [body["iem_cd"] for path, body in self.nh.calls if path == realized_pnl.KR_DAILY]
        self.assertEqual(sorted(daily), ["000660", "005930"])  # 매수만 한 카카오는 일별 조회를 하지 않는다.

    async def test_overseas_ledger_keeps_both_bases_without_account_number(self):
        await self.collect()
        rows = [row for row in await self.ledger_rows() if row["market"] == "overseas"]
        self.assertEqual(sorted(row["stock_code"] for row in rows), ["AAPL", "NVDA", "TSLA"])
        self.assertEqual(await self.overseas_total(), 9_779_000)  # 2,786,000 + 6,993,000
        tesla = next(row for row in rows if row["stock_code"] == "TSLA")
        self.assertEqual((tesla["realized_native"], tesla["realized_krw"], tesla["sell_native"], tesla["sell_krw"]), (-1004, -1_405_600, 4000, 5_600_000))
        self.assertEqual(tesla["issues"], [])
        self.assertEqual(json.loads(tesla["data_json"])["krw"]["charges"], 5_600)
        db = await get_db()
        raw = await (await db.execute("SELECT * FROM broker_realized_pnl")).fetchall()
        self.assertNotIn(ACCOUNT, json.dumps([dict(row) for row in raw], ensure_ascii=False))
        state = await ledger.state("u1", self.aid, 2026, "overseas")
        self.assertEqual(state["broker_total"], 9_779_000)
        self.assertEqual(state["failed_attempts"], 0)

    async def test_domestic_daily_rows_reconcile_and_fall_back_to_stock_total(self):
        await self.collect()
        rows = [row for row in await self.ledger_rows() if row["market"] == "domestic"]
        samsung = sorted((row["trade_date"], row["realized_krw"]) for row in rows if row["stock_code"] == "005930")
        self.assertEqual(samsung, [("2026-02-03", 60_000), ("2026-05-04", 40_000)])  # 매수 행은 넣지 않는다.
        hynix = next(row for row in rows if row["stock_code"] == "000660")
        # 일별 응답이 비어도 연도 합계가 맞도록 종목 합계 1행을 유지하고 다시 조회하지 않는다.
        self.assertEqual((hynix["realized_krw"], hynix["issues"]), (-30_000, ["detail_missing"]))
        self.assertEqual(sum(row["realized_krw"] for row in rows), 70_000)
        self.assertFalse(any(row["stock_code"] == "035720" for row in rows))

    async def test_recollection_is_idempotent_and_calls_only_period_apis(self):
        await self.collect()
        before = {row["id"]: row for row in await self.ledger_rows()}
        self.nh.calls.clear()
        await self.collect(seconds=realized_pnl.DEFAULT_INTERVAL_SECONDS)
        after = {row["id"]: row for row in await self.ledger_rows()}
        self.assertEqual(before, after)
        self.assertEqual(self.nh.count(realized_pnl.GB_DETAIL), 0)
        self.assertEqual(self.nh.count(realized_pnl.KR_DAILY), 0)
        self.assertEqual(self.nh.count(realized_pnl.GB_PERIOD), 2)
        self.assertEqual(self.nh.count(realized_pnl.KR_TRADING), 1)  # 지난해는 7일 간격이라 조회하지 않는다.

    async def test_not_due_before_interval(self):
        await self.collect()
        self.nh.calls.clear()
        await self.collect(seconds=realized_pnl.DEFAULT_INTERVAL_SECONDS - 1)
        self.assertEqual(self.nh.calls, [])

    async def test_broker_correction_refetches_only_changed_date(self):
        await self.collect()
        untouched = {row["id"] for row in await self.ledger_rows() if row["trade_date"] == "2026-06-15"}
        self.nh.period["2"][0] = gb("20260310", 14_000_000, 2_700_000, 14_000, 2_686_000)
        self.nh.detail[("20260310", "2")][1] = stock("TSLA", "테슬라", 5_600_000, -1_500_000, 5_600, -1_505_600)
        self.nh.calls.clear()
        await self.collect(seconds=realized_pnl.DEFAULT_INTERVAL_SECONDS)
        details = [body["orr_dt"] for path, body in self.nh.calls if path == realized_pnl.GB_DETAIL]
        self.assertEqual(details, ["20260310", "20260310"])
        rows = await self.ledger_rows()
        self.assertEqual({row["id"] for row in rows if row["trade_date"] == "2026-06-15"}, untouched)
        self.assertEqual(next(row for row in rows if row["stock_code"] == "TSLA")["realized_krw"], -1_505_600)
        self.assertEqual(await self.overseas_total(), 9_679_000)  # 2,686,000 + 6,993,000

    async def test_cancelled_date_disappears_from_ledger(self):
        await self.collect()
        self.nh.period["1"].pop()
        self.nh.period["2"].pop()
        await self.collect(seconds=realized_pnl.DEFAULT_INTERVAL_SECONDS)
        self.assertEqual(await self.overseas_total(), 2_786_000)
        self.assertFalse(any(row["stock_code"] == "NVDA" for row in await self.ledger_rows()))

    async def test_detail_mismatch_keeps_period_total_and_is_not_refetched(self):
        self.nh.detail[("20260310", "2")][0] = stock("AAPL", "애플", 8_400_000, 4_200_000, 8_400, 4_000_000)
        await self.collect()
        rows = [row for row in await self.ledger_rows() if row["trade_date"] == "2026-03-10"]
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]["symbol"], rows[0]["issues"], rows[0]["realized_krw"]), ("", ["detail_missing"], 2_786_000))
        self.assertEqual(await self.overseas_total(), 9_779_000)
        self.nh.calls.clear()
        await self.collect(seconds=realized_pnl.DEFAULT_INTERVAL_SECONDS)
        self.assertEqual(self.nh.count(realized_pnl.GB_DETAIL), 0)

    async def test_detail_failure_keeps_totals_and_retries_pending_groups(self):
        self.nh.fail[realized_pnl.GB_DETAIL] = "나무 조회 한도를 초과했습니다."
        await self.collect()
        self.assertEqual(await self.overseas_total(), 9_779_000)  # 합계 행은 이미 반영됐다.
        rows = [row for row in await self.ledger_rows() if row["market"] == "overseas"]
        self.assertTrue(all(row["issues"] == ["detail_pending"] and row["symbol"] == "" for row in rows))
        state = await ledger.state("u1", self.aid, 2026, "overseas")
        self.assertEqual(state["failed_attempts"], 1)
        self.assertEqual(state["error"], "나무 조회 한도를 초과했습니다.")
        self.assertIsNotNone(state["last_success_at"])
        del self.nh.fail[realized_pnl.GB_DETAIL]
        await self.collect(seconds=realized_pnl.FAILURE_BACKOFF_SECONDS)
        self.assertEqual(sorted(row["stock_code"] for row in await self.ledger_rows() if row["market"] == "overseas"), ["AAPL", "NVDA", "TSLA"])
        state = await ledger.state("u1", self.aid, 2026, "overseas")
        self.assertEqual((state["failed_attempts"], state["error"]), (0, None))

    async def test_failure_preserves_ledger_and_backs_off(self):
        await self.collect()
        before = await self.ledger_rows()
        self.nh.fail[realized_pnl.GB_PERIOD] = "나무 조회 서버에 연결하지 못했습니다."
        await self.collect(seconds=realized_pnl.DEFAULT_INTERVAL_SECONDS)
        self.assertEqual(await self.ledger_rows(), before)
        state = await ledger.state("u1", self.aid, 2026, "overseas")
        self.assertEqual(state["failed_attempts"], 1)
        # 한 구분의 실패가 다른 구분을 막지 않는다.
        self.assertIsNone((await ledger.state("u1", self.aid, 2026, "domestic"))["error"])
        self.nh.calls.clear()
        await self.collect(seconds=realized_pnl.FAILURE_BACKOFF_SECONDS - 1)
        self.assertEqual(self.nh.count(realized_pnl.GB_PERIOD), 0)
        await self.collect(seconds=1)
        self.assertEqual(self.nh.count(realized_pnl.GB_PERIOD), 1)  # 5분 뒤 재시도(실패 즉시 원화 기준은 부르지 않는다)
        self.assertEqual((await ledger.state("u1", self.aid, 2026, "overseas"))["failed_attempts"], 2)
        self.nh.calls.clear()
        await self.collect(seconds=realized_pnl.FAILURE_BACKOFF_SECONDS)  # 2회 실패 후에는 10분 간격
        self.assertEqual(self.nh.count(realized_pnl.GB_PERIOD), 0)

    async def test_missing_block_is_a_failure_but_known_empty_is_zero_rows(self):
        await self.collect()
        pages = {"rsp_cd": "00000", "rsp_msg": "정상"}
        with patch.object(realized_pnl.namuh, "pages", AsyncMock(return_value=[pages])):
            await self.collect(seconds=realized_pnl.DEFAULT_INTERVAL_SECONDS)
        self.assertEqual(await self.overseas_total(), 9_779_000)  # 누락을 0건으로 처리해 원장을 지우지 않는다.
        self.assertIn("NH 손익 응답", (await ledger.state("u1", self.aid, 2026, "overseas"))["error"])
        self.assertEqual(realized_pnl._rows([dict(EMPTY)], "Output_1", "Output_0"), [])
        self.assertEqual(realized_pnl._rows([{"rsp_cd": "00000", "Output_0": {"pls_amt": 0, "pft_rt": "0"}}], "Output_1", "Output_0"), [])
        with self.assertRaises(BrokerError):
            realized_pnl._rows([{"rsp_cd": "00000", "Output_0": {"pls_amt": 100}}], "Output_1", "Output_0")
        with self.assertRaises(BrokerError):
            realized_pnl._rows([{"rsp_cd": "00000", "Output_1": "x"}], "Output_1")

    async def test_malformed_values_hold_the_update(self):
        await self.collect()
        self.nh.period["1"][0] = gb("20260310", "nan", 2_000, 10, 1_990)
        await self.collect(seconds=realized_pnl.DEFAULT_INTERVAL_SECONDS)
        self.assertEqual(await self.overseas_total(), 9_779_000)
        self.assertIsNotNone((await ledger.state("u1", self.aid, 2026, "overseas"))["error"])
        for bad in (gb("20270101", 1, 1, 0, 1), gb("20260310", 1, 1, 0, 1, nat="US"), gb("20260310", 1, 1, 0, 1, cur="")):
            with self.assertRaises(BrokerError):
                realized_pnl.merge_overseas([bad], [], date(2026, 1, 1), date(2026, 11, 8))

    async def test_owner_and_relinked_account_are_rejected(self):
        await self.collect()
        with self.assertRaises(BrokerError):
            await realized_pnl.collect_account("u2", self.aid, now=self.clock.replace(tzinfo=KST))
        with self.assertRaises(BrokerError):
            await ledger.apply_period("u2", self.link, 2026, "overseas", {}, None)
        with self.assertRaises(BrokerError):
            await ledger.record_failure("u2", self.link, 2026, "overseas", "x")
        with self.assertRaises(BrokerError):
            await ledger.apply_period("u1", {**self.link, "account_fingerprint": "other"}, 2026, "overseas", {}, None)
        self.assertEqual(await ledger.rows("u2"), [])
        self.assertEqual(await ledger.states("u2"), [])
        self.assertEqual((await service.build_summary("u2", today=date(2026, 11, 8)))["available"], False)

    async def test_ledger_survives_disconnect_and_blocks_account_delete(self):
        await self.collect()
        await brokers.disconnect("u1", self.aid)
        self.assertEqual(await self.overseas_total(), 9_779_000)
        with self.assertRaisesRegex(accounts.AccountError, "거래내역"):
            await accounts.delete_account("u1", self.aid)

    async def test_income_backfill_runs_once_from_new_year_through_existing_path(self):
        await self.collect()
        self.income_sync.assert_awaited_once_with("u1", self.aid, include_activity=True, start=date(2026, 1, 1), end=date(2026, 11, 8))
        await self.collect(seconds=realized_pnl.DEFAULT_INTERVAL_SECONDS)
        self.income_sync.assert_awaited_once()
        self.assertIsNotNone((await ledger.state("u1", self.aid, 2026, "income"))["last_success_at"])

    async def test_income_backfill_failure_is_retried_after_backoff(self):
        self.income_sync.return_value = {"ok": True, "activity_error": "거래내역 조회 실패"}
        await self.collect()
        state = await ledger.state("u1", self.aid, 2026, "income")
        self.assertEqual((state["error"], state["failed_attempts"], state["last_success_at"]), ("거래내역 조회 실패", 1, None))
        self.income_sync.side_effect = BrokerError("잔고 실패")
        await self.collect(seconds=60)
        self.assertEqual(self.income_sync.await_count, 1)
        await self.collect(seconds=realized_pnl.FAILURE_BACKOFF_SECONDS)
        self.assertEqual(self.income_sync.await_count, 2)
        self.assertEqual((await ledger.state("u1", self.aid, 2026, "income"))["error"], "잔고 실패")

    async def test_non_stock_nh_link_only_backfills_income_and_mock_is_skipped(self):
        async with transaction() as db:
            await db.execute("UPDATE broker_account_links SET product='krfuture' WHERE account_id=?", (self.aid,))
        await self.collect()
        self.income_sync.assert_awaited_once()
        self.assertEqual(self.nh.calls, [])
        self.income_sync.reset_mock()
        async with transaction() as db:
            await db.execute("UPDATE broker_account_links SET environment='mock' WHERE account_id=?", (self.aid,))
        await self.collect(seconds=realized_pnl.DEFAULT_INTERVAL_SECONDS)
        self.income_sync.assert_not_awaited()

    async def test_collect_due_isolates_accounts_and_run_loop_stops(self):
        for error in (BrokerError("실패"), httpx.ConnectError("끊김"), TimeoutError(), ValueError("x")):
            failing = AsyncMock(side_effect=error)
            with patch.object(realized_pnl, "collect_account", failing):
                await realized_pnl.collect_due()  # 어떤 일시 오류도 수집 루프를 끝내지 않는다.
            failing.assert_awaited_once()
        stop = asyncio.Event()

        async def collect_once(**_):
            stop.set()

        with patch.object(realized_pnl, "collect_due", side_effect=collect_once) as due:
            await asyncio.wait_for(realized_pnl.run_loop(stop, initial_delay_seconds=0), timeout=2)
        due.assert_called_once()
        self.nh.calls.clear()
        stopped = asyncio.Event()
        stopped.set()
        await asyncio.wait_for(realized_pnl.run_loop(stopped, initial_delay_seconds=0), timeout=1)
        self.assertEqual(self.nh.calls, [])


class OverseasChecksTests(CollectionBase):
    async def test_krw_basis_that_is_not_krw_is_excluded_from_tax(self):
        # 원화 기준 응답이 거래통화 금액 그대로면(환율 1) 세액을 크게 줄이므로 합계에 넣지 않는다.
        self.nh.period["2"] = copy.deepcopy(self.nh.period["1"])
        self.nh.detail[("20260310", "2")] = copy.deepcopy(self.nh.detail[("20260310", "1")])
        self.nh.detail[("20260615", "2")] = copy.deepcopy(self.nh.detail[("20260615", "1")])
        await self.collect()
        rows = [row for row in await self.ledger_rows() if row["market"] == "overseas"]
        self.assertTrue(rows)
        self.assertTrue(all(row["realized_krw"] is None and "fx_implausible" in row["issues"] for row in rows))
        summary = await service.build_summary("u1", today=date(2026, 11, 8))
        self.assertEqual(summary["current"]["tax_status"], "incomplete")
        self.assertEqual(summary["current"]["overseas"]["excluded_rows"], len(rows))

    async def test_missing_krw_basis_is_flagged_not_counted_as_zero_gain(self):
        self.nh.period["2"] = []
        await self.collect()
        rows = [row for row in await self.ledger_rows() if row["market"] == "overseas"]
        self.assertEqual({row["trade_date"] for row in rows}, {"2026-03-10", "2026-06-15"})
        self.assertTrue(all(row["realized_krw"] is None and "krw_missing" in row["issues"] for row in rows))
        self.assertEqual((await service.build_summary("u1", today=date(2026, 11, 8)))["current"]["tax_status"], "incomplete")

    async def test_krw_rows_labelled_krw_join_by_date_and_nation(self):
        native = [gb("20260310", 100, 10, 1, 9)]
        krw = [gb("20260310", 140_000, 14_000, 1_400, 12_600, cur="KRW")]
        groups = realized_pnl.merge_overseas(native, krw, date(2026, 1, 1), date(2026, 11, 8))
        self.assertEqual(list(groups), ["gb:2026-03-10:200:USD"])
        self.assertEqual(len(groups["gb:2026-03-10:200:USD"]["krw"]), 1)
        # 어느 거래통화인지 알 수 없는 원화 행은 버리지 않고 확인 필요로 남긴다.
        ambiguous = [gb("20260310", 100, 10, 1, 9, cur="HKD", nat="120"), gb("20260310", 100, 10, 1, 9, cur="CNY", nat="120")]
        groups = realized_pnl.merge_overseas(ambiguous, [gb("20260310", 1, 1, 0, 1, cur="KRW", nat="120")], date(2026, 1, 1), date(2026, 11, 8))
        self.assertEqual(sorted(groups), ["gb:2026-03-10:120:CNY", "gb:2026-03-10:120:HKD", "gb:2026-03-10:120:KRW"])
        entry = realized_pnl.overseas_group_entry(self.link, groups["gb:2026-03-10:120:KRW"], 1.0)
        self.assertEqual(entry["realized_krw"], 1)
        self.assertIn("native_missing", entry["issues"])

    def test_fx_plausibility_and_arithmetic_flags(self):
        from decimal import Decimal as D
        self.assertTrue(realized_pnl.fx_plausible(D(100), D(140_000), 1400.0))
        self.assertTrue(realized_pnl.fx_plausible(D(100), D(100_000), 1400.0))  # 환율이 많이 달라도 단위가 맞으면 통과
        self.assertFalse(realized_pnl.fx_plausible(D(100), D(100), 1400.0))
        self.assertFalse(realized_pnl.fx_plausible(D(100), D(100), None))
        self.assertTrue(realized_pnl.fx_plausible(D(0), D(100), 1400.0))
        native = {"trading": D(100), "charges": D(1), "realized": D(99), "sell_amount": D(1000)}
        krw = {"trading": D(140_000), "charges": D(1_400), "realized": D(138_600), "sell_amount": D(1_400_000)}
        self.assertEqual(realized_pnl.overseas_issues(native, krw, 1400.0), ([], True))
        broken = {**krw, "realized": D(130_000)}
        self.assertEqual(realized_pnl.overseas_issues(native, broken, 1400.0), (["arith_mismatch"], True))
        self.assertEqual(realized_pnl.overseas_issues(native, None, 1400.0), (["krw_missing"], False))
        self.assertEqual(realized_pnl.overseas_issues(None, krw, 1400.0), (["native_missing"], True))


class SummaryTests(CollectionBase):
    async def _dividend(self, key, *, kind="dividend", **data):
        async with transaction() as db:
            await db.execute(
                "INSERT INTO broker_transactions (google_sub,account_id,source_key,source_revision,data_json,kind,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)",
                ("u1", self.aid, key, key, json.dumps({"nh_dividend": True, **data}), kind, "t", "t"))

    async def _evidence(self, day, kind, amount):
        async with transaction() as db:
            await db.execute("INSERT INTO broker_cash_review_evidence VALUES (?,?,?,?,?,?,?,?)",
                             ("u1", self.aid, self.link["account_fingerprint"], day, day, "KRW", kind, amount))

    async def _holding(self, account, code, name, quantity, avg, currency):
        async with transaction() as db:
            await db.execute("INSERT INTO account_holdings VALUES (?,?,?,?,?,?,?,?,?,?)",
                             ("u1", account, code, name, quantity, avg, currency, currency, "t", "t"))

    async def _seed_summary(self):
        await self.collect()
        await self._dividend("kr", date="2026-04-10", currency="KRW", gross_amount=5_000_000, net_amount=4_230_000)
        await self._dividend("us", date="2026-05-10", currency="USD", gross_amount=5_000, gross_krw=8_000_000)
        await self._dividend("review", kind="review", date="2026-06-10", currency="KRW", gross_amount=1_000_000)
        await self._dividend("nofx", date="2026-07-10", currency="USD", gross_amount=100, gross_krw=None)
        await self._dividend("old", date="2025-12-10", currency="KRW", gross_amount=9_000_000)
        await self._evidence("2026-10-11", "interest", 84_600)  # 세후 → 세전 100,000
        await self._evidence("2026-10-12", "mmw_interest", 200_000)
        await self._evidence("2026-10-13", "other_income", 999_999)  # 대여료는 금융소득이 아니다.
        await self._evidence("2025-10-11", "interest", 84_600)
        await self._holding(self.aid, "AAPL", "애플", 10, 200, "USD")
        await self._holding(self.aid, "TSLA", "테슬라", 5, 300, "USD")
        await self._holding(self.aid, "NVDA", "엔비디아", 1, 100, "USD")
        await self._holding(self.aid, "CASH_USD", "USD 현금", 100, 1, "USD")
        await self._holding(self.aid, "005930", "삼성전자", 10, 70_000, "KRW")
        await snapshots.save_stock_snapshots("u1", "2026-11-06", [
            {"stock_code": "AAPL", "market_value": 1_960_000, "quantity": 10, "unit_price": 196_000, "currency": "USD", "fx_rate": 1400},
            {"stock_code": "TSLA", "market_value": 2_450_000, "quantity": 5, "unit_price": 490_000, "currency": "USD", "fx_rate": 1400},
            {"stock_code": "NVDA", "market_value": 70_000, "quantity": 1, "unit_price": 70_000, "currency": "USD", "fx_rate": 1400, "priced_from_fallback": True},
        ])
        self.manual = (await accounts.create_account("u1", name="수동 계좌"))["account_id"]
        await self._holding(self.manual, "AAPL", "애플", 10, 100, "USD")
        self.isa = (await accounts.create_account("u1", name="ISA", type="isa"))["account_id"]
        await self._holding(self.isa, "005930", "삼성전자", 1, 70_000, "KRW")

    def _calendar(self, events=None):
        calendar = {"events": events if events is not None else [
            {"date": "2026-12-10", "stock_code": "AAPL", "expected_amount_krw": 5_800_000, "shares": 20, "cashflow": True, "date_status": "announced"},
        ]}
        return patch.object(service.dividend_calendar, "build_calendar", AsyncMock(return_value=calendar))

    async def test_summary_matches_hand_calculation(self):
        await self._seed_summary()
        with self._calendar() as calendar:
            summary = await service.build_summary("u1", today=date(2026, 11, 8))
        calendar.assert_awaited_once_with("u1", months_back=0, months_forward=1, today=date(2026, 11, 8))
        self.assertIs(summary["fields_verified"], False)
        current = summary["current"]
        self.assertEqual(current["year"], 2026)
        self.assertEqual(current["overseas"]["realized_krw"], 9_779_000)
        self.assertEqual(current["overseas"]["total_gap_krw"], 0)
        self.assertEqual(current["domestic"]["realized_krw"], 70_000)
        self.assertEqual(current["domestic"]["total_gap_krw"], 0)
        self.assertEqual(current["tax_status"], "estimated")
        # (9,779,000 − 2,500,000) × 22% = 1,601,380
        self.assertEqual(current["tax"]["estimated_tax_krw"], 1_601_380)
        self.assertEqual(current["tax"]["deduction_remaining_krw"], 0)
        self.assertEqual([row["year"] for row in summary["years"]], [2026, 2025])
        self.assertEqual(summary["years"][1]["overseas"]["realized_krw"], 0)

        harvest = summary["loss_harvest"]
        # 애플 10주 × (196,000 − 200 × 1,400) = −840,000. 840,000 × 22% = 184,800 감소. 테슬라는 평가이익.
        self.assertEqual([(row["stock_code"], row["unrealized_krw"], row["tax_saving_krw"]) for row in harvest["candidates"]], [("AAPL", -840_000, 184_800)])
        self.assertEqual(harvest["max_saving_krw"], 184_800)
        self.assertEqual(harvest["tax_after_all_krw"], 1_416_580)
        self.assertEqual(harvest["loss_to_zero_tax_krw"], 7_279_000)
        self.assertEqual(harvest["position_count"], 3)  # 애플·테슬라·엔비디아. 현금·국내주식과 NH 외 계좌는 제외.
        self.assertEqual(harvest["unpriced_count"], 1)  # 대체 시세를 쓴 엔비디아
        self.assertEqual(harvest["as_of"], "2026-11-06")

        income = summary["financial_income"]
        self.assertEqual(income["dividends_krw"], 13_000_000)  # 5,000,000 + 8,000,000
        self.assertEqual(income["interest_krw"], 300_000)  # 100,000 + 200,000
        self.assertEqual(income["received_krw"], 13_300_000)
        # 일정 5,800,000원은 전 계좌 20주 기준, NH 몫 10주 → 2,900,000원.
        self.assertEqual(income["upcoming_krw"], 2_900_000)
        self.assertEqual(income["upcoming"]["other_accounts_krw"], 2_900_000)
        self.assertEqual(income["projected_krw"], 16_200_000)
        self.assertEqual(income["ratio_pct"], 81.0)
        self.assertEqual(income["status"], "near")
        self.assertEqual((income["review_rows"], income["unverified_rows"], income["dividend_rows"]), (1, 1, 2))
        self.assertIs(income["interest_complete"], True)

        coverage = summary["coverage"]
        self.assertEqual(coverage["nh_accounts"], ["NH 위탁"])
        self.assertEqual(coverage["not_nh_count"], 1)
        self.assertEqual({row["name"]: row["reason"] for row in coverage["excluded_accounts"]}, {"수동 계좌": "not_nh", "ISA": "account_type"})
        self.assertEqual(coverage["errors"], [])
        self.assertTrue(any("선입선출" in note for note in summary["notes"]))
        self.assertNotIn(ACCOUNT, json.dumps(summary, ensure_ascii=False))

    async def test_interest_rows_in_income_ledger_count_once(self):
        await self.collect()
        await self._evidence("2026-10-11", "interest", 84_600)
        await self._evidence("2026-10-12", "interest", 8_460)  # 수입 원장에 없는 날은 일별 합계 사용: 세전 10,000

        async def income_row(key, day, income_krw, **data):
            async with transaction() as db:
                await db.execute(
                    "INSERT INTO broker_transactions (google_sub,account_id,source_key,source_revision,data_json,kind,income_krw,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                    ("u1", self.aid, key, key, json.dumps({"date": day, **data}), "interest", income_krw, "t", "t"))

        await income_row("a", "2026-10-11", 84_600, currency="KRW", gross_amount=100_000)  # 세전 표시 그대로
        await income_row("b", "2026-10-20", 8_460, currency="KRW", gross_amount=None)  # 세후만 있으면 세전 10,000
        await income_row("c", "2026-10-21", None, currency="USD", gross_amount=5)  # 원화를 모르면 금액에 넣지 않는다
        await income_row("d", "2025-10-21", 8_460, currency="KRW", gross_amount=10_000)
        with self._calendar([]):
            income = (await service.build_summary("u1", today=date(2026, 11, 8)))["financial_income"]
        self.assertEqual(income["interest_krw"], 120_000)  # 100,000 + 10,000 + 10,000
        self.assertEqual(income["unverified_rows"], 1)
        self.assertEqual(income["interest_from"], "2026-10-11")

    async def test_calendar_failure_keeps_received_amounts(self):
        await self._seed_summary()
        with patch.object(service.dividend_calendar, "build_calendar", AsyncMock(side_effect=TimeoutError())):
            income = (await service.build_summary("u1", today=date(2026, 11, 8)))["financial_income"]
        self.assertEqual(income["received_krw"], 13_300_000)
        self.assertEqual(income["upcoming_krw"], 0)
        self.assertIs(income["upcoming"]["available"], False)

    async def test_calendar_is_cached_per_day_and_failures_are_not(self):
        await self._seed_summary()
        with patch.object(service.dividend_calendar, "build_calendar", AsyncMock(side_effect=TimeoutError())) as broken:
            await service.build_summary("u1", today=date(2026, 11, 8))
        broken.assert_awaited_once()
        with self._calendar() as calendar:
            await service.build_summary("u1", today=date(2026, 11, 8))
            await service.build_summary("u1", today=date(2026, 11, 8))
        calendar.assert_awaited_once()

    async def test_tax_is_not_shown_before_collection(self):
        with self._calendar([]):
            summary = await service.build_summary("u1", today=date(2026, 11, 8))
        self.assertIs(summary["available"], True)
        self.assertIsNone(summary["current"]["tax"])
        self.assertEqual(summary["current"]["tax_status"], "unavailable")
        self.assertEqual(summary["current"]["overseas"]["status"], "pending")
        self.assertIs(summary["loss_harvest"]["available"], False)
        self.assertEqual(service.action_items(summary, date(2026, 11, 8)), [])

    async def test_failed_collection_reports_error_without_tax(self):
        self.nh.fail[realized_pnl.GB_PERIOD] = "NH 조회 실패"
        await self.collect()
        with self._calendar([]):
            summary = await service.build_summary("u1", today=date(2026, 11, 8))
        self.assertEqual(summary["current"]["overseas"]["status"], "failed")
        self.assertIsNone(summary["current"]["tax"])
        self.assertEqual({(row["year"], row["market"], row["message"]) for row in summary["coverage"]["errors"]},
                         {(2026, "overseas", "NH 조회 실패"), (2025, "overseas", "NH 조회 실패")})

    async def test_later_failure_marks_estimate_incomplete(self):
        await self.collect()
        self.nh.fail[realized_pnl.GB_PERIOD] = "NH 조회 실패"
        await self.collect(seconds=realized_pnl.DEFAULT_INTERVAL_SECONDS)
        with self._calendar([]):
            summary = await service.build_summary("u1", today=date(2026, 11, 8))
        self.assertEqual(summary["current"]["overseas"]["status"], "partial")
        self.assertEqual(summary["current"]["tax_status"], "incomplete")
        self.assertEqual(summary["current"]["tax"]["estimated_tax_krw"], 1_601_380)

    async def test_non_general_accounts_are_left_out_of_tax(self):
        await self.collect()
        async with transaction() as db:
            await db.execute("UPDATE portfolio_accounts SET type='isa' WHERE account_id=?", (self.aid,))
        with self._calendar([]):
            summary = await service.build_summary("u1", today=date(2026, 11, 8))
        self.assertIs(summary["available"], False)
        self.assertEqual(await ledger.rows("u1"), [])

    async def test_disconnected_account_rows_stay_visible_without_false_gap(self):
        await self.collect()
        await brokers.disconnect("u1", self.aid)
        with self._calendar([]):
            summary = await service.build_summary("u1", today=date(2026, 11, 8))
        current = summary["current"]
        self.assertEqual(current["overseas"]["realized_krw"], 9_779_000)
        self.assertEqual(current["overseas"]["status"], "partial")
        self.assertEqual(current["tax_status"], "incomplete")
        self.assertIsNone(current["overseas"]["total_gap_krw"])

    async def test_gap_compares_same_accounts_and_reports_difference(self):
        await self.collect()
        async with transaction() as db:
            await db.execute("UPDATE broker_realized_pnl_state SET broker_total=broker_total+1000 WHERE account_id=? AND year=2026 AND market='overseas'", (self.aid,))
        with self._calendar([]):
            current = (await service.build_summary("u1", today=date(2026, 11, 8)))["current"]
        self.assertEqual(current["overseas"]["total_gap_krw"], -1000)
        self.assertEqual(current["domestic"]["total_gap_krw"], 0)

    async def test_futures_only_account_is_not_shown_as_collecting(self):
        async with transaction() as db:
            await db.execute("UPDATE broker_account_links SET product='krfuture' WHERE account_id=?", (self.aid,))
        with self._calendar([]):
            summary = await service.build_summary("u1", today=date(2026, 11, 8))
        self.assertIs(summary["available"], True)
        self.assertEqual(summary["current"]["overseas"]["status"], "no_account")
        self.assertIsNone(summary["current"]["tax"])
        self.assertIsNotNone(summary["financial_income"])

    async def test_holdings_in_markets_outside_spec_are_called_out(self):
        await self.collect()
        await self._holding(self.aid, "BHP.AX", "BHP", 3, 40, "AUD")
        await self._holding(self.aid, "VOW3.DE", "폭스바겐", 2, 90, "EUR")
        await self._holding(self.aid, "AAPL", "애플", 1, 100, "USD")
        with self._calendar([]):
            summary = await service.build_summary("u1", today=date(2026, 11, 8))
        self.assertEqual(summary["unlisted_currencies"], ["AUD", "EUR"])
        # 해당 통화 매매가 실제로 원장에 들어오면 경고를 지운다.
        self.nh.period["1"].append(gb("20260701", 100, 10, 1, 9, cur="AUD", nat="020"))
        self.nh.period["2"].append(gb("20260701", 90_000, 9_000, 900, 8_100, cur="AUD", nat="020"))
        with patch.object(realized_pnl, "_fx_reference", AsyncMock(return_value=900.0)):
            await self.collect(seconds=realized_pnl.DEFAULT_INTERVAL_SECONDS)
        service._upcoming_cache.clear()
        with self._calendar([]):
            summary = await service.build_summary("u1", today=date(2026, 11, 8))
        self.assertEqual(summary["unlisted_currencies"], ["EUR"])

    async def test_year_outside_rule_review_is_flagged(self):
        await self.collect()
        with self._calendar([]):
            summary = await service.build_summary("u1", today=date(rules.RULES_REVIEWED_THROUGH + 1, 1, 2))
        self.assertIs(summary["current"]["rules_stale"], True)
        self.assertIs(summary["financial_income"]["rules_stale"], True)
        self.assertIs(next(row for row in summary["years"] if row["year"] == 2026)["rules_stale"], False)

    async def test_action_board_shows_tax_items_and_keeps_review_state(self):
        from repositories import action_reviews
        await self._seed_summary()
        with self._calendar(), \
             patch.object(service, "today_kst_date", return_value=date(2026, 11, 8)), \
             patch.object(action_board.rebalance_service, "compute_rebalance", AsyncMock(return_value={"items": [], "breached_count": 0, "as_of": None})), \
             patch.object(action_board.external_tools, "fetch_portfolio_signals", AsyncMock(return_value={})):
            board = await action_board.build_action_board("u1")
            keys = [item["key"] for item in board["actions"] if item["category"] == "tax"]
            self.assertEqual(keys, ["tax:financial-income:2026:near", "tax:overseas:2026:exhausted"])
            exhausted = next(item for item in board["actions"] if item["key"] == "tax:overseas:2026:exhausted")
            self.assertEqual(exhausted["url"], "#pfTaxWrap")
            self.assertEqual(exhausted["title"], "해외 양도세 추정 약 160만원")
            self.assertIn("최대 약 18만원", exhausted["detail"])
            await action_reviews.set_review_status("u1", "tax:overseas:2026:exhausted", "done")
            board = await action_board.build_action_board("u1")
        reviewed = next(item for item in board["actions"] if item["key"] == "tax:overseas:2026:exhausted")
        self.assertEqual(reviewed["status"], "done")
        self.assertEqual([item["key"] for item in board["queue"] if item["category"] == "tax"], ["tax:financial-income:2026:near"])

    async def test_action_board_without_nh_stays_empty_without_calendar_call(self):
        with patch.object(service.dividend_calendar, "build_calendar", AsyncMock()) as calendar:
            self.assertEqual(await service.actions("u2", today=date(2026, 11, 8)), [])
        calendar.assert_not_awaited()


class RouteTests(CollectionBase):
    async def _client(self):
        return httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(), raise_app_exceptions=False), base_url="http://test")

    async def test_requires_login_and_returns_only_own_summary(self):
        async with await self._client() as client:
            with patch.object(insights_route, "get_current_user", AsyncMock(return_value=None)):
                self.assertEqual((await client.get("/api/portfolio/realized-tax")).status_code, 401)
            with patch.object(insights_route, "get_current_user", AsyncMock(return_value={"google_sub": "u2"})):
                other = await client.get("/api/portfolio/realized-tax")
        self.assertEqual(other.status_code, 200)
        self.assertIs(other.json()["available"], False)
        self.assertIn("NH 실계좌", other.json()["message"])

    async def test_route_reads_ledger_without_calling_nh(self):
        await self.collect()
        self.nh.calls.clear()
        calendar = {"events": []}
        async with await self._client() as client:
            with patch.object(insights_route, "get_current_user", AsyncMock(return_value={"google_sub": "u1"})), \
                 patch.object(service.dividend_calendar, "build_calendar", AsyncMock(return_value=calendar)):
                response = await client.get("/api/portfolio/realized-tax")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIs(payload["available"], True)
        self.assertIs(payload["fields_verified"], False)
        self.assertEqual(payload["current"]["overseas"]["realized_krw"], 9_779_000)
        self.assertEqual(self.nh.calls, [])
