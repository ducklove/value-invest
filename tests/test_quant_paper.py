import asyncio
import json
from copy import deepcopy
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from _harness import TempDbMixin, seed_user

from repositories import quant_scanner
from repositories.quant import QuantError
from services.quant import paper, rollover, scanner
from services.quant import paper_service as quant_paper
from services.quant.scanner_model import KST, ScannerConfig, book

NOW = datetime(2026, 9, 17, 10, tzinfo=KST)
CONFIG = ScannerConfig(account_id="mock", enabled=True)
ROW = {"contract": "KA116A000", "spot_code": "005930", "name": "검증",
       "contract_name": "검증 F 202610 ( 10)", "expiry": "20261008", "roll_on": "2026-10-06",
       "roll_policy": rollover.POLICY, "expiry_verified": True, "error": None}


def tick(state, seconds, *, date=NOW, closing=False, future_size=1000, **overrides):
    now = date + timedelta(seconds=seconds)
    row = {**ROW, "observed_at": now.timestamp()}
    s = book(10000 if closing else 9990, 10010 if closing else 10000, 10000, 10000, now.strftime("%H%M%S"), now)
    f = book(10000 if closing else 10300, 10010 if closing else 10310, future_size, future_size, now.strftime("%H%M%S"), now)
    row.update(overrides.get("row", {}));s.update(overrides.get("spot", {}));f.update(overrides.get("future", {}))
    return paper.step(state, row, s, f, CONFIG, now)


def account():
    return paper.initial(paper.PaperConfig(), "mock", NOW.timestamp())


def opened():
    state = account()
    assert tick(state, 0)[0]["type"] == "pending"
    event = tick(state, 1)[0]
    assert event["type"] == "fill" and event["action"] == "entry"
    return state, event


def test_bid_ask_entry_exit_costs_and_equity_reconcile_without_last_price():
    state, entry = opened()
    assert entry["spot_price"] == 10000 and entry["future_price"] == 10300
    assert entry["shares"] == entry["contracts"] * 10
    assert entry["contracts"] == 82
    assert paper.summary(state, NOW + timedelta(seconds=1))["equity"] < 100_000_000
    assert tick(state, 2, closing=True)[0]["type"] == "pending"
    exit = tick(state, 3, closing=True, spot={"last": 999999}, future={"last": 1})[0]
    assert exit["spot_price"] == 10000 and exit["future_price"] == 10010
    funding_per_share = 12060 * .04 * 2 / (365 * 86400)
    expected = (290 - 5.678 - 25.6026 - funding_per_share) * 820
    assert state["realized_pnl"] == pytest.approx(expected)
    assert not state["positions"] and state["closed"] == state["wins"] == 1
    assert state["total_costs"] == pytest.approx(290 * 820 - expected)
    assert paper.summary(state)["equity"] == pytest.approx(100_000_000 + expected)


def test_delay_requires_new_exchange_time_on_both_sides_and_cancels_changed_opportunity():
    state = account();tick(state, 0)
    assert not tick(state, 1, future={"at": NOW.timestamp()})
    assert state["fills"] == 0
    cancelled = tick(state, 2, closing=True)
    assert cancelled[0]["type"] == "cancel" and not state["positions"]
    tick(state, 3)
    assert paper.expire_pending(state, NOW + timedelta(seconds=13))[0]["type"] == "cancel"
    assert state["fills"] == 0


@pytest.mark.parametrize("overrides", [
    {"spot": {"at": NOW.timestamp() - 6}}, {"future": {"received_at": NOW.timestamp() - 6}},
    {"future": {"at": NOW.timestamp() - 3}}, {"spot": {"bid": 10001}},
    {"row": {"expiry_verified": False}}, {"row": {"observed_at": NOW.timestamp() - 181}},
    {"row": {"error": "거래상태 미확인"}}, {"row": {"contract_name": "조정 F 202610 ( 15)"}},
    {"spot": {"bid_size": .5}}, {"spot": {"ask": float("nan")}},
])
def test_unusable_quotes_never_create_pending_or_fill(overrides):
    state = account();tick(state, 0, **overrides)
    assert not state["pending"] and state["fills"] == 0


def test_visible_contract_liquidity_and_partial_exit_do_not_reuse_same_quote():
    state = account();tick(state, 0, future_size=4)
    entry = tick(state, 1, future_size=4)[0]
    assert entry["contracts"] == 1 and entry["shares"] == 10
    state, _ = opened()
    tick(state, 2, closing=True, future_size=4)
    exit = tick(state, 3, closing=True, future_size=4)[0]
    assert exit["contracts"] == 1 and state["positions"][ROW["contract"]]["contracts"] == 81
    tick(state, 4, closing=True)
    assert not tick(state, 4, closing=True)
    assert state["fills"] == 2
    tick(state, 5, closing=True)
    assert state["fills"] == 3 and state["closed"] == 1


def test_roll_exit_is_prioritized_and_missing_quotes_never_fake_settlement():
    state, _ = opened()
    roll = datetime(2026, 10, 6, 9, tzinfo=KST)
    first = tick(state, 0, date=roll)
    assert first[0]["pending"]["reason"] == "월물 전환"
    assert tick(state, 1, date=roll)[0]["action"] == "exit"
    assert not tick(state, 2, date=roll)  # 구월물 재진입 금지
    state, _ = opened()
    tick(state, 0, date=datetime(2026, 10, 12, 9, tzinfo=KST))
    assert state["positions"] and state["fills"] == 1
    assert "만기" in state["positions"][ROW["contract"]]["blocked"]
    assert paper.summary(state)["stale_positions"] == 1


def test_pause_blocks_new_entries_but_keeps_existing_exit_and_capital_limits():
    state, _ = opened();state["enabled"] = False
    tick(state, 2, closing=True);tick(state, 3, closing=True)
    assert state["closed"] == 1
    assert not tick(state, 70)
    state = paper.initial(paper.PaperConfig(capital=100_000), "mock", NOW.timestamp())
    tick(state, 0)
    assert tick(state, 1)[0]["type"] == "cancel"
    assert not state["positions"] and state["realized_pnl"] == 0


def test_simultaneous_pending_orders_recheck_position_limit_on_fill():
    state = account();state["config"]["max_positions"] = 1
    tick(state, 0)
    other = {"contract": "KA506A000", "spot_code": "000660"}
    tick(state, 0, row=other)
    tick(state, 1)
    assert tick(state, 1, row=other)[0]["type"] == "cancel"
    assert len(state["positions"]) == 1


def test_stale_position_valuation_blocks_new_capital_allocation():
    state, _ = opened()
    tick(state, 10, row={"contract": "KA506A000", "spot_code": "000660"})
    assert len(state["positions"]) == 1 and not state["pending"]
    assert "평가 대기" in state["last_rejection"]["reason"]


def test_recalculating_old_quotes_does_not_make_valuation_fresh():
    state, _ = opened()
    tick(state, 5, spot={"at": (NOW + timedelta(seconds=1)).timestamp()},
         future={"at": (NOW + timedelta(seconds=1)).timestamp()})
    position = state["positions"][ROW["contract"]]
    assert position["mark"]["at"] == (NOW + timedelta(seconds=1)).timestamp()
    assert paper.summary(state, NOW + timedelta(seconds=7))["stale_positions"] == 1


def test_pinned_position_survives_catalog_roll_and_has_subscription_priority():
    state, _ = opened()
    observer = scanner.Watcher("u", "c", "mock", CONFIG, "g", {})
    observer.paper_state = state
    selection = rollover.universe([{**ROW, "contract": "KA116B000", "contract_name": "검증 F 202611 ( 10)"}], NOW.date())
    scanner.sync_universe(observer, selection, {})
    assert ROW["contract"] in observer.rows and ROW["contract"] in observer.selected
    observer.select()
    assert next(iter(observer.selected)) == ROW["contract"]


class TestPaperPersistence(TempDbMixin):
    async def seed(self):
        await seed_user("u1");await seed_user("u2")
        await quant_scanner.configure("u1", CONFIG.model_dump())

    async def test_owner_generation_restart_and_idempotent_start(self):
        await quant_paper.start("u1", paper.PaperConfig())
        assert await quant_paper.get("u2") is None
        generation = (await quant_scanner.settings("u1"))[0]["generation"]
        s = book(9990, 10000, 10000, 10000, "100000", NOW)
        f = book(10300, 10310, 1000, 1000, "100000", NOW)
        row = {**ROW, "observed_at": NOW.timestamp()}
        await quant_paper.process("u1", generation, row, s, f, CONFIG, NOW)
        saved = await quant_paper.get("u1")
        assert saved["state"]["pending"]
        later = NOW + timedelta(seconds=1)
        s.update(at=later.timestamp(), received_at=later.timestamp());f.update(at=later.timestamp(), received_at=later.timestamp())
        await quant_paper.process("u1", generation, row, s, f, CONFIG, later)
        original = deepcopy((await quant_paper.get("u1"))["state"])
        await quant_paper.start("u1", paper.PaperConfig())
        assert (await quant_paper.get("u1"))["state"] == original
        await quant_scanner.stop("u1")
        assert await quant_paper.process("u1", generation, row, s, f, CONFIG, later) is None
        assert (await quant_paper.get("u1"))["state"] == original
        with self.assertRaises(QuantError):
            await quant_paper.assert_account("u1", "another")
        with self.assertRaises(QuantError):
            await quant_paper.start("u2", paper.PaperConfig())

    async def test_restart_timeout_and_pause_leave_auditable_state(self):
        await quant_paper.start("u1", paper.PaperConfig())
        await quant_paper.pause("u1")
        assert (await quant_paper.get("u1"))["state"]["enabled"] is False
        await quant_paper.start("u1", paper.PaperConfig())
        generation = (await quant_scanner.settings("u1"))[0]["generation"]
        spot = book(9990, 10000, 10000, 10000, "100000", NOW)
        future = book(10300, 10310, 1000, 1000, "100000", NOW)
        await quant_paper.process("u1", generation, {**ROW, "observed_at": NOW.timestamp()}, spot, future, CONFIG, NOW)
        await quant_paper.process("u1", generation, now=NOW + timedelta(seconds=15))
        saved = await quant_paper.get("u1")
        assert not saved["state"]["pending"] and saved["events"][0]["type"] == "cancel"

    async def test_live_watcher_routes_both_book_updates_to_paper_ledger(self):
        await quant_paper.start("u1", paper.PaperConfig())
        generation = (await quant_scanner.settings("u1"))[0]["generation"]
        watcher = scanner.Watcher("u1", "cid", "mock", CONFIG, generation, {})
        watcher.paper_state = (await quant_paper.get("u1"))["state"]
        watcher.selected = {ROW["contract"]: NOW.timestamp()}
        watcher.rows = {ROW["contract"]: {**ROW, "observed_at": NOW.timestamp()}}
        messages = [{"header":{"rsp_cd":"00000"},"body":{"tr_key":["005930","A116A000"]}}]
        times = []
        for n in range(3):
            clock = NOW + timedelta(seconds=n)
            times.extend([clock, clock])
            messages.extend([
                {"header":{"tr_cd":"ob","tr_key":"005930"},"body":{"code":"005930","hotime":clock.strftime("%H%M%S"),"bid":9990,"offer":10000,"bidrem":10000,"offerrem":10000}},
                {"header":{"tr_cd":"vH","tr_key":"A116A000"},"body":{"fuitem":"A116A000","futime":clock.strftime("%H%M%S"),"bid":10300,"offer":10310,"bidjan":1000,"offerjan":1000}},
            ])

        class Socket:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def send(self, message):
                pass

            async def recv(self):
                if not messages:
                    raise asyncio.CancelledError
                return json.dumps(messages.pop(0))

        with patch.object(scanner.websockets, "connect", return_value=Socket()), \
             patch.object(scanner.namuh, "token", AsyncMock(return_value="test")), \
             patch.object(scanner, "datetime") as clock:
            clock.now.side_effect = times
            with self.assertRaises(asyncio.CancelledError):
                await watcher.run()
        saved = await quant_paper.get("u1")
        fills = [e for e in saved["events"] if e["type"] == "fill"]
        assert saved["state"]["fills"] == 1
        assert fills[0]["spot_price"] == 10000 and fills[0]["future_price"] == 10300

    async def test_refresh_keeps_expiring_held_contract_for_real_quote_exit(self):
        state, _ = opened()
        watcher = scanner.Watcher("u1", "cid", "mock", CONFIG, "g", {})
        day = datetime(2026, 10, 8, 9, tzinfo=KST)
        spot = book(9990, 10000, 10000, 10000, "090000", day)
        future = book(10300, 10310, 1000, 1000, "090000", day)
        with patch.object(scanner.quant_paper, "process", AsyncMock(return_value=state)), \
             patch.object(scanner.scanner_feed, "catalog", AsyncMock(return_value=[ROW])), \
             patch.object(scanner.scanner_feed, "snapshot", AsyncMock(return_value=(spot, future, ROW["expiry"]))), \
             patch.object(scanner.quant_scanner, "record", AsyncMock(return_value=False)), \
             patch.object(scanner, "edge", side_effect=AssertionError("진입 만기 제약으로 청산을 막으면 안 됨")), \
             patch.object(scanner.time, "time", return_value=day.timestamp()), \
             patch.object(scanner, "datetime") as clock:
            clock.now.return_value = day
            await watcher.refresh()
        row = watcher.rows[ROW["contract"]]
        assert row["error"] is None and row["observed_at"] == day.timestamp()
        events = paper.step(state, row, spot, future, CONFIG, day)
        assert events[0]["pending"]["reason"] == "월물 전환"
