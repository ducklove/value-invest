import asyncio
import time
from unittest.mock import AsyncMock, patch

from services.portfolio import dividends


def test_dividend_warmup_targets_include_common_stock_for_preferred_share():
    assert dividends.dividend_warmup_targets("33637K") == ["33637K", "336370"]


def test_dividend_warmup_targets_ignore_non_domestic_assets():
    assert dividends.dividend_warmup_targets("AAPL") == []
    assert dividends.dividend_warmup_targets("CASH_USD") == []


def test_due_dividend_warmup_targets_deduplicates_and_honors_ttl():
    last = {"005930": 95.0}

    due = dividends.due_dividend_warmup_targets(
        ["005930", "005930", "33637K"],
        now=100.0,
        last_warmup=last,
        ttl_seconds=10.0,
    )

    assert due == ["33637K", "336370"]
    assert last == {"005930": 95.0}


def test_due_dividend_warmup_targets_skips_running_codes():
    due = dividends.due_dividend_warmup_targets(
        ["33637K"],
        now=100.0,
        last_warmup={},
        running_codes={"336370"},
        ttl_seconds=10.0,
    )

    assert due == ["33637K"]


# --- Warmup state machine (moved from routes/portfolio.py) ---


async def _drain_done_callbacks():
    # add_done_callback fires via call_soon; yield twice to let it run.
    await asyncio.sleep(0)
    await asyncio.sleep(0)


def _seed_expired(code: str) -> None:
    # ``last`` defaults to 0 and ``now`` is time.monotonic() (time since boot),
    # so on a fresh machine nothing is "due" until uptime exceeds the TTL.
    # Seed an expired stamp to make the code due deterministically.
    dividends._warmup_last[code] = time.monotonic() - dividends.DIVIDEND_WARMUP_TTL - 10


async def test_schedule_for_portfolio_registers_task_and_honors_ttl():
    dividends.reset_warmup_state()
    try:
        _seed_expired("005930")
        dividends.schedule_for_portfolio(["005930"])

        assert "005930" in dividends._warmup_tasks
        assert dividends.running_warmup_codes() == {"005930"}
        task = dividends._warmup_tasks["005930"]

        # Re-scheduling within the TTL must not spawn a second task.
        dividends.schedule_for_portfolio(["005930"])
        assert dividends._warmup_tasks["005930"] is task

        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await _drain_done_callbacks()
        assert "005930" not in dividends._warmup_tasks
        assert dividends.running_warmup_codes() == set()
    finally:
        dividends.reset_warmup_state()


async def test_schedule_for_portfolio_skips_foreign_and_cash_codes():
    dividends.reset_warmup_state()
    try:
        dividends.schedule_for_portfolio(["AAPL", "CASH_USD"])
        assert dividends._warmup_tasks == {}
        assert dividends._warmup_last == {}
    finally:
        dividends.reset_warmup_state()


async def test_warm_for_response_waits_for_started_warmups():
    dividends.reset_warmup_state()
    warm = AsyncMock()
    try:
        _seed_expired("005930")
        with patch.object(dividends, "warm_market_data_for_dividend", warm), \
             patch.object(dividends, "WARMUP_START_DELAY", 0):
            await dividends.warm_for_response(["005930"], timeout=2.0)
        warm.assert_awaited_once_with("005930")
        await _drain_done_callbacks()
        assert dividends.running_warmup_codes() == set()
    finally:
        dividends.reset_warmup_state()


async def test_warm_for_response_is_noop_within_ttl():
    dividends.reset_warmup_state()
    warm = AsyncMock()
    try:
        dividends._warmup_last["005930"] = time.monotonic()
        with patch.object(dividends, "warm_market_data_for_dividend", warm):
            await dividends.warm_for_response(["005930"], timeout=0.1)
        warm.assert_not_awaited()
        assert dividends._warmup_tasks == {}
    finally:
        dividends.reset_warmup_state()


async def test_background_warmups_run_one_at_a_time():
    # 재시작 직후 보유 종목 수십 개의 예열이 한꺼번에 돌면 kis-proxy 429와
    # finance-pi 10초 초과가 났다 — 배경 예열은 한 번에 하나씩 돈다.
    dividends.reset_warmup_state()
    active = 0
    peak = 0

    async def fake_warm(code):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01)
        active -= 1

    try:
        with patch.object(dividends, "WARMUP_START_DELAY", 0), \
             patch.object(dividends, "warm_market_data_for_dividend", new=fake_warm):
            for code in ("005930", "000660", "035420"):
                _seed_expired(code)
            dividends.schedule_for_portfolio(["005930", "000660", "035420"])
            tasks = list(dividends._warmup_tasks.values())
            assert len(tasks) == 3
            await asyncio.gather(*tasks)
        assert peak == dividends.WARMUP_CONCURRENCY == 1
    finally:
        dividends.reset_warmup_state()
