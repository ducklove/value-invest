"""services/market/sources/yfinance_runner.py — 공용 yfinance 실행기."""

import asyncio
import threading
import time

import pytest

from services.market.sources import yfinance_runner


@pytest.fixture(autouse=True)
def _reset():
    yfinance_runner.reset_negative_cache()
    yield
    yfinance_runner.reset_negative_cache()


def test_runs_on_dedicated_pool_with_args():
    def work(a, b):
        return a + b, threading.current_thread().name

    total, thread = asyncio.run(yfinance_runner.run(work, 2, 3))
    assert total == 5
    assert thread.startswith("yf")


def test_concurrency_is_bounded():
    active = 0
    peak = 0
    lock = threading.Lock()

    def work():
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        time.sleep(0.02)
        with lock:
            active -= 1

    async def run():
        await asyncio.gather(*(yfinance_runner.run(work) for _ in range(12)))

    asyncio.run(run())
    assert 1 < peak <= yfinance_runner.MAX_CONCURRENCY


def test_timeout_raises_and_populates_negative_cache():
    calls = []

    def slow():
        calls.append(1)
        time.sleep(0.2)

    async def run():
        with pytest.raises(asyncio.TimeoutError):
            await yfinance_runner.run(slow, timeout=0.01, negative_key="aux:X")
        with pytest.raises(yfinance_runner.YFinanceUnavailable):
            await yfinance_runner.run(slow, timeout=0.01, negative_key="aux:X")

    asyncio.run(run())
    assert calls == [1]
    assert yfinance_runner.recently_failed("aux:X")


def test_error_marks_only_keyed_calls():
    def boom():
        raise ValueError("bad ticker")

    async def run():
        with pytest.raises(ValueError):
            await yfinance_runner.run(boom)
        with pytest.raises(ValueError):
            await yfinance_runner.run(boom)  # 키 없음 — 매번 실행
        with pytest.raises(ValueError):
            await yfinance_runner.run(boom, negative_key="k")
        assert yfinance_runner.recently_failed("k")
        assert await yfinance_runner.run(lambda: "ok", negative_key="other") == "ok"

    asyncio.run(run())
    assert not yfinance_runner.recently_failed("other")
