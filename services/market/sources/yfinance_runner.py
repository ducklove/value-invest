"""yfinance 공용 실행기 — 블로킹 yfinance 호출의 단일 출입구.

yfinance 는 동기 라이브러리이고, ``asyncio.wait_for`` 가 타임아웃을 내도 실행
중인 스레드는 멈추지 않는다. 예전에는 stock_price(전용 4-스레드 풀),
foreign(기본 executor + Semaphore(3)), benchmark_history·foreign_dividends
(기본 executor, 한도 없음)가 제각각 스레드를 띄워, 멈춘 Yahoo 호출이 기본
executor 를 쓰는 ``asyncio.to_thread`` 사용처 전체를 굶길 수 있었다.

* **executor** — ``EXECUTOR`` (전용 소형 풀). 최악에도 이 풀만 막힌다.
* **동시성** — 이벤트 루프별 세마포어(``MAX_CONCURRENCY``)로 대기열을 풀
  바깥(asyncio)에 둔다. ``timeout`` 은 세마포어를 얻은 뒤부터 잰다.
* **negative cache** — ``negative_key`` 를 주면 실패(예외·타임아웃)한 키는
  ``NEGATIVE_TTL_SECONDS`` 동안 스레드를 쓰지 않고 곧바로
  ``YFinanceUnavailable`` 을 낸다. 같은 종목의 멈춘 호출이 요청마다 풀을
  다시 점유하지 않게 한다.
"""

from __future__ import annotations

import asyncio
import weakref
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, Callable, TypeVar

from cache_layer import MemoryTTLCache

T = TypeVar("T")

MAX_WORKERS = 4
MAX_CONCURRENCY = MAX_WORKERS
NEGATIVE_TTL_SECONDS = 60.0

EXECUTOR = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix="yf")

_semaphores: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Semaphore]" = weakref.WeakKeyDictionary()
_failed = MemoryTTLCache("yfinance.failed", NEGATIVE_TTL_SECONDS, evict_expired_after=0)


class YFinanceUnavailable(RuntimeError):
    """최근 실패한 키 — negative cache 가 호출을 건너뛰었다."""


def _limit() -> asyncio.Semaphore:
    loop = asyncio.get_running_loop()
    semaphore = _semaphores.get(loop)
    if semaphore is None:
        semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        _semaphores[loop] = semaphore
    return semaphore


def recently_failed(key: str) -> bool:
    return bool(_failed.get(key))


def reset_negative_cache() -> None:
    """테스트·운영 도구용."""
    _failed.clear()


async def run(
    fn: Callable[..., T],
    *args: Any,
    timeout: float | None = None,
    negative_key: str | None = None,
) -> T:
    """``fn(*args)`` 를 전용 풀에서 실행한다. ``timeout`` 초과 시
    ``asyncio.TimeoutError``, 그 밖의 예외는 그대로 전파한다."""
    if negative_key is not None and recently_failed(negative_key):
        raise YFinanceUnavailable(f"yfinance recently failed for {negative_key}")
    loop = asyncio.get_running_loop()
    call = partial(fn, *args) if args else fn
    try:
        async with _limit():
            future = loop.run_in_executor(EXECUTOR, call)
            if timeout is None:
                return await future
            return await asyncio.wait_for(future, timeout=timeout)
    except Exception:
        if negative_key is not None:
            _failed.set(negative_key, True)
        raise
