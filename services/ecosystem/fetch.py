"""형제 JSON 공용 캐시 fetch — ``MemoryTTLCache`` + single-flight + stale-while-error.

같은 키의 동시 cold miss 는 업스트림 호출 하나를 공유한다(액션보드와 인사이트 카드가
같은 파일을 동시에 요청해도 1회). 새로 받기에 실패하면 ``stale_max`` 이내의 마지막
성공값을 돌려주고, 그것도 없으면 예외를 그대로 올린다(호출부가 도구별로 실패 허용).
"""

from __future__ import annotations

import asyncio
import copy
import logging
from datetime import datetime
from typing import Any, Awaitable, Callable

import httpx

from cache_layer import MemoryTTLCache, parse_iso
from core.errors import ExternalServiceError

logger = logging.getLogger(__name__)

# 형제 fetch 실패로 취급하는 예외(광역 Exception 금지). JSON 파싱 실패는 ValueError,
# 모양이 어긋난 페이로드를 변환하다 난 오류는 KeyError/TypeError/AttributeError.
FETCH_ERRORS: tuple[type[BaseException], ...] = (
    httpx.HTTPError, ValueError, TypeError, KeyError, AttributeError, ExternalServiceError,
)
STALE_MAX_SECONDS = 24 * 3600

_inflight: dict[tuple[str, str], asyncio.Task] = {}


def _mark_retrieved(task: asyncio.Task) -> None:
    # 기다리던 호출자가 모두 취소돼도 "exception was never retrieved" 경고가 없게.
    if not task.cancelled():
        task.exception()


async def single_flight(key: tuple[str, str], factory: Callable[[], Awaitable[Any]]) -> tuple[Any, bool]:
    """같은 key 의 진행 중 작업이 있으면 합류한다. (결과, 합류 여부) 를 돌려준다."""
    loop = asyncio.get_running_loop()
    task = _inflight.get(key)
    joined = task is not None and not task.done() and task.get_loop() is loop
    if not joined:
        task = loop.create_task(factory())
        _inflight[key] = task

        def _done(t: asyncio.Task, key=key) -> None:
            if _inflight.get(key) is t:
                _inflight.pop(key, None)
            _mark_retrieved(t)

        task.add_done_callback(_done)
    return await asyncio.shield(task), joined


def entry_age_seconds(cached_at: str | None) -> float | None:
    at = parse_iso(cached_at)
    if at is None:
        return None
    return max(0.0, (datetime.now() - at).total_seconds())


def stale_value(cache: MemoryTTLCache, key: str, *, stale_max: float = STALE_MAX_SECONDS) -> Any | None:
    """만료됐더라도 ``stale_max`` 이내의 마지막 값(복사본). 없으면 None."""
    entry = cache.get_entry(key, allow_stale=True)
    if entry is None:
        return None
    age = entry_age_seconds(entry.cached_at)
    if age is not None and age > stale_max:
        return None
    return entry.value


async def cached_fetch(
    cache: MemoryTTLCache,
    key: str,
    factory: Callable[[], Awaitable[Any]],
    *,
    ttl: float | None = None,
    stale_on_error: bool = True,
    stale_max: float = STALE_MAX_SECONDS,
) -> Any:
    """캐시 hit 이면 복사본, miss 면 single-flight 로 ``factory()`` 를 한 번만 돌려 저장한다.

    ``factory`` 가 :data:`FETCH_ERRORS` 로 실패하면 stale 값(있으면)을 돌려주고, 없으면
    예외를 올린다. 실패는 캐시하지 않는다(다음 호출이 다시 시도한다).
    """
    hit = cache.get(key)
    if hit is not None:
        return hit

    async def run() -> Any:
        value = await factory()
        if value is not None:
            cache.set(key, value, ttl_seconds=ttl)
        return value

    try:
        value, joined = await single_flight((cache.namespace, key), run)
    except FETCH_ERRORS as exc:
        if stale_on_error:
            stale = stale_value(cache, key, stale_max=stale_max)
            if stale is not None:
                logger.warning("sibling fetch failed, serving stale %s/%s: %s", cache.namespace, key, exc)
                return stale
        raise
    # 합류한 호출자는 공유 결과를 변경하지 못하게 복사본을 받는다(캐시 get 과 같은 의미).
    return copy.deepcopy(value) if joined else value
