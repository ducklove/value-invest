from __future__ import annotations

import asyncio
import copy
import logging
import os
from datetime import date
from typing import Any

import httpx

import close_price_client
from cache_layer import MemoryTTLCache
from core import config as app_config
from core.errors import ExternalServiceError
from core.http import get_http_client, register_timeout_profile

# 허브와 kis-proxy 는 운영에서 같은 호스트에 있다. 운영 기본값은 loopback 이라
# 토큰이 DDNS NAT hairpin 을 거치는 평문 HTTP 로 나가지 않는다. 개발 PC 에는
# 로컬 프록시가 없으므로 공개 주소를 유지한다. env 값이 있으면 항상 우선한다.
LOOPBACK_BASE_URL = "http://127.0.0.1:3288"
PUBLIC_BASE_URL = "http://ducklove.duckdns.org:3288"


def default_base_url(environment: str | None = None) -> str:
    """프로필별 기본 KIS 프록시 주소 (production → loopback, 그 외 → 공개 주소)."""
    env = app_config._normalize_env(environment) if environment else app_config._current_env()
    return LOOPBACK_BASE_URL if env == "production" else PUBLIC_BASE_URL


def resolve_base_url(environment: str | None = None) -> str:
    override = (os.getenv("KIS_PROXY_BASE_URL") or "").strip()
    return (override or default_base_url(environment)).rstrip("/")


BASE_URL = resolve_base_url()
TIMEOUT_SECONDS = float(os.getenv("KIS_PROXY_TIMEOUT_SECONDS", "20"))
PROXY_TOKEN = os.getenv("KIS_PROXY_TOKEN", os.getenv("KIS_PROXY_PUBLIC_TOKEN", "")).strip()
logger = logging.getLogger(__name__)
# 공유 core/http 클라이언트 이름. 기본 timeout 은 KIS_PROXY_TIMEOUT_SECONDS.
HTTP_CLIENT_NAME = "kis_proxy"
register_timeout_profile(HTTP_CLIENT_NAME, TIMEOUT_SECONDS)

# Hard rate limit: KIS Open API caps at 5 transactions / second per app key
# and returns EGW00201 ("초당 거래건수를 초과하였습니다.") on overshoot. We
# stay safely below by serializing every outgoing request through an async
# interval limiter at ~4 req/s. A semaphore alone is NOT enough — concurrent
# requests that each take <250ms still blow the per-second budget.
_RATE_PER_SEC = float(os.getenv("KIS_PROXY_RATE_PER_SEC", "4"))
_MIN_INTERVAL = 1.0 / _RATE_PER_SEC
_rate_lock: asyncio.Lock | None = None
_last_send_ts: float = 0.0


def _get_rate_lock() -> asyncio.Lock:
    global _rate_lock
    if _rate_lock is None:
        _rate_lock = asyncio.Lock()
    return _rate_lock


async def _acquire_rate_slot() -> None:
    """Block until the next outgoing request slot is available.
    Strict serial spacing of _MIN_INTERVAL between request *starts*."""
    global _last_send_ts
    async with _get_rate_lock():
        loop = asyncio.get_event_loop()
        now = loop.time()
        wait = _last_send_ts + _MIN_INTERVAL - now
        if wait > 0:
            await asyncio.sleep(wait)
            now = loop.time()
        _last_send_ts = now


class KISProxyError(ExternalServiceError):
    pass


async def init_client():
    await _get_client()


async def close_client():
    """lifespan 훅 — 공유 'kis_proxy' 클라이언트는 core/http 매니저가 닫는다."""


async def _get_client() -> httpx.AsyncClient:
    return await get_http_client(HTTP_CLIENT_NAME)


async def _get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"
    headers = {"X-KIS-Proxy-Token": PROXY_TOKEN} if PROXY_TOKEN else None
    last_exc = None
    for attempt in range(3):
        try:
            client = await _get_client()
            await _acquire_rate_slot()
            response = await client.get(url, params=params, headers=headers)
            response.raise_for_status()
            payload = response.json()
            return payload if isinstance(payload, dict) else {}
        except Exception as exc:
            last_exc = exc
            should_retry = False
            if isinstance(exc, httpx.HTTPStatusError):
                body = exc.response.text.strip()
                should_retry = exc.response.status_code >= 500 and (
                    "EGW00201" in body or "초당 거래건수" in body
                )
            if isinstance(exc, httpx.HTTPStatusError):
                status_code = exc.response.status_code
                if status_code == 429 or status_code >= 500:
                    should_retry = True
            elif isinstance(exc, (httpx.TimeoutException, httpx.TransportError)):
                should_retry = True
            if should_retry and attempt < 2:
                await asyncio.sleep(0.4 * (attempt + 1))
                continue

            detail = ""
            if isinstance(exc, httpx.HTTPStatusError):
                body = exc.response.text.strip()
                if body:
                    detail = f" status={exc.response.status_code} body={body[:200]}"
                else:
                    detail = f" status={exc.response.status_code}"
            raise KISProxyError(f"KIS proxy request failed: {url}{detail}") from exc

    raise KISProxyError(f"KIS proxy request failed: {url}") from last_exc


def _iso(value: date | None) -> str | None:
    return value.isoformat() if value else None


async def get_quote(symbol: str, *, market: str | None = None) -> dict[str, Any]:
    params = {"market": market} if market else None
    return await _get(f"/v1/stocks/{symbol}/quote", params=params)


async def get_overseas_quote(symbol: str, exchange: str) -> dict[str, Any]:
    return await _get(f"/v1/overseas/{exchange}/{symbol}/quote")


async def get_history(
    symbol: str,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    period: str = "D",
    adjusted: bool = True,
) -> dict[str, Any]:
    params = {
        "start_date": _iso(start_date),
        "end_date": _iso(end_date),
        "period": period,
        "adjusted": str(adjusted).lower(),
    }
    use_local_daily = str(period or "D").upper() == "D" and adjusted

    if use_local_daily:
        try:
            items = await close_price_client.get_daily_price_items(
                symbol,
                since=start_date,
                until=end_date,
            )
            if items:
                return {"items": items, "source": "local_daily_price_api"}
            logger.info("local daily price API returned no rows for %s; trying KIS history", symbol)
        except close_price_client.ClosePriceClientError as exc:
            logger.info("local daily price API failed for %s; trying KIS history: %s", symbol, exc)

    try:
        payload = await _get(f"/v1/stocks/{symbol}/history", params=params)
        if not use_local_daily or payload.get("items"):
            return payload
        logger.info("KIS history returned no daily rows for %s", symbol)
    except KISProxyError:
        raise

    return payload


# 재무·배당은 하루에도 거의 바뀌지 않는데 분석 1회가 재무를 최대 4번, 배당을
# 약 3번 부른다. 모두 실시간 시세와 같은 4 req/s limiter 를 공유하므로
# 60분 TTL + in-flight 공유로 같은 요청을 한 번만 보낸다.
RESPONSE_CACHE_TTL_SECONDS = float(os.getenv("KIS_PROXY_RESPONSE_CACHE_TTL_SECONDS", "3600"))
_response_cache = MemoryTTLCache("kis_proxy.responses", RESPONSE_CACHE_TTL_SECONDS, evict_expired_after=0)
_response_inflight: dict[str, asyncio.Future] = {}


def clear_response_cache() -> None:
    _response_cache.clear()
    _response_inflight.clear()


async def _cached_get(key: str, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    """TTL 캐시 + single-flight 로 감싼 ``_get``. 빈 응답과 실패는 캐시하지 않는다."""
    cached = _response_cache.get(key)
    if cached is not None:
        return cached
    pending = _response_inflight.get(key)
    if pending is not None:
        # 먼저 시작한 호출의 결과(또는 예외)를 공유한다.
        try:
            return copy.deepcopy(await asyncio.shield(pending))
        except asyncio.CancelledError:
            task = asyncio.current_task()
            if pending.cancelled() and not (task and task.cancelling()):
                # 소유 호출만 취소됐다 — 이 호출은 스스로 다시 받는다.
                return await _cached_get(key, path, params)
            raise
    future: asyncio.Future = asyncio.get_running_loop().create_future()
    _response_inflight[key] = future
    try:
        payload = await _get(path, params=params)
    except asyncio.CancelledError:
        future.cancel()
        raise
    except Exception as exc:
        if not future.done():
            future.set_exception(exc)
            # 대기자가 없을 때 "exception was never retrieved" 경고를 막는다.
            future.exception()
        raise
    else:
        if payload:
            _response_cache.set(key, payload)
        if not future.done():
            # 소유 호출자가 payload 를 변경해도 대기자에게 새지 않게 복사본을 넘긴다.
            future.set_result(copy.deepcopy(payload))
        return payload
    finally:
        if _response_inflight.get(key) is future:
            del _response_inflight[key]


async def get_financials(
    symbol: str,
    *,
    period_div_code: str = "0",
) -> dict[str, Any]:
    return await _cached_get(
        f"financials:{symbol}:{period_div_code}",
        f"/v1/stocks/{symbol}/financials",
        params={"period_div_code": period_div_code},
    )


async def get_night_futures_quote() -> dict[str, Any]:
    """코스피200 야간선물(EUREX 연계) 최근월물 시세.

    응답의 ``summary`` 에 current_price/change/change_sign/change_rate/
    previous_close 가 정규화돼 있다(부호 코드는 KIS 관례: 1·2=상승, 4·5=하락).
    """
    return await _get("/v1/futures/kospi-night/near-month/quote")


async def get_dividends(
    symbol: str,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    params = {
        "start_date": _iso(start_date),
        "end_date": _iso(end_date),
    }
    return await _cached_get(
        f"dividends:{symbol}:{params['start_date']}:{params['end_date']}",
        f"/v1/stocks/{symbol}/dividends",
        params=params,
    )
