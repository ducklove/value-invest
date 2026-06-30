from __future__ import annotations

import asyncio
import logging
import os
from datetime import date
from typing import Any

import httpx

import close_price_client
from core.errors import ExternalServiceError

BASE_URL = os.getenv("KIS_PROXY_BASE_URL", "http://cantabile.tplinkdns.com:3288").rstrip("/")
TIMEOUT_SECONDS = float(os.getenv("KIS_PROXY_TIMEOUT_SECONDS", "20"))
PROXY_TOKEN = os.getenv("KIS_PROXY_TOKEN", os.getenv("KIS_PROXY_PUBLIC_TOKEN", "")).strip()
logger = logging.getLogger(__name__)
_client: httpx.AsyncClient | None = None
_client_lock: asyncio.Lock | None = None

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


def _get_client_lock() -> asyncio.Lock:
    global _client_lock
    if _client_lock is None:
        _client_lock = asyncio.Lock()
    return _client_lock


async def init_client():
    await _get_client()


async def close_client():
    global _client
    async with _get_client_lock():
        client = _client
        _client = None
    if client is not None:
        await client.aclose()


async def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is not None:
        return _client

    async with _get_client_lock():
        if _client is None:
            _client = httpx.AsyncClient(
                timeout=TIMEOUT_SECONDS,
                follow_redirects=True,
            )
        return _client


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


async def get_financials(
    symbol: str,
    *,
    period_div_code: str = "0",
) -> dict[str, Any]:
    return await _get(
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
    return await _get(
        f"/v1/stocks/{symbol}/dividends",
        params={
            "start_date": _iso(start_date),
            "end_date": _iso(end_date),
        },
    )
