from __future__ import annotations

import asyncio
import os
from datetime import date
from typing import Any

import httpx


BASE_URL = os.getenv("KIS_PROXY_BASE_URL", "http://cantabile.tplinkdns.com:3288").rstrip("/")
TIMEOUT_SECONDS = float(os.getenv("KIS_PROXY_TIMEOUT_SECONDS", "20"))


class KISProxyError(RuntimeError):
    pass


async def _get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"
    last_exc = None
    for attempt in range(3):
        try:
            async with httpx.AsyncClient(
                timeout=TIMEOUT_SECONDS,
                follow_redirects=True,
            ) as client:
                response = await client.get(url, params=params)
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


async def get_quote(symbol: str) -> dict[str, Any]:
    return await _get(f"/v1/stocks/{symbol}/quote")


async def get_history(
    symbol: str,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    period: str = "D",
    adjusted: bool = True,
) -> dict[str, Any]:
    return await _get(
        f"/v1/stocks/{symbol}/history",
        params={
            "start_date": _iso(start_date),
            "end_date": _iso(end_date),
            "period": period,
            "adjusted": str(adjusted).lower(),
        },
    )


async def get_financials(
    symbol: str,
    *,
    period_div_code: str = "0",
) -> dict[str, Any]:
    return await _get(
        f"/v1/stocks/{symbol}/financials",
        params={"period_div_code": period_div_code},
    )


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
