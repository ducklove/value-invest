from __future__ import annotations

import asyncio
import logging
import os
from datetime import date, datetime
from typing import Any

import httpx


logger = logging.getLogger(__name__)

BASE_URL = os.getenv("CLOSE_PRICE_API_BASE_URL", "http://192.168.68.84").rstrip("/")
TIMEOUT_SECONDS = float(os.getenv("CLOSE_PRICE_API_TIMEOUT_SECONDS", "2.5"))
FAILURE_COOLDOWN_SECONDS = float(os.getenv("CLOSE_PRICE_API_FAILURE_COOLDOWN_SECONDS", "60"))
ENABLED = os.getenv("CLOSE_PRICE_API_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}

_client: httpx.AsyncClient | None = None
_client_lock: asyncio.Lock | None = None
_skip_until: float = 0.0


class ClosePriceClientError(RuntimeError):
    pass


def _get_client_lock() -> asyncio.Lock:
    global _client_lock
    if _client_lock is None:
        _client_lock = asyncio.Lock()
    return _client_lock


async def init_client() -> None:
    if ENABLED:
        await _get_client()


async def close_client() -> None:
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
            _client = httpx.AsyncClient(timeout=TIMEOUT_SECONDS, follow_redirects=True)
        return _client


def _iso(value: date | datetime | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text[:10]


def _normalize_ticker(ticker: str) -> str:
    return str(ticker or "").strip().upper()


def _parse_date(value: Any) -> str | None:
    text = _iso(value)
    if not text:
        return None
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return None


def _parse_close(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def normalize_close_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict):
        raw_rows = payload.get("prices") or payload.get("items") or payload.get("rows") or []
    elif isinstance(payload, list):
        raw_rows = payload
    else:
        raw_rows = []

    rows: list[dict[str, Any]] = []
    for raw in raw_rows:
        if not isinstance(raw, dict):
            continue
        trade_date = _parse_date(
            raw.get("date") or raw.get("trade_date") or raw.get("business_date") or raw.get("stck_bsop_date")
        )
        close = _parse_close(raw.get("close") or raw.get("close_price") or raw.get("stck_clpr"))
        if trade_date and close is not None:
            rows.append({"date": trade_date, "close": close})
    rows.sort(key=lambda row: row["date"])
    return rows


def close_rows_to_kis_items(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in normalize_close_rows(rows):
        trade_date = row["date"]
        close = row["close"]
        items.append(
            {
                "stck_bsop_date": trade_date.replace("-", ""),
                "stck_clpr": close,
                "date": trade_date,
                "close": close,
                "close_price": close,
            }
        )
    return items


def _cooldown_active() -> bool:
    if _skip_until <= 0:
        return False
    return asyncio.get_event_loop().time() < _skip_until


def _mark_failure() -> None:
    global _skip_until
    if FAILURE_COOLDOWN_SECONDS > 0:
        _skip_until = asyncio.get_event_loop().time() + FAILURE_COOLDOWN_SECONDS


async def get_daily_closes(
    ticker: str,
    *,
    since: date | datetime | str | None = None,
    until: date | datetime | str | None = None,
) -> list[dict[str, Any]]:
    ticker = _normalize_ticker(ticker)
    if not ENABLED or not ticker or _cooldown_active():
        return []

    params = {"ticker": ticker}
    since_iso = _iso(since)
    until_iso = _iso(until)
    if not since_iso or not until_iso:
        return []
    params["since"] = since_iso
    params["until"] = until_iso

    url = f"{BASE_URL}/api/prices/close"
    try:
        client = await _get_client()
        response = await client.get(url, params=params)
        response.raise_for_status()
        return normalize_close_rows(response.json())
    except Exception as exc:
        _mark_failure()
        logger.warning("internal close price API failed (%s): %s", ticker, exc)
        raise ClosePriceClientError(f"internal close price API failed: {ticker}") from exc


async def get_daily_close_items(
    ticker: str,
    *,
    since: date | datetime | str | None = None,
    until: date | datetime | str | None = None,
) -> list[dict[str, Any]]:
    return close_rows_to_kis_items(await get_daily_closes(ticker, since=since, until=until))
