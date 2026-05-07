from __future__ import annotations

import asyncio
import logging
import os
from datetime import date, datetime
from typing import Any

import httpx


logger = logging.getLogger(__name__)

BASE_URL = os.getenv("CLOSE_PRICE_API_BASE_URL", "http://192.168.68.84:8400").rstrip("/")
TIMEOUT_SECONDS = float(os.getenv("CLOSE_PRICE_API_TIMEOUT_SECONDS", "2.5"))
FAILURE_COOLDOWN_SECONDS = float(os.getenv("CLOSE_PRICE_API_FAILURE_COOLDOWN_SECONDS", "60"))
ENABLED = os.getenv("CLOSE_PRICE_API_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
API_TOKEN = os.getenv("CLOSE_PRICE_API_TOKEN", os.getenv("FINANCE_PI_API_TOKEN", "")).strip()

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


def _parse_number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _extract_price_rows(payload: Any, ticker: str | None = None) -> list[Any]:
    if isinstance(payload, dict):
        raw_rows = payload.get("prices") or payload.get("items") or payload.get("rows") or []
    elif isinstance(payload, list):
        raw_rows = payload
    else:
        return []

    if isinstance(raw_rows, dict):
        if ticker:
            normalized = _normalize_ticker(ticker)
            exact = raw_rows.get(normalized) or raw_rows.get(str(ticker))
            if isinstance(exact, list):
                return exact
        flattened: list[Any] = []
        for value in raw_rows.values():
            if isinstance(value, list):
                flattened.extend(value)
        return flattened
    if isinstance(raw_rows, list):
        return raw_rows
    return []


def normalize_close_rows(payload: Any, ticker: str | None = None) -> list[dict[str, Any]]:
    raw_rows = _extract_price_rows(payload, ticker)

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


def normalize_daily_rows(payload: Any, ticker: str | None = None) -> list[dict[str, Any]]:
    raw_rows = _extract_price_rows(payload, ticker)

    rows: list[dict[str, Any]] = []
    for raw in raw_rows:
        if not isinstance(raw, dict):
            continue
        trade_date = _parse_date(
            raw.get("date") or raw.get("trade_date") or raw.get("business_date") or raw.get("stck_bsop_date")
        )
        close = _parse_close(raw.get("close") or raw.get("close_price") or raw.get("stck_clpr"))
        if not trade_date or close is None:
            continue
        row: dict[str, Any] = {"date": trade_date, "close": close}
        for target, keys in {
            "open": ("open", "open_price", "stck_oprc"),
            "high": ("high", "high_price", "stck_hgpr"),
            "low": ("low", "low_price", "stck_lwpr"),
            "volume": ("volume", "acml_vol", "accumulated_volume"),
            "trading_value": ("trading_value", "trade_value", "acml_tr_pbmn", "accumulated_trade_value"),
            "market_cap": ("market_cap",),
            "listed_shares": ("listed_shares", "lstn_stcn"),
        }.items():
            value = None
            for key in keys:
                if key in raw:
                    value = _parse_number(raw.get(key))
                    break
            if value is not None:
                row[target] = value
        rows.append(row)
    rows.sort(key=lambda row: row["date"])
    return rows


def close_rows_to_kis_items(rows: Any) -> list[dict[str, Any]]:
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


def daily_rows_to_kis_items(rows: Any) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in normalize_daily_rows(rows):
        trade_date = row["date"]
        close = row["close"]
        item = {
            "stck_bsop_date": trade_date.replace("-", ""),
            "stck_clpr": close,
            "date": trade_date,
            "close": close,
            "close_price": close,
        }
        field_map = {
            "open": ("stck_oprc", "open"),
            "high": ("stck_hgpr", "high"),
            "low": ("stck_lwpr", "low"),
            "volume": ("acml_vol", "volume"),
            "trading_value": ("acml_tr_pbmn", "trade_value", "trading_value"),
            "market_cap": ("market_cap",),
            "listed_shares": ("listed_shares", "lstn_stcn"),
        }
        for source, targets in field_map.items():
            if row.get(source) is None:
                continue
            for target in targets:
                item[target] = row[source]
        items.append(item)
    return items


def _cooldown_active() -> bool:
    if _skip_until <= 0:
        return False
    return asyncio.get_event_loop().time() < _skip_until


def _mark_failure() -> None:
    global _skip_until
    if FAILURE_COOLDOWN_SECONDS > 0:
        _skip_until = asyncio.get_event_loop().time() + FAILURE_COOLDOWN_SECONDS


async def _get_json(path: str, params: dict[str, Any]) -> Any:
    client = await _get_client()
    headers = {"X-Admin-Token": API_TOKEN} if API_TOKEN else None
    response = await client.get(f"{BASE_URL}{path}", params=params, headers=headers)
    response.raise_for_status()
    return response.json()


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

    try:
        return normalize_close_rows(await _get_json("/api/prices/close", params), ticker=ticker)
    except Exception as exc:
        _mark_failure()
        logger.warning("internal close price API failed (%s): %s", ticker, exc)
        raise ClosePriceClientError(f"internal close price API failed: {ticker}") from exc


async def get_daily_prices(
    ticker: str,
    *,
    since: date | datetime | str | None = None,
    until: date | datetime | str | None = None,
    fields: list[str] | tuple[str, ...] | str | None = None,
) -> list[dict[str, Any]]:
    ticker = _normalize_ticker(ticker)
    if not ENABLED or not ticker or _cooldown_active():
        return []

    since_iso = _iso(since)
    until_iso = _iso(until)
    if not since_iso or not until_iso:
        return []
    if fields is None:
        fields = ("open", "high", "low", "close", "volume", "trading_value")
    if not isinstance(fields, str):
        fields = ",".join(fields)
    params = {
        "ticker": ticker,
        "since": since_iso,
        "until": until_iso,
        "fields": fields,
    }
    try:
        return normalize_daily_rows(await _get_json("/api/prices/daily", params), ticker=ticker)
    except Exception as exc:
        _mark_failure()
        logger.warning("internal daily price API failed (%s): %s", ticker, exc)
        raise ClosePriceClientError(f"internal daily price API failed: {ticker}") from exc


async def get_daily_close_items(
    ticker: str,
    *,
    since: date | datetime | str | None = None,
    until: date | datetime | str | None = None,
) -> list[dict[str, Any]]:
    return close_rows_to_kis_items(await get_daily_closes(ticker, since=since, until=until))


async def get_daily_price_items(
    ticker: str,
    *,
    since: date | datetime | str | None = None,
    until: date | datetime | str | None = None,
) -> list[dict[str, Any]]:
    try:
        rows = await get_daily_prices(ticker, since=since, until=until)
        if rows:
            return daily_rows_to_kis_items(rows)
    except ClosePriceClientError:
        # `/api/prices/close` is cheaper and older clients already know its
        # shape. If the richer daily endpoint hiccups, still serve adjusted
        # closes rather than immediately falling back to slower upstreams.
        global _skip_until
        saved_skip_until = _skip_until
        _skip_until = 0.0
        try:
            return await get_daily_close_items(ticker, since=since, until=until)
        finally:
            _skip_until = max(_skip_until, saved_skip_until)
    return await get_daily_close_items(ticker, since=since, until=until)
