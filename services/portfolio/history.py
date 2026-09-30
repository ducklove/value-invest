from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta

from cache_layer import MemoryTTLCache
from services import stock_price
from services.market.sources import close_price as close_price_client
from services.market.sources import kis_proxy as kis_proxy_client
from services.market.sources import yahoo
from services.portfolio.currencies import infer_yf_currency
from services.portfolio.identifiers import is_korean_stock

logger = logging.getLogger(__name__)

ASSET_HISTORY_CACHE_TTL = 15 * 60
asset_history_cache = MemoryTTLCache("portfolio.asset_history", ASSET_HISTORY_CACHE_TTL, evict_expired_after=0)

LOCAL_BENCHMARK_INDEX_SERIES = {
    "IDX_KOSPI": "KOSPI",
    "IDX_KOSDAQ": "KOSDAQ",
    "IDX_SP500": "SP500",
}
LOCAL_BENCHMARK_COMMODITIES = {
    "GOLD": "gold",
}


async def fetch_yahoo_chart(ticker: str, *, range_: str = "1y", interval: str = "1d") -> dict:
    """``{rows: [{date, close}], currency, meta}`` — 공용 Yahoo chart provider
    위임. insight 히스토리 응답 모양을 유지하려고 session_date 는 뺀다."""
    return await yahoo.fetch_close_series(
        ticker, range_=range_, interval=interval, session_date=False,
        currency_fallback=infer_yf_currency,
    )


async def download_yfinance_history(ticker: str, period: str = "1y") -> dict:
    ticker = (ticker or "").strip()
    if not ticker:
        return {"rows": [], "currency": None}
    key = f"{ticker}:{period}"
    cached = asset_history_cache.get_entry(key)
    if cached is not None:
        return cached.value

    try:
        payload = await asyncio.wait_for(fetch_yahoo_chart(ticker, range_=period), timeout=7.0)
    except Exception as exc:
        logger.warning("asset insight history fetch failed (%s): %s", ticker, exc)
        payload = {"rows": [], "currency": None}
    result = {
        "rows": payload.get("rows") or [],
        "currency": payload.get("currency") or infer_yf_currency(ticker),
    }
    asset_history_cache.set(key, result)
    return result


async def download_korean_history(code: str, period_days: int = 370) -> dict:
    code = (code or "").strip()
    if not is_korean_stock(code):
        return {"rows": [], "currency": None}
    key = f"KIS:{code}:{period_days}"
    cached = asset_history_cache.get_entry(key)
    if cached is not None:
        return cached.value

    end_date = date.today()
    start_date = end_date - timedelta(days=period_days)
    if code.isdigit():
        try:
            local_rows = await asyncio.wait_for(
                close_price_client.get_daily_closes(code, since=start_date, until=end_date),
                timeout=3.0,
            )
        except Exception as exc:
            logger.info("Local Korean asset insight history unavailable (%s): %s", code, exc)
            local_rows = []
        if local_rows:
            result = {
                "rows": [
                    {"date": row["date"], "close": round(float(row["close"]), 6)}
                    for row in local_rows
                    if row.get("date") and row.get("close") is not None
                ],
                "currency": "KRW",
            }
            asset_history_cache.set(key, result)
            return result

    try:
        payload = await asyncio.wait_for(
            kis_proxy_client.get_history(
                code,
                start_date=start_date,
                end_date=end_date,
                period="D",
                adjusted=True,
            ),
            timeout=8.0,
        )
    except Exception as exc:
        logger.warning("Korean asset insight history fetch failed (%s): %s", code, exc)
        return {"rows": [], "currency": "KRW"}

    items = payload.get("items") if isinstance(payload, dict) else []
    if not items and isinstance(payload, dict):
        for value in payload.values():
            if isinstance(value, list) and value:
                items = value
                break
    rows = []
    for item in stock_price._sorted_history_items(items):
        trade_date = stock_price._parse_date(
            stock_price._get_first(item, "stck_bsop_date", "date", "trade_date", "business_date")
        )
        close = stock_price._safe_float(
            stock_price._get_first(item, "stck_clpr", "close_price", "close"),
            zero_as_none=False,
        )
        if trade_date and close is not None:
            rows.append({"date": trade_date.isoformat(), "close": round(float(close), 6)})

    result = {"rows": rows, "currency": "KRW"}
    asset_history_cache.set(key, result)
    return result


async def download_local_benchmark_history(benchmark_code: str, period_days: int = 370) -> list[dict]:
    series_id = LOCAL_BENCHMARK_INDEX_SERIES.get(benchmark_code)
    commodity = LOCAL_BENCHMARK_COMMODITIES.get(benchmark_code)
    if not series_id and not commodity:
        return []

    key = f"LOCAL_BENCH:{benchmark_code}:{period_days}"
    cached = asset_history_cache.get_entry(key)
    if cached is not None:
        return cached.value.get("rows") or []

    end_date = date.today()
    start_date = end_date - timedelta(days=period_days)
    try:
        if series_id:
            rows = await asyncio.wait_for(
                close_price_client.get_macro_index(series_id, since=start_date, until=end_date),
                timeout=2.0,
            )
        else:
            rows = await asyncio.wait_for(
                close_price_client.get_macro_commodity(commodity, since=start_date, until=end_date),
                timeout=2.0,
            )
    except Exception as exc:
        logger.info("Local benchmark insight history unavailable (%s): %s", benchmark_code, exc)
        rows = []

    normalized = [
        {"date": row["date"], "close": round(float(row["close"]), 6)}
        for row in rows
        if row.get("date") and row.get("close") is not None
    ]
    if normalized:
        asset_history_cache.set(key, {"rows": normalized, "currency": "KRW" if series_id else "USD"})
    return normalized
