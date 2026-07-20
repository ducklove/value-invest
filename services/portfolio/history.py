from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote

import httpx

import close_price_client
import kis_proxy_client
import stock_price
from cache_layer import MemoryTTLCache
from core.http import get_http_client
from services.portfolio.currencies import infer_yf_currency
from services.portfolio.identifiers import is_korean_stock

logger = logging.getLogger(__name__)

YAHOO_HTTP_TIMEOUT = httpx.Timeout(6.0, connect=3.0)
YAHOO_SEM = asyncio.Semaphore(4)
ASSET_HISTORY_CACHE_TTL = 15 * 60
asset_history_cache = MemoryTTLCache("portfolio.asset_history", ASSET_HISTORY_CACHE_TTL)

LOCAL_BENCHMARK_INDEX_SERIES = {
    "IDX_KOSPI": "KOSPI",
    "IDX_KOSDAQ": "KOSDAQ",
    "IDX_SP500": "SP500",
}
LOCAL_BENCHMARK_COMMODITIES = {
    "GOLD": "gold",
}


async def fetch_yahoo_chart(ticker: str, *, range_: str = "1y", interval: str = "1d") -> dict:
    ticker = (ticker or "").strip()
    if not ticker:
        return {"rows": [], "currency": None, "meta": {}}
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{quote(ticker, safe='')}"
    try:
        async with YAHOO_SEM:
            client = await get_http_client("yahoo")
            resp = await client.get(
                url,
                params={"range": range_, "interval": interval, "includePrePost": "false"},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=YAHOO_HTTP_TIMEOUT,
            )
            resp.raise_for_status()
        result = (((resp.json() or {}).get("chart") or {}).get("result") or [None])[0]
        if not result:
            return {"rows": [], "currency": None, "meta": {}}
        meta = result.get("meta") or {}
        timestamps = result.get("timestamp") or []
        quote_data = (((result.get("indicators") or {}).get("quote") or [{}])[0] or {})
        closes = quote_data.get("close") or []
        opens = quote_data.get("open") or []
        highs = quote_data.get("high") or []
        lows = quote_data.get("low") or []
        rows = []
        for i, (ts, close) in enumerate(zip(timestamps, closes)):
            try:
                if close is None:
                    continue
                row = {
                    "date": datetime.fromtimestamp(int(ts), tz=timezone.utc).date().isoformat(),
                    "close": round(float(close), 6),
                }
                # 일봉 캔들(hover 툴팁)용 OHLC — close 만 읽는 기존 소비자와 호환.
                for key, series in (("open", opens), ("high", highs), ("low", lows)):
                    value = series[i] if i < len(series) else None
                    if value is not None:
                        row[key] = round(float(value), 6)
                rows.append(row)
            except Exception:
                continue
        return {
            "rows": rows,
            "currency": (meta.get("currency") or infer_yf_currency(ticker)).upper(),
            "meta": meta,
        }
    except Exception as exc:
        logger.warning("Yahoo chart fetch failed (%s): %s", ticker, exc)
        return {"rows": [], "currency": None, "meta": {}}


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
