"""Foreign-exchange rates and currency conversion.

Extracted from routes/portfolio.py so FX logic lives in one cohesive module
the quote fetchers depend on (router → fx → currencies) instead of being
interleaved with HTTP handlers in a 2,900-line file. Behavior and caches are
preserved verbatim from the original implementation.
"""

from __future__ import annotations

import asyncio
import logging
import re

import httpx

from cache_layer import MemoryTTLCache
from services.portfolio import currencies

logger = logging.getLogger(__name__)

_FX_CACHE_TTL = 300  # 5 minutes
_FX_DAILY_CACHE_TTL = 300

_fx_cache = MemoryTTLCache("portfolio.fx_rates", None)
_fx_daily_cache = MemoryTTLCache("portfolio.fx_daily", _FX_DAILY_CACHE_TTL)
SUPPORTED_PRICE_CURRENCIES = frozenset({"KRW", *currencies.CURRENCY_TO_FX_CODE.keys()})


async def get_fx_rates() -> dict[str, float]:
    cached = _fx_cache.get("rates")
    if cached is not None:
        return cached
    try:
        rates: dict[str, float] = {}
        async with httpx.AsyncClient(timeout=5) as c:
            for page in (1, 2):
                r = await c.get(
                    f"https://finance.naver.com/marketindex/exchangeList.naver?page={page}",
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                rows = re.findall(
                    r'marketindexCd=(\w+)"[^>]*>[^<]*</a>.*?<td class="sale">([^<]+)',
                    r.text, re.DOTALL,
                )
                for code, val in rows:
                    try:
                        rates[code] = float(val.strip().replace(",", ""))
                    except ValueError:
                        pass
        if rates:
            _fx_cache.set("rates", rates, ttl_seconds=_FX_CACHE_TTL)
    except Exception:
        pass
    return _fx_cache.get("rates", allow_stale=True) or {}


async def fetch_fx_daily_change(fx_code: str) -> dict:
    """Today's FX rate + change vs. the previous business day, from Naver.

    Uses the per-currency daily-quote page. Returns {} on failure; callers can
    fall back to a plain rate lookup.
    """
    cached = _fx_daily_cache.get_entry(fx_code, allow_stale=True)
    if cached is not None and cached.fresh:
        return dict(cached.value)
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(
                f"https://finance.naver.com/marketindex/exchangeDailyQuote.naver?marketindexCd={fx_code}",
                headers={"User-Agent": "Mozilla/5.0"},
            )
            html = resp.content.decode("euc-kr", errors="ignore")
            rows = re.findall(
                r'<tr class="(?:up|down)">\s*<td class="date">[^<]+</td>\s*<td class="num">([\d,\.]+)</td>',
                html,
            )
            if len(rows) >= 2:
                price = float(rows[0].replace(",", ""))
                prev = float(rows[1].replace(",", ""))
                change = price - prev
                change_pct = round(change / prev * 100, 2) if prev else 0.0
                result = {"price": price, "change": change, "change_pct": change_pct}
                _fx_daily_cache.set(fx_code, result)
                return result
            if rows:
                # Only one row available (first listing day?) — no delta.
                price = float(rows[0].replace(",", ""))
                result = {"price": price, "change": 0.0, "change_pct": 0.0}
                _fx_daily_cache.set(fx_code, result)
                return result
    except Exception as e:
        logger.warning("FX daily fetch failed for %s: %s", fx_code, e)
    if cached is not None:
        stale = dict(cached.value)
        stale["_stale"] = True
        return stale
    return {}


async def fx_rate_for_code(fx_code: str) -> float | None:
    unit = currencies.FX_UNIT.get(fx_code, 1)
    daily = await fetch_fx_daily_change(fx_code)
    if daily.get("price"):
        return float(daily["price"]) / unit
    rates = await get_fx_rates()
    rate = rates.get(fx_code)
    if not rate:
        return None
    return float(rate) / unit


async def fx_rate_for_currency(currency: str | None) -> float:
    currency = (currency or "KRW").upper()
    if currency == "KRW":
        return 1.0
    fx_code = currencies.CURRENCY_TO_FX_CODE.get(currency)
    if not fx_code:
        return 1.0
    rate = await fx_rate_for_code(fx_code)
    return rate if rate and rate > 0 else 1.0


def normalize_price_currency(currency: str | None, *, default: str = "KRW") -> str:
    normalized = (currency or default or "KRW").strip().upper()
    return normalized if normalized in SUPPORTED_PRICE_CURRENCIES else (default or "KRW")


async def price_to_krw(amount: float, currency: str | None) -> float:
    rate = await fx_rate_for_currency(normalize_price_currency(currency))
    return float(amount or 0) * rate


async def annotate_avg_price_krw(items: list[dict]) -> list[dict]:
    currencies_needed = {
        normalize_price_currency(item.get("avg_price_currency"))
        for item in items
        if normalize_price_currency(item.get("avg_price_currency")) != "KRW"
    }
    rates: dict[str, float] = {"KRW": 1.0}
    if currencies_needed:
        resolved = await asyncio.gather(
            *(fx_rate_for_currency(currency) for currency in currencies_needed),
            return_exceptions=True,
        )
        for currency, rate in zip(currencies_needed, resolved):
            rates[currency] = float(rate) if isinstance(rate, (int, float)) and rate > 0 else 1.0

    for item in items:
        currency = normalize_price_currency(item.get("avg_price_currency"))
        item["avg_price_currency"] = currency
        try:
            avg_price = float(item.get("avg_price") or 0)
        except (TypeError, ValueError):
            avg_price = 0.0
        item["avg_price_krw"] = avg_price * rates.get(currency, 1.0)
    return items


async def fx_to_krw(nation: str, amount: float) -> float:
    """Convert a foreign-currency amount to KRW."""
    fx_code = currencies.NATION_TO_FX.get(nation)
    if not fx_code:
        return amount  # unknown nation, assume already KRW-like
    rate = await fx_rate_for_code(fx_code)
    if not rate:
        return amount
    return amount * rate
