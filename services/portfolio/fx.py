"""Foreign-exchange rates and currency conversion.

Extracted from routes/portfolio.py so FX logic lives in one cohesive module
the quote fetchers depend on (router → fx → currencies) instead of being
interleaved with HTTP handlers in a 2,900-line file. Behavior and caches are
preserved verbatim from the original implementation.
"""

from __future__ import annotations

import asyncio
import logging
import math

import httpx

from cache_layer import MemoryTTLCache
from core.errors import ExternalServiceError
from core.http import get_http_client
from services.portfolio import currencies

logger = logging.getLogger(__name__)

_FX_CACHE_TTL = 300  # 5 minutes
_FX_DAILY_CACHE_TTL = 300
# 2026-09 구형 환율 HTML은 HTTP 410. 시장지표와 같은 네이버 JSON을 사용한다.
_FX_HTTP_TIMEOUT = 5.0

_fx_cache = MemoryTTLCache("portfolio.fx_rates", None)
_fx_daily_cache = MemoryTTLCache("portfolio.fx_daily", _FX_DAILY_CACHE_TTL)
SUPPORTED_PRICE_CURRENCIES = frozenset({"KRW", *currencies.CURRENCY_TO_FX_CODE.keys()})


class FXUnavailableError(ExternalServiceError):
    status_code = 503

    def __init__(self, currency: str):
        super().__init__(f"{currency} 환율을 확인할 수 없어 원화 환산을 보류합니다. 잠시 후 다시 시도해 주세요.")


async def get_fx_rates() -> dict[str, float]:
    cached = _fx_cache.get("rates")
    if cached is not None:
        return cached
    semaphore = asyncio.Semaphore(4)

    async def fetch(code):
        async with semaphore:
            return code, await fetch_fx_daily_change(code)

    rows = await asyncio.gather(*(fetch(code) for code in currencies.CURRENCY_TO_FX_CODE.values()))
    rates = {code: row["price"] for code, row in rows if row.get("price") and not row.get("_stale")}
    if rates:
        _fx_cache.set("rates", rates, ttl_seconds=_FX_CACHE_TTL)
    return _fx_cache.get("rates", allow_stale=True) or {}


def _fx_number(value) -> float:
    if value is None or isinstance(value, bool):
        raise ValueError("환율 숫자 누락")
    number = float(str(value).replace(",", "").strip())
    if not math.isfinite(number):
        raise ValueError("환율 숫자 오류")
    return number


async def fetch_fx_daily_change(fx_code: str) -> dict:
    """네이버 고시 환율과 전일 대비. JPY·VND는 원본의 100단위 호가를 유지한다."""
    if fx_code not in currencies.CURRENCY_TO_FX_CODE.values():
        return {}
    cached = _fx_daily_cache.get_entry(fx_code, allow_stale=True)
    if cached is not None and cached.fresh:
        return dict(cached.value)
    try:
        client = await get_http_client("naver")
        resp = await client.get(
            f"https://api.stock.naver.com/marketindex/exchange/{fx_code}",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=_FX_HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        payload = resp.json()
        row = payload.get("exchangeInfo") if isinstance(payload, dict) else None
        if not isinstance(row, dict) or row.get("reutersCode") != fx_code or row.get("unit") != "KRW":
            raise ValueError("환율 종목 또는 단위 불일치")
        price = _fx_number(row.get("closePrice"))
        change = _fx_number(row.get("fluctuations"))
        change_pct = _fx_number(row.get("fluctuationsRatio"))
        if price <= 0 or price - change <= 0:
            raise ValueError("환율은 양수여야 합니다")
        result = {"price": price, "change": change, "change_pct": change_pct,
                  "source": "naver_json", "as_of": row.get("localTradedAt")}
        _fx_daily_cache.set(fx_code, result)
        return result
    except (httpx.HTTPError, UnicodeError, ValueError, TypeError) as e:
        logger.warning("FX daily fetch failed for %s: %s", fx_code, e)
    if cached is not None:
        stale = dict(cached.value)
        stale["_stale"] = True
        return stale
    return {}


async def fx_rate_for_code(fx_code: str) -> float | None:
    unit = currencies.FX_UNIT.get(fx_code, 1)
    daily = await fetch_fx_daily_change(fx_code)
    if daily.get("price") and not daily.get("_stale"):
        rate = float(daily["price"]) / unit
        if math.isfinite(rate) and rate > 0:
            return rate
    rates = await get_fx_rates()
    entry = _fx_cache.get_entry("rates", allow_stale=True)
    if entry is not None and not entry.fresh:
        return None
    rate = rates.get(fx_code)
    if not rate:
        return None
    rate = float(rate) / unit
    return rate if math.isfinite(rate) and rate > 0 else None


async def fx_rate_for_currency(currency: str | None) -> float:
    currency = (currency or "KRW").upper()
    if currency == "KRW":
        return 1.0
    fx_code = currencies.CURRENCY_TO_FX_CODE.get(currency)
    if not fx_code:
        raise FXUnavailableError(currency)
    rate = await fx_rate_for_code(fx_code)
    if rate is None or not math.isfinite(rate) or rate <= 0:
        raise FXUnavailableError(currency)
    return rate


def cached_rate_for_currency(currency: str) -> float | None:
    """정산 메타데이터용 신선한 캐시 읽기. 추가 HTTP 요청을 만들지 않는다."""
    if currency == "KRW":
        return 1.0
    code = currencies.CURRENCY_TO_FX_CODE.get(currency)
    if not code:
        return None
    daily = _fx_daily_cache.get(code)
    rates = _fx_cache.get("rates") or {}
    value = daily.get("price") if daily and not daily.get("_stale") else rates.get(code)
    try:
        rate = float(value) / currencies.FX_UNIT.get(code, 1)
        return rate if math.isfinite(rate) and rate > 0 else None
    except (TypeError, ValueError):
        return None


def normalize_price_currency(currency: str | None, *, default: str = "KRW") -> str:
    normalized = (currency or default or "KRW").strip().upper()
    return normalized if normalized in SUPPORTED_PRICE_CURRENCIES else (default or "KRW")


async def price_to_krw(amount: float, currency: str | None) -> float:
    rate = await fx_rate_for_currency((currency or "KRW").strip().upper())
    return float(amount or 0) * rate


async def annotate_avg_price_krw(items: list[dict]) -> list[dict]:
    components = [part for item in items for part in (item.get("account_positions") or [item])]
    currencies_needed = {normalize_price_currency(part.get("avg_price_currency")) for part in components} - {"KRW"}
    rates = {"KRW": 1.0}
    resolved = await asyncio.gather(*(fx_rate_for_currency(currency) for currency in currencies_needed))
    rates.update(zip(currencies_needed, resolved))
    for item in items:
        parts = item.get("account_positions") or [item]
        quantity = float(item.get("quantity") or 0)
        if quantity and item.get("account_positions"):
            cost = sum(float(part["quantity"]) * float(part["avg_price"]) * rates[normalize_price_currency(part.get("avg_price_currency"))] for part in parts)
            item["avg_price_krw"] = cost / quantity
            if len({part["avg_price_currency"] for part in parts}) > 1:
                item.update(avg_price=cost / quantity, avg_price_currency="KRW", mixed_cost_currency=True)
        else:
            currency = normalize_price_currency(item.get("avg_price_currency"))
            item["avg_price_currency"] = currency
            try:
                item["avg_price_krw"] = float(item.get("avg_price") or 0) * rates[currency]
            except (TypeError, ValueError):
                item["avg_price_krw"] = 0.0
    return items


async def fx_to_krw(nation: str, amount: float) -> float:
    """Convert a foreign-currency amount to KRW."""
    fx_code = currencies.NATION_TO_FX.get(nation)
    if not fx_code:
        if nation in ("KR", "KOR", "KRW"):
            return amount
        raise FXUnavailableError(nation)
    rate = await fx_rate_for_code(fx_code)
    if not rate or not math.isfinite(rate) or rate <= 0:
        raise FXUnavailableError(nation)
    return amount * rate
