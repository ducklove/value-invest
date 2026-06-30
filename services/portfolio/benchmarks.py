from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

import market_indicators
from cache_layer import MemoryTTLCache
from services.portfolio import foreign, fx, quote_service
from services.portfolio.identifiers import (
    CASH_FX_CODE,
    SPECIAL_ASSETS,
    common_stock_code,
    is_cash_asset,
    is_korean_stock,
    is_preferred_stock,
)

logger = logging.getLogger(__name__)


BENCHMARK_NAMES = {
    "IDX_KOSPI": "코스피",
    "IDX_KOSDAQ": "코스닥",
    "IDX_SP500": "S&P500",
    "GOLD": "금",
    "AGG": "미국 종합채권",
    "FX_USDKRW": "USD/KRW",
    "FX_EURKRW": "EUR/KRW",
    "FX_JPYKRW": "JPY/KRW",
    "FX_CNYKRW": "CNY/KRW",
}

BENCHMARK_TO_INDICATOR = {
    "IDX_KOSPI": "KOSPI",
    "IDX_KOSDAQ": "KOSDAQ",
    "IDX_SP500": "SPX",
    "GOLD": "CMDT_GC",
    "FX_USDKRW": "USD_KRW",
    "FX_EURKRW": "EUR_KRW",
    "FX_JPYKRW": "JPY_KRW",
    "FX_CNYKRW": "CNY_KRW",
}

BENCHMARK_YF_TICKER = {
    "IDX_KOSPI": "^KS11",
    "IDX_KOSDAQ": "^KQ11",
    "IDX_SP500": "^GSPC",
    "GOLD": "GC=F",
    "AGG": "AGG",
    "FX_USDKRW": "KRW=X",
    "FX_EURKRW": "EURKRW=X",
    "FX_JPYKRW": "JPYKRW=X",
    "FX_CNYKRW": "CNYKRW=X",
}

BENCHMARK_CACHE_TTL = 120
BENCHMARK_FETCH_TIMEOUT = 2.5
BENCHMARK_ENDPOINT_ITEM_TIMEOUT = 2.0


class BenchmarkQuoteCache:
    def __init__(self, ttl_seconds: float = BENCHMARK_CACHE_TTL):
        self.ttl_seconds = ttl_seconds
        self._data = MemoryTTLCache("portfolio.benchmark_quote", ttl_seconds)

    def get(self, benchmark_code: str, *, allow_stale: bool = True) -> dict[str, Any] | None:
        cached = self._data.get_entry(benchmark_code, allow_stale=allow_stale)
        if not cached:
            return None
        quote = dict(cached.value or {})
        if cached.stale:
            quote["_stale"] = True
        return quote

    def set(self, benchmark_code: str, quote: dict[str, Any]) -> None:
        self._data.set(benchmark_code, dict(quote or {}))

    def clear(self) -> None:
        self._data.clear()


def default_benchmark_for_code(code: str, *, market_type: str | None = None) -> str:
    if is_cash_asset(code):
        return CASH_FX_CODE.get(code, "FX_USDKRW")
    if code in SPECIAL_ASSETS:
        return "FX_USDKRW"
    if is_korean_stock(code):
        if is_preferred_stock(code):
            return common_stock_code(code)
        return "IDX_KOSDAQ" if market_type == "KOSDAQ" else "IDX_KOSPI"
    return "IDX_SP500"


def fast_default_benchmark_for_code(code: str, *, cached_market_type: str | None = None) -> str:
    return default_benchmark_for_code(code, market_type=cached_market_type)


def benchmark_name(code: str) -> str | None:
    return BENCHMARK_NAMES.get(code)


def benchmark_name_fast(code: str, items: list[dict] | None = None, name_cache: dict[str, str] | None = None) -> str:
    if code in BENCHMARK_NAMES:
        return BENCHMARK_NAMES[code]
    if name_cache and code in name_cache:
        return name_cache[code]
    for item in items or []:
        if item.get("stock_code") == code and item.get("stock_name"):
            return item["stock_name"]
    return code


def indicator_to_change_pct(data: dict) -> float | None:
    """Convert market_indicators result into signed percent."""
    if not data:
        return None
    raw = (data.get("change_pct") or "").strip().rstrip("%").replace(",", "")
    if not raw:
        return None
    try:
        pct = float(raw)
    except ValueError:
        return None
    if data.get("direction") == "down" and pct > 0:
        pct = -pct
    return pct


# --- Runtime caches & orchestration (extracted from routes/portfolio.py) ---

market_type_cache = MemoryTTLCache("portfolio.market_type")  # stock_code -> "KOSPI" | "KOSDAQ"
benchmark_name_cache = MemoryTTLCache("portfolio.benchmark_name")
benchmark_quote_cache = BenchmarkQuoteCache()


async def detect_market_type(code: str) -> str:
    """Detect if a Korean stock is KOSPI or KOSDAQ via Naver Finance."""
    if code in market_type_cache:
        return market_type_cache[code]
    try:
        async with foreign._NAVER_SEM:
            async with httpx.AsyncClient(timeout=foreign._NAVER_HTTP_TIMEOUT) as client:
                resp = await client.get(
                    f"https://finance.naver.com/item/main.naver?code={code}",
                    headers={"User-Agent": "Mozilla/5.0"},
                    follow_redirects=True,
                )
        if "코스닥" in resp.text[:30000]:
            market_type_cache.set(code, "KOSDAQ")
        else:
            market_type_cache.set(code, "KOSPI")
    except Exception:
        market_type_cache.set(code, "KOSPI")
    return market_type_cache[code]


async def prefetch_market_types(codes: list[str]):
    """Bulk-detect market types in parallel for codes not yet cached."""
    uncached = [c for c in codes if c not in market_type_cache and is_korean_stock(c) and not is_preferred_stock(c)]
    if not uncached:
        return

    async def _detect(code):
        await detect_market_type(code)

    await asyncio.gather(*[_detect(c) for c in uncached])


async def resolve_default_benchmark(code: str) -> str:
    """Return the default benchmark code for a stock."""
    market_type = await detect_market_type(code) if is_korean_stock(code) and not is_preferred_stock(code) else None
    return default_benchmark_for_code(code, market_type=market_type)


def resolve_default_benchmark_fast(code: str) -> str:
    """Cheap benchmark fallback for first-paint portfolio loading."""
    return fast_default_benchmark_for_code(code, cached_market_type=market_type_cache.get(code))


async def resolve_benchmark_name(code: str) -> str:
    """Resolve a benchmark code to a human-readable name."""
    builtin_name = benchmark_name(code)
    if builtin_name:
        return builtin_name
    if code in benchmark_name_cache:
        return benchmark_name_cache[code]
    # For codes with dots/slashes, try dash variant first (faster for yfinance)
    alt = code.replace(".", "-").replace("/", "-") if not is_korean_stock(code) else None
    if alt and alt != code:
        name = await foreign.yfinance_resolve_name(alt)
        if not name:
            name = await foreign.resolve_name(code)
    else:
        name = await foreign.resolve_name(code)
    result = name or code
    benchmark_name_cache.set(code, result)
    return result


def cached_benchmark_quote(benchmark_code: str, *, allow_stale: bool = True) -> dict | None:
    return benchmark_quote_cache.get(benchmark_code, allow_stale=allow_stale)


def resolve_benchmark_name_fast(code: str, items: list[dict] | None = None) -> str:
    return benchmark_name_fast(code, items, benchmark_name_cache)


def resolve_benchmark_name_from_code_table(
    code: str,
    items: list[dict] | None,
    corp_code_table: dict[str, dict] | None,
) -> str:
    name = resolve_benchmark_name_fast(code, items)
    if name != code:
        return name
    if is_korean_stock(code):
        row = (corp_code_table or {}).get(code) or {}
        corp_name = row.get("corp_name")
        if corp_name:
            benchmark_name_cache.set(code, corp_name)
            return corp_name
    return name


async def fetch_benchmark_quote(benchmark_code: str) -> dict:
    """Fetch a benchmark quote (cached). Reuses market_indicators for shared sources."""
    cached = cached_benchmark_quote(benchmark_code, allow_stale=False)
    if cached is not None:
        return cached

    indicator_code = BENCHMARK_TO_INDICATOR.get(benchmark_code)
    if indicator_code:
        try:
            data = await asyncio.wait_for(
                market_indicators.fetch_indicators([indicator_code]),
                timeout=BENCHMARK_FETCH_TIMEOUT,
            )
            pct = indicator_to_change_pct(data.get(indicator_code) or {})
            q = {"change_pct": pct} if pct is not None else {}
        except Exception as e:
            logger.warning("Indicator-based benchmark fetch failed for %s: %s", benchmark_code, e)
            return cached_benchmark_quote(benchmark_code, allow_stale=True) or {}
    elif benchmark_code.startswith("FX_"):
        daily = await fx.fetch_fx_daily_change(benchmark_code)
        q = {"change_pct": daily.get("change_pct")} if daily and daily.get("change_pct") is not None else {}
    else:
        # It's a stock code (e.g., common stock for preferred)
        # For codes with dots/slashes, try dash variant directly first (faster)
        preset_ticker = BENCHMARK_YF_TICKER.get(benchmark_code)
        if preset_ticker:
            stock_q = await foreign.yfinance_fetch_quote_fast(preset_ticker)
        else:
            alt = foreign.yfinance_direct_ticker(benchmark_code) if not is_korean_stock(benchmark_code) else None
            stock_q = None
        if not preset_ticker and alt and alt != benchmark_code:
            stock_q = await foreign.yfinance_fetch_quote_fast(alt)
            if not stock_q or not stock_q.get("change_pct"):
                stock_q = await quote_service.fetch_quote(benchmark_code)
        elif not preset_ticker:
            stock_q = await quote_service.fetch_quote(benchmark_code)
        q = {"change_pct": stock_q.get("change_pct")} if stock_q else {}

    if not q:
        stale = cached_benchmark_quote(benchmark_code, allow_stale=True)
        if stale:
            return stale
    benchmark_quote_cache.set(benchmark_code, q)
    return q
