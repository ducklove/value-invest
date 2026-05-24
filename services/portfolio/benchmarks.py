from __future__ import annotations

from typing import Any

from cache_layer import MemoryTTLCache

from services.portfolio.identifiers import (
    CASH_FX_CODE,
    SPECIAL_ASSETS,
    common_stock_code,
    is_cash_asset,
    is_korean_stock,
    is_preferred_stock,
)


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
