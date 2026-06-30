"""Asset-insight orchestration for the portfolio analysis panel.

Extracted from ``routes/portfolio.py`` so the AI/asset-insight logic (per-asset
history, benchmark history, macro indicators, FX, valuation, gold-gap/holding
context, and the background warmup) lives in one cohesive service instead of the
HTTP router. Behavior, caches and warmup state are preserved verbatim; the
``asset_insight`` endpoint stays in the router and now calls ``insights.*``.

Dependency direction: this module depends on the sibling portfolio services
(``benchmarks``, ``quote_service``, ``foreign``, ``history``, ``valuation``,
``time_windows``, ``identifiers``) and the shared data modules
(``asset_insights``, ``integrations``, ``market_indicators``). It must not import
``routes``.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from urllib.parse import quote

import asset_insights
import integrations
import market_indicators
from cache_layer import MemoryTTLCache
from services.portfolio import benchmarks, foreign, quote_service
from services.portfolio import history as portfolio_history
from services.portfolio.identifiers import (
    CASH_FX_CODE as _CASH_FX_CODE,
)
from services.portfolio.identifiers import (
    is_cash_asset as _is_cash_asset,
)
from services.portfolio.identifiers import (
    is_korean_stock as _is_korean_stock,
)
from services.portfolio.identifiers import (
    static_foreign_ticker as _static_foreign_ticker,
)
from services.portfolio.time_windows import today_kst_date
from services.portfolio.valuation import fetch_valuation_basis

logger = logging.getLogger(__name__)


ASSET_HISTORY_CACHE_TTL = portfolio_history.ASSET_HISTORY_CACHE_TTL
asset_history_cache = portfolio_history.asset_history_cache
_INSIGHT_FX_CACHE_TTL = 300
_INSIGHT_WARMUP_TTL = 15 * 60
_insight_fx_cache = MemoryTTLCache("portfolio.insight_fx", _INSIGHT_FX_CACHE_TTL)
_insight_item_warm_cache = MemoryTTLCache("portfolio.insight_item_warmup", _INSIGHT_WARMUP_TTL)
_insight_common_warm_cache = MemoryTTLCache("portfolio.insight_common_warmup", _INSIGHT_WARMUP_TTL)
_insight_common_warm_ts: float = 0.0
_insight_warmup_task: asyncio.Task | None = None

_INSIGHT_FX_TICKER = {
    "USD": "KRW=X",
    "EUR": "EURKRW=X",
    "JPY": "JPYKRW=X",
    "CNY": "CNYKRW=X",
    "HKD": "HKDKRW=X",
    "GBP": "GBPKRW=X",
    "AUD": "AUDKRW=X",
    "CAD": "CADKRW=X",
    "CHF": "CHFKRW=X",
}


async def insight_fx_rate(currency: str | None) -> float:
    currency = (currency or "KRW").upper()
    if currency == "KRW":
        return 1.0
    cached = _insight_fx_cache.get(currency)
    if cached is not None:
        return cached
    ticker = _INSIGHT_FX_TICKER.get(currency)
    if not ticker:
        return 1.0
    payload = await asyncio.wait_for(foreign.fetch_yahoo_chart(ticker, range_="5d"), timeout=7.0)
    rows = payload.get("rows") or []
    meta = payload.get("meta") or {}
    rate = asset_insights.safe_float(meta.get("regularMarketPrice"))
    if rate is None and rows:
        rate = asset_insights.safe_float(rows[-1].get("close"))
    if rate is None or rate <= 0:
        return 1.0
    _insight_fx_cache.set(currency, rate)
    return rate


async def resolve_insight_benchmark(item: dict) -> str:
    code = item["stock_code"]
    manual = item.get("benchmark_code")
    if manual:
        return manual
    if code == "KRX_GOLD":
        return "GOLD"
    if code in {"CRYPTO_BTC", "CRYPTO_ETH", "CRYPTO_USDT"}:
        return "IDX_SP500"
    if _is_cash_asset(code):
        return _CASH_FX_CODE.get(code, "FX_USDKRW")
    profile = asset_insights.classify_asset(code, item.get("stock_name") or "", item.get("currency") or "")
    if profile.get("assetClass") == "bond_etf":
        return "AGG"
    return await benchmarks.resolve_default_benchmark(code)


async def download_yfinance_history(ticker: str, period: str = "1y") -> dict:
    return await portfolio_history.download_yfinance_history(ticker, period=period)


async def download_korean_history(code: str, period_days: int = 370) -> dict:
    return await portfolio_history.download_korean_history(code, period_days=period_days)


async def download_local_benchmark_history(benchmark_code: str, period_days: int = 370) -> list[dict]:
    return await portfolio_history.download_local_benchmark_history(benchmark_code, period_days=period_days)


async def asset_history_for_insight(code: str, item: dict) -> dict:
    special = asset_insights.yfinance_ticker_for_special_asset(code)
    if special:
        return await download_yfinance_history(special)
    if _is_cash_asset(code):
        return {"rows": [], "currency": code.replace("CASH_", "")}
    if _is_korean_stock(code):
        return await download_korean_history(code)
    static = _static_foreign_ticker(code)
    if static:
        return await download_yfinance_history(static["ticker"])
    await foreign.ensure_ticker_map()
    ticker = foreign._ticker_map.get(code) or foreign.yfinance_direct_ticker(code)
    return await download_yfinance_history(ticker)


async def benchmark_history_for_insight(benchmark_code: str | None) -> list[dict]:
    if not benchmark_code:
        return []
    if _is_korean_stock(benchmark_code):
        payload = await download_korean_history(benchmark_code)
        return payload.get("rows") or []
    local_rows = await download_local_benchmark_history(benchmark_code)
    if local_rows:
        return local_rows
    ticker = benchmarks.BENCHMARK_YF_TICKER.get(benchmark_code)
    if not ticker:
        ticker = foreign.yfinance_direct_ticker(benchmark_code)
    if not ticker:
        return []
    payload = await download_yfinance_history(ticker)
    return payload.get("rows") or []


async def fetch_quote_for_insight(stock_code: str) -> dict:
    return await quote_service.fetch_quote(stock_code)


def macro_codes_for_asset(profile: dict, currency: str | None) -> list[str]:
    asset_class = profile.get("assetClass")
    currency = (currency or "").upper()
    codes = ["USD_KRW"]
    if asset_class in {"foreign_stock", "foreign_etf"}:
        codes.extend(["SPX", "IXIC", "US10Y"])
    elif asset_class == "korean_stock":
        codes.extend(["KOSPI", "KOSDAQ", "KR3Y"])
    elif asset_class == "bond_etf":
        codes.extend(["US10Y", "KR3Y"])
    elif asset_class == "gold":
        codes.extend(["CMDT_GC", "US10Y"])
    elif asset_class == "crypto":
        codes.extend(["SPX", "US10Y"])
    elif asset_class == "cash":
        codes.extend(["US10Y", "KR3Y"])
    if currency == "EUR":
        codes.append("EUR_KRW")
    elif currency == "JPY":
        codes.append("JPY_KRW")
    elif currency == "CNY":
        codes.append("CNY_KRW")
    return list(dict.fromkeys(codes))


def format_macro(indicators: dict) -> list[dict]:
    result = []
    for code, data in indicators.items():
        if not data:
            continue
        catalog = market_indicators.CATALOG.get(code, {})
        result.append({
            "code": code,
            "label": catalog.get("label", code),
            "category": catalog.get("category", ""),
            "value": data.get("value") or "",
            "change": data.get("change") or "",
            "changePct": data.get("change_pct") or "",
            "direction": data.get("direction") or "",
        })
    return result


async def fetch_insight_indicators(codes: list[str]) -> dict:
    try:
        return await asyncio.wait_for(market_indicators.fetch_indicators(codes), timeout=5.0)
    except Exception as exc:
        logger.warning("asset insight indicator fetch failed: %s", exc)
        return {}


async def warm_asset_insight_common(initial_delay_seconds: float = 0.0) -> None:
    """Warm shared dependencies that made the first asset-insight click slow."""
    global _insight_common_warm_ts
    if initial_delay_seconds > 0:
        await asyncio.sleep(initial_delay_seconds)
    if _insight_common_warm_cache.get("common") is not None:
        return
    try:
        await asyncio.gather(
            insight_fx_rate("USD"),
            download_yfinance_history("^KS11"),
            download_yfinance_history("^KQ11"),
            download_yfinance_history("^GSPC"),
            download_yfinance_history("AGG"),
            download_yfinance_history("GC=F"),
            benchmarks.fetch_benchmark_quote("IDX_SP500"),
            benchmarks.fetch_benchmark_quote("IDX_KOSPI"),
            benchmarks.fetch_benchmark_quote("IDX_KOSDAQ"),
            fetch_insight_indicators(["USD_KRW", "KOSPI", "KOSDAQ", "SPX", "IXIC", "US10Y", "KR3Y", "CMDT_GC"]),
            return_exceptions=True,
        )
        _insight_common_warm_cache.set("common", True)
        _insight_common_warm_ts = time.monotonic()
        logger.info("Portfolio asset insight common warmup completed")
    except Exception as exc:
        logger.warning("Portfolio asset insight common warmup failed: %s", exc)


def is_asset_insight_candidate(code: str) -> bool:
    return bool((code or "").strip())


async def warm_asset_insight_item(item: dict) -> None:
    code = item.get("stock_code") or ""
    if not is_asset_insight_candidate(code):
        return
    if _insight_item_warm_cache.get(code) is not None:
        return

    effective_benchmark = await resolve_insight_benchmark(item)
    profile = {
        "code": code,
        "name": item.get("stock_name") or code,
        "currency": item.get("currency") or "",
        **asset_insights.classify_asset(code, item.get("stock_name") or "", item.get("currency") or ""),
    }
    await asyncio.gather(
        fetch_quote_for_insight(code),
        asset_history_for_insight(code, item),
        benchmark_history_for_insight(effective_benchmark),
        fetch_insight_indicators(macro_codes_for_asset(profile, item.get("currency"))),
        return_exceptions=True,
    )
    _insight_item_warm_cache.set(code, True)


async def warm_asset_insights_for_items(items: list[dict]) -> None:
    await warm_asset_insight_common()
    limit = int(os.environ.get("PORTFOLIO_INSIGHT_WARMUP_LIMIT", "4"))
    candidates = [it for it in items if is_asset_insight_candidate(it.get("stock_code") or "")]
    if limit > 0:
        candidates = candidates[:limit]
    if not candidates:
        return
    await asyncio.gather(*[warm_asset_insight_item(it) for it in candidates], return_exceptions=True)


def schedule_asset_insight_warmup(items: list[dict]) -> None:
    global _insight_warmup_task
    if os.environ.get("PORTFOLIO_INSIGHT_WARMUP", "0") != "1":
        return
    if _insight_warmup_task and not _insight_warmup_task.done():
        return
    try:
        _insight_warmup_task = asyncio.create_task(warm_asset_insights_for_items(items))
        _insight_warmup_task.add_done_callback(lambda task: task.exception())
    except RuntimeError:
        return


def gold_gap_for_asset(code: str) -> dict | None:
    config = integrations.build_public_integrations().get("goldGap", {})
    asset_key = (config.get("assetByPortfolioCode") or {}).get(code)
    if not asset_key:
        return None
    asset = (config.get("assets") or {}).get(asset_key) or {}
    return {
        "asset": asset_key,
        "label": asset.get("label") or asset_key,
        "latestGapPct": asset.get("latestGapPct"),
        "latestDate": asset.get("latestDate"),
        "thresholdPct": asset.get("thresholdPct"),
        "updatedAt": config.get("updatedAt"),
        "url": f"{config.get('baseUrl', '').rstrip('/')}/?asset={asset_key}" if config.get("baseUrl") else "",
    }


def holding_context_for_asset(code: str) -> dict | None:
    config = integrations.build_public_integrations().get("holdingValue") or {}
    meta = (config.get("meta") or {}).get(code)
    if not isinstance(meta, dict):
        return None
    subsidiaries = meta.get("subsidiaries") or []
    base_url = str(config.get("baseUrl") or "").rstrip("/")
    return {
        "applicable": True,
        "code": code,
        "baseUrl": base_url,
        "url": f"{base_url}/?code={quote(code)}" if base_url else "",
        "subsidiaryCount": len(subsidiaries),
        "meta": meta,
    }


async def fetch_insight_valuation_basis(stock_code: str) -> dict:
    return await fetch_valuation_basis(stock_code, as_of=today_kst_date())


def build_insight_valuation(quote: dict | None, basis: dict) -> dict:
    if not basis.get("applicable"):
        return {"applicable": False}
    price = asset_insights.safe_float((quote or {}).get("price")) or asset_insights.safe_float(basis.get("closePrice"))
    calculated = asset_insights.calculate_valuation_metrics(
        price=price,
        eps=basis.get("eps"),
        bps=basis.get("bps"),
        net_income=basis.get("netIncome"),
        equity=basis.get("equity"),
        per=basis.get("perFallback"),
        pbr=basis.get("pbrFallback"),
    )
    return {
        "applicable": True,
        **calculated,
        "source": basis.get("source"),
        "sourceCode": basis.get("sourceCode"),
        "fiscalYear": basis.get("fiscalYear"),
        "asOf": basis.get("asOf"),
        "treasuryShareRatioPct": basis.get("treasuryShareRatioPct"),
        "treasuryShares": basis.get("treasuryShares"),
        "issuedShares": basis.get("issuedShares"),
        "outstandingShares": basis.get("outstandingShares"),
    }
