"""Runtime quote implementation for portfolio assets.

This module holds the concrete quote-fetching logic that used to live in
``routes/portfolio.py`` and was reachable only by importing the HTTP router.
Pulling it here lets ``services.portfolio.runtime_quotes`` resolve a provider
without a back-import into ``routes`` — batch jobs and other services depend on
the seam, and the seam now depends on this service instead of the web layer.

On import this module registers:
- the external-asset quote fetcher used by ``services.stock_quotes``
  (cash / gold / crypto / foreign), and
- the ``RuntimeQuoteProvider`` used by ``runtime_quotes``.

Behavior is preserved verbatim from the original router implementation.
"""

from __future__ import annotations

from typing import Any

from services import stock_quotes
from services.portfolio import currencies
from services.portfolio import foreign
from services.portfolio import fx
from services.portfolio import runtime_quotes
from services.portfolio import special_assets
from services.portfolio.identifiers import (
    CASH_FX_CODE as _CASH_FX_CODE,
    is_cash_asset as _is_cash_asset,
    is_korean_stock as _is_korean_stock,
)

# How fresh a cached WebSocket quote must be to satisfy a quote request before
# falling back to a REST/polling fetch.
WS_QUOTE_MAX_AGE_SECONDS = 90


async def fetch_cash_quote(stock_code: str) -> dict:
    """Fetch cash quote: KRW=1, others=FX rate to KRW with daily change."""
    if stock_code == "CASH_KRW":
        return {"price": 1, "change": 0, "change_pct": 0}
    fx_code = _CASH_FX_CODE.get(stock_code)
    if not fx_code:
        return {}
    unit = currencies.FX_UNIT.get(fx_code, 1)
    # Prefer the per-currency daily-quote scrape — gives us change vs prev close.
    daily = await fx.fetch_fx_daily_change(fx_code)
    if daily.get("price"):
        price = daily["price"] / unit
        change = daily["change"] / unit
        return {
            "price": round(price, 2),
            "change": round(change, 4),
            "change_pct": daily["change_pct"],
        }
    # Fallback: exchangeList scrape — current rate only, no change.
    rate = await fx.fx_rate_for_code(fx_code)
    if not rate:
        return {}
    return {"price": round(rate, 2), "change": 0, "change_pct": 0}


async def fetch_external_quote_for_stock_service(stock_code: str) -> dict:
    if _is_cash_asset(stock_code):
        return await fetch_cash_quote(stock_code)
    elif stock_code == "KRX_GOLD":
        return await special_assets.fetch_krx_gold_quote()
    elif special_assets.is_crypto_asset(stock_code):
        return await special_assets.fetch_crypto_quote(stock_code)
    elif not _is_korean_stock(stock_code):
        # Use resolved ticker if available, otherwise try to resolve
        await foreign.ensure_ticker_map()
        ticker = foreign._ticker_map.get(stock_code, stock_code)
        q = await foreign.fetch_foreign_quote(ticker)
        if not q and ticker == stock_code:
            resolved = await foreign.resolve_foreign_reuters(stock_code)
            if resolved and resolved != stock_code:
                await foreign.save_ticker(stock_code, resolved)
                q = await foreign.fetch_foreign_quote(resolved)
        return q
    return {}


async def fetch_quote(
    stock_code: str,
    *,
    force_refresh: bool = False,
    use_ws_cache: bool = True,
) -> dict:
    q = stock_quotes.stock_to_quote(
        await stock_quotes.get_stock(
            stock_code,
            force_refresh=force_refresh,
            use_ws_cache=use_ws_cache,
            max_ws_age_seconds=WS_QUOTE_MAX_AGE_SECONDS,
        )
    )
    return q


def cached_quote_for_code(code: str) -> dict:
    return stock_quotes.stock_to_quote(stock_quotes.get_stock_cached(code, allow_stale=False))


async def enrich_with_cached_quotes(items: list[dict]) -> list[dict]:
    """Attach cached quotes — WebSocket cache preferred, then polling cache."""
    result = []
    for item in items:
        enriched = dict(item)
        enriched["quote"] = cached_quote_for_code(item["stock_code"])
        result.append(enriched)
    return result


class _PortfolioRuntimeQuoteProvider:
    async def fetch_quote(
        self,
        stock_code: str,
        *,
        force_refresh: bool = False,
        use_ws_cache: bool = True,
    ) -> dict[str, Any]:
        return await fetch_quote(
            stock_code,
            force_refresh=force_refresh,
            use_ws_cache=use_ws_cache,
        )

    async def fetch_cash_quote(self, stock_code: str) -> dict[str, Any]:
        return await fetch_cash_quote(stock_code)

    async def load_ticker_map(self) -> dict[str, str]:
        await foreign.ensure_ticker_map()
        return dict(foreign._ticker_map)

    async def fx_to_krw(self, nation: str, amount: float) -> float:
        return await fx.fx_to_krw(nation, amount)


def register() -> None:
    """Wire this service into the stock-quote and runtime-quote seams.

    Idempotent: the module-level registrations below run once on import, and
    ``runtime_quotes._get_provider`` imports this module to trigger them when a
    provider is requested outside the HTTP app (batch jobs, other services).
    """
    stock_quotes.register_quote_fetcher(fetch_external_quote_for_stock_service)
    runtime_quotes.register_provider(_PortfolioRuntimeQuoteProvider())


register()
