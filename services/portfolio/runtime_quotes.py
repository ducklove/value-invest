from __future__ import annotations

from typing import Any, Protocol

# 재수출(seam): snapshot_nav/snapshot_intraday 등 배치 호출자가 identifiers를
# 직접 import하지 않고 이 모듈을 통해 쓴다. 테스트도 이 경로를 patch한다.
from services.portfolio.identifiers import is_korean_stock  # noqa: F401

__all__ = [
    "RuntimeQuoteProvider",
    "register_provider",
    "fetch_quote",
    "fetch_cash_quote",
    "load_ticker_map",
    "fx_to_krw",
    "is_korean_stock",
]


class RuntimeQuoteProvider(Protocol):
    async def fetch_quote(
        self,
        stock_code: str,
        *,
        force_refresh: bool = False,
        use_ws_cache: bool = True,
    ) -> dict[str, Any]: ...

    async def fetch_cash_quote(self, stock_code: str) -> dict[str, Any]: ...

    async def load_ticker_map(self) -> dict[str, str]: ...

    async def fx_to_krw(self, nation: str, amount: float) -> float: ...


_provider: RuntimeQuoteProvider | None = None


def register_provider(provider: RuntimeQuoteProvider) -> None:
    global _provider
    _provider = provider


def _get_provider() -> RuntimeQuoteProvider:
    if _provider is None:
        # Importing the quote service registers the provider (and the external
        # stock-quote fetcher). This no longer reaches back into routes, so
        # batch jobs and other services get a provider without loading the HTTP
        # layer.
        from services.portfolio import quote_service  # noqa: F401
    if _provider is None:
        raise RuntimeError("portfolio quote provider is not registered")
    return _provider


async def fetch_quote(
    stock_code: str,
    *,
    force_refresh: bool = False,
    use_ws_cache: bool = True,
) -> dict[str, Any]:
    """Public quote seam for batch/service callers.

    The implementation still delegates to the existing portfolio runtime while
    quote fetching is being extracted. Callers outside HTTP routes should depend
    on this module so the backing implementation can move without touching them.
    """
    return await _get_provider().fetch_quote(
        stock_code,
        force_refresh=force_refresh,
        use_ws_cache=use_ws_cache,
    )


async def fetch_cash_quote(stock_code: str) -> dict[str, Any]:
    return await _get_provider().fetch_cash_quote(stock_code)


async def load_ticker_map() -> dict[str, str]:
    return await _get_provider().load_ticker_map()


async def fx_to_krw(nation: str, amount: float) -> float:
    return await _get_provider().fx_to_krw(nation, amount)
