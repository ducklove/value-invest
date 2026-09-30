from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime
from typing import Any, Iterable, Protocol

from domain import market_calendar
from domain.timeutil import KST as _KST

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
    "fetch_bulk_kr_quotes",
    "fetch_quote_map",
    "kr_trading_day",
    "usable_quote",
]

logger = logging.getLogger(__name__)
# 패스당 개별 조회 동시성. KIS 프록시는 자체 4 req/s limiter 로 직렬화한다.
QUOTE_MAP_CONCURRENCY = 4


class RuntimeQuoteProvider(Protocol):
    async def fetch_quote(
        self,
        stock_code: str,
        *,
        force_refresh: bool = False,
        use_ws_cache: bool = True,
    ) -> dict[str, Any]: ...

    async def fetch_cash_quote(self, stock_code: str) -> dict[str, Any]: ...

    async def fetch_bulk_kr_quotes(self, stock_codes: list[str]) -> dict[str, dict[str, Any]]: ...

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


async def fetch_bulk_kr_quotes(stock_codes: Iterable[str]) -> dict[str, dict[str, Any]]:
    """국내 코드 다건을 벌크 1회로 조회한다(best-effort — 빠진 코드는 호출자가 폴백)."""
    codes = [code for code in dict.fromkeys(stock_codes) if code]
    if not codes:
        return {}
    bulk = getattr(_get_provider(), "fetch_bulk_kr_quotes", None)
    if bulk is None:
        return {}
    return await bulk(codes)


def usable_quote(quote: dict[str, Any] | None) -> bool:
    """가격이 있고 stale 표기가 없는 시세인지 — 배치 평가가 받아들이는 기준."""
    return bool(quote) and quote.get("_stale") is not True and quote.get("price") not in (None, "")


def kr_trading_day(day: date | None = None) -> bool:
    """KRX 거래일 여부. 달력이 모르는 날(미등록 연도·수능일)은 거래일로 본다.

    거래일로 오판하면 종전처럼 강제 조회할 뿐이고, 휴장일로 오판하면 최신가를
    놓치므로 불확실하면 보수적으로 거래일 쪽을 택한다.
    """
    day = day or datetime.now(_KST).date()
    try:
        return market_calendar.closing_at(day.isoformat()) is not None
    except ValueError:
        return True


def _per_code_kwargs(stock_code: str, *, force_kr: bool) -> dict[str, Any]:
    if force_kr and is_korean_stock(stock_code):
        return {"force_refresh": True, "use_ws_cache": False}
    return {}


async def fetch_quote_map(
    stock_codes: Iterable[str],
    *,
    force_kr: bool = True,
    concurrency: int = QUOTE_MAP_CONCURRENCY,
) -> dict[str, dict[str, Any]]:
    """배치 패스(장중 스냅샷·알림)가 공유할 시세 맵을 만든다.

    - 사용자 전체의 고유 코드를 한 번에 받는다(중복 조회 없음).
    - 국내 코드는 벌크 1회(``fetch_bulk_kr_quotes``)로 먼저 채운다.
    - 벌크가 못 채운 국내 코드와 해외·특수자산은 동시성 ``concurrency`` 로
      개별 조회한다. 국내는 ``force_kr`` 이면 종전과 같이 REST 강제 조회,
      아니면(휴장일) 캐시된 마지막 시세를 우선하는 일반 조회를 쓴다.
    - 실패한 코드는 ``{}`` 로 남긴다 — 호출자의 결측 처리(스냅샷 폴백·중단)는
      개별 조회 실패와 같다.
    """
    codes = [code for code in dict.fromkeys(stock_codes) if code]
    if not codes:
        return {}
    quote_map: dict[str, dict[str, Any]] = {}
    korean = [code for code in codes if is_korean_stock(code)]
    if korean:
        # best-effort: 벌크 실패는 개별 조회로 흡수한다(return_exceptions).
        (bulk,) = await asyncio.gather(fetch_bulk_kr_quotes(korean), return_exceptions=True)
        if isinstance(bulk, BaseException):
            logger.warning("bulk quote prefetch failed; falling back per code: %s", bulk)
            bulk = {}
        for code in korean:
            quote = bulk.get(code)
            if usable_quote(quote):
                quote_map[code] = quote

    misses = [code for code in codes if code not in quote_map]
    semaphore = asyncio.Semaphore(max(1, int(concurrency)))

    async def _one(code: str) -> dict[str, Any]:
        async with semaphore:
            return await fetch_quote(code, **_per_code_kwargs(code, force_kr=force_kr))

    outcomes = await asyncio.gather(*(_one(code) for code in misses), return_exceptions=True)
    for code, outcome in zip(misses, outcomes):
        if isinstance(outcome, BaseException):
            logger.warning("quote fetch failed for %s: %s", code, outcome)
            quote_map[code] = {}
        else:
            quote_map[code] = outcome or {}
    return quote_map
