"""현재가 조회의 단일 진입점 — 국내 주식·해외 주식·특수자산(현금/금/암호화폐).

모든 quote 조회(API route, 배치, 알림 엔진)는 이 모듈의 ``get_stock`` /
``get_quote_snapshot`` / ``get_bulk_quote_snapshots`` 를 거친다. 호출자가
``stock_price`` 나 개별 수집기를 직접 부르지 않아야 캐시·폴백·동시성 제어가
한 곳에서 유지된다.

폴백 정책 (위에서 아래로, 성공하면 멈춤):

1. **메모리 캐시** (``_stock_cache``, TTL 60초) — 코드 무관 공통.
2. **KIS WebSocket 캐시** — 국내 주식, 시장 모드(KRX/NXT)가 일치하고
   ``max_ws_age_seconds`` 이내일 때만.
3. **업스트림 조회**
   - 국내 주식: ``stock_price.fetch_quote_snapshot`` — KIS proxy REST
     (NXT 실패 시 KRX 1회 재시도) → 일봉 히스토리 종가(stale 표기).
     다건은 ``get_bulk_quote_snapshots`` 가 네이버 벌크 API 1회 호출로
     처리하고, 빠진 코드만 위 개별 경로로 흘려보낸다(호출자 책임).
   - 해외 주식·특수자산: ``register_quote_fetcher`` 로 주입된 외부 fetcher
     (``services.portfolio.quote_service`` — 현금/FX 환율 스크레이프,
     KRX 금, 암호화폐, 해외는 ticker 해석 후 yfinance/Naver).
4. **dead-stock 캐시 + last-known** — 업스트림 실패 시 5분간 재시도를
   막고(_dead_stock_cache) 마지막으로 성공한 시세를 stale 로 반환.
"""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Awaitable, Callable

from cache_layer import MemoryTTLCache
import kis_ws_manager
import stock_price
from services.portfolio.quotes import should_accept_quote_snapshot


STOCK_CACHE_TTL_SECONDS = 60
DEAD_STOCK_TTL_SECONDS = 300
STOCK_CONT_POLL_SECONDS = 5.0


@dataclass(frozen=True)
class Stock:
    code: str
    current_price: float
    previous_close: float | None
    volume: float | None
    created_at: datetime
    source: str | None = None
    market: str | None = None
    trade_value: float | None = None
    stale: bool = False

    @property
    def price(self) -> float:
        return self.current_price


StockCallback = Callable[[Stock | None], None | Awaitable[None]]
QuoteFetcher = Callable[[str], Awaitable[dict[str, Any]]]


class StockSubscription:
    def __init__(self, task: asyncio.Task):
        self._task = task

    def cancel(self) -> None:
        self._task.cancel()

    @property
    def done(self) -> bool:
        return self._task.done()


_stock_cache = MemoryTTLCache("stock.current", STOCK_CACHE_TTL_SECONDS)
_dead_stock_cache = MemoryTTLCache("stock.current.dead", DEAD_STOCK_TTL_SECONDS)
_last_known: dict[str, Stock] = {}
_locks: dict[str, asyncio.Lock] = {}
_locks_guard = asyncio.Lock()
_quote_fetcher: QuoteFetcher | None = None


def register_quote_fetcher(fetcher: QuoteFetcher) -> None:
    global _quote_fetcher
    _quote_fetcher = fetcher


def _normalize_code(code: str | None) -> str:
    return (code or "").strip().upper()


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _first(mapping: dict[str, Any] | None, *keys: str) -> Any:
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return value
    return None


def _created_at_from_quote(quote: dict[str, Any]) -> datetime:
    for key in ("created_at", "fetched_at"):
        raw = quote.get(key)
        if raw:
            try:
                return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).replace(tzinfo=None)
            except (TypeError, ValueError):
                pass
    raw_ts = quote.get("ts")
    if raw_ts not in (None, ""):
        try:
            return datetime.fromtimestamp(float(raw_ts))
        except (TypeError, ValueError, OSError):
            pass
    return datetime.now()


def stock_from_quote(code: str, quote: dict[str, Any] | None) -> Stock | None:
    if not isinstance(quote, dict):
        return None
    normalized = _normalize_code(code or quote.get("code"))
    current_price = _safe_float(_first(quote, "current_price", "price", "stck_prpr"))
    if current_price is None:
        return None
    previous_close = _safe_float(_first(quote, "previous_close", "base_price", "stck_sdpr"))
    if previous_close is None:
        change = _safe_float(_first(quote, "change", "price_change", "prdy_vrss"))
        if change is not None:
            previous_close = round(current_price - change, 6)
    volume = _safe_float(_first(quote, "volume", "acml_vol", "acml_volm"))
    trade_value = _safe_float(_first(quote, "trade_value", "acml_tr_pbmn", "trading_value"))
    return Stock(
        code=normalized,
        current_price=current_price,
        previous_close=previous_close,
        volume=volume,
        created_at=_created_at_from_quote(quote),
        source=str(quote.get("source") or quote.get("_source") or "") or None,
        market=quote.get("market"),
        trade_value=trade_value,
        stale=quote.get("_stale") is True,
    )


def stock_to_quote(stock: Stock | None) -> dict[str, Any]:
    if stock is None:
        return {}
    change = None
    change_pct = None
    if stock.previous_close not in (None, 0):
        change = round(stock.current_price - stock.previous_close, 2)
        change_pct = round(change / stock.previous_close * 100, 2)
    result: dict[str, Any] = {
        "code": stock.code,
        "date": stock.created_at.date().isoformat(),
        "price": stock.current_price,
        "previous_close": stock.previous_close,
        "change": change,
        "change_pct": change_pct,
        "volume": stock.volume,
        "trade_value": stock.trade_value,
        "source": stock.source or "stock_service",
        "market": stock.market,
        "fetched_at": stock.created_at.isoformat(),
    }
    if stock.stale:
        result["_stale"] = True
    return result


def get_stock_cached(code: str, *, allow_stale: bool = True) -> Stock | None:
    normalized = _normalize_code(code)
    cached = _stock_cache.get(normalized, allow_stale=allow_stale)
    if cached:
        return cached
    return _last_known.get(normalized) if allow_stale else None


async def _lock_for(code: str) -> asyncio.Lock:
    async with _locks_guard:
        lock = _locks.get(code)
        if lock is None:
            lock = asyncio.Lock()
            _locks[code] = lock
        return lock


def _remember(stock: Stock | None) -> Stock | None:
    if stock is None:
        return None
    if stock.stale:
        return stock
    current = _last_known.get(stock.code)
    if current and not should_accept_quote_snapshot(stock_to_quote(current), stock_to_quote(stock)):
        return current
    _dead_stock_cache.delete(stock.code)
    _stock_cache.set(stock.code, stock)
    _last_known[stock.code] = stock
    return stock


def remember_quote(code: str | None, quote: dict[str, Any] | None) -> Stock | None:
    return _remember(stock_from_quote(_normalize_code(code), quote))


def _get_ws_stock(code: str, *, max_age_seconds: float | None) -> Stock | None:
    if not kis_ws_manager.ws_cache_matches_rest_market():
        return None
    quote = kis_ws_manager.get_cached_quote(code)
    if not quote or quote.get("price") is None:
        return None
    if max_age_seconds is not None:
        try:
            ts = float(quote["ts"])
        except (KeyError, TypeError, ValueError):
            return None
        if (datetime.now().timestamp() - ts) > max_age_seconds:
            return None
    return stock_from_quote(code, quote)


async def _fetch_upstream_quote(
    code: str,
    *,
    use_ws_cache: bool,
    max_ws_age_seconds: float | None,
) -> dict[str, Any]:
    if kis_ws_manager.is_korean_stock(code) or _quote_fetcher is None:
        return await stock_price.fetch_quote_snapshot(
            code,
            use_ws_cache=use_ws_cache,
            max_ws_age_seconds=max_ws_age_seconds,
        )
    return await _quote_fetcher(code)


async def get_stock(
    code: str,
    *,
    force_refresh: bool = False,
    use_ws_cache: bool = True,
    max_ws_age_seconds: float | None = stock_price.WS_QUOTE_MAX_AGE_SECONDS,
) -> Stock | None:
    normalized = _normalize_code(code)
    if not normalized:
        return None
    if not force_refresh:
        cached = _stock_cache.get(normalized)
        if cached:
            return cached
        # WS realtime takes priority over a stale dead-stock marker so a
        # single REST failure does not blind us to live ticks for 5 minutes.
        if use_ws_cache:
            ws_stock = _get_ws_stock(normalized, max_age_seconds=max_ws_age_seconds)
            if ws_stock:
                return _remember(ws_stock)
        if _dead_stock_cache.get(normalized):
            return _last_known.get(normalized)

    lock = await _lock_for(normalized)
    async with lock:
        if not force_refresh:
            cached = _stock_cache.get(normalized)
            if cached:
                return cached
            if use_ws_cache:
                ws_stock = _get_ws_stock(normalized, max_age_seconds=max_ws_age_seconds)
                if ws_stock:
                    return _remember(ws_stock)
            if _dead_stock_cache.get(normalized):
                return _last_known.get(normalized)

        quote = await _fetch_upstream_quote(
            normalized,
            use_ws_cache=use_ws_cache,
            max_ws_age_seconds=max_ws_age_seconds,
        )
        stock = stock_from_quote(normalized, quote)
        if stock and not stock.stale:
            return _remember(stock)
        _dead_stock_cache.set(normalized, True)
        return _last_known.get(normalized) or stock


async def get_quote_snapshot(code: str, **kwargs: Any) -> dict[str, Any]:
    return stock_to_quote(await get_stock(code, **kwargs))


async def get_bulk_quote_snapshots(codes: list[str]) -> dict[str, dict[str, Any]]:
    """국내(KRX) 코드 다건을 한 번의 업스트림 호출로 조회해 캐시에 반영한다.

    Best-effort: 업스트림이 해석하지 못한 코드는 결과에서 빠지므로 호출자는
    빠진 코드를 개별 경로(``get_stock``/``get_quote_snapshot``)로 폴백해야
    한다. 성공한 시세는 단건 경로와 같은 캐시(``remember_quote``)에 기록돼
    이후 단건 조회·cached 조회와 일관된 값을 돌려준다.
    """
    normalized = [c for c in dict.fromkeys(_normalize_code(c) for c in codes) if c]
    if not normalized:
        return {}
    bulk = await stock_price.fetch_bulk_quotes_kr(normalized)
    results: dict[str, dict[str, Any]] = {}
    for code, quote in bulk.items():
        remembered = remember_quote(code, quote)
        results[_normalize_code(code)] = stock_to_quote(remembered) if remembered else quote
    return results


def getStock(code: str) -> Stock | None:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(get_stock(code))
    raise RuntimeError("getStock cannot run inside an active event loop; use await get_stock(code)")


def _stock_value_key(stock: Stock | None) -> tuple[Any, ...] | None:
    if stock is None:
        return None
    return (stock.code, stock.current_price, stock.previous_close, stock.volume)


async def _call_callback(callback: StockCallback, stock: Stock | None) -> None:
    result = callback(stock)
    if inspect.isawaitable(result):
        await result


def get_stock_cont(
    code: str,
    callback: StockCallback,
    *,
    interval_seconds: float = STOCK_CONT_POLL_SECONDS,
) -> StockSubscription:
    async def _runner() -> None:
        last_key: tuple[Any, ...] | None = None
        first = True
        while True:
            stock = await get_stock(code)
            current_key = _stock_value_key(stock)
            if first or current_key != last_key:
                await _call_callback(callback, stock)
                last_key = current_key
            first = False
            await asyncio.sleep(max(0.2, float(interval_seconds)))

    task = asyncio.create_task(_runner())
    return StockSubscription(task)


def getStockCont(code: str, callback: StockCallback) -> StockSubscription:
    return get_stock_cont(code, callback)
