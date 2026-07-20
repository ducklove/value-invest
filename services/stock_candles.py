"""종목 일봉(OHLC) 단일 진입점 — hover 캔들 툴팁용 경량 조회.

- 국내 주식: ``kis_proxy_client.get_history`` (finance-pi 로컬 우선 + KIS proxy
  폴백)가 OHLC 를 포함해 그대로 쓴다. 별도 저장 없음.
- 해외 주식/ETF: Yahoo v8 chart — ``services.portfolio.history.fetch_yahoo_chart``
  가 OHLC 확장 rows 를 돌려준다. ticker 해석은 asset-insight 와 동일 규칙
  (static ticker → ticker_map → direct ticker).
- 특수자산(현금/KRX 금/암호화폐): 미지원(``supported: False``) — gold_gap
  프로젝트가 일봉 API 를 제공하면 그때 연결한다. 프론트는 supported=False 를
  캐시해 툴팁을 띄우지 않는다.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta

import kis_proxy_client
import stock_price
from cache_layer import MemoryTTLCache
from services.portfolio import foreign
from services.portfolio import history as portfolio_history
from services.portfolio.identifiers import (
    is_korean_stock,
    is_special_asset,
    static_foreign_ticker,
)

logger = logging.getLogger(__name__)

CANDLE_CACHE_TTL_SECONDS = 15 * 60
_candle_cache = MemoryTTLCache("stock.candles", CANDLE_CACHE_TTL_SECONDS)

DEFAULT_DAYS = 60
MAX_DAYS = 120


def _payload(code: str, days: int, *, candles: list[dict], currency: str | None,
             source: str | None, supported: bool = True) -> dict:
    return {
        "code": code,
        "days": days,
        "currency": currency,
        "source": source,
        "supported": supported,
        "candles": candles,
    }


def _candle_from_kis_item(item: dict) -> dict | None:
    trade_date = stock_price._parse_date(
        stock_price._get_first(item, "stck_bsop_date", "date", "trade_date", "business_date")
    )
    close = stock_price._safe_float(
        stock_price._get_first(item, "stck_clpr", "close_price", "close"),
        zero_as_none=False,
    )
    if trade_date is None or close is None:
        return None
    candle: dict = {"date": trade_date.isoformat(), "close": close}
    for key, *aliases in (
        ("open", "stck_oprc"),
        ("high", "stck_hgpr"),
        ("low", "stck_lwpr"),
        ("volume", "acml_vol"),
    ):
        value = stock_price._safe_float(stock_price._get_first(item, key, *aliases))
        if value is not None:
            candle[key] = value
    return candle


async def _korean_candles(code: str, days: int) -> dict:
    end_date = date.today()
    # 주말·휴장 감안 넉넉한 달력일 범위에서 마지막 days 개만 취한다.
    start_date = end_date - timedelta(days=days * 2 + 14)
    payload = await kis_proxy_client.get_history(
        code, start_date=start_date, end_date=end_date, period="D", adjusted=True,
    )
    items = payload.get("items") if isinstance(payload, dict) else []
    candles = []
    for item in stock_price._sorted_history_items(items):
        candle = _candle_from_kis_item(item)
        if candle:
            candles.append(candle)
    return _payload(code, days, candles=candles[-days:], currency="KRW", source="kis")


def _resolve_yahoo_ticker(code: str) -> str:
    static = static_foreign_ticker(code)
    if static:
        return static["ticker"]
    return foreign._ticker_map.get(code) or foreign.yfinance_direct_ticker(code)


async def _foreign_candles(code: str, days: int) -> dict:
    await foreign.ensure_ticker_map()
    ticker = _resolve_yahoo_ticker(code)
    if not ticker:
        return _payload(code, days, candles=[], currency=None, source=None, supported=False)
    period = "3mo" if days <= 60 else "6mo" if days <= 120 else "1y"
    history = await portfolio_history.download_yfinance_history(ticker, period=period)
    rows = history.get("rows") or []
    return _payload(
        code, days,
        candles=rows[-days:],
        currency=history.get("currency"),
        source="yahoo",
    )


async def get_daily_candles(code: str, days: int = DEFAULT_DAYS) -> dict:
    code = (code or "").strip().upper()
    days = max(20, min(int(days or DEFAULT_DAYS), MAX_DAYS))
    if not code:
        return _payload(code, days, candles=[], currency=None, source=None, supported=False)
    if is_special_asset(code):
        # gold_gap 일봉 API 연결 전까지 미지원 — 프론트는 툴팁을 띄우지 않는다.
        return _payload(code, days, candles=[], currency=None, source=None, supported=False)

    cache_key = f"{code}:{days}"
    cached = _candle_cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        if is_korean_stock(code):
            result = await _korean_candles(code, days)
        else:
            result = await _foreign_candles(code, days)
    except Exception as exc:
        logger.warning("일봉 캔들 조회 실패(%s): %s", code, exc)
        return _payload(code, days, candles=[], currency=None, source=None)

    if result["candles"] or not result["supported"]:
        _candle_cache.set(cache_key, result)
    return result
