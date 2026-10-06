"""선물 행의 일간 등락률만 기초자산 시세로 보완한다."""

import asyncio
import math

from domain.broker_assets import is_futures_contract
from services import stock_quotes
from services.brokers import futures_underlyings
from services.brokers.realtime import quote as namuh_quote


def _change_pct(quote: dict) -> float | None:
    value = quote.get("change_pct")
    if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
        return value
    return None


async def enrich_quotes(quotes: dict[str, dict], user: str | None = None) -> dict[str, dict]:
    future_codes = [code for code in quotes if is_futures_contract(code)]
    if not future_codes:
        return quotes
    mapping = await futures_underlyings.underlying_codes()
    underlyings = {mapping[code.removeprefix("KRFUT_")] for code in future_codes
                   if code.removeprefix("KRFUT_") in mapping}
    resolved = {}
    for code in underlyings:
        # 함께 조회한 현물 행과 같은 시세를 사용하며 사용자별 NH 틱은 공유하지 않는다.
        quote = quotes.get(code) or (namuh_quote(user, code) if user else None)
        quote = quote or stock_quotes.stock_to_quote(stock_quotes.get_stock_cached(code, allow_stale=False))
        if quote and _change_pct(quote) is not None:
            resolved[code] = quote
    missing = sorted(underlyings - resolved.keys())
    if missing:
        try:
            bulk = await asyncio.wait_for(stock_quotes.get_bulk_quote_snapshots(missing), timeout=5)
            resolved.update(bulk)
        except Exception:
            pass
        # 벌크 누락 종목은 기존 시세 서비스의 폴백을 사용한다.
        async def fetch(code):
            try:
                stock = await asyncio.wait_for(stock_quotes.get_stock(code), timeout=5)
            except Exception:
                stock = stock_quotes.get_stock_cached(code, allow_stale=True)
            return code, stock_quotes.stock_to_quote(stock)
        remaining = [code for code in missing if _change_pct(resolved.get(code) or {}) is None]
        resolved.update(await asyncio.gather(*(fetch(code) for code in remaining)))
    result = dict(quotes)
    for code in future_codes:
        underlying = mapping.get(code.removeprefix("KRFUT_"))
        # Stock 캐시에 넣으면 previous_close에서 선물 등락률을 다시 계산하므로
        # 응답 직전에 덧붙인다. 선물 가격·전일종가·평가액 계산은 그대로 유지한다.
        result[code] = {**quotes[code], "change_pct": _change_pct(resolved.get(underlying) or {})}
        if underlying:
            result[code]["underlying_code"] = underlying
    return result


async def enrich_items(items: list[dict], user: str) -> None:
    quotes = {item["stock_code"]: item.get("quote") or {} for item in items}
    enriched = await enrich_quotes(quotes, user)
    for item in items:
        if is_futures_contract(item["stock_code"]):
            item["quote"] = enriched[item["stock_code"]]
