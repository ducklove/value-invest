"""카드의 배치 목록·공시 정보는 보존하고 가격 의존 값만 공유 시세로 갱신한다.

호출 빈도는 external_tools의 120초 single-flight 캐시가 제한한다. 국내 종목은
전체 후보를 한 벌크로 받아 순위까지 다시 계산한다. 누락/스테일 시세는 원본을
유지하며, 조회 시각(quoteCheckedAt)은 원본 데이터 기준시각과 구분한다.
"""

from __future__ import annotations

import asyncio
import logging
import math
import sqlite3
import statistics

from cache_layer import MemoryTTLCache
from core.errors import DBError
from domain.market_calendar import MarketCalendarUnknown, closing_at
from domain.numbers import parse_number
from domain.portfolio_codes import is_korean_stock
from domain.timeutil import now_kst
from services import stock_quotes
from services.ecosystem import siblings
from services.ecosystem.fetch import FETCH_ERRORS, cached_fetch
from services.market import indicators
from services.market.sources import premium_quotes

logger = logging.getLogger(__name__)
_config_cache = MemoryTTLCache("external.card_config", 6 * 3600)
_quotes_cache = MemoryTTLCache("external.card_domestic", 120)
_ERRORS = (*FETCH_ERRORS, TimeoutError, sqlite3.Error, DBError)


def number(value) -> float | None:
    n = parse_number(value)
    return n if n is not None and math.isfinite(n) else None


def code(ticker) -> str:
    return str(ticker or "").split(".", 1)[0].strip().upper()


def price(quote) -> float | None:
    if not isinstance(quote, dict) or quote.get("_stale") is True:
        return None
    n = number(quote.get("price"))
    return n if n is not None and n > 0 else None


async def best_effort(awaitable, fallback):
    try:
        return await asyncio.wait_for(awaitable, timeout=10)
    except _ERRORS as exc:
        logger.info("card live quote unavailable: %s", exc)
        return fallback


async def holding_config() -> list:
    from services.ecosystem.external_tools import _get_json

    async def load():
        result = await _get_json(siblings.data_url("holding_value", "config"))
        if not isinstance(result, list):
            raise ValueError("holding config must be a list")
        for pair in result:
            if not isinstance(pair, dict) or not isinstance(pair.get("subsidiaries") or [], list):
                raise ValueError("invalid holding config row")
            if any(not isinstance(sub, dict) for sub in pair.get("subsidiaries") or []):
                raise ValueError("invalid holding subsidiary")
        return result

    return await cached_fetch(_config_cache, "holding", load)


async def domestic_quotes(codes: list[str]) -> dict:
    now = now_kst()
    try:
        trading_day = closing_at(now.date().isoformat()) is not None
    except MarketCalendarUnknown:
        trading_day = now.weekday() < 5
    active = trading_day and 8 <= now.hour < 20  # KRX/NXT/애프터마켓 포함
    # 세션 경계를 키에 넣어 개장 때 장외 캐시가 남지 않게 한다.
    key = ("active:" if active else "closed:") + ",".join(codes)
    return await cached_fetch(_quotes_cache, key, lambda: stock_quotes.get_bulk_quote_snapshots(codes),
                              ttl=120 if active else 900, stale_on_error=False)


def refresh_holding(current: dict, config: list, quotes: dict) -> int:
    """형제와 동일한 자사주 차감·보유주식수 산식. 입력이 모두 있을 때만 갱신."""
    by_id = {r["id"]: r for r in current.get("pairs", [])}
    updated = 0
    for pair in config:
        row = by_id.get(pair.get("id"))
        subs = pair.get("subsidiaries") or []
        hp = price(quotes.get(code(pair.get("holdingTicker"))))
        # 해외 자회사는 환산·거래시각 규칙이 다르므로 형제의 원본을 유지한다.
        if row is None or hp is None or not subs or any(not is_korean_stock(code(s.get("ticker"))) for s in subs):
            continue
        shares = number(pair.get("holdingAdjustedShares"))
        if shares is None:
            total, treasury = number(pair.get("holdingTotalShares")), number(pair.get("holdingTreasuryShares"))
            shares = total - treasury if total is not None and treasury is not None else None
        values = [(price(quotes.get(code(s.get("ticker")))), number(s.get("sharesHeld"))) for s in subs]
        if shares is None or shares <= 0 or any(p is None or qty is None or qty < 0 for p, qty in values):
            continue
        value = math.fsum(p * qty for p, qty in values)
        cap = shares * hp
        row.update(holdingValue=round(value / 1e8, 1), marketCap=round(cap / 1e8, 1),
                   ratio=round(value / cap * 100, 2), ratioChange=None)
        updated += 1
    pair_ids = {p.get("id") for p in config}
    ratios = [number(r.get("ratio")) for r in current.get("pairs", []) if r.get("id") in pair_ids]
    ratios = [r for r in ratios if r is not None]
    if updated and ratios:
        # 형제 averageRatio는 대표값인 중앙값이다(pipeline.snapshot.build_average_entry).
        current.setdefault("summary", {})["averageRatio"] = round(statistics.median(ratios), 2)
    return updated


def refresh_spread(current: dict, config: list, quotes: dict) -> int:
    updated = 0
    for pair in config:
        cp = price(quotes.get(code(pair.get("commonTicker"))))
        pp = price(quotes.get(code(pair.get("preferredTicker"))))
        if cp is None or pp is None:
            continue
        current.setdefault("prices", {}).setdefault(pair["id"], {}).update(
            commonPrice=cp, preferredPrice=pp, spread=round((cp - pp) / cp * 100, 2), spreadChange=None)
        updated += 1
    # 형제도 다중 우선주는 보통주별 최대 괴리 쌍 하나로 평균을 낸다.
    representatives = {}
    for pair in config:
        spread = number((current.get("prices", {}).get(pair["id"]) or {}).get("spread"))
        common = code(pair.get("commonTicker"))
        if spread is not None:
            representatives[common] = max(representatives.get(common, spread), spread)
    spreads = list(representatives.values())
    if updated and spreads:
        current["averageSpread"] = round(math.fsum(spreads) / len(spreads), 2)
        current["averageSpreadChange"] = None
    return updated


def mark(card: dict, updated: int, total: int) -> None:
    if updated:
        card["quoteCheckedAt"] = now_kst().isoformat(timespec="seconds")
        card["partialQuotes"] = updated < total


async def refresh(out: dict) -> None:
    # 배치 조회(최대 summary 8초 + legacy 8초) 뒤 시세 보강은 최대 12초.
    # 부분 완료된 카드는 그대로 반환하고 단일 제공처 지연이 전체 응답을 막지 않게 한다.
    try:
        await asyncio.wait_for(_refresh(out), timeout=12)
    except _ERRORS as exc:
        logger.info("card live refresh incomplete, keeping snapshot: %s", exc)


async def _refresh(out: dict) -> None:
    from services.ecosystem import external_tools as ext

    holding, spread, spac = await asyncio.gather(
        best_effort(ext._load_pair("holding_value"), None) if "holding" in out else asyncio.sleep(0, result=None),
        best_effort(ext._load_pair("common_preferred_spread"), None) if "spread" in out else asyncio.sleep(0, result=None),
        best_effort(ext._spac_current(), None) if "spac" in out else asyncio.sleep(0, result=None),
    )
    cfg = await best_effort(holding_config(), []) if holding else []
    codes = set()
    for pair in cfg:
        codes.add(code(pair.get("holdingTicker")))
        codes.update(code(s.get("ticker")) for s in pair.get("subsidiaries") or [])
    if spread:
        codes.update(code(p.get(k)) for p in spread[1] for k in ("commonTicker", "preferredTicker"))
    if spac:
        codes.update(spac.get("prices") or {})
    wanted = sorted(c for c in codes if is_korean_stock(c))
    # 지표 서비스의 항목별 TTL·실패 쿨다운을 공유한다(기준금리는 원천의 일별 주기).
    indicator_codes = ["USD_KRW", "CMDT_GC"] if "goldGap" in out else []
    if "bondMate" in out:
        indicator_codes += ["US_BASE", "KR_BASE", "US10Y", "US2Y", "KR10Y", "KR3Y"]
    quotes, market = await asyncio.gather(
        best_effort(domestic_quotes(wanted), {}) if wanted else asyncio.sleep(0, result={}),
        best_effort(indicators.fetch_indicators(indicator_codes), {}) if indicator_codes else asyncio.sleep(0, result={}),
    )
    if holding:
        updated = refresh_holding(holding[0], cfg, quotes)
        out["holding"] = ext._summarize_holding(*holding)
        mark(out["holding"], updated, sum(r.get("id") != "_average" for r in holding[0].get("pairs") or []))
    if spread:
        updated = refresh_spread(*spread, quotes)
        out["spread"] = ext._summarize_spread(*spread)
        mark(out["spread"], updated, len(spread[1]))
    if spac:
        updated = 0
        for c, row in (spac.get("prices") or {}).items():
            p = price(quotes.get(c))
            if p is not None:
                row["currentPrice"] = p
                updated += 1
        out["spac"] = ext._summarize_spac(spac)
        mark(out["spac"], updated, len(spac.get("prices") or {}))
    if "bondMate" in out:
        refresh_bonds(out["bondMate"], market)
    if "goldGap" in out:
        await refresh_gold(out["goldGap"], market)


def indicator_price(market: dict, key: str) -> float | None:
    q = market.get(key) or {}
    return None if q.get("_stale") is True else number(q.get("value"))


async def refresh_gold(card: dict, market: dict) -> None:
    fx = indicator_price(market, "USD_KRW")
    if fx is None or fx <= 0:
        return
    rows = card.get("assets") or []
    symbols = {"bitcoin": ("CRYPTO_BTC", "BTC-USD"), "eth": ("CRYPTO_ETH", "ETH-USD"),
               "usdt": (None, "USDT-USD"), "gold": ("KRX_GOLD", None)}
    async def update(row):
        domestic_code, intl_symbol = symbols[row["key"]]
        domestic, intl = await asyncio.gather(
            best_effort(stock_quotes.get_quote_snapshot(domestic_code) if domestic_code else premium_quotes.usdt_krw(), {}),
            best_effort(premium_quotes.international_crypto(intl_symbol), {}) if intl_symbol else asyncio.sleep(0, result={}),
        )
        dp = price(domestic)
        if intl_symbol:
            usd = price(intl)
        elif row["key"] == "gold":
            gold = indicator_price(market, "CMDT_GC")
            usd = gold / 31.1035 if gold is not None else None
        else:
            usd = None
        if dp is None or usd is None or usd <= 0:
            return False
        row.update(gap=round((dp / (usd * fx) - 1) * 100, 2), quoteCheckedAt=now_kst().isoformat(timespec="seconds"))
        # 전체 제한시간에 걸려도 이미 갱신한 자산의 조회시각·부분갱신 표시는 남긴다.
        mark(card, sum("quoteCheckedAt" in r for r in rows), len(rows))
        return True
    updated = await asyncio.gather(*(update(r) for r in rows if r.get("key") in symbols))
    mark(card, sum(updated), len(rows))


def refresh_bonds(card: dict, market: dict) -> None:
    keys = {"US_BASE": "usBase", "KR_BASE": "krBase", "US10Y": "us10y", "US2Y": "us2y",
            "KR10Y": "kr10y", "KR3Y": "kr3y"}
    updated = 0
    for c, key in keys.items():
        value = indicator_price(market, c)
        if value is not None:
            q = market[c]
            card[key] = {"value": value, "change": number(q.get("change")), "asOf": q.get("as_of") or q.get("date")}
            updated += 1
    # 두 레그 모두 이번 지표 조회에서 확보했을 때만 스프레드를 바꾼다.
    for long, short, field in (("US10Y", "US2Y", "usCurveSpreadBp"), ("KR10Y", "KR3Y", "krCurveSpreadBp")):
        a, b = indicator_price(market, long), indicator_price(market, short)
        if a is not None and b is not None:
            card[field] = round((a - b) * 100, 1)
            if long == "US10Y":
                card["usCurveInverted"] = a < b
    mark(card, updated, len(keys))
