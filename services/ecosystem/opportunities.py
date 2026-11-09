"""기회 보드와 종목 360° — 형제 도구 신호를 모아 같은 척도로 보여 준다(입력 없음, 보유 무관).

정규화 계산은 :mod:`services.ecosystem.signals` 의 순수 함수이고, 이 모듈은 데이터를 받아 넘긴다.

- 도구마다 ``summary.json`` 의 새 선택 필드를 먼저 보고(data-contract §6), 형제가 아직 발행하지 않았으면
  이미 공개된 대시보드 파일(``dashboard-summary``, nps ``current``)로 같은 값을 만든다. 그 파일이 실패하면
  백분위만 빠지고 현재 값은 그대로 나온다.
- 도구는 서로 독립적으로 실패한다. 실패한 도구는 ``tools[].status`` 에 드러나고 나머지는 표시된다.
- 거래대금은 형제의 20일 평균을 먼저 쓰고, 없으면 공유 벌크 시세의 당일 누적을 쓴다. 조회에 실패하면
  값 없이(``None``) 표시한다.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from collections import Counter
from collections.abc import Awaitable, Callable
from datetime import date
from typing import Any

from cache_layer import MemoryTTLCache
from core.errors import DBError
from domain.market_calendar import is_trading_day
from domain.portfolio_codes import is_korean_stock
from domain.timeutil import now_kst, today_kst
from services import stock_quotes
from services.ecosystem import external_tools, links, live_cards, siblings, signals
from services.ecosystem.fetch import FETCH_ERRORS, cached_fetch

logger = logging.getLogger(__name__)

_SOURCE_TTL = 300  # 형제 파일을 보는 주기(external_tools 와 같다)
_BOARD_TTL = 120
_sources = MemoryTTLCache("ecosystem.opportunity.sources", _SOURCE_TTL)
_board_cache = MemoryTTLCache("ecosystem.opportunity.board", _BOARD_TTL)

TOOL_ORDER = (signals.TOOL_PREFERRED, signals.TOOL_HOLDING, signals.TOOL_SPAC, signals.TOOL_BUYBACKS, signals.TOOL_NPS)
_DB_ERRORS = (sqlite3.Error, OSError, DBError)
SPAC_FIELDS = (
    "code", "name", "currentPrice", "annualizedReturn", "liquidationDiscountPct", "currentLiquidationValue",
    "payoutDate", "mergerStatus", "status",
)


def reset() -> None:
    """테스트용 — 캐시를 비운다."""
    _sources.clear()
    _board_cache.clear()


async def _cached_source(key: str, factory: Callable[[], Awaitable[dict]]) -> dict:
    return await cached_fetch(_sources, key, factory)


async def _dashboard_file(tool_id: str) -> Any:
    """형제 대시보드 요약(수백 KB). 실패하면 None — 그 도구의 백분위만 빠진다."""
    try:
        return await external_tools._get_json(siblings.data_url(tool_id, "dashboard-summary"))
    except FETCH_ERRORS as exc:
        logger.info("dashboard summary %s unavailable: %s", tool_id, exc)
        return None


def _published_pairs(envelope: dict | None, percentile_key: str) -> list[dict] | None:
    """새 선택 필드를 발행한 summary 면 그 pairs, 아니면 None."""
    pairs = ((envelope or {}).get("data") or {}).get("pairs")
    if isinstance(pairs, list) and any(isinstance(p, dict) and percentile_key in p for p in pairs):
        return [p for p in pairs if isinstance(p, dict)]
    return None


async def _load_spread_source() -> dict:
    envelope = await siblings.fetch_summary(signals.TOOL_PREFERRED)
    pairs = _published_pairs(envelope, "spreadPct3y")
    if pairs is not None:
        return {"asOf": envelope.get("asOf"), "pairs": pairs, "origin": "summary"}
    current, config = await external_tools._load_pair(signals.TOOL_PREFERRED)
    dashboard = await _dashboard_file(signals.TOOL_PREFERRED)
    return {"asOf": current.get("lastUpdated"), "pairs": signals.spread_pairs(current, config, dashboard),
            "origin": "dashboard" if dashboard is not None else "partial"}


async def _load_holding_source() -> dict:
    envelope = await siblings.fetch_summary(signals.TOOL_HOLDING)
    pairs = _published_pairs(envelope, "ratioPct3y")
    if pairs is not None:
        return {"asOf": envelope.get("asOf"), "pairs": pairs, "origin": "summary"}
    current, config = await external_tools._load_pair(signals.TOOL_HOLDING)
    dashboard = await _dashboard_file(signals.TOOL_HOLDING)
    return {"asOf": current.get("generatedAt") or current.get("lastUpdated"),
            "pairs": signals.holding_pairs(current, config, dashboard),
            "origin": "dashboard" if dashboard is not None else "partial"}


async def _load_spac_source() -> dict:
    envelope = await siblings.fetch_summary(signals.TOOL_SPAC)
    if envelope is not None and isinstance(envelope["data"].get("spacs"), list):
        data = envelope["data"]
        as_of, spacs, origin = data.get("valuationDate") or envelope.get("asOf"), data["spacs"], "summary"
    else:
        payload = await external_tools.fetch_spac_data()
        if not isinstance(payload.get("spacs"), list):
            raise ValueError("SPAC Hunter data unavailable")
        as_of, spacs, origin = payload.get("lastUpdated"), payload["spacs"], "legacy"
    slim = [{key: s.get(key) for key in SPAC_FIELDS} for s in spacs if isinstance(s, dict)]
    return {"asOf": as_of, "spacs": slim, "origin": origin}


async def _load_nps_source() -> dict:
    envelope = await siblings.fetch_summary(signals.TOOL_NPS)
    data = (envelope or {}).get("data") or {}
    if isinstance(data.get("weights"), dict):
        summary = data.get("summary") if isinstance(data.get("summary"), dict) else {}
        return {
            "weights": data["weights"],
            "ownership": data.get("ownership") if isinstance(data.get("ownership"), dict) else {},
            "change": data.get("ownershipChange"),
            "positionsAsOf": data.get("positionsAsOf"),
            "priceAsOf": summary.get("asOf"),
            "names": {t.get("code"): t.get("name") for t in data.get("top") or [] if isinstance(t, dict)},
            "origin": "summary",
        }
    current = await external_tools._get_json(siblings.data_url(signals.TOOL_NPS, "current"))
    return {**signals.nps_source_from_current(current), "origin": "legacy"}


def _attach_urls(items: list[dict]) -> None:
    """응답에 실리는 신호만 딥링크를 만든다(자사주·국민연금은 수천 건이라 전부 만들지 않는다)."""
    for item in items:
        if item.get("url") is None:
            item["url"] = links.go_url(item["tool"], code=item["code"])


async def _preferred_signals(today: date) -> tuple[list[dict], dict]:
    source = await _cached_source("spread", _load_spread_source)
    items = [s for s in (signals.preferred_signal(p, source.get("asOf"), today) for p in source["pairs"]) if s]
    return items, source


async def _holding_signals(today: date) -> tuple[list[dict], dict]:
    source = await _cached_source("holding", _load_holding_source)
    items = [s for s in (signals.holding_signal(p, source.get("asOf"), today) for p in source["pairs"]) if s]
    return items, source


async def _spac_signals(today: date) -> tuple[list[dict], dict]:
    source = await _cached_source("spac", _load_spac_source)
    return signals.spac_signals(source["spacs"], source.get("asOf"), today), source


async def _buyback_signals(today: date, codes: list[str] | None = None) -> tuple[list[dict], dict]:
    data = await external_tools._buybacks_data()
    card = data.get("card") if isinstance(data.get("card"), dict) else {}
    return signals.buyback_signals(data["byCode"], today, codes=codes), {
        "asOf": card.get("asOf"), "origin": "summary-or-legacy",
    }


async def _nps_signals(today: date, codes: list[str] | None = None) -> tuple[list[dict], dict]:
    source = await _cached_source("nps", _load_nps_source)
    return signals.nps_signals(source, today, codes=codes), {
        "asOf": source.get("positionsAsOf"), "origin": source.get("origin"), "change": source.get("change"),
    }


def _tool_status(tool_id: str, result: Any, today: date) -> dict:
    """도구 한 줄 상태. 실패해도 다른 도구에 영향이 없고, 오래된 기준일은 여기서도 드러난다."""
    status: dict[str, Any] = {
        "tool": tool_id,
        "label": signals.TOOL_LABELS[tool_id],
        "status": "failed",
        "asOf": None,
        "distributionEnd": None,
        "warning": "이번 조회에서 데이터를 받지 못했습니다",
        "count": 0,
        "rankable": 0,
    }
    if isinstance(result, BaseException):
        logger.warning("opportunity source %s failed: %s", tool_id, result)
        return status
    items, source = result
    ends = Counter(item["distributionEnd"] for item in items if item.get("distributionEnd"))
    end = ends.most_common(1)[0][0] if ends else None
    days = sorted(item["asOf"] for item in items if item.get("asOf"))
    as_of = days[-1] if days else signals.day_text(source.get("asOf"))
    rankable = sum(1 for item in items if item.get("rankable"))
    no_percentile = source.get("origin") == "partial"
    # 국민연금은 순위 후보가 없어도 정상이다(변화가 없으면 문맥으로만 쓴다).
    no_candidates = bool(items) and not rankable and not no_percentile and tool_id != signals.TOOL_NPS
    notes = [
        signals.freshness_warning(tool_id, as_of, today),
        signals.history_score(100, end, today)[1] if end else None,
        "백분위 분포를 받지 못해 현재 값만 표시합니다" if no_percentile else None,
        "순위를 매길 수 있는 값이 없어 현재 값만 표시합니다" if no_candidates else None,
        _nps_change_warning(source.get("change")) if tool_id == signals.TOOL_NPS else None,
    ]
    status.update(
        status="partial" if no_percentile or no_candidates else "ok",
        asOf=as_of,
        distributionEnd=end,
        warning=" ".join(note for note in notes if note) or None,
        count=len(items),
        rankable=rankable,
    )
    return status


def _nps_change_warning(change: Any) -> str | None:
    if not isinstance(change, dict):
        return "지분율 변화 데이터가 없어 순위에 넣지 않고 문맥으로만 표시합니다"
    if not change.get("comparable"):
        return (f"{change.get('from')}·{change.get('to')} 공시의 지분율이 같아 변화를 계산할 수 없습니다. "
                "순위에 넣지 않고 문맥으로만 표시합니다")
    return None


def _session_complete() -> bool:
    now = now_kst()
    if is_trading_day(now.date()) is False:
        return True
    return now.hour >= 16 or now.hour < 8


async def _liquidity_quotes(codes: list[str]) -> dict:
    domestic = sorted({code for code in codes if is_korean_stock(code)})
    if not domestic:
        return {}
    return await live_cards.best_effort(live_cards.domestic_quotes(domestic), {})


async def _fill_names(rows: list[dict]) -> None:
    """자사주 요약의 ``ratios`` 에는 이름이 없다 — 허브 코드표로 채운다."""
    for row in rows:
        if not row.get("name"):
            name = await external_tools._buyback_stock_name(row["code"])
            row["name"] = name
            row["primary"]["name"] = name


async def _build_board() -> dict:
    today = today_kst()
    results = await asyncio.gather(
        _preferred_signals(today), _holding_signals(today), _spac_signals(today),
        _buyback_signals(today), _nps_signals(today),
        return_exceptions=True,
    )
    tools = [_tool_status(tool_id, result, today) for tool_id, result in zip(TOOL_ORDER, results)]
    if all(tool["status"] == "failed" for tool in tools):
        raise ValueError("all opportunity sources failed")
    collected = [item for result in results if not isinstance(result, BaseException) for item in result[0]]
    rows, eligible = signals.select_candidates(collected)
    quotes = await _liquidity_quotes([row["code"] for row in rows])
    rows = signals.rank_rows(rows, quotes, session_complete=_session_complete())
    await _fill_names(rows)
    for row in rows:
        _attach_urls([row["primary"], *row["others"]])
    return {
        "generatedAt": now_kst().isoformat(timespec="seconds"),
        "rows": rows,
        "tools": tools,
        "eligible": eligible,
        "minScore": signals.BOARD_MIN_SCORE,
        "perTool": signals.BOARD_PER_TOOL,
    }


async def fetch_opportunity_board() -> dict:
    """보유 여부와 무관한 기회 후보 순위. 120초 캐시, 전부 실패하면 1일 이내 마지막 결과나 빈 보드."""
    try:
        return await cached_fetch(_board_cache, "board", _build_board)
    except FETCH_ERRORS as exc:
        logger.warning("opportunity board unavailable: %s", exc)
        return {
            "generatedAt": now_kst().isoformat(timespec="seconds"),
            "rows": [],
            "tools": [_tool_status(tool_id, exc, today_kst()) for tool_id in TOOL_ORDER],
            "eligible": {},
            "minScore": signals.BOARD_MIN_SCORE,
            "perTool": signals.BOARD_PER_TOOL,
        }


async def held_codes(google_sub: str) -> list[str]:
    """보유 배지용 종목코드. 조회 실패는 보드 표시를 막지 않는다."""
    from repositories import portfolio as portfolio_repo

    try:
        holdings = await portfolio_repo.get_portfolio(google_sub)
    except _DB_ERRORS as exc:
        logger.info("held codes unavailable: %s", exc)
        return []
    return sorted({str(item.get("stock_code") or "").strip().upper() for item in holdings if item.get("stock_code")})


async def _etf_signal(code: str, today: date) -> tuple[list[dict], dict]:
    link = await external_tools.etf_link_for(code)
    if not link:
        return [], {"asOf": None, "origin": "none"}
    try:
        rankings = await external_tools._etf_rankings()
    except FETCH_ERRORS as exc:
        logger.info("eiayn rankings unavailable: %s", exc)
        rankings = {}
    ranking = next((e for e in rankings.get("etfs") or []
                    if isinstance(e, dict) and str(e.get("ticker") or "").upper() == link["code"]), None)
    signal = signals.etf_signal(link["code"], ranking, rankings.get("generatedAt"), today)
    signal["url"] = link["url"]
    return [signal], {"asOf": rankings.get("generatedAt"), "origin": "summary-or-legacy"}


async def _fill_signal_names(items: list[dict]) -> None:
    """이름 없는 신호(자사주 ratios·국민연금 맵)에 같은 종목의 이름이나 허브 코드표 이름을 넣는다."""
    for code in {item["code"] for item in items if not item.get("name")}:
        same = [item for item in items if item["code"] == code]
        name = next((item["name"] for item in same if item.get("name")), None)
        name = name or await external_tools._buyback_stock_name(code)
        for item in same:
            item["name"] = name


async def fetch_stock_360(code: str) -> dict:
    """한 종목의 모든 도구 신호(수치). 해당 없는 도구는 빠지고, 실패한 도구는 ``failedTools`` 에 나온다.

    우선주를 보면 보통주의 자사주·국민연금·지주사 신호도 함께 준다. 같은 회사의 문맥이기 때문이다.
    """
    code = (code or "").strip().upper()
    today = today_kst()
    out: dict[str, Any] = {
        "code": code,
        "generatedAt": now_kst().isoformat(timespec="seconds"),
        "signals": [],
        "liquidity": signals.liquidity_info(),
        "failedTools": [],
    }
    if not is_korean_stock(code):
        return out
    preferred = (await asyncio.gather(_preferred_signals(today), return_exceptions=True))[0]
    linked = {code}
    if not isinstance(preferred, BaseException):
        linked |= {s["relatedCode"] for s in preferred[0] if s["code"] == code and s.get("relatedCode")}
    codes = sorted(linked)
    others = await asyncio.gather(
        _holding_signals(today), _spac_signals(today),
        _buyback_signals(today, codes), _nps_signals(today, codes), _etf_signal(code, today),
        return_exceptions=True,
    )
    collected: list[dict] = []
    for tool_id, result in zip((*TOOL_ORDER, signals.TOOL_ETF), (preferred, *others)):
        if isinstance(result, BaseException):
            logger.warning("stock 360 source %s failed (%s): %s", tool_id, code, result)
            out["failedTools"].append({"tool": tool_id, "label": signals.TOOL_LABELS[tool_id]})
            continue
        collected.extend(item for item in result[0] if item["code"] in linked or item.get("relatedCode") == code)
    # 이 코드가 거래 대상인 신호를 먼저, 그 안에서 점수가 있는 신호를 앞에 둔다.
    collected.sort(key=lambda s: (s.get("code") != code, s.get("score") is None, -(s.get("score") or 0)))
    _attach_urls(collected)
    await _fill_signal_names(collected)
    out["signals"] = collected
    with_average = next((s for s in collected if s.get("code") == code and s.get("averageTradedValue20") is not None), None)
    quote = None
    if with_average is None and collected:
        # 보드와 같은 벌크 시세를 먼저 쓴다(같은 종목이 두 화면에서 다른 거래대금을 보이지 않게).
        quote = (await _liquidity_quotes([code])).get(code)
        if quote is None:
            quote = await live_cards.best_effort(stock_quotes.get_quote_snapshot(code), None)
    out["liquidity"] = signals.signal_liquidity(with_average, quote, session_complete=_session_complete())
    return out


async def history_percentiles() -> dict[str, dict[str, int]]:
    """액션 보드용 {'preferred': {코드: 3년 백분위}, 'holding': {…}}. 실패하면 빈 맵(고정 임계값으로 간다)."""
    today = today_kst()
    preferred, holding = await asyncio.gather(
        _preferred_signals(today), _holding_signals(today), return_exceptions=True,
    )
    out: dict[str, dict[str, int]] = {"preferred": {}, "holding": {}}
    if isinstance(preferred, BaseException):
        logger.info("preferred percentiles unavailable: %s", preferred)
    else:
        out["preferred"] = signals.percentile_by_code(preferred[0], "code", "relatedCode")
    if isinstance(holding, BaseException):
        logger.info("holding percentiles unavailable: %s", holding)
    else:
        out["holding"] = signals.percentile_by_code(holding[0], "code")
    return out
