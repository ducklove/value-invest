"""외부 분석 도구(GitHub Pages) 요약 소비 모듈.

ducklove 계정의 독립 정적 대시보드들이 GitHub Actions 배치로 생성·커밋하는
public JSON을 읽어, 투자정보 대시보드의 허브 위젯과 종목분석 deep-link 카드에
쓸 요약을 만든다.

- holding_value         : 지주사 NAV 디스카운트(보유지분가치 vs 조정시총 비율)
- common_preferred_spread: 우선주-보통주 괴리율
- gold_gap              : 김치프리미엄(금/BTC/USDT 국내외 가격차)
- spac-hunter           : 국내 스팩 현재가/공모가·연환산 기대수익률
- nps-tracker           : 국민연금 국내주식 포트폴리오(비중 상위·NAV·총평가액)
- eiayn                 : ETF 평가(AIYN 점수) — 오늘의 추천 ETF(TOP 100 일일 추첨 5선)
- buybacks              : 자사주 매입·처분·소각 분석(자사주 보유비중 상위)

데이터 출처(docs/ecosystem/data-contract.md §7):

1. 도구별 ``<레지스트리 url>/summary.json``(발행 데이터 계약 v1 envelope)을 먼저 본다
   (:mod:`services.ecosystem.siblings` — ETag·음성 캐시·stale 1일).
2. 없거나 계약 위반이면 레지스트리 ``data[]`` 의 레거시 파일(current.json/data.json …)과
   아래 기존 요약기로 폴백한다. summary 는 :mod:`services.ecosystem.adapters` 가 레거시
   모양으로 되돌리므로 응답 모양은 소스와 무관하게 같다(프론트 변경 없음).

URL 은 전부 ``config/ecosystem.json``(``core.ecosystem``)에서 오고 각 도구의 envOverride
(``HOLDING_VALUE_BASE_URL`` 등)를 따른다. 모든 형제 JSON fetch 는 ``MemoryTTLCache`` +
single-flight(:func:`services.ecosystem.fetch.cached_fetch`)를 거친다 — 큰 파일
(holding_snapshots 1.2 MB, gold_gap data.json 563 KB, etfs.json 9.3 MB)은 쓰는 부분만
남긴 슬림 모양으로 캐시한다. 각 도구 fetch 는 서로 독립적으로 실패를 허용해, 하나가 죽어도
나머지는 표시된다.
"""

from __future__ import annotations

import asyncio
import logging
import random
import sqlite3
from collections.abc import Mapping
from datetime import datetime
from typing import Any, Awaitable, Callable, Iterator

import httpx

from cache_layer import MemoryTTLCache
from core.errors import DBError
from core.http import get_http_client
from domain.portfolio_codes import is_korean_stock
from domain.timeutil import KST
from services.ecosystem import adapters, links, siblings
from services.ecosystem.fetch import FETCH_ERRORS, cached_fetch, stale_value

logger = logging.getLogger(__name__)


# SITE 키 → 레지스트리 도구 id.
_SITE_TOOLS = {
    "holding": "holding_value",
    "spread": "common_preferred_spread",
    "goldGap": "gold_gap",
    "spac": "spac-hunter",
    "nps": "nps-tracker",
    "etf": "eiayn",
    "buybacks": "buybacks",
    "bondMate": "bond-mate",
}


class _SiteMap(Mapping):
    """사용자에게 보여줄(새 탭) 도구 홈. 조회 시점에 레지스트리 + envOverride 로 만든다."""

    def __getitem__(self, key: str) -> str:
        return siblings.site_url(_SITE_TOOLS[key])

    def __iter__(self) -> Iterator[str]:
        return iter(_SITE_TOOLS)

    def __len__(self) -> int:
        return len(_SITE_TOOLS)


SITE: Mapping[str, str] = _SiteMap()

_TTL = 300  # 형제 배치 파일은 5분마다 확인(시세는 별도 2분 캐시).
_INSIGHTS_TTL = 120
_cache = MemoryTTLCache("external.tools", _INSIGHTS_TTL)
_raw_cache = MemoryTTLCache("external.raw", _TTL)  # 형제 원본(슬림) — 요약·deep-link·액션보드 공용
_SEM = asyncio.Semaphore(3)
_TIMEOUT = httpx.Timeout(8.0, connect=4.0)

# eiayn(ETF) 프로젝트의 커버 종목 universe. 우선주·지주사처럼 "외부 프로젝트가
# 발행한 목록과 코드 매칭" 패턴 — 국내(6자리/6자 KRX)·해외(VOO 등 티커) 모두 포함.
# 레거시 파일이 크고(수 MB) 느리게 바뀌므로 길게 캐시한다.
_ETF_UNIVERSE_TTL = 6 * 3600  # 6시간
_etf_universe_cache = MemoryTTLCache("external.etf_universe", _ETF_UNIVERSE_TTL)
_ETF_TIMEOUT = httpx.Timeout(20.0, connect=5.0)


def _data_url(tool_id: str, data_id: str) -> str:
    """레지스트리 ``data[]`` 레거시 파일 URL(envOverride 반영)."""
    return siblings.data_url(tool_id, data_id)


async def _sibling(
    key: str,
    tool_id: str,
    from_summary: Callable[[dict], Any],
    legacy: Callable[[], Awaitable[Any]],
    *,
    cache: MemoryTTLCache | None = None,
) -> Any:
    """summary 우선·레거시 폴백 결과를 ``cache``(기본 ``_raw_cache``)에 single-flight 로 캐시."""
    return await cached_fetch(
        cache if cache is not None else _raw_cache,
        key,
        lambda: siblings.summary_or_legacy(tool_id, from_summary, legacy),
    )


async def _legacy_etf_universe() -> set[str]:
    async with _SEM:
        client = await get_http_client("external_tools_etf")
        resp = await client.get(
            _data_url("eiayn", "etfs"),
            headers={"User-Agent": "value-invest/1.0"},
            timeout=_ETF_TIMEOUT,
        )
        resp.raise_for_status()
    data = resp.json()
    universe = {
        str(c).strip().upper()
        for c in (data.get("universe") or [])
        if str(c).strip()
    }
    if not universe:  # 빈 목록은 캐시하지 않는다(다음 호출이 다시 시도)
        raise ValueError("empty ETF universe")
    return universe


def _summary_universe(envelope: dict) -> set[str]:
    universe = adapters.eiayn_universe(envelope["data"])
    if not universe:
        raise ValueError("empty ETF universe in summary.json")
    return universe


async def fetch_etf_universe() -> set[str]:
    """eiayn 이 커버하는 ETF 코드 집합(대문자 정규화). 실패 시 스테일/빈 집합."""
    try:
        universe = await _sibling(
            "universe", "eiayn",
            _summary_universe,
            _legacy_etf_universe,
            cache=_etf_universe_cache,
        )
    except FETCH_ERRORS as exc:
        logger.warning("ETF universe fetch failed: %s", exc)
        return set()
    return universe or set()


def etf_deep_link(code: str) -> str:
    """eiayn 종목 딥링크 — 레지스트리 stockLink(예: .../eiayn/?code=VOO)."""
    return links.go_url("eiayn", code=code)


async def etf_link_for(code: str) -> dict | None:
    """code 가 ETF(eiayn universe)면 딥링크 정보를, 아니면 None.

    해외 티커에 거래소 접미사가 붙어 와도(예: VOO.US) 앞부분으로 한 번 더 맞춰본다.
    """
    norm = (code or "").strip().upper()
    if not norm:
        return None
    universe = await fetch_etf_universe()
    if not universe:
        return None
    base = norm.split(".", 1)[0]
    matched = norm if norm in universe else (base if base in universe else None)
    if not matched:
        return None
    return {"code": matched, "url": etf_deep_link(matched)}

_GOLD_LABELS = {"gold": "금", "bitcoin": "비트코인", "eth": "이더리움", "usdt": "USDT"}
# gold_gap deep-link용 기본 소스(gold만 소스 선택이 있음)
# gold_gap 자산 딥링크는 레지스트리 assetLink(?asset={asset}); 금은 뉴욕 선물 기준 비교를 덧붙인다.
_GOLD_LINK_EXTRA = {"gold": "gold_source=ny_futures"}


def _gold_asset_link(key: str) -> str:
    url = links.go_url("gold_gap", asset=key)
    extra = _GOLD_LINK_EXTRA.get(key)
    return f"{url}&{extra}" if extra else url


async def _get_json(url: str):
    async with _SEM:
        client = await get_http_client("external_tools")
        resp = await client.get(
            url,
            headers={"User-Agent": "value-invest/1.0"},
            timeout=_TIMEOUT,
        )
        resp.raise_for_status()
    return resp.json()


def _code(ticker: str) -> str:
    """'005930.KS' -> '005930'."""
    return (ticker or "").split(".", 1)[0]


def _num(value) -> float | None:
    if value in (None, ""):
        return None
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    return n if n == n else None


def _summarize_holding(current: dict, config: list, top_n: int = 5) -> dict:
    """지주사: ratio(보유가치/시총 %, 높을수록 저평가) 내림차순 TOP."""
    meta = {
        c.get("id"): {"name": c.get("name") or c.get("id"), "code": _code(c.get("holdingTicker", ""))}
        for c in (config or [])
    }
    rows = []
    for p in current.get("pairs", []) or []:
        ratio = p.get("ratio")
        if ratio is None:
            continue
        m = meta.get(p.get("id"), {})
        rows.append({
            "name": m.get("name", p.get("id")),
            "code": m.get("code", ""),
            "ratio": ratio,
            "ratioChange": p.get("ratioChange"),
        })
    rows.sort(key=lambda r: r["ratio"], reverse=True)
    summary = current.get("summary") or {}
    return {
        "averageRatio": summary.get("averageRatio"),
        "pairCount": summary.get("pairCount"),
        "top": rows[:top_n],
        "lastUpdated": current.get("lastUpdated"),
        "url": SITE["holding"],
    }


def _summarize_spread(current: dict, config: list, top_n: int = 5) -> dict:
    """우선주: spread(괴리율 %, 높을수록 우선주 할인 큼) 내림차순 TOP.

    같은 보통주에 우선주가 여럿이면(예: 두산퓨얼셀1우/2우B) 괴리율이 가장 큰
    우선주 하나만 노출한다.
    """
    meta = {
        c.get("id"): {"name": c.get("name") or c.get("commonName") or c.get("id"), "code": _code(c.get("commonTicker", ""))}
        for c in (config or [])
    }
    rows = []
    for cid, v in (current.get("prices") or {}).items():
        m = meta.get(cid)
        if not m:  # config에 정의된 보통주 쌍만(우선주 단독 항목 제외)
            continue
        spread = v.get("spread")
        if spread is None:
            continue
        rows.append({
            "name": m["name"],
            "code": m["code"],
            "spread": spread,
            "spreadChange": v.get("spreadChange"),
        })
    # 같은 보통주의 다중우선주는 괴리율(spread)이 가장 큰 하나만 남긴다.
    best_by_code: dict = {}
    for r in rows:
        cur = best_by_code.get(r["code"])
        if cur is None or r["spread"] > cur["spread"]:
            best_by_code[r["code"]] = r
    rows = sorted(best_by_code.values(), key=lambda r: r["spread"], reverse=True)
    return {
        "averageSpread": current.get("averageSpread"),
        "averageSpreadChange": current.get("averageSpreadChange"),
        "top": rows[:top_n],
        "lastUpdated": current.get("lastUpdated"),
        "url": SITE["spread"],
    }


def _summarize_gold(data: dict) -> dict:
    """김치프리미엄: 자산별 최신 gap_pct(양수=김프, 음수=역프)."""
    assets = []
    for key in ("gold", "bitcoin", "eth", "usdt"):
        a = data.get(key) or {}
        gp = a.get("gap_pct") or []
        dates = a.get("dates") or []
        if not gp:
            continue
        assets.append({
            "key": key,
            "label": _GOLD_LABELS[key],
            "gap": gp[-1],
            "date": dates[-1] if dates else None,
            "link": _gold_asset_link(key),
        })
    return {"assets": assets, "updatedAt": data.get("updated_at"), "url": SITE["goldGap"]}


def _summarize_etf_picks(data: dict, seed: str, pick_n: int = 5) -> dict:
    """오늘의 추천 ETF: AIYN 점수 TOP 100(rankings.json) 중 날짜 시드로 5개 추첨.

    같은 날짜에는 항상 같은 5개가 나오도록 KST 날짜 문자열을 시드로 쓴다
    (15분 캐시가 만료돼 다시 뽑아도 그날 안에서는 결과 불변). 노출은 rank 오름차순.
    """
    rows = []
    for e in data.get("etfs") or []:
        if not isinstance(e, dict):
            continue
        rows.append({
            "rank": e.get("rank"),
            "name": e.get("shortName") or e.get("name") or e.get("ticker"),
            "code": e.get("ticker") or e.get("id"),
            "score": e.get("aiynScore"),
            "market": e.get("market"),
            "link": e.get("link") or SITE["etf"],
        })
    picks = random.Random(seed).sample(rows, min(pick_n, len(rows))) if rows else []
    picks.sort(key=lambda r: (r["rank"] is None, r["rank"]))
    return {
        "date": seed,
        "universe": data.get("count") or len(rows),
        "top": picks,
        "generatedAt": data.get("generatedAt"),
        "url": SITE["etf"],
    }


def _summarize_spac(current: dict, top_n: int = 5) -> dict:
    """스팩: 현재가가 낮은(공모가 대비 할인 큰) 순 TOP."""
    rows = []
    for code, v in (current.get("prices") or {}).items():
        if not isinstance(v, dict):
            continue
        price = v.get("currentPrice")
        if price is None:
            continue
        rows.append({
            "name": v.get("name") or code,
            "code": code,
            "currentPrice": price,
            "ipoPrice": v.get("ipoPrice"),
            "annualizedReturn": v.get("annualizedReturn"),
            "ratio": v.get("ratio"),
        })
    rows.sort(key=lambda r: r["currentPrice"])
    summary = current.get("summary") or {}
    return {
        "averageAnnualizedReturn": summary.get("averageAnnualizedReturn"),
        "belowIpoCount": summary.get("belowIpoCount"),
        "totalCount": summary.get("totalCount"),
        "top": rows[:top_n],
        "lastUpdated": current.get("lastUpdated"),
        "url": SITE["spac"],
    }


def _summarize_nps(current: dict, top_n: int = 5) -> dict:
    """국민연금: 포트폴리오 비중(weight) 상위 보유 종목 + 요약(총평가액·NAV·종목수)."""
    rows = []
    for h in current.get("holdings", []) or []:
        if not isinstance(h, dict):
            continue
        rows.append({
            "name": h.get("stock_name") or h.get("stock_code"),
            "code": h.get("stock_code"),
            "weight": h.get("weight"),
            "marketValue": h.get("market_value"),
            "changePct": h.get("change_pct"),
        })
    # 비중(weight) 내림차순 — 값이 없는 종목은 뒤로.
    rows.sort(key=lambda r: (r["weight"] is None, -(r["weight"] or 0)))
    summary = current.get("summary") or {}
    return {
        "totalValue": summary.get("totalValue"),
        "nav": summary.get("nav"),
        "count": summary.get("count"),
        "todayPct": summary.get("todayPct"),
        "asOf": summary.get("asOf") or current.get("asOf"),
        "allocation": current.get("allocation"),
        "top": rows[:top_n],
        "lastUpdated": current.get("lastUpdated"),
        "url": SITE["nps"],
    }


def _buyback_kind_key(row: dict) -> str:
    kind = str(row.get("stock_kind") or "").strip().replace(" ", "").lower()
    if "보통" in kind or "common" in kind:
        return "common"
    if "우선" in kind or "preferred" in kind:
        return f"preferred:{kind}"
    return kind or "unknown"


def _buyback_is_common(row: dict) -> bool:
    return _buyback_kind_key(row) == "common"


def _buyback_completeness(row: dict) -> int:
    return sum(row.get(k) is not None for k in ("treasury_ratio", "ending_qty", "issued_shares", "floating_shares"))


def _buyback_is_newer(row: dict, current: dict) -> bool:
    row_date = str(row.get("as_of_date") or "")
    cur_date = str(current.get("as_of_date") or "")
    if row_date != cur_date:
        return row_date > cur_date
    row_quality = _buyback_completeness(row)
    cur_quality = _buyback_completeness(current)
    if row_quality != cur_quality:
        return row_quality > cur_quality
    return str(row.get("report_code") or "") > str(current.get("report_code") or "")


def _summarize_buybacks(holdings: list, top_n: int = 5) -> dict:
    """자사주: 최신 보통주 스냅샷 기준 treasury_ratio 내림차순 TOP."""
    latest: dict[tuple[str, str], dict] = {}
    for h in holdings or []:
        if not isinstance(h, dict):
            continue
        code = str(h.get("stock_code") or "").strip().upper()
        if not code:
            continue
        key = (code, _buyback_kind_key(h))
        current = latest.get(key)
        if current is None or _buyback_is_newer(h, current):
            latest[key] = h

    rows = []
    for h in latest.values():
        if not _buyback_is_common(h):
            continue
        ratio = _num(h.get("treasury_ratio"))
        if ratio is None:
            continue
        rows.append({
            "name": h.get("corp_name") or h.get("stock_code"),
            "code": h.get("stock_code"),
            "asOf": h.get("as_of_date"),
            "stockKind": h.get("stock_kind"),
            "treasuryRatio": ratio,
            "treasuryRatioPct": ratio * 100,
            "endingQty": h.get("ending_qty"),
            "issuedShares": h.get("issued_shares"),
        })
    rows.sort(key=lambda r: r["treasuryRatioPct"], reverse=True)
    dates = [r["asOf"] for r in rows if r.get("asOf")]
    return {
        "asOf": max(dates) if dates else None,
        "count": len(rows),
        "top": rows[:top_n],
        "url": SITE["buybacks"],
    }


_PAIR_ADAPTERS: dict[str, Callable[[dict], tuple[dict, list]]] = {
    "holding_value": adapters.holding_pair,
    "common_preferred_spread": adapters.spread_pair,
}


async def _load_pair(repo: str) -> tuple[dict, list]:
    """(current, config) 원본 모양을 받아 캐시. 요약·deep-link·액션보드가 함께 쓴다.

    ``repo`` 는 레지스트리 도구 id(holding_value / common_preferred_spread).
    """
    adapt = _PAIR_ADAPTERS[repo]

    async def legacy() -> tuple[dict, list]:
        cur, cfg = await asyncio.gather(
            _get_json(_data_url(repo, "current")),
            _get_json(_data_url(repo, "config")),
        )
        return cur, cfg

    return await _sibling(repo, repo, lambda env: adapt(env["data"]), legacy)


async def _holding_summary() -> dict | None:
    cur, cfg = await _load_pair("holding_value")
    return _summarize_holding(cur, cfg)


async def _spread_summary() -> dict | None:
    cur, cfg = await _load_pair("common_preferred_spread")
    return _summarize_spread(cur, cfg)


def _slim_gold(data: dict) -> dict:
    """gold_gap data.json(563 KB, 자산별 전체 히스토리) → 마지막 값만 남긴 같은 모양.

    ``_summarize_gold`` 와 ``integrations`` 의 최신 갭 표시는 마지막 값만 읽는다.
    """
    if not isinstance(data, dict):
        raise TypeError("gold_gap data.json must be an object")
    out: dict[str, Any] = {"updated_at": data.get("updated_at")}
    for key in _GOLD_LABELS:
        asset = data.get(key)
        if not isinstance(asset, dict):
            continue
        gaps = asset.get("gap_pct") or []
        dates = asset.get("dates") or []
        if not gaps:
            continue
        out[key] = {"gap_pct": [gaps[-1]], "dates": [dates[-1]] if dates else []}
    return out


async def _gold_latest() -> dict:
    """gold_gap 자산별 최신 갭(data.json 모양, 마지막 값만).

    data.json 은 gold_gap master 에 없다(orphan ``data`` 브랜치 → Pages 배포). 레거시
    경로는 레지스트리의 Pages URL 을 쓴다.
    """

    async def legacy() -> dict:
        return _slim_gold(await _get_json(_data_url("gold_gap", "data")))

    return await _sibling("gold_gap/latest", "gold_gap", lambda env: adapters.gold_data(env["data"]), legacy)


def peek_gold_latest() -> dict | None:
    """캐시에 있는 gold_gap 최신 갭(만료 1일까지). 네트워크를 타지 않는다 — integrations 용."""
    return stale_value(_raw_cache, "gold_gap/latest")


async def _gold_summary() -> dict | None:
    return _summarize_gold(await _gold_latest())


async def _fill_etf_changes(picks: list[dict]) -> None:
    """추천 ETF 의 일간 등락률(changePct)을 실시간 시세로 채운다.

    rankings.json 은 일배치 산출물이라 등락률이 하루 묵는다 — 목록(추첨)만
    거기서 쓰고 등락률은 시세 서비스로 별도 조회한다.
    국내(KRX)는 벌크로, 해외는 공유 단건 서비스로 조회하고, 시세를 못 구한 종목은
    changePct 없이 둔다(프론트는 '-' 표시).
    """
    codes = list(dict.fromkeys(str(p.get("code") or "").strip().upper() for p in picks if p.get("code")))
    if not codes:
        return
    try:
        from services import stock_quotes
        from services.ecosystem import live_cards
        domestic = [c for c in codes if is_korean_stock(c)]
        foreign = [c for c in codes if not is_korean_stock(c)]
        results = await asyncio.gather(
            asyncio.wait_for(live_cards.domestic_quotes(domestic), 10) if domestic else asyncio.sleep(0, result={}),
            *(asyncio.wait_for(stock_quotes.get_quote_snapshot(c), 10) for c in foreign),
            return_exceptions=True,
        )
        quotes = results[0] if isinstance(results[0], dict) else {}
        quotes.update({c: q for c, q in zip(foreign, results[1:]) if isinstance(q, dict)})
    except Exception as exc:
        logger.warning("ETF picks quote fetch failed: %s", exc)
        return
    for p in picks:
        q = quotes.get(str(p.get("code") or "").strip().upper())
        if isinstance(q, dict) and q.get("_stale") is not True and q.get("change_pct") is not None:
            p["changePct"] = q["change_pct"]
            p["asOf"] = q.get("as_of") or q.get("date")


async def _etf_rankings() -> dict:
    async def legacy() -> dict:
        return await _get_json(_data_url("eiayn", "rankings"))

    return await _sibling(
        "eiayn/rankings", "eiayn",
        lambda env: adapters.eiayn_rankings(env, etf_deep_link),
        legacy,
    )


async def _etf_picks_summary() -> dict | None:
    data = await _etf_rankings()
    out = _summarize_etf_picks(data, datetime.now(KST).strftime("%Y-%m-%d"))
    await _fill_etf_changes(out["top"])
    if any(p.get("changePct") is not None for p in out["top"]):
        out["quoteCheckedAt"] = datetime.now(KST).isoformat(timespec="seconds")
        out["partialQuotes"] = any(p.get("changePct") is None for p in out["top"])
    return out


async def _spac_current() -> dict:
    # spac-hunter 는 current.json 만으로 요약 가능하다
    # (종목명이 prices 안에 들어 있어 별도 config 가 필요 없음).
    async def legacy() -> dict:
        return await _get_json(_data_url("spac-hunter", "current"))

    return await _sibling(
        "spac-hunter/current", "spac-hunter",
        lambda env: adapters.spac_current(env["data"]), legacy,
    )


async def _spac_summary() -> dict | None:
    return _summarize_spac(await _spac_current())


def _valid_spac_data(data: Any) -> dict:
    if not isinstance(data, dict) or not isinstance(data.get("spacs"), list):
        raise ValueError("Invalid SPAC Hunter data")
    return data


async def fetch_spac_data() -> dict:
    """Full SPAC Hunter valuation inputs; current.json only contains summary prices.

    summary.json(§6.3)이 있으면 그 ``spacs``/``valuationAssumptions`` 를, 없으면 3.8 MB
    data.json 을 쓴다. 둘 다 실패하면 1일 이내 stale, 그것도 없으면 ``{}``.
    """

    async def legacy() -> dict:
        return _valid_spac_data(await _get_json(_data_url("spac-hunter", "data")))

    try:
        return await _sibling(
            "spac-hunter/data", "spac-hunter",
            lambda env: _valid_spac_data(adapters.spac_valuation(env["data"])),
            legacy,
        )
    except FETCH_ERRORS as exc:
        logger.warning("SPAC Hunter valuation fetch failed: %s", exc)
        return {}


async def _nps_card() -> dict:
    # nps-tracker current.json(313 KB)은 카드로 줄여서 캐시한다.
    async def legacy() -> dict:
        return _summarize_nps(await _get_json(_data_url("nps-tracker", "current")))

    return await _sibling(
        "nps-tracker/card", "nps-tracker",
        lambda env: _summarize_nps(adapters.nps_current(env["data"])), legacy,
    )


async def _nps_summary() -> dict | None:
    return await _nps_card()


def _buybacks_index(holdings: list) -> dict:
    """holding_snapshots.json(1.2 MB) → {"card": 인사이트 카드, "byCode": 종목별 매칭}.

    card 는 ``_summarize_buybacks``, byCode 는 ``_match_buyback`` 과 같은 결과다(한 번 훑어서
    만든다). summary.json 경로(``adapters.buybacks_index``)와 같은 모양이라 캐시 하나로 쓴다.
    """
    if not isinstance(holdings, list):
        raise TypeError("buybacks holding_snapshots.json must be a list")
    return {
        "card": _summarize_buybacks(holdings),
        "byCode": {code: _buyback_match_row(code, row) for code, row in _latest_common_buybacks(holdings).items()},
    }


async def _buybacks_data() -> dict:
    async def legacy() -> dict:
        return _buybacks_index(await _get_json(_data_url("buybacks", "holding-snapshots")))

    return await _sibling(
        "buybacks/index", "buybacks",
        lambda env: adapters.buybacks_index(env["data"], SITE["buybacks"]),
        legacy,
    )


async def _buybacks_summary() -> dict | None:
    return (await _buybacks_data())["card"]


def _summarize_bond_mate(current: dict) -> dict:
    """채권 시황: 장단기 스프레드·주요 금리·신용 스프레드·최근 회사채 발행.

    bond-mate 가 이미 계산해 둔 highlights 를 그대로 쓰고, AI 인사이트가
    문장으로 풀기 좋은 최소한만 골라 담는다."""
    highlights = current.get("highlights") or {}
    rates = current.get("rates") or {}
    fx = current.get("fx") or {}

    def quote(container: dict, key: str) -> dict | None:
        row = container.get(key)
        if not isinstance(row, dict) or row.get("value") is None:
            return None
        return {"value": row.get("value"), "change": row.get("change"), "asOf": row.get("date")}

    credit = current.get("credit") or {}
    spreads = {
        rating: round(meta["oas"]["value"] * 100, 1)
        for rating, meta in credit.items()
        if isinstance(meta, dict)
        and isinstance(meta.get("oas"), dict)
        and meta["oas"].get("value") is not None
    }

    return {
        "asOf": current.get("generated_at"),
        "us10y": quote(rates, "US10Y"),
        "us2y": quote(rates, "US2Y"),
        "kr10y": quote(rates, "KR10Y"),
        "kr3y": quote(rates, "KR3Y"),
        "usBase": quote(rates, "US_BASE"),
        "krBase": quote(rates, "KR_BASE"),
        "usdKrw": quote(fx, "USD_KRW"),
        "usCurveSpreadBp": highlights.get("us_curve_spread_bp"),
        "usCurveInverted": highlights.get("us_curve_inverted"),
        "krCurveSpreadBp": highlights.get("kr_curve_spread_bp"),
        "creditSpreadBp": spreads,
        "latestOffering": highlights.get("latest_offering"),
        "url": SITE["bondMate"],
    }


async def _bond_mate_summary() -> dict | None:
    async def legacy() -> dict:
        return _summarize_bond_mate(await _get_json(_data_url("bond-mate", "current")))

    return await _sibling(
        "bond-mate/card", "bond-mate",
        lambda env: _summarize_bond_mate(adapters.bond_current(env)), legacy,
    )


async def fetch_external_insights() -> dict:
    """120초 캐시 + single-flight. 일일 ETF 추첨은 KST 날짜별로 분리한다."""
    key = "insights:" + datetime.now(KST).strftime("%Y-%m-%d")
    try:
        return await cached_fetch(_cache, key, _load_external_insights)
    except FETCH_ERRORS as exc:
        logger.warning("external insights unavailable: %s", exc)
        return {}


async def _load_external_insights() -> dict:
    from services.ecosystem import live_cards

    results = await asyncio.gather(
        _holding_summary(), _spread_summary(), _gold_summary(), _spac_summary(), _nps_summary(),
        _etf_picks_summary(), _buybacks_summary(), _bond_mate_summary(),
        return_exceptions=True,
    )
    keys = ("holding", "spread", "goldGap", "spac", "nps", "etfPicks", "buybacks", "bondMate")
    out: dict = {}
    for key, res in zip(keys, results):
        if isinstance(res, Exception) or res is None:
            logger.warning("external insight '%s' failed: %s", key, res)
            continue
        out[key] = res

    if out:
        await live_cards.refresh(out)
        return out
    # cached_fetch가 24시간 이내의 마지막 성공 응답으로 제한해 폴백한다.
    raise ValueError("all external insight sources failed")


def _match_preferred(code: str, current: dict, config: list) -> dict | None:
    """보통주/우선주 코드 어느 쪽이든 매칭되면 그 쌍의 괴리율 정보를 돌려준다."""
    prices = current.get("prices") or {}
    for c in config or []:
        if code in (_code(c.get("commonTicker", "")), _code(c.get("preferredTicker", ""))):
            v = prices.get(c.get("id")) or {}
            if v.get("spread") is None:
                return None
            return {
                "name": c.get("name") or c.get("commonName") or c.get("id"),
                "preferredName": c.get("preferredName"),
                "spread": v.get("spread"),
                "spreadChange": v.get("spreadChange"),
                "commonPrice": v.get("commonPrice"),
                "preferredPrice": v.get("preferredPrice"),
                "url": SITE["spread"],
            }
    return None


def _match_holding(code: str, current: dict, config: list) -> dict | None:
    """종목이 지주사면 보유지분가치/시총 비율 정보를 돌려준다(?code= deep-link)."""
    pairs = {p.get("id"): p for p in current.get("pairs", []) or []}
    for c in config or []:
        if _code(c.get("holdingTicker", "")) == code:
            p = pairs.get(c.get("id")) or {}
            if p.get("ratio") is None:
                return None
            return {
                "name": c.get("name") or c.get("id"),
                "ratio": p.get("ratio"),
                "ratioChange": p.get("ratioChange"),
                "holdingValue": p.get("holdingValue"),
                "marketCap": p.get("marketCap"),
                "url": links.go_url("holding_value", code=code),
            }
    return None


def _latest_common_buybacks(holdings: list) -> dict[str, dict]:
    """종목코드별 최신 보통주 스냅샷(treasury_ratio 가 있는 행 중에서) — 한 번 훑는다."""
    latest: dict[str, dict] = {}
    for row in holdings or []:
        if not isinstance(row, dict):
            continue
        code = str(row.get("stock_code") or "").strip().upper()
        if not code or not _buyback_is_common(row):
            continue
        if _num(row.get("treasury_ratio")) is None:
            continue
        current = latest.get(code)
        if current is None or _buyback_is_newer(row, current):
            latest[code] = row
    return latest


def _buyback_match_row(code: str, row: dict) -> dict:
    ratio = _num(row.get("treasury_ratio"))
    return {
        "name": row.get("corp_name") or code,
        "asOf": row.get("as_of_date"),
        "stockKind": row.get("stock_kind"),
        "treasuryRatio": ratio,
        "treasuryRatioPct": ratio * 100,
        "endingQty": row.get("ending_qty"),
        "issuedShares": row.get("issued_shares"),
        "url": SITE["buybacks"],
    }


def _match_buyback(code: str, holdings: list) -> dict | None:
    """Return the latest common-stock buyback snapshot for one code."""
    code = str(code or "").strip().upper()
    row = _latest_common_buybacks(
        [r for r in holdings or [] if isinstance(r, dict) and str(r.get("stock_code") or "").strip().upper() == code]
    ).get(code)
    return _buyback_match_row(code, row) if row is not None else None


async def _buyback_stock_name(code: str) -> str | None:
    """summary.json 의 ``ratios`` 에는 종목명이 없다 — 허브 DART 코드표에서 찾는다."""
    from repositories import corp_codes

    try:
        return await corp_codes.get_corp_name(code)
    except (sqlite3.Error, OSError, DBError) as exc:
        logger.info("buyback stock name lookup failed (%s): %s", code, exc)
        return None


def _match_gold_gap_asset(code: str, summary: dict) -> dict | None:
    asset_key_by_code = {
        "KRX_GOLD": "gold",
        "CRYPTO_BTC": "bitcoin",
        "CRYPTO_ETH": "eth",
        "CRYPTO_USDT": "usdt",
    }
    asset_key = asset_key_by_code.get(str(code or "").strip().upper())
    if not asset_key:
        return None
    for asset in summary.get("assets") or []:
        if asset.get("key") == asset_key:
            return {
                "name": asset.get("label") or asset_key,
                "gap": asset.get("gap"),
                "date": asset.get("date"),
                "url": asset.get("link") or summary.get("url") or SITE["goldGap"],
            }
    return None


def _signal(kind: str, title: str, detail: str, url: str, *, severity: str = "info", metric=None, short_label: str = "") -> dict:
    return {
        "kind": kind,
        "title": title,
        "detail": detail,
        "url": url,
        "severity": severity,
        "metric": metric,
        "short_label": short_label or kind,
    }


async def fetch_portfolio_signals(codes: list[str]) -> dict[str, list[dict]]:
    """Linked-dashboard signals keyed by portfolio stock code.

    This is intentionally best-effort. Each external dashboard is independent;
    one stale/failed source should not block the action board.
    """
    normalized = []
    for code in codes or []:
        norm = str(code or "").strip().upper()
        if norm and norm not in normalized:
            normalized.append(norm)
    if not normalized:
        return {}

    out: dict[str, list[dict]] = {code: [] for code in normalized}

    try:
        cur, cfg = await _load_pair("common_preferred_spread")
        for code in normalized:
            pref = _match_preferred(code, cur, cfg)
            if not pref:
                continue
            spread = _num(pref.get("spread"))
            severity = "high" if spread is not None and spread >= 50 else "watch"
            detail = f"우선주 괴리율 {spread:.1f}%" if spread is not None else "우선주 괴리율 신호"
            out[code].append(_signal(
                "preferred",
                f"{pref.get('name') or code} 우선주 괴리",
                detail,
                links.go_url("common_preferred_spread", code=code),
                severity=severity,
                metric=spread,
                short_label="우선주",
            ))
    except Exception as exc:
        logger.warning("portfolio signal preferred lookup failed: %s", exc)

    try:
        cur, cfg = await _load_pair("holding_value")
        for code in normalized:
            hold = _match_holding(code, cur, cfg)
            if not hold:
                continue
            ratio = _num(hold.get("ratio"))
            severity = "high" if ratio is not None and ratio >= 150 else "watch"
            detail = f"보유지분가치/시총 {ratio:.1f}%" if ratio is not None else "지주사 NAV 신호"
            out[code].append(_signal(
                "holding",
                f"{hold.get('name') or code} 지주사 NAV",
                detail,
                hold.get("url") or links.go_url("holding_value", code=code),
                severity=severity,
                metric=ratio,
                short_label="지주사",
            ))
    except Exception as exc:
        logger.warning("portfolio signal holding lookup failed: %s", exc)

    try:
        universe = await fetch_etf_universe()
        if universe:
            for code in normalized:
                base = code.split(".", 1)[0]
                matched = code if code in universe else (base if base in universe else None)
                if not matched:
                    continue
                out[code].append(_signal(
                    "etf",
                    f"{matched} ETF 상세",
                    "ETF 평가 대시보드에서 최신 순위와 구성 정보를 확인할 수 있습니다.",
                    etf_deep_link(matched),
                    severity="info",
                    short_label="ETF",
                ))
    except Exception as exc:
        logger.warning("portfolio signal etf lookup failed: %s", exc)

    try:
        by_code = (await _buybacks_data())["byCode"]
        for code in normalized:
            buyback = by_code.get(code)
            if not buyback:
                continue
            name = buyback.get("name") or await _buyback_stock_name(code)
            ratio_pct = _num(buyback.get("treasuryRatioPct"))
            severity = "high" if ratio_pct is not None and ratio_pct >= 20 else "watch"
            detail = f"자사주 보유비중 {ratio_pct:.1f}%" if ratio_pct is not None else "자사주 데이터 신호"
            if buyback.get("asOf"):
                detail += f" · {buyback['asOf']} 기준"
            out[code].append(_signal(
                "buybacks",
                f"{name or code} 자사주",
                detail,
                links.go_url("buybacks", code=code),  # 레지스트리 stockLink: ?stock={code}
                severity=severity,
                metric=ratio_pct,
                short_label="자사주",
            ))
    except Exception as exc:
        logger.warning("portfolio signal buybacks lookup failed: %s", exc)

    try:
        gold_summary = await _gold_summary()
        if gold_summary:
            for code in normalized:
                asset = _match_gold_gap_asset(code, gold_summary)
                if not asset:
                    continue
                gap = _num(asset.get("gap"))
                severity = "watch" if gap is not None and abs(gap) >= 2 else "info"
                detail = f"국내외 갭 {gap:+.2f}%" if gap is not None else "국내외 가격 갭 신호"
                if asset.get("date"):
                    detail += f" · {asset['date']}"
                out[code].append(_signal(
                    "goldGap",
                    f"{asset.get('name') or code} 갭",
                    detail,
                    asset.get("url") or SITE["goldGap"],
                    severity=severity,
                    metric=gap,
                    short_label="갭",
                ))
    except Exception as exc:
        logger.warning("portfolio signal gold-gap lookup failed: %s", exc)

    return {code: signals for code, signals in out.items() if signals}


async def fetch_stock_links(code: str) -> dict:
    """종목분석 deep-link 카드용 — 이 종목코드에 해당하는 우선주/지주사 정보.

    config 전체를 매핑하므로 TOP 요약과 달리 임의 종목을 커버한다. 두 도구는
    서로 독립적으로 조회·실패 허용.
    """
    code = (code or "").strip()
    if not code:
        return {}
    result: dict = {}
    try:
        cur, cfg = await _load_pair("common_preferred_spread")
        pref = _match_preferred(code, cur, cfg)
        if pref:
            result["preferred"] = pref
    except Exception as exc:
        logger.warning("stock-link preferred lookup failed (%s): %s", code, exc)
    try:
        cur, cfg = await _load_pair("holding_value")
        hold = _match_holding(code, cur, cfg)
        if hold:
            result["holding"] = hold
    except Exception as exc:
        logger.warning("stock-link holding lookup failed (%s): %s", code, exc)
    try:
        etf = await etf_link_for(code)
        if etf:
            result["etf"] = etf
    except Exception as exc:
        logger.warning("stock-link etf lookup failed (%s): %s", code, exc)
    return result
