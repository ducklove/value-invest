"""종목 타임라인 — 종목분석 화면에 입력 없이 뉴스와 DART 공시를 한 줄로 보여 준다.

설계(2026-11 UI 회의 결론):
- 뉴스는 일일 시황과 같은 네이버 모바일 종목 뉴스 API 를 쓰되, 코드별로 고정 최대
  건수(``NEWS_FETCH_MAX``)를 캐시하고 화면 한도는 나중에 자른다. 동시 cold 요청은
  single-flight, 동시 upstream 은 세마포어로 묶는다.
- 공시는 ``essential=False`` 로 조회한다 — 화면을 둘러보는 것이 신규 공시 푸시가
  쓰는 OpenDART 일일 한도를 잠식하지 않게(예산 소진 시 ``quota`` 상태로 응답).
- 소스별 상태(ok/empty/error/quota/unsupported)를 따로 돌려 "조회 실패"를 "새 소식
  없음"으로 보이지 않게 한다. 한 소스가 실패해도 다른 소스 결과는 그대로 쓴다.
- 행 계약: ``{type, id, date, time, title, source, url}`` — 2단계(알림 이력)가 같은
  형식으로 합류한다.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timedelta
from typing import Any

import httpx

from core.cache import MemoryTTLCache, cached_fetch_result
from core.http import get_http_client
from repositories import corp_codes
from services.dart import client as dart_client
from services.market.daily import NAVER_MOBILE_NEWS_API, _flatten_news_payload, _map_news_item

logger = logging.getLogger(__name__)

NEWS_FETCH_MAX = 20
NEWS_TTL_S = 300.0
DISCLOSURE_TTL_S = 600.0
DISCLOSURE_DAYS = 90
_STOCK_CODE_RE = re.compile(r"^[0-9][0-9A-Z]{5}$")

_news_cache = MemoryTTLCache("stock_timeline.news", NEWS_TTL_S)
_disc_cache = MemoryTTLCache("stock_timeline.disc", DISCLOSURE_TTL_S)
_SEM = asyncio.Semaphore(3)
_NEWS_ERRORS = (httpx.HTTPError, TimeoutError, ValueError, TypeError, KeyError, AttributeError)


def is_supported_code(code: str) -> bool:
    return bool(_STOCK_CODE_RE.match(str(code or "").upper()))


def _split_published(published_at: str) -> tuple[str, str]:
    """``2026.11.28 09:31`` → (``2026-11-28``, ``09:31``)."""
    m = re.match(r"(\d{4})\.(\d{2})\.(\d{2})(?:\s+(\d{2}:\d{2}))?", published_at or "")
    if not m:
        return "", ""
    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}", m.group(4) or ""


def news_rows(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """일일 시황 뉴스 형식 → 타임라인 행. 같은 기사 URL·제목은 한 번만."""
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        key = item.get("url") or item.get("title") or ""
        if not key or key in seen:
            continue
        seen.add(key)
        day, hhmm = _split_published(str(item.get("published_at") or ""))
        rows.append({
            "type": "news",
            "id": key,
            "date": day,
            "time": hhmm,
            "title": item.get("title") or "",
            "source": item.get("outlet") or "",
            "url": item.get("url") or "",
            "snippet": item.get("snippet") or "",
        })
    return rows


def disclosure_rows(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """DART list.json 항목 → 타임라인 행. 공시는 시각이 없어 날짜만 쓴다."""
    rows: list[dict[str, Any]] = []
    for item in items:
        rcept = str(item.get("rcept_no") or "").strip()
        title = str(item.get("report_nm") or "").strip()
        if not rcept or not title:
            continue
        raw_dt = str(item.get("rcept_dt") or "")
        day = f"{raw_dt[0:4]}-{raw_dt[4:6]}-{raw_dt[6:8]}" if len(raw_dt) == 8 and raw_dt.isdigit() else ""
        rows.append({
            "type": "disclosure",
            "id": rcept,
            "date": day,
            "time": "",
            "title": title,
            "source": str(item.get("flr_nm") or "DART").strip() or "DART",
            "url": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept}",
        })
    return rows


async def _load_news(code: str) -> list[dict[str, Any]]:
    async with _SEM:
        client = await get_http_client("naver")
        resp = await client.get(
            NAVER_MOBILE_NEWS_API.format(code=code, size=NEWS_FETCH_MAX),
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://m.stock.naver.com/"},
            timeout=8.0,
        )
        resp.raise_for_status()
        payload = resp.json()
    mapped = (_map_news_item(item, code) for item in _flatten_news_payload(payload))
    return news_rows([m for m in mapped if m])[:NEWS_FETCH_MAX]


async def _news_source(code: str) -> dict[str, Any]:
    try:
        result = await cached_fetch_result(
            _news_cache, code, lambda: _load_news(code), errors=_NEWS_ERRORS, stale_ttl=6 * 3600,
        )
    except _NEWS_ERRORS as exc:
        logger.info("stock timeline news failed for %s: %s", code, exc)
        return {"status": "error", "items": [], "message": "뉴스를 불러오지 못했습니다."}
    rows = result.value or []
    out: dict[str, Any] = {"status": "ok" if rows else "empty", "items": rows}
    entry = _news_cache.get_entry(code, allow_stale=True, copy=False)
    if entry is not None:
        out["fetched_at"] = entry.cached_at
    if result.stale:
        # 이번 조회는 실패했고 마지막 정상 목록을 보여 준다 — 화면이 "지난 목록"이라고 알린다.
        out["stale"] = True
    return out


async def _load_disclosures(corp_code: str) -> list[dict[str, Any]]:
    end = datetime.now()
    start = end - timedelta(days=DISCLOSURE_DAYS)
    items = await dart_client.fetch_filing_list(
        corp_code,
        start.strftime("%Y%m%d"),
        end.strftime("%Y%m%d"),
        sort="date",
        sort_mth="desc",
        page_no=1,
        page_count=15,
        timeout=15,
        essential=False,
        cache_ttl=DISCLOSURE_TTL_S,
    )
    return disclosure_rows(items)


async def _disclosure_source(code: str) -> dict[str, Any]:
    if not dart_client.api_key():
        return {"status": "unsupported", "items": [], "message": "공시 조회가 설정되지 않았습니다."}
    corp_code = await corp_codes.get_corp_code(code)
    if not corp_code:
        return {"status": "unsupported", "items": [], "message": "DART 고유번호가 없는 종목입니다."}
    hit = _disc_cache.get_entry(corp_code)
    if hit is not None:
        rows = hit.value
        return {"status": "ok" if rows else "empty", "items": rows, "fetched_at": hit.cached_at}
    try:
        rows = await _load_disclosures(corp_code)
    except dart_client.DartQuotaError:
        return {"status": "quota", "items": [], "message": "공시 조회 한도를 알림용으로 아끼는 중입니다."}
    except (dart_client.DartListError, httpx.HTTPError, TimeoutError) as exc:
        logger.info("stock timeline disclosures failed for %s: %s", code, exc)
        return {"status": "error", "items": [], "message": "공시를 불러오지 못했습니다."}
    _disc_cache.set(corp_code, rows)
    entry = _disc_cache.get_entry(corp_code, copy=False)
    return {"status": "ok" if rows else "empty", "items": rows, "fetched_at": entry.cached_at if entry else None}


def merge_rows(*groups: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    """날짜·시각 내림차순으로 합친다. 시각 없는 공시는 같은 날 뉴스 뒤에 온다."""
    rows = [row for group in groups for row in group]
    rows.sort(key=lambda r: (r.get("date") or "", r.get("time") or ""), reverse=True)
    return rows[:limit]


async def get_stock_timeline(code: str, limit: int = 12) -> dict[str, Any]:
    code = str(code or "").upper()
    if not is_supported_code(code):
        return {"stock_code": code, "items": [], "sources": {
            "news": {"status": "unsupported", "items": []},
            "disclosure": {"status": "unsupported", "items": []},
        }}
    news, disc = await asyncio.gather(_news_source(code), _disclosure_source(code))
    # 한도는 소스별로 적용한다 — 합친 뒤 자르면 당일 뉴스가 많을 때 공시가 통째로 밀려난다.
    limit = max(1, min(int(limit or 12), 40))
    items = merge_rows(news["items"][:limit], disc["items"][:limit], limit=limit * 2)
    sources = {
        name: {k: v for k, v in src.items() if k != "items"} | {"count": len(src["items"])}
        for name, src in (("news", news), ("disclosure", disc))
    }
    return {"stock_code": code, "items": items, "sources": sources}
