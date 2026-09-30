"""Naver-finance 주요 뉴스 scraper for the 투자정보 dashboard.

Pulls the main finance news block (``/news/mainnews.naver``) — headline,
source, timestamp, and a short summary — to fill the dashboard's main column
with timely, Naver-style content. Public; no auth required. EUC-KR page,
TTL-cached behind a semaphore so a slow upstream can't pin workers.

이관 노트(ST-03): 루트 ``market_news.py`` 에서 이 패키지로 옮겨옴. 공유 httpx
클라이언트(core.http)를 사용한다.
"""

from __future__ import annotations

import asyncio
import logging

from bs4 import BeautifulSoup

from cache_layer import FETCH_ERRORS, MemoryTTLCache, cached_fetch_result
from core.http import get_http_client

logger = logging.getLogger(__name__)

_BASE = "https://finance.naver.com"
_NEWS_URL = f"{_BASE}/news/mainnews.naver"
_NEWS_TTL = 180  # seconds — 주요 뉴스는 분 단위로만 바뀐다
_news_cache = MemoryTTLCache("market.news", _NEWS_TTL)
_SEM = asyncio.Semaphore(3)
# Naver 뉴스는 가벼운 GET — 공유 "naver" 클라이언트(기본 8s)에서 per-request
# 로 더 짧은 timeout 을 건다. 커넥션 풀은 앱 매니저가 재사용한다.
_HTTP_TIMEOUT = 6.0
# 뉴스 섹션은 절대 예외를 올리지 않는다 — 네트워크·HTTP·파싱 오류를 모두 흡수.
_NEWS_ERRORS = (*FETCH_ERRORS, AttributeError)


def _parse_news(html: str) -> list[dict]:
    """Parse ``ul.newsList li`` blocks into {title, url, source, date, summary}.

    The summary cell (``dd.articleSummary``) also carries .press/.bar/.wdate
    child spans; we lift those out separately and keep only the lead text.
    Pure (no network) so it can be unit-tested against saved fixture HTML.
    """
    soup = BeautifulSoup(html or "", "html.parser")
    items: list[dict] = []
    for li in soup.select("ul.newsList li"):
        a = li.select_one(".articleSubject a") or li.select_one("a")
        if not a:
            continue
        title = a.get_text(strip=True)
        if not title:
            continue
        href = a.get("href", "") or ""
        url = href if href.startswith("http") else (_BASE + href)

        press = li.select_one(".press")
        wdate = li.select_one(".wdate")
        source = press.get_text(strip=True) if press else ""
        date = wdate.get_text(strip=True) if wdate else ""

        summ_el = li.select_one(".articleSummary")
        summary = ""
        if summ_el:
            for span in summ_el.select(".press, .bar, .wdate"):
                span.extract()
            summary = summ_el.get_text(" ", strip=True)

        items.append({
            "title": title,
            "url": url,
            "source": source,
            "date": date,
            "summary": summary,
        })
    return items


async def _load_market_news() -> list[dict]:
    async with _SEM:
        client = await get_http_client("naver")
        resp = await client.get(
            _NEWS_URL,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=_HTTP_TIMEOUT,
        )
        resp.raise_for_status()
    return _parse_news(resp.content.decode("euc-kr", errors="replace"))


async def fetch_market_news(limit: int = 8) -> list[dict]:
    """Fetch 주요 뉴스 (cached). Falls back to stale cache on upstream failure.

    ``cached_fetch``: 동시 cold 요청은 upstream 한 번(single-flight), 실패하거나
    빈 목록이면 마지막 정상 목록(나이 무관)을 돌려준다. 빈 목록은 캐시하지 않는다.
    """
    try:
        result = await cached_fetch_result(
            _news_cache,
            "mainnews",
            _load_market_news,
            stale_ttl=float("inf"),
            is_valid=bool,
            errors=_NEWS_ERRORS,
        )
    except _NEWS_ERRORS as exc:
        logger.warning("market news fetch failed: %s", exc)
        return []
    if result.error is not None:
        logger.warning("market news fetch failed: %s", result.error)
    return list(result.value)[:limit]
