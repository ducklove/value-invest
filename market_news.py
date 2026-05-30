"""Naver-finance 주요 뉴스 scraper for the 투자정보 dashboard.

Pulls the main finance news block (``/news/mainnews.naver``) — headline,
source, timestamp, and a short summary — to fill the dashboard's main column
with timely, Naver-style content. Public; no auth required. EUC-KR page,
TTL-cached behind a semaphore so a slow upstream can't pin workers.
"""

from __future__ import annotations

import asyncio
import logging

import httpx
from bs4 import BeautifulSoup

from cache_layer import MemoryTTLCache

logger = logging.getLogger(__name__)

_BASE = "https://finance.naver.com"
_NEWS_URL = f"{_BASE}/news/mainnews.naver"
_NEWS_TTL = 180  # seconds — 주요 뉴스는 분 단위로만 바뀐다
_news_cache = MemoryTTLCache("market.news", _NEWS_TTL)
_SEM = asyncio.Semaphore(3)
_HTTP_TIMEOUT = httpx.Timeout(6.0, connect=3.0)


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


async def fetch_market_news(limit: int = 8) -> list[dict]:
    """Fetch 주요 뉴스 (cached). Falls back to stale cache on upstream failure."""
    key = "mainnews"
    cached = _news_cache.get(key)
    if cached is not None:
        return cached[:limit]
    try:
        async with _SEM:
            async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT, follow_redirects=True) as client:
                resp = await client.get(_NEWS_URL, headers={"User-Agent": "Mozilla/5.0"})
                resp.raise_for_status()
        items = _parse_news(resp.content.decode("euc-kr", errors="replace"))
        if items:
            _news_cache.set(key, items)
        return items[:limit]
    except Exception as exc:
        logger.warning("market news fetch failed: %s", exc)
        entry = _news_cache.get_entry(key, allow_stale=True) if hasattr(_news_cache, "get_entry") else None
        if entry and getattr(entry, "value", None):
            return list(entry.value)[:limit]
        return []
