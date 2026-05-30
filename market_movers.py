"""Naver-finance market ranking scrapers (시총상위·거래상위·급상승·급하락).

Fills the 투자정보 dashboard with dense, Naver-style ranking content. Each kind
is a sise ranking page sharing the same ``table.type_2`` layout (N · 종목명 ·
현재가 · 전일비 · 등락률 · …); we extract the common leading columns plus an
optional metric column (시가총액 / 거래량). Pages are EUC-KR.

Results are TTL-cached so a dashboard refresh doesn't re-hit Naver every time,
and bounded by a semaphore + deadline so a slow upstream can't pin workers.
"""

from __future__ import annotations

import asyncio
import logging
import re

import httpx
from bs4 import BeautifulSoup

from cache_layer import MemoryTTLCache

logger = logging.getLogger(__name__)

_BASE = "https://finance.naver.com"
# kind -> (path, metric header to surface or None)
_KINDS: dict[str, tuple[str, str | None]] = {
    "market_cap": ("/sise/sise_market_sum.naver", "시가총액"),
    "volume": ("/sise/sise_quant.naver", "거래량"),
    "rising": ("/sise/sise_rise.naver", None),
    "falling": ("/sise/sise_fall.naver", None),
}

_MOVERS_TTL = 90  # seconds — rankings move intraday but not every second
_movers_cache = MemoryTTLCache("market.movers", _MOVERS_TTL)
_SECTOR_TTL = 120  # 업종 등락은 더 천천히 움직인다
_sector_cache = MemoryTTLCache("market.sectors", _SECTOR_TTL)
_SEM = asyncio.Semaphore(3)
_HTTP_TIMEOUT = httpx.Timeout(6.0, connect=3.0)


async def _get_html(url: str) -> str:
    """Fetch a Naver finance page and decode EUC-KR. Bounded by _SEM/timeout."""
    async with _SEM:
        async with httpx.AsyncClient(timeout=_HTTP_TIMEOUT, follow_redirects=True) as client:
            resp = await client.get(url, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
    return resp.content.decode("euc-kr", errors="replace")


def _direction_from_blind(text: str) -> str:
    if text in ("상승", "상한가"):
        return "up"
    if text in ("하락", "하한가"):
        return "down"
    return "flat"


def _parse_ranking_table(html: str, metric_header: str | None = None) -> list[dict]:
    """Parse a Naver sise ``table.type_2`` ranking table into row dicts.

    Each row: {rank, code, name, price, change_pct, direction[, metric]}.
    Pure (no network) so it can be unit-tested against saved fixture HTML.
    """
    soup = BeautifulSoup(html or "", "html.parser")
    table = soup.select_one("table.type_2")
    if not table:
        return []
    headers = [th.get_text(strip=True) for th in table.select("thead th")]
    metric_idx = headers.index(metric_header) if (metric_header and metric_header in headers) else None

    items: list[dict] = []
    # Naver's market-cap page wraps rows in <tbody>, but the rise/fall/volume
    # pages emit bare <tr> (and html.parser, unlike browsers, does NOT inject a
    # <tbody>). Iterate every <tr>; non-data rows lack an a.tltle and are skipped.
    for tr in table.find_all("tr"):
        link = tr.select_one("a.tltle")
        if not link:
            continue
        href = link.get("href", "") or ""
        m = re.search(r"code=(\w+)", href)
        code = m.group(1) if m else ""
        name = link.get_text(strip=True)
        tds = tr.find_all("td")
        if len(tds) < 5:
            continue

        def cell(i: int) -> str:
            return tds[i].get_text(" ", strip=True) if i < len(tds) else ""

        price = cell(2).replace(" ", "")
        change_pct = cell(4).replace(" ", "")
        if "%" not in change_pct:
            for td in tds:
                txt = td.get_text(strip=True)
                if "%" in txt:
                    change_pct = txt
                    break
        blind = tr.select_one("td .blind")
        direction = _direction_from_blind(blind.get_text(strip=True) if blind else "")
        if direction == "flat" and change_pct:
            if change_pct.startswith("+"):
                direction = "up"
            elif change_pct.startswith("-"):
                direction = "down"

        item = {
            "rank": cell(0),
            "code": code,
            "name": name,
            "price": price,
            "change_pct": change_pct,
            "direction": direction,
        }
        if metric_idx is not None and metric_idx < len(tds):
            item["metric"] = tds[metric_idx].get_text(strip=True)
        items.append(item)
    return items


async def fetch_market_movers(kind: str, market: str = "kospi", limit: int = 10) -> list[dict]:
    """Fetch a ranking list (cached). market: 'kospi' (sosok=0) | 'kosdaq' (sosok=1)."""
    if kind not in _KINDS:
        kind = "market_cap"
    market = "kosdaq" if market == "kosdaq" else "kospi"
    key = f"{kind}:{market}"
    cached = _movers_cache.get(key)
    if cached is not None:
        return cached[:limit]

    path, metric = _KINDS[kind]
    sosok = "1" if market == "kosdaq" else "0"
    url = f"{_BASE}{path}?sosok={sosok}"
    try:
        html = await _get_html(url)
        items = _parse_ranking_table(html, metric)
        if items:
            _movers_cache.set(key, items)
        return items[:limit]
    except Exception as exc:
        logger.warning("market movers fetch failed (%s/%s): %s", kind, market, exc)
        return _stale(_movers_cache, key, limit)


def _stale(cache: MemoryTTLCache, key: str, limit: int) -> list[dict]:
    """Best-effort stale fallback when a live fetch fails."""
    entry = cache.get_entry(key, allow_stale=True) if hasattr(cache, "get_entry") else None
    if entry and getattr(entry, "value", None):
        return list(entry.value)[:limit]
    return []


def _parse_sector_table(html: str) -> list[dict]:
    """Parse 업종별 시세 (sise_group?type=upjong) into {name, change_pct, direction}.

    The page is a ``table.type_1`` of 업종명 · 전일대비(등락률) · 상승/보합/하락
    counts. Direction comes from the 등락률 sign (no .blind marker here). Pure.
    """
    soup = BeautifulSoup(html or "", "html.parser")
    table = soup.select_one("table.type_1")
    if not table:
        return []
    items: list[dict] = []
    for tr in table.find_all("tr"):
        a = tr.select_one("a[href*='sise_group_detail']")
        if not a:
            continue
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue
        change_pct = tds[1].get_text(" ", strip=True).replace(" ", "")
        direction = (
            "up" if change_pct.startswith("+")
            else "down" if change_pct.startswith("-")
            else "flat"
        )
        items.append({
            "name": a.get_text(strip=True),
            "change_pct": change_pct,
            "direction": direction,
        })
    return items


async def fetch_sectors(limit: int = 12) -> list[dict]:
    """Fetch 업종별 등락 (cached). Naver pre-sorts by 등락률 descending."""
    key = "upjong"
    cached = _sector_cache.get(key)
    if cached is not None:
        return cached[:limit]
    url = f"{_BASE}/sise/sise_group.naver?type=upjong"
    try:
        items = _parse_sector_table(await _get_html(url))
        if items:
            _sector_cache.set(key, items)
        return items[:limit]
    except Exception as exc:
        logger.warning("sector fetch failed: %s", exc)
        return _stale(_sector_cache, key, limit)
