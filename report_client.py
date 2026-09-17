"""네이버 증권 리서치(국내종목) 클라이언트.

2026-09 네이버가 `finance.naver.com/research/company_list.naver` HTML 페이지를
폐지하고 `stock.naver.com/research/company` (Next.js) 로 302 리다이렉트하게
바뀌면서 기존 EUC-KR 테이블 스크래핑이 빈 목록만 돌려줬다. 새 페이지가
쓰는 JSON API 를 직접 호출한다.

- 목록: ``GET {NAVER_RESEARCH_API}?itemCodes=005930&size=50&index=0``
  → ``{"hasNext", "totalCount", "items": [{nid, title, content, brokerName,
  writeDate, goalPrice, opinionText, ...}]}`` — ``index`` 는 0-based 페이지,
  ``size`` 는 최대 50. ``startDate``/``endDate`` 는 ISO(YYYY-MM-DD).
- 상세: ``GET {NAVER_RESEARCH_API}/{nid}`` → ``attachUrl`` (PDF, stock.pstatic.net),
  ``prevGoalPrice``, ``priceAtWriteDate`` 등. 목록에는 PDF 링크가 없어서
  건별로 한 번 더 호출한다.

반환 dict 스키마(date/title/firm/target_price/recommendation/summary/pdf_url/
source_url …)는 프론트·wiki_ingestion·notifications 가 그대로 쓰므로 유지한다.
"""

import asyncio
import re
from datetime import datetime
from typing import Any

import httpx

from core.http import get_http_client

NAVER_RESEARCH_API = "https://stock.naver.com/api/stockSecurity/researches/v2/company"
NAVER_RESEARCH_PAGE = "https://stock.naver.com/research/company"
NAVER_RESEARCH_PAGE_SIZE_MAX = 50

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Referer": "https://stock.naver.com/research/company",
}


def _clean_html(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<br\s*/?>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"</p\s*>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = (
        text.replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", '"')
        .replace("&#39;", "'")
    )
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_date(date_str: str) -> str:
    """'2026-09-07' 은 그대로, 구형 '26.09.07' 도 ISO 로 정규화."""
    if not date_str:
        return ""
    value = str(date_str).strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return value
    parts = value.split(".")
    if len(parts) == 3 and all(p.strip().isdigit() for p in parts):
        yy, mm, dd = (p.strip() for p in parts)
        year = int(yy) + 2000 if int(yy) < 100 else int(yy)
        return f"{year}-{mm.zfill(2)}-{dd.zfill(2)}"
    return value


def _normalize_target_price(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    if not text or "없음" in text:
        return ""
    match = re.search(r"[\d,]+", text)
    if not match:
        return ""
    digits = match.group(0).replace(",", "")
    return digits if digits.isdigit() and int(digits) > 0 else ""


def _normalize_recommendation(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text or "없음" in text:
        return ""
    return text


def _json_body(resp: httpx.Response) -> Any:
    try:
        return resp.json()
    except ValueError:
        return None


def _parse_api_item(item: dict) -> dict | None:
    """목록/상세 API 의 한 건을 기존 리포트 dict 스키마로 변환."""
    if not isinstance(item, dict):
        return None
    nid = str(item.get("nid") or item.get("researchId") or "").strip()
    title = str(item.get("title") or "").strip()
    if not nid or not title:
        return None

    firm = str(item.get("brokerName") or "").strip()
    return {
        "date": _parse_date(item.get("writeDate") or ""),
        "title": title,
        "analyst": str(item.get("analystName") or "").strip(),
        "firm": firm,
        "firm_short": firm,
        "target_price": _normalize_target_price(item.get("goalPrice")),
        "recommendation": _normalize_recommendation(item.get("opinionText") or item.get("opinion")),
        "summary": _clean_html(item.get("content") or ""),
        "pdf_url": str(item.get("attachUrl") or "").strip(),
        "source_url": f"{NAVER_RESEARCH_PAGE}/{nid}",
        "pages": 0,
        "nid": nid,
    }


def _detail_fields(payload: Any) -> dict:
    """상세 응답에서 목록에 없는 필드만 뽑는다 (빈 값은 제외)."""
    body = payload.get("researchContent") if isinstance(payload, dict) and "researchContent" in payload else payload
    if not isinstance(body, dict):
        return {}
    parsed = _parse_api_item(body)
    if parsed is None:
        return {}
    out = {
        "pdf_url": parsed["pdf_url"],
        "summary": parsed["summary"],
        "target_price": parsed["target_price"],
        "recommendation": parsed["recommendation"],
        "analyst": parsed["analyst"],
    }
    prev_goal = _normalize_target_price(body.get("prevGoalPrice"))
    if prev_goal:
        out["prev_target_price"] = prev_goal
    price_at = _normalize_target_price(body.get("priceAtWriteDate"))
    if price_at:
        out["price_at_write_date"] = price_at
    return {key: value for key, value in out.items() if value}


async def _fetch_report_detail(client: httpx.AsyncClient, nid: str) -> dict:
    if not nid:
        return {}
    try:
        resp = await client.get(f"{NAVER_RESEARCH_API}/{nid}", headers=HEADERS, timeout=15.0)
        if resp.status_code != 200:
            return {}
    except Exception:
        return {}
    return _detail_fields(_json_body(resp))


async def _fetch_list_page(
    client: httpx.AsyncClient,
    stock_code: str,
    *,
    index: int,
    size: int,
    start_date: str | None = None,
) -> tuple[list[dict], bool]:
    """한 페이지의 리포트와 hasNext 를 돌려준다. 실패하면 ([], False)."""
    params: dict[str, str] = {
        "itemCodes": stock_code,
        "size": str(max(1, min(size, NAVER_RESEARCH_PAGE_SIZE_MAX))),
        "index": str(max(0, index)),
    }
    if start_date:
        params["startDate"] = start_date
    resp = await client.get(NAVER_RESEARCH_API, params=params, headers=HEADERS, timeout=15.0)
    if resp.status_code != 200:
        return [], False
    payload = _json_body(resp)
    if not isinstance(payload, dict):
        return [], False
    items = payload.get("items")
    if not isinstance(items, list):
        return [], False
    reports = [r for r in (_parse_api_item(item) for item in items) if r is not None]
    return reports, bool(payload.get("hasNext"))


def _merge_detail(report: dict, detail: dict) -> dict:
    report.update({key: value for key, value in detail.items() if value})
    return report


async def fetch_latest_report(stock_code: str) -> dict | None:
    client = await get_http_client("report")
    reports, _ = await _fetch_list_page(client, stock_code, index=0, size=1)
    if not reports:
        return None
    report = reports[0]
    detail = await _fetch_report_detail(client, report.get("nid", ""))
    return _merge_detail(report, detail)


def _dedupe_reports(reports: list[dict]) -> list[dict]:
    """Drop duplicates produced by Naver's pagination.

    Naver's research list occasionally returns the same report row on
    multiple pages — 058650 was coming back as 25 rows for ~13 unique
    reports, inflating the count on the analysis page and confusing the
    "왜 위키가 1건뿐이냐" question (sha1-based dedup in the wiki layer
    was hiding upstream duplication).

    Key: (date, firm, title). pdf_url isn't used because rows without a
    PDF link still collide legitimately on that tuple. First occurrence
    wins so enriched-earlier rows (with pdf_url set) are preferred.
    """
    seen: set[tuple[str, str, str]] = set()
    out: list[dict] = []
    for r in reports:
        key = (
            (r.get("date") or "").strip(),
            (r.get("firm") or "").strip(),
            (r.get("title") or "").strip(),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


async def fetch_reports(stock_code: str, max_pages: int = 2, per_page: int = 50) -> list[dict]:
    """최근 3년치 리포트 (기본 최대 100건). 각 건은 상세 호출로 PDF 링크를 채운다."""
    cutoff_year = datetime.now().year - 3
    reports: list[dict] = []
    detail_limit = asyncio.Semaphore(6)

    client = await get_http_client("report")

    async def enrich(report: dict) -> dict:
        async with detail_limit:
            detail = await _fetch_report_detail(client, report.get("nid", ""))
        return _merge_detail(report, detail)

    for index in range(max(1, max_pages)):
        page_reports, has_next = await _fetch_list_page(
            client,
            stock_code,
            index=index,
            size=per_page,
            start_date=f"{cutoff_year}-01-01",
        )
        if not page_reports:
            break

        kept: list[dict] = []
        reached_cutoff = False
        for report in page_reports:
            date_str = report["date"]
            if date_str[:4].isdigit() and int(date_str[:4]) < cutoff_year:
                reached_cutoff = True
                break
            kept.append(report)

        reports.extend(await asyncio.gather(*(enrich(report) for report in kept)))

        if reached_cutoff or not has_next:
            break

    return _dedupe_reports(reports)
