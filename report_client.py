import asyncio
import re
from datetime import datetime
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup


NAVER_RESEARCH_LIST = "https://finance.naver.com/research/company_list.naver"
NAVER_RESEARCH_READ = "https://finance.naver.com/research/company_read.naver"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Referer": "https://finance.naver.com/",
}


def _decode_html(resp: httpx.Response) -> str:
    return resp.content.decode("euc-kr", errors="ignore")


def _clean_html(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_date(date_str: str) -> str:
    if not date_str:
        return ""
    parts = date_str.strip().split(".")
    if len(parts) == 3:
        yy, mm, dd = parts
        year = int(yy) + 2000 if int(yy) < 100 else int(yy)
        return f"{year}-{mm.zfill(2)}-{dd.zfill(2)}"
    return date_str


def _normalize_target_price(value: str) -> str:
    if not value:
        return ""
    if "없음" in value:
        return ""
    match = re.search(r"[\d,]+", value)
    return match.group(0) if match else ""


def _parse_row_report(row) -> dict | None:
    cells = row.find_all("td")
    if len(cells) < 6:
        return None

    title_link = cells[1].find("a", href=True)
    if title_link is None:
        return None

    pdf_link = cells[3].find("a", href=True)
    firm = cells[2].get_text(" ", strip=True)
    return {
        "date": _parse_date(cells[4].get_text(" ", strip=True)),
        "title": title_link.get_text(" ", strip=True),
        "analyst": "",
        "firm": firm,
        "firm_short": firm,
        "target_price": "",
        "recommendation": "",
        "summary": "",
        "pdf_url": urljoin(NAVER_RESEARCH_LIST, pdf_link["href"]) if pdf_link else "",
        "source_url": urljoin(NAVER_RESEARCH_LIST, title_link["href"]),
        "pages": 0,
    }


async def _fetch_report_detail(client: httpx.AsyncClient, source_url: str) -> dict:
    try:
        resp = await client.get(source_url)
        if resp.status_code != 200:
            return {}
    except Exception:
        return {}

    soup = BeautifulSoup(_decode_html(resp), "html.parser")

    pdf_link = soup.select_one("th.view_report a[href]")
    body_root = soup.select_one("td.view_cnt")
    body_div = body_root.find("div") if body_root else None
    target_el = soup.select_one("div.view_info_1 em.money")
    recomm_el = soup.select_one("div.view_info_1 em.coment")

    return {
        "target_price": _normalize_target_price(target_el.get_text(" ", strip=True) if target_el else ""),
        "recommendation": "" if recomm_el is None or "없음" in recomm_el.get_text(" ", strip=True) else recomm_el.get_text(" ", strip=True),
        "summary": _clean_html(body_div.get_text(" ", strip=True) if body_div else ""),
        "pdf_url": urljoin(NAVER_RESEARCH_READ, pdf_link["href"]) if pdf_link else "",
    }


async def fetch_latest_report(stock_code: str) -> dict | None:
    async with httpx.AsyncClient(timeout=15, headers=HEADERS, follow_redirects=True) as client:
        resp = await client.get(
            NAVER_RESEARCH_LIST,
            params={
                "searchType": "itemCode",
                "itemCode": stock_code,
                "page": "1",
            },
        )
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(_decode_html(resp), "html.parser")
        for row in soup.select("table.type_1 tr"):
            report = _parse_row_report(row)
            if report is None:
                continue
            if report["source_url"]:
                detail = await _fetch_report_detail(client, report["source_url"])
                if detail:
                    report.update({key: value for key, value in detail.items() if value})
            return report
    return None


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


async def fetch_reports(stock_code: str, max_pages: int = 5, per_page: int = 20) -> list[dict]:
    cutoff_year = datetime.now().year - 3
    reports = []
    detail_limit = asyncio.Semaphore(6)

    async with httpx.AsyncClient(timeout=15, headers=HEADERS, follow_redirects=True) as client:
        for page in range(1, max_pages + 1):
            resp = await client.get(
                NAVER_RESEARCH_LIST,
                params={
                    "searchType": "itemCode",
                    "itemCode": stock_code,
                    "page": str(page),
                },
            )
            if resp.status_code != 200:
                break

            soup = BeautifulSoup(_decode_html(resp), "html.parser")
            rows = soup.select("table.type_1 tr")
            page_reports = []

            for row in rows:
                report = _parse_row_report(row)
                if report is None:
                    continue

                date_str = report["date"]
                if date_str and int(date_str[:4]) < cutoff_year:
                    return reports

                page_reports.append(report)

            if not page_reports:
                break

            async def enrich(report: dict) -> dict:
                if not report["source_url"]:
                    return report
                async with detail_limit:
                    detail = await _fetch_report_detail(client, report["source_url"])
                if detail:
                    report.update({key: value for key, value in detail.items() if value})
                return report

            reports.extend(await asyncio.gather(*(enrich(report) for report in page_reports)))

    return _dedupe_reports(reports)
