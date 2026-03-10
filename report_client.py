import httpx
import re
from datetime import datetime


WISEREPORT_API = "https://comp.wisereport.co.kr/company/ajax/c1080001_data.aspx"
WISEREPORT_PDF = "http://www.wisereport.co.kr/comm/LoadReport.aspx"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Referer": "https://comp.wisereport.co.kr/",
}


def _clean_html(text: str) -> str:
    """HTML 태그 및 불필요한 공백 제거."""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_date(date_str: str) -> str:
    """'26/02/12' -> '2026-02-12' 형식 변환."""
    if not date_str:
        return ""
    parts = date_str.split("/")
    if len(parts) == 3:
        yy, mm, dd = parts
        year = int(yy) + 2000 if int(yy) < 100 else int(yy)
        return f"{year}-{mm}-{dd}"
    return date_str


async def fetch_reports(stock_code: str, max_pages: int = 5, per_page: int = 20) -> list[dict]:
    """WiseReport에서 증권사 리포트 목록을 가져온다. 최근 3년치."""
    cutoff_year = datetime.now().year - 3
    reports = []

    async with httpx.AsyncClient(timeout=15, headers=HEADERS) as client:
        for page in range(1, max_pages + 1):
            resp = await client.get(WISEREPORT_API, params={
                "cmp_cd": stock_code,
                "perPage": str(per_page),
                "curPage": str(page),
            })

            if resp.status_code != 200:
                break

            data = resp.json()
            items = data.get("lists", [])
            if not items:
                break

            for item in items:
                date_str = _parse_date(item.get("ANL_DT", ""))
                if date_str and int(date_str[:4]) < cutoff_year:
                    return reports

                pdf_url = ""
                rpt_id = item.get("RPT_ID")
                brk_cd = item.get("BRK_CD")
                file_nm = item.get("FILE_NM")
                if rpt_id and brk_cd and file_nm:
                    pdf_url = f"{WISEREPORT_PDF}?rpt_id={rpt_id}&brk_cd={brk_cd}&fpath={file_nm}&target=comp"

                target_prc = item.get("TARGET_PRC", "")
                recomm = item.get("RECOMM", "")

                reports.append({
                    "date": date_str,
                    "title": item.get("RPT_TITLE", ""),
                    "analyst": item.get("ANL_NM_KOR", ""),
                    "firm": item.get("BRK_NM_KOR", ""),
                    "firm_short": item.get("BRK_NM_SHORT_KOR", ""),
                    "target_price": target_prc,
                    "recommendation": recomm,
                    "summary": _clean_html(item.get("COMMENT2", "")),
                    "pdf_url": pdf_url,
                    "pages": item.get("PAGE_CNT", 0),
                })

    return reports
