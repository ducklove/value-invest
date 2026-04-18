import logging
from pathlib import Path
from urllib.parse import urlparse

import httpx
from fastapi import APIRouter, HTTPException, Query, Response

import cache
import report_client
from deps import LATEST_REPORT_CACHE_TTL_MINUTES, REPORT_LIST_CACHE_TTL_MINUTES

logger = logging.getLogger(__name__)
router = APIRouter()


def _is_allowed_report_pdf_url(url: str) -> bool:
    """Naver hosts broker-research PDFs across TWO CDN paths that both
    resolve to pstatic.net — either one is safe to proxy/download.

    This function previously allowed only the first path, which silently
    dropped ~50% of large-cap reports (LG화학 was the trigger: 120
    reports, 106 with pdf_url, only 55 passing — the missing 51 were
    all on ssl.pstatic.net/imgstock/upload/research/…). Both hosts are
    *.pstatic.net (Naver's CDN) so the security posture is unchanged.
    """
    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.path.endswith(".pdf"):
        return False
    # CDN #1 — research portal PDFs.
    if parsed.netloc == "stock.pstatic.net" and parsed.path.startswith("/stock-research/"):
        return True
    # CDN #2 — image/research upload bucket where many firms that publish
    # through Naver Research actually land.
    if parsed.netloc == "ssl.pstatic.net" and parsed.path.startswith("/imgstock/upload/research/"):
        return True
    return False


def _report_signature(report: dict | None) -> tuple:
    if not report:
        return ()
    return (
        report.get("date"),
        report.get("title"),
        report.get("firm"),
        report.get("pdf_url"),
    )


@router.get("/api/reports/{stock_code}")
async def get_reports(stock_code: str, refresh: bool = False):
    """증권사 리포트 목록 (네이버 금융, 최근 3년)."""
    try:
        if not refresh:
            cached_reports = await cache.get_report_list(stock_code, REPORT_LIST_CACHE_TTL_MINUTES)
            if cached_reports:
                return {
                    "stock_code": stock_code,
                    "reports": cached_reports["reports"],
                    "cached": True,
                    "cached_at": cached_reports["fetched_at"],
                }

        reports = await report_client.fetch_reports(stock_code)
        await cache.save_report_list(stock_code, reports)
        if reports:
            await cache.save_latest_report(stock_code, reports[0])
        return {"stock_code": stock_code, "reports": reports, "cached": False}
    except Exception as e:
        logger.error(f"증권사 리포트 조회 실패: {e}")
        return {"stock_code": stock_code, "reports": [], "error": str(e)}


@router.get("/api/reports/{stock_code}/latest")
async def get_latest_report(stock_code: str, refresh: bool = False):
    try:
        cached_report = await cache.get_latest_report(
            stock_code,
            None if refresh else LATEST_REPORT_CACHE_TTL_MINUTES,
        )
        if not refresh and cached_report:
            cached_at = cached_report.pop("_cached_at", None)
            return {"stock_code": stock_code, "report": cached_report, "cached": True, "cached_at": cached_at}

        report = await report_client.fetch_latest_report(stock_code)
        if report:
            changed = _report_signature(cached_report) != _report_signature(report)
            await cache.save_latest_report(stock_code, report)
            return {"stock_code": stock_code, "report": report, "cached": False, "changed": changed}
        if cached_report:
            cached_at = cached_report.pop("_cached_at", None)
            return {"stock_code": stock_code, "report": cached_report, "cached": True, "cached_at": cached_at, "stale": True}
        return {"stock_code": stock_code, "report": None, "cached": False, "changed": False}
    except Exception as e:
        logger.error(f"최신 증권사 리포트 조회 실패: {e}")
        return {"stock_code": stock_code, "report": None, "cached": False, "error": str(e)}


@router.get("/api/report-pdf")
async def proxy_report_pdf(url: str = Query(..., min_length=1)):
    if not _is_allowed_report_pdf_url(url):
        raise HTTPException(status_code=400, detail="허용되지 않은 리포트 URL입니다.")

    try:
        async with httpx.AsyncClient(timeout=30, headers=report_client.HEADERS, follow_redirects=True) as client:
            resp = await client.get(url)
    except Exception as e:
        logger.error(f"리포트 PDF 프록시 실패: {e}")
        raise HTTPException(status_code=502, detail="리포트 원문을 불러오지 못했습니다.") from e

    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail="리포트 원문 응답이 올바르지 않습니다.")

    filename = Path(urlparse(url).path).name or "report.pdf"
    headers = {
        "Content-Disposition": f'inline; filename="{filename}"',
        "Cache-Control": "public, max-age=86400",
    }
    return Response(content=resp.content, media_type="application/pdf", headers=headers)
