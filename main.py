import asyncio
import json
import logging
from pathlib import Path
from urllib.parse import urlparse

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

import cache
import dart_client
import stock_price
import analyzer
import report_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="한국 주식 가치투자 분석")

STATIC_DIR = Path(__file__).parent / "static"
ANALYSIS_SEMAPHORE = asyncio.Semaphore(2)
ANALYSIS_LOCKS: dict[str, asyncio.Lock] = {}
ANALYSIS_LOCKS_GUARD = asyncio.Lock()
LATEST_REPORT_CACHE_TTL_MINUTES = 15

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost",
        "http://127.0.0.1",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
        "https://ducklove.github.io",
    ],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    await cache.init_db()
    needs_corp_refresh = not await cache.is_corp_codes_loaded() or await cache.corp_codes_need_refresh()
    if needs_corp_refresh:
        logger.info("corp_codes 테이블을 DART 기준으로 갱신합니다...")
        try:
            codes = await dart_client.fetch_corp_codes()
            await cache.save_corp_codes(codes)
            logger.info(f"{len(codes)}개 상장사 코드를 저장했습니다.")
        except Exception as e:
            logger.error(f"corp_codes 다운로드 실패: {e}")


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/app-config.js")
async def app_config():
    return FileResponse(STATIC_DIR / "app-config.js", media_type="application/javascript")


@app.get("/api/search")
async def search(q: str = Query(..., min_length=1)):
    results = await cache.search_corp(q)
    return results


async def _get_analysis_lock(stock_code: str) -> asyncio.Lock:
    async with ANALYSIS_LOCKS_GUARD:
        lock = ANALYSIS_LOCKS.get(stock_code)
        if lock is None:
            lock = asyncio.Lock()
            ANALYSIS_LOCKS[stock_code] = lock
        return lock


def _sse_event(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def _is_allowed_report_pdf_url(url: str) -> bool:
    parsed = urlparse(url)
    return (
        parsed.scheme == "https"
        and parsed.netloc == "stock.pstatic.net"
        and parsed.path.startswith("/stock-research/")
        and parsed.path.endswith(".pdf")
    )


async def _build_analysis_response(
    stock_code: str,
    corp_name: str,
    fin_data: list[dict],
    mkt_data: list[dict],
    cached: bool,
    analyzed_at: str | None = None,
) -> dict:
    try:
        weekly_mkt_data = await stock_price.fetch_weekly_market_data(stock_code, fin_data)
    except Exception as e:
        logger.warning(f"주간 시장 데이터 계산 실패({stock_code}): {e}")
        weekly_mkt_data = []

    try:
        quote_snapshot = await stock_price.fetch_quote_snapshot(stock_code)
    except Exception as e:
        logger.warning(f"현재가 스냅샷 계산 실패({stock_code}): {e}")
        quote_snapshot = {}

    result = analyzer.analyze(fin_data, mkt_data, weekly_mkt_data)
    payload = {
        "stock_code": stock_code,
        "corp_name": corp_name,
        "cached": cached,
        "quote_snapshot": quote_snapshot,
        **result,
    }
    if analyzed_at:
        payload["analyzed_at"] = analyzed_at
    return payload


async def _ensure_financial_report_dates(stock_code: str, corp_code: str | None, fin_data: list[dict]) -> list[dict]:
    if not fin_data or all(item.get("report_date") for item in fin_data):
        return fin_data
    if not corp_code:
        return fin_data

    years = [item["year"] for item in fin_data if item.get("year") is not None]
    if not years:
        return fin_data

    report_dates = await dart_client.fetch_annual_report_dates(corp_code, min(years), max(years))
    updated = False
    for item in fin_data:
        if item.get("report_date"):
            continue
        report_date = report_dates.get(item["year"])
        if report_date:
            item["report_date"] = report_date
            updated = True

    if updated:
        await cache.save_financial_data(stock_code, fin_data)

    return fin_data


@app.get("/api/analyze/{stock_code}")
async def analyze_stock(stock_code: str):
    corp_code = await cache.get_corp_code(stock_code)
    if not corp_code:
        raise HTTPException(status_code=404, detail="종목코드를 찾을 수 없습니다.")

    # 캐시 확인
    meta = await cache.get_analysis_meta(stock_code)
    if meta:
        fin_data = await cache.get_financial_data(stock_code)
        fin_data = await _ensure_financial_report_dates(stock_code, corp_code, fin_data)
        mkt_data = await cache.get_market_data(stock_code)
        try:
            refreshed = await stock_price.fetch_market_data(stock_code, fin_data)
            if refreshed:
                mkt_data = refreshed
                await cache.save_market_data(stock_code, refreshed)
        except Exception as e:
            logger.warning(f"시장 데이터 재계산 실패({stock_code}): {e}")
        return await _build_analysis_response(
            stock_code,
            meta["corp_name"],
            fin_data,
            mkt_data,
            cached=True,
            analyzed_at=meta["analyzed_at"],
        )

    corp_name = await cache.get_corp_name(stock_code)

    async def stream():
        stock_lock = await _get_analysis_lock(stock_code)

        if stock_lock.locked():
            yield _sse_event("progress", {"step": "queued", "message": "같은 종목 분석이 진행 중입니다. 완료 후 캐시 결과를 사용합니다..."})
        if ANALYSIS_SEMAPHORE.locked():
            yield _sse_event("progress", {"step": "queued_global", "message": "다른 분석 작업이 많아 잠시 대기합니다..."})

        async with stock_lock:
            meta = await cache.get_analysis_meta(stock_code)
            if meta:
                fin_data = await cache.get_financial_data(stock_code)
                fin_data = await _ensure_financial_report_dates(stock_code, corp_code, fin_data)
                mkt_data = await cache.get_market_data(stock_code)
                try:
                    refreshed = await stock_price.fetch_market_data(stock_code, fin_data)
                    if refreshed:
                        mkt_data = refreshed
                        await cache.save_market_data(stock_code, refreshed)
                except Exception as e:
                    logger.warning(f"시장 데이터 재계산 실패({stock_code}): {e}")
                payload = await _build_analysis_response(
                    stock_code,
                    meta["corp_name"],
                    fin_data,
                    mkt_data,
                    cached=True,
                    analyzed_at=meta["analyzed_at"],
                )
                yield _sse_event("result", payload)
                return

            async with ANALYSIS_SEMAPHORE:
                yield _sse_event("progress", {"step": "start", "message": f"{corp_name} 분석을 시작합니다..."})
                report_dates = {}

                # DART 재무제표 수집
                yield _sse_event("progress", {"step": "dart_start", "message": "DART 재무제표를 수집합니다..."})
                fin_data = []
                try:
                    from datetime import datetime as dt
                    end_year = dt.now().year - 1
                    start_year = dart_client.DART_ANNUAL_DATA_START_YEAR
                    total_years = end_year - start_year + 1
                    report_dates = await dart_client.fetch_annual_report_dates(corp_code, start_year, end_year)

                    for i, year in enumerate(range(start_year, end_year + 1)):
                        yield _sse_event("progress", {
                            "step": "dart_fetch",
                            "message": f"DART 재무제표 조회 중... ({i+1}/{total_years}) - {year}년",
                            "current": i + 1, "total": total_years,
                        })
                        stmt = await dart_client.fetch_financial_statement(corp_code, year)
                        if stmt:
                            report_date = report_dates.get(year)
                            if report_date:
                                stmt["report_date"] = report_date
                            fin_data.append(stmt)
                        await asyncio.sleep(0.5)

                    yield _sse_event("progress", {
                        "step": "dart_done",
                        "message": f"DART 재무제표 수집 완료 ({len(fin_data)}개년 데이터)",
                    })
                except Exception as e:
                    logger.error(f"DART 재무제표 조회 실패: {e}")
                    yield _sse_event("progress", {"step": "dart_error", "message": f"DART 조회 실패: {e}"})

                yield _sse_event("progress", {"step": "market_start", "message": "시장 데이터와 파생 지표를 계산합니다..."})
                mkt_data = []
                try:
                    mkt_data = await stock_price.fetch_market_data(stock_code, fin_data)
                    yield _sse_event("progress", {
                        "step": "market_done",
                        "message": f"시장 데이터 수집 완료 ({len(mkt_data)}개년 데이터)",
                    })
                except Exception as e:
                    logger.error(f"시장 데이터 조회 실패: {e}")
                    yield _sse_event("progress", {"step": "market_error", "message": f"시장 데이터 조회 실패: {e}"})

                if not fin_data and not mkt_data:
                    yield _sse_event("error", {"message": "데이터를 가져올 수 없습니다."})
                    return

                yield _sse_event("progress", {"step": "saving", "message": "데이터를 캐시에 저장합니다..."})
                if fin_data:
                    await cache.save_financial_data(stock_code, fin_data)
                if mkt_data:
                    await cache.save_market_data(stock_code, mkt_data)
                await cache.save_analysis_meta(stock_code, corp_name or stock_code)

                yield _sse_event("progress", {"step": "analyzing", "message": "지표를 계산합니다..."})
                payload = await _build_analysis_response(
                    stock_code,
                    corp_name,
                    fin_data,
                    mkt_data,
                    cached=False,
                )
                yield _sse_event("result", payload)

    return StreamingResponse(stream(), media_type="text/event-stream")


@app.get("/api/reports/{stock_code}")
async def get_reports(stock_code: str):
    """증권사 리포트 목록 (네이버 금융, 최근 3년)."""
    try:
        reports = await report_client.fetch_reports(stock_code)
        return {"stock_code": stock_code, "reports": reports}
    except Exception as e:
        logger.error(f"증권사 리포트 조회 실패: {e}")
        return {"stock_code": stock_code, "reports": [], "error": str(e)}


@app.get("/api/reports/{stock_code}/latest")
async def get_latest_report(stock_code: str):
    try:
        cached_report = await cache.get_latest_report(stock_code, LATEST_REPORT_CACHE_TTL_MINUTES)
        if cached_report:
            cached_at = cached_report.pop("_cached_at", None)
            return {"stock_code": stock_code, "report": cached_report, "cached": True, "cached_at": cached_at}

        report = await report_client.fetch_latest_report(stock_code)
        if report:
            await cache.save_latest_report(stock_code, report)
        return {"stock_code": stock_code, "report": report, "cached": False}
    except Exception as e:
        logger.error(f"최신 증권사 리포트 조회 실패: {e}")
        return {"stock_code": stock_code, "report": None, "cached": False, "error": str(e)}


@app.get("/api/report-pdf")
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


@app.delete("/api/cache/{stock_code}")
async def delete_cache(stock_code: str):
    await cache.delete_analysis(stock_code)
    return {"ok": True}


@app.get("/api/cache/list")
async def cache_list():
    return await cache.get_cached_analyses()


# 정적 파일 서빙 (CSS, JS 등 추가 시 대비)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
