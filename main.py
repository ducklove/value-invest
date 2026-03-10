import asyncio
import json
import logging
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
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
    if not await cache.is_corp_codes_loaded():
        logger.info("corp_codes 테이블이 비어 있습니다. DART에서 다운로드합니다...")
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


def _sse_event(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


@app.get("/api/analyze/{stock_code}")
async def analyze_stock(stock_code: str):
    # 캐시 확인
    meta = await cache.get_analysis_meta(stock_code)
    if meta:
        fin_data = await cache.get_financial_data(stock_code)
        mkt_data = await cache.get_market_data(stock_code)
        result = analyzer.analyze(fin_data, mkt_data)
        return {
            "stock_code": stock_code,
            "corp_name": meta["corp_name"],
            "cached": True,
            "analyzed_at": meta["analyzed_at"],
            **result,
        }

    # corp_code 조회
    corp_code = await cache.get_corp_code(stock_code)
    if not corp_code:
        raise HTTPException(status_code=404, detail="종목코드를 찾을 수 없습니다.")

    corp_name = await cache.get_corp_name(stock_code)

    async def stream():
        yield _sse_event("progress", {"step": "start", "message": f"{corp_name} 분석을 시작합니다..."})

        # DART 재무제표 수집
        yield _sse_event("progress", {"step": "dart_start", "message": "DART 재무제표를 수집합니다..."})
        fin_data = []
        try:
            async def on_dart_progress(current, total, year):
                yield_event = _sse_event("progress", {
                    "step": "dart_fetch",
                    "message": f"DART 재무제표 조회 중... ({current}/{total}) - {year}년",
                    "current": current, "total": total, "year": year,
                })
                # We can't yield from a callback, so we use a list to collect events
                progress_events.append(yield_event)

            # Use a different approach: yield progress inline
            from datetime import datetime as dt
            end_year = dt.now().year - 1
            start_year = dart_client.DART_ANNUAL_DATA_START_YEAR
            total_years = end_year - start_year + 1

            for i, year in enumerate(range(start_year, end_year + 1)):
                yield _sse_event("progress", {
                    "step": "dart_fetch",
                    "message": f"DART 재무제표 조회 중... ({i+1}/{total_years}) - {year}년",
                    "current": i + 1, "total": total_years,
                })
                stmt = await dart_client.fetch_financial_statement(corp_code, year)
                if stmt:
                    fin_data.append(stmt)
                await asyncio.sleep(0.5)

            yield _sse_event("progress", {
                "step": "dart_done",
                "message": f"DART 재무제표 수집 완료 ({len(fin_data)}개년 데이터)",
            })
        except Exception as e:
            logger.error(f"DART 재무제표 조회 실패: {e}")
            yield _sse_event("progress", {"step": "dart_error", "message": f"DART 조회 실패: {e}"})

        # pykrx 시장 데이터 수집
        yield _sse_event("progress", {"step": "market_start", "message": "KRX 시장 데이터를 수집합니다... (PER/PBR/EPS/주가)"})
        mkt_data = []
        try:
            mkt_data = await stock_price.fetch_market_data(stock_code)
            yield _sse_event("progress", {
                "step": "market_done",
                "message": f"KRX 시장 데이터 수집 완료 ({len(mkt_data)}개년 데이터)",
            })
        except Exception as e:
            logger.error(f"pykrx 시장 데이터 조회 실패: {e}")
            yield _sse_event("progress", {"step": "market_error", "message": f"KRX 조회 실패: {e}"})

        if not fin_data and not mkt_data:
            yield _sse_event("error", {"message": "데이터를 가져올 수 없습니다."})
            return

        # 캐시 저장
        yield _sse_event("progress", {"step": "saving", "message": "데이터를 캐시에 저장합니다..."})
        if fin_data:
            await cache.save_financial_data(stock_code, fin_data)
        if mkt_data:
            await cache.save_market_data(stock_code, mkt_data)
        await cache.save_analysis_meta(stock_code, corp_name or stock_code)

        # 분석
        yield _sse_event("progress", {"step": "analyzing", "message": "지표를 계산합니다..."})
        result = analyzer.analyze(fin_data, mkt_data)

        yield _sse_event("result", {
            "stock_code": stock_code,
            "corp_name": corp_name,
            "cached": False,
            **result,
        })

    return StreamingResponse(stream(), media_type="text/event-stream")


@app.get("/api/reports/{stock_code}")
async def get_reports(stock_code: str):
    """증권사 리포트 목록 (WiseReport, 최근 3년)."""
    try:
        reports = await report_client.fetch_reports(stock_code)
        return {"stock_code": stock_code, "reports": reports}
    except Exception as e:
        logger.error(f"증권사 리포트 조회 실패: {e}")
        return {"stock_code": stock_code, "reports": [], "error": str(e)}


@app.delete("/api/cache/{stock_code}")
async def delete_cache(stock_code: str):
    await cache.delete_analysis(stock_code)
    return {"ok": True}


@app.get("/api/cache/list")
async def cache_list():
    return await cache.get_cached_analyses()


# 정적 파일 서빙 (CSS, JS 등 추가 시 대비)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
