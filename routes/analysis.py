import asyncio
import logging

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse

import analyzer
import cache
import dart_client
import stock_price
from deps import (
    ANALYSIS_SEMAPHORE,
    ANALYSIS_LOCKS,
    ANALYSIS_LOCKS_GUARD,
    analysis_snapshot_is_stale,
    default_user_preference,
    get_current_user,
    sse_event,
)

logger = logging.getLogger(__name__)
router = APIRouter()


async def _get_analysis_lock(stock_code: str) -> asyncio.Lock:
    async with ANALYSIS_LOCKS_GUARD:
        lock = ANALYSIS_LOCKS.get(stock_code)
        if lock is None:
            lock = asyncio.Lock()
            ANALYSIS_LOCKS[stock_code] = lock
        return lock


async def _decorate_analysis_payload(payload: dict, user: dict | None) -> dict:
    enriched = dict(payload)
    enriched["authenticated"] = bool(user)
    if user:
        enriched["user_preference"] = await cache.get_user_stock_preference(
            user["google_sub"],
            payload["stock_code"],
        )
    else:
        enriched["user_preference"] = default_user_preference()
    return enriched


async def _remember_recent_analysis(user: dict | None, stock_code: str):
    if user:
        await cache.touch_user_recent_analysis(user["google_sub"], stock_code)


async def _build_analysis_response(
    stock_code: str,
    corp_name: str,
    fin_data: list[dict],
    mkt_data: list[dict],
    cached: bool,
    analyzed_at: str | None = None,
) -> dict:
    weekly_mkt_data, quote_snapshot = await asyncio.gather(
        stock_price.fetch_weekly_market_data(stock_code, fin_data),
        stock_price.fetch_quote_snapshot(stock_code),
        return_exceptions=True,
    )
    if isinstance(weekly_mkt_data, Exception):
        logger.warning(f"주간 시장 데이터 계산 실패({stock_code}): {weekly_mkt_data}")
        weekly_mkt_data = []
    if isinstance(quote_snapshot, Exception):
        logger.warning(f"현재가 스냅샷 계산 실패({stock_code}): {quote_snapshot}")
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


async def _ensure_financial_coverage(stock_code: str, corp_code: str | None, fin_data: list[dict]) -> list[dict]:
    merged = await stock_price.ensure_financial_data_coverage(stock_code, fin_data)
    if merged != fin_data:
        await cache.save_financial_data(stock_code, merged)
    return await _ensure_financial_report_dates(stock_code, corp_code, merged)


async def _load_cached_analysis_payload(
    stock_code: str,
    corp_code: str | None,
    corp_name: str,
    analyzed_at: str | None,
) -> dict:
    fin_data = await cache.get_financial_data(stock_code)
    if corp_code:
        fin_data = await _ensure_financial_report_dates(stock_code, corp_code, fin_data)

    mkt_data = await cache.get_market_data(stock_code)
    needs_market_refresh = stock_price.market_data_needs_refresh(mkt_data)

    if corp_code and (not fin_data or needs_market_refresh):
        fin_data = await _ensure_financial_coverage(stock_code, corp_code, fin_data)

    if needs_market_refresh:
        try:
            refreshed = await stock_price.fetch_market_data(stock_code, fin_data)
            if refreshed:
                mkt_data = refreshed
                await cache.save_market_data(stock_code, refreshed)
        except Exception as e:
            logger.warning(f"시장 데이터 갱신 실패({stock_code}): {e}")

    payload = await _build_analysis_response(
        stock_code,
        corp_name,
        fin_data,
        mkt_data,
        cached=True,
        analyzed_at=analyzed_at,
    )
    await cache.save_analysis_snapshot(stock_code, corp_name, payload)
    return payload


@router.get("/api/analyze/{stock_code}")
async def analyze_stock(stock_code: str, request: Request):
    current_user = await get_current_user(request)
    snapshot = await cache.get_analysis_snapshot(stock_code)
    if snapshot and not analysis_snapshot_is_stale(snapshot.get("analyzed_at")):
        await _remember_recent_analysis(current_user, stock_code)
        return await _decorate_analysis_payload(snapshot, current_user)

    meta = await cache.get_analysis_meta(stock_code)
    if meta:
        corp_code = await cache.get_corp_code(stock_code)
        payload = await _load_cached_analysis_payload(
            stock_code,
            corp_code,
            meta["corp_name"],
            meta.get("analyzed_at"),
        )
        await _remember_recent_analysis(current_user, stock_code)
        return await _decorate_analysis_payload(payload, current_user)

    corp_code = await cache.get_corp_code(stock_code)
    if not corp_code:
        raise HTTPException(status_code=404, detail="종목코드를 찾을 수 없습니다.")

    corp_name = await cache.get_corp_name(stock_code)

    async def stream():
        stock_lock = await _get_analysis_lock(stock_code)

        if stock_lock.locked():
            yield sse_event("progress", {"step": "queued", "message": "같은 종목 분석이 진행 중입니다. 완료 후 캐시 결과를 사용합니다..."})
        if ANALYSIS_SEMAPHORE.locked():
            yield sse_event("progress", {"step": "queued_global", "message": "다른 분석 작업이 많아 잠시 대기합니다..."})

        async with stock_lock:
            snapshot = await cache.get_analysis_snapshot(stock_code)
            if snapshot and not analysis_snapshot_is_stale(snapshot.get("analyzed_at")):
                await _remember_recent_analysis(current_user, stock_code)
                yield sse_event("result", await _decorate_analysis_payload(snapshot, current_user))
                return

            meta = await cache.get_analysis_meta(stock_code)
            if meta:
                payload = await _load_cached_analysis_payload(
                    stock_code,
                    corp_code,
                    meta["corp_name"],
                    meta.get("analyzed_at"),
                )
                await _remember_recent_analysis(current_user, stock_code)
                yield sse_event("result", await _decorate_analysis_payload(payload, current_user))
                return

            async with ANALYSIS_SEMAPHORE:
                yield sse_event("progress", {"step": "start", "message": f"{corp_name} 분석을 시작합니다..."})
                report_dates = {}

                # DART 재무제표 수집
                yield sse_event("progress", {"step": "dart_start", "message": "DART 재무제표를 수집합니다..."})
                fin_data = []
                try:
                    from datetime import datetime as dt
                    end_year = dt.now().year - 1
                    start_year = dart_client.DART_ANNUAL_DATA_START_YEAR
                    total_years = end_year - start_year + 1
                    report_dates = await dart_client.fetch_annual_report_dates(corp_code, start_year, end_year)

                    for i, year in enumerate(range(start_year, end_year + 1)):
                        yield sse_event("progress", {
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

                    yield sse_event("progress", {
                        "step": "dart_done",
                        "message": f"DART 재무제표 수집 완료 ({len(fin_data)}개년 데이터)",
                    })
                except Exception as e:
                    logger.error(f"DART 재무제표 조회 실패: {e}")
                    yield sse_event("progress", {"step": "dart_error", "message": f"DART 조회 실패: {e}"})

                try:
                    fin_data = await _ensure_financial_coverage(stock_code, corp_code, fin_data)
                    yield sse_event("progress", {
                        "step": "dart_done",
                        "message": f"연간 재무 범위 보강 완료 ({len(fin_data)}개년 데이터)",
                    })
                except Exception as e:
                    logger.warning(f"재무 범위 보강 실패({stock_code}): {e}")

                yield sse_event("progress", {"step": "market_start", "message": "시장 데이터와 파생 지표를 계산합니다..."})
                mkt_data = []
                try:
                    mkt_data = await stock_price.fetch_market_data(stock_code, fin_data)
                    yield sse_event("progress", {
                        "step": "market_done",
                        "message": f"시장 데이터 수집 완료 ({len(mkt_data)}개년 데이터)",
                    })
                except Exception as e:
                    logger.error(f"시장 데이터 조회 실패: {e}")
                    yield sse_event("progress", {"step": "market_error", "message": f"시장 데이터 조회 실패: {e}"})

                if not fin_data and not mkt_data:
                    yield sse_event("error", {"message": "데이터를 가져올 수 없습니다."})
                    return

                yield sse_event("progress", {"step": "saving", "message": "데이터를 캐시에 저장합니다..."})
                if fin_data:
                    await cache.save_financial_data(stock_code, fin_data)
                if mkt_data:
                    await cache.save_market_data(stock_code, mkt_data)

                yield sse_event("progress", {"step": "analyzing", "message": "지표를 계산합니다..."})
                payload = await _build_analysis_response(
                    stock_code,
                    corp_name,
                    fin_data,
                    mkt_data,
                    cached=False,
                )
                await cache.save_analysis_snapshot(stock_code, corp_name or stock_code, payload)
                await _remember_recent_analysis(current_user, stock_code)
                yield sse_event("result", await _decorate_analysis_payload(payload, current_user))

    return StreamingResponse(stream(), media_type="text/event-stream")
