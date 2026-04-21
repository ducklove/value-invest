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


@router.get("/api/analyze/{stock_code}/daily")
async def get_daily_data(stock_code: str):
    """Return 1-year daily price + valuation data for a stock."""
    from datetime import date, timedelta
    end_date = date.today()
    start_date = end_date - timedelta(days=365)

    financials_payload, dividends_payload, history_payload = await asyncio.gather(
        stock_price.kis_proxy_client.get_financials(stock_code),
        stock_price.kis_proxy_client.get_dividends(stock_code, start_date=start_date, end_date=end_date),
        stock_price.kis_proxy_client.get_history(stock_code, start_date=start_date, end_date=end_date, period="D", adjusted=True),
        return_exceptions=True,
    )
    if isinstance(financials_payload, Exception):
        financials_payload = {}
    if isinstance(dividends_payload, Exception):
        dividends_payload = {}
    if isinstance(history_payload, Exception):
        raise HTTPException(status_code=500, detail="일봉 데이터 조회 실패")

    normalized = stock_price._normalize_financial_rows(financials_payload, None)
    financial_timeline = stock_price._financial_timeline(None, normalized)
    kis_dividend_events = stock_price._build_dividend_events(dividends_payload.get("items"))

    results = []
    active_financial = None
    timeline_index = 0

    for item in stock_price._sorted_history_items(history_payload.get("items")):
        trade_date = stock_price._parse_date(stock_price._get_first(item, "stck_bsop_date", "date", "trade_date"))
        if trade_date is None:
            continue
        from datetime import datetime
        trade_dt = datetime.combine(trade_date, datetime.min.time())
        while timeline_index < len(financial_timeline) and financial_timeline[timeline_index][0] <= trade_dt:
            active_financial = financial_timeline[timeline_index][1]
            timeline_index += 1

        close = stock_price._safe_float(stock_price._get_first(item, "stck_clpr", "close_price", "close"))
        eps = active_financial.get("eps") if active_financial else None
        bps = active_financial.get("bps") if active_financial else None
        shares = active_financial.get("shares_outstanding") if active_financial else None
        trailing_div = stock_price._sum_trailing_dividends(kis_dividend_events, trade_date)
        if trailing_div is None and kis_dividend_events and close is not None:
            trailing_div = 0.0

        results.append({
            "date": trade_date.isoformat(),
            "close_price": close,
            "per": stock_price._safe_div(close, eps) if close else None,
            "pbr": stock_price._safe_div(close, bps) if close else None,
            "eps": eps,
            "dividend_yield": stock_price._safe_div(trailing_div, close, 100) if trailing_div is not None and close else None,
            "market_cap": round(close * shares, 2) if close and shares else None,
            "roe": active_financial.get("roe") if active_financial else None,
            "debt_ratio": active_financial.get("debt_ratio") if active_financial else None,
            "operating_margin": active_financial.get("operating_margin") if active_financial else None,
        })
    return results


def _compute_beta_from_pairs(pairs: list[tuple[float, float]]) -> float | None:
    """pairs = [(stock_close, market_close), ...] 날짜 오름차순.
    수익률 (r_t = P_t / P_{t-1} - 1) 로 Cov / Var 계산.
    표본 < 20 이면 None.
    """
    if len(pairs) < 21:
        return None
    stock_rets: list[float] = []
    market_rets: list[float] = []
    for i in range(1, len(pairs)):
        s0, m0 = pairs[i - 1]
        s1, m1 = pairs[i]
        if not (s0 and m0 and s1 and m1):
            continue
        stock_rets.append(s1 / s0 - 1.0)
        market_rets.append(m1 / m0 - 1.0)
    n = len(stock_rets)
    if n < 20:
        return None
    mean_s = sum(stock_rets) / n
    mean_m = sum(market_rets) / n
    cov = sum((stock_rets[i] - mean_s) * (market_rets[i] - mean_m) for i in range(n)) / (n - 1)
    var_m = sum((market_rets[i] - mean_m) ** 2 for i in range(n)) / (n - 1)
    if var_m <= 0:
        return None
    return round(cov / var_m, 3)


@router.get("/api/analyze/{stock_code}/beta")
async def get_stock_beta(stock_code: str):
    """최근 1년 일별 수익률 기준 코스피 대비 베타.

    KIS 일봉 1년치 + benchmark_daily 테이블의 KOSPI 일별 close 를
    날짜로 inner-join 한 뒤 일별 수익률 (simple return) 로 β = Cov/Var
    계산. 반환 포맷: {beta: float|null, sample_size: int, start: iso,
    end: iso}. 데이터가 부족하거나 해외 종목 등으로 매칭 불가하면
    beta=null (UI 는 'N/A' 표기).
    """
    from datetime import date, timedelta
    import benchmark_history

    end_date = date.today()
    start_date = end_date - timedelta(days=400)  # ~1y 여유 (주말/휴장 포함)
    start_iso = start_date.isoformat()

    # KOSPI benchmark lazy backfill + load. 실패는 조용히 — beta=null 반환.
    try:
        await benchmark_history.backfill_benchmark("KOSPI", start_iso)
        kospi_rows = await cache.get_benchmark_rows("KOSPI", start=start_iso)
    except Exception as e:
        logger.warning(f"KOSPI 벤치마크 로드 실패({stock_code}): {e}")
        kospi_rows = []
    kospi_map = {r["date"]: r["close"] for r in kospi_rows if r.get("close") is not None}

    # 종목 일봉 — KIS Proxy. 실패 시 beta=null.
    try:
        history_payload = await stock_price.kis_proxy_client.get_history(
            stock_code, start_date=start_date, end_date=end_date, period="D", adjusted=True,
        )
    except Exception as e:
        logger.warning(f"종목 일봉 로드 실패({stock_code}): {e}")
        return {"beta": None, "sample_size": 0, "start": None, "end": None, "benchmark": "KOSPI"}

    pairs: list[tuple[float, float]] = []
    matched_dates: list[str] = []
    for item in stock_price._sorted_history_items(history_payload.get("items") if isinstance(history_payload, dict) else []):
        trade_date = stock_price._parse_date(stock_price._get_first(item, "stck_bsop_date", "date", "trade_date"))
        if trade_date is None:
            continue
        iso = trade_date.isoformat()
        close = stock_price._safe_float(stock_price._get_first(item, "stck_clpr", "close_price", "close"))
        market_close = kospi_map.get(iso)
        if close is None or market_close is None:
            continue
        pairs.append((close, market_close))
        matched_dates.append(iso)

    # 최근 252 거래일만 (1년).
    pairs = pairs[-253:]
    matched_dates = matched_dates[-253:]

    beta = _compute_beta_from_pairs(pairs)
    return {
        "beta": beta,
        "sample_size": max(0, len(pairs) - 1),
        "start": matched_dates[0] if matched_dates else None,
        "end": matched_dates[-1] if matched_dates else None,
        "benchmark": "KOSPI",
    }


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

                # KIS 재무제표 수집 (1회 호출)
                yield sse_event("progress", {"step": "financial_start", "message": "재무제표를 수집합니다..."})
                fin_data = []
                try:
                    fin_data = await _ensure_financial_coverage(stock_code, corp_code, [])
                    yield sse_event("progress", {
                        "step": "financial_done",
                        "message": f"재무제표 수집 완료 ({len(fin_data)}개년 데이터)",
                    })
                except Exception as e:
                    logger.error(f"재무제표 조회 실패: {e}")
                    yield sse_event("progress", {"step": "financial_error", "message": f"재무제표 조회 실패: {e}"})

                yield sse_event("progress", {"step": "market_start", "message": "시장 데이터를 계산합니다..."})
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
