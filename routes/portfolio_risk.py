"""포트폴리오 리스크 패널 API (로드맵 신규 기능 ② — NAV 히스토리 기반).

GET /api/portfolio/risk?window=1Y&benchmark=IDX_KOSPI

NAV 스냅샷 히스토리(repositories.snapshots.get_nav_history)와 벤치마크 일별
종가(repositories.benchmark_daily.get_benchmark_rows)를 읽어
services.portfolio.risk 의 순수 계산 모듈에 넘긴다. 스냅샷은 하루 한 번
갱신되므로 결과는 사용자+윈도+벤치마크 키로 10분 TTL 메모리 캐시.
"""

from __future__ import annotations

import logging
import os
import re

from fastapi import APIRouter, HTTPException, Query, Request

from cache_layer import MemoryTTLCache
from deps import get_current_user
from repositories import benchmark_daily as benchmark_daily_repo
from repositories import snapshots as snapshots_repo
from services.portfolio import benchmarks, risk

logger = logging.getLogger(__name__)
router = APIRouter()

RISK_CACHE_TTL_SECONDS = 600  # 10분
# 포트폴리오 전체 레벨의 사용자별 기본 벤치마크 설정은 아직 없다 —
# services.portfolio.benchmarks 의 기본값은 종목 코드 단위라서,
# 전체 NAV 비교는 앱 전반에서 쓰는 코스피 지수 코드를 기본으로 쓴다.
DEFAULT_BENCHMARK = "IDX_KOSPI"
# benchmark_daily.code 는 IDX_KOSPI / GOLD / FX_USDKRW 같은 영대문자 코드.
_BENCHMARK_CODE_RE = re.compile(r"^[A-Z0-9_.\-^=]{1,24}$")

_risk_cache = MemoryTTLCache("portfolio.risk", RISK_CACHE_TTL_SECONDS)


def _require_user(user):
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    return user


def _risk_free_rate_pct() -> float:
    """연 무위험수익률(%) — env RISK_FREE_RATE_PCT, 기본 0."""
    raw = os.environ.get("RISK_FREE_RATE_PCT")
    if not raw:
        return 0.0
    try:
        return float(raw)
    except (TypeError, ValueError):
        logger.warning("RISK_FREE_RATE_PCT 값이 숫자가 아님: %r — 0%%로 대체", raw)
        return 0.0


@router.get("/api/portfolio/risk")
async def get_portfolio_risk(
    request: Request,
    window: str = Query("1Y"),
    benchmark: str | None = Query(None),
):
    user = _require_user(await get_current_user(request))

    win = (window or "1Y").strip().upper()
    if win not in risk.WINDOWS:
        raise HTTPException(
            status_code=400,
            detail=f"지원하지 않는 window 값입니다: {window} (가능: {', '.join(risk.WINDOWS)})",
        )
    bench_code = (benchmark or DEFAULT_BENCHMARK).strip().upper()
    if not _BENCHMARK_CODE_RE.match(bench_code):
        raise HTTPException(status_code=400, detail="benchmark 코드 형식이 올바르지 않습니다.")

    cache_key = f"{user['google_sub']}|{win}|{bench_code}"
    cached = _risk_cache.get(cache_key)
    if cached is not None:
        return cached

    nav_series = await snapshots_repo.get_nav_history(user["google_sub"])
    bench_rows: list[dict] = []
    if nav_series:
        # NAV 시리즈 시작일 이후 구간만 읽는다(교집합 밖 데이터는 쓸 일 없음).
        bench_rows = await benchmark_daily_repo.get_benchmark_rows(
            bench_code, start=str(nav_series[0].get("date") or "") or None
        )

    result = risk.compute_risk_metrics(
        nav_series,
        win,
        benchmark_rows=bench_rows or None,
        risk_free_rate_pct=_risk_free_rate_pct(),
    )
    if result.get("benchmark") is not None:
        # 계산 결과(beta/correlation/overlap_returns)에 표시용 코드·이름을 덧붙인다.
        result["benchmark"] = {
            "code": bench_code,
            "name": benchmarks.benchmark_name(bench_code),
            **result["benchmark"],
        }

    _risk_cache.set(cache_key, result)
    return result
