"""배당 캘린더 API (로드맵 신규 기능 ④).

GET /api/portfolio/dividend-calendar?months=12

services.dividend_calendar.build_calendar 가 기존 배당 테이블(market_data /
preferred_dividends / foreign_dividends)과 보유 수량에서 월별 예상 배당
현금흐름을 만든다. 입력 데이터가 천천히 변하므로(연간 DPS + 일 1회 brief)
사용자+months 키 30분 TTL 메모리 캐시.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query, Request

from cache_layer import MemoryTTLCache
from deps import get_current_user
from services import dividend_calendar

logger = logging.getLogger(__name__)
router = APIRouter()

CALENDAR_CACHE_TTL_SECONDS = 1800  # 30분
# 과거 구간은 2개월 고정 — '최근 받은 배당' 맥락용. months 는 앞으로 볼 개월 수.
MONTHS_BACK = 2
MIN_MONTHS, MAX_MONTHS = 3, 24

_calendar_cache = MemoryTTLCache("portfolio.dividend_calendar", CALENDAR_CACHE_TTL_SECONDS)


def _require_user(user):
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    return user


@router.get("/api/portfolio/dividend-calendar")
async def get_dividend_calendar(
    request: Request,
    months: int = Query(12),
):
    user = _require_user(await get_current_user(request))

    # 직접 호출(테스트) 경로에서도 일관되게 검증 — Query 제약 대신 명시 400.
    try:
        months = int(months)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="months 값이 올바르지 않습니다.")
    if not (MIN_MONTHS <= months <= MAX_MONTHS):
        raise HTTPException(
            status_code=400,
            detail=f"months 는 {MIN_MONTHS}~{MAX_MONTHS} 사이여야 합니다.",
        )

    cache_key = f"{user['google_sub']}|{months}"
    cached = _calendar_cache.get(cache_key)
    if cached is not None:
        return cached

    result = await dividend_calendar.build_calendar(
        user["google_sub"],
        months_back=MONTHS_BACK,
        months_forward=months - MONTHS_BACK,
    )
    _calendar_cache.set(cache_key, result)
    return result
