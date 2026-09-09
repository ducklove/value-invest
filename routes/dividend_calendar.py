"""배당 캘린더 API (로드맵 신규 기능 ④).

GET /api/portfolio/dividend-calendar?months=12

공시와 배당락 이력은 종목별 DB 캐시를 사용한다. 현재 보유 수량과 오늘
날짜는 요청마다 반영하여 매매 뒤 합계나 월 경계가 오래 남지 않게 한다.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query, Request

from deps import get_current_user
from services import dividend_calendar

logger = logging.getLogger(__name__)
router = APIRouter()

# 과거 구간은 2개월 고정 — '최근 받은 배당' 맥락용. months 는 앞으로 볼 개월 수.
MONTHS_BACK = 2
MIN_MONTHS, MAX_MONTHS = 3, 24



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

    result = await dividend_calendar.build_calendar(
        user["google_sub"],
        months_back=MONTHS_BACK,
        months_forward=months - MONTHS_BACK,
    )
    return result
