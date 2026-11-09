"""포트폴리오 X-Ray API — 입력 없이 계산하는 실제 노출.

GET /api/portfolio/exposure[?account_id=…]

선택 계좌의 보유 수량(없으면 전체 합산)을 services.portfolio.exposure_service 로 넘긴다.
없거나 남의 계좌면 ``AccountError``(400)가 ``core/errors`` 핸들러로 응답된다.
"""

from __future__ import annotations

from fastapi import APIRouter, Query, Request

from deps import get_current_user
from deps import require_user as _require_user
from services.portfolio import exposure_service

router = APIRouter()


@router.get("/api/portfolio/exposure")
async def get_portfolio_exposure(
    request: Request,
    account_id: str | None = Query(None, max_length=64),
):
    user = _require_user(await get_current_user(request))
    return await exposure_service.get_exposure_report(user["google_sub"], (account_id or "").strip() or None)
