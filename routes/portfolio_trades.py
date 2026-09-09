"""로그인 사용자의 현물 매매 미리보기·원자적 저장·내역 API."""

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, Request

from deps import get_current_user
from domain.portfolio_trades import TradeCreate, TradeInput, TradePreview, TradeRecord
from repositories import portfolio_trades as repo

router = APIRouter(prefix="/api/portfolio/trades")


async def _user_id(request: Request) -> str:
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    return user["google_sub"]


@router.post("/preview", response_model=TradePreview)
async def preview(request: Request, payload: TradeInput) -> dict:
    return await repo.preview_trade(await _user_id(request), payload)


@router.post("", response_model=TradeRecord)
async def record(request: Request, payload: TradeCreate) -> dict:
    return await repo.record_trade(await _user_id(request), payload)


@router.get("", response_model=list[TradeRecord])
async def history(request: Request, limit: Annotated[int, Query(ge=1, le=100)] = 20) -> list[dict]:
    return await repo.list_trades(await _user_id(request), limit)
