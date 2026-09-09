"""로그인 사용자의 현물 매매 미리보기·원자적 저장·내역 API."""

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, Request

from deps import get_current_user
from domain.portfolio_exchanges import ExchangeCreate, ExchangeInput, ExchangePreview, ExchangeRecord
from domain.portfolio_trades import TradeCreate, TradeInput, TradePreview, TradeRecord
from repositories import portfolio_exchanges as exchanges
from repositories import portfolio_trades as repo

router = APIRouter(prefix="/api/portfolio/trades")


async def _user_id(request: Request) -> str:
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    return user["google_sub"]


@router.post("/preview", response_model=TradePreview | ExchangePreview)
async def preview(request: Request, payload: TradeInput | ExchangeInput) -> dict:
    user = await _user_id(request)
    if isinstance(payload, ExchangeInput):
        return await exchanges.preview_exchange(user, payload)
    return await repo.preview_trade(user, payload)


@router.post("", response_model=TradeRecord | ExchangeRecord)
async def record(request: Request, payload: TradeCreate | ExchangeCreate) -> dict:
    user = await _user_id(request)
    if isinstance(payload, ExchangeCreate):
        return await exchanges.record_exchange(user, payload)
    return await repo.record_trade(user, payload)


@router.get("", response_model=list[TradeRecord | ExchangeRecord])
async def history(request: Request, limit: Annotated[int, Query(ge=1, le=100)] = 20) -> list[dict]:
    return await repo.list_trades(await _user_id(request), limit)
