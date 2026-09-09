"""배당 스케줄 입력 준비와 실제 배당금 수취 API."""

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, Request

from deps import get_current_user
from domain.dividend_receipts import DividendCreate, DividendInput, DividendPreview, DividendRecord
from repositories import dividend_receipts as repo
from services import dividend_calendar

router = APIRouter(prefix="/api/portfolio/dividend-receipts")


async def _user_id(request: Request) -> str:
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    return user["google_sub"]


@router.get("/candidates")
async def candidates(request: Request) -> dict:
    user = await _user_id(request)
    calendar = await dividend_calendar.build_calendar(user, months_back=12, months_forward=3)
    received = await repo.received_source_keys(user)
    events = []
    for event in calendar["events"]:
        key = f"{event['stock_code']}:{event['type']}:{event['date']}"
        events.append({**event, "source_key": key, "received": key in received})
    return {"as_of": calendar["as_of"], "events": events}


@router.post("/preview", response_model=DividendPreview)
async def preview(request: Request, payload: DividendInput) -> dict:
    return await repo.preview_dividend(await _user_id(request), payload)


@router.get("/totals")
async def totals(request: Request) -> list[dict]:
    return await repo.receipt_totals(await _user_id(request))


@router.post("", response_model=DividendRecord)
async def record(request: Request, payload: DividendCreate) -> dict:
    return await repo.record_dividend(await _user_id(request), payload)


@router.get("", response_model=list[DividendRecord])
async def history(request: Request, limit: Annotated[int, Query(ge=1, le=100)] = 20) -> list[dict]:
    return await repo.list_receipts(await _user_id(request), limit)
