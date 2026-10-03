"""기존 배당 수취 기록과 누계의 조회 API. 수동 입력 기능은 종료했다."""

from typing import Annotated

from fastapi import APIRouter, Query, Request

from deps import require_user_id as _user_id
from domain.dividend_receipts import DividendRecord
from repositories import dividend_receipts as repo

router = APIRouter(prefix="/api/portfolio/dividend-receipts")


@router.get("/totals")
async def totals(request: Request) -> list[dict]:
    return await repo.receipt_totals(await _user_id(request))


@router.get("", response_model=list[DividendRecord])
async def history(request: Request, limit: Annotated[int, Query(ge=1, le=100)] = 20) -> list[dict]:
    return await repo.list_receipts(await _user_id(request), limit)
