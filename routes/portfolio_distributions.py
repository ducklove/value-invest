"""배당 분배금의 미리보기·출금·누계 API."""

from typing import Annotated

from fastapi import APIRouter, Query, Request

from deps import require_user_id as _user_id
from domain.portfolio_distributions import (
    DistributionCreate,
    DistributionInput,
    DistributionPreview,
    DistributionRecord,
)
from repositories import portfolio_distributions as repo

router = APIRouter(prefix="/api/portfolio/distributions")


@router.get("/balances")
async def balances(request: Request) -> list[dict]:
    return await repo.balances(await _user_id(request))


@router.post("/preview", response_model=DistributionPreview)
async def preview(request: Request, payload: DistributionInput) -> dict:
    return await repo.preview_distribution(await _user_id(request), payload)


@router.post("", response_model=DistributionRecord)
async def record(request: Request, payload: DistributionCreate) -> dict:
    return await repo.record_distribution(await _user_id(request), payload)


@router.get("", response_model=list[DistributionRecord])
async def history(request: Request, limit: Annotated[int, Query(ge=1, le=100)] = 20) -> list[dict]:
    return await repo.list_distributions(await _user_id(request), limit)
