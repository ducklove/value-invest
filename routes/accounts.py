"""Portfolio accounts API — multi-account phase 1.

CRUD for a user's accounts (일반계좌 / ISA / 퇴직연금 / 배우자 …). All
endpoints are user-scoped via ``get_current_user``. The account layer is
additive: phase 1 introduces it alongside the existing single-portfolio
behavior, so a user with one default account sees no change until they create
more.

Endpoints (all prefix ``/api/portfolio/accounts``):

* ``GET    /``              — list accounts
* ``POST   /``              — create account
* ``PATCH  /{account_id}``  — update name/type
* ``DELETE /{account_id}``  — delete (re-parents holdings to default)
* ``PUT    /reorder``       — set sort order from id list
"""

from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Request

from core.errors import AppError
from deps import get_current_user
from repositories import accounts as accounts_repo

router = APIRouter(prefix="/api/portfolio/accounts", tags=["accounts"])

MAX_REORDER_IDS = 20


def _require_user(user: dict | None) -> dict:
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    return user


@router.get("")
async def list_accounts(request: Request):
    user = _require_user(await get_current_user(request))
    return await accounts_repo.list_accounts(user["google_sub"])


@router.post("")
async def create_account(request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    try:
        return await accounts_repo.create_account(
            user["google_sub"],
            name=payload.get("name", ""),
            type=payload.get("type", "general"),
            sort_order=payload.get("sort_order"),
        )
    except accounts_repo.AccountError:
        raise
    except AppError:
        raise


@router.patch("/{account_id}")
async def update_account(account_id: str, request: Request, payload: dict = Body(default={})):
    user = _require_user(await get_current_user(request))
    try:
        return await accounts_repo.update_account(
            user["google_sub"],
            account_id,
            name=payload.get("name"),
            type=payload.get("type"),
        )
    except accounts_repo.AccountError:
        raise
    except AppError:
        raise


@router.delete("/{account_id}")
async def delete_account(account_id: str, request: Request):
    user = _require_user(await get_current_user(request))
    try:
        await accounts_repo.delete_account(user["google_sub"], account_id)
    except accounts_repo.AccountError:
        raise
    except AppError:
        raise
    return {"ok": True, "account_id": account_id}


@router.put("/reorder")
async def reorder_accounts(request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    raw_ids = payload.get("account_ids") or []
    if not isinstance(raw_ids, list):
        raise HTTPException(status_code=400, detail="account_ids 는 배열이어야 합니다.")
    if len(raw_ids) > MAX_REORDER_IDS:
        raise HTTPException(status_code=400, detail=f"account_ids 는 {MAX_REORDER_IDS}개 이하여야 합니다.")
    ordered_ids = [str(x) for x in raw_ids]
    return await accounts_repo.reorder_accounts(user["google_sub"], ordered_ids)
