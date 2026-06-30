"""Portfolio action board API."""

from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Request

from deps import get_current_user
from repositories import action_reviews
from services.portfolio import action_board

router = APIRouter()

MAX_ACTION_KEY_LENGTH = 180
MAX_NOTE_LENGTH = 500


def _require_user(user):
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    return user


def _validate_action_key(raw: str) -> str:
    key = str(raw or "").strip()
    if not key or len(key) > MAX_ACTION_KEY_LENGTH:
        raise HTTPException(status_code=400, detail="action_key 값이 올바르지 않습니다.")
    return key


@router.get("/api/portfolio/action-board")
async def get_action_board(request: Request):
    user = _require_user(await get_current_user(request))
    return await action_board.build_action_board(user["google_sub"])


@router.put("/api/portfolio/action-board/queue/{action_key}")
async def set_action_review(action_key: str, request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    key = _validate_action_key(action_key)
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="요청 본문이 올바르지 않습니다.")
    status = str(payload.get("status") or "").strip().lower()
    if status not in action_reviews.VALID_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"status 는 {', '.join(action_reviews.VALID_STATUSES)} 중 하나여야 합니다.",
        )
    note = payload.get("note")
    if note is not None:
        note = str(note).strip()
        if len(note) > MAX_NOTE_LENGTH:
            raise HTTPException(status_code=400, detail=f"note 는 {MAX_NOTE_LENGTH}자 이하여야 합니다.")
    review = await action_reviews.set_review_status(
        user["google_sub"],
        key,
        status,
        note=note or None,
    )
    return {"ok": True, "review": review}
