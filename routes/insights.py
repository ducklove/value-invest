"""Research insight board.

This board is intentionally independent from the built-in backtest menu. It is
for manual/external backtest runs, strategy experiments, and short investment
notes that deserve to be kept with their result snapshot.
"""

from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, Body, HTTPException, Request

import cache
import observability
from deps import get_current_user

router = APIRouter(prefix="/api/insights", tags=["insights"])

MAX_TITLE_CHARS = 120
MAX_BODY_CHARS = 8000
MAX_RESULT_JSON_CHARS = 200_000
ALLOWED_VISIBILITY = {"public", "private"}
ALLOWED_SOURCE_TYPES = {"manual", "backtest", "valuation", "portfolio", "memo"}


def _require_user(user: dict | None) -> dict:
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    return user


def _clean_tags(value: Any) -> list[str]:
    if isinstance(value, str):
        raw = [part.strip() for part in value.split(",")]
    elif isinstance(value, list):
        raw = [str(part).strip() for part in value]
    else:
        raw = []
    tags: list[str] = []
    seen: set[str] = set()
    for tag in raw:
        tag = tag[:30]
        if not tag or tag.lower() in seen:
            continue
        seen.add(tag.lower())
        tags.append(tag)
        if len(tags) >= 10:
            break
    return tags


def _json_size(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, default=str))


def _clean_payload(payload: dict) -> dict:
    title = str(payload.get("title") or "").strip()
    if not title:
        raise HTTPException(status_code=400, detail="제목을 입력하세요.")
    if len(title) > MAX_TITLE_CHARS:
        raise HTTPException(status_code=400, detail=f"제목은 {MAX_TITLE_CHARS}자 이하로 입력하세요.")

    insight_md = str(payload.get("insight_md") or payload.get("body") or "").strip()
    if not insight_md:
        raise HTTPException(status_code=400, detail="인사이트 내용을 입력하세요.")
    if len(insight_md) > MAX_BODY_CHARS:
        raise HTTPException(status_code=400, detail=f"내용은 {MAX_BODY_CHARS}자 이하로 입력하세요.")

    visibility = str(payload.get("visibility") or "public").strip().lower()
    if visibility not in ALLOWED_VISIBILITY:
        raise HTTPException(status_code=400, detail="visibility 는 public/private 중 하나여야 합니다.")

    source_type = str(payload.get("source_type") or "manual").strip().lower()
    if source_type not in ALLOWED_SOURCE_TYPES:
        source_type = "manual"

    result_summary = payload.get("result_summary") or {}
    if not isinstance(result_summary, dict):
        raise HTTPException(status_code=400, detail="result_summary 는 object 여야 합니다.")
    if _json_size(result_summary) > 20_000:
        raise HTTPException(status_code=400, detail="result_summary 가 너무 큽니다.")

    result_payload = payload.get("result_payload")
    if result_payload is not None and _json_size(result_payload) > MAX_RESULT_JSON_CHARS:
        raise HTTPException(status_code=400, detail="result_payload 가 너무 큽니다.")

    return {
        "title": title,
        "insight_md": insight_md,
        "visibility": visibility,
        "source_type": source_type,
        "result_summary": result_summary,
        "result_payload": result_payload,
        "tags": _clean_tags(payload.get("tags")),
    }


def _with_permissions(post: dict, user: dict | None) -> dict:
    out = dict(post)
    google_sub = user.get("google_sub") if user else None
    out["can_delete"] = bool(user and (user.get("is_admin") or out.get("google_sub") == google_sub))
    return out


@router.get("")
async def list_insights(request: Request, limit: int = 50):
    user = await get_current_user(request)
    rows = await cache.list_insight_posts(
        viewer_google_sub=user.get("google_sub") if user else None,
        limit=limit,
    )
    return [_with_permissions(row, user) for row in rows]


@router.get("/{post_id}")
async def get_insight(post_id: int, request: Request):
    user = await get_current_user(request)
    post = await cache.get_insight_post(
        post_id,
        viewer_google_sub=user.get("google_sub") if user else None,
    )
    if not post:
        raise HTTPException(status_code=404, detail="인사이트 글을 찾을 수 없습니다.")
    return _with_permissions(post, user)


@router.post("")
async def create_insight(request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    cleaned = _clean_payload(payload or {})
    post = await cache.create_insight_post(
        google_sub=user["google_sub"],
        **cleaned,
    )
    await observability.record_event(
        "insight_board",
        "post_created",
        level="info",
        details={
            "post_id": post.get("id"),
            "actor": user.get("email") or user.get("google_sub"),
            "source_type": cleaned["source_type"],
            "visibility": cleaned["visibility"],
        },
        wait=True,
    )
    return _with_permissions(post, user)


@router.delete("/{post_id}")
async def delete_insight(post_id: int, request: Request):
    user = _require_user(await get_current_user(request))
    deleted = await cache.delete_insight_post(
        post_id,
        google_sub=user["google_sub"],
        is_admin=bool(user.get("is_admin")),
    )
    if not deleted:
        raise HTTPException(status_code=404, detail="삭제할 수 있는 인사이트 글이 없습니다.")
    await observability.record_event(
        "insight_board",
        "post_deleted",
        level="warning",
        details={"post_id": post_id, "actor": user.get("email") or user.get("google_sub")},
        wait=True,
    )
    return {"ok": True, "id": post_id}
