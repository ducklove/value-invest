"""Household-wide asset and retirement planning API."""

from __future__ import annotations

import math
import re
import uuid

from fastapi import APIRouter, Body, HTTPException, Request

from deps import get_current_user
from repositories import household_assets as household_repo
from services.household_assets import (
    ASSET_CATEGORIES,
    DEFAULT_RETIREMENT_PROFILE,
    OWNER_LABELS,
    reference_payload,
)

router = APIRouter(prefix="/api/household-assets", tags=["household-assets"])

MAX_ITEMS = 100
MAX_AMOUNT = 10_000_000_000_000_000
_ASSET_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,80}$")


def _require_user(user: dict | None) -> dict:
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    return user


def _bounded_number(value, *, field: str, minimum: float, maximum: float, integer: bool = False):
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail=f"{field} 값이 올바르지 않습니다.")
    if not math.isfinite(number) or number < minimum or number > maximum:
        raise HTTPException(status_code=400, detail=f"{field} 값은 {minimum}~{maximum} 범위여야 합니다.")
    return int(round(number)) if integer else number


def normalize_payload(payload: dict) -> tuple[list[dict], dict]:
    raw_items = payload.get("items") or []
    if not isinstance(raw_items, list):
        raise HTTPException(status_code=400, detail="items 는 배열이어야 합니다.")
    if len(raw_items) > MAX_ITEMS:
        raise HTTPException(status_code=400, detail=f"자산 항목은 {MAX_ITEMS}개까지 저장할 수 있습니다.")

    seen_ids: set[str] = set()
    items: list[dict] = []
    for raw in raw_items:
        if not isinstance(raw, dict):
            raise HTTPException(status_code=400, detail="자산 항목 형식이 올바르지 않습니다.")
        category = str(raw.get("category") or "").strip()
        if category not in ASSET_CATEGORIES:
            raise HTTPException(status_code=400, detail="지원하지 않는 자산 분류입니다.")
        name = str(raw.get("name") or "").strip()
        if not name or len(name) > 60:
            raise HTTPException(status_code=400, detail="자산 이름은 1~60자로 입력해 주세요.")
        owner = str(raw.get("owner") or "household").strip()
        if owner not in OWNER_LABELS:
            raise HTTPException(status_code=400, detail="지원하지 않는 소유 구분입니다.")
        amount = _bounded_number(
            raw.get("amount", 0), field="자산 금액", minimum=0, maximum=MAX_AMOUNT, integer=True
        )
        asset_id = str(raw.get("asset_id") or raw.get("id") or "").strip()
        if not _ASSET_ID_RE.fullmatch(asset_id) or asset_id in seen_ids:
            asset_id = f"ha-{uuid.uuid4().hex}"
        seen_ids.add(asset_id)
        items.append({
            "asset_id": asset_id,
            "category": category,
            "name": name,
            "owner": owner,
            "amount": amount,
            "retirement_eligible": bool(raw.get("retirement_eligible")),
        })

    raw_profile = payload.get("retirement") or {}
    if not isinstance(raw_profile, dict):
        raise HTTPException(status_code=400, detail="retirement 형식이 올바르지 않습니다.")
    profile = dict(DEFAULT_RETIREMENT_PROFILE)
    household_type = str(raw_profile.get("household_type") or profile["household_type"])
    if household_type not in {"single", "couple"}:
        raise HTTPException(status_code=400, detail="가구 유형은 single 또는 couple 이어야 합니다.")
    profile["household_type"] = household_type
    profile["current_age"] = _bounded_number(
        raw_profile.get("current_age"), field="현재 나이", minimum=18, maximum=100, integer=True
    )
    for field, label, minimum, maximum in (
        ("retirement_age", "은퇴 나이", 40, 90),
        ("plan_to_age", "계획 수명", 50, 110),
    ):
        value = raw_profile.get(field, profile[field])
        profile[field] = _bounded_number(value, field=label, minimum=minimum, maximum=maximum, integer=True)
    if profile["plan_to_age"] <= profile["retirement_age"]:
        raise HTTPException(status_code=400, detail="계획 수명은 은퇴 나이보다 커야 합니다.")
    if profile["current_age"] is not None and profile["retirement_age"] < profile["current_age"]:
        profile["retirement_age"] = profile["current_age"]
    if profile["plan_to_age"] <= profile["retirement_age"]:
        raise HTTPException(status_code=400, detail="계획 수명은 은퇴 나이보다 커야 합니다.")

    for field, label in (
        ("monthly_spending", "월 노후생활비"),
        ("monthly_public_pension", "월 공적연금"),
        ("monthly_other_income", "월 기타소득"),
        ("monthly_contribution", "월 추가저축"),
    ):
        profile[field] = _bounded_number(
            raw_profile.get(field, profile[field]), field=label, minimum=0,
            maximum=1_000_000_000, integer=True,
        )
    for field, label in (("annual_return_pct", "연 수익률"), ("inflation_pct", "물가상승률")):
        profile[field] = _bounded_number(
            raw_profile.get(field, profile[field]), field=label, minimum=-10, maximum=30
        )
    return items, profile


@router.get("")
async def get_household_assets(request: Request):
    user = _require_user(await get_current_user(request))
    saved = await household_repo.get_household_plan(user["google_sub"])
    return {**saved, **reference_payload()}


@router.put("")
async def save_household_assets(request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    items, retirement = normalize_payload(payload if isinstance(payload, dict) else {})
    saved = await household_repo.replace_household_plan(user["google_sub"], items, retirement)
    return {**saved, **reference_payload()}
