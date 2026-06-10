"""리밸런싱 도우미 API (로드맵 신규 기능 ① — 목표 비중 + 드리프트 보고서).

GET    /api/portfolio/rebalance            → 목표 + 현재 비중 + 드리프트/조정 제안
PUT    /api/portfolio/rebalance/targets    → 목표 전체 목록 교체(upsert)
DELETE /api/portfolio/rebalance/targets    → scope+key 단건 삭제 (쿼리 파라미터 —
                                             그룹명은 한글이라 경로보다 안전)

현재 비중은 최근 일별 종목 스냅샷 기준(services.portfolio.rebalance 참조) —
응답의 ``as_of`` 가 그 날짜다.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Body, HTTPException, Query, Request

from deps import get_current_user
from repositories import rebalance_targets as targets_repo
from services.portfolio import rebalance as rebalance_service

logger = logging.getLogger(__name__)
router = APIRouter()

# 그룹명/종목코드 키 길이 상한 — portfolio_groups 의 그룹명도 이 안에 든다.
_MAX_KEY_LEN = 100
_MAX_TARGETS = 200


def _require_user(user):
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    return user


def _validate_target(raw: dict, index: int) -> dict:
    """단일 목표 항목 검증 → {scope, key, target_weight_pct, tolerance_pct}."""
    where = f"targets[{index}]"
    if not isinstance(raw, dict):
        raise HTTPException(status_code=400, detail=f"{where}: 객체 형식이어야 합니다.")
    scope = str(raw.get("scope") or "").strip()
    if scope not in rebalance_service.VALID_SCOPES:
        raise HTTPException(
            status_code=400,
            detail=f"{where}: scope 는 'stock' 또는 'group' 이어야 합니다.",
        )
    key = str(raw.get("key") or "").strip()
    if not key or len(key) > _MAX_KEY_LEN:
        raise HTTPException(
            status_code=400,
            detail=f"{where}: key(종목코드/그룹명)를 입력해주세요.",
        )
    try:
        target_pct = float(raw.get("target_weight_pct"))
    except (TypeError, ValueError):
        raise HTTPException(
            status_code=400, detail=f"{where}: 목표 비중을 숫자로 입력해주세요."
        )
    if not 0 < target_pct <= 100:
        raise HTTPException(
            status_code=400,
            detail=f"{where}: 목표 비중은 0 초과 100 이하(%)여야 합니다.",
        )
    tolerance_raw = raw.get("tolerance_pct")
    if tolerance_raw in (None, ""):
        tolerance = targets_repo.DEFAULT_TOLERANCE_PCT
    else:
        try:
            tolerance = float(tolerance_raw)
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=400, detail=f"{where}: 허용 오차를 숫자로 입력해주세요."
            )
    if not 0 < tolerance <= 100:
        raise HTTPException(
            status_code=400,
            detail=f"{where}: 허용 오차는 0 초과 100 이하(%p)여야 합니다.",
        )
    return {
        "scope": scope,
        "key": key,
        "target_weight_pct": target_pct,
        "tolerance_pct": tolerance,
    }


def _validate_targets_payload(payload: dict) -> list[dict]:
    raw_targets = payload.get("targets")
    if not isinstance(raw_targets, list):
        raise HTTPException(status_code=400, detail="targets 배열이 필요합니다.")
    if len(raw_targets) > _MAX_TARGETS:
        raise HTTPException(
            status_code=400, detail=f"목표는 최대 {_MAX_TARGETS}개까지 설정할 수 있습니다."
        )
    targets = [_validate_target(raw, i) for i, raw in enumerate(raw_targets)]

    # (scope, key) 중복 금지 + scope 별 목표 합 100% 초과 금지.
    seen: set[tuple[str, str]] = set()
    sums: dict[str, float] = {}
    for t in targets:
        pair = (t["scope"], t["key"])
        if pair in seen:
            raise HTTPException(
                status_code=400,
                detail=f"중복된 목표가 있습니다: {t['scope']} / {t['key']}",
            )
        seen.add(pair)
        sums[t["scope"]] = sums.get(t["scope"], 0.0) + t["target_weight_pct"]
    for scope, total in sums.items():
        if total > 100.0 + 1e-9:
            label = "종목" if scope == "stock" else "그룹"
            raise HTTPException(
                status_code=400,
                detail=f"{label} 목표 비중 합이 100%를 넘습니다 ({total:g}%).",
            )
    return targets


@router.get("/api/portfolio/rebalance")
async def get_rebalance_report(request: Request):
    """목표 비중 + 드리프트 보고서 — 목표가 없으면 빈 items 로 응답."""
    user = _require_user(await get_current_user(request))
    return await rebalance_service.compute_rebalance(user["google_sub"])


@router.put("/api/portfolio/rebalance/targets")
async def put_rebalance_targets(request: Request, payload: dict = Body(...)):
    """목표 전체 목록 교체 — 빈 배열이면 전부 삭제."""
    user = _require_user(await get_current_user(request))
    targets = _validate_targets_payload(payload)
    saved = await targets_repo.replace_all_targets(user["google_sub"], targets)
    logger.info(
        "rebalance targets replaced for %s: %d items", user["google_sub"][:8], len(saved)
    )
    return {"ok": True, "targets": saved}


@router.delete("/api/portfolio/rebalance/targets")
async def delete_rebalance_target(
    request: Request,
    scope: str = Query(...),
    key: str = Query(...),
):
    user = _require_user(await get_current_user(request))
    scope = scope.strip()
    key = key.strip()
    if scope not in rebalance_service.VALID_SCOPES or not key:
        raise HTTPException(status_code=400, detail="scope/key 가 올바르지 않습니다.")
    ok = await targets_repo.delete_target(user["google_sub"], scope, key)
    if not ok:
        raise HTTPException(status_code=404, detail="해당 목표를 찾을 수 없습니다.")
    return {"ok": True}
