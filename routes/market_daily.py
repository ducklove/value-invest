from __future__ import annotations

import os
from datetime import datetime

from fastapi import APIRouter, Query, Request

from repositories import market_brief as market_brief_repo
import market_daily
from deps import get_current_user


router = APIRouter()

MARKET_DAILY_CACHE_TTL_MINUTES = int(os.environ.get("MARKET_DAILY_CACHE_TTL_MINUTES", "30"))

# 시황은 시장 전체 기준(포트폴리오 무관)이라 사용자별로 달라지지 않는다 —
# 모든 사용자가 브리프 하나를 공유한다(생성 비용·실패 확률 모두 감소).
_BRIEF_CACHE_SUB = "public"


@router.get("/api/market/daily-brief")
async def get_daily_market_brief(
    request: Request,
    refresh: bool = Query(False),
):
    user = await get_current_user(request)
    brief_date = market_daily._today_iso()

    if not refresh:
        cached = await market_brief_repo.get_daily_market_brief(
            _BRIEF_CACHE_SUB,
            brief_date,
            max_age_minutes=MARKET_DAILY_CACHE_TTL_MINUTES,
        )
        if cached:
            cached["cached"] = True
            return cached

    generated = await market_daily.build_daily_market_brief(
        google_sub=user["google_sub"] if user else None,  # AI 비용 귀속용
        brief_date=brief_date,
    )
    if not generated.get("llm_ok"):
        # 실패한 생성은 저장하지 않는다 — 저장하면 TTL 동안 모든 요청이 실패
        # 본문을 받는다. 휘발로 반환해 다음 요청이 곧바로 재시도하게 한다.
        now = datetime.now().isoformat()
        return {
            "google_sub": _BRIEF_CACHE_SUB,
            "brief_date": generated["brief_date"],
            "source_hash": generated["source_hash"],
            "payload": generated["payload"],
            "markdown": generated["markdown"],
            "model": generated.get("model"),
            "tokens_in": generated.get("tokens_in"),
            "tokens_out": generated.get("tokens_out"),
            "cost_usd": generated.get("cost_usd"),
            "created_at": now,
            "updated_at": now,
            "cached": False,
            "llm_ok": False,
            "error": generated.get("error"),
        }
    saved = await market_brief_repo.save_daily_market_brief(
        google_sub=_BRIEF_CACHE_SUB,
        brief_date=generated["brief_date"],
        source_hash=generated["source_hash"],
        payload=generated["payload"],
        markdown=generated["markdown"],
        model=generated.get("model"),
        tokens_in=generated.get("tokens_in"),
        tokens_out=generated.get("tokens_out"),
        cost_usd=generated.get("cost_usd"),
    )
    saved["cached"] = False
    saved["llm_ok"] = True
    saved["error"] = None
    return saved


@router.get("/api/market/tape")
async def get_market_tape(
    request: Request,
    refresh: bool = Query(False),
):
    user = await get_current_user(request)
    return await market_daily.build_market_tape(
        google_sub=user["google_sub"] if user else None,
        refresh=refresh,
    )
