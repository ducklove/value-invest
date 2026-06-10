from __future__ import annotations

import os

from fastapi import APIRouter, Query, Request

from repositories import market_brief as market_brief_repo
import market_daily
from deps import get_current_user


router = APIRouter()

MARKET_DAILY_CACHE_TTL_MINUTES = int(os.environ.get("MARKET_DAILY_CACHE_TTL_MINUTES", "30"))


@router.get("/api/market/daily-brief")
async def get_daily_market_brief(
    request: Request,
    refresh: bool = Query(False),
):
    user = await get_current_user(request)
    google_sub = user["google_sub"] if user else "public"
    brief_date = market_daily._today_iso()

    if not refresh:
        cached = await market_brief_repo.get_daily_market_brief(
            google_sub,
            brief_date,
            max_age_minutes=MARKET_DAILY_CACHE_TTL_MINUTES,
        )
        if cached:
            cached["cached"] = True
            return cached

    generated = await market_daily.build_daily_market_brief(
        google_sub=user["google_sub"] if user else None,
        brief_date=brief_date,
    )
    saved = await market_brief_repo.save_daily_market_brief(
        google_sub=google_sub,
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
    saved["llm_ok"] = generated.get("llm_ok")
    saved["error"] = generated.get("error")
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
