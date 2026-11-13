"""Value screener HTTP API.

* ``GET /api/screener/spec`` — describes available filters, history
  conditions and sorts plus current data coverage. The UI renders its
  controls from this.
* ``POST /api/screener/run`` — runs a screen and returns a paginated page of
  matches. POST (not GET) because the filter body is a JSON object that
  doesn't fit query-string ergonomics, and a screen is a query action we
  don't want cached by intermediaries.
* ``GET /api/screener/watchlist`` · ``PUT /api/screener/watchlist/{code}`` —
  the signed-in user's 관심종목. Storage is the same
  ``user_stock_preferences.is_starred`` flag the analysis screen uses, so a
  stock starred here shows up in the analysis sidebar's 관심종목 tab and
  vice versa.

Screening itself is market data and works signed out; only the watchlist
(reading it, toggling it, "관심종목만 보기") needs a session.
"""

from __future__ import annotations

import re

from fastapi import APIRouter, Body, HTTPException, Query, Request

from deps import get_current_user, require_user_id
from repositories import user_stocks as user_stocks_repo
from services import screener as screener_service

router = APIRouter(prefix="/api/screener", tags=["screener"])

# 국내 상장 종목 코드(숫자 6자리, 신형 코드는 영문 대문자 포함).
_STOCK_CODE_RE = re.compile(r"^[0-9A-Z]{6}$")


@router.get("/spec")
async def get_spec(request: Request):
    # get_current_user is called purely to keep the session warm (auth-aware
    # rendering on the client); the spec itself is not user-scoped.
    await get_current_user(request)
    return await screener_service.get_filter_specs()


@router.post("/run")
async def run_screen(
    request: Request,
    payload: dict = Body(default={}),
    sort_by: str = Query(screener_service.DEFAULT_SORT),
    sort_dir: str = Query(screener_service.DEFAULT_SORT_DIR),
    limit: int = Query(50, ge=1, le=screener_service.MAX_LIMIT),
    offset: int = Query(0, ge=0),
):
    user = await get_current_user(request)
    if not isinstance(payload, dict):
        payload = {}
    filters_raw = payload.get("filters")
    if filters_raw is None:
        filters_raw = {}
    watchlist = None
    if payload.get("watchlist_only"):
        if not user:
            raise HTTPException(status_code=401, detail="관심종목은 로그인 후 사용할 수 있습니다.")
        watchlist = await user_stocks_repo.get_starred_stock_codes(user["google_sub"])
    # ScreenerError(AppError, 400)·ExternalServiceError 는 AppError 핸들러가 상태 코드로 바꾼다.
    return await screener_service.run_screen(
        filters_raw,
        conditions_raw=payload.get("conditions"),
        watchlist=watchlist,
        sort_by=sort_by,
        sort_dir=sort_dir,
        limit=limit,
        offset=offset,
    )


@router.get("/watchlist")
async def get_watchlist(request: Request):
    """로그인 사용자의 관심종목 코드. 비로그인은 401 대신 빈 목록 — 화면이 별 표시만 끈다."""
    user = await get_current_user(request)
    if not user:
        return {"authenticated": False, "codes": []}
    codes = await user_stocks_repo.get_starred_stock_codes(user["google_sub"])
    return {"authenticated": True, "codes": codes}


@router.put("/watchlist/{stock_code}")
async def set_watchlist(stock_code: str, request: Request, payload: dict = Body(...)):
    google_sub = await require_user_id(request)
    code = stock_code.strip().upper()
    if not _STOCK_CODE_RE.match(code):
        raise HTTPException(status_code=400, detail="종목 코드가 올바르지 않습니다.")
    starred = payload.get("starred") if isinstance(payload, dict) else None
    if not isinstance(starred, bool):
        raise HTTPException(status_code=400, detail="starred 는 true 또는 false 여야 합니다.")
    preference = await user_stocks_repo.save_user_stock_preference(google_sub, code, is_starred=starred)
    return {"stock_code": code, "starred": bool(preference["is_starred"])}
