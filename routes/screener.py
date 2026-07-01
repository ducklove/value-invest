"""Value screener HTTP API.

Two endpoints:

* ``GET /api/screener/spec`` — describes available filters/sorts and current
  data coverage. The UI renders its filter controls from this.
* ``POST /api/screener/run`` — runs a screen with a filter spec and returns a
  paginated page of matches. POST (not GET) because the filter body is a JSON
  object that doesn't fit query-string ergonomics, and a screen is a query
  action we don't want cached by intermediaries.

Auth is optional (read-only market data, no per-user secrets), matching the
``routes/analysis.py`` style for market-level endpoints. The screener returns
the same data regardless of who asks.
"""

from __future__ import annotations

from fastapi import APIRouter, Body, Query, Request

from core.errors import AppError
from deps import get_current_user
from services import screener as screener_service

router = APIRouter(prefix="/api/screener", tags=["screener"])

# Note: ScreenerError is an AppError subclass; the AppError handler maps it to
# its status_code (400). We re-export the alias here so the route layer doesn't
# need to know the concrete type — it just lets AppError propagate.
_ScreenerError = screener_service.ScreenerError


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
    await get_current_user(request)
    filters_raw = payload.get("filters") if isinstance(payload, dict) else None
    if filters_raw is None:
        filters_raw = {}
    try:
        return await screener_service.run_screen(
            filters_raw,
            sort_by=sort_by,
            sort_dir=sort_dir,
            limit=limit,
            offset=offset,
        )
    except _ScreenerError:
        # AppError handler already maps this to 400; let it propagate instead
        # of double-wrapping in HTTPException.
        raise
    except AppError:
        raise
