import asyncio
import logging

from fastapi import APIRouter, Body, HTTPException, Request

import dart_report_review
from deps import get_current_user


logger = logging.getLogger(__name__)
router = APIRouter()

_GENERATION_LOCKS: dict[str, asyncio.Lock] = {}
_LOCKS_GUARD = asyncio.Lock()


async def _lock_for(stock_code: str) -> asyncio.Lock:
    async with _LOCKS_GUARD:
        lock = _GENERATION_LOCKS.get(stock_code)
        if lock is None:
            lock = asyncio.Lock()
            _GENERATION_LOCKS[stock_code] = lock
        return lock


def _require_json(request: Request):
    content_type = request.headers.get("content-type", "")
    if not content_type.startswith("application/json"):
        raise HTTPException(status_code=415, detail="JSON 요청만 허용됩니다.")


def _to_http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, dart_report_review.DartReportReviewError):
        return HTTPException(status_code=400, detail=str(exc))
    logger.exception("DART report review failed")
    return HTTPException(status_code=500, detail="DART 보고서 리뷰 처리 중 오류가 발생했습니다.")


@router.get("/api/analysis/{stock_code}/filing-review")
async def get_filing_review(stock_code: str, request: Request):
    """Return cached DART filing review status without spending AI tokens."""
    try:
        result = await dart_report_review.latest_review_status(stock_code)
        user = await get_current_user(request)
        result["authenticated"] = bool(user)
        return result
    except Exception as exc:
        raise _to_http_error(exc) from exc


@router.post("/api/analysis/{stock_code}/filing-review")
async def create_filing_review(
    stock_code: str,
    request: Request,
    payload: dict = Body(default={}),
):
    """Generate or refresh an AI review for the latest DART periodic filing."""
    _require_json(request)
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    force = bool((payload or {}).get("force"))
    lock = await _lock_for(stock_code)
    async with lock:
        try:
            review = await dart_report_review.generate_review(
                stock_code,
                google_sub=user.get("google_sub"),
                force=force,
            )
            return {
                "stock_code": stock_code,
                "status": "ready",
                "authenticated": True,
                "review": review,
            }
        except Exception as exc:
            raise _to_http_error(exc) from exc
