import logging

from fastapi import APIRouter, Body, HTTPException, Request

import dart_report_review
from deps import get_current_user


logger = logging.getLogger(__name__)
router = APIRouter()


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
    """Admin-only manual generation path for the latest DART filing review."""
    _require_json(request)
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    if not user.get("is_admin"):
        raise HTTPException(status_code=403, detail="관리자 권한이 필요합니다.")

    try:
        force = bool((payload or {}).get("force", True))
        review = await dart_report_review.generate_review(
            stock_code,
            google_sub=user.get("google_sub"),
            force=force,
        )
        result = {
            "stock_code": stock_code,
            "status": "ready",
            "can_generate": True,
            "authenticated": True,
            "admin": True,
            "generated": not bool(review.get("cached")),
            "force": force,
            "review": review,
            "message": "DART AI 리뷰를 생성했습니다." if not review.get("cached") else "캐시된 DART AI 리뷰를 반환했습니다.",
        }
        return result
    except Exception as exc:
        raise _to_http_error(exc) from exc
