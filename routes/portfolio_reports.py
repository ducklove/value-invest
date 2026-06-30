"""Monthly/annual portfolio period report APIs."""

from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query, Request

from deps import get_current_user
from services.portfolio import period_reports

router = APIRouter()


def _require_user(user):
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    return user


@router.get("/api/portfolio/period-reports/periods")
async def get_period_report_periods(request: Request):
    user = _require_user(await get_current_user(request))
    return await period_reports.available_periods(user["google_sub"])


@router.get("/api/portfolio/period-reports")
async def list_period_reports(
    request: Request,
    period_type: str | None = Query(None),
    limit: int = Query(36, ge=1, le=120),
):
    user = _require_user(await get_current_user(request))
    try:
        return await period_reports.list_saved_period_reports(
            user["google_sub"],
            period_type=period_type,
            limit=limit,
        )
    except period_reports.PeriodReportError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/api/portfolio/period-reports/{period_type}/{period_key}")
async def get_period_report(period_type: str, period_key: str, request: Request):
    user = _require_user(await get_current_user(request))
    try:
        report = await period_reports.get_saved_period_report(
            user["google_sub"],
            period_type,
            period_key,
        )
    except period_reports.PeriodReportError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    if not report:
        raise HTTPException(status_code=404, detail="저장된 기간 보고서가 없습니다.")
    return report


@router.post("/api/portfolio/period-reports/generate")
async def generate_period_report(request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    try:
        return await period_reports.generate_and_save_period_report(
            user["google_sub"],
            str((payload or {}).get("period_type") or ""),
            str((payload or {}).get("period_key") or ""),
        )
    except period_reports.PeriodReportError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
