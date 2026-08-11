"""ePaper 기기(ESP32-S3 PhotoPainter)가 폴링하는 읽기 전용 엔드포인트.

브라우저 세션이 없는 기기라서 Google OAuth 를 쓸 수 없다. ``routes/internal.py``
와 같은 공유 토큰 방식을 쓰되, 내부 배치와 달리 이 엔드포인트는 loopback 이
아닌 LAN 의 기기가 호출하므로 **loopback 예외를 두지 않는다** — 토큰이 설정돼
있지 않으면 아예 비활성(503)이다.

대상 사용자는 ``DEVICE_USER_EMAIL`` 로 고정한다. 기기는 자기가 누구인지 말할
방법이 없고, 토큰 하나로 아무 사용자나 조회할 수 있게 두는 것은 위험하다.
"""
from __future__ import annotations

import hmac
import logging
import os
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, Query, Request

from repositories import users as users_repo
from services.portfolio import device_summary

router = APIRouter(prefix="/api/device", include_in_schema=False)
logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))


def _require_device_token(request: Request) -> None:
    expected = os.getenv("DEVICE_API_TOKEN", "").strip()
    if not expected:
        # 토큰 미설정 = 기능 자체가 꺼진 상태. 열어두고 loopback 으로
        # 봐주는 internal.py 와 달리 여기서는 무조건 닫는다.
        raise HTTPException(status_code=503, detail="device API is not configured")
    provided = (request.headers.get("x-device-token") or "").strip()
    if not hmac.compare_digest(provided, expected):
        logger.warning("device endpoint rejected missing/invalid token")
        raise HTTPException(status_code=403, detail="device token required")


async def _resolve_device_user() -> dict:
    email = os.getenv("DEVICE_USER_EMAIL", "").strip()
    if not email:
        raise HTTPException(status_code=503, detail="DEVICE_USER_EMAIL is not configured")
    user = await users_repo.get_user_by_email(email)
    if not user:
        raise HTTPException(status_code=404, detail="device user not found")
    return user


@router.get("/portfolio")
async def device_portfolio(
    request: Request,
    top: int = Query(device_summary.DEFAULT_TOP_N, ge=1, le=30),
) -> dict:
    """기기 화면 한 장 분량의 포트폴리오 요약."""
    _require_device_token(request)
    user = await _resolve_device_user()
    summary = await device_summary.build_summary(user["google_sub"], top_n=top)
    summary["generated_at"] = datetime.now(KST).isoformat(timespec="seconds")
    return summary
