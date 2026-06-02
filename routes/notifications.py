"""User-facing notification settings: channels, Telegram bot linking, alerts.

Auth-gated (`/api/notifications/*`). The Telegram link flow hands the client a
`t.me/<bot>?start=<code>` deep link; the bot poller (services.notifications.
telegram.run_poll_loop) captures the chat_id and marks the channel verified, so
the client just polls `link-status` until connected.
"""

from __future__ import annotations

import logging
import secrets
from datetime import datetime, timedelta

from fastapi import APIRouter, Body, HTTPException, Request

import cache
from deps import get_current_user
from services.notifications import engine, telegram


router = APIRouter(prefix="/api/notifications", tags=["notifications"])
logger = logging.getLogger(__name__)

LINK_TTL_MINUTES = 10


def _require_user(user):
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    return user


# --- Channels ---------------------------------------------------------------

@router.get("/channels")
async def get_channels(request: Request):
    user = _require_user(await get_current_user(request))
    items = await cache.list_notification_channels(user["google_sub"])
    telegram_status = {"connected": False, "enabled": False, "username": None}
    for ch in items:
        if ch["channel"] == "telegram":
            telegram_status = {
                "connected": bool(ch.get("verified")),
                "enabled": bool(ch.get("enabled")),
                "username": (ch.get("config") or {}).get("username"),
            }
    return {"bot_configured": telegram.is_configured(), "telegram": telegram_status}


@router.post("/telegram/link")
async def telegram_link(request: Request):
    user = _require_user(await get_current_user(request))
    if not telegram.is_configured():
        raise HTTPException(status_code=503, detail="텔레그램 봇이 서버에 설정되어 있지 않습니다.")
    bot = await telegram.get_bot_username()
    if not bot:
        raise HTTPException(status_code=503, detail="텔레그램 봇 정보를 확인할 수 없습니다.")
    code = secrets.token_urlsafe(9)
    expires_at = (datetime.now() + timedelta(minutes=LINK_TTL_MINUTES)).isoformat()
    await cache.create_notification_link(code, user["google_sub"], "telegram", expires_at)
    return {
        "code": code,
        "deep_link": f"https://t.me/{bot}?start={code}",
        "bot_username": bot,
        "expires_in_minutes": LINK_TTL_MINUTES,
    }


@router.get("/telegram/link-status")
async def telegram_link_status(request: Request):
    user = _require_user(await get_current_user(request))
    ch = await cache.get_notification_channel(user["google_sub"], "telegram")
    if not ch:
        return {"connected": False, "username": None}
    return {
        "connected": bool(ch.get("verified")),
        "username": (ch.get("config") or {}).get("username"),
    }


@router.put("/channels/telegram")
async def toggle_telegram(request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    enabled = bool(payload.get("enabled", True))
    ok = await cache.set_notification_channel_enabled(user["google_sub"], "telegram", enabled)
    if not ok:
        raise HTTPException(status_code=404, detail="연결된 텔레그램이 없습니다.")
    return {"ok": True, "enabled": enabled}


@router.post("/channels/telegram/test")
async def test_telegram(request: Request):
    user = _require_user(await get_current_user(request))
    ch = await cache.get_notification_channel(user["google_sub"], "telegram")
    if not ch or not ch.get("verified"):
        raise HTTPException(status_code=400, detail="먼저 텔레그램을 연결해주세요.")
    chat_id = (ch.get("config") or {}).get("chat_id")
    ok = await telegram.send_message(
        chat_id, "🔔 Value Compass 테스트 알림입니다. 정상적으로 연결되었습니다."
    )
    if not ok:
        raise HTTPException(status_code=502, detail="텔레그램 전송에 실패했습니다.")
    return {"ok": True}


@router.delete("/telegram")
async def unlink_telegram(request: Request):
    user = _require_user(await get_current_user(request))
    await cache.delete_notification_channel(user["google_sub"], "telegram")
    return {"ok": True}


# --- Alert rules ------------------------------------------------------------

async def _validate_alert_payload(google_sub: str, payload: dict) -> dict:
    alert_type = str(payload.get("alert_type") or "").strip()
    if alert_type not in engine.ALL_ALERT_TYPES:
        raise HTTPException(status_code=400, detail="지원하지 않는 알림 유형입니다.")
    # scope is derived from the type so the two can't disagree.
    scope = "stock" if alert_type in engine.STOCK_ALERT_TYPES else "portfolio"

    try:
        threshold = float(payload.get("threshold"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="임계값을 숫자로 입력해주세요.")

    stock_code = None
    if scope == "stock":
        stock_code = str(payload.get("stock_code") or "").strip()
        if not stock_code:
            raise HTTPException(status_code=400, detail="종목을 선택해주세요.")
        held = {it["stock_code"] for it in await cache.get_portfolio(google_sub)}
        if stock_code not in held:
            raise HTTPException(status_code=400, detail="보유 종목에 대해서만 지정가 알림을 설정할 수 있습니다.")
        if threshold <= 0:
            raise HTTPException(status_code=400, detail="지정가는 0보다 커야 합니다.")
    elif alert_type in ("nav_above", "nav_below") and threshold <= 0:
        raise HTTPException(status_code=400, detail="총평가액 기준은 0보다 커야 합니다.")

    note = str(payload.get("note") or "").strip()[:200]
    enabled = bool(payload.get("enabled", True))
    return {
        "scope": scope,
        "alert_type": alert_type,
        "threshold": threshold,
        "stock_code": stock_code,
        "note": note,
        "enabled": enabled,
    }


@router.get("/alerts")
async def get_alerts(request: Request):
    user = _require_user(await get_current_user(request))
    return await cache.list_portfolio_alerts(user["google_sub"])


@router.post("/alerts")
async def create_alert(request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    rule = await _validate_alert_payload(user["google_sub"], payload)
    alert_id = await cache.create_portfolio_alert(user["google_sub"], **rule)
    return await cache.get_portfolio_alert(user["google_sub"], alert_id)


@router.put("/alerts/{alert_id}")
async def update_alert(alert_id: int, request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    existing = await cache.get_portfolio_alert(user["google_sub"], alert_id)
    if not existing:
        raise HTTPException(status_code=404, detail="알림 규칙을 찾을 수 없습니다.")

    # Pure on/off toggle: skip stock-membership re-validation so disabling a
    # rule whose stock was later removed still works.
    if set(payload.keys()) <= {"enabled"}:
        await cache.update_portfolio_alert(
            user["google_sub"], alert_id, enabled=bool(payload.get("enabled", True))
        )
        return await cache.get_portfolio_alert(user["google_sub"], alert_id)

    merged = dict(existing)
    for key in ("alert_type", "threshold", "stock_code", "note", "enabled"):
        if key in payload:
            merged[key] = payload[key]
    validated = await _validate_alert_payload(user["google_sub"], merged)
    await cache.update_portfolio_alert(user["google_sub"], alert_id, **validated)
    return await cache.get_portfolio_alert(user["google_sub"], alert_id)


@router.delete("/alerts/{alert_id}")
async def delete_alert(alert_id: int, request: Request):
    user = _require_user(await get_current_user(request))
    ok = await cache.delete_portfolio_alert(user["google_sub"], alert_id)
    if not ok:
        raise HTTPException(status_code=404, detail="알림 규칙을 찾을 수 없습니다.")
    return {"ok": True}
