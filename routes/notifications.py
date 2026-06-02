"""User-facing notification settings: per-user channel registration + alerts.

Auth-gated (`/api/notifications/*`). Credentials are per-user:
* Telegram — the user registers their own bot token (from @BotFather) and a
  chat_id (auto-detected via getUpdates after they message their bot, or typed).
* Kakao    — the user registers their own app REST key and OAuths against it;
  since they're the app admin, "나에게 보내기" works without business review.

Env tokens (TELEGRAM_BOT_TOKEN / KAKAO_REST_API_KEY) remain an optional shared
fallback only.
"""

from __future__ import annotations

import logging
import secrets
from datetime import datetime, timedelta

from fastapi import APIRouter, Body, HTTPException, Request
from fastapi.responses import Response

import cache
from deps import get_current_user
from services.notifications import engine, kakao, telegram


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
    telegram_status = {"connected": False, "enabled": False, "username": None, "chat_id": None}
    kakao_status = {"connected": False, "enabled": False, "nickname": None, "redirect_uri": kakao.redirect_uri(str(request.base_url))}
    for ch in items:
        if ch["channel"] == "telegram":
            cfg = ch.get("config") or {}
            telegram_status.update({
                "connected": bool(ch.get("verified")),
                "enabled": bool(ch.get("enabled")),
                "username": cfg.get("username"),
                "chat_id": cfg.get("chat_id"),
            })
        elif ch["channel"] == "kakao":
            kakao_status.update({
                "connected": bool(ch.get("verified")),
                "enabled": bool(ch.get("enabled")),
                "nickname": (ch.get("config") or {}).get("nickname"),
            })
    return {"telegram": telegram_status, "kakao": kakao_status}


@router.post("/telegram/register")
async def telegram_register(request: Request, payload: dict = Body(...)):
    """Register the user's own bot token. chat_id is taken from the body, or
    auto-detected via getUpdates (the user must have messaged their bot once)."""
    user = _require_user(await get_current_user(request))
    token = str(payload.get("bot_token") or "").strip()
    if not token:
        raise HTTPException(status_code=400, detail="봇 토큰을 입력하세요.")
    me = await telegram.get_me(token)
    if not me:
        raise HTTPException(status_code=400, detail="봇 토큰이 올바르지 않습니다. BotFather에서 발급한 토큰을 확인하세요.")
    username = me.get("username")

    chat_id = payload.get("chat_id")
    chat_id = str(chat_id).strip() if chat_id not in (None, "") else ""
    if not chat_id:
        detected = await telegram.get_recent_chat_id(token)
        if detected:
            chat_id = detected[0]

    if not chat_id:
        # Token is valid but we don't know where to send yet — keep it and
        # tell the user to message the bot, then retry.
        await cache.upsert_notification_channel(
            user["google_sub"], "telegram",
            config={"bot_token": token, "username": username},
            enabled=True, verified=False,
        )
        return {
            "connected": False,
            "username": username,
            "detail": f"@{username} 봇에게 텔레그램에서 아무 메시지나 보낸 뒤 다시 [연결]을 누르세요. (또는 chat_id를 직접 입력)",
        }

    config = {"bot_token": token, "chat_id": chat_id, "username": username}
    await cache.upsert_notification_channel(
        user["google_sub"], "telegram", config=config, enabled=True, verified=True
    )
    await telegram.send_message(token, chat_id, "✅ Value Compass 알림이 연결되었습니다.")
    return {"connected": True, "username": username, "chat_id": chat_id}


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
    cfg = ch.get("config") or {}
    token = cfg.get("bot_token") or telegram.default_token()
    chat_id = cfg.get("chat_id")
    ok = await telegram.send_message(
        token, chat_id, "🔔 Value Compass 테스트 알림입니다. 정상적으로 연결되었습니다."
    )
    if not ok:
        raise HTTPException(status_code=502, detail="텔레그램 전송에 실패했습니다.")
    return {"ok": True}


@router.delete("/telegram")
async def unlink_telegram(request: Request):
    user = _require_user(await get_current_user(request))
    await cache.delete_notification_channel(user["google_sub"], "telegram")
    return {"ok": True}


# --- KakaoTalk (OAuth "나에게 보내기") --------------------------------------

def _kakao_result_page(title: str, message: str) -> Response:
    """Tiny page shown in the OAuth popup after Kakao redirects back."""
    html = (
        "<!doctype html><html lang='ko'><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        f"<title>{title}</title>"
        "<style>body{font-family:system-ui,'Apple SD Gothic Neo',sans-serif;"
        "display:flex;flex-direction:column;align-items:center;justify-content:center;"
        "height:100vh;margin:0;background:#f7f7f8;color:#222;text-align:center;padding:20px}"
        "h1{font-size:18px;margin:0 0 8px}p{color:#666;font-size:14px;margin:0 0 20px}"
        "button{padding:9px 18px;border:none;border-radius:8px;background:#2563eb;color:#fff;"
        "font-size:14px;cursor:pointer}</style></head><body>"
        f"<h1>{title}</h1><p>{message}</p>"
        "<button onclick='window.close()'>창 닫기</button>"
        "<script>setTimeout(function(){try{window.close();}catch(e){}},1500);</script>"
        "</body></html>"
    )
    return Response(content=html, media_type="text/html; charset=utf-8")


@router.post("/kakao/connect")
async def kakao_connect(request: Request, payload: dict = Body(default={})):
    """Start Kakao OAuth using the user's own REST key (env key as fallback).

    The REST key is stashed in the (unverified) kakao channel so the public
    callback can exchange the code with the same key.
    """
    user = _require_user(await get_current_user(request))
    rest_key = str((payload or {}).get("rest_key") or "").strip() or kakao.env_key()
    if not rest_key:
        raise HTTPException(status_code=400, detail="카카오 REST API 키를 입력하세요.")
    redirect = kakao.redirect_uri(str(request.base_url))
    if not redirect:
        raise HTTPException(status_code=503, detail="Redirect URI를 확인할 수 없습니다.")
    # Persist the rest_key (unverified) so the callback can use it.
    await cache.upsert_notification_channel(
        user["google_sub"], "kakao", config={"rest_key": rest_key}, enabled=True, verified=False
    )
    state = secrets.token_urlsafe(12)
    expires_at = (datetime.now() + timedelta(minutes=LINK_TTL_MINUTES)).isoformat()
    await cache.create_notification_link(state, user["google_sub"], "kakao", expires_at)
    return {
        "authorize_url": kakao.authorize_url(rest_key, state, redirect),
        "redirect_uri": redirect,
        "expires_in_minutes": LINK_TTL_MINUTES,
    }


@router.get("/kakao/callback")
async def kakao_callback(request: Request):
    # Public endpoint: identity comes from the single-use `state` code, not the
    # session. Kakao redirects the OAuth popup here with ?code & ?state.
    params = request.query_params
    if params.get("error") or not params.get("code") or not params.get("state"):
        return _kakao_result_page(
            "연결 실패",
            "연결이 취소되었거나 오류가 발생했습니다. 카카오 앱 설정(카카오 로그인 활성화, "
            "카카오톡 메시지 동의항목, Redirect URI 등록)을 확인한 뒤 다시 시도하세요.",
        )
    link = await cache.pop_notification_link(params["state"])
    if not link or link.get("channel") != "kakao":
        return _kakao_result_page("연결 실패", "연결 코드가 만료되었습니다. 다시 시도해주세요.")
    google_sub = link["google_sub"]
    pending = await cache.get_notification_channel(google_sub, "kakao")
    rest_key = ((pending or {}).get("config") or {}).get("rest_key") or kakao.env_key()
    redirect = kakao.redirect_uri(str(request.base_url))
    if not rest_key:
        return _kakao_result_page("연결 실패", "REST API 키 정보가 없습니다. 처음부터 다시 시도하세요.")
    try:
        tokens = await kakao.exchange_code(rest_key, params["code"], redirect)
    except Exception as exc:
        logger.warning("kakao token exchange failed: %s", exc)
        return _kakao_result_page("연결 실패", "토큰 발급에 실패했습니다. REST 키와 Redirect URI 등록을 확인하세요.")
    config = {"rest_key": rest_key, **tokens}
    nickname = await kakao.fetch_nickname(tokens["access_token"])
    if nickname:
        config["nickname"] = nickname
    await cache.upsert_notification_channel(
        google_sub, "kakao", config=config, enabled=True, verified=True
    )
    logger.info("kakao linked user=%s", google_sub[:8])
    return _kakao_result_page("연결 완료", "카카오톡 알림이 연결되었습니다. 이 창은 닫으셔도 됩니다.")


@router.put("/channels/kakao")
async def toggle_kakao(request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    enabled = bool(payload.get("enabled", True))
    ok = await cache.set_notification_channel_enabled(user["google_sub"], "kakao", enabled)
    if not ok:
        raise HTTPException(status_code=404, detail="연결된 카카오 계정이 없습니다.")
    return {"ok": True, "enabled": enabled}


@router.post("/channels/kakao/test")
async def test_kakao(request: Request):
    user = _require_user(await get_current_user(request))
    ch = await cache.get_notification_channel(user["google_sub"], "kakao")
    if not ch or not ch.get("verified"):
        raise HTTPException(status_code=400, detail="먼저 카카오 계정을 연결해주세요.")
    ok = await kakao.send_to_user(
        user["google_sub"], ch, "🔔 Value Compass 테스트 알림입니다. 정상적으로 연결되었습니다."
    )
    if not ok:
        raise HTTPException(status_code=502, detail="카카오 전송에 실패했습니다. 연결을 다시 시도해주세요.")
    return {"ok": True}


@router.delete("/kakao")
async def unlink_kakao(request: Request):
    user = _require_user(await get_current_user(request))
    await cache.delete_notification_channel(user["google_sub"], "kakao")
    return {"ok": True}


# --- Alert rules ------------------------------------------------------------

async def _validate_alert_payload(google_sub: str, payload: dict) -> dict:
    alert_type = str(payload.get("alert_type") or "").strip()
    if alert_type not in engine.ALL_ALERT_TYPES:
        raise HTTPException(status_code=400, detail="지원하지 않는 알림 유형입니다.")

    # Scope is derived from the type: price is per-stock; target_reached and
    # daily_change_abs are blanket (all holdings); nav / portfolio daily are
    # whole-portfolio.
    if alert_type in engine.PRICE_TYPES:
        scope = "stock"
    elif alert_type in engine.BLANKET_TYPES:
        scope = "all_stocks"
    else:
        scope = "portfolio"

    # target_reached uses each holding's own 목표가, so no user threshold.
    if alert_type in engine.TARGET_TYPES:
        threshold = 0.0
    else:
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
            raise HTTPException(status_code=400, detail="보유 종목에 대해서만 종목 알림을 설정할 수 있습니다.")
        if threshold <= 0:
            raise HTTPException(status_code=400, detail="지정가는 0보다 커야 합니다.")
    elif alert_type in engine.NAV_TYPES and threshold <= 0:
        raise HTTPException(status_code=400, detail="총평가액 기준은 0보다 커야 합니다.")
    elif alert_type in engine.DAILY_ABS_TYPES and threshold <= 0:
        raise HTTPException(status_code=400, detail="등락률 기준은 0보다 커야 합니다.")

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
    sub = user["google_sub"]
    rule = await _validate_alert_payload(sub, payload)
    # Blanket rules (목표가 도달 / 전 종목 일간 등락률) are singletons per user:
    # creating one again just updates the existing rule instead of duplicating.
    if rule["alert_type"] in engine.BLANKET_TYPES:
        for existing in await cache.list_portfolio_alerts(sub):
            if existing["alert_type"] == rule["alert_type"]:
                await cache.update_portfolio_alert(
                    sub, existing["id"],
                    threshold=rule["threshold"], note=rule["note"], enabled=rule["enabled"],
                )
                return await cache.get_portfolio_alert(sub, existing["id"])
    alert_id = await cache.create_portfolio_alert(sub, **rule)
    return await cache.get_portfolio_alert(sub, alert_id)


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
