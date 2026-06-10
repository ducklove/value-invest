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
from services.notifications import channels, engine, kakao, telegram


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
    # Scope 매핑: 개별 종목(가격/일간등락/신규공시/신규리포트)=stock,
    # target/daily_abs/limit=blanket(all_stocks), 나머지=portfolio.
    if alert_type in (engine.PRICE_TYPES | engine.STOCK_DAILY_ABS_TYPES | engine.STOCK_FEED_TYPES):
        scope = "stock"
    elif alert_type in engine.BLANKET_TYPES:
        scope = "all_stocks"
    else:
        scope = "portfolio"

    # 사용자 임계값이 없는 유형: 목표가/상하한가 도달, 개별·전체 신규 공시/리포트,
    # 리밸런싱 드리프트(임계값은 목표별 tolerance 가 대신함).
    if alert_type in (
        engine.TARGET_TYPES | engine.LIMIT_TYPES | engine.STOCK_FEED_TYPES
        | engine.BLANKET_FEED_TYPES | engine.REBALANCE_TYPES
    ):
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
        # 분석 화면(source="analysis")은 임의 종목 허용. 포트폴리오 경유는 보유 종목만.
        if str(payload.get("source") or "") != "analysis":
            held = {it["stock_code"] for it in await cache.get_portfolio(google_sub)}
            if stock_code not in held:
                raise HTTPException(status_code=400, detail="보유 종목에 대해서만 종목 알림을 설정할 수 있습니다.")
        if alert_type in engine.PRICE_TYPES and threshold <= 0:
            raise HTTPException(status_code=400, detail="가격은 0보다 커야 합니다.")
        if alert_type in engine.STOCK_DAILY_ABS_TYPES and threshold <= 0:
            raise HTTPException(status_code=400, detail="등락률 기준은 0보다 커야 합니다.")
    elif alert_type in engine.NAV_TYPES and threshold <= 0:
        raise HTTPException(status_code=400, detail="총평가액 기준은 0보다 커야 합니다.")
    elif alert_type in engine.DAILY_ABS_TYPES and threshold <= 0:
        raise HTTPException(status_code=400, detail="등락률 기준은 0보다 커야 합니다.")

    note = str(payload.get("note") or "").strip()[:200]
    enabled = bool(payload.get("enabled", True))
    important = bool(payload.get("important", False))
    return {
        "scope": scope,
        "alert_type": alert_type,
        "threshold": threshold,
        "stock_code": stock_code,
        "note": note,
        "enabled": enabled,
        "important": important,
    }


@router.get("/alerts")
async def get_alerts(request: Request, stock_code: str | None = None):
    user = _require_user(await get_current_user(request))
    alerts = await cache.list_portfolio_alerts(user["google_sub"])
    if stock_code:
        code = stock_code.strip()
        alerts = [a for a in alerts if a.get("stock_code") == code]
    return alerts


@router.post("/alerts")
async def create_alert(request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    sub = user["google_sub"]
    rule = await _validate_alert_payload(sub, payload)
    # Blanket(목표가/전종목 일간등락/상하한가)은 사용자당 singleton, 개별 종목의
    # 일간등락·신규공시·신규리포트는 (종목, 유형) 단위 singleton — 재생성 시 기존
    # 규칙을 갱신(중복 방지). 가격(price_*)은 한 종목에 여러 개 둘 수 있어 제외.
    singleton_per_stock = engine.STOCK_DAILY_ABS_TYPES | engine.STOCK_FEED_TYPES
    # 리밸런싱 드리프트도 사용자당 singleton — 목표 목록 전체를 한 규칙이 본다.
    if rule["alert_type"] in (engine.BLANKET_TYPES | engine.REBALANCE_TYPES):
        for existing in await cache.list_portfolio_alerts(sub):
            if existing["alert_type"] == rule["alert_type"]:
                await cache.update_portfolio_alert(
                    sub, existing["id"],
                    threshold=rule["threshold"], note=rule["note"], enabled=rule["enabled"],
                    important=rule["important"],
                )
                return await cache.get_portfolio_alert(sub, existing["id"])
    elif rule["alert_type"] in singleton_per_stock:
        for existing in await cache.list_portfolio_alerts(sub):
            if existing["alert_type"] == rule["alert_type"] and existing["stock_code"] == rule["stock_code"]:
                await cache.update_portfolio_alert(
                    sub, existing["id"],
                    threshold=rule["threshold"], note=rule["note"], enabled=rule["enabled"],
                    important=rule["important"],
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

    # 중요 표시만 토글: 검증·엣지 리셋 없이 가볍게 갱신.
    if set(payload.keys()) <= {"important"}:
        await cache.set_portfolio_alert_important(
            user["google_sub"], alert_id, bool(payload.get("important"))
        )
        return await cache.get_portfolio_alert(user["google_sub"], alert_id)

    merged = dict(existing)
    for key in ("alert_type", "threshold", "stock_code", "note", "enabled", "important"):
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


# --- Economic calendar event subscriptions ---------------------------------

@router.get("/calendar")
async def get_calendar_subscriptions(request: Request):
    """이 사용자가 결과 알림을 신청한 경제캘린더 이벤트들. 프론트는 event_ids로
    체크박스 상태를 복원한다(아직 발송 전인 것만)."""
    user = _require_user(await get_current_user(request))
    subs = await cache.list_calendar_subscriptions(user["google_sub"], pending_only=True)
    return {"event_ids": [s["event_id"] for s in subs]}


@router.post("/calendar")
async def subscribe_calendar(request: Request, payload: dict = Body(...)):
    """경제캘린더 이벤트 결과 발표 시 알림 구독. 활성 채널이 없으면 409."""
    user = _require_user(await get_current_user(request))
    if not await channels.has_active_channel(user["google_sub"]):
        raise HTTPException(
            status_code=409,
            detail="알림을 받으려면 먼저 텔레그램 또는 카카오톡을 연결하세요.",
        )
    event_id = str(payload.get("event_id") or "").strip()[:40]
    event_date = str(payload.get("event_date") or "").strip()[:10]
    if not event_id or not event_date:
        raise HTTPException(status_code=400, detail="이벤트 정보가 올바르지 않습니다.")

    def _s(key: str, limit: int) -> str:
        return str(payload.get(key) or "").strip()[:limit]

    await cache.upsert_calendar_subscription(
        user["google_sub"], event_id,
        event_date=event_date,
        event_datetime=_s("event_datetime", 30),
        country=_s("country", 4),
        country_name=_s("country_name", 20),
        event=_s("event", 200),
        importance=_s("importance", 8),
        forecast=_s("forecast", 40),
        previous=_s("previous", 40),
    )
    return {"ok": True, "event_id": event_id}


@router.delete("/calendar/{event_id}")
async def unsubscribe_calendar(event_id: str, request: Request):
    user = _require_user(await get_current_user(request))
    await cache.delete_calendar_subscription(user["google_sub"], event_id)
    return {"ok": True}


@router.get("/calendar/status")
async def calendar_alert_status(request: Request):
    """캘린더 결과 알림 진단(로그인 사용자 본인 기준).

    "알림이 안 온다"의 원인을 한 번에 가린다:
    - alert_loop_enabled : 서버 알림 루프가 켜져 있나(NOTIFY_ALERT_INTERVAL_S>0).
    - has_active_channel : 텔레그램/카카오가 연결+활성인가.
    - pending            : 아직 발송 전인 구독 목록(없으면 구독이 저장 안 된 것).
    - ready_to_fire_now  : 현재 실제치(actual)가 이미 나와 다음 틱에 발송될 항목.
    """
    import os
    from datetime import date

    import economic_calendar

    user = _require_user(await get_current_user(request))
    sub = user["google_sub"]

    try:
        interval = float(os.environ.get("NOTIFY_ALERT_INTERVAL_S", "0") or 0)
    except (TypeError, ValueError):
        interval = 0.0
    has_channel = await channels.has_active_channel(sub)
    pending = await cache.list_calendar_subscriptions(sub, pending_only=True)

    today = date.today().isoformat()
    candidates = [s for s in pending if (s.get("event_date") or "") <= today]
    ready: list[dict] = []
    if candidates:
        countries = sorted({s.get("country") for s in candidates if s.get("country")})
        try:
            data = await economic_calendar.fetch_economic_calendar(
                start_date=min(s["event_date"] for s in candidates),
                end_date=today,
                countries=countries or None,
                importance=["high", "mid", "low"],
            )
            by_id = {e["index_id"]: e for e in data.get("events", []) if e.get("index_id")}
            for s in candidates:
                ev = by_id.get(s.get("event_id"))
                if ev and (ev.get("actual") or "").strip():
                    ready.append({"event": s.get("event"), "actual": ev.get("actual")})
        except Exception:
            pass

    return {
        "alert_loop_enabled": interval > 0,
        "alert_interval_s": interval,
        "has_active_channel": has_channel,
        "server_today": today,
        "pending_count": len(pending),
        "pending": [
            {
                "event": s.get("event"),
                "country": s.get("country_name") or s.get("country"),
                "date": s.get("event_date"),
                "datetime": s.get("event_datetime"),
            }
            for s in pending
        ],
        "ready_to_fire_now": ready,
    }
