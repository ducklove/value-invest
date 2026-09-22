"""Per-user quiet hours and durable deferred delivery for condition alerts."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from zoneinfo import ZoneInfo

from repositories import notifications as notifications_repo
from repositories import user_settings
from services.notifications import channels

SETTING_KEY = "portfolio_alert_quiet_hours"
DEFAULT_SETTINGS = {"enabled": True, "start": "21:00", "end": "08:00", "mode": "skip"}
KST = ZoneInfo("Asia/Seoul")


def now_kst() -> datetime:
    return datetime.now(KST)


def validate_settings(payload: dict) -> dict:
    if not isinstance(payload.get("enabled"), bool):
        raise ValueError("알림 제한 사용 여부를 선택하세요.")
    for field in ("start", "end"):
        if not isinstance(payload.get(field), str) or not re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]", payload[field]):
            raise ValueError("시작·종료 시간을 HH:MM 형식으로 입력하세요.")
    if payload["start"] == payload["end"]:
        raise ValueError("시작과 종료 시간을 다르게 설정하세요. 항상 받으려면 시간 제한을 끄세요.")
    if payload.get("mode") not in ("skip", "defer"):
        raise ValueError("제한 시간의 알림 처리 방법을 선택하세요.")
    return {key: payload[key] for key in DEFAULT_SETTINGS}


async def get_settings(google_sub: str) -> dict:
    raw = await user_settings.get_user_setting(google_sub, SETTING_KEY)
    return validate_settings(json.loads(raw)) if raw else dict(DEFAULT_SETTINGS)


async def save_settings(google_sub: str, payload: dict) -> dict:
    settings = validate_settings(payload)
    await user_settings.set_user_setting(google_sub, SETTING_KEY, json.dumps(settings))
    return settings


def is_quiet(settings: dict, now: datetime | None = None) -> bool:
    if not settings["enabled"]:
        return False
    time = (now or now_kst()).astimezone(KST).strftime("%H:%M")
    start, end = settings["start"], settings["end"]
    if start < end:
        return start <= time < end
    return time >= start or time < end


async def dispatch(google_sub: str, rule_id: int, text: str, *, dedupe_key: str | None = None) -> int:
    settings = await get_settings(google_sub)
    now = now_kst()
    if is_quiet(settings, now):
        if settings["mode"] == "defer":
            event_key = hashlib.sha256(f"{now.date()}:{dedupe_key or text}".encode()).hexdigest()
            await notifications_repo.enqueue_portfolio_alert(
                google_sub, rule_id, text, event_key, now.isoformat()
            )
        # Both skip and defer consume the trigger, preventing repeat alerts for
        # the same crossing when quiet hours end. Deferred text survives restart.
        return 0
    if dedupe_key:
        await channels.dispatch(google_sub, text, dedupe_key=dedupe_key)
    else:
        await channels.dispatch(google_sub, text)
    return 1


async def flush_pending(google_sub: str) -> int:
    if is_quiet(await get_settings(google_sub)):
        return 0
    sent = 0
    for item in await notifications_repo.list_pending_portfolio_alerts(google_sub):
        # Check each send: draining a large queue must stop if quiet hours begin.
        if is_quiet(await get_settings(google_sub)):
            break
        text = f"⏰ 모아둔 알림 · {item['occurred_at'][5:16].replace('T', ' ')} (한국 시간)\n{item['message']}"
        delivered = await channels.dispatch(google_sub, text, only_channel=item["channel"])
        if delivered:
            await notifications_repo.delete_pending_portfolio_alert(google_sub, item["id"])
            sent += 1
    return sent
