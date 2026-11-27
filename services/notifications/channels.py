"""Channel fan-out. The engine calls ``dispatch`` and stays channel-agnostic.

Adding KakaoTalk later: implement a ``kakao`` sender and add an ``elif`` branch
below — the alert engine and rule storage do not change.
"""

from __future__ import annotations

import hashlib
import logging
import sqlite3
from datetime import datetime

from core.errors import DBError
from repositories import notifications as notifications_repo
from services.notifications import kakao, telegram, telegram_refs

logger = logging.getLogger(__name__)
_DB_ERRORS = (DBError, sqlite3.Error, OSError)


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _target_key(google_sub: str, channel: str, config: dict) -> str:
    if channel == "telegram":
        chat_id = str(config.get("chat_id") or "").strip()
        token = str(config.get("bot_token") or telegram.default_token() or "").strip()
        return _digest(f"telegram:{token}:{chat_id}") if chat_id else _digest(f"telegram:{google_sub}")
    if channel == "kakao":
        rest_key = str(config.get("rest_key") or kakao.env_key() or "").strip()
        return _digest(f"kakao:{rest_key}") if rest_key else _digest(f"kakao:{google_sub}")
    return _digest(f"{channel}:{google_sub}")


async def _claim_delivery(
    google_sub: str,
    channel: str,
    config: dict,
    dedupe_key: str | None,
) -> tuple[bool, str | None, str | None]:
    if not dedupe_key:
        return True, None, None
    target_key = _target_key(google_sub, channel, config)
    sent_date = datetime.now().strftime("%Y-%m-%d")
    claimed = await notifications_repo.claim_notification_delivery(
        channel,
        target_key,
        dedupe_key,
        sent_date,
    )
    return claimed, target_key, sent_date


async def _release_delivery(
    channel: str,
    target_key: str | None,
    dedupe_key: str | None,
    sent_date: str | None,
) -> None:
    if not dedupe_key or not target_key or not sent_date:
        return
    await notifications_repo.release_notification_delivery(
        channel,
        target_key,
        dedupe_key,
        sent_date,
    )


async def dispatch(
    google_sub: str, text: str, *, dedupe_key: str | None = None, only_channel: str | None = None,
    stock_code: str | None = None,
) -> int:
    """Send ``text`` to every enabled+verified channel. Returns messages sent.

    ``stock_code``(또는 ``telegram_refs.stock_context``)가 있으면 텔레그램 메시지의
    message_id 를 종목에 매핑해, 사용자가 답장하면 그 종목 메모가 된다.

    Never raises — a failing channel is logged and skipped so one broken target
    can't block the others or the evaluation loop.
    """
    try:
        channels = await notifications_repo.list_notification_channels(google_sub)
    except _DB_ERRORS as exc:
        logger.warning("dispatch channel lookup failed user=%s: %s", google_sub[:8], exc)
        return 0

    sent = 0
    for ch in channels:
        if not ch.get("enabled") or not ch.get("verified"):
            continue
        name = ch.get("channel")
        if only_channel is not None and name != only_channel:
            continue
        config = ch.get("config") or {}
        target_key = None
        sent_date = None
        try:
            claimed, target_key, sent_date = await _claim_delivery(
                google_sub,
                str(name or ""),
                config,
                dedupe_key,
            )
            if not claimed:
                logger.info(
                    "dispatch suppressed duplicate user=%s channel=%s key=%s",
                    google_sub[:8],
                    name,
                    dedupe_key,
                )
                continue
            if name == "telegram":
                token = config.get("bot_token") or telegram.default_token()
                chat_id = config.get("chat_id")
                if token and chat_id and await telegram_refs.send_message(
                    google_sub, token, chat_id, text, stock_code=stock_code
                ):
                    sent += 1
                else:
                    await _release_delivery(str(name or ""), target_key, dedupe_key, sent_date)
            elif name == "kakao":
                if await kakao.send_to_user(google_sub, ch, text):
                    sent += 1
                else:
                    await _release_delivery(str(name or ""), target_key, dedupe_key, sent_date)
            else:
                await _release_delivery(str(name or ""), target_key, dedupe_key, sent_date)
                logger.debug("dispatch: unknown channel %s", name)
        except Exception as exc:  # noqa: BLE001 — 의도된 경계: 채널별 독립 실패 격리
            try:
                await _release_delivery(str(name or ""), target_key, dedupe_key, sent_date)
            except _DB_ERRORS as release_exc:
                logger.debug("dispatch release delivery failed user=%s channel=%s: %s", google_sub[:8], name, release_exc)
            logger.warning("dispatch failed user=%s channel=%s: %s", google_sub[:8], name, exc)
    return sent


async def has_active_channel(google_sub: str) -> bool:
    try:
        channels = await notifications_repo.list_notification_channels(google_sub)
    except _DB_ERRORS as exc:
        logger.warning("has_active_channel lookup failed user=%s: %s", google_sub[:8], exc)
        return False
    return any(c.get("enabled") and c.get("verified") for c in channels)
