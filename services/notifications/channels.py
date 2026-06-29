"""Channel fan-out. The engine calls ``dispatch`` and stays channel-agnostic.

Adding KakaoTalk later: implement a ``kakao`` sender and add an ``elif`` branch
below — the alert engine and rule storage do not change.
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime

from repositories import notifications as notifications_repo
from services.notifications import kakao, telegram


logger = logging.getLogger(__name__)


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


async def dispatch(google_sub: str, text: str, *, dedupe_key: str | None = None) -> int:
    """Send ``text`` to every enabled+verified channel. Returns messages sent.

    Never raises — a failing channel is logged and skipped so one broken target
    can't block the others or the evaluation loop.
    """
    try:
        channels = await notifications_repo.list_notification_channels(google_sub)
    except Exception as exc:
        logger.warning("dispatch channel lookup failed user=%s: %s", google_sub[:8], exc)
        return 0

    sent = 0
    for ch in channels:
        if not ch.get("enabled") or not ch.get("verified"):
            continue
        name = ch.get("channel")
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
                if token and chat_id and await telegram.send_message(token, chat_id, text):
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
        except Exception as exc:
            try:
                await _release_delivery(str(name or ""), target_key, dedupe_key, sent_date)
            except Exception:
                pass
            logger.warning("dispatch failed user=%s channel=%s: %s", google_sub[:8], name, exc)
    return sent


async def has_active_channel(google_sub: str) -> bool:
    try:
        channels = await notifications_repo.list_notification_channels(google_sub)
    except Exception:
        return False
    return any(c.get("enabled") and c.get("verified") for c in channels)
