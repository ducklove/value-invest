"""Channel fan-out. The engine calls ``dispatch`` and stays channel-agnostic.

Adding KakaoTalk later: implement a ``kakao`` sender and add an ``elif`` branch
below — the alert engine and rule storage do not change.
"""

from __future__ import annotations

import logging

import cache
from services.notifications import kakao, telegram


logger = logging.getLogger(__name__)


async def dispatch(google_sub: str, text: str) -> int:
    """Send ``text`` to every enabled+verified channel. Returns messages sent.

    Never raises — a failing channel is logged and skipped so one broken target
    can't block the others or the evaluation loop.
    """
    try:
        channels = await cache.list_notification_channels(google_sub)
    except Exception as exc:
        logger.warning("dispatch channel lookup failed user=%s: %s", google_sub[:8], exc)
        return 0

    sent = 0
    for ch in channels:
        if not ch.get("enabled") or not ch.get("verified"):
            continue
        name = ch.get("channel")
        config = ch.get("config") or {}
        try:
            if name == "telegram":
                token = config.get("bot_token") or telegram.default_token()
                chat_id = config.get("chat_id")
                if token and chat_id and await telegram.send_message(token, chat_id, text):
                    sent += 1
            elif name == "kakao":
                if await kakao.send_to_user(google_sub, ch, text):
                    sent += 1
            else:
                logger.debug("dispatch: unknown channel %s", name)
        except Exception as exc:
            logger.warning("dispatch failed user=%s channel=%s: %s", google_sub[:8], name, exc)
    return sent


async def has_active_channel(google_sub: str) -> bool:
    try:
        channels = await cache.list_notification_channels(google_sub)
    except Exception:
        return False
    return any(c.get("enabled") and c.get("verified") for c in channels)
