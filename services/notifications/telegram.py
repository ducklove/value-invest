"""Telegram Bot API client — per-user bot tokens.

Each user registers their own bot (created via @BotFather) and its chat_id, so
notifications are fully self-service and don't depend on a server-wide bot. The
token lives in ``notification_channels.config_json`` (per google_sub); these
functions take the token explicitly.

``TELEGRAM_BOT_TOKEN`` env remains an optional fallback (a shared bot) used only
when a user's channel config has no ``bot_token``.
"""

from __future__ import annotations

import logging
import os

from core.http import get_http_client

logger = logging.getLogger(__name__)

API_BASE = "https://api.telegram.org"


def default_token() -> str:
    return (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()


def is_configured() -> bool:
    """Whether the optional server-wide fallback bot is set. Per-user bots work
    regardless of this."""
    return bool(default_token())


async def _api(token: str, method: str, payload: dict | None = None, *, timeout: float = 15.0):
    if not token:
        raise RuntimeError("telegram bot token missing")
    url = f"{API_BASE}/bot{token}/{method}"
    client = await get_http_client("telegram")
    resp = await client.post(url, json=payload or {}, timeout=timeout)
    data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"telegram {method} failed: {data.get('description') or data}")
    return data.get("result")


async def get_me(token: str) -> dict | None:
    """Validate a bot token; returns the bot info (incl. username) or None."""
    try:
        return await _api(token, "getMe")
    except Exception as exc:
        logger.info("telegram getMe failed: %s", exc)
        return None


async def get_recent_chat_id(token: str) -> tuple[str, str] | None:
    """Most recent chat that messaged this bot → (chat_id, display name).

    Used to auto-capture the user's chat_id after they message their own bot,
    so they don't have to look it up manually.
    """
    try:
        updates = await _api(token, "getUpdates", {"timeout": 0, "limit": 10}, timeout=15.0)
    except Exception as exc:
        logger.info("telegram getUpdates failed: %s", exc)
        return None
    for upd in reversed(updates or []):
        msg = upd.get("message") or upd.get("edited_message") or {}
        chat = msg.get("chat") or {}
        if chat.get("id") is not None:
            name = chat.get("username") or chat.get("first_name") or ""
            return str(chat["id"]), name
    return None


async def send_message(token: str, chat_id, text: str, *, parse_mode: str | None = None) -> bool:
    """Send a plain-text message via a specific bot token. Never raises."""
    if not token or chat_id in (None, ""):
        return False
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    try:
        await _api(token, "sendMessage", payload)
        return True
    except Exception as exc:
        logger.warning("telegram sendMessage failed chat=%s: %s", chat_id, exc)
        return False
