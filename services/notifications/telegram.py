"""Telegram Bot API client + getUpdates link/command poller.

Two responsibilities:
* ``send_message`` / ``get_bot_username`` — outbound Bot API calls used by the
  alert engine and the link UI.
* ``run_poll_loop`` — long-polls ``getUpdates`` so a user who taps the
  ``t.me/<bot>?start=<code>`` deep link gets their chat_id captured and bound to
  their account automatically (no manual chat_id copy-paste).

Everything is a no-op when ``TELEGRAM_BOT_TOKEN`` is unset, so the app boots
fine without a bot configured.
"""

from __future__ import annotations

import asyncio
import logging
import os

import httpx

import cache


logger = logging.getLogger(__name__)

API_BASE = "https://api.telegram.org"
_OFFSET_KEY = "telegram_update_offset"
_bot_username_cache: str | None = None


def _token() -> str:
    return (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()


def is_configured() -> bool:
    return bool(_token())


async def _api(method: str, payload: dict | None = None, *, timeout: float = 15.0):
    token = _token()
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN not configured")
    url = f"{API_BASE}/bot{token}/{method}"
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(url, json=payload or {})
        data = resp.json()
    if not data.get("ok"):
        raise RuntimeError(f"telegram {method} failed: {data.get('description') or data}")
    return data.get("result")


async def send_message(chat_id, text: str, *, parse_mode: str | None = None) -> bool:
    """Send a plain-text message. Returns True on success, never raises."""
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    try:
        await _api("sendMessage", payload)
        return True
    except Exception as exc:
        logger.warning("telegram sendMessage failed chat=%s: %s", chat_id, exc)
        return False


async def get_bot_username() -> str | None:
    """Resolve the bot username for building the deep link.

    ``TELEGRAM_BOT_USERNAME`` env wins; otherwise call ``getMe`` once and cache.
    """
    global _bot_username_cache
    env = (os.getenv("TELEGRAM_BOT_USERNAME") or "").strip().lstrip("@")
    if env:
        return env
    if _bot_username_cache:
        return _bot_username_cache
    if not is_configured():
        return None
    try:
        me = await _api("getMe")
        _bot_username_cache = (me or {}).get("username")
        return _bot_username_cache
    except Exception as exc:
        logger.warning("telegram getMe failed: %s", exc)
        return None


async def _get_offset() -> int:
    try:
        row = await cache.get_app_setting(_OFFSET_KEY)
        return int(row["value"]) if row and row.get("value") else 0
    except (TypeError, ValueError, Exception):
        return 0


async def _set_offset(offset: int) -> None:
    try:
        await cache.set_app_setting(_OFFSET_KEY, str(offset))
    except Exception as exc:
        logger.warning("telegram offset persist failed: %s", exc)


async def _handle_update(update: dict) -> None:
    msg = update.get("message") or update.get("edited_message")
    if not msg:
        return
    chat = msg.get("chat") or {}
    chat_id = chat.get("id")
    text = (msg.get("text") or "").strip()
    if not chat_id or not text:
        return

    if text.split(maxsplit=1)[0] in ("/start", "/link"):
        parts = text.split(maxsplit=1)
        code = parts[1].strip() if len(parts) > 1 else ""
        if code:
            link = await cache.pop_notification_link(code)
            if link and link.get("channel") == "telegram":
                username = chat.get("username") or chat.get("first_name") or ""
                await cache.upsert_notification_channel(
                    link["google_sub"],
                    "telegram",
                    config={"chat_id": chat_id, "username": username},
                    enabled=True,
                    verified=True,
                )
                await send_message(
                    chat_id,
                    "✅ Value Compass 알림이 연결되었습니다.\n이제 포트폴리오 조건 알림을 이 채팅으로 받습니다.",
                )
                logger.info("telegram linked chat_id=%s user=%s", chat_id, link["google_sub"][:8])
                return
            await send_message(
                chat_id,
                "연결 코드가 만료되었거나 올바르지 않습니다. 웹에서 다시 '연결'을 눌러주세요.",
            )
            return
        await send_message(
            chat_id,
            "Value Compass 알림 봇입니다.\n웹 포트폴리오 화면의 🔔 알림 → '텔레그램 연결' 버튼으로 연결해주세요.",
        )


async def run_poll_loop(stop_event: asyncio.Event, *, long_poll_timeout: int = 25) -> None:
    """Long-poll getUpdates until ``stop_event`` is set.

    Cancellation-safe: an in-flight long poll is interrupted by task
    cancellation on shutdown. Only one consumer per bot may call getUpdates, so
    this must run as a single instance.
    """
    if not is_configured():
        logger.info("telegram poller disabled (no TELEGRAM_BOT_TOKEN)")
        return
    logger.info("telegram poller starting")
    offset = await _get_offset()
    while not stop_event.is_set():
        try:
            updates = await _api(
                "getUpdates",
                {"offset": offset, "timeout": long_poll_timeout},
                timeout=long_poll_timeout + 10,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("telegram getUpdates failed: %s", exc)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass
            continue
        for upd in updates or []:
            offset = max(offset, int(upd.get("update_id", 0)) + 1)
            try:
                await _handle_update(upd)
            except Exception as exc:
                logger.warning("telegram update handling failed: %s", exc)
        if updates:
            await _set_offset(offset)
            try:
                await cache.delete_expired_notification_links()
            except Exception:
                pass
    logger.info("telegram poller stopped")
