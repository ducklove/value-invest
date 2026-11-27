"""Telegram Bot API client — per-user bot tokens.

Each user registers their own bot (created via @BotFather) and its chat_id, so
notifications are fully self-service and don't depend on a server-wide bot. The
token lives in ``notification_channels.config_json`` (per google_sub); these
functions take the token explicitly.

``TELEGRAM_BOT_TOKEN`` env remains an optional fallback (a shared bot) used only
when a user's channel config has no ``bot_token``.
"""

from __future__ import annotations

import hashlib
import logging
import os
import time

import httpx

from core.errors import ExternalServiceError
from core.http import get_http_client

logger = logging.getLogger(__name__)

API_BASE = "https://api.telegram.org"
_TELEGRAM_ERRORS = (httpx.HTTPError, ExternalServiceError, ValueError, TypeError, AttributeError)


def default_token() -> str:
    return (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()


def is_configured() -> bool:
    """Whether the optional server-wide fallback bot is set. Per-user bots work
    regardless of this."""
    return bool(default_token())


async def _api(token: str, method: str, payload: dict | None = None, *, timeout: float = 15.0):
    if not token:
        raise ExternalServiceError("telegram bot token missing")
    url = f"{API_BASE}/bot{token}/{method}"
    client = await get_http_client("telegram")
    resp = await client.post(url, json=payload or {}, timeout=timeout)
    data = resp.json()
    if not data.get("ok"):
        raise ExternalServiceError(f"telegram {method} failed: {data.get('description') or data}")
    return data.get("result")


async def get_me(token: str) -> dict | None:
    """Validate a bot token; returns the bot info (incl. username) or None."""
    try:
        return await _api(token, "getMe")
    except _TELEGRAM_ERRORS as exc:
        logger.info("telegram getMe failed: %s", exc)
        return None


async def get_recent_chat_id(token: str) -> tuple[str, str] | None:
    """Most recent chat that messaged this bot → (chat_id, display name).

    Used to auto-capture the user's chat_id after they message their own bot,
    so they don't have to look it up manually.

    수신 루프(telegram_bot)가 같은 봇의 업데이트를 이미 확인(offset 전진)했다면
    getUpdates 가 비어 있으므로, 그 루프가 기억해 둔 "미등록 개인 채팅"으로 보완한다.
    """
    try:
        updates = await _api(token, "getUpdates", {"timeout": 0, "limit": 10}, timeout=15.0)
    except _TELEGRAM_ERRORS as exc:
        logger.info("telegram getUpdates failed: %s", exc)
        return recent_unregistered_chat(token)
    if not updates:
        return recent_unregistered_chat(token)
    # 서로 다른 채팅이 여럿 말을 걸었으면 자동으로 고르지 않는다(위 폴백과 같은 이유).
    found: dict[str, str] = {}
    for upd in reversed(updates or []):
        msg = upd.get("message") or upd.get("edited_message") or {}
        chat = msg.get("chat") or {}
        if chat.get("id") is not None:
            found.setdefault(str(chat["id"]), chat.get("username") or chat.get("first_name") or "")
    if len(found) != 1:
        if len(found) > 1:
            logger.info("telegram getUpdates: %d distinct chats, not auto-linking", len(found))
        return None
    return next(iter(found.items()))


async def send_message(
    token: str, chat_id, text: str, *, parse_mode: str | None = None, sent_ids: list[int] | None = None
) -> bool:
    """Send a plain-text message via a specific bot token. Never raises.

    ``sent_ids`` 를 넘기면 보낸 메시지의 message_id 를 거기에 담는다(답장=메모 매핑용).
    """
    if not token or chat_id in (None, ""):
        return False
    payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    try:
        result = await _api(token, "sendMessage", payload)
        if sent_ids is not None and isinstance(result, dict) and isinstance(result.get("message_id"), int):
            sent_ids.append(result["message_id"])
        return True
    except _TELEGRAM_ERRORS as exc:
        logger.warning("telegram sendMessage failed chat=%s: %s", chat_id, exc)
        return False


# --- 양방향 비서(수신 루프)용 ---------------------------------------------------


class TelegramApiError(ExternalServiceError):
    """Bot API 가 ok=false 로 답한 경우 — error_code(401/409/429…)를 보존한다."""

    def __init__(self, method: str, error_code: int | None, description: str, retry_after: float | None = None):
        super().__init__(f"telegram {method} failed: {error_code} {description}")
        self.error_code = error_code
        self.description = description
        self.retry_after = retry_after


async def get_updates(token: str, *, offset: int | None, timeout: int = 25) -> list[dict]:
    """long polling getUpdates. 실패는 :class:`TelegramApiError`/httpx 오류로 올린다."""
    if not token:
        raise ExternalServiceError("telegram bot token missing")
    payload: dict = {"timeout": int(timeout), "allowed_updates": ["message"]}
    if offset is not None:
        payload["offset"] = int(offset)
    client = await get_http_client("telegram_poll")
    resp = await client.post(f"{API_BASE}/bot{token}/getUpdates", json=payload, timeout=timeout + 15)
    try:
        data = resp.json()
    except ValueError as exc:
        raise TelegramApiError("getUpdates", resp.status_code, "invalid json") from exc
    if not isinstance(data, dict) or not data.get("ok"):
        data = data if isinstance(data, dict) else {}
        params = data.get("parameters") or {}
        retry_after = params.get("retry_after") if isinstance(params, dict) else None
        raise TelegramApiError(
            "getUpdates",
            data.get("error_code") or resp.status_code,
            str(data.get("description") or ""),
            float(retry_after) if isinstance(retry_after, (int, float)) else None,
        )
    result = data.get("result")
    return result if isinstance(result, list) else []


async def send_message_get_id(
    token: str, chat_id, text: str, *, reply_to_message_id: int | None = None
) -> int | None:
    """sendMessage 후 보낸 메시지의 message_id. 실패 시 None, 보냈지만 id 를 못 받으면 0.

    Never raises."""
    if not token or chat_id in (None, ""):
        return None
    payload: dict = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    if reply_to_message_id is not None:
        payload["reply_parameters"] = {"message_id": int(reply_to_message_id), "allow_sending_without_reply": True}
    try:
        result = await _api(token, "sendMessage", payload)
    except _TELEGRAM_ERRORS as exc:
        logger.warning("telegram sendMessage failed: %s", exc)
        return None
    message_id = result.get("message_id") if isinstance(result, dict) else None
    return message_id if isinstance(message_id, int) else 0


# 수신 루프가 본 "등록되지 않은 개인 채팅" — 루프가 offset 을 전진시키면 채널 연결의
# get_recent_chat_id 가 빈 결과를 받으므로 여기서 보완한다. 프로세스 메모리, 10분.
_UNREGISTERED_TTL_S = 600.0
# token digest → {chat_id: (seen_at, name)}. 연결 폴백은 창 안의 낯선 채팅이 정확히 하나일
# 때만 쓴다 — 봇 이름을 아는 다른 사람이 먼저 말을 걸어 두면 양방향 비서 권한(보유 조회·
# 메모 쓰기)까지 가로챌 수 있으므로, 둘 이상이면 자동 연결하지 않고 chat_id 입력을 받는다.
_recent_unregistered: dict[str, dict[str, tuple[float, str]]] = {}


def _token_digest(token: str) -> str:
    return hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()[:16]


def remember_unregistered_chat(token: str, chat_id: str, name: str) -> None:
    seen = _recent_unregistered.setdefault(_token_digest(token), {})
    seen[str(chat_id)] = (time.monotonic(), str(name or ""))


def recent_unregistered_chat(token: str) -> tuple[str, str] | None:
    seen = _recent_unregistered.get(_token_digest(token)) or {}
    now = time.monotonic()
    live = {cid: v for cid, v in seen.items() if now - v[0] <= _UNREGISTERED_TTL_S}
    if live:
        _recent_unregistered[_token_digest(token)] = live
    else:
        _recent_unregistered.pop(_token_digest(token), None)
    if len(live) != 1:
        return None  # 없거나 여러 명 — 누가 주인인지 알 수 없다
    cid, (_, name) = next(iter(live.items()))
    return cid, name
