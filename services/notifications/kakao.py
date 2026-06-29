"""KakaoTalk channel via the "나에게 보내기"(memo) API — per-user app keys.

Each user registers their OWN Kakao app (REST API key). Because they are the
admin of their own app, the ``talk_message`` "나에게 보내기" scope works for
them **without** Kakao's business review — that review/test-user gating only
bites when many users share one operator app.

Per-user flow:
* the user pastes their REST API key + registers our redirect URI in their app;
* OAuth (connect/callback routes) yields access(~6h)+refresh(~2mo) tokens;
* tokens + the rest_key live in ``notification_channels.config_json``;
* ``send_to_user`` refreshes on demand using the stored rest_key.

``KAKAO_REST_API_KEY`` env remains an optional fallback (shared operator app).
"""

from __future__ import annotations

import json
import logging
import os
import time
from urllib.parse import urlencode

import httpx

from repositories import notifications as notifications_repo


logger = logging.getLogger(__name__)

AUTH_BASE = "https://kauth.kakao.com"
API_BASE = "https://kapi.kakao.com"
MEMO_TEXT_MAX = 200  # Kakao default text template limit.
_APP_URL = "https://cantabile.tplinkdns.com:3691"


def env_key() -> str:
    return (os.getenv("KAKAO_REST_API_KEY") or "").strip()


def is_configured() -> bool:
    """Whether the optional server-wide fallback app key is set. Per-user app
    keys work regardless of this."""
    return bool(env_key())


def redirect_uri(request_base: str | None = None) -> str:
    explicit = (os.getenv("KAKAO_REDIRECT_URI") or "").strip()
    if explicit:
        return explicit
    base = (os.getenv("PUBLIC_API_BASE_URL") or "").rstrip("/")
    if not base and request_base:
        base = str(request_base).rstrip("/")
    return f"{base}/api/notifications/kakao/callback" if base else ""


def authorize_url(rest_key: str, state: str, redirect: str) -> str:
    params = {
        "client_id": rest_key,
        "redirect_uri": redirect,
        "response_type": "code",
        "scope": "talk_message",
        "state": state,
    }
    return f"{AUTH_BASE}/oauth/authorize?{urlencode(params)}"


def _store_format(payload: dict) -> dict:
    now = time.time()
    out = {
        "access_token": payload["access_token"],
        "access_expires_at": now + float(payload.get("expires_in", 21600)) - 60,
    }
    if payload.get("refresh_token"):
        out["refresh_token"] = payload["refresh_token"]
        out["refresh_expires_at"] = now + float(payload.get("refresh_token_expires_in", 5184000)) - 60
    return out


async def exchange_code(rest_key: str, code: str, redirect: str) -> dict:
    data = {
        "grant_type": "authorization_code",
        "client_id": rest_key,
        "redirect_uri": redirect,
        "code": code,
    }
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(f"{AUTH_BASE}/oauth/token", data=data)
        payload = resp.json()
    if "access_token" not in payload:
        raise RuntimeError(f"kakao token exchange failed: {payload}")
    return _store_format(payload)


async def _refresh_token(rest_key: str, refresh: str) -> dict:
    data = {"grant_type": "refresh_token", "client_id": rest_key, "refresh_token": refresh}
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(f"{AUTH_BASE}/oauth/token", data=data)
        payload = resp.json()
    if "access_token" not in payload:
        raise RuntimeError(f"kakao refresh failed: {payload}")
    return _store_format(payload)


async def fetch_nickname(access_token: str) -> str | None:
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{API_BASE}/v2/user/me",
                headers={"Authorization": f"Bearer {access_token}"},
            )
            data = resp.json()
        return ((data.get("properties") or {}).get("nickname")) or None
    except Exception:
        return None


def split_memo_text(text: str, limit: int = MEMO_TEXT_MAX) -> list[str]:
    """Split Kakao text templates without cutting normal briefing lines."""
    src = str(text or "").strip()
    if not src:
        return []
    chunks: list[str] = []
    current = ""
    for line in src.splitlines():
        line = line.rstrip()
        if not line:
            continue
        while len(line) > limit:
            if current:
                chunks.append(current)
                current = ""
            chunks.append(line[:limit])
            line = line[limit:]
        candidate = line if not current else f"{current}\n{line}"
        if len(candidate) <= limit:
            current = candidate
        else:
            if current:
                chunks.append(current)
            current = line
    if current:
        chunks.append(current)
    return chunks


async def _send_memo(access_token: str, text: str) -> int:
    template = {
        "object_type": "text",
        "text": text,
        "link": {"web_url": _APP_URL, "mobile_web_url": _APP_URL},
    }
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            f"{API_BASE}/v2/api/talk/memo/default/send",
            headers={"Authorization": f"Bearer {access_token}"},
            data={"template_object": json.dumps(template, ensure_ascii=False)},
        )
    return resp.status_code


async def _send_memos(access_token: str, text: str) -> int:
    chunks = split_memo_text(text)
    if not chunks:
        return 400
    for chunk in chunks:
        status = await _send_memo(access_token, chunk)
        if status != 200:
            return status
    return 200


def _expired(expires_at) -> bool:
    try:
        return not expires_at or time.time() >= float(expires_at)
    except (TypeError, ValueError):
        return True


async def _refresh_into(google_sub: str, config: dict, enabled: bool) -> bool:
    """Refresh the access token in-place (using the per-user rest_key) and
    persist. Returns success."""
    refresh = config.get("refresh_token")
    rest_key = config.get("rest_key") or env_key()
    if not refresh or not rest_key:
        return False
    try:
        fresh = await _refresh_token(rest_key, refresh)
    except Exception as exc:
        logger.warning("kakao refresh failed user=%s: %s", google_sub[:8], exc)
        try:
            await notifications_repo.upsert_notification_channel(
                google_sub, "kakao", config=config, enabled=enabled, verified=False
            )
        except Exception:
            pass
        return False
    config["access_token"] = fresh["access_token"]
    config["access_expires_at"] = fresh["access_expires_at"]
    if fresh.get("refresh_token"):
        config["refresh_token"] = fresh["refresh_token"]
        config["refresh_expires_at"] = fresh["refresh_expires_at"]
    await notifications_repo.upsert_notification_channel(
        google_sub, "kakao", config=config, enabled=enabled, verified=True
    )
    return True


async def send_to_user(google_sub: str, channel: dict, text: str) -> bool:
    """Send a memo to one user, refreshing the token if needed. Never raises."""
    config = dict(channel.get("config") or {})
    enabled = bool(channel.get("enabled", True))
    try:
        if _expired(config.get("access_expires_at")) or not config.get("access_token"):
            if not await _refresh_into(google_sub, config, enabled):
                return False
        status = await _send_memos(config["access_token"], text)
        if status == 401:
            if not await _refresh_into(google_sub, config, enabled):
                return False
            status = await _send_memos(config["access_token"], text)
        if status != 200:
            logger.warning("kakao memo failed status=%s user=%s", status, google_sub[:8])
            return False
        return True
    except Exception as exc:
        logger.warning("kakao send error user=%s: %s", google_sub[:8], exc)
        return False
