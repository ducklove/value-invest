"""KakaoTalk channel via the "나에게 보내기"(memo) API.

Unlike Telegram (one permanent bot token), Kakao needs per-user OAuth:
* the user logs in + consents to ``talk_message`` (handled by the connect/
  callback routes), which yields an access token (~6h) + refresh token (~2mo);
* tokens live in ``notification_channels.config_json`` (channel='kakao');
* ``send_to_user`` refreshes the access token on demand and persists it, so the
  alert engine just calls ``channels.dispatch`` like any other channel.

Everything is a no-op when ``KAKAO_REST_API_KEY`` is unset.
"""

from __future__ import annotations

import json
import logging
import os
import time
from urllib.parse import urlencode

import httpx

import cache


logger = logging.getLogger(__name__)

AUTH_BASE = "https://kauth.kakao.com"
API_BASE = "https://kapi.kakao.com"
MEMO_TEXT_MAX = 200  # Kakao default text template limit.
_APP_URL = "https://cantabile.tplinkdns.com:3691"


def _rest_key() -> str:
    return (os.getenv("KAKAO_REST_API_KEY") or "").strip()


def is_configured() -> bool:
    return bool(_rest_key())


def redirect_uri(request_base: str | None = None) -> str:
    explicit = (os.getenv("KAKAO_REDIRECT_URI") or "").strip()
    if explicit:
        return explicit
    base = (os.getenv("PUBLIC_API_BASE_URL") or "").rstrip("/")
    if not base and request_base:
        base = str(request_base).rstrip("/")
    return f"{base}/api/notifications/kakao/callback" if base else ""


def authorize_url(state: str, *, request_base: str | None = None) -> str:
    params = {
        "client_id": _rest_key(),
        "redirect_uri": redirect_uri(request_base),
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


async def exchange_code(code: str, *, request_base: str | None = None) -> dict:
    data = {
        "grant_type": "authorization_code",
        "client_id": _rest_key(),
        "redirect_uri": redirect_uri(request_base),
        "code": code,
    }
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(f"{AUTH_BASE}/oauth/token", data=data)
        payload = resp.json()
    if "access_token" not in payload:
        raise RuntimeError(f"kakao token exchange failed: {payload}")
    return _store_format(payload)


async def _refresh_token(refresh: str) -> dict:
    data = {"grant_type": "refresh_token", "client_id": _rest_key(), "refresh_token": refresh}
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


async def _send_memo(access_token: str, text: str) -> int:
    template = {
        "object_type": "text",
        "text": text[:MEMO_TEXT_MAX],
        "link": {"web_url": _APP_URL, "mobile_web_url": _APP_URL},
    }
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(
            f"{API_BASE}/v2/api/talk/memo/default/send",
            headers={"Authorization": f"Bearer {access_token}"},
            data={"template_object": json.dumps(template, ensure_ascii=False)},
        )
    return resp.status_code


def _expired(expires_at) -> bool:
    try:
        return not expires_at or time.time() >= float(expires_at)
    except (TypeError, ValueError):
        return True


async def _refresh_into(google_sub: str, config: dict, enabled: bool) -> bool:
    """Refresh the access token in-place and persist. Returns success."""
    refresh = config.get("refresh_token")
    if not refresh:
        return False
    try:
        fresh = await _refresh_token(refresh)
    except Exception as exc:
        logger.warning("kakao refresh failed user=%s: %s", google_sub[:8], exc)
        # Refresh token is dead — flag for reconnect so the UI can prompt.
        try:
            await cache.upsert_notification_channel(
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
    await cache.upsert_notification_channel(
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
        status = await _send_memo(config["access_token"], text)
        if status == 401:
            if not await _refresh_into(google_sub, config, enabled):
                return False
            status = await _send_memo(config["access_token"], text)
        if status != 200:
            logger.warning("kakao memo failed status=%s user=%s", status, google_sub[:8])
            return False
        return True
    except Exception as exc:
        logger.warning("kakao send error user=%s: %s", google_sub[:8], exc)
        return False
