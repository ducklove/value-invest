import asyncio
import hashlib
import hmac
import os
import secrets
from datetime import datetime, timedelta
from pathlib import Path

from google.auth.transport import requests as google_requests
from google.oauth2 import id_token

SESSION_COOKIE_NAME = "value_invest_session"
SESSION_MAX_AGE_SECONDS = 60 * 60 * 24 * 30


def _load_keys() -> dict[str, str]:
    keys_path = Path(__file__).parent / "keys.txt"
    values: dict[str, str] = {}
    if not keys_path.exists():
        return values

    for raw_line in keys_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        name, value = line.split("=", 1)
        values[name.strip()] = value.strip()
    return values


_KEYS_CACHE: dict[str, str] | None = None


def _keys() -> dict[str, str]:
    """Lazily read keys.txt once.

    keys.txt is a static file so caching it is fine. The point of reading
    lazily is os.getenv: values loaded by core.config.load_environment()
    (e.g. .env.production) must be seen instead of being frozen at import
    time — otherwise importing this module before load_environment() (batch
    scripts, tests) silently disables auth.
    """
    global _KEYS_CACHE
    if _KEYS_CACHE is None:
        _KEYS_CACHE = _load_keys()
    return _KEYS_CACHE


def google_client_id() -> str:
    return os.getenv("GOOGLE_CLIENT_ID") or _keys().get("GOOGLE_CLIENT_ID", "")


def session_secret() -> str:
    return os.getenv("SESSION_SECRET") or _keys().get("SESSION_SECRET", "")


def is_enabled() -> bool:
    return bool(google_client_id() and session_secret())


def public_config() -> dict:
    return {
        "enabled": is_enabled(),
        "google_client_id": google_client_id() if is_enabled() else "",
    }


def new_session_token() -> str:
    return secrets.token_urlsafe(32)


def hash_session_token(token: str) -> str:
    secret = session_secret()
    if not secret:
        raise RuntimeError("SESSION_SECRET is not configured.")
    digest = hmac.new(
        secret.encode("utf-8"),
        token.encode("utf-8"),
        hashlib.sha256,
    )
    return digest.hexdigest()


def session_expiry_iso() -> str:
    return (datetime.now() + timedelta(seconds=SESSION_MAX_AGE_SECONDS)).isoformat()


async def verify_google_credential(credential: str) -> dict:
    return await asyncio.to_thread(_verify_google_credential_sync, credential)


def _verify_google_credential_sync(credential: str) -> dict:
    client_id = google_client_id()
    if not client_id:
        raise RuntimeError("GOOGLE_CLIENT_ID is not configured.")

    idinfo = id_token.verify_oauth2_token(
        credential,
        google_requests.Request(),
        client_id,
    )

    google_sub = (idinfo.get("sub") or "").strip()
    email = (idinfo.get("email") or "").strip().lower()
    if not google_sub or not email:
        raise ValueError("Google account information is incomplete.")

    return {
        "google_sub": google_sub,
        "email": email,
        "name": (idinfo.get("name") or email).strip(),
        "picture": (idinfo.get("picture") or "").strip(),
        "email_verified": bool(idinfo.get("email_verified")),
    }
