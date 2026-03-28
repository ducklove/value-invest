import asyncio
import hashlib
import hmac
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


_KEYS = _load_keys()
GOOGLE_CLIENT_ID = _KEYS.get("GOOGLE_CLIENT_ID", "")
SESSION_SECRET = _KEYS.get("SESSION_SECRET", "")


def is_enabled() -> bool:
    return bool(GOOGLE_CLIENT_ID and SESSION_SECRET)


def public_config() -> dict:
    return {
        "enabled": is_enabled(),
        "google_client_id": GOOGLE_CLIENT_ID if is_enabled() else "",
    }


def new_session_token() -> str:
    return secrets.token_urlsafe(32)


def hash_session_token(token: str) -> str:
    if not SESSION_SECRET:
        raise RuntimeError("SESSION_SECRET is not configured.")
    digest = hmac.new(
        SESSION_SECRET.encode("utf-8"),
        token.encode("utf-8"),
        hashlib.sha256,
    )
    return digest.hexdigest()


def session_expiry_iso() -> str:
    return (datetime.now() + timedelta(seconds=SESSION_MAX_AGE_SECONDS)).isoformat()


async def verify_google_credential(credential: str) -> dict:
    return await asyncio.to_thread(_verify_google_credential_sync, credential)


def _verify_google_credential_sync(credential: str) -> dict:
    if not GOOGLE_CLIENT_ID:
        raise RuntimeError("GOOGLE_CLIENT_ID is not configured.")

    idinfo = id_token.verify_oauth2_token(
        credential,
        google_requests.Request(),
        GOOGLE_CLIENT_ID,
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
