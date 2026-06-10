"""Shared dependencies used across route modules."""

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta

from fastapi import Request, Response

import auth_service
import cache
from services import stock_quotes

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(os.getenv(name, default)))
    except (TypeError, ValueError):
        return default


# --- Constants ---
ANALYSIS_CONCURRENCY = _env_int("ANALYSIS_CONCURRENCY", 2)
ANALYSIS_SEMAPHORE = asyncio.Semaphore(ANALYSIS_CONCURRENCY)
ANALYSIS_LOCKS: dict[str, asyncio.Lock] = {}
ANALYSIS_LOCKS_GUARD = asyncio.Lock()
# 종목코드마다 락이 하나씩 쌓이므로 상한을 두고, 넘으면 아무도 잡고 있지 않은
# 오래된 락부터 비운다. dict 삽입 순서가 곧 LRU 근사다.
ANALYSIS_LOCKS_MAX = _env_int("ANALYSIS_LOCKS_MAX", 512)


async def get_analysis_lock(stock_code: str) -> asyncio.Lock:
    async with ANALYSIS_LOCKS_GUARD:
        lock = ANALYSIS_LOCKS.pop(stock_code, None)
        if lock is None:
            lock = asyncio.Lock()
        ANALYSIS_LOCKS[stock_code] = lock  # 재삽입으로 최신 사용 순서 유지
        if len(ANALYSIS_LOCKS) > ANALYSIS_LOCKS_MAX:
            for code, candidate in list(ANALYSIS_LOCKS.items()):
                if len(ANALYSIS_LOCKS) <= ANALYSIS_LOCKS_MAX:
                    break
                if code != stock_code and not candidate.locked():
                    del ANALYSIS_LOCKS[code]
        return lock
LATEST_REPORT_CACHE_TTL_MINUTES = 1440  # 24h — 최신 리포트는 하루 1회만 재수집
REPORT_LIST_CACHE_TTL_MINUTES = 60
ANALYSIS_SNAPSHOT_TTL_MINUTES = 60
RECENT_QUOTES_SEMAPHORE = asyncio.Semaphore(4)
SESSION_COOKIE_NAME = auth_service.SESSION_COOKIE_NAME
TRUSTED_RETURN_ORIGINS = {
    "http://localhost:8000",
    "http://127.0.0.1:8000",
    "http://localhost:8010",
    "http://127.0.0.1:8010",
    "https://ducklove.github.io",
    "https://cantabile.tplinkdns.com:3691",
}


# --- Auth / Session helpers ---

def is_secure_request(request: Request) -> bool:
    forwarded_proto = request.headers.get("x-forwarded-proto", "").split(",", 1)[0].strip()
    return request.url.scheme == "https" or forwarded_proto == "https"


def session_cookie_samesite(request: Request) -> str:
    return "none" if is_secure_request(request) else "lax"


def serialize_user(user: dict | None) -> dict | None:
    if not user:
        return None
    result = {
        "google_sub": user["google_sub"],
        "email": user["email"],
        "name": user["name"],
        "picture": user.get("picture") or "",
        "email_verified": bool(user.get("email_verified")),
    }
    if user.get("is_admin"):
        result["is_admin"] = True
    return result


async def get_current_user(request: Request) -> dict | None:
    if not auth_service.is_enabled():
        return None
    session_token = request.cookies.get(SESSION_COOKIE_NAME)
    if not session_token:
        return None
    try:
        token_hash = auth_service.hash_session_token(session_token)
    except RuntimeError:
        return None
    return await cache.get_user_by_session(token_hash)


def set_session_cookie(response: Response, request: Request, session_token: str):
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=session_token,
        max_age=auth_service.SESSION_MAX_AGE_SECONDS,
        httponly=True,
        secure=is_secure_request(request),
        samesite=session_cookie_samesite(request),
        path="/",
    )


def clear_session_cookie(response: Response, request: Request):
    response.delete_cookie(
        key=SESSION_COOKIE_NAME,
        path="/",
        secure=is_secure_request(request),
        samesite=session_cookie_samesite(request),
    )


def default_user_preference() -> dict:
    return {
        "is_starred": False,
        "is_pinned": False,
        "note": "",
        "updated_at": None,
    }


# --- Datetime / SSE helpers ---

def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def analysis_snapshot_is_stale(analyzed_at: str | None) -> bool:
    analyzed_dt = parse_iso_datetime(analyzed_at)
    if analyzed_dt is None:
        return True
    return datetime.now() - analyzed_dt > timedelta(minutes=ANALYSIS_SNAPSHOT_TTL_MINUTES)


def sse_event(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


# --- Quote helpers ---

def has_quote_snapshot(quote_snapshot: dict | None) -> bool:
    return isinstance(quote_snapshot, dict) and quote_snapshot.get("price") is not None


async def attach_quote_snapshots(items: list[dict]) -> list[dict]:
    async def enrich(item: dict) -> dict:
        enriched = dict(item)
        if has_quote_snapshot(enriched.get("quote_snapshot")):
            return enriched
        try:
            async with RECENT_QUOTES_SEMAPHORE:
                enriched["quote_snapshot"] = await stock_quotes.get_quote_snapshot(item["stock_code"])
        except Exception as exc:
            logger.warning("사이드바 현재가 조회 실패(%s): %s", item.get("stock_code"), exc)
            enriched["quote_snapshot"] = {}
        return enriched

    return await asyncio.gather(*(enrich(item) for item in items))
