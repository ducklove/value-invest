from __future__ import annotations

import time
from collections import deque

from fastapi import HTTPException, Request

_RATE_LIMIT_BUCKETS: dict[str, deque[float]] = {}


def client_identity(request: Request) -> str:
    forwarded_for = request.headers.get("x-forwarded-for", "")
    if forwarded_for:
        first_forwarded = forwarded_for.split(",", 1)[0].strip()
        if first_forwarded:
            return first_forwarded
    real_ip = request.headers.get("x-real-ip", "").strip()
    if real_ip:
        return real_ip
    return request.client.host if request.client else "unknown"


def rate_limit_identity(request: Request, user: dict | None = None) -> str:
    google_sub = (user or {}).get("google_sub")
    if google_sub:
        return f"user:{google_sub}"
    return f"ip:{client_identity(request)}"


def _trim(bucket: deque[float], now: float, window_seconds: int) -> None:
    cutoff = now - window_seconds
    while bucket and bucket[0] < cutoff:
        bucket.popleft()


def enforce_rate_limit(
    request: Request,
    *,
    scope: str,
    user: dict | None = None,
    max_requests: int,
    window_seconds: int,
    detail: str,
) -> None:
    if max_requests <= 0 or window_seconds <= 0:
        return
    now = time.monotonic()
    key = f"{scope}:{rate_limit_identity(request, user)}"
    bucket = _RATE_LIMIT_BUCKETS.setdefault(key, deque())
    _trim(bucket, now, window_seconds)
    if len(bucket) >= max_requests:
        raise HTTPException(status_code=429, detail=detail)
    bucket.append(now)


def reset_rate_limits() -> None:
    _RATE_LIMIT_BUCKETS.clear()
