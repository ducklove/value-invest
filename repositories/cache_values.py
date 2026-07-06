"""cache_values 테이블(DB 영속 K/V JSON 캐시) repository.

cache.py 해체(Phase 2)로 이관. namespace+key 한 칸을 읽고 쓰는 저수준
헬퍼와, 그 위의 리포트 캐시(최신 리포트 / 리포트 목록) 래퍼가 산다.
인메모리 TTL 캐시는 cache_layer.MemoryTTLCache 로 별개 계층이다.
"""

from __future__ import annotations

import json
from datetime import datetime

import aiosqlite

from cache_layer import (
    CACHE_NS_LATEST_REPORT,
    CACHE_NS_REPORT_LIST,
    CacheEntry,
    expires_at_for,
    parse_iso,
)
from repositories.db import get_db, transaction


def _ttl_for_minutes(ttl_minutes: int | None) -> float | None:
    if ttl_minutes is None:
        return None
    return float(ttl_minutes) * 60


def _cache_entry_from_row(
    row: aiosqlite.Row,
    *,
    ttl_seconds: float | None = None,
) -> CacheEntry | None:
    cached_at = parse_iso(row["cached_at"])
    if cached_at is None:
        return None
    stored_ttl = row["ttl_seconds"]
    effective_ttl = ttl_seconds if ttl_seconds is not None else (
        float(stored_ttl) if stored_ttl is not None else None
    )
    expires_at = expires_at_for(cached_at, effective_ttl)
    if expires_at is None:
        expires_at = row["expires_at"]
    stale = False
    expires_at_dt = parse_iso(expires_at)
    if expires_at_dt is not None:
        stale = datetime.now() >= expires_at_dt
    try:
        value = json.loads(row["value_json"])
    except (TypeError, json.JSONDecodeError):
        return None
    return CacheEntry(
        key=row["key"],
        value=value,
        cached_at=row["cached_at"],
        expires_at=expires_at,
        ttl_seconds=effective_ttl,
        stale=stale,
    )


async def set_cache_value(
    namespace: str,
    key: str,
    value,
    *,
    ttl_seconds: float | None = None,
) -> CacheEntry:
    """Store one JSON cache value with explicit cached/expires timestamps."""
    cached_at_dt = datetime.now()
    cached_at = cached_at_dt.isoformat(timespec="seconds")
    expires_at = expires_at_for(cached_at_dt, ttl_seconds)
    async with transaction() as db:
        await db.execute(
            """
            INSERT INTO cache_values
                (namespace, key, value_json, cached_at, expires_at, ttl_seconds, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(namespace, key) DO UPDATE SET
                value_json = excluded.value_json,
                cached_at = excluded.cached_at,
                expires_at = excluded.expires_at,
                ttl_seconds = excluded.ttl_seconds,
                updated_at = excluded.updated_at
            """,
            (
                namespace,
                key,
                json.dumps(value, ensure_ascii=False),
                cached_at,
                expires_at,
                ttl_seconds,
                cached_at,
            ),
        )
    return CacheEntry(
        key=key,
        value=value,
        cached_at=cached_at,
        expires_at=expires_at,
        ttl_seconds=ttl_seconds,
        stale=False,
    )


async def get_cache_value_entry(
    namespace: str,
    key: str,
    *,
    ttl_seconds: float | None = None,
    allow_stale: bool = False,
) -> CacheEntry | None:
    """Read one cache value. A stale row is returned only when allowed."""
    db = await get_db()
    cursor = await db.execute(
        """
        SELECT namespace, key, value_json, cached_at, expires_at, ttl_seconds
        FROM cache_values
        WHERE namespace = ? AND key = ?
        """,
        (namespace, key),
    )
    row = await cursor.fetchone()
    if not row:
        return None
    entry = _cache_entry_from_row(row, ttl_seconds=ttl_seconds)
    if entry is None:
        return None
    if entry.stale and not allow_stale:
        return None
    return entry.with_value_copy()


async def delete_cache_value(namespace: str, key: str) -> None:
    async with transaction() as db:
        await db.execute(
            "DELETE FROM cache_values WHERE namespace = ? AND key = ?",
            (namespace, key),
        )


# --- 리포트 캐시 래퍼 (reports.latest / reports.list 네임스페이스) ----------


async def save_latest_report(stock_code: str, report: dict):
    await set_cache_value(CACHE_NS_LATEST_REPORT, stock_code, report)


async def get_latest_report(stock_code: str, ttl_minutes: int | None = None) -> dict | None:
    entry = await get_cache_value_entry(
        CACHE_NS_LATEST_REPORT,
        stock_code,
        ttl_seconds=_ttl_for_minutes(ttl_minutes),
        allow_stale=ttl_minutes is None,
    )
    if entry is None:
        return None
    report = entry.copy_value()
    report["_cached_at"] = entry.cached_at
    report["_expires_at"] = entry.expires_at
    report["_stale"] = entry.stale
    return report


async def save_report_list(stock_code: str, reports: list[dict]):
    await set_cache_value(CACHE_NS_REPORT_LIST, stock_code, reports)


async def get_report_list(stock_code: str, ttl_minutes: int | None = None) -> dict | None:
    entry = await get_cache_value_entry(
        CACHE_NS_REPORT_LIST,
        stock_code,
        ttl_seconds=_ttl_for_minutes(ttl_minutes),
        allow_stale=ttl_minutes is None,
    )
    if entry is None:
        return None
    return {
        "reports": entry.copy_value(),
        "fetched_at": entry.cached_at,
        "expires_at": entry.expires_at,
        "stale": entry.stale,
    }
