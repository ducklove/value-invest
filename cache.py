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
from repositories import accounts as accounts_repo
from repositories import db as _db
from repositories import schema as schema_repo

# 커넥션 싱글톤의 원본은 repositories/db.py 로 이동했다. 아래는 기존
# ``cache.get_db()`` / ``cache.DB_PATH`` 호출부(레거시 모듈, deploy.sh
# repair 스크립트)를 위한 얇은 재수출. 주의: DB_PATH 는 읽기 전용 별칭
# 이라 테스트/스크립트가 경로를 바꾸려면 repositories.db.DB_PATH 를
# 패치해야 한다 — get_db() 가 호출 시점에 그쪽 전역을 읽는다.
DB_PATH = _db.DB_PATH
get_db = _db.get_db
transaction = _db.transaction

_corp_code_table: dict[str, dict[str, str]] | None = None


async def close_db():
    """Shutdown: close the shared connection (+ cache.py 의 메모리 테이블 리셋)."""
    global _corp_code_table
    await _db.close_db()
    _corp_code_table = None


async def init_db():
    # Migration-only repository helpers (one-time backfills below). Imported
    # locally so cache.py's module surface stays free of repository names.
    from repositories.portfolio import backfill_portfolio_defaults
    from repositories.snapshots import ensure_initial_snapshot_backfills

    db = await get_db()
    await schema_repo.create_core_schema(db)
    await schema_repo.apply_core_column_migrations(db)
    await db.execute(
        """
        UPDATE users
        SET google_identity_sub = google_sub
        WHERE google_identity_sub IS NULL
          AND google_sub NOT LIKE 'local:%'
        """
    )
    await db.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_google_identity_sub "
        "ON users(google_identity_sub) WHERE google_identity_sub IS NOT NULL"
    )
    await db.execute("CREATE INDEX IF NOT EXISTS idx_users_email_lower ON users(lower(email))")
    await schema_repo.backfill_legacy_cache_values(db)
    await db.execute("CREATE INDEX IF NOT EXISTS idx_stock_snapshots_sub_group_date ON portfolio_stock_snapshots(google_sub, group_name, date)")
    await ensure_initial_snapshot_backfills(db)
    await backfill_portfolio_defaults(db)
    await db.commit()


async def _ensure_column(db: aiosqlite.Connection, table: str, column: str, definition: str):
    # 구현은 repositories.schema.ensure_column 으로 이관(ST-01). 동작은 동일하되
    # 식별자 allowlist 검증이 추가됐다 — 호출부는 모두 하드코딩 상수이므로 정상
    # 경로에선 영향 없고, 외부 값이 섞여 들어오는 실수를 방어한다.
    from repositories.schema import ensure_column

    await ensure_column(db, table, column, definition)


async def _ensure_default_account(db: aiosqlite.Connection, google_sub: str) -> None:
    """Compatibility wrapper; implementation lives in repositories.accounts."""
    await accounts_repo.ensure_default_account(db, google_sub)


async def _backfill_legacy_cache_values(db: aiosqlite.Connection) -> None:
    """Compatibility wrapper; implementation lives in repositories.schema."""
    await schema_repo.backfill_legacy_cache_values(db)


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
    db = await get_db()
    cached_at_dt = datetime.now()
    cached_at = cached_at_dt.isoformat(timespec="seconds")
    expires_at = expires_at_for(cached_at_dt, ttl_seconds)
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
    await db.commit()
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
    db = await get_db()
    await db.execute(
        "DELETE FROM cache_values WHERE namespace = ? AND key = ?",
        (namespace, key),
    )
    await db.commit()


async def is_corp_codes_loaded() -> bool:
    db = await get_db()
    cursor = await db.execute("SELECT COUNT(*) FROM corp_codes")
    row = await cursor.fetchone()
    return row[0] > 0


async def corp_codes_need_refresh() -> bool:
    db = await get_db()
    cursor = await db.execute(
        "SELECT COUNT(*) AS total, SUM(CASE WHEN modify_date IS NOT NULL AND modify_date != '' THEN 1 ELSE 0 END) AS filled FROM corp_codes"
    )
    row = await cursor.fetchone()
    total = row["total"] or 0
    filled = row["filled"] or 0
    return total > 0 and filled == 0


async def save_corp_codes(codes: list[dict]):
    global _corp_code_table
    db = await get_db()
    now = datetime.now().isoformat()
    await db.executemany(
        "INSERT OR REPLACE INTO corp_codes (stock_code, corp_code, corp_name, modify_date, updated_at) VALUES (?, ?, ?, ?, ?)",
        [(c["stock_code"], c["corp_code"], c["corp_name"], c.get("modify_date"), now) for c in codes],
    )
    await db.commit()
    _corp_code_table = {
        str(c["stock_code"]): {
            "stock_code": str(c["stock_code"]),
            "corp_code": str(c.get("corp_code") or ""),
            "corp_name": str(c.get("corp_name") or ""),
        }
        for c in codes
        if c.get("stock_code") and c.get("corp_name")
    }


_CORP_SEARCH_ALIASES = {
    # DART stores the KCC parent company as the Korean legal name, so a plain
    # "KCC" search otherwise only finds KCC건설.
    "KCC": ["002380"],
}


def _corp_search_alias_codes(query: str) -> list[str]:
    return _CORP_SEARCH_ALIASES.get((query or "").strip().upper(), [])


async def search_corp(query: str) -> list[dict]:
    query = (query or "").strip()
    db = await get_db()
    alias_rows = []
    for stock_code in _corp_search_alias_codes(query):
        cursor = await db.execute(
            "SELECT stock_code, corp_code, corp_name FROM corp_codes WHERE stock_code = ?",
            (stock_code,),
        )
        row = await cursor.fetchone()
        if row:
            alias_rows.append(dict(row))

    cursor = await db.execute(
        "SELECT stock_code, corp_code, corp_name FROM corp_codes "
        "WHERE corp_name LIKE ? OR stock_code LIKE ? "
        "ORDER BY "
        "CASE "
        "WHEN stock_code = ? THEN 0 "
        "WHEN corp_name = ? THEN 1 "
        "WHEN corp_name LIKE ? THEN 2 "
        "ELSE 3 END, "
        "COALESCE(modify_date, '') DESC, "
        "LENGTH(corp_name), stock_code "
        "LIMIT 20",
        (f"%{query}%", f"%{query}%", query, query, f"{query}%"),
    )
    rows = await cursor.fetchall()
    results = []
    seen_codes = set()
    exact_name_seen = set()
    for item in [*alias_rows, *(dict(row) for row in rows)]:
        if item["stock_code"] in seen_codes:
            continue
        seen_codes.add(item["stock_code"])
        if item["corp_name"] == query:
            if item["corp_name"] in exact_name_seen:
                continue
            exact_name_seen.add(item["corp_name"])
        results.append(item)
    return results


async def resolve_corp_search_query(query: str) -> dict | None:
    query = (query or "").strip()
    if not query:
        return None
    rows = await search_corp(query)
    if not rows:
        return None
    first = rows[0]
    normalized = query.upper()
    if (
        _corp_search_alias_codes(query)
        or first["stock_code"] == query
        or first["corp_name"] == query
        or first["corp_name"].upper() == normalized
    ):
        return first
    return None


async def get_corp_code(stock_code: str) -> str | None:
    row = (await load_corp_code_table()).get(str(stock_code or "").strip())
    return row["corp_code"] if row else None


async def get_corp_name(stock_code: str) -> str | None:
    row = (await load_corp_code_table()).get(str(stock_code or "").strip())
    return row["corp_name"] if row else None


async def resolve_stock_name(stock_code: str) -> str | None:
    name = await get_corp_name(stock_code)
    if name:
        return name
    return None


async def load_corp_code_table(*, force: bool = False) -> dict[str, dict[str, str]]:
    """Return the full internal listed-company code table.

    This is a reference table, not a best-effort quote/name cache. It is
    refreshed from DART at startup and then kept in memory for fast UI paths
    such as benchmark label rendering.
    """
    global _corp_code_table
    if _corp_code_table is not None and not force:
        return _corp_code_table
    db = await get_db()
    cursor = await db.execute(
        "SELECT stock_code, corp_code, corp_name FROM corp_codes WHERE stock_code IS NOT NULL AND stock_code != ''"
    )
    _corp_code_table = {
        row["stock_code"]: {
            "stock_code": row["stock_code"],
            "corp_code": row["corp_code"],
            "corp_name": row["corp_name"],
        }
        for row in await cursor.fetchall()
    }
    return _corp_code_table


async def get_db_stats() -> dict:
    db = await get_db()
    tables = {}
    cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    for row in await cursor.fetchall():
        tname = row["name"]
        cnt = await db.execute(f"SELECT COUNT(*) as c FROM [{tname}]")
        tables[tname] = (await cnt.fetchone())["c"]
    # DB file size
    import os
    db_size = os.path.getsize(_db.DB_PATH) if _db.DB_PATH.exists() else 0
    return {"tables": tables, "db_size_bytes": db_size}


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
