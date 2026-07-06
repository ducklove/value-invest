"""Tests for repositories/schema.py — ensure_column with identifier allowlist."""

from __future__ import annotations

import pytest

from cache_layer import CACHE_NS_LATEST_REPORT, CACHE_NS_REPORT_LIST
from repositories import schema as schema_mod


@pytest.mark.asyncio
async def test_create_core_schema_creates_representative_tables():
    import aiosqlite

    db = await aiosqlite.connect(":memory:")
    db.row_factory = aiosqlite.Row
    try:
        await schema_mod.create_core_schema(db)
        cursor = await db.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name IN (?, ?, ?, ?)",
            ("users", "user_portfolio", "portfolio_accounts", "cache_values"),
        )
        names = {row["name"] for row in await cursor.fetchall()}
        assert names == {"users", "user_portfolio", "portfolio_accounts", "cache_values"}
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_ensure_column_adds_missing_column(tmp_path):
    """컬럼이 없으면 추가한다."""
    import aiosqlite

    db = await aiosqlite.connect(":memory:")
    db.row_factory = aiosqlite.Row
    await db.execute("CREATE TABLE t (a TEXT)")
    await db.commit()
    try:
        await schema_mod.ensure_column(db, "t", "b", "TEXT NOT NULL DEFAULT 'x'")
        await db.commit()
        cur = await db.execute("PRAGMA table_info(t)")
        names = {row[1] for row in await cur.fetchall()}
        assert {"a", "b"} <= names
        # default 반영 확인
        await db.execute("INSERT INTO t (a) VALUES ('hi')")
        cur = await db.execute("SELECT b FROM t WHERE a='hi'")
        assert (await cur.fetchone())["b"] == "x"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_ensure_column_noop_when_present(tmp_path):
    """컬럼이 이미 있으면 ALTER 를 실행하지 않는다(에러 없이 통과)."""
    import aiosqlite

    db = await aiosqlite.connect(":memory:")
    await db.execute("CREATE TABLE t (a TEXT, b TEXT)")
    await db.commit()
    try:
        await schema_mod.ensure_column(db, "t", "b", "TEXT")
        cur = await db.execute("PRAGMA table_info(t)")
        names = [row[1] for row in await cur.fetchall()]
        # b 가 하나만 존재(중복 추가 시 SQLite 는 에러를 내므로 성공=미추가).
        assert names.count("b") == 1
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_ensure_columns_applies_multiple_specs_in_order():
    import aiosqlite

    db = await aiosqlite.connect(":memory:")
    await db.execute("CREATE TABLE t (a TEXT)")
    await db.commit()
    try:
        await schema_mod.ensure_columns(
            db,
            (
                ("t", "b", "TEXT DEFAULT 'b'"),
                ("t", "c", "INTEGER NOT NULL DEFAULT 3"),
            ),
        )
        cur = await db.execute("PRAGMA table_info(t)")
        names = [row[1] for row in await cur.fetchall()]
        assert names == ["a", "b", "c"]
    finally:
        await db.close()


def test_core_column_migrations_cover_known_late_columns():
    columns = {(table, column) for table, column, _ in schema_mod.CORE_COLUMN_MIGRATIONS}
    assert ("user_portfolio", "target_price") in columns
    assert ("user_portfolio", "account_id") in columns
    assert ("portfolio_alerts", "state_json") in columns
    assert ("portfolio_stock_snapshots", "cost_basis") in columns


@pytest.mark.asyncio
async def test_backfill_legacy_cache_values_promotes_report_tables_idempotently():
    import aiosqlite

    db = await aiosqlite.connect(":memory:")
    db.row_factory = aiosqlite.Row
    await db.executescript(
        """
        CREATE TABLE latest_report_cache (
            stock_code TEXT PRIMARY KEY,
            report_json TEXT NOT NULL,
            fetched_at TEXT NOT NULL
        );
        CREATE TABLE report_list_cache (
            stock_code TEXT PRIMARY KEY,
            reports_json TEXT NOT NULL,
            fetched_at TEXT NOT NULL
        );
        CREATE TABLE cache_values (
            namespace TEXT NOT NULL,
            key TEXT NOT NULL,
            value_json TEXT NOT NULL,
            cached_at TEXT NOT NULL,
            expires_at TEXT,
            ttl_seconds INTEGER,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (namespace, key)
        );
        """
    )
    await db.execute(
        "INSERT INTO latest_report_cache (stock_code, report_json, fetched_at) VALUES ('005930', '{\"a\":1}', '2026-01-01T00:00:00')"
    )
    await db.execute(
        "INSERT INTO report_list_cache (stock_code, reports_json, fetched_at) VALUES ('005930', '[1]', '2026-01-02T00:00:00')"
    )
    try:
        await schema_mod.backfill_legacy_cache_values(db)
        await schema_mod.backfill_legacy_cache_values(db)
        cursor = await db.execute(
            "SELECT namespace, key, value_json, cached_at FROM cache_values ORDER BY namespace"
        )
        rows = [dict(row) for row in await cursor.fetchall()]
        assert rows == [
            {
                "namespace": CACHE_NS_LATEST_REPORT,
                "key": "005930",
                "value_json": '{"a":1}',
                "cached_at": "2026-01-01T00:00:00",
            },
            {
                "namespace": CACHE_NS_REPORT_LIST,
                "key": "005930",
                "value_json": "[1]",
                "cached_at": "2026-01-02T00:00:00",
            },
        ]
    finally:
        await db.close()


def test_validate_identifier_accepts_valid():
    assert schema_mod._validate_identifier("corp_codes", kind="table") == "corp_codes"
    assert schema_mod._validate_identifier("_under_score99", kind="column") == "_under_score99"


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "1starts_with_digit",
        "has space",
        "has;drop",
        "table(x)",
        "한글컬럼",
        "quote'injection",
    ],
)
def test_validate_identifier_rejects_invalid(bad):
    with pytest.raises(ValueError):
        schema_mod._validate_identifier(bad, kind="table")


@pytest.mark.asyncio
async def test_ensure_column_rejects_invalid_table_identifier():
    """잘못된 테이블 식별자는 ALTER 전에 ValueError."""
    import aiosqlite

    db = await aiosqlite.connect(":memory:")
    try:
        with pytest.raises(ValueError):
            await schema_mod.ensure_column(db, "t; DROP TABLE x;--", "col", "TEXT")
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_ensure_column_rejects_invalid_column_identifier():
    """잘못된 컬럼 식별자도 거부."""
    import aiosqlite

    db = await aiosqlite.connect(":memory:")
    await db.execute("CREATE TABLE t (a TEXT)")
    try:
        with pytest.raises(ValueError):
            await schema_mod.ensure_column(db, "t", "col) VALUES (1);--", "TEXT")
    finally:
        await db.close()


