"""Tests for repositories/schema.py — ensure_column with identifier allowlist."""

from __future__ import annotations

import pytest

from repositories import schema as schema_mod


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


@pytest.mark.asyncio
async def test_cache_ensure_column_delegates_to_schema(tmp_path):
    """cache._ensure_column 이 repositories.schema.ensure_column 과 동일 동작."""
    import aiosqlite

    import cache

    db = await aiosqlite.connect(":memory:")
    db.row_factory = aiosqlite.Row
    await db.execute("CREATE TABLE my_table (a TEXT)")
    await db.commit()
    try:
        await cache._ensure_column(db, "my_table", "new_col", "TEXT DEFAULT 'z'")
        await db.commit()
        cur = await db.execute("PRAGMA table_info(my_table)")
        names = {row[1] for row in await cur.fetchall()}
        assert "new_col" in names
    finally:
        await db.close()
