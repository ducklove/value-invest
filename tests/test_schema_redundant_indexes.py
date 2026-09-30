"""O9 — indexes that duplicate a PRIMARY KEY (or its leftmost prefix) are gone."""

from __future__ import annotations

import sqlite3

import aiosqlite
import pytest

from repositories import schema as schema_mod

REDUNDANT = {name: (table, cols) for name, table, cols in schema_mod.REDUNDANT_PK_INDEXES}


async def _index_names(db) -> set[str]:
    async with db.execute("SELECT name FROM sqlite_master WHERE type = 'index'") as cur:
        return {row[0] for row in await cur.fetchall()}


async def _plan(db, sql: str, params=()) -> str:
    async with db.execute("EXPLAIN QUERY PLAN " + sql, params) as cur:
        return " | ".join(str(row[3]) for row in await cur.fetchall())


async def _create_old_indexes(db) -> None:
    for name, (table, cols) in REDUNDANT.items():
        await db.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {table}({', '.join(cols)})")
    await db.commit()


def test_schema_sql_no_longer_creates_redundant_indexes():
    sql = schema_mod.CORE_SCHEMA_SQL + schema_mod.INVESTMENT_INSIGHTS_SCHEMA_SQL
    for name in REDUNDANT:
        assert name not in sql, name


def test_each_listed_index_is_a_pk_prefix_in_the_schema_definition():
    con = sqlite3.connect(":memory:")
    try:
        con.executescript(schema_mod.CORE_SCHEMA_SQL)
        for name, (table, cols) in REDUNDANT.items():
            pk = sorted((r for r in con.execute(f"PRAGMA table_info({table})") if r[5] > 0), key=lambda r: r[5])
            pk_cols = tuple(r[1] for r in pk)
            assert pk_cols[: len(cols)] == cols, (name, pk_cols)
    finally:
        con.close()


@pytest.mark.asyncio
async def test_fresh_schema_has_pk_autoindexes_and_no_redundant_indexes():
    db = await aiosqlite.connect(":memory:")
    try:
        await schema_mod.create_core_schema(db)
        names = await _index_names(db)
        assert not set(REDUNDANT) & names
        for table in {t for t, _ in REDUNDANT.values()}:
            assert f"sqlite_autoindex_{table}_1" in names, table
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_existing_db_with_old_indexes_drops_them_idempotently():
    db = await aiosqlite.connect(":memory:")
    try:
        await schema_mod.create_core_schema(db)
        await _create_old_indexes(db)
        assert set(REDUNDANT) <= await _index_names(db)

        await schema_mod.create_core_schema(db)
        assert not set(REDUNDANT) & await _index_names(db)
        # Second pass is a no-op.
        assert await schema_mod.drop_redundant_pk_indexes(db) == []
        await schema_mod.create_core_schema(db)
        assert not set(REDUNDANT) & await _index_names(db)
        # Unrelated indexes survive.
        names = await _index_names(db)
        assert "idx_portfolio_cashflows_sub" in names
        assert "idx_stock_weight_snapshots_sub_group_date" in names
        assert "idx_rebalance_targets_user" in names
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_index_kept_when_table_lacks_the_primary_key():
    # A legacy DB whose table predates the PK must keep its only index.
    db = await aiosqlite.connect(":memory:")
    try:
        await db.execute("CREATE TABLE portfolio_intraday (google_sub TEXT NOT NULL, ts TEXT NOT NULL, total_value REAL)")
        await db.execute("CREATE INDEX idx_intraday_sub_ts ON portfolio_intraday(google_sub, ts)")
        await db.commit()
        await schema_mod.create_core_schema(db)
        assert "idx_intraday_sub_ts" in await _index_names(db)
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_same_name_with_different_columns_is_kept():
    db = await aiosqlite.connect(":memory:")
    try:
        await schema_mod.create_core_schema(db)
        await db.execute("CREATE INDEX idx_nps_holdings_date ON nps_holdings(stock_code)")
        await db.commit()
        assert await schema_mod.drop_redundant_pk_indexes(db) == []
        assert "idx_nps_holdings_date" in await _index_names(db)
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_snapshot_queries_use_the_pk_autoindex():
    db = await aiosqlite.connect(":memory:")
    try:
        await schema_mod.create_core_schema(db)
        cases = [
            ("SELECT ts, total_value FROM portfolio_intraday WHERE google_sub = ? AND ts >= ? ORDER BY ts",
             ("u", "2026-09-30"), "sqlite_autoindex_portfolio_intraday_1"),
            ("SELECT * FROM portfolio_snapshots WHERE google_sub = ? ORDER BY date",
             ("u",), "sqlite_autoindex_portfolio_snapshots_1"),
            ("SELECT * FROM portfolio_stock_snapshots WHERE google_sub = ? AND date = ?",
             ("u", "2026-09-30"), "sqlite_autoindex_portfolio_stock_snapshots_1"),
            ("SELECT * FROM portfolio_group_snapshots WHERE google_sub = ? AND date = ?",
             ("u", "2026-09-30"), "sqlite_autoindex_portfolio_group_snapshots_1"),
            ("SELECT close FROM benchmark_daily WHERE code = ? AND date BETWEEN ? AND ? ORDER BY date",
             ("KOSPI", "2026-01-01", "2026-09-30"), "sqlite_autoindex_benchmark_daily_1"),
        ]
        for sql, params, expected in cases:
            plan = await _plan(db, sql, params)
            assert expected in plan, (sql, plan)
            assert "SCAN" not in plan.replace("USING INDEX", ""), (sql, plan)
    finally:
        await db.close()


async def test_init_db_leaves_redundant_indexes_absent(temp_db):
    from repositories import bootstrap
    from repositories import db as db_repo

    db = await db_repo.get_db()
    await _create_old_indexes(db)
    await bootstrap.init_db()
    await bootstrap.init_db()
    assert not set(REDUNDANT) & await _index_names(db)
