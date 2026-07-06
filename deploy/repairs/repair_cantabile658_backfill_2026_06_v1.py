# ruff: noqa: E402, I001
from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.config import load_environment  # noqa: E402

load_environment(ROOT, force=True)

from repositories import bootstrap

from repositories import db as db_repo
import snapshot_nav  # noqa: E402
from repositories import snapshots as snapshots_repo  # noqa: E402


TARGET_EMAIL = "cantabile658@gmail.com"
TARGET_LOCAL_SUB = "local:cantabile658-gmail-com"
SOURCE_EMAIL = "ducklv@gmail.com"
BASELINE_DATE = "2026-05-31"
BASELINE_CLOSE_DATE = "2026-05-29"
START_DATE = date(2026, 6, 1)
END_DATE = date(2026, 6, 18)
BASE_NAV = 1000.0
COPY_TABLES = (
    "portfolio_groups",
    "user_portfolio",
    "portfolio_tags",
    "user_stock_preferences",
)


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def backup_db() -> str:
    db_path = Path("cache.db").resolve()
    backup_dir = Path("data/db-imports")
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = backup_dir / f"cache.before-cantabile658-live-backfill.{stamp}.db"
    src = sqlite3.connect(db_path)
    dst = sqlite3.connect(backup_path)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    return str(backup_path)


async def fetchone(db, sql: str, params: tuple = ()) -> dict | None:
    cursor = await db.execute(sql, params)
    row = await cursor.fetchone()
    return dict(row) if row else None


async def fetchval(db, sql: str, params: tuple = ()):
    cursor = await db.execute(sql, params)
    row = await cursor.fetchone()
    if not row:
        return None
    return row[0]


async def table_columns(db, table: str) -> list[str]:
    cursor = await db.execute(f"PRAGMA table_info({quote_ident(table)})")
    return [row["name"] for row in await cursor.fetchall()]


async def ensure_target_user(db) -> tuple[str, bool]:
    now = datetime.now().isoformat(timespec="seconds")
    existing = await fetchone(
        db,
        "SELECT * FROM users WHERE lower(email) = lower(?) LIMIT 1",
        (TARGET_EMAIL,),
    )
    if existing:
        return existing["google_sub"], False

    existing_local = await fetchone(
        db,
        "SELECT * FROM users WHERE google_sub = ? LIMIT 1",
        (TARGET_LOCAL_SUB,),
    )
    if existing_local:
        if str(existing_local.get("email") or "").lower() != TARGET_EMAIL.lower():
            raise RuntimeError(f"{TARGET_LOCAL_SUB} already exists with another email")
        return existing_local["google_sub"], False

    columns = await table_columns(db, "users")
    values = {
        "google_sub": TARGET_LOCAL_SUB,
        "email": TARGET_EMAIL,
        "name": "cantabile658",
        "picture": None,
        "email_verified": 1,
        "created_at": now,
        "last_login_at": now,
        "is_admin": 0,
        "password_hash": None,
        "password_updated_at": None,
        "google_identity_sub": None,
    }
    insert_cols = [col for col in columns if col in values]
    placeholders = ",".join("?" for _ in insert_cols)
    await db.execute(
        f"INSERT INTO users ({','.join(quote_ident(c) for c in insert_cols)}) VALUES ({placeholders})",
        tuple(values[col] for col in insert_cols),
    )
    return TARGET_LOCAL_SUB, True


async def find_source_sub(db) -> str:
    source = await fetchone(
        db,
        "SELECT google_sub FROM users WHERE lower(email) = lower(?) LIMIT 1",
        (SOURCE_EMAIL,),
    )
    if source:
        return source["google_sub"]

    source = await fetchone(
        db,
        """
        SELECT google_sub
        FROM user_portfolio
        GROUP BY google_sub
        ORDER BY COUNT(*) DESC
        LIMIT 1
        """,
    )
    if not source:
        raise RuntimeError("No source portfolio exists to copy from")
    return source["google_sub"]


async def copy_table_for_user(db, table: str, source_sub: str, target_sub: str) -> int:
    columns = await table_columns(db, table)
    if "google_sub" not in columns:
        raise RuntimeError(f"{table} does not have google_sub")
    non_user_cols = [col for col in columns if col != "google_sub"]
    insert_cols = ["google_sub", *non_user_cols]
    select_sql = ", ".join(quote_ident(col) for col in non_user_cols)
    if select_sql:
        sql = (
            f"INSERT OR REPLACE INTO {quote_ident(table)} "
            f"({','.join(quote_ident(c) for c in insert_cols)}) "
            f"SELECT ?, {select_sql} FROM {quote_ident(table)} WHERE google_sub = ?"
        )
    else:
        sql = f"INSERT OR REPLACE INTO {quote_ident(table)} (google_sub) SELECT ? FROM {quote_ident(table)} WHERE google_sub = ?"
    cursor = await db.execute(sql, (target_sub, source_sub))
    return int(cursor.rowcount or 0)


async def clear_generated_snapshots(db, target_sub: str) -> None:
    await db.execute(
        "DELETE FROM portfolio_intraday WHERE google_sub = ? AND ts >= ?",
        (target_sub, BASELINE_DATE + "T00:00"),
    )
    for table in (
        "portfolio_stock_weight_snapshots",
        "portfolio_group_snapshots",
        "portfolio_stock_snapshots",
        "portfolio_snapshots",
    ):
        await db.execute(
            f"DELETE FROM {quote_ident(table)} WHERE google_sub = ? AND date >= ?",
            (target_sub, BASELINE_DATE),
        )


async def main() -> None:
    backup_path = backup_db()
    await bootstrap.init_db()
    db = await db_repo.get_db()

    source_sub = await find_source_sub(db)
    target_sub, created_user = await ensure_target_user(db)
    source_holdings = int(await fetchval(db, "SELECT COUNT(*) FROM user_portfolio WHERE google_sub = ?", (source_sub,)) or 0)
    target_holdings = int(await fetchval(db, "SELECT COUNT(*) FROM user_portfolio WHERE google_sub = ?", (target_sub,)) or 0)
    copied = {}

    if target_holdings == 0:
        if source_holdings == 0:
            raise RuntimeError("Source portfolio has no holdings")
        await db.execute("BEGIN IMMEDIATE")
        try:
            for table in ("portfolio_tags", "user_stock_preferences", "portfolio_groups", "user_portfolio"):
                await db.execute(f"DELETE FROM {quote_ident(table)} WHERE google_sub = ?", (target_sub,))
            for table in COPY_TABLES:
                copied[table] = await copy_table_for_user(db, table, source_sub, target_sub)
            await db.commit()
        except Exception:
            await db.rollback()
            raise
        target_holdings = int(await fetchval(db, "SELECT COUNT(*) FROM user_portfolio WHERE google_sub = ?", (target_sub,)) or 0)

    if target_holdings == 0:
        raise RuntimeError("Target portfolio is empty after preparation")

    await clear_generated_snapshots(db, target_sub)
    await db.commit()

    await snapshot_nav._fetch_fx_usdkrw()
    total_value, total_invested, per_stock = await snapshot_nav._fetch_total_value(target_sub, BASELINE_CLOSE_DATE)
    if total_value <= 0:
        raise RuntimeError("Baseline total value is zero")
    total_units = total_value / BASE_NAV
    await snapshots_repo.save_snapshot(
        target_sub,
        BASELINE_DATE,
        total_value,
        total_invested,
        BASE_NAV,
        total_units,
        snapshot_nav._fx_usdkrw,
    )
    await snapshots_repo.save_stock_snapshots(target_sub, BASELINE_DATE, per_stock)

    written_dates = [BASELINE_DATE]
    current = START_DATE
    while current <= END_DATE:
        if current.weekday() < 5:
            snap_date = current.isoformat()
            await snapshot_nav.take_snapshot(target_sub, snap_date)
            written_dates.append(snap_date)
        current += timedelta(days=1)

    db = await db_repo.get_db()
    latest = await fetchone(
        db,
        """
        SELECT date, nav, total_value, total_units
        FROM portfolio_snapshots
        WHERE google_sub = ?
        ORDER BY date DESC
        LIMIT 1
        """,
        (target_sub,),
    )
    counts = {}
    for table in (
        "user_portfolio",
        "portfolio_snapshots",
        "portfolio_stock_snapshots",
        "portfolio_group_snapshots",
        "portfolio_stock_weight_snapshots",
    ):
        counts[table] = int(await fetchval(db, f"SELECT COUNT(*) FROM {quote_ident(table)} WHERE google_sub = ?", (target_sub,)) or 0)

    print(json.dumps({
        "ok": True,
        "backup_path": backup_path,
        "source_sub": source_sub,
        "target_sub": target_sub,
        "created_user": created_user,
        "source_holdings": source_holdings,
        "target_holdings": target_holdings,
        "copied": copied,
        "written_dates": written_dates,
        "latest": latest,
        "counts": counts,
    }, ensure_ascii=False, indent=2))
    await bootstrap.close_db()


asyncio.run(main())
