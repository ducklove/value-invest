from __future__ import annotations

import argparse
import asyncio
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import cache  # noqa: E402
import repositories.db  # noqa: E402

USER_TABLES = [
    "portfolio_groups",
    "user_portfolio",
    "portfolio_tags",
    "user_stock_preferences",
    "portfolio_snapshots",
    "portfolio_stock_snapshots",
    "portfolio_group_snapshots",
    "portfolio_stock_weight_snapshots",
    "portfolio_intraday",
    "portfolio_cashflows",
]

DELETE_USER_TABLES = [
    "user_stock_preferences",
    "portfolio_tags",
    "portfolio_cashflows",
    "portfolio_intraday",
    "portfolio_stock_weight_snapshots",
    "portfolio_group_snapshots",
    "portfolio_stock_snapshots",
    "portfolio_snapshots",
    "user_portfolio",
    "portfolio_groups",
]

SKIP_AUTOINCREMENT_COLUMNS = {
    "portfolio_cashflows": {"id"},
}


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _connect(path: Path, *, readonly: bool = False) -> sqlite3.Connection:
    if readonly:
        uri = path.resolve().as_uri() + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
    else:
        conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def _table_exists(conn: sqlite3.Connection, table: str, *, schema: str = "main") -> bool:
    row = conn.execute(
        f"SELECT 1 FROM {_quote_ident(schema)}.sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _columns(conn: sqlite3.Connection, table: str, *, schema: str = "main") -> list[str]:
    if not _table_exists(conn, table, schema=schema):
        return []
    return [row["name"] for row in conn.execute(f"PRAGMA {_quote_ident(schema)}.table_info({_quote_ident(table)})")]


def _backup_sqlite(source: Path, backup_path: Path) -> None:
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(source) as src, sqlite3.connect(backup_path) as dst:
        src.backup(dst)


async def _ensure_target_schema(target: Path) -> None:
    # 커넥션이 읽는 경로의 원본은 repositories.db.DB_PATH 다 (cache.DB_PATH
    # 는 읽기 전용 별칭이라 대입해도 효과 없음).
    original_db_path = repositories.db.DB_PATH
    try:
        repositories.db.DB_PATH = target
        await cache.close_db()
        await cache.init_db()
        await cache.close_db()
    finally:
        repositories.db.DB_PATH = original_db_path


def _find_source_user(conn: sqlite3.Connection, source_google_sub: str | None, source_email: str | None) -> sqlite3.Row:
    if source_google_sub:
        row = conn.execute("SELECT * FROM src.users WHERE google_sub = ?", (source_google_sub,)).fetchone()
        if not row:
            raise SystemExit(f"source user not found for google_sub={source_google_sub}")
        return row
    if source_email:
        row = conn.execute("SELECT * FROM src.users WHERE lower(email) = lower(?)", (source_email,)).fetchone()
        if not row:
            raise SystemExit(f"source user not found for email={source_email}")
        return row

    rows = conn.execute(
        """
        SELECT u.*, COUNT(up.stock_code) AS portfolio_count
        FROM src.users u
        JOIN src.user_portfolio up ON up.google_sub = u.google_sub
        GROUP BY u.google_sub
        ORDER BY portfolio_count DESC, u.last_login_at DESC
        """
    ).fetchall()
    if not rows:
        raise SystemExit("source DB has no users with portfolio rows")
    if len(rows) > 1:
        emails = ", ".join(f"{row['email']} ({row['portfolio_count']} items)" for row in rows[:10])
        raise SystemExit(
            "source DB has multiple portfolio users; pass --source-email or --source-google-sub. "
            f"Candidates: {emails}"
        )
    return rows[0]


def _upsert_target_user(
    conn: sqlite3.Connection,
    source_user: sqlite3.Row,
    *,
    dest_google_sub: str,
    dest_email: str | None,
    dest_name: str | None,
) -> None:
    target_cols = _columns(conn, "users")
    src_cols = set(source_user.keys())
    cols = [col for col in target_cols if col in src_cols]
    values: dict[str, Any] = {col: source_user[col] for col in cols}
    values["google_sub"] = dest_google_sub
    if "email" in values and dest_email:
        values["email"] = dest_email
    if "name" in values and dest_name:
        values["name"] = dest_name

    col_sql = ", ".join(_quote_ident(col) for col in cols)
    placeholders = ", ".join("?" for _ in cols)
    update_sql = ", ".join(
        f"{_quote_ident(col)} = excluded.{_quote_ident(col)}"
        for col in cols
        if col != "google_sub"
    )
    conn.execute(
        f"""
        INSERT INTO users ({col_sql})
        VALUES ({placeholders})
        ON CONFLICT(google_sub) DO UPDATE SET {update_sql}
        """,
        [values[col] for col in cols],
    )


def _copy_user_table(
    conn: sqlite3.Connection,
    table: str,
    *,
    source_google_sub: str,
    dest_google_sub: str,
) -> int:
    source_cols = _columns(conn, table, schema="src")
    target_cols = _columns(conn, table)
    if not source_cols or not target_cols or "google_sub" not in source_cols or "google_sub" not in target_cols:
        return 0

    skip_cols = SKIP_AUTOINCREMENT_COLUMNS.get(table, set())
    cols = [col for col in target_cols if col in source_cols and col not in skip_cols]
    col_sql = ", ".join(_quote_ident(col) for col in cols)
    select_sql = ", ".join(_quote_ident(col) for col in cols)
    placeholders = ", ".join("?" for _ in cols)

    rows = conn.execute(
        f"SELECT {select_sql} FROM src.{_quote_ident(table)} WHERE google_sub = ?",
        (source_google_sub,),
    ).fetchall()
    for row in rows:
        values = [dest_google_sub if col == "google_sub" else row[col] for col in cols]
        conn.execute(
            f"INSERT OR REPLACE INTO {_quote_ident(table)} ({col_sql}) VALUES ({placeholders})",
            values,
        )
    return len(rows)


def _delete_target_user_rows(conn: sqlite3.Connection, dest_google_sub: str) -> None:
    for table in DELETE_USER_TABLES:
        if _table_exists(conn, table) and "google_sub" in _columns(conn, table):
            conn.execute(f"DELETE FROM {_quote_ident(table)} WHERE google_sub = ?", (dest_google_sub,))


def import_portfolio_rows(
    source: Path,
    target: Path,
    *,
    source_google_sub: str | None = None,
    source_email: str | None = None,
    dest_google_sub: str | None = None,
    dest_email: str | None = None,
    dest_name: str | None = None,
    backup: bool = True,
) -> dict[str, int | str]:
    source = source.resolve()
    target = target.resolve()
    if source == target:
        raise SystemExit("source and target DB must be different files")
    if not source.exists():
        raise SystemExit(f"source DB not found: {source}")

    if backup and target.exists():
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        backup_path = target.parent / f"{target.stem}.before-prod-portfolio-import.{stamp}{target.suffix}"
        _backup_sqlite(target, backup_path)

    with _connect(target) as conn:
        conn.execute("ATTACH DATABASE ? AS src", (str(source),))
        try:
            source_user = _find_source_user(conn, source_google_sub, source_email)
            source_sub = source_user["google_sub"]
            target_sub = dest_google_sub or source_sub
            conn.execute("BEGIN IMMEDIATE")
            _upsert_target_user(
                conn,
                source_user,
                dest_google_sub=target_sub,
                dest_email=dest_email,
                dest_name=dest_name,
            )
            _delete_target_user_rows(conn, target_sub)
            counts: dict[str, int | str] = {
                "source_google_sub": source_sub,
                "dest_google_sub": target_sub,
            }
            for table in USER_TABLES:
                counts[table] = _copy_user_table(
                    conn,
                    table,
                    source_google_sub=source_sub,
                    dest_google_sub=target_sub,
                )
            conn.commit()
            return counts
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.execute("DETACH DATABASE src")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import production portfolio rows into the local SQLite DB.")
    parser.add_argument("--source", required=True, type=Path, help="SQLite snapshot copied from production.")
    parser.add_argument("--target", default=PROJECT_ROOT / "cache.db", type=Path, help="Local cache.db to seed.")
    parser.add_argument("--source-google-sub", default=None, help="Production google_sub to import.")
    parser.add_argument("--source-email", default=None, help="Production user email to import.")
    parser.add_argument("--dest-google-sub", default=None, help="Optional local google_sub to write into.")
    parser.add_argument("--dest-email", default=None, help="Optional local email override when mapping users.")
    parser.add_argument("--dest-name", default=None, help="Optional local display name override when mapping users.")
    parser.add_argument("--no-backup", action="store_true", help="Do not create a local DB backup before importing.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    asyncio.run(_ensure_target_schema(args.target.resolve()))
    counts = import_portfolio_rows(
        args.source,
        args.target,
        source_google_sub=args.source_google_sub,
        source_email=args.source_email,
        dest_google_sub=args.dest_google_sub,
        dest_email=args.dest_email,
        dest_name=args.dest_name,
        backup=not args.no_backup,
    )
    print("Imported production portfolio rows:")
    for key, value in counts.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
