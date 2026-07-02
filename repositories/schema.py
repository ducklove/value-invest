"""Schema maintenance helpers shared across init_db / migrations.

cache.py 가 소유하던 스키마 보조 함수 중 동적 SQL 이 포함된 것을 분리해
(1) 식별자 allowlist 검증을 명시적으로 두고, (2) 향후 init_db 의 스키마
소유권을 이 모듈로 점진 이관할 수 있는 기반을 만든다.

보안 노트: ``_ensure_column`` 의 동적 SQL 은 테이블/컬럼 식별자를 문자열
보간으로 넣는다. 이 값들은 내부 하드코딩 상수(이 모듈과 cache.py init_db
에서만 호출)이므로 외부 입력이 들어오지 않아 SQL injection 경로는 없다.
그럼에도 식별자를 allowlist 로 제한해 "실수로 외부 값이 들어와도 거부"되게
만든다 — 방어적 품질 향상(ST-01 보조).
"""

from __future__ import annotations

import re

import aiosqlite

ColumnSpec = tuple[str, str, str]

# SQLite 식별자 허용 패턴: ASCII 문자/숫자/밑줄. 한글 식별자는 쓰지 않으므로
# 이 패턴을 벗어나면 ValueError 로 거부한다. 테이블·컬럼명 모두에 적용.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Columns that were added after the original CREATE TABLE definitions. Keeping
# the list here makes init_db's migration surface explicit while the larger
# table-creation script is still being split out of cache.py.
CORE_COLUMN_MIGRATIONS: tuple[ColumnSpec, ...] = (
    ("corp_codes", "modify_date", "TEXT"),
    ("financial_data", "report_date", "TEXT"),
    ("market_data", "dividend_per_share", "REAL"),
    ("analysis_meta", "payload_json", "TEXT"),
    ("user_stock_preferences", "sort_order", "INTEGER"),
    ("user_stock_preferences", "starred_order", "INTEGER"),
    ("user_portfolio", "currency", "TEXT DEFAULT 'KRW'"),
    ("user_portfolio", "avg_price_currency", "TEXT NOT NULL DEFAULT 'KRW'"),
    ("user_portfolio", "group_name", "TEXT"),
    ("user_portfolio", "benchmark_code", "TEXT"),
    # Manual target override fields.
    ("user_portfolio", "target_price", "REAL"),
    ("user_portfolio", "target_price_disabled", "INTEGER NOT NULL DEFAULT 0"),
    ("user_portfolio", "target_price_formula", "TEXT"),
    ("users", "is_admin", "INTEGER NOT NULL DEFAULT 0"),
    ("users", "password_hash", "TEXT"),
    ("users", "password_updated_at", "TEXT"),
    ("users", "google_identity_sub", "TEXT"),
    # Notification edge-trigger and priority flags.
    ("portfolio_alerts", "state_json", "TEXT NOT NULL DEFAULT '{}'"),
    ("portfolio_alerts", "important", "INTEGER NOT NULL DEFAULT 0"),
    ("portfolio_snapshots", "fx_usdkrw", "REAL"),
    ("portfolio_stock_snapshots", "group_name", "TEXT"),
    ("portfolio_stock_snapshots", "quantity", "REAL"),
    ("portfolio_stock_snapshots", "unit_price", "REAL"),
    ("portfolio_stock_snapshots", "avg_price_krw", "REAL"),
    ("portfolio_stock_snapshots", "cost_basis", "REAL"),
    ("portfolio_groups", "default_type", "TEXT"),
    # Multi-account phase 1. Nullable by design; uniqueness across
    # (google_sub, account_id, stock_code) is deferred to a later table rebuild.
    ("user_portfolio", "account_id", "TEXT"),
)


def _validate_identifier(value: str, *, kind: str) -> str:
    """동적 SQL 에 들어갈 식별자를 allowlist 패턴으로 검증한다.

    허용 패턴을 벗어나면 ValueError — 이 함수의 호출부는 내부 상수만 쓰므로
    정상이라면 절대 발생하지 않는다. 외부 입력이 섞여 들어오는 실수를
    컴파일 타임에 가깝게 막기 위한 방어막.
    """
    if not isinstance(value, str) or not _IDENTIFIER_RE.match(value):
        raise ValueError(f"invalid {kind} identifier: {value!r}")
    return value


async def ensure_column(db: aiosqlite.Connection, table: str, column: str, definition: str) -> None:
    """Add ``column`` to ``table`` if it does not already exist.

    ``table``/``column`` are internal constants; both are validated against an
    identifier allowlist before interpolation (defense-in-depth — no external
    input reaches here). ``definition`` is a type/default fragment like
    ``"TEXT NOT NULL DEFAULT '{}'"`` and is also part of DDL, so it is passed
    through as-is by design (call sites are all hardcoded).

    ``PRAGMA table_info`` 는 각 행이 (cid, name, type, notnull, dflt_value, pk)
    순서의 튜플 또는 Row. row_factory 설정과 무관하게 동작하도록 name 은
    인덱스 1 로 추출한다(aiosqlite Row 도 인덱스 접근 지원).
    """
    table = _validate_identifier(table, kind="table")
    column = _validate_identifier(column, kind="column")
    cursor = await db.execute(f"PRAGMA table_info({table})")
    rows = await cursor.fetchall()
    existing = {row[1] for row in rows}  # row[1] == name
    if column not in existing:
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


async def ensure_columns(db: aiosqlite.Connection, columns: tuple[ColumnSpec, ...]) -> None:
    """Apply a sequence of idempotent ADD COLUMN migrations."""
    for table, column, definition in columns:
        await ensure_column(db, table, column, definition)


async def apply_core_column_migrations(db: aiosqlite.Connection) -> None:
    """Apply init_db's legacy ADD COLUMN migrations."""
    await ensure_columns(db, CORE_COLUMN_MIGRATIONS)
