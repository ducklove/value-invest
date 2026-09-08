"""SQLite 연결과 읽기·쓰기 트랜잭션의 소유권.

일반 조회와 직렬화된 쓰기는 서로 다른 연결을 사용한다. 같은 쓰기 작업의
중첩 호출만 쓰기 연결에 합류한다. 여러 SELECT의 기준 시점이 같아야 하는
조회는 read_snapshot()을 사용한다. 요청 쓰기는 반드시 transaction()을 쓴다.

테스트는 ``patch.object(repositories.db, "DB_PATH", ...)`` 로 경로를
바꾼다 — get_db() 가 호출 시점에 모듈 전역 DB_PATH 를 읽으므로 패치가
즉시 반영된다.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
from contextlib import AsyncExitStack, asynccontextmanager
from contextvars import ContextVar
from pathlib import Path

import aiosqlite

from core.errors import DBError

# 프로젝트 루트의 cache.db — cache.py 가 들고 있던 경로와 동일.
DB_PATH = Path(__file__).resolve().parent.parent / "cache.db"

_conn: aiosqlite.Connection | None = None
_writer_conn: aiosqlite.Connection | None = None
_conn_lock = asyncio.Lock()
_read_scope: ContextVar[tuple[asyncio.Task, aiosqlite.Connection] | None] = ContextVar("db_read_scope", default=None)

# 쓰기 연결은 transaction()을 소유한 task만 사용한다. 일반 조회 연결은
# 분리하여 아직 커밋하지 않은 변경이 다른 요청에 노출되지 않게 한다.
_txn_lock = asyncio.Lock()
# 락을 잡고 있는 task — 같은 task 의 중첩 transaction() 호출(재진입)을
# 바깥 트랜잭션에 합류시키기 위한 표식.
_txn_owner: asyncio.Task | None = None


async def _open_connection() -> aiosqlite.Connection:
    conn = await aiosqlite.connect(DB_PATH)
    try:
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA busy_timeout=5000")
        await conn.execute("PRAGMA foreign_keys=ON")
    except BaseException:
        await conn.close()
        raise
    return conn


async def get_db() -> aiosqlite.Connection:
    """현재 작업의 연결. 쓰기 소유자 외에는 미커밋 쓰기를 볼 수 없다.

    기본 연결의 쓰기 접근은 시작 시 bootstrap과 테스트 시드의 호환을
    위해 유지한다. 서비스가 시작된 후의 쓰기는 transaction()으로만 한다.
    """
    global _conn, _writer_conn
    current = asyncio.current_task()
    writing = _txn_owner is not None and _txn_owner is current
    scope = _read_scope.get()
    if not writing and scope is not None and scope[0] is current:
        return scope[1]
    conn = _writer_conn if writing else _conn
    if conn is None:
        async with _conn_lock:
            conn = _writer_conn if writing else _conn
            if conn is None:
                conn = await _open_connection()
                if writing:
                    _writer_conn = conn
                else:
                    _conn = conn
    return conn


@asynccontextmanager
async def read_snapshot():
    """여러 조회를 같은 커밋 상태로 묶는다. 자식 task는 연결을 상속하지 않는다."""
    current = asyncio.current_task()
    if current is None:
        raise RuntimeError("읽기 트랜잭션에는 실행 중인 task가 필요합니다.")
    scope = _read_scope.get()
    if (_txn_owner is current) or (scope is not None and scope[0] is current):
        yield await get_db()
        return
    conn = await _open_connection()
    token = _read_scope.set((current, conn))
    try:
        await conn.execute("PRAGMA query_only=ON")
        await conn.execute("BEGIN")
        yield conn
    finally:
        _read_scope.reset(token)
        await conn.close()


async def close_db():
    """앱 소유의 조회·쓰기 연결을 모두 닫는다."""
    global _conn, _writer_conn, _conn_lock, _txn_lock, _txn_owner
    connections = [conn for conn in (_conn, _writer_conn) if conn is not None]
    _conn = _writer_conn = None
    try:
        async with AsyncExitStack() as cleanup:
            for conn in connections:
                cleanup.push_async_callback(conn.close)
    finally:
        # 테스트의 이벤트 루프 교체에도 안전하게 연결과 잠금 소유권을 초기화한다.
        _txn_lock = asyncio.Lock()
        _conn_lock = asyncio.Lock()
        _txn_owner = None


@asynccontextmanager
async def transaction():
    """공유 커넥션 위에서 멀티-스테이트먼트 쓰기를 원자적으로 묶는다.

    사용법::

        async with db.transaction() as conn:
            await conn.execute(...)
            await conn.execute(...)
        # 정상 종료 시 COMMIT, 예외 시 ROLLBACK

    동시성: 전용 쓰기 연결을 락으로 직렬화하고 BEGIN IMMEDIATE로 연다.
    다른 task의 일반 조회는 별도 연결에서 마지막 커밋 상태를 읽는다.

    재진입: transaction() 으로 감싼 함수가 또 감싼 함수를 부르는 중첩
    호출은, 같은 task 라면 바깥 트랜잭션에 그대로 합류한다 (BEGIN/COMMIT
    없이 커넥션만 yield) — 데드락 없이 바깥 블록이 commit/rollback 을
    단독 결정한다.

    예외: 블록 안에서 발생한 sqlite 오류는 롤백 후 ``core.errors.DBError``
    로 변환된다 (원본 메시지 보존, ``__cause__`` 유지). 앱 정의 예외나
    일반 예외는 타입 그대로 통과한다.
    """
    global _txn_owner
    current = asyncio.current_task()
    if _txn_owner is not None and _txn_owner is current:
        # 같은 task 의 중첩 호출 — 바깥 트랜잭션에 합류.
        yield await get_db()
        return
    scope = _read_scope.get()
    if scope is not None and scope[0] is current:
        raise RuntimeError("읽기 스냅샷 안에서는 쓰기 트랜잭션을 시작할 수 없습니다.")
    async with _txn_lock:
        _txn_owner = current
        try:
            db = await get_db()
            await db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                await db.commit()
            except sqlite3.Error as exc:
                await db.rollback()
                raise DBError(str(exc)) from exc
            except BaseException:
                await db.rollback()
                raise
        finally:
            _txn_owner = None


async def get_db_stats() -> dict:
    """Admin 진단용 — 테이블별 행 수와 DB 파일 크기."""
    db = await get_db()
    tables = {}
    cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    for row in await cursor.fetchall():
        tname = row["name"]
        cnt = await db.execute(f"SELECT COUNT(*) as c FROM [{tname}]")
        tables[tname] = (await cnt.fetchone())["c"]
    db_size = os.path.getsize(DB_PATH) if DB_PATH.exists() else 0
    return {"tables": tables, "db_size_bytes": db_size}
