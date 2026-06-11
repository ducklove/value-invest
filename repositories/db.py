"""Shared aiosqlite connection ownership for the data layer.

cache.py 의 god-module 결합을 끊는 첫 단계: 커넥션 싱글톤(DB_PATH /
get_db / close_db)과 트랜잭션 원자성 헬퍼(transaction)가 여기 산다.
repositories/* 는 cache 를 import 하지 않고 이 모듈만 본다. cache.py 는
하위 호환을 위해 get_db/close_db/DB_PATH 를 얇게 재수출한다.

테스트는 ``patch.object(repositories.db, "DB_PATH", ...)`` 로 경로를
바꾼다 — get_db() 가 호출 시점에 모듈 전역 DB_PATH 를 읽으므로 패치가
즉시 반영된다 (cache.DB_PATH 는 읽기 전용 별칭일 뿐, 패치 대상이 아님).
"""

from __future__ import annotations

import asyncio
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite

from core.errors import DBError

# 프로젝트 루트의 cache.db — cache.py 가 들고 있던 경로와 동일.
DB_PATH = Path(__file__).resolve().parent.parent / "cache.db"

_conn: aiosqlite.Connection | None = None

# transaction() 직렬화 락. 앱 전체가 하나의 aiosqlite 커넥션을 모든
# asyncio task 가 공유하므로, 락 없이 BEGIN/COMMIT 을 쓰면 서로 다른
# task 의 문장이 한 트랜잭션에 섞여 들어간다(interleave). 락이 잡힌
# 동안만 명시 트랜잭션이 열리도록 보장한다.
_txn_lock = asyncio.Lock()
# 락을 잡고 있는 task — 같은 task 의 중첩 transaction() 호출(재진입)을
# 바깥 트랜잭션에 합류시키기 위한 표식.
_txn_owner: asyncio.Task | None = None


async def get_db() -> aiosqlite.Connection:
    global _conn
    if _conn is None:
        _conn = await aiosqlite.connect(DB_PATH)
        _conn.row_factory = aiosqlite.Row
        await _conn.execute("PRAGMA journal_mode=WAL")
        await _conn.execute("PRAGMA busy_timeout=5000")
        await _conn.execute("PRAGMA foreign_keys=ON")
    return _conn


async def close_db():
    """Shutdown: close the shared connection."""
    global _conn, _txn_lock, _txn_owner
    if _conn is not None:
        await _conn.close()
        _conn = None
    # asyncio.Lock 은 처음 acquire 한 이벤트 루프에 묶인다. 테스트가
    # (IsolatedAsyncioTestCase 처럼) 루프를 매번 새로 만들고 setUp 에서
    # close_db() 를 부르므로, 여기서 락도 새로 만들어 루프 교체에 안전하게.
    _txn_lock = asyncio.Lock()
    _txn_owner = None


@asynccontextmanager
async def transaction():
    """공유 커넥션 위에서 멀티-스테이트먼트 쓰기를 원자적으로 묶는다.

    사용법::

        async with db.transaction() as conn:
            await conn.execute(...)
            await conn.execute(...)
        # 정상 종료 시 COMMIT, 예외 시 ROLLBACK

    동시성: 앱은 하나의 aiosqlite 커넥션을 모든 asyncio task 가 공유한다.
    락 없이 BEGIN/COMMIT 을 쓰면 동시에 진행되는 다른 task 의 문장이 이
    트랜잭션에 섞여 들어가므로, 모듈 전역 asyncio.Lock 을 블록이 끝날
    때까지 보유한 채 BEGIN IMMEDIATE 로 쓰기 트랜잭션을 연다.

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
    async with _txn_lock:
        _txn_owner = current
        try:
            db = await get_db()
            await db.execute("BEGIN IMMEDIATE")
            try:
                yield db
            except sqlite3.Error as exc:
                await db.rollback()
                raise DBError(str(exc)) from exc
            except BaseException:
                await db.rollback()
                raise
            await db.commit()
        finally:
            _txn_owner = None
