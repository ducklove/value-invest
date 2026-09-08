"""실제 SQLite 연결로 미커밋 조회·읽기 시점·취소 격리를 검사한다."""

import asyncio
import sqlite3

import pytest

from repositories import db


async def seed():
    async with db.transaction() as conn:
        await conn.execute("CREATE TABLE isolation_probe(value INTEGER)")
        await conn.execute("INSERT INTO isolation_probe VALUES (10)")


async def value(conn=None):
    conn = conn if conn is not None else await db.get_db()
    return (await (await conn.execute("SELECT value FROM isolation_probe")).fetchone())[0]


@pytest.mark.parametrize("rollback", [False, True])
async def test_reader_never_sees_another_tasks_uncommitted_change(temp_db, rollback):
    await seed()
    reader = await db.get_db()  # 쓰기가 시작되기 전에 얻은 연결도 격리돼야 한다.
    entered, inspected = asyncio.Event(), asyncio.Event()

    async def writer():
        try:
            async with db.transaction() as conn:
                await conn.execute("UPDATE isolation_probe SET value=999")
                entered.set()
                await inspected.wait()
                if rollback:
                    raise ValueError("의도한 롤백")
        except ValueError:
            pass

    async def inspect():
        await entered.wait()
        try:
            assert await value(reader) == 10
            assert await value() == 10
        finally:
            inspected.set()

    await asyncio.wait_for(asyncio.gather(writer(), inspect()), 5)
    assert await value() == (10 if rollback else 999)


async def test_snapshot_stays_consistent_while_another_task_commits(temp_db):
    await seed()
    async with db.read_snapshot() as conn:
        assert await value() == 10

        async def writer():
            async with db.transaction() as tx:
                await tx.execute("UPDATE isolation_probe SET value=20")

        await asyncio.wait_for(asyncio.create_task(writer()), 5)
        async with db.read_snapshot() as nested:
            assert nested is conn
            assert await value() == 10
        # 자식 task는 부모의 읽기 연결을 공유하지 않는다.
        assert await asyncio.create_task(value()) == 20
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            await conn.execute("UPDATE isolation_probe SET value=30")
        with pytest.raises(RuntimeError, match="읽기 스냅샷"):
            async with db.transaction():
                pass
    assert await value() == 20


async def test_nested_writer_reads_its_own_changes_and_rolls_back(temp_db):
    await seed()
    with pytest.raises(ValueError):
        async with db.transaction() as tx:
            await tx.execute("UPDATE isolation_probe SET value=30")
            async with db.transaction() as nested:
                assert nested is tx
                async with db.read_snapshot() as snapshot:
                    assert snapshot is tx
                    assert await value() == 30
            raise ValueError("의도한 롤백")
    assert await value() == 10


async def test_cancellation_releases_writer_and_read_snapshot(temp_db):
    await seed()
    entered = asyncio.Event()

    async def writer():
        async with db.transaction() as tx:
            await tx.execute("UPDATE isolation_probe SET value=40")
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(writer())
    await asyncio.wait_for(entered.wait(), 5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await value() == 10
    with pytest.raises(asyncio.CancelledError):
        async with db.read_snapshot():
            assert await value() == 10
            raise asyncio.CancelledError
    async with db.transaction() as tx:
        await tx.execute("UPDATE isolation_probe SET value=50")
    assert await value() == 50
