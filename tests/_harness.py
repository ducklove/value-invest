"""tests 공용 temp-DB 하니스.

여러 테스트 파일이 똑같이 반복하던 수명주기 —
TemporaryDirectory → repositories.db.DB_PATH 패치 → cache.close_db()
→ cache.init_db() (+ teardown 역순) — 를 한 곳으로 모았다.

unittest.IsolatedAsyncioTestCase 는 pytest fixture 를 주입받을 수 없으므로
(인스턴스 생성/실행을 unittest 러너가 소유) 클래스 기반 테스트는
``TempDbMixin`` 을 상속한다. function-style 테스트는 tests/conftest.py 의
``temp_db`` fixture 가 아래의 동일한 헬퍼를 감싼다.
"""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import cache
import repositories.db


async def open_temp_db(tmp: tempfile.TemporaryDirectory) -> tuple[Path, object]:
    """repositories.db.DB_PATH 를 임시 경로로 패치하고 새 커넥션을 연다.

    Returns ``(db_path, db_patch)``. 호출자는 ``close_temp_db()`` 로 정리한다.
    """
    db_path = Path(tmp.name) / "cache.db"
    db_patch = patch.object(repositories.db, "DB_PATH", db_path)
    db_patch.start()
    # Previous test may have left cache._conn pointing at a now-deleted
    # temp DB or a closed handle. close_db() is idempotent and resets
    # the singleton so init_db() opens a fresh conn on the patched path.
    await cache.close_db()
    await cache.init_db()
    return db_path, db_patch


async def close_temp_db(tmp: tempfile.TemporaryDirectory, db_patch) -> None:
    await cache.close_db()
    db_patch.stop()
    tmp.cleanup()


class TempDbMixin(unittest.IsolatedAsyncioTestCase):
    """temp-DB 수명주기 mixin.

    클래스별 시드 데이터는 ``seed()`` 훅을 override 해서 넣는다
    (``init_db()`` 직후, 각 테스트 메서드 전에 실행). setUp/tearDown 에
    추가 패치가 필요한 클래스는 ``asyncSetUp``/``asyncTearDown`` 을
    override 하고 ``super()`` 를 호출한다.
    """

    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path, self.db_patch = await open_temp_db(self.tmp)
        await self.seed()

    async def seed(self):
        """Override point — init_db() 직후 클래스별 픽스처 삽입."""

    async def asyncTearDown(self):
        await close_temp_db(self.tmp, self.db_patch)


# --- 공용 시드 헬퍼 (3개 이상 파일이 똑같이 중복하던 INSERT) ----------------

async def seed_user(sub: str = "u1", email: str = "user@example.com", name: str = "User") -> None:
    """users 테이블에 기본 사용자 1행을 삽입한다."""
    db = await cache.get_db()
    await db.execute(
        "INSERT INTO users (google_sub, email, name, picture, email_verified, created_at, last_login_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (sub, email, name, "", 1, "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
    )
    await db.commit()


async def seed_corp_codes(rows: list[tuple[str, str, str, str]]) -> None:
    """corp_codes 삽입. rows: (stock_code, corp_code, corp_name, updated_at)."""
    db = await cache.get_db()
    await db.executemany(
        "INSERT INTO corp_codes (stock_code, corp_code, corp_name, updated_at) VALUES (?, ?, ?, ?)",
        rows,
    )
    await db.commit()
