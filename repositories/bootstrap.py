"""DB 부트스트랩 — init_db()/close_db() 수명주기 오케스트레이션.

cache.py 해체(Phase 2)로 이관. 스키마 생성·컬럼 마이그레이션·1회성 백필을
한 경로로 실행한다. FastAPI lifespan(core/lifespan.py)과 단독 프로세스
(snapshot_nav/snapshot_intraday, deploy/repairs/*, scripts/*)가 같은
초기화·종료 경로를 공유한다.
"""

from __future__ import annotations

from repositories import corp_codes
from repositories import db as _db
from repositories import schema as schema_repo


async def init_db():
    # Migration-only repository helpers (one-time backfills below). Imported
    # locally so module import stays cycle-free (portfolio/snapshots import
    # other repositories at module scope).
    from repositories.portfolio import backfill_portfolio_defaults
    from repositories.snapshots import ensure_initial_snapshot_backfills

    db = await _db.get_db()
    await schema_repo.create_core_schema(db)
    await schema_repo.apply_core_column_migrations(db)
    await db.execute(
        """
        UPDATE users
        SET google_identity_sub = google_sub
        WHERE google_identity_sub IS NULL
          AND google_sub NOT LIKE 'local:%'
        """
    )
    await db.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_google_identity_sub "
        "ON users(google_identity_sub) WHERE google_identity_sub IS NOT NULL"
    )
    await db.execute("CREATE INDEX IF NOT EXISTS idx_users_email_lower ON users(lower(email))")
    await schema_repo.backfill_legacy_cache_values(db)
    await db.execute("CREATE INDEX IF NOT EXISTS idx_stock_snapshots_sub_group_date ON portfolio_stock_snapshots(google_sub, group_name, date)")
    await ensure_initial_snapshot_backfills(db)
    await backfill_portfolio_defaults(db)
    await db.commit()


async def close_db():
    """Shutdown: 공유 커넥션을 닫고 corp_codes 인메모리 테이블도 리셋한다."""
    await _db.close_db()
    corp_codes.reset_memory_table()
