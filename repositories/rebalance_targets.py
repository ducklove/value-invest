"""리밸런싱 목표 비중(rebalance_targets) 저장소.

사용자가 종목(scope='stock', target_key=종목코드) 또는 그룹(scope='group',
target_key=그룹명) 단위로 설정한 목표 비중과 허용 오차(tolerance)를 보관한다.
(google_sub, scope, target_key) 당 한 행 — 같은 키로 다시 저장하면 갱신.

스키마는 다른 테이블처럼 ``repositories.bootstrap.init_db`` 가 만들고, 이 모듈은 CRUD 만 담당.
전체 목록 교체(replace_all_targets)는 DELETE + INSERT 멀티-스테이트먼트라
``repositories.db.transaction()`` 으로 원자성을 보장한다.
"""

from __future__ import annotations

import logging
from datetime import datetime

from repositories.db import get_db, transaction

logger = logging.getLogger(__name__)

# 알림 드리프트 허용 오차 기본값(%p) — 사용자가 지정하지 않은 목표에 적용.
DEFAULT_TOLERANCE_PCT = 5.0

_TARGET_COLUMNS = (
    "id, google_sub, scope, target_key, target_weight_pct, tolerance_pct,"
    " created_at, updated_at"
)


def _now() -> str:
    return datetime.now().isoformat()


async def list_targets(google_sub: str) -> list[dict]:
    """사용자의 모든 목표 비중 — 그룹 먼저, 그 다음 종목(키 오름차순)."""
    db = await get_db()
    cursor = await db.execute(
        f"SELECT {_TARGET_COLUMNS} FROM rebalance_targets"
        " WHERE google_sub = ? ORDER BY scope ASC, target_key ASC",
        (google_sub,),
    )
    return [dict(row) for row in await cursor.fetchall()]


async def get_target(google_sub: str, scope: str, target_key: str) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        f"SELECT {_TARGET_COLUMNS} FROM rebalance_targets"
        " WHERE google_sub = ? AND scope = ? AND target_key = ?",
        (google_sub, scope, target_key),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def upsert_target(
    google_sub: str,
    scope: str,
    target_key: str,
    target_weight_pct: float,
    tolerance_pct: float = DEFAULT_TOLERANCE_PCT,
) -> None:
    """단건 저장 — (google_sub, scope, target_key) 충돌 시 갱신."""
    db = await get_db()
    now = _now()
    await db.execute(
        """
        INSERT INTO rebalance_targets
            (google_sub, scope, target_key, target_weight_pct, tolerance_pct, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(google_sub, scope, target_key) DO UPDATE SET
            target_weight_pct = excluded.target_weight_pct,
            tolerance_pct = excluded.tolerance_pct,
            updated_at = excluded.updated_at
        """,
        (google_sub, scope, target_key, target_weight_pct, tolerance_pct, now, now),
    )
    await db.commit()


async def replace_all_targets(google_sub: str, targets: list[dict]) -> list[dict]:
    """전체 목록 교체(PUT 시멘틱) — 기존 행 전부 삭제 후 새 목록 삽입.

    DELETE + INSERT 가 부분 실패하면 목표가 빈 채로 남으므로 한 트랜잭션으로
    묶는다. targets: [{scope, key, target_weight_pct, tolerance_pct}].
    """
    now = _now()
    async with transaction() as db:
        await db.execute(
            "DELETE FROM rebalance_targets WHERE google_sub = ?", (google_sub,)
        )
        for t in targets:
            await db.execute(
                """
                INSERT INTO rebalance_targets
                    (google_sub, scope, target_key, target_weight_pct, tolerance_pct, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    google_sub,
                    t["scope"],
                    t["key"],
                    float(t["target_weight_pct"]),
                    float(t.get("tolerance_pct") or DEFAULT_TOLERANCE_PCT),
                    now,
                    now,
                ),
            )
    return await list_targets(google_sub)


async def delete_target(google_sub: str, scope: str, target_key: str) -> bool:
    db = await get_db()
    cursor = await db.execute(
        "DELETE FROM rebalance_targets WHERE google_sub = ? AND scope = ? AND target_key = ?",
        (google_sub, scope, target_key),
    )
    await db.commit()
    return cursor.rowcount > 0
