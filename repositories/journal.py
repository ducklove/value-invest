"""투자 일지(investment_journal) 저장소.

매수/매도/메모 판단의 '이유'(note)와 작성 시점 스냅샷(당시 가격·당시 목표가)을
보관한다. 항목은 append-only 에 가깝다 — 수정은 note 만 허용(복기 기록의
신뢰성을 위해 가격 스냅샷·유형은 불변), 삭제는 허용.

스키마는 다른 테이블처럼 ``repositories.bootstrap.init_db`` 가 만들고, 이 모듈은 CRUD 만 담당.
모든 연산이 단일 스테이트먼트라 transaction() 은 필요 없다. 소유권은 모든
쿼리에 ``google_sub`` 조건을 함께 거는 방식으로 강제한다 — 남의 id 로는
조회/수정/삭제 자체가 매칭되지 않는다.
"""

from __future__ import annotations

import logging
from datetime import datetime

from repositories.db import get_db

logger = logging.getLogger(__name__)

# 일지 항목 유형 — buy(매수)/sell(매도)/memo(메모).
ENTRY_TYPES = ("buy", "sell", "memo")

_ENTRY_COLUMNS = (
    "id, google_sub, stock_code, stock_name, entry_type, note,"
    " price_at_entry, quantity, target_price_at_entry, created_at, updated_at"
)


def _now() -> str:
    return datetime.now().isoformat()


async def list_entries(google_sub: str, stock_code: str | None = None) -> list[dict]:
    """사용자의 일지 항목 — 최신 작성순. stock_code 로 종목 필터 가능."""
    db = await get_db()
    if stock_code:
        cursor = await db.execute(
            f"SELECT {_ENTRY_COLUMNS} FROM investment_journal"
            " WHERE google_sub = ? AND stock_code = ?"
            " ORDER BY created_at DESC, id DESC",
            (google_sub, stock_code),
        )
    else:
        cursor = await db.execute(
            f"SELECT {_ENTRY_COLUMNS} FROM investment_journal"
            " WHERE google_sub = ? ORDER BY created_at DESC, id DESC",
            (google_sub,),
        )
    return [dict(row) for row in await cursor.fetchall()]


async def get_entry(google_sub: str, entry_id: int) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        f"SELECT {_ENTRY_COLUMNS} FROM investment_journal WHERE google_sub = ? AND id = ?",
        (google_sub, entry_id),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def insert_entry(
    google_sub: str,
    stock_code: str,
    entry_type: str,
    note: str,
    *,
    stock_name: str | None = None,
    price_at_entry: float | None = None,
    quantity: float | None = None,
    target_price_at_entry: float | None = None,
) -> dict:
    """단건 작성 — 작성 시점 스냅샷 포함. 저장된 행을 그대로 돌려준다."""
    if entry_type not in ENTRY_TYPES:
        raise ValueError(f"entry_type 은 {ENTRY_TYPES} 중 하나여야 합니다: {entry_type!r}")
    db = await get_db()
    now = _now()
    cursor = await db.execute(
        """
        INSERT INTO investment_journal
            (google_sub, stock_code, stock_name, entry_type, note,
             price_at_entry, quantity, target_price_at_entry, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            google_sub,
            stock_code,
            stock_name,
            entry_type,
            note,
            price_at_entry,
            quantity,
            target_price_at_entry,
            now,
            now,
        ),
    )
    await db.commit()
    entry = await get_entry(google_sub, cursor.lastrowid)
    assert entry is not None
    return entry


async def get_holding_snapshot(google_sub: str, stock_code: str) -> dict | None:
    """작성 시점 스냅샷 캡처용 보유 행 읽기(read-only).

    user_portfolio 의 종목명과 '저장된 목표가'를 돌려준다. target_price 컬럼은
    수동 override 또는 수식의 마지막 계산값이 이미 저장돼 있어(routes/portfolio
    의 _resolve_target_formula_price 가 저장 시점에 채움) 수식 머신을 다시
    돌리지 않고도 싸게 스냅샷할 수 있다. get_portfolio_item(repositories.
    portfolio)은 목표가 컬럼을 노출하지 않아 일지 전용 조회를 따로 둔다.
    """
    db = await get_db()
    cursor = await db.execute(
        "SELECT stock_name, target_price, COALESCE(target_price_disabled, 0) AS target_price_disabled"
        " FROM user_portfolio WHERE google_sub = ? AND stock_code = ?",
        (google_sub, stock_code),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def update_note(google_sub: str, entry_id: int, note: str) -> bool:
    """note 만 수정 가능 — 스냅샷(가격/목표가/유형)은 복기 기록이라 불변."""
    db = await get_db()
    cursor = await db.execute(
        "UPDATE investment_journal SET note = ?, updated_at = ?"
        " WHERE google_sub = ? AND id = ?",
        (note, _now(), google_sub, entry_id),
    )
    await db.commit()
    return cursor.rowcount > 0


async def delete_entry(google_sub: str, entry_id: int) -> bool:
    db = await get_db()
    cursor = await db.execute(
        "DELETE FROM investment_journal WHERE google_sub = ? AND id = ?",
        (google_sub, entry_id),
    )
    await db.commit()
    return cursor.rowcount > 0
