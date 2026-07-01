"""Portfolio accounts — multi-account phase 1.

A user may hold positions across several accounts (일반계좌 / ISA / 퇴직연금 /
배우자). Phase 1 introduces the ``portfolio_accounts`` table and an optional
``account_id`` on ``user_portfolio`` (nullable, defaulting to a per-user
"default" account via ``cache._ensure_default_account``). Existing queries that
filter by ``google_sub`` alone keep working unchanged.

Design notes:

* **account_id shape.`` A caller-supplied stable id (e.g. ``"isa"``) is hashed
  with the user's ``google_sub`` so the stored PK is globally unique without
  leaking a raw user key — mirroring the default-account id scheme in
  ``cache._ensure_default_account``.
* **Soft uniqueness.`` The DB UNIQUE(google_sub, name) prevents duplicate names,
  but (google_sub, account_id, stock_code) uniqueness on holdings is enforced in
  app code for now — SQLite can't add it via ALTER TABLE (deferred to a later
  table-rebuild phase). See the docs/broker-integration.md roadmap.
* **Deletion guard.`` Deleting an account re-parents its holdings to the user's
  default account rather than orphaning them (NULL account_id). The default
  account itself cannot be deleted.
"""

from __future__ import annotations

import hashlib

from cache_layer import now_iso
from core.errors import AppError
from repositories.db import get_db

# Account "type" taxonomy. 'general' is the default bucket created by the
# backfill; the rest are user-facing labels. Kept open — UI may add more later.
ALLOWED_ACCOUNT_TYPES = {"general", "isa", "pension", "spouse", "cash", "other"}

MAX_NAME_LENGTH = 40


class AccountError(AppError):
    """User-facing account input/state error. Maps to HTTP 400 via AppError."""

    status_code = 400
    default_detail = "계좌 정보가 올바르지 않습니다."


def _make_account_id(google_sub: str, key: str) -> str:
    """Stable, globally-unique account id from user + caller key."""
    digest = hashlib.sha256(f"{google_sub}:{key}".encode("utf-8")).hexdigest()[:16]
    return f"acc-{digest}"


async def list_accounts(google_sub: str) -> list[dict]:
    db = await get_db()
    cursor = await db.execute(
        "SELECT account_id, google_sub, name, type, sort_order, created_at, updated_at "
        "FROM portfolio_accounts WHERE google_sub = ? ORDER BY sort_order, created_at",
        (google_sub,),
    )
    return [dict(r) for r in await cursor.fetchall()]


async def get_account(google_sub: str, account_id: str) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT account_id, google_sub, name, type, sort_order, created_at, updated_at "
        "FROM portfolio_accounts WHERE google_sub = ? AND account_id = ?",
        (google_sub, account_id),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def get_default_account_id(google_sub: str) -> str | None:
    """Return the user's lowest-sort_order account id, or None if none exist."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT account_id FROM portfolio_accounts WHERE google_sub = ? ORDER BY sort_order LIMIT 1",
        (google_sub,),
    )
    row = await cursor.fetchone()
    return row["account_id"] if row else None


async def create_account(
    google_sub: str,
    *,
    name: str,
    type: str = "general",
    sort_order: int | None = None,
) -> dict:
    name = (name or "").strip()
    if not name:
        raise AccountError("계좌 이름을 입력하세요.")
    if len(name) > MAX_NAME_LENGTH:
        raise AccountError(f"계좌 이름은 {MAX_NAME_LENGTH}자 이하여야 합니다.")
    if type not in ALLOWED_ACCOUNT_TYPES:
        raise AccountError(f"계좌 종류는 {', '.join(sorted(ALLOWED_ACCOUNT_TYPES))} 중 하나여야 합니다.")

    db = await get_db()
    # sort_order 미지정 시 맨 뒤로.
    if sort_order is None:
        cursor = await db.execute(
            "SELECT COALESCE(MAX(sort_order), -1) + 1 AS next_order "
            "FROM portfolio_accounts WHERE google_sub = ?",
            (google_sub,),
        )
        row = await cursor.fetchone()
        sort_order = int(row["next_order"])

    now = now_iso()
    # 재시도 안전한 id 생성: 이름 기반 + nonce. 같은 이름으로 여러 번 만들어도
    # id 충돌이 나지 않게 초단위 nonce를 섞는다.
    raw_key = f"{name}:{sort_order}:{now}"
    account_id = _make_account_id(google_sub, raw_key)
    try:
        await db.execute(
            "INSERT INTO portfolio_accounts "
            "(account_id, google_sub, name, type, sort_order, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (account_id, google_sub, name, type, sort_order, now, now),
        )
        await db.commit()
    except Exception as exc:
        # UNIQUE(google_sub, name) 위반 가능 — 사용자 친화 메시지로 변환.
        if "UNIQUE" in str(exc) and "name" in str(exc).lower():
            raise AccountError("같은 이름의 계좌가 이미 있습니다.") from exc
        raise
    return {
        "account_id": account_id,
        "google_sub": google_sub,
        "name": name,
        "type": type,
        "sort_order": sort_order,
        "created_at": now,
        "updated_at": now,
    }


async def update_account(
    google_sub: str,
    account_id: str,
    *,
    name: str | None = None,
    type: str | None = None,
) -> dict:
    account = await get_account(google_sub, account_id)
    if not account:
        raise AccountError("계좌를 찾을 수 없습니다.")

    updates: list[str] = []
    params: list = []
    if name is not None:
        name = name.strip()
        if not name:
            raise AccountError("계좌 이름을 입력하세요.")
        if len(name) > MAX_NAME_LENGTH:
            raise AccountError(f"계좌 이름은 {MAX_NAME_LENGTH}자 이하여야 합니다.")
        updates.append("name = ?")
        params.append(name)
    if type is not None:
        if type not in ALLOWED_ACCOUNT_TYPES:
            raise AccountError(f"계좌 종류는 {', '.join(sorted(ALLOWED_ACCOUNT_TYPES))} 중 하나여야 합니다.")
        updates.append("type = ?")
        params.append(type)
    if not updates:
        return account

    updates.append("updated_at = ?")
    params.append(now_iso())
    params.extend([account_id, google_sub])

    db = await get_db()
    try:
        await db.execute(
            "UPDATE portfolio_accounts SET " + ", ".join(updates)
            + " WHERE account_id = ? AND google_sub = ?",
            params,
        )
        await db.commit()
    except Exception as exc:
        if "UNIQUE" in str(exc) and "name" in str(exc).lower():
            raise AccountError("같은 이름의 계좌가 이미 있습니다.") from exc
        raise
    return await get_account(google_sub, account_id)  # type: ignore[return-value]


async def delete_account(google_sub: str, account_id: str) -> None:
    """Delete an account, re-parenting its holdings to the user's default.

    The lowest-sort_order account is the default and cannot be deleted (it is
    the fallback bucket). Holdings on the deleted account move to the default.
    """
    default_id = await get_default_account_id(google_sub)
    if default_id is None:
        raise AccountError("삭제할 계좌가 없습니다.")
    if account_id == default_id:
        raise AccountError("기본 계좌는 삭제할 수 없습니다.")

    db = await get_db()
    # 보유 종목을 default 계좌로 재귀속 (orphan NULL 방지).
    await db.execute(
        "UPDATE user_portfolio SET account_id = ? WHERE google_sub = ? AND account_id = ?",
        (default_id, google_sub, account_id),
    )
    await db.execute(
        "DELETE FROM portfolio_accounts WHERE account_id = ? AND google_sub = ?",
        (account_id, google_sub),
    )
    await db.commit()


async def reorder_accounts(google_sub: str, ordered_ids: list[str]) -> list[dict]:
    """Set sort_order from the position of each id in ``ordered_ids``.

    Only ids belonging to ``google_sub`` are accepted; unknown ids are ignored
    rather than erroring so the UI can send a partial list safely.
    """
    if not ordered_ids:
        return await list_accounts(google_sub)
    db = await get_db()
    now = now_iso()
    for idx, aid in enumerate(ordered_ids):
        await db.execute(
            "UPDATE portfolio_accounts SET sort_order = ?, updated_at = ? "
            "WHERE account_id = ? AND google_sub = ?",
            (idx, now, aid, google_sub),
        )
    await db.commit()
    return await list_accounts(google_sub)
