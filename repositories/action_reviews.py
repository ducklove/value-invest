"""Portfolio action-board review queue state.

The board itself is derived from current portfolio data and external signals.
This repository stores only the user's review state for each stable action key.
"""

from __future__ import annotations

from datetime import datetime

from repositories.db import get_db

VALID_STATUSES = ("open", "done", "dismissed")

_REVIEW_COLUMNS = "id, google_sub, action_key, status, note, created_at, updated_at"


def _now() -> str:
    return datetime.now().isoformat()


async def list_reviews(google_sub: str, action_keys: list[str] | None = None) -> dict[str, dict]:
    """Return review rows keyed by action_key.

    If action_keys is empty, no rows are returned. Passing None returns all rows
    for the user, which is useful for admin/debug flows but not used by the
    action board hot path.
    """
    db = await get_db()
    if action_keys is not None:
        keys = [str(k) for k in action_keys if str(k)]
        if not keys:
            return {}
        placeholders = ",".join("?" for _ in keys)
        cursor = await db.execute(
            f"SELECT {_REVIEW_COLUMNS} FROM portfolio_action_reviews"
            f" WHERE google_sub = ? AND action_key IN ({placeholders})",
            (google_sub, *keys),
        )
    else:
        cursor = await db.execute(
            f"SELECT {_REVIEW_COLUMNS} FROM portfolio_action_reviews WHERE google_sub = ?",
            (google_sub,),
        )
    rows = [dict(row) for row in await cursor.fetchall()]
    return {row["action_key"]: row for row in rows}


async def set_review_status(
    google_sub: str,
    action_key: str,
    status: str,
    *,
    note: str | None = None,
) -> dict:
    """Upsert one review state and return the saved row."""
    if status not in VALID_STATUSES:
        raise ValueError(f"unsupported action review status: {status!r}")
    key = str(action_key or "").strip()
    if not key:
        raise ValueError("action_key is required")
    now = _now()
    db = await get_db()
    await db.execute(
        """
        INSERT INTO portfolio_action_reviews
            (google_sub, action_key, status, note, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(google_sub, action_key) DO UPDATE SET
            status = excluded.status,
            note = excluded.note,
            updated_at = excluded.updated_at
        """,
        (google_sub, key, status, note, now, now),
    )
    await db.commit()
    cursor = await db.execute(
        f"SELECT {_REVIEW_COLUMNS} FROM portfolio_action_reviews"
        " WHERE google_sub = ? AND action_key = ?",
        (google_sub, key),
    )
    row = await cursor.fetchone()
    if row is None:
        raise RuntimeError("saved action review was not readable")
    return dict(row)
