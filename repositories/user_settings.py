"""User-settings repository (per-user key-value store).

Extracted verbatim from cache.py. cache.py re-exports these as ``cache.<fn>``.
"""

from __future__ import annotations

from datetime import datetime

from repositories.db import get_db


async def get_user_setting(google_sub: str, key: str) -> str | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT value FROM user_settings WHERE google_sub = ? AND key = ?",
        (google_sub, key),
    )
    row = await cursor.fetchone()
    return row["value"] if row else None


async def set_user_setting(google_sub: str, key: str, value: str):
    db = await get_db()
    await db.execute(
        """INSERT INTO user_settings (google_sub, key, value, updated_at)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(google_sub, key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at""",
        (google_sub, key, value, datetime.now().isoformat()),
    )
    await db.commit()
