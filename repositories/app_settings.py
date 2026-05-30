"""Application key-value settings (incl. secret flag).

Extracted verbatim from cache.py; re-exported as ``cache.<fn>``.
"""

from __future__ import annotations

from datetime import datetime

import cache


async def get_app_setting(key: str) -> dict | None:
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT key, value, is_secret, updated_by, updated_at FROM app_settings WHERE key = ?",
        (key,),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def set_app_setting(key: str, value: str, *, is_secret: bool = False, updated_by: str | None = None):
    db = await cache.get_db()
    await db.execute(
        """
        INSERT INTO app_settings (key, value, is_secret, updated_by, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            is_secret = excluded.is_secret,
            updated_by = excluded.updated_by,
            updated_at = excluded.updated_at
        """,
        (key, value, 1 if is_secret else 0, updated_by, datetime.now().isoformat()),
    )
    await db.commit()


async def delete_app_setting(key: str):
    db = await cache.get_db()
    await db.execute("DELETE FROM app_settings WHERE key = ?", (key,))
    await db.commit()
