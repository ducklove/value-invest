"""Users + login sessions.

Extracted verbatim from cache.py; re-exported as ``cache.<fn>``.
"""

from __future__ import annotations

from datetime import datetime

import cache


async def upsert_user(user: dict):
    db = await cache.get_db()
    now = datetime.now().isoformat()
    await db.execute(
        """
        INSERT INTO users (google_sub, email, name, picture, email_verified, created_at, last_login_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(google_sub) DO UPDATE SET
            email = excluded.email,
            name = excluded.name,
            picture = excluded.picture,
            email_verified = excluded.email_verified,
            last_login_at = excluded.last_login_at
        """,
        (
            user["google_sub"],
            user["email"],
            user["name"],
            user.get("picture"),
            1 if user.get("email_verified") else 0,
            now,
            now,
        ),
    )
    await db.commit()


async def create_user_session(session_token_hash: str, google_sub: str, expires_at: str):
    db = await cache.get_db()
    await db.execute(
        """
        INSERT OR REPLACE INTO user_sessions (session_token_hash, google_sub, created_at, expires_at)
        VALUES (?, ?, ?, ?)
        """,
        (session_token_hash, google_sub, datetime.now().isoformat(), expires_at),
    )
    await db.commit()


async def get_user_by_session(session_token_hash: str) -> dict | None:
    db = await cache.get_db()
    cursor = await db.execute(
        """
        SELECT u.google_sub, u.email, u.name, u.picture, u.email_verified, u.is_admin
        FROM user_sessions s
        JOIN users u ON u.google_sub = s.google_sub
        WHERE s.session_token_hash = ? AND s.expires_at > ?
        """,
        (session_token_hash, datetime.now().isoformat()),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def delete_user_session(session_token_hash: str):
    db = await cache.get_db()
    await db.execute(
        "DELETE FROM user_sessions WHERE session_token_hash = ?",
        (session_token_hash,),
    )
    await db.commit()


async def delete_expired_sessions():
    db = await cache.get_db()
    await db.execute(
        "DELETE FROM user_sessions WHERE expires_at <= ?",
        (datetime.now().isoformat(),),
    )
    await db.commit()


async def get_all_users() -> list[dict]:
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT google_sub, email, name, picture, is_admin, created_at, last_login_at FROM users ORDER BY last_login_at DESC"
    )
    return [dict(row) for row in await cursor.fetchall()]
