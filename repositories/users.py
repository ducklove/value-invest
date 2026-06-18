"""Users + login sessions.

Extracted verbatim from cache.py; re-exported as ``cache.<fn>``.
"""

from __future__ import annotations

from datetime import datetime

from repositories.db import get_db


async def upsert_user(user: dict):
    db = await get_db()
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
    db = await get_db()
    await db.execute(
        """
        INSERT OR REPLACE INTO user_sessions (session_token_hash, google_sub, created_at, expires_at)
        VALUES (?, ?, ?, ?)
        """,
        (session_token_hash, google_sub, datetime.now().isoformat(), expires_at),
    )
    await db.commit()


async def get_user_by_session(session_token_hash: str) -> dict | None:
    db = await get_db()
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
    db = await get_db()
    await db.execute(
        "DELETE FROM user_sessions WHERE session_token_hash = ?",
        (session_token_hash,),
    )
    await db.commit()


async def delete_expired_sessions():
    db = await get_db()
    await db.execute(
        "DELETE FROM user_sessions WHERE expires_at <= ?",
        (datetime.now().isoformat(),),
    )
    await db.commit()


async def get_all_users() -> list[dict]:
    db = await get_db()
    cursor = await db.execute(
        """
        SELECT
            u.google_sub, u.email, u.name, u.picture, u.email_verified,
            u.is_admin, u.created_at, u.last_login_at,
            COALESCE(p.portfolio_count, 0) AS portfolio_count,
            p.last_portfolio_updated
        FROM users u
        LEFT JOIN (
            SELECT google_sub, COUNT(*) AS portfolio_count, MAX(updated_at) AS last_portfolio_updated
            FROM user_portfolio
            GROUP BY google_sub
        ) p ON p.google_sub = u.google_sub
        ORDER BY u.last_login_at DESC
        """
    )
    return [dict(row) for row in await cursor.fetchall()]


async def get_user(google_sub: str) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        """
        SELECT google_sub, email, name, picture, email_verified, is_admin,
               created_at, last_login_at
        FROM users
        WHERE google_sub = ?
        """,
        (google_sub,),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def count_admin_users() -> int:
    db = await get_db()
    cursor = await db.execute("SELECT COUNT(*) AS n FROM users WHERE is_admin = 1")
    row = await cursor.fetchone()
    return int(row["n"]) if row else 0


async def update_user_profile(
    google_sub: str,
    *,
    email: str,
    name: str,
    picture: str | None,
    email_verified: bool,
) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        """
        UPDATE users
        SET email = ?, name = ?, picture = ?, email_verified = ?
        WHERE google_sub = ?
        """,
        (email, name, picture, 1 if email_verified else 0, google_sub),
    )
    await db.commit()
    if cursor.rowcount <= 0:
        return None
    return await get_user(google_sub)


async def set_user_admin(google_sub: str, is_admin: bool) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        "UPDATE users SET is_admin = ? WHERE google_sub = ?",
        (1 if is_admin else 0, google_sub),
    )
    await db.commit()
    if cursor.rowcount <= 0:
        return None
    return await get_user(google_sub)


async def delete_user(google_sub: str) -> bool:
    db = await get_db()
    cursor = await db.execute("DELETE FROM users WHERE google_sub = ?", (google_sub,))
    await db.commit()
    return cursor.rowcount > 0


async def search_user_portfolios(query: str, *, limit: int = 50) -> list[dict]:
    """Search holdings across users for the admin console.

    Matches user name/email and portfolio stock code/name. Returns one row per
    holding, capped for dashboard use.
    """
    q = " ".join(str(query or "").strip().split())
    if not q:
        return []
    like = f"%{q.casefold()}%"
    limit = max(1, min(int(limit), 200))
    db = await get_db()
    cursor = await db.execute(
        """
        SELECT
            u.google_sub, u.email, u.name, u.picture, u.is_admin,
            p.stock_code, p.stock_name, p.quantity, p.avg_price,
            COALESCE(p.currency, 'KRW') AS currency,
            p.group_name, p.created_at, p.updated_at,
            COUNT(*) OVER (PARTITION BY u.google_sub) AS matched_holdings
        FROM user_portfolio p
        JOIN users u ON u.google_sub = p.google_sub
        WHERE lower(u.email) LIKE ?
           OR lower(u.name) LIKE ?
           OR lower(p.stock_code) LIKE ?
           OR lower(p.stock_name) LIKE ?
           OR lower(COALESCE(p.group_name, '')) LIKE ?
        ORDER BY u.last_login_at DESC, p.sort_order ASC, p.created_at ASC
        LIMIT ?
        """,
        (like, like, like, like, like, limit),
    )
    return [dict(row) for row in await cursor.fetchall()]
