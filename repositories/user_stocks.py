"""Per-user stock interactions: recent analyses, starred/preference, ordering.

Extracted verbatim from cache.py; re-exported as ``cache.<fn>``.
"""

from __future__ import annotations

import json
from datetime import datetime

from repositories.db import get_db

USER_RECENT_MAX = 20


async def touch_user_recent_analysis(google_sub: str, stock_code: str):
    db = await get_db()
    now = datetime.now().isoformat()
    await db.execute(
        """
        INSERT OR REPLACE INTO user_recent_analyses (google_sub, stock_code, viewed_at)
        VALUES (?, ?, ?)
        """,
        (google_sub, stock_code, now),
    )
    await db.execute(
        """
        UPDATE user_stock_preferences
        SET sort_order = sort_order + 1
        WHERE google_sub = ? AND sort_order IS NOT NULL
        """,
        (google_sub,),
    )
    await db.execute(
        """
        INSERT INTO user_stock_preferences (google_sub, stock_code, is_starred, is_pinned, sort_order, note, updated_at)
        VALUES (?, ?, 0, 0, 0, '', ?)
        ON CONFLICT(google_sub, stock_code) DO UPDATE SET
            sort_order = 0,
            updated_at = excluded.updated_at
        """,
        (google_sub, stock_code, now),
    )
    # Remove overflow items beyond the limit (keep starred/pinned)
    cursor = await db.execute(
        """
        SELECT r.stock_code
        FROM user_recent_analyses r
        LEFT JOIN user_stock_preferences p
            ON p.google_sub = r.google_sub AND p.stock_code = r.stock_code
        WHERE r.google_sub = ?
          AND COALESCE(p.is_starred, 0) = 0
          AND COALESCE(p.is_pinned, 0) = 0
        ORDER BY COALESCE(p.sort_order, 999999) ASC
        LIMIT -1 OFFSET ?
        """,
        (google_sub, USER_RECENT_MAX),
    )
    overflow = [row["stock_code"] for row in await cursor.fetchall()]
    if overflow:
        placeholders = ",".join("?" for _ in overflow)
        await db.execute(
            f"DELETE FROM user_recent_analyses WHERE google_sub = ? AND stock_code IN ({placeholders})",
            (google_sub, *overflow),
        )
    await db.commit()


async def delete_user_recent_analysis(google_sub: str, stock_code: str):
    db = await get_db()
    await db.execute(
        "DELETE FROM user_recent_analyses WHERE google_sub = ? AND stock_code = ?",
        (google_sub, stock_code),
    )
    await db.commit()


async def get_user_stock_preference(google_sub: str, stock_code: str) -> dict:
    db = await get_db()
    cursor = await db.execute(
        """
        SELECT is_starred, is_pinned, sort_order, starred_order, note, updated_at
        FROM user_stock_preferences
        WHERE google_sub = ? AND stock_code = ?
        """,
        (google_sub, stock_code),
    )
    row = await cursor.fetchone()
    if not row:
        return {
            "is_starred": False,
            "is_pinned": False,
            "sort_order": None,
            "starred_order": None,
            "note": "",
            "updated_at": None,
        }
    return {
        "is_starred": bool(row["is_starred"]),
        "is_pinned": bool(row["is_pinned"]),
        "sort_order": row["sort_order"],
        "starred_order": row["starred_order"],
        "note": row["note"] or "",
        "updated_at": row["updated_at"],
    }


async def save_user_stock_preference(
    google_sub: str,
    stock_code: str,
    *,
    is_starred: bool | None = None,
    is_pinned: bool | None = None,
    note: str | None = None,
    sort_order: int | None = None,
) -> dict:
    current = await get_user_stock_preference(google_sub, stock_code)
    next_pref = {
        "is_starred": current["is_starred"] if is_starred is None else bool(is_starred),
        "is_pinned": current["is_pinned"] if is_pinned is None else bool(is_pinned),
        "sort_order": current["sort_order"] if sort_order is None else int(sort_order),
        "note": current["note"] if note is None else note.strip()[:2000],
    }

    db = await get_db()
    updated_at = datetime.now().isoformat()
    becoming_starred = next_pref["is_starred"] and not current["is_starred"]
    becoming_unstarred = not next_pref["is_starred"] and current["is_starred"]

    if becoming_starred:
        await db.execute(
            "UPDATE user_stock_preferences SET starred_order = starred_order + 1 WHERE google_sub = ? AND starred_order IS NOT NULL",
            (google_sub,),
        )

    await db.execute(
        """
        INSERT OR REPLACE INTO user_stock_preferences (
            google_sub, stock_code, is_starred, is_pinned, sort_order, starred_order, note, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            google_sub,
            stock_code,
            1 if next_pref["is_starred"] else 0,
            1 if next_pref["is_pinned"] else 0,
            next_pref["sort_order"],
            0 if becoming_starred else (None if becoming_unstarred else current.get("starred_order")),
            next_pref["note"],
            updated_at,
        ),
    )
    await db.commit()

    return {
        **next_pref,
        "updated_at": updated_at,
    }


async def get_cached_analyses(
    limit: int | None = None,
    include_quotes: bool = False,
    google_sub: str | None = None,
    tab: str = "recent",
) -> list[dict]:
    db = await get_db()
    select_fields = "stock_code, corp_name, analyzed_at"
    if include_quotes:
        select_fields += ", payload_json"

    if google_sub and tab == "starred":
        query = (
            "SELECT a.stock_code, a.corp_name, a.analyzed_at"
            + (", a.payload_json" if include_quotes else "")
            + ", 1 AS is_starred"
            + ", COALESCE(p.note, '') AS note"
            + " FROM user_stock_preferences p"
            + " JOIN analysis_meta a ON a.stock_code = p.stock_code"
            + " WHERE p.google_sub = ? AND p.is_starred = 1"
            + " ORDER BY CASE WHEN p.starred_order IS NULL THEN 1 ELSE 0 END, p.starred_order ASC, p.updated_at DESC"
        )
        params: tuple = (google_sub,)
    elif google_sub:
        query = (
            "SELECT a.stock_code, a.corp_name, r.viewed_at AS analyzed_at"
            + (", a.payload_json" if include_quotes else "")
            + ", COALESCE(p.is_starred, 0) AS is_starred"
            + ", COALESCE(p.note, '') AS note"
            + " FROM user_recent_analyses r"
            + " JOIN analysis_meta a ON a.stock_code = r.stock_code"
            + " LEFT JOIN user_stock_preferences p ON p.google_sub = r.google_sub AND p.stock_code = r.stock_code"
            + " WHERE r.google_sub = ?"
            + " ORDER BY CASE WHEN p.sort_order IS NULL THEN 1 ELSE 0 END, p.sort_order ASC, r.viewed_at DESC"
        )
        params = (google_sub,)
    else:
        query = f"SELECT {select_fields} FROM analysis_meta ORDER BY analyzed_at DESC"
        params = ()

    if limit is not None:
        query += " LIMIT ?"
        params = (*params, limit)

    cursor = await db.execute(query, params)
    rows = await cursor.fetchall()
    items = []
    for row in rows:
        item = dict(row)
        item["is_starred"] = bool(item.get("is_starred"))
        item["note"] = item.get("note") or ""
        item.pop("sort_order", None)
        item.pop("is_pinned", None)
        if include_quotes:
            payload_json = item.pop("payload_json", None)
            quote_snapshot = {}
            if payload_json:
                try:
                    payload = json.loads(payload_json)
                except json.JSONDecodeError:
                    payload = {}
                if isinstance(payload, dict):
                    cached_quote = payload.get("quote_snapshot")
                    if isinstance(cached_quote, dict):
                        quote_snapshot = cached_quote
            item["quote_snapshot"] = quote_snapshot
        items.append(item)
    return items


async def save_user_stock_order(google_sub: str, ordered_stock_codes: list[str]):
    db = await get_db()
    updated_at = datetime.now().isoformat()
    await db.executemany(
        """
        INSERT INTO user_stock_preferences (
            google_sub, stock_code, is_starred, is_pinned, sort_order, note, updated_at
        ) VALUES (?, ?, 0, 0, ?, '', ?)
        ON CONFLICT(google_sub, stock_code) DO UPDATE SET
            sort_order = excluded.sort_order,
            updated_at = excluded.updated_at
        """,
        [
            (google_sub, stock_code, index, updated_at)
            for index, stock_code in enumerate(ordered_stock_codes)
        ],
    )
    await db.commit()


async def save_starred_order(google_sub: str, ordered_stock_codes: list[str]):
    db = await get_db()
    updated_at = datetime.now().isoformat()
    await db.executemany(
        """
        UPDATE user_stock_preferences
        SET starred_order = ?, updated_at = ?
        WHERE google_sub = ? AND stock_code = ?
        """,
        [
            (index, updated_at, google_sub, stock_code)
            for index, stock_code in enumerate(ordered_stock_codes)
        ],
    )
    await db.commit()


async def unstar_stock(google_sub: str, stock_code: str):
    db = await get_db()
    await db.execute(
        """
        UPDATE user_stock_preferences
        SET is_starred = 0, starred_order = NULL, updated_at = ?
        WHERE google_sub = ? AND stock_code = ?
        """,
        (datetime.now().isoformat(), google_sub, stock_code),
    )
    await db.commit()
