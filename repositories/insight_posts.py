"""Community insight-post feed (CRUD + visibility).

Extracted verbatim from cache.py; re-exported as ``cache.<fn>``.
"""

from __future__ import annotations

from datetime import datetime
import json

from repositories.db import get_db


async def create_insight_post(
    *,
    google_sub: str,
    title: str,
    insight_md: str,
    source_type: str = "manual",
    result_summary: dict | None = None,
    result_payload: dict | list | None = None,
    tags: list[str] | None = None,
    visibility: str = "public",
) -> dict:
    db = await get_db()
    now = datetime.now().isoformat()
    await db.execute(
        """
        INSERT INTO insight_posts
            (google_sub, title, insight_md, source_type, result_summary_json, result_payload_json, tags_json, visibility, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            google_sub,
            title,
            insight_md,
            source_type,
            json.dumps(result_summary or {}, ensure_ascii=False),
            json.dumps(result_payload, ensure_ascii=False) if result_payload is not None else None,
            json.dumps(tags or [], ensure_ascii=False),
            visibility,
            now,
            now,
        ),
    )
    await db.commit()
    cursor = await db.execute("SELECT last_insert_rowid() AS id")
    post_id = int((await cursor.fetchone())["id"])
    post = await get_insight_post(post_id, viewer_google_sub=google_sub)
    return post or {"id": post_id}


def _parse_json_field(value: str | None, fallback):
    if not value:
        return fallback
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return fallback


def _inflate_insight_row(row) -> dict:
    out = dict(row)
    out["tags"] = _parse_json_field(out.pop("tags_json", None), [])
    out["result_summary"] = _parse_json_field(out.pop("result_summary_json", None), {})
    payload_raw = out.pop("result_payload_json", None)
    out["has_result_payload"] = bool(payload_raw)
    if payload_raw is not None:
        out["result_payload"] = _parse_json_field(payload_raw, None)
    return out


async def list_insight_posts(*, viewer_google_sub: str | None = None, limit: int = 50) -> list[dict]:
    db = await get_db()
    limit = max(1, min(int(limit or 50), 100))
    if viewer_google_sub:
        cursor = await db.execute(
            """
            SELECT p.*, u.name AS author_name, u.email AS author_email
            FROM insight_posts p
            JOIN users u ON u.google_sub = p.google_sub
            WHERE p.visibility = 'public' OR p.google_sub = ?
            ORDER BY p.created_at DESC, p.id DESC
            LIMIT ?
            """,
            (viewer_google_sub, limit),
        )
    else:
        cursor = await db.execute(
            """
            SELECT p.*, u.name AS author_name, u.email AS author_email
            FROM insight_posts p
            JOIN users u ON u.google_sub = p.google_sub
            WHERE p.visibility = 'public'
            ORDER BY p.created_at DESC, p.id DESC
            LIMIT ?
            """,
            (limit,),
        )
    rows = [_inflate_insight_row(row) for row in await cursor.fetchall()]
    for row in rows:
        row.pop("result_payload", None)
    return rows


async def get_insight_post(post_id: int, *, viewer_google_sub: str | None = None) -> dict | None:
    db = await get_db()
    if viewer_google_sub:
        cursor = await db.execute(
            """
            SELECT p.*, u.name AS author_name, u.email AS author_email
            FROM insight_posts p
            JOIN users u ON u.google_sub = p.google_sub
            WHERE p.id = ? AND (p.visibility = 'public' OR p.google_sub = ?)
            """,
            (post_id, viewer_google_sub),
        )
    else:
        cursor = await db.execute(
            """
            SELECT p.*, u.name AS author_name, u.email AS author_email
            FROM insight_posts p
            JOIN users u ON u.google_sub = p.google_sub
            WHERE p.id = ? AND p.visibility = 'public'
            """,
            (post_id,),
        )
    row = await cursor.fetchone()
    return _inflate_insight_row(row) if row else None


async def delete_insight_post(post_id: int, *, google_sub: str, is_admin: bool = False) -> bool:
    db = await get_db()
    if is_admin:
        cursor = await db.execute("DELETE FROM insight_posts WHERE id = ?", (post_id,))
    else:
        cursor = await db.execute(
            "DELETE FROM insight_posts WHERE id = ? AND google_sub = ?",
            (post_id, google_sub),
        )
    await db.commit()
    return cursor.rowcount > 0
