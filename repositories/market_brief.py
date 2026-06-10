"""Per-user daily market brief cache.

Extracted verbatim from cache.py; re-exported as ``cache.<fn>``.
"""

from __future__ import annotations

from datetime import datetime
import json

from repositories.db import get_db


async def get_daily_market_brief(
    google_sub: str,
    brief_date: str,
    *,
    max_age_minutes: int | None = None,
) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        """
        SELECT google_sub, brief_date, source_hash, payload_json, markdown,
               model, tokens_in, tokens_out, cost_usd, created_at, updated_at
        FROM daily_market_briefs
        WHERE google_sub = ? AND brief_date = ?
        """,
        (google_sub, brief_date),
    )
    row = await cursor.fetchone()
    if not row:
        return None
    item = dict(row)
    if max_age_minutes is not None:
        try:
            updated_at = datetime.fromisoformat(item["updated_at"])
        except Exception:
            updated_at = datetime.min
        age_seconds = (datetime.now() - updated_at).total_seconds()
        if age_seconds > max_age_minutes * 60:
            return None
    try:
        item["payload"] = json.loads(item.pop("payload_json") or "{}")
    except json.JSONDecodeError:
        item["payload"] = {}
    return item


async def save_daily_market_brief(
    *,
    google_sub: str,
    brief_date: str,
    source_hash: str,
    payload: dict,
    markdown: str,
    model: str | None,
    tokens_in: int | None = None,
    tokens_out: int | None = None,
    cost_usd: float | None = None,
) -> dict:
    db = await get_db()
    now = datetime.now().isoformat()
    await db.execute(
        """
        INSERT INTO daily_market_briefs
            (google_sub, brief_date, source_hash, payload_json, markdown,
             model, tokens_in, tokens_out, cost_usd, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(google_sub, brief_date) DO UPDATE SET
            source_hash = excluded.source_hash,
            payload_json = excluded.payload_json,
            markdown = excluded.markdown,
            model = excluded.model,
            tokens_in = excluded.tokens_in,
            tokens_out = excluded.tokens_out,
            cost_usd = excluded.cost_usd,
            updated_at = excluded.updated_at
        """,
        (
            google_sub,
            brief_date,
            source_hash,
            json.dumps(payload, ensure_ascii=False),
            markdown,
            model,
            int(tokens_in or 0) if tokens_in is not None else None,
            int(tokens_out or 0) if tokens_out is not None else None,
            float(cost_usd or 0) if cost_usd is not None else None,
            now,
            now,
        ),
    )
    await db.commit()
    return await get_daily_market_brief(google_sub, brief_date) or {
        "google_sub": google_sub,
        "brief_date": brief_date,
        "source_hash": source_hash,
        "payload": payload,
        "markdown": markdown,
        "model": model,
        "tokens_in": tokens_in,
        "tokens_out": tokens_out,
        "cost_usd": cost_usd,
        "created_at": now,
        "updated_at": now,
    }
