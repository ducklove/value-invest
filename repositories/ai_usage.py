"""AI usage events + rolling usage summary.

Extracted verbatim from cache.py; re-exported as ``cache.<fn>``.
"""

from __future__ import annotations

from datetime import datetime

import cache


async def insert_ai_usage_event(
    *,
    google_sub: str | None,
    feature: str,
    model: str,
    model_profile: str | None,
    input_tokens: int = 0,
    output_tokens: int = 0,
    cost_usd: float = 0.0,
    latency_ms: int | None = None,
    ok: bool = True,
    error: str | None = None,
):
    db = await cache.get_db()
    await db.execute(
        """
        INSERT INTO ai_usage_events
            (ts, google_sub, feature, model, model_profile, input_tokens, output_tokens, cost_usd, latency_ms, ok, error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            datetime.now().isoformat(),
            google_sub,
            feature,
            model,
            model_profile,
            int(input_tokens or 0),
            int(output_tokens or 0),
            float(cost_usd or 0),
            latency_ms,
            1 if ok else 0,
            (error or "")[:500] if error else None,
        ),
    )
    await db.commit()


async def summarize_ai_usage(days: int = 30) -> dict:
    db = await cache.get_db()
    cursor = await db.execute(
        """
        SELECT feature, model, model_profile,
               COUNT(*) AS calls,
               SUM(input_tokens) AS input_tokens,
               SUM(output_tokens) AS output_tokens,
               SUM(cost_usd) AS cost_usd,
               SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END) AS errors,
               AVG(latency_ms) AS avg_latency_ms,
               MAX(ts) AS latest_at
        FROM ai_usage_events
        WHERE ts >= datetime('now', ?)
        GROUP BY feature, model, model_profile
        ORDER BY cost_usd DESC, calls DESC
        """,
        (f"-{int(days)} days",),
    )
    by_feature = [dict(row) for row in await cursor.fetchall()]
    cursor = await db.execute(
        """
        SELECT ts, feature, model, model_profile, input_tokens, output_tokens, cost_usd, latency_ms, ok, error
        FROM ai_usage_events
        ORDER BY ts DESC, id DESC
        LIMIT 50
        """,
    )
    recent = [dict(row) for row in await cursor.fetchall()]
    return {"days": days, "by_feature": by_feature, "recent": recent}
