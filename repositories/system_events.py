"""System-events (observability) repository.

Extracted verbatim from cache.py. Behavior is unchanged; cache.py re-exports
these as ``cache.<fn>`` so observability and the admin dashboard keep working.
"""

from __future__ import annotations

from datetime import datetime

from repositories.db import get_db


async def insert_system_event(
    level: str,
    source: str,
    kind: str,
    *,
    stock_code: str | None = None,
    details: str | None = None,
    ts: str | None = None,
) -> int:
    """Append a structured event row. `details` should be a JSON string —
    callers (via observability.record_event) serialize their payload once
    to avoid a redundant json.loads at read time."""
    db = await get_db()
    if ts is None:
        ts = datetime.now().isoformat(timespec="seconds")
    cursor = await db.execute(
        """INSERT INTO system_events (ts, level, source, kind, stock_code, details)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (ts, level, source, kind, stock_code, details),
    )
    await db.commit()
    return cursor.lastrowid


async def get_system_events(
    *,
    source: str | None = None,
    level: str | None = None,
    stock_code: str | None = None,
    since: str | None = None,
    limit: int = 100,
) -> list[dict]:
    """Filtered newest-first fetch for the admin dashboard."""
    clauses: list[str] = []
    params: list = []
    if source:
        clauses.append("source = ?")
        params.append(source)
    if level:
        clauses.append("level = ?")
        params.append(level)
    if stock_code:
        clauses.append("stock_code = ?")
        params.append(stock_code)
    if since:
        clauses.append("ts >= ?")
        params.append(since)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    # Hard cap — prevent a frontend bug or curl typo from slurping the
    # whole table.
    limit = max(1, min(int(limit), 1000))
    params.append(limit)
    db = await get_db()
    cursor = await db.execute(
        f"SELECT id, ts, level, source, kind, stock_code, details "
        f"FROM system_events {where} ORDER BY ts DESC, id DESC LIMIT ?",
        params,
    )
    return [dict(r) for r in await cursor.fetchall()]


async def summarize_system_events(since_iso: str) -> dict:
    """Aggregate counts by (source, level) since `since_iso`. Used by the
    top-of-dashboard status card so the admin sees failure spikes without
    scrolling through events."""
    db = await get_db()
    cursor = await db.execute(
        """SELECT source, level, COUNT(*) AS n
           FROM system_events WHERE ts >= ?
           GROUP BY source, level""",
        (since_iso,),
    )
    out: dict[str, dict[str, int]] = {}
    for row in await cursor.fetchall():
        out.setdefault(row["source"], {})[row["level"]] = int(row["n"])
    return out


async def prune_system_events(max_age_days: int = 30, max_rows: int = 100_000) -> int:
    """Best-effort cleanup: drop events older than `max_age_days`, then if
    still above `max_rows` trim the oldest until under the cap. Returns
    rows deleted (0 if table was already small)."""
    db = await get_db()
    # Age-based trim.
    cursor = await db.execute(
        "DELETE FROM system_events WHERE ts < datetime('now', ?)",
        (f"-{int(max_age_days)} days",),
    )
    age_deleted = cursor.rowcount or 0
    # Row-count trim as a safety net. Count only if there's risk of being
    # over — skip when obviously fine.
    cursor = await db.execute("SELECT COUNT(*) AS n FROM system_events")
    row = await cursor.fetchone()
    total = int(row["n"]) if row else 0
    overflow_deleted = 0
    if total > max_rows:
        excess = total - max_rows
        cursor = await db.execute(
            "DELETE FROM system_events WHERE id IN "
            "(SELECT id FROM system_events ORDER BY ts ASC, id ASC LIMIT ?)",
            (excess,),
        )
        overflow_deleted = cursor.rowcount or 0
    await db.commit()
    return age_deleted + overflow_deleted


async def summarize_http_metrics(since_iso: str, *, limit: int = 100) -> list[dict]:
    """Aggregate recorded HTTP events (source='http') by request path.

    The latency-observer middleware only records *slow* (>= threshold) and
    *error* (5xx) requests, so these rows describe the problematic tail — not
    all traffic. Counts/latencies should be read as "how bad is the slow path",
    not "average response time". Durations live in the JSON ``details`` blob, so
    we pull them out with ``json_extract``.
    """
    db = await get_db()
    limit = max(1, min(int(limit), 1000))
    cursor = await db.execute(
        """
        SELECT
          json_extract(details, '$.path') AS path,
          COUNT(*) AS count,
          SUM(CASE WHEN kind = 'error' THEN 1 ELSE 0 END) AS errors,
          AVG(json_extract(details, '$.duration_ms')) AS avg_ms,
          MAX(json_extract(details, '$.duration_ms')) AS max_ms,
          MAX(ts) AS last_ts
        FROM system_events
        WHERE source = 'http' AND ts >= ?
        GROUP BY path
        ORDER BY errors DESC, count DESC, max_ms DESC
        LIMIT ?
        """,
        (since_iso, limit),
    )
    out: list[dict] = []
    for row in await cursor.fetchall():
        item = dict(row)
        if item.get("avg_ms") is not None:
            item["avg_ms"] = round(float(item["avg_ms"]), 1)
        if item.get("max_ms") is not None:
            item["max_ms"] = round(float(item["max_ms"]), 1)
        out.append(item)
    return out


async def get_latest_event(source: str, kind: str | None = None) -> dict | None:
    """Return the most recent matching event. Dashboard uses this to show
    'last successful tick' per subsystem."""
    db = await get_db()
    if kind:
        cursor = await db.execute(
            "SELECT id, ts, level, source, kind, stock_code, details "
            "FROM system_events WHERE source = ? AND kind = ? "
            "ORDER BY ts DESC, id DESC LIMIT 1",
            (source, kind),
        )
    else:
        cursor = await db.execute(
            "SELECT id, ts, level, source, kind, stock_code, details "
            "FROM system_events WHERE source = ? "
            "ORDER BY ts DESC, id DESC LIMIT 1",
            (source,),
        )
    row = await cursor.fetchone()
    return dict(row) if row else None
