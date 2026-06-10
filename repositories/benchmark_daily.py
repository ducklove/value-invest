"""Benchmark daily-series repository (KOSPI / SP500 / GOLD / ...).

Extracted verbatim from cache.py. Behavior is unchanged; cache.py re-exports
these as ``cache.<fn>``.
"""

from __future__ import annotations

from datetime import datetime

from repositories.db import get_db


async def save_benchmark_rows(code: str, rows: list[dict], source: str = "yfinance") -> int:
    """Upsert daily rows for a benchmark code.

    rows: [{"date": "YYYY-MM-DD", "close": float}, ...]
    Returns number of rows written. Safe to call with overlapping ranges —
    the (code, date) PK makes each call idempotent.
    """
    if not rows:
        return 0
    # Match the rest of this module — plain local-time ISO (no TZ suffix).
    fetched_at = datetime.now().isoformat(timespec="seconds")
    db = await get_db()
    await db.executemany(
        """INSERT INTO benchmark_daily (code, date, close, source, fetched_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(code, date) DO UPDATE SET
               close=excluded.close,
               source=excluded.source,
               fetched_at=excluded.fetched_at""",
        [(code, r["date"], float(r["close"]), source, fetched_at) for r in rows if r.get("close") is not None],
    )
    await db.commit()
    return len(rows)


async def get_benchmark_rows(code: str, start: str | None = None, end: str | None = None) -> list[dict]:
    """Read {date, close} rows for a benchmark code, optionally bounded by
    inclusive [start, end] (YYYY-MM-DD). Ordered by date ascending."""
    db = await get_db()
    clauses = ["code = ?"]
    params: list = [code]
    if start:
        clauses.append("date >= ?")
        params.append(start)
    if end:
        clauses.append("date <= ?")
        params.append(end)
    sql = f"SELECT date, close FROM benchmark_daily WHERE {' AND '.join(clauses)} ORDER BY date ASC"
    cursor = await db.execute(sql, params)
    return [{"date": r["date"], "close": r["close"]} for r in await cursor.fetchall()]


async def get_benchmark_last_date(code: str) -> str | None:
    """Return the latest stored date for a code, or None if table is empty
    for that code. Used to compute the incremental fetch window."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT MAX(date) AS d FROM benchmark_daily WHERE code = ?",
        (code,),
    )
    row = await cursor.fetchone()
    return row["d"] if row and row["d"] else None


async def get_benchmark_earliest_date(code: str) -> str | None:
    """Return the earliest stored date for a code. Used by lazy backfill to
    decide whether the request's `start` predates what we have on disk."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT MIN(date) AS d FROM benchmark_daily WHERE code = ?",
        (code,),
    )
    row = await cursor.fetchone()
    return row["d"] if row and row["d"] else None
