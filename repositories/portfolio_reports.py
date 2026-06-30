"""Saved monthly/annual portfolio period reports."""

from __future__ import annotations

import json
from typing import Any

from repositories.db import get_db, transaction


def _loads_report(value: str) -> dict[str, Any]:
    try:
        data = json.loads(value or "{}")
    except json.JSONDecodeError:
        data = {}
    return data if isinstance(data, dict) else {}


def _row_to_report(row) -> dict[str, Any]:
    data = _loads_report(row["report_json"])
    return {
        "google_sub": row["google_sub"],
        "period_type": row["period_type"],
        "period_key": row["period_key"],
        "schema_version": row["schema_version"],
        "start_date": row["start_date"],
        "end_date": row["end_date"],
        "baseline_date": row["baseline_date"],
        "source_hash": row["source_hash"],
        "generated_at": row["generated_at"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "report_md": row["report_md"] or "",
        "report": data,
    }


async def save_period_report(
    google_sub: str,
    report: dict[str, Any],
    *,
    report_md: str = "",
) -> dict[str, Any]:
    period = report.get("period") or {}
    period_type = str(period.get("type") or "")
    period_key = str(period.get("key") or "")
    if not period_type or not period_key:
        raise ValueError("report period.type and period.key are required")

    now = str(report.get("generated_at") or "")
    if not now:
        raise ValueError("report.generated_at is required")

    report_json = json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    async with transaction() as db:
        await db.execute(
            """
            INSERT INTO portfolio_period_reports
            (google_sub, period_type, period_key, schema_version, start_date, end_date,
             baseline_date, report_json, report_md, source_hash, generated_at, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(google_sub, period_type, period_key) DO UPDATE SET
                schema_version = excluded.schema_version,
                start_date = excluded.start_date,
                end_date = excluded.end_date,
                baseline_date = excluded.baseline_date,
                report_json = excluded.report_json,
                report_md = excluded.report_md,
                source_hash = excluded.source_hash,
                generated_at = excluded.generated_at,
                updated_at = excluded.updated_at
            """,
            (
                google_sub,
                period_type,
                period_key,
                int(report.get("schema_version") or 1),
                str(period.get("start_date") or ""),
                str(period.get("end_date") or ""),
                period.get("baseline_date"),
                report_json,
                report_md or "",
                str(report.get("source_hash") or ""),
                now,
                now,
                now,
            ),
        )
    saved = await get_period_report(google_sub, period_type, period_key)
    if saved is None:
        raise RuntimeError("saved period report was not readable")
    return saved


async def get_period_report(
    google_sub: str,
    period_type: str,
    period_key: str,
) -> dict[str, Any] | None:
    db = await get_db()
    cursor = await db.execute(
        """
        SELECT google_sub, period_type, period_key, schema_version, start_date, end_date,
               baseline_date, report_json, report_md, source_hash, generated_at, created_at, updated_at
        FROM portfolio_period_reports
        WHERE google_sub = ? AND period_type = ? AND period_key = ?
        """,
        (google_sub, period_type, period_key),
    )
    row = await cursor.fetchone()
    return _row_to_report(row) if row else None


async def list_period_reports(
    google_sub: str,
    *,
    period_type: str | None = None,
    limit: int = 36,
) -> list[dict[str, Any]]:
    params: list[Any] = [google_sub]
    where = "WHERE google_sub = ?"
    if period_type:
        where += " AND period_type = ?"
        params.append(period_type)
    params.append(int(limit))
    db = await get_db()
    cursor = await db.execute(
        f"""
        SELECT google_sub, period_type, period_key, schema_version, start_date, end_date,
               baseline_date, report_json, report_md, source_hash, generated_at, created_at, updated_at
        FROM portfolio_period_reports
        {where}
        ORDER BY end_date DESC, updated_at DESC
        LIMIT ?
        """,
        tuple(params),
    )
    return [_row_to_report(row) for row in await cursor.fetchall()]
