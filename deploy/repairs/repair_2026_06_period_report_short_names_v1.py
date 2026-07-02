# ruff: noqa: E402, I001
from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.config import load_environment  # noqa: E402

load_environment(ROOT, force=True)

import cache  # noqa: E402
from services.portfolio import period_reports  # noqa: E402

PERIOD_TYPE = "monthly"
PERIOD_KEY = "2026-06"


def backup_db() -> str:
    db_path = Path("cache.db").resolve()
    backup_dir = Path("data/db-imports")
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = backup_dir / f"cache.before-2026-06-period-report-regen.{stamp}.db"
    src = sqlite3.connect(db_path)
    dst = sqlite3.connect(backup_path)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    return str(backup_path)


async def fetchall(db, sql: str, params: tuple = ()) -> list[dict]:
    cursor = await db.execute(sql, params)
    return [dict(row) for row in await cursor.fetchall()]


async def main() -> None:
    backup_path = backup_db()
    await cache.init_db()
    db = await cache.get_db()
    users = await fetchall(
        db,
        """
        SELECT DISTINCT pr.google_sub, u.email
        FROM portfolio_period_reports pr
        LEFT JOIN users u
          ON u.google_sub = pr.google_sub
        WHERE pr.period_type = ?
          AND pr.period_key = ?
        ORDER BY u.email, pr.google_sub
        """,
        (PERIOD_TYPE, PERIOD_KEY),
    )

    regenerated_reports = []
    for user in users:
        google_sub = user["google_sub"]
        try:
            saved = await period_reports.generate_and_save_period_report(google_sub, PERIOD_TYPE, PERIOD_KEY)
            comp = (saved.get("report") or {}).get("composition_changes") or {}
            regenerated_reports.append({
                "google_sub": google_sub,
                "email": user.get("email"),
                "period_type": PERIOD_TYPE,
                "period_key": PERIOD_KEY,
                "source_hash": saved.get("source_hash"),
                "summary": comp.get("summary") or {},
                "warnings": (saved.get("report") or {}).get("data_quality", {}).get("warnings", []),
            })
        except Exception as exc:
            regenerated_reports.append({
                "google_sub": google_sub,
                "email": user.get("email"),
                "period_type": PERIOD_TYPE,
                "period_key": PERIOD_KEY,
                "error": str(exc),
            })

    errors = [row for row in regenerated_reports if row.get("error")]
    print(json.dumps({
        "ok": not errors,
        "backup_path": backup_path,
        "target_saved_reports": len(users),
        "regenerated_reports": regenerated_reports,
    }, ensure_ascii=False, indent=2))
    await cache.close_db()
    if errors:
        raise SystemExit(1)


asyncio.run(main())
