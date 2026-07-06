# ruff: noqa: E402, I001
from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.config import load_environment  # noqa: E402

load_environment(ROOT, force=True)

from repositories import bootstrap

from repositories import db as db_repo
from services.portfolio import fx, period_reports  # noqa: E402

SNAP_DATE = "2026-06-30"
REPORT_PERIODS = (("monthly", "2026-06"),)


def backup_db() -> str:
    db_path = Path("cache.db").resolve()
    backup_dir = Path("data/db-imports")
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = backup_dir / f"cache.before-2026-06-30-quantity-backfill.{stamp}.db"
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
    await bootstrap.init_db()
    db = await db_repo.get_db()
    rows = await fetchall(
        db,
        """
        SELECT
            ps.google_sub,
            u.email,
            ps.stock_code,
            ps.market_value,
            ps.group_name AS snapshot_group_name,
            up.stock_name,
            up.quantity AS current_quantity,
            up.avg_price,
            COALESCE(up.avg_price_currency, 'KRW') AS avg_price_currency,
            up.group_name AS current_group_name
        FROM portfolio_stock_snapshots ps
        LEFT JOIN user_portfolio up
          ON up.google_sub = ps.google_sub
         AND up.stock_code = ps.stock_code
        LEFT JOIN users u
          ON u.google_sub = ps.google_sub
        WHERE ps.date = ?
          AND (ps.quantity IS NULL OR ps.unit_price IS NULL OR ps.avg_price_krw IS NULL OR ps.cost_basis IS NULL)
        ORDER BY ps.google_sub, ps.stock_code
        """,
        (SNAP_DATE,),
    )

    by_user: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_user[row["google_sub"]].append(row)

    prepared: list[dict] = []
    skipped_users: list[dict] = []
    for google_sub, user_rows in by_user.items():
        missing = [row["stock_code"] for row in user_rows if row.get("current_quantity") is None]
        if missing:
            skipped_users.append({
                "google_sub": google_sub,
                "email": user_rows[0].get("email"),
                "reason": "snapshot stock is not in current portfolio",
                "stock_codes": missing,
            })
            continue
        holdings = [
            {
                "avg_price": row.get("avg_price"),
                "avg_price_currency": row.get("avg_price_currency") or "KRW",
            }
            for row in user_rows
        ]
        await fx.annotate_avg_price_krw(holdings)
        for row, holding in zip(user_rows, holdings):
            qty = float(row.get("current_quantity") or 0)
            market_value = float(row.get("market_value") or 0)
            avg_price_krw = float(holding.get("avg_price_krw") or 0)
            prepared.append({
                "google_sub": row["google_sub"],
                "email": row.get("email"),
                "stock_code": row["stock_code"],
                "quantity": qty,
                "unit_price": (market_value / qty) if abs(qty) > 1e-12 else None,
                "avg_price_krw": avg_price_krw,
                "cost_basis": qty * avg_price_krw,
                "group_name": row.get("snapshot_group_name") or row.get("current_group_name"),
            })

    await db.execute("BEGIN IMMEDIATE")
    try:
        for row in prepared:
            await db.execute(
                """
                UPDATE portfolio_stock_snapshots
                   SET quantity = ?,
                       unit_price = ?,
                       avg_price_krw = ?,
                       cost_basis = ?,
                       group_name = COALESCE(group_name, ?)
                 WHERE google_sub = ?
                   AND date = ?
                   AND stock_code = ?
                """,
                (
                    row["quantity"],
                    row["unit_price"],
                    row["avg_price_krw"],
                    row["cost_basis"],
                    row["group_name"],
                    row["google_sub"],
                    SNAP_DATE,
                    row["stock_code"],
                ),
            )
        await db.commit()
    except Exception:
        await db.rollback()
        raise

    impacted_users = sorted({row["google_sub"] for row in prepared})
    regenerated_reports = []
    for google_sub in impacted_users:
        for period_type, period_key in REPORT_PERIODS:
            try:
                saved = await period_reports.generate_and_save_period_report(google_sub, period_type, period_key)
                regenerated_reports.append({
                    "google_sub": google_sub,
                    "email": next((row.get("email") for row in prepared if row["google_sub"] == google_sub), None),
                    "period_type": period_type,
                    "period_key": period_key,
                    "source_hash": saved.get("source_hash"),
                    "warnings": (saved.get("report") or {}).get("data_quality", {}).get("warnings", []),
                })
            except Exception as exc:
                regenerated_reports.append({
                    "google_sub": google_sub,
                    "period_type": period_type,
                    "period_key": period_key,
                    "error": str(exc),
                })

    remaining = await fetchall(
        db,
        """
        SELECT ps.google_sub, u.email, COUNT(*) AS missing_rows
        FROM portfolio_stock_snapshots ps
        LEFT JOIN users u ON u.google_sub = ps.google_sub
        WHERE ps.date = ?
          AND ps.quantity IS NULL
        GROUP BY ps.google_sub, u.email
        ORDER BY missing_rows DESC
        """,
        (SNAP_DATE,),
    )
    print(json.dumps({
        "ok": True,
        "backup_path": backup_path,
        "snapshot_date": SNAP_DATE,
        "updated_rows": len(prepared),
        "updated_users": len(impacted_users),
        "skipped_users": skipped_users,
        "remaining_quantity_null": remaining,
        "regenerated_reports": regenerated_reports,
    }, ensure_ascii=False, indent=2))
    await bootstrap.close_db()


asyncio.run(main())
