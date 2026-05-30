"""NPS (국민연금공단) holdings + snapshots repository.

Extracted verbatim from cache.py. cache.py re-exports these as ``cache.<fn>``.
"""

from __future__ import annotations

import cache


async def save_nps_holdings(date: str, items: list[dict]):
    db = await cache.get_db()
    await db.execute("DELETE FROM nps_holdings WHERE date = ?", (date,))
    await db.executemany(
        """INSERT OR REPLACE INTO nps_holdings
        (date, stock_code, stock_name, shares, ownership_pct, price, market_value, change_pct)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        [(date, it["stock_code"], it["stock_name"], it["shares"],
          it.get("ownership_pct", 0), it.get("price"), it.get("market_value"), it.get("change_pct"))
         for it in items],
    )
    await db.commit()


async def get_nps_holdings(date: str) -> list[dict]:
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT * FROM nps_holdings WHERE date = ? ORDER BY market_value DESC",
        (date,),
    )
    return [dict(r) for r in await cursor.fetchall()]


async def save_nps_snapshot(date: str, total_value: float, nav: float, count: int, html: str | None = None):
    db = await cache.get_db()
    await db.execute(
        """INSERT OR REPLACE INTO nps_snapshots (date, total_value, nav, total_count, generated_html)
           VALUES (?, ?, ?, ?, ?)""",
        (date, total_value, nav, count, html),
    )
    await db.commit()


async def get_nps_snapshots() -> list[dict]:
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT date, total_value, nav, total_count FROM nps_snapshots ORDER BY date ASC"
    )
    return [dict(r) for r in await cursor.fetchall()]


async def get_latest_nps_snapshot() -> dict | None:
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT date, total_value, nav, total_count FROM nps_snapshots ORDER BY date DESC LIMIT 1"
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def get_latest_nps_html() -> str | None:
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT generated_html FROM nps_snapshots WHERE generated_html IS NOT NULL ORDER BY date DESC LIMIT 1"
    )
    row = await cursor.fetchone()
    return row["generated_html"] if row else None
