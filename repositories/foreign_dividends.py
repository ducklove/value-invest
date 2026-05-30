"""Foreign-dividend overrides repository (yfinance-sourced + admin-manual).

Extracted verbatim from cache.py. cache.py re-exports these as ``cache.<fn>`` so
the foreign-dividend batch and admin routes are unchanged.
"""

from __future__ import annotations

from datetime import datetime

import cache


async def upsert_foreign_dividends_auto(rows: list[dict]) -> int:
    """Upsert yfinance-fetched rows. PRESERVES existing source='manual'
    rows — admin's explicit override must not be clobbered by the next
    refresh. SQLite's ON CONFLICT ... WHERE clause does the guard.

    rows: [{
        "stock_code": "AAPL",
        "dps_native": 0.96,
        "currency": "USD",
        "dps_krw": 1320.0,
    }, ...]
    """
    if not rows:
        return 0
    now = datetime.now().isoformat(timespec="seconds")
    db = await cache.get_db()
    written = 0
    for r in rows:
        code = (r.get("stock_code") or "").strip()
        if not code:
            continue
        await db.execute(
            """INSERT INTO foreign_dividends
               (stock_code, dps_native, currency, dps_krw, source, manual_note, fetched_at)
               VALUES (?, ?, ?, ?, 'yfinance', NULL, ?)
               ON CONFLICT(stock_code) DO UPDATE SET
                   dps_native = excluded.dps_native,
                   currency   = excluded.currency,
                   dps_krw    = excluded.dps_krw,
                   source     = 'yfinance',
                   fetched_at = excluded.fetched_at
               WHERE foreign_dividends.source != 'manual'""",
            (code, r.get("dps_native"), r.get("currency"), r.get("dps_krw"), now),
        )
        written += 1
    await db.commit()
    return written


async def upsert_foreign_dividend_manual(
    stock_code: str,
    dps_krw: float,
    note: str | None = None,
) -> None:
    """Admin-entered override. Unconditionally overwrites whatever's there
    (yfinance value or previous manual). Doesn't track native currency
    because the admin enters the KRW-equivalent directly — simpler UI."""
    code = (stock_code or "").strip()
    if not code:
        raise ValueError("stock_code is required")
    now = datetime.now().isoformat(timespec="seconds")
    db = await cache.get_db()
    await db.execute(
        """INSERT INTO foreign_dividends
           (stock_code, dps_native, currency, dps_krw, source, manual_note, fetched_at)
           VALUES (?, NULL, 'KRW', ?, 'manual', ?, ?)
           ON CONFLICT(stock_code) DO UPDATE SET
               dps_native  = NULL,
               currency    = 'KRW',
               dps_krw     = excluded.dps_krw,
               source      = 'manual',
               manual_note = excluded.manual_note,
               fetched_at  = excluded.fetched_at""",
        (code, float(dps_krw), note, now),
    )
    await db.commit()


async def delete_foreign_dividend(stock_code: str) -> bool:
    """Remove a row entirely — used both for "clear this override" (so
    the next auto refresh can repopulate) and for cleanup of stocks no
    longer in any portfolio. Returns True if a row was removed."""
    code = (stock_code or "").strip()
    if not code:
        return False
    db = await cache.get_db()
    cursor = await db.execute(
        "DELETE FROM foreign_dividends WHERE stock_code = ?",
        (code,),
    )
    await db.commit()
    return (cursor.rowcount or 0) > 0


async def list_foreign_dividends() -> list[dict]:
    """Return all foreign_dividends rows for the admin dashboard, with
    source surfaced so the UI can badge manual vs. auto. Sorted by
    source (manual first — those are the ones the admin is curating)
    then by stock_code."""
    db = await cache.get_db()
    cursor = await db.execute(
        """SELECT stock_code, dps_native, currency, dps_krw, source,
                  manual_note, fetched_at
           FROM foreign_dividends
           ORDER BY CASE source WHEN 'manual' THEN 0 ELSE 1 END, stock_code ASC"""
    )
    return [dict(row) for row in await cursor.fetchall()]


async def get_foreign_dividends_count() -> int:
    db = await cache.get_db()
    cursor = await db.execute("SELECT COUNT(*) AS n FROM foreign_dividends")
    row = await cursor.fetchone()
    return int(row["n"]) if row else 0


async def get_foreign_dividend(stock_code: str) -> dict | None:
    """Single-row lookup — used by the portfolio PUT dispatch to avoid
    firing a fresh yfinance call when we already have data (auto OR
    manual) for this code."""
    code = (stock_code or "").strip()
    if not code:
        return None
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT stock_code, dps_krw, source FROM foreign_dividends WHERE stock_code = ?",
        (code,),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None
