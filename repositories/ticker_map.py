"""Ticker-map repository (foreign stock code → resolved yfinance ticker).

Extracted verbatim from cache.py. cache.py re-exports these as ``cache.<fn>``.
"""

from __future__ import annotations

from datetime import datetime

from repositories.db import get_db


async def load_ticker_map() -> dict[str, str]:
    db = await get_db()
    cursor = await db.execute("SELECT stock_code, resolved_ticker FROM ticker_map")
    return {r["stock_code"]: r["resolved_ticker"] for r in await cursor.fetchall()}


async def save_ticker(stock_code: str, resolved_ticker: str):
    db = await get_db()
    await db.execute(
        """INSERT INTO ticker_map (stock_code, resolved_ticker, updated_at)
           VALUES (?, ?, ?)
           ON CONFLICT(stock_code) DO UPDATE SET resolved_ticker = excluded.resolved_ticker, updated_at = excluded.updated_at""",
        (stock_code, resolved_ticker, datetime.now().isoformat()),
    )
    await db.commit()
