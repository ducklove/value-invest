"""Ticker-map repository (foreign stock code → resolved yfinance ticker).

Extracted verbatim from cache.py. cache.py re-exports these as ``cache.<fn>``.
"""

from __future__ import annotations

from datetime import datetime

from repositories.db import get_db, transaction


async def load_ticker_map() -> dict[str, str]:
    db = await get_db()
    cursor = await db.execute("SELECT stock_code, resolved_ticker FROM ticker_map")
    return {r["stock_code"]: r["resolved_ticker"] for r in await cursor.fetchall()}


async def save_ticker(stock_code: str, resolved_ticker: str):
    async with transaction() as db:
        await db.execute(
            """INSERT INTO ticker_map (stock_code, resolved_ticker, updated_at)
               VALUES (?, ?, ?)
               ON CONFLICT(stock_code) DO UPDATE SET resolved_ticker = excluded.resolved_ticker, updated_at = excluded.updated_at""",
            (stock_code, resolved_ticker, datetime.now().isoformat()),
        )


async def delete_ticker(stock_code: str, *, expected_ticker: str | None = None) -> bool:
    """매핑 한 줄을 지운다. ``expected_ticker`` 를 주면 저장값이 그 티커일 때만
    지워(그 사이 다른 경로가 새로 해석해 저장한 값은 건드리지 않는다). 지웠으면 True."""
    async with transaction() as db:
        if expected_ticker is None:
            cursor = await db.execute("DELETE FROM ticker_map WHERE stock_code = ?", (stock_code,))
        else:
            cursor = await db.execute(
                "DELETE FROM ticker_map WHERE stock_code = ? AND resolved_ticker = ?",
                (stock_code, expected_ticker),
            )
        return (cursor.rowcount or 0) > 0
