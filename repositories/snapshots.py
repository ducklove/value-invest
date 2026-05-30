"""Portfolio NAV / snapshots / cashflows repository.

Daily + intraday snapshots, NAV/group-weight/constituent history, and the
cashflow transactions (incl. the atomic add/delete_cashflow_and_sync_cash that
keep CASH_KRW in step). Extracted verbatim from cache.py; cache.py re-exports
these as ``cache.<fn>``. The atomic paths open their own aiosqlite connection
against cache.DB_PATH; group/stock weight rebuilds go through the cache facade.
"""

from __future__ import annotations

from datetime import datetime

import aiosqlite

import cache


async def get_latest_snapshot(google_sub: str) -> dict | None:
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT date, total_value, total_invested, nav, total_units FROM portfolio_snapshots WHERE google_sub = ? ORDER BY date DESC LIMIT 1",
        (google_sub,),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def get_snapshot_by_date(google_sub: str, snap_date: str) -> dict | None:
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT date, total_value, total_invested, nav, total_units, fx_usdkrw FROM portfolio_snapshots WHERE google_sub = ? AND date = ?",
        (google_sub, snap_date),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def get_latest_snapshot_before_date(google_sub: str, snap_date: str) -> dict | None:
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT date, total_value, total_invested, nav, total_units, fx_usdkrw FROM portfolio_snapshots WHERE google_sub = ? AND date < ? ORDER BY date DESC LIMIT 1",
        (google_sub, snap_date),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def save_snapshot(google_sub: str, date: str, total_value: float, total_invested: float, nav: float, total_units: float, fx_usdkrw: float | None = None):
    db = await cache.get_db()
    await db.execute(
        """INSERT OR REPLACE INTO portfolio_snapshots (google_sub, date, total_value, total_invested, nav, total_units, fx_usdkrw)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (google_sub, date, total_value, total_invested, nav, total_units, fx_usdkrw),
    )
    await db.commit()


async def get_month_end_snapshot(google_sub: str) -> dict | None:
    """Get the portfolio snapshot at the end of the previous month."""
    from datetime import date, timedelta
    month_end = date.today().replace(day=1) - timedelta(days=1)
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT date, total_value, total_invested, nav, total_units, fx_usdkrw FROM portfolio_snapshots WHERE google_sub = ? AND date <= ? ORDER BY date DESC LIMIT 1",
        (google_sub, month_end.isoformat()),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def get_year_start_snapshot(google_sub: str) -> dict | None:
    """Get the last portfolio snapshot of the previous year (YTD base)."""
    from datetime import date
    year_end = date(date.today().year - 1, 12, 31).isoformat()
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT date, total_value, total_invested, nav, total_units, fx_usdkrw FROM portfolio_snapshots WHERE google_sub = ? AND date <= ? ORDER BY date DESC LIMIT 1",
        (google_sub, year_end),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def get_nav_history(google_sub: str) -> list[dict]:
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT date, nav, total_value, total_invested, total_units, fx_usdkrw FROM portfolio_snapshots WHERE google_sub = ? ORDER BY date ASC",
        (google_sub,),
    )
    return [dict(row) for row in await cursor.fetchall()]


async def get_group_weight_history(google_sub: str) -> list[dict]:
    """Return pre-aggregated per-date portfolio group weights."""
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT COUNT(*) AS n FROM portfolio_group_snapshots WHERE google_sub = ?",
        (google_sub,),
    )
    row = await cursor.fetchone()
    if not row or row["n"] == 0:
        await cache._refresh_group_snapshots(db, google_sub=google_sub)
        await db.commit()

    cursor = await db.execute(
        """
        SELECT date, group_name, market_value, stock_count, total_value, weight_pct
        FROM portfolio_group_snapshots
        WHERE google_sub = ?
        ORDER BY date ASC, market_value DESC
        """,
        (google_sub,),
    )
    return [dict(row) for row in await cursor.fetchall()]


async def get_group_constituent_history(google_sub: str, group_name: str) -> list[dict]:
    """Return pre-aggregated stock weights within one portfolio group."""
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT COUNT(*) AS n FROM portfolio_stock_weight_snapshots WHERE google_sub = ?",
        (google_sub,),
    )
    row = await cursor.fetchone()
    if not row or row["n"] == 0:
        await cache._refresh_stock_weight_snapshots(db, google_sub=google_sub)
        await db.commit()

    cursor = await db.execute(
        """
        SELECT
            date,
            stock_code,
            stock_name,
            market_value,
            group_value,
            total_value,
            group_weight_pct AS weight_pct,
            portfolio_weight_pct
        FROM portfolio_stock_weight_snapshots
        WHERE google_sub = ?
          AND group_name = ?
        ORDER BY date ASC, market_value DESC
        """,
        (google_sub, group_name),
    )
    return [dict(row) for row in await cursor.fetchall()]


async def get_cashflows(google_sub: str) -> list[dict]:
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT id, date, type, amount, nav_at_time, units_change, memo, created_at FROM portfolio_cashflows WHERE google_sub = ? ORDER BY date DESC, created_at DESC",
        (google_sub,),
    )
    return [dict(row) for row in await cursor.fetchall()]


class CashflowBalanceError(ValueError):
    def __init__(self, balance: float, amount: float):
        self.balance = balance
        self.amount = amount
        super().__init__(f"insufficient CASH_KRW balance: {balance} < {amount}")


async def add_cashflow(google_sub: str, date: str, cf_type: str, amount: float, memo: str | None, nav_at_time: float | None, units_change: float | None) -> dict:
    db = await cache.get_db()
    now = datetime.now().isoformat()
    cursor = await db.execute(
        "INSERT INTO portfolio_cashflows (google_sub, date, type, amount, nav_at_time, units_change, memo, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (google_sub, date, cf_type, amount, nav_at_time, units_change, memo, now),
    )
    await db.commit()
    return {"id": cursor.lastrowid, "date": date, "type": cf_type, "amount": amount, "nav_at_time": nav_at_time, "units_change": units_change, "memo": memo, "created_at": now}


async def add_cashflow_and_sync_cash(
    google_sub: str,
    date: str,
    cf_type: str,
    amount: float,
    memo: str | None,
    nav_at_time: float | None,
    units_change: float | None,
) -> dict:
    now = datetime.now().isoformat()
    async with aiosqlite.connect(cache.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA busy_timeout=5000")
        await db.execute("PRAGMA foreign_keys=ON")
        await db.execute("BEGIN IMMEDIATE")
        try:
            cash_cursor = await db.execute(
                "SELECT quantity, avg_price FROM user_portfolio WHERE google_sub = ? AND stock_code = 'CASH_KRW'",
                (google_sub,),
            )
            cash_item = await cash_cursor.fetchone()
            cash_balance = (cash_item["quantity"] * cash_item["avg_price"]) if cash_item else 0
            if cf_type == "withdrawal" and cash_balance < amount:
                raise CashflowBalanceError(cash_balance, amount)

            cursor = await db.execute(
                "INSERT INTO portfolio_cashflows (google_sub, date, type, amount, nav_at_time, units_change, memo, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (google_sub, date, cf_type, amount, nav_at_time, units_change, memo, now),
            )

            delta = int(amount) if cf_type == "deposit" else -int(amount)
            if cash_item:
                new_qty = max(0, int(cash_item["quantity"]) + delta)
                await db.execute(
                    "UPDATE user_portfolio SET quantity = ?, updated_at = ? WHERE google_sub = ? AND stock_code = 'CASH_KRW'",
                    (new_qty, now, google_sub),
                )
            elif cf_type == "deposit":
                await db.execute(
                    "INSERT INTO user_portfolio (google_sub, stock_code, stock_name, avg_price, quantity, currency, created_at, updated_at) VALUES (?, 'CASH_KRW', '원화', 1.0, ?, 'KRW', ?, ?)",
                    (google_sub, int(amount), now, now),
                )

            await db.commit()
            return {
                "id": cursor.lastrowid,
                "date": date,
                "type": cf_type,
                "amount": amount,
                "nav_at_time": nav_at_time,
                "units_change": units_change,
                "memo": memo,
                "created_at": now,
            }
        except Exception:
            await db.rollback()
            raise


async def delete_cashflow_and_sync_cash(google_sub: str, cf_id: int) -> bool:
    async with aiosqlite.connect(cache.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA busy_timeout=5000")
        await db.execute("PRAGMA foreign_keys=ON")
        await db.execute("BEGIN IMMEDIATE")
        try:
            cursor = await db.execute(
                "SELECT id, type, amount FROM portfolio_cashflows WHERE id = ? AND google_sub = ?",
                (cf_id, google_sub),
            )
            cf = await cursor.fetchone()
            if not cf:
                await db.commit()
                return False

            await db.execute("DELETE FROM portfolio_cashflows WHERE id = ? AND google_sub = ?", (cf_id, google_sub))
            cash_cursor = await db.execute(
                "SELECT quantity FROM user_portfolio WHERE google_sub = ? AND stock_code = 'CASH_KRW'",
                (google_sub,),
            )
            cash_item = await cash_cursor.fetchone()
            if cash_item:
                reverse_delta = -cf["amount"] if cf["type"] == "deposit" else cf["amount"]
                new_qty = max(0, int(cash_item["quantity"]) + int(reverse_delta))
                await db.execute(
                    "UPDATE user_portfolio SET quantity = ?, updated_at = ? WHERE google_sub = ? AND stock_code = 'CASH_KRW'",
                    (new_qty, datetime.now().isoformat(), google_sub),
                )

            await db.commit()
            return True
        except Exception:
            await db.rollback()
            raise

async def get_cashflow(google_sub: str, cf_id: int) -> dict | None:
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT id, type, amount FROM portfolio_cashflows WHERE id = ? AND google_sub = ?",
        (cf_id, google_sub),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def delete_cashflow(google_sub: str, cf_id: int):
    db = await cache.get_db()
    await db.execute("DELETE FROM portfolio_cashflows WHERE id = ? AND google_sub = ?", (cf_id, google_sub))
    await db.commit()

async def get_all_users_with_portfolio() -> list[str]:
    db = await cache.get_db()
    cursor = await db.execute("SELECT DISTINCT google_sub FROM user_portfolio")
    return [row["google_sub"] for row in await cursor.fetchall()]


async def get_pending_cashflows(google_sub: str, date: str) -> list[dict]:
    """Get cashflows for a specific date that haven't been applied to snapshots yet."""
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT id, type, amount, units_change FROM portfolio_cashflows WHERE google_sub = ? AND date = ?",
        (google_sub, date),
    )
    return [dict(row) for row in await cursor.fetchall()]


async def save_stock_snapshots(google_sub: str, date: str, items: list[dict]):
    """Save per-stock market values for a date. items: [{stock_code, market_value}, ...]

    The per-stock write and the two aggregate rebuilds (group + weight
    snapshots) must succeed or fail together: each rebuild DELETEs the day's
    rows before re-INSERTing, so a partial failure would otherwise leave the
    aggregate tables emptied. Run them in one explicit transaction on a
    dedicated connection and roll back on any error.
    """
    async with aiosqlite.connect(cache.DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA busy_timeout=5000")
        await db.execute("PRAGMA foreign_keys=ON")
        await db.execute("BEGIN IMMEDIATE")
        try:
            await db.executemany(
                """
                INSERT OR REPLACE INTO portfolio_stock_snapshots
                (google_sub, date, stock_code, market_value, group_name)
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (
                        google_sub,
                        date,
                        it["stock_code"],
                        it["market_value"],
                        it.get("group_name"),
                    )
                    for it in items
                ],
            )
            await cache._refresh_group_snapshots(db, google_sub=google_sub, snap_date=date)
            await cache._refresh_stock_weight_snapshots(db, google_sub=google_sub, snap_date=date)
            await db.commit()
        except Exception:
            await db.rollback()
            raise


async def get_stock_snapshots_by_date(google_sub: str, date: str) -> list[dict]:
    """Get per-stock snapshots on or before a given date (latest available)."""
    db = await cache.get_db()
    # Find the latest snapshot date on or before the target date
    cursor = await db.execute(
        "SELECT MAX(date) AS snap_date FROM portfolio_stock_snapshots WHERE google_sub = ? AND date <= ?",
        (google_sub, date),
    )
    row = await cursor.fetchone()
    snap_date = row["snap_date"] if row else None
    if not snap_date:
        return []
    cursor = await db.execute(
        "SELECT stock_code, market_value FROM portfolio_stock_snapshots WHERE google_sub = ? AND date = ?",
        (google_sub, snap_date),
    )
    return [dict(r) for r in await cursor.fetchall()]


async def get_stock_snapshots_before_date(google_sub: str, date: str) -> list[dict]:
    """Get per-stock snapshots strictly before a date (latest available)."""
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT MAX(date) AS snap_date FROM portfolio_stock_snapshots WHERE google_sub = ? AND date < ?",
        (google_sub, date),
    )
    row = await cursor.fetchone()
    snap_date = row["snap_date"] if row else None
    if not snap_date:
        return []
    cursor = await db.execute(
        "SELECT stock_code, market_value FROM portfolio_stock_snapshots WHERE google_sub = ? AND date = ?",
        (google_sub, snap_date),
    )
    return [dict(r) for r in await cursor.fetchall()]


async def save_intraday_snapshot(google_sub: str, ts: str, total_value: float):
    db = await cache.get_db()
    await db.execute(
        "INSERT OR REPLACE INTO portfolio_intraday (google_sub, ts, total_value) VALUES (?, ?, ?)",
        (google_sub, ts, total_value),
    )
    await db.commit()


async def get_intraday_snapshots(google_sub: str, date: str) -> list[dict]:
    """Get intraday snapshots for a given date (YYYY-MM-DD)."""
    return await get_intraday_snapshots_between(
        google_sub,
        date + "T00:00",
        date + "T99:99",
        include_start=True,
    )


async def get_intraday_snapshots_between(
    google_sub: str,
    start_ts: str,
    end_ts: str,
    *,
    include_start: bool = False,
) -> list[dict]:
    """Get intraday snapshots in a timestamp range.

    Timestamps are stored as local KST ISO minutes, so lexicographic range
    scans match chronological ordering.
    """
    db = await cache.get_db()
    start_op = ">=" if include_start else ">"
    cursor = await db.execute(
        f"SELECT ts, total_value FROM portfolio_intraday WHERE google_sub = ? AND ts {start_op} ? AND ts < ? ORDER BY ts ASC",
        (google_sub, start_ts, end_ts),
    )
    return [dict(r) for r in await cursor.fetchall()]


async def delete_old_intraday(days_to_keep: int = 7):
    """Remove intraday data older than N days."""
    from datetime import date, timedelta
    cutoff = (date.today() - timedelta(days=days_to_keep)).isoformat()
    db = await cache.get_db()
    await db.execute("DELETE FROM portfolio_intraday WHERE ts < ?", (cutoff + "T00:00",))
    await db.commit()
