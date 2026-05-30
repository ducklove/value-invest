"""Per-stock financial data, market data and dividend years.

Extracted verbatim from cache.py; re-exported as ``cache.<fn>``.
"""

from __future__ import annotations

from datetime import datetime

import cache


async def save_financial_data(stock_code: str, data: list[dict]):
    db = await cache.get_db()
    await db.executemany(
        "INSERT OR REPLACE INTO financial_data "
        "(stock_code, year, report_date, revenue, operating_profit, net_income, total_assets, total_liabilities, total_equity) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                stock_code,
                d["year"],
                d.get("report_date"),
                d.get("revenue"),
                d.get("operating_profit"),
                d.get("net_income"),
                d.get("total_assets"),
                d.get("total_liabilities"),
                d.get("total_equity"),
            )
            for d in data
        ],
    )
    await db.commit()


async def get_financial_data(stock_code: str) -> list[dict]:
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT * FROM financial_data WHERE stock_code = ? ORDER BY year",
        (stock_code,),
    )
    rows = await cursor.fetchall()
    return [dict(r) for r in rows]


async def save_market_data(stock_code: str, data: list[dict]):
    db = await cache.get_db()
    await db.executemany(
        "INSERT OR REPLACE INTO market_data "
        "(stock_code, year, close_price, per, pbr, eps, bps, dividend_per_share, dividend_yield, market_cap) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                stock_code,
                d["year"],
                d.get("close_price"),
                d.get("per"),
                d.get("pbr"),
                d.get("eps"),
                d.get("bps"),
                d.get("dividend_per_share"),
                d.get("dividend_yield"),
                d.get("market_cap"),
            )
            for d in data
        ],
    )
    await db.commit()


async def upsert_market_dividends(stock_code: str, dividends_by_year: dict[int, float]) -> int:
    if not dividends_by_year:
        return 0
    db = await cache.get_db()
    rows = []
    for year, dps in dividends_by_year.items():
        if dps is None:
            continue
        try:
            rows.append((stock_code, int(year), float(dps)))
        except (TypeError, ValueError):
            continue
    if not rows:
        return 0

    await db.executemany(
        """INSERT INTO market_data (stock_code, year, dividend_per_share)
           VALUES (?, ?, ?)
           ON CONFLICT(stock_code, year) DO UPDATE SET
             dividend_per_share = excluded.dividend_per_share,
             dividend_yield = CASE
               WHEN market_data.close_price IS NOT NULL AND market_data.close_price != 0
                 THEN ROUND(excluded.dividend_per_share / market_data.close_price * 100, 2)
               ELSE market_data.dividend_yield
             END""",
        rows,
    )
    await db.commit()
    return len(rows)


async def get_market_data(stock_code: str) -> list[dict]:
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT * FROM market_data WHERE stock_code = ? ORDER BY year",
        (stock_code,),
    )
    rows = await cursor.fetchall()
    return [dict(r) for r in rows]


async def get_latest_dividend_years(stock_codes: list[str]) -> dict[str, int]:
    if not stock_codes:
        return {}
    current_year = datetime.now().year
    placeholders = ",".join("?" for _ in stock_codes)
    db = await cache.get_db()
    cursor = await db.execute(
        f"""SELECT stock_code, MAX(year) AS latest_year
            FROM market_data
            WHERE stock_code IN ({placeholders})
              AND dividend_per_share IS NOT NULL
              AND year < ?
            GROUP BY stock_code""",
        (*stock_codes, current_year),
    )
    return {
        row["stock_code"]: int(row["latest_year"])
        for row in await cursor.fetchall()
        if row["latest_year"] is not None
    }
