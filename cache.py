import aiosqlite
import json
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent / "cache.db"


async def get_db() -> aiosqlite.Connection:
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    return db


async def init_db():
    db = await get_db()
    try:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS corp_codes (
                stock_code TEXT PRIMARY KEY,
                corp_code TEXT NOT NULL,
                corp_name TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS financial_data (
                stock_code TEXT NOT NULL,
                year INTEGER NOT NULL,
                revenue REAL,
                operating_profit REAL,
                net_income REAL,
                total_assets REAL,
                total_liabilities REAL,
                total_equity REAL,
                PRIMARY KEY (stock_code, year)
            );

            CREATE TABLE IF NOT EXISTS market_data (
                stock_code TEXT NOT NULL,
                year INTEGER NOT NULL,
                close_price REAL,
                per REAL,
                pbr REAL,
                eps REAL,
                bps REAL,
                dividend_yield REAL,
                market_cap REAL,
                PRIMARY KEY (stock_code, year)
            );

            CREATE TABLE IF NOT EXISTS analysis_meta (
                stock_code TEXT PRIMARY KEY,
                corp_name TEXT NOT NULL,
                analyzed_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_corp_name ON corp_codes(corp_name);
        """)
        await db.commit()
    finally:
        await db.close()


async def is_corp_codes_loaded() -> bool:
    db = await get_db()
    try:
        cursor = await db.execute("SELECT COUNT(*) FROM corp_codes")
        row = await cursor.fetchone()
        return row[0] > 0
    finally:
        await db.close()


async def save_corp_codes(codes: list[dict]):
    db = await get_db()
    try:
        now = datetime.now().isoformat()
        await db.executemany(
            "INSERT OR REPLACE INTO corp_codes (stock_code, corp_code, corp_name, updated_at) VALUES (?, ?, ?, ?)",
            [(c["stock_code"], c["corp_code"], c["corp_name"], now) for c in codes],
        )
        await db.commit()
    finally:
        await db.close()


async def search_corp(query: str) -> list[dict]:
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT stock_code, corp_code, corp_name FROM corp_codes "
            "WHERE corp_name LIKE ? OR stock_code LIKE ? LIMIT 20",
            (f"%{query}%", f"%{query}%"),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def get_corp_code(stock_code: str) -> str | None:
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT corp_code FROM corp_codes WHERE stock_code = ?", (stock_code,)
        )
        row = await cursor.fetchone()
        return row["corp_code"] if row else None
    finally:
        await db.close()


async def get_corp_name(stock_code: str) -> str | None:
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT corp_name FROM corp_codes WHERE stock_code = ?", (stock_code,)
        )
        row = await cursor.fetchone()
        return row["corp_name"] if row else None
    finally:
        await db.close()


async def save_financial_data(stock_code: str, data: list[dict]):
    db = await get_db()
    try:
        await db.executemany(
            "INSERT OR REPLACE INTO financial_data "
            "(stock_code, year, revenue, operating_profit, net_income, total_assets, total_liabilities, total_equity) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    stock_code,
                    d["year"],
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
    finally:
        await db.close()


async def get_financial_data(stock_code: str) -> list[dict]:
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT * FROM financial_data WHERE stock_code = ? ORDER BY year",
            (stock_code,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def save_market_data(stock_code: str, data: list[dict]):
    db = await get_db()
    try:
        await db.executemany(
            "INSERT OR REPLACE INTO market_data "
            "(stock_code, year, close_price, per, pbr, eps, bps, dividend_yield, market_cap) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    stock_code,
                    d["year"],
                    d.get("close_price"),
                    d.get("per"),
                    d.get("pbr"),
                    d.get("eps"),
                    d.get("bps"),
                    d.get("dividend_yield"),
                    d.get("market_cap"),
                )
                for d in data
            ],
        )
        await db.commit()
    finally:
        await db.close()


async def get_market_data(stock_code: str) -> list[dict]:
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT * FROM market_data WHERE stock_code = ? ORDER BY year",
            (stock_code,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()


async def save_analysis_meta(stock_code: str, corp_name: str):
    db = await get_db()
    try:
        await db.execute(
            "INSERT OR REPLACE INTO analysis_meta (stock_code, corp_name, analyzed_at) VALUES (?, ?, ?)",
            (stock_code, corp_name, datetime.now().isoformat()),
        )
        await db.commit()
    finally:
        await db.close()


async def get_analysis_meta(stock_code: str) -> dict | None:
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT * FROM analysis_meta WHERE stock_code = ?", (stock_code,)
        )
        row = await cursor.fetchone()
        return dict(row) if row else None
    finally:
        await db.close()


async def delete_analysis(stock_code: str):
    db = await get_db()
    try:
        await db.execute("DELETE FROM financial_data WHERE stock_code = ?", (stock_code,))
        await db.execute("DELETE FROM market_data WHERE stock_code = ?", (stock_code,))
        await db.execute("DELETE FROM analysis_meta WHERE stock_code = ?", (stock_code,))
        await db.commit()
    finally:
        await db.close()


async def get_cached_analyses() -> list[dict]:
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT stock_code, corp_name, analyzed_at FROM analysis_meta ORDER BY analyzed_at DESC"
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]
    finally:
        await db.close()
