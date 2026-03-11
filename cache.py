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
                modify_date TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS financial_data (
                stock_code TEXT NOT NULL,
                year INTEGER NOT NULL,
                report_date TEXT,
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

            CREATE TABLE IF NOT EXISTS latest_report_cache (
                stock_code TEXT PRIMARY KEY,
                report_json TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS report_list_cache (
                stock_code TEXT PRIMARY KEY,
                reports_json TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_corp_name ON corp_codes(corp_name);
        """)
        await _ensure_column(db, "corp_codes", "modify_date", "TEXT")
        await _ensure_column(db, "financial_data", "report_date", "TEXT")
        await db.commit()
    finally:
        await db.close()


async def _ensure_column(db: aiosqlite.Connection, table: str, column: str, definition: str):
    cursor = await db.execute(f"PRAGMA table_info({table})")
    rows = await cursor.fetchall()
    columns = {row["name"] for row in rows}
    if column not in columns:
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


async def is_corp_codes_loaded() -> bool:
    db = await get_db()
    try:
        cursor = await db.execute("SELECT COUNT(*) FROM corp_codes")
        row = await cursor.fetchone()
        return row[0] > 0
    finally:
        await db.close()


async def corp_codes_need_refresh() -> bool:
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT COUNT(*) AS total, SUM(CASE WHEN modify_date IS NOT NULL AND modify_date != '' THEN 1 ELSE 0 END) AS filled FROM corp_codes"
        )
        row = await cursor.fetchone()
        total = row["total"] or 0
        filled = row["filled"] or 0
        return total > 0 and filled == 0
    finally:
        await db.close()


async def save_corp_codes(codes: list[dict]):
    db = await get_db()
    try:
        now = datetime.now().isoformat()
        await db.executemany(
            "INSERT OR REPLACE INTO corp_codes (stock_code, corp_code, corp_name, modify_date, updated_at) VALUES (?, ?, ?, ?, ?)",
            [(c["stock_code"], c["corp_code"], c["corp_name"], c.get("modify_date"), now) for c in codes],
        )
        await db.commit()
    finally:
        await db.close()


async def search_corp(query: str) -> list[dict]:
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT stock_code, corp_code, corp_name FROM corp_codes "
            "WHERE corp_name LIKE ? OR stock_code LIKE ? "
            "ORDER BY "
            "CASE "
            "WHEN stock_code = ? THEN 0 "
            "WHEN corp_name = ? THEN 1 "
            "WHEN corp_name LIKE ? THEN 2 "
            "ELSE 3 END, "
            "COALESCE(modify_date, '') DESC, "
            "LENGTH(corp_name), stock_code "
            "LIMIT 20",
            (f"%{query}%", f"%{query}%", query, query, f"{query}%"),
        )
        rows = await cursor.fetchall()
        results = []
        exact_name_seen = set()
        for row in rows:
            item = dict(row)
            if item["corp_name"] == query:
                if item["corp_name"] in exact_name_seen:
                    continue
                exact_name_seen.add(item["corp_name"])
            results.append(item)
        return results
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
        await db.execute("DELETE FROM latest_report_cache WHERE stock_code = ?", (stock_code,))
        await db.execute("DELETE FROM report_list_cache WHERE stock_code = ?", (stock_code,))
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


async def save_latest_report(stock_code: str, report: dict):
    db = await get_db()
    try:
        await db.execute(
            "INSERT OR REPLACE INTO latest_report_cache (stock_code, report_json, fetched_at) VALUES (?, ?, ?)",
            (stock_code, json.dumps(report, ensure_ascii=False), datetime.now().isoformat()),
        )
        await db.commit()
    finally:
        await db.close()


async def get_latest_report(stock_code: str, ttl_minutes: int | None = None) -> dict | None:
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT report_json, fetched_at FROM latest_report_cache WHERE stock_code = ?",
            (stock_code,),
        )
        row = await cursor.fetchone()
        if not row:
            return None

        fetched_at = datetime.fromisoformat(row["fetched_at"])
        if ttl_minutes is not None:
            age_seconds = (datetime.now() - fetched_at).total_seconds()
            if age_seconds > ttl_minutes * 60:
                return None

        report = json.loads(row["report_json"])
        report["_cached_at"] = row["fetched_at"]
        return report
    finally:
        await db.close()


async def save_report_list(stock_code: str, reports: list[dict]):
    db = await get_db()
    try:
        await db.execute(
            "INSERT OR REPLACE INTO report_list_cache (stock_code, reports_json, fetched_at) VALUES (?, ?, ?)",
            (stock_code, json.dumps(reports, ensure_ascii=False), datetime.now().isoformat()),
        )
        await db.commit()
    finally:
        await db.close()


async def get_report_list(stock_code: str, ttl_minutes: int | None = None) -> dict | None:
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT reports_json, fetched_at FROM report_list_cache WHERE stock_code = ?",
            (stock_code,),
        )
        row = await cursor.fetchone()
        if not row:
            return None

        fetched_at = datetime.fromisoformat(row["fetched_at"])
        if ttl_minutes is not None:
            age_seconds = (datetime.now() - fetched_at).total_seconds()
            if age_seconds > ttl_minutes * 60:
                return None

        return {
            "reports": json.loads(row["reports_json"]),
            "fetched_at": row["fetched_at"],
        }
    finally:
        await db.close()
