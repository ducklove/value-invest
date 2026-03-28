import aiosqlite
import json
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent / "cache.db"


async def get_db() -> aiosqlite.Connection:
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA foreign_keys=ON")
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
                dividend_per_share REAL,
                dividend_yield REAL,
                market_cap REAL,
                PRIMARY KEY (stock_code, year)
            );

            CREATE TABLE IF NOT EXISTS analysis_meta (
                stock_code TEXT PRIMARY KEY,
                corp_name TEXT NOT NULL,
                analyzed_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS users (
                google_sub TEXT PRIMARY KEY,
                email TEXT NOT NULL,
                name TEXT NOT NULL,
                picture TEXT,
                email_verified INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                last_login_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS user_sessions (
                session_token_hash TEXT PRIMARY KEY,
                google_sub TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS user_recent_analyses (
                google_sub TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                viewed_at TEXT NOT NULL,
                PRIMARY KEY (google_sub, stock_code),
                FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE,
                FOREIGN KEY (stock_code) REFERENCES analysis_meta(stock_code) ON DELETE CASCADE
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
            CREATE INDEX IF NOT EXISTS idx_user_sessions_google_sub ON user_sessions(google_sub);
            CREATE INDEX IF NOT EXISTS idx_user_recent_viewed_at ON user_recent_analyses(google_sub, viewed_at DESC);
        """)
        await _ensure_column(db, "corp_codes", "modify_date", "TEXT")
        await _ensure_column(db, "financial_data", "report_date", "TEXT")
        await _ensure_column(db, "market_data", "dividend_per_share", "REAL")
        await _ensure_column(db, "analysis_meta", "payload_json", "TEXT")
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
        cursor = await db.execute(
            "SELECT payload_json FROM analysis_meta WHERE stock_code = ?",
            (stock_code,),
        )
        row = await cursor.fetchone()
        await db.execute(
            "INSERT OR REPLACE INTO analysis_meta (stock_code, corp_name, analyzed_at, payload_json) VALUES (?, ?, ?, ?)",
            (
                stock_code,
                corp_name,
                datetime.now().isoformat(),
                row["payload_json"] if row else None,
            ),
        )
        await db.commit()
    finally:
        await db.close()


async def save_analysis_snapshot(stock_code: str, corp_name: str, payload: dict):
    db = await get_db()
    try:
        analyzed_at = payload.get("analyzed_at") or datetime.now().isoformat()
        snapshot = dict(payload)
        snapshot["analyzed_at"] = analyzed_at
        await db.execute(
            "INSERT OR REPLACE INTO analysis_meta (stock_code, corp_name, analyzed_at, payload_json) VALUES (?, ?, ?, ?)",
            (
                stock_code,
                corp_name,
                analyzed_at,
                json.dumps(snapshot, ensure_ascii=False),
            ),
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


async def get_analysis_snapshot(stock_code: str) -> dict | None:
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT stock_code, corp_name, analyzed_at, payload_json FROM analysis_meta WHERE stock_code = ?",
            (stock_code,),
        )
        row = await cursor.fetchone()
        if not row or not row["payload_json"]:
            return None
        try:
            payload = json.loads(row["payload_json"])
        except json.JSONDecodeError:
            return None

        if not isinstance(payload, dict):
            return None

        payload.setdefault("stock_code", row["stock_code"])
        payload.setdefault("corp_name", row["corp_name"])
        payload.setdefault("analyzed_at", row["analyzed_at"])
        payload["cached"] = True
        return payload
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


async def upsert_user(user: dict):
    db = await get_db()
    try:
        now = datetime.now().isoformat()
        await db.execute(
            """
            INSERT INTO users (google_sub, email, name, picture, email_verified, created_at, last_login_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(google_sub) DO UPDATE SET
                email = excluded.email,
                name = excluded.name,
                picture = excluded.picture,
                email_verified = excluded.email_verified,
                last_login_at = excluded.last_login_at
            """,
            (
                user["google_sub"],
                user["email"],
                user["name"],
                user.get("picture"),
                1 if user.get("email_verified") else 0,
                now,
                now,
            ),
        )
        await db.commit()
    finally:
        await db.close()


async def create_user_session(session_token_hash: str, google_sub: str, expires_at: str):
    db = await get_db()
    try:
        await db.execute(
            """
            INSERT OR REPLACE INTO user_sessions (session_token_hash, google_sub, created_at, expires_at)
            VALUES (?, ?, ?, ?)
            """,
            (session_token_hash, google_sub, datetime.now().isoformat(), expires_at),
        )
        await db.commit()
    finally:
        await db.close()


async def get_user_by_session(session_token_hash: str) -> dict | None:
    db = await get_db()
    try:
        cursor = await db.execute(
            """
            SELECT u.google_sub, u.email, u.name, u.picture, u.email_verified
            FROM user_sessions s
            JOIN users u ON u.google_sub = s.google_sub
            WHERE s.session_token_hash = ? AND s.expires_at > ?
            """,
            (session_token_hash, datetime.now().isoformat()),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None
    finally:
        await db.close()


async def delete_user_session(session_token_hash: str):
    db = await get_db()
    try:
        await db.execute(
            "DELETE FROM user_sessions WHERE session_token_hash = ?",
            (session_token_hash,),
        )
        await db.commit()
    finally:
        await db.close()


async def delete_expired_sessions():
    db = await get_db()
    try:
        await db.execute(
            "DELETE FROM user_sessions WHERE expires_at <= ?",
            (datetime.now().isoformat(),),
        )
        await db.commit()
    finally:
        await db.close()


async def touch_user_recent_analysis(google_sub: str, stock_code: str):
    db = await get_db()
    try:
        await db.execute(
            """
            INSERT OR REPLACE INTO user_recent_analyses (google_sub, stock_code, viewed_at)
            VALUES (?, ?, ?)
            """,
            (google_sub, stock_code, datetime.now().isoformat()),
        )
        await db.commit()
    finally:
        await db.close()


async def delete_user_recent_analysis(google_sub: str, stock_code: str):
    db = await get_db()
    try:
        await db.execute(
            "DELETE FROM user_recent_analyses WHERE google_sub = ? AND stock_code = ?",
            (google_sub, stock_code),
        )
        await db.commit()
    finally:
        await db.close()


async def get_cached_analyses(
    limit: int | None = None,
    include_quotes: bool = False,
    google_sub: str | None = None,
) -> list[dict]:
    db = await get_db()
    try:
        select_fields = "stock_code, corp_name, analyzed_at"
        if include_quotes:
            select_fields += ", payload_json"

        if google_sub:
            query = (
                f"SELECT a.stock_code, a.corp_name, r.viewed_at AS analyzed_at"
                + (", a.payload_json" if include_quotes else "")
                + " FROM user_recent_analyses r"
                + " JOIN analysis_meta a ON a.stock_code = r.stock_code"
                + " WHERE r.google_sub = ?"
                + " ORDER BY r.viewed_at DESC"
            )
            params: tuple = (google_sub,)
        else:
            query = f"SELECT {select_fields} FROM analysis_meta ORDER BY analyzed_at DESC"
            params = ()

        if limit is not None:
            query += " LIMIT ?"
            params = (*params, limit)

        cursor = await db.execute(query, params)
        rows = await cursor.fetchall()
        items = []
        for row in rows:
            item = dict(row)
            if include_quotes:
                payload_json = item.pop("payload_json", None)
                quote_snapshot = {}
                if payload_json:
                    try:
                        payload = json.loads(payload_json)
                    except json.JSONDecodeError:
                        payload = {}
                    if isinstance(payload, dict):
                        cached_quote = payload.get("quote_snapshot")
                        if isinstance(cached_quote, dict):
                            quote_snapshot = cached_quote
                item["quote_snapshot"] = quote_snapshot
            items.append(item)
        return items
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
