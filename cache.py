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

            CREATE TABLE IF NOT EXISTS user_stock_preferences (
                google_sub TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                is_starred INTEGER NOT NULL DEFAULT 0,
                is_pinned INTEGER NOT NULL DEFAULT 0,
                sort_order INTEGER,
                note TEXT NOT NULL DEFAULT '',
                updated_at TEXT NOT NULL,
                PRIMARY KEY (google_sub, stock_code),
                FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
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

            CREATE TABLE IF NOT EXISTS user_portfolio (
                google_sub TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                stock_name TEXT NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 0,
                avg_price REAL NOT NULL DEFAULT 0,
                sort_order INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (google_sub, stock_code),
                FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS portfolio_groups (
                google_sub TEXT NOT NULL,
                group_name TEXT NOT NULL,
                sort_order INTEGER DEFAULT 0,
                is_default INTEGER DEFAULT 0,
                PRIMARY KEY (google_sub, group_name),
                FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_corp_name ON corp_codes(corp_name);
            CREATE INDEX IF NOT EXISTS idx_user_sessions_google_sub ON user_sessions(google_sub);
            CREATE INDEX IF NOT EXISTS idx_user_recent_viewed_at ON user_recent_analyses(google_sub, viewed_at DESC);
            CREATE INDEX IF NOT EXISTS idx_user_stock_prefs_rank ON user_stock_preferences(google_sub, is_pinned DESC, is_starred DESC, updated_at DESC);
        """)
        await _ensure_column(db, "corp_codes", "modify_date", "TEXT")
        await _ensure_column(db, "financial_data", "report_date", "TEXT")
        await _ensure_column(db, "market_data", "dividend_per_share", "REAL")
        await _ensure_column(db, "analysis_meta", "payload_json", "TEXT")
        await _ensure_column(db, "user_stock_preferences", "sort_order", "INTEGER")
        await _ensure_column(db, "user_stock_preferences", "starred_order", "INTEGER")
        await _ensure_column(db, "user_portfolio", "currency", "TEXT DEFAULT 'KRW'")
        await _ensure_column(db, "user_portfolio", "group_name", "TEXT")
        await _ensure_column(db, "portfolio_groups", "default_type", "TEXT")
        # Backfill default_type for existing default groups by sort_order
        _type_by_order = {0: "kr", 1: "foreign", 2: "etc"}
        for order, dtype in _type_by_order.items():
            await db.execute(
                "UPDATE portfolio_groups SET default_type = ? WHERE is_default = 1 AND sort_order = ? AND default_type IS NULL",
                (dtype, order),
            )
        # If sort_order was changed, fill by name pattern as fallback
        await db.execute("UPDATE portfolio_groups SET default_type = 'kr' WHERE is_default = 1 AND default_type IS NULL AND group_name LIKE '%한국%'")
        await db.execute("UPDATE portfolio_groups SET default_type = 'foreign' WHERE is_default = 1 AND default_type IS NULL AND group_name LIKE '%해외%'")
        await db.execute("UPDATE portfolio_groups SET default_type = 'etc' WHERE is_default = 1 AND default_type IS NULL")
        # Migrate: ensure default groups exist for all users with portfolio items
        cursor = await db.execute("SELECT DISTINCT google_sub FROM user_portfolio")
        subs = [row["google_sub"] for row in await cursor.fetchall()]
        for sub in subs:
            await _ensure_default_groups(db, sub)
            await db.execute("""
                UPDATE user_portfolio SET group_name = '기타'
                WHERE google_sub = ? AND group_name IS NULL AND stock_code IN ('KRX_GOLD', 'CRYPTO_BTC', 'CRYPTO_ETH')
            """, (sub,))
            await db.execute("""
                UPDATE user_portfolio SET group_name = '한국주식'
                WHERE google_sub = ? AND group_name IS NULL AND length(stock_code) = 6 AND substr(stock_code, 1, 5) GLOB '[0-9][0-9][0-9][0-9][0-9]'
            """, (sub,))
            await db.execute("""
                UPDATE user_portfolio SET group_name = '해외주식'
                WHERE google_sub = ? AND group_name IS NULL
            """, (sub,))
        await db.commit()
    finally:
        await db.close()


async def _ensure_column(db: aiosqlite.Connection, table: str, column: str, definition: str):
    cursor = await db.execute(f"PRAGMA table_info({table})")
    rows = await cursor.fetchall()
    columns = {row["name"] for row in rows}
    if column not in columns:
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


_DEFAULT_GROUPS = [
    ("한국주식", 0, 1, "kr"),
    ("해외주식", 1, 1, "foreign"),
    ("기타", 2, 1, "etc"),
]

_SPECIAL_ASSETS_SET = {"KRX_GOLD", "CRYPTO_BTC", "CRYPTO_ETH"}


def _is_special_or_cash(code: str) -> bool:
    return code in _SPECIAL_ASSETS_SET or code.startswith("CASH_")


def _default_type_for_code(stock_code: str) -> str:
    """Return the default_type key (kr/foreign/etc) for a stock code."""
    if _is_special_or_cash(stock_code):
        return "etc"
    if len(stock_code) == 6 and stock_code[:5].isdigit():
        return "kr"
    return "foreign"


async def _resolve_default_group_name(db: aiosqlite.Connection, google_sub: str, stock_code: str) -> str:
    """Look up the actual current group name for a default group type, even if renamed."""
    dtype = _default_type_for_code(stock_code)
    cursor = await db.execute(
        "SELECT group_name FROM portfolio_groups WHERE google_sub = ? AND default_type = ?",
        (google_sub, dtype),
    )
    row = await cursor.fetchone()
    if row:
        return row["group_name"]
    # Fallback: original name
    for name, _, _, dt in _DEFAULT_GROUPS:
        if dt == dtype:
            return name
    return "기타"


async def _ensure_default_groups(db: aiosqlite.Connection, google_sub: str):
    cursor = await db.execute(
        "SELECT COUNT(*) AS cnt FROM portfolio_groups WHERE google_sub = ? AND is_default = 1",
        (google_sub,),
    )
    row = await cursor.fetchone()
    if row["cnt"] >= len(_DEFAULT_GROUPS):
        return
    for name, order, is_default, dtype in _DEFAULT_GROUPS:
        await db.execute(
            "INSERT OR IGNORE INTO portfolio_groups (google_sub, group_name, sort_order, is_default, default_type) VALUES (?, ?, ?, ?, ?)",
            (google_sub, name, order, is_default, dtype),
        )


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


USER_RECENT_MAX = 20


async def touch_user_recent_analysis(google_sub: str, stock_code: str):
    db = await get_db()
    try:
        now = datetime.now().isoformat()
        await db.execute(
            """
            INSERT OR REPLACE INTO user_recent_analyses (google_sub, stock_code, viewed_at)
            VALUES (?, ?, ?)
            """,
            (google_sub, stock_code, now),
        )
        await db.execute(
            """
            UPDATE user_stock_preferences
            SET sort_order = sort_order + 1
            WHERE google_sub = ? AND sort_order IS NOT NULL
            """,
            (google_sub,),
        )
        await db.execute(
            """
            INSERT INTO user_stock_preferences (google_sub, stock_code, is_starred, is_pinned, sort_order, note, updated_at)
            VALUES (?, ?, 0, 0, 0, '', ?)
            ON CONFLICT(google_sub, stock_code) DO UPDATE SET
                sort_order = 0,
                updated_at = excluded.updated_at
            """,
            (google_sub, stock_code, now),
        )
        # Remove overflow items beyond the limit (keep starred/pinned)
        cursor = await db.execute(
            """
            SELECT r.stock_code
            FROM user_recent_analyses r
            LEFT JOIN user_stock_preferences p
                ON p.google_sub = r.google_sub AND p.stock_code = r.stock_code
            WHERE r.google_sub = ?
              AND COALESCE(p.is_starred, 0) = 0
              AND COALESCE(p.is_pinned, 0) = 0
            ORDER BY COALESCE(p.sort_order, 999999) ASC
            LIMIT -1 OFFSET ?
            """,
            (google_sub, USER_RECENT_MAX),
        )
        overflow = [row["stock_code"] for row in await cursor.fetchall()]
        if overflow:
            placeholders = ",".join("?" for _ in overflow)
            await db.execute(
                f"DELETE FROM user_recent_analyses WHERE google_sub = ? AND stock_code IN ({placeholders})",
                (google_sub, *overflow),
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


async def get_user_stock_preference(google_sub: str, stock_code: str) -> dict:
    db = await get_db()
    try:
        cursor = await db.execute(
            """
            SELECT is_starred, is_pinned, sort_order, starred_order, note, updated_at
            FROM user_stock_preferences
            WHERE google_sub = ? AND stock_code = ?
            """,
            (google_sub, stock_code),
        )
        row = await cursor.fetchone()
        if not row:
            return {
                "is_starred": False,
                "is_pinned": False,
                "sort_order": None,
                "starred_order": None,
                "note": "",
                "updated_at": None,
            }
        return {
            "is_starred": bool(row["is_starred"]),
            "is_pinned": bool(row["is_pinned"]),
            "sort_order": row["sort_order"],
            "starred_order": row["starred_order"],
            "note": row["note"] or "",
            "updated_at": row["updated_at"],
        }
    finally:
        await db.close()


async def save_user_stock_preference(
    google_sub: str,
    stock_code: str,
    *,
    is_starred: bool | None = None,
    is_pinned: bool | None = None,
    note: str | None = None,
    sort_order: int | None = None,
) -> dict:
    current = await get_user_stock_preference(google_sub, stock_code)
    next_pref = {
        "is_starred": current["is_starred"] if is_starred is None else bool(is_starred),
        "is_pinned": current["is_pinned"] if is_pinned is None else bool(is_pinned),
        "sort_order": current["sort_order"] if sort_order is None else int(sort_order),
        "note": current["note"] if note is None else note.strip()[:2000],
    }

    db = await get_db()
    try:
        updated_at = datetime.now().isoformat()
        becoming_starred = next_pref["is_starred"] and not current["is_starred"]
        becoming_unstarred = not next_pref["is_starred"] and current["is_starred"]

        if becoming_starred:
            await db.execute(
                "UPDATE user_stock_preferences SET starred_order = starred_order + 1 WHERE google_sub = ? AND starred_order IS NOT NULL",
                (google_sub,),
            )

        await db.execute(
            """
            INSERT OR REPLACE INTO user_stock_preferences (
                google_sub, stock_code, is_starred, is_pinned, sort_order, starred_order, note, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                google_sub,
                stock_code,
                1 if next_pref["is_starred"] else 0,
                1 if next_pref["is_pinned"] else 0,
                next_pref["sort_order"],
                0 if becoming_starred else (None if becoming_unstarred else current.get("starred_order")),
                next_pref["note"],
                updated_at,
            ),
        )
        await db.commit()
    finally:
        await db.close()

    return {
        **next_pref,
        "updated_at": updated_at,
    }


async def get_cached_analyses(
    limit: int | None = None,
    include_quotes: bool = False,
    google_sub: str | None = None,
    tab: str = "recent",
) -> list[dict]:
    db = await get_db()
    try:
        select_fields = "stock_code, corp_name, analyzed_at"
        if include_quotes:
            select_fields += ", payload_json"

        if google_sub and tab == "starred":
            query = (
                "SELECT a.stock_code, a.corp_name, a.analyzed_at"
                + (", a.payload_json" if include_quotes else "")
                + ", 1 AS is_starred"
                + ", COALESCE(p.note, '') AS note"
                + " FROM user_stock_preferences p"
                + " JOIN analysis_meta a ON a.stock_code = p.stock_code"
                + " WHERE p.google_sub = ? AND p.is_starred = 1"
                + " ORDER BY CASE WHEN p.starred_order IS NULL THEN 1 ELSE 0 END, p.starred_order ASC, p.updated_at DESC"
            )
            params: tuple = (google_sub,)
        elif google_sub:
            query = (
                "SELECT a.stock_code, a.corp_name, r.viewed_at AS analyzed_at"
                + (", a.payload_json" if include_quotes else "")
                + ", COALESCE(p.is_starred, 0) AS is_starred"
                + ", COALESCE(p.note, '') AS note"
                + " FROM user_recent_analyses r"
                + " JOIN analysis_meta a ON a.stock_code = r.stock_code"
                + " LEFT JOIN user_stock_preferences p ON p.google_sub = r.google_sub AND p.stock_code = r.stock_code"
                + " WHERE r.google_sub = ?"
                + " ORDER BY CASE WHEN p.sort_order IS NULL THEN 1 ELSE 0 END, p.sort_order ASC, r.viewed_at DESC"
            )
            params = (google_sub,)
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
            item["is_starred"] = bool(item.get("is_starred"))
            item["note"] = item.get("note") or ""
            item.pop("sort_order", None)
            item.pop("is_pinned", None)
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


async def save_user_stock_order(google_sub: str, ordered_stock_codes: list[str]):
    db = await get_db()
    try:
        updated_at = datetime.now().isoformat()
        await db.executemany(
            """
            INSERT INTO user_stock_preferences (
                google_sub, stock_code, is_starred, is_pinned, sort_order, note, updated_at
            ) VALUES (?, ?, 0, 0, ?, '', ?)
            ON CONFLICT(google_sub, stock_code) DO UPDATE SET
                sort_order = excluded.sort_order,
                updated_at = excluded.updated_at
            """,
            [
                (google_sub, stock_code, index, updated_at)
                for index, stock_code in enumerate(ordered_stock_codes)
            ],
        )
        await db.commit()
    finally:
        await db.close()


async def save_starred_order(google_sub: str, ordered_stock_codes: list[str]):
    db = await get_db()
    try:
        updated_at = datetime.now().isoformat()
        await db.executemany(
            """
            UPDATE user_stock_preferences
            SET starred_order = ?, updated_at = ?
            WHERE google_sub = ? AND stock_code = ?
            """,
            [
                (index, updated_at, google_sub, stock_code)
                for index, stock_code in enumerate(ordered_stock_codes)
            ],
        )
        await db.commit()
    finally:
        await db.close()


async def unstar_stock(google_sub: str, stock_code: str):
    db = await get_db()
    try:
        await db.execute(
            """
            UPDATE user_stock_preferences
            SET is_starred = 0, starred_order = NULL, updated_at = ?
            WHERE google_sub = ? AND stock_code = ?
            """,
            (datetime.now().isoformat(), google_sub, stock_code),
        )
        await db.commit()
    finally:
        await db.close()


# --- Portfolio ---

async def resolve_stock_name(stock_code: str) -> str | None:
    name = await get_corp_name(stock_code)
    if name:
        return name
    return None


async def get_portfolio(google_sub: str) -> list[dict]:
    db = await get_db()
    try:
        cursor = await db.execute(
            """
            SELECT stock_code, stock_name, quantity, avg_price, sort_order,
                   COALESCE(currency, 'KRW') AS currency, group_name
            FROM user_portfolio
            WHERE google_sub = ?
            ORDER BY CASE WHEN sort_order IS NULL THEN 1 ELSE 0 END, sort_order ASC, created_at ASC
            """,
            (google_sub,),
        )
        return [dict(row) for row in await cursor.fetchall()]
    finally:
        await db.close()


async def save_portfolio_item(
    google_sub: str, stock_code: str, stock_name: str, quantity: float, avg_price: float,
    currency: str = "KRW", group_name: str | None = None,
) -> dict:
    db = await get_db()
    try:
        now = datetime.now().isoformat()
        cursor = await db.execute(
            "SELECT sort_order, group_name FROM user_portfolio WHERE google_sub = ? AND stock_code = ?",
            (google_sub, stock_code),
        )
        existing = await cursor.fetchone()
        sort_order = existing["sort_order"] if existing else None
        if group_name is None:
            if existing:
                group_name = existing["group_name"]
            else:
                group_name = await _resolve_default_group_name(db, google_sub, stock_code)

        if sort_order is None and not existing:
            cursor = await db.execute(
                "SELECT MIN(sort_order) AS mn FROM user_portfolio WHERE google_sub = ? AND sort_order IS NOT NULL",
                (google_sub,),
            )
            row = await cursor.fetchone()
            min_order = row["mn"] if row and row["mn"] is not None else 0
            sort_order = min_order - 1

        await db.execute(
            """
            INSERT INTO user_portfolio (google_sub, stock_code, stock_name, quantity, avg_price, sort_order, currency, group_name, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(google_sub, stock_code) DO UPDATE SET
                stock_name = excluded.stock_name,
                quantity = excluded.quantity,
                avg_price = excluded.avg_price,
                currency = excluded.currency,
                group_name = excluded.group_name,
                updated_at = excluded.updated_at
            """,
            (google_sub, stock_code, stock_name, quantity, avg_price, sort_order, currency, group_name, now, now),
        )
        await db.commit()
        return {"stock_code": stock_code, "stock_name": stock_name, "quantity": quantity, "avg_price": avg_price, "currency": currency, "group_name": group_name}
    finally:
        await db.close()


async def clear_portfolio(google_sub: str):
    db = await get_db()
    try:
        await db.execute("DELETE FROM user_portfolio WHERE google_sub = ?", (google_sub,))
        await db.commit()
    finally:
        await db.close()


async def delete_portfolio_item(google_sub: str, stock_code: str):
    db = await get_db()
    try:
        await db.execute(
            "DELETE FROM user_portfolio WHERE google_sub = ? AND stock_code = ?",
            (google_sub, stock_code),
        )
        await db.commit()
    finally:
        await db.close()


async def save_portfolio_order(google_sub: str, ordered_stock_codes: list[str]):
    db = await get_db()
    try:
        await db.executemany(
            "UPDATE user_portfolio SET sort_order = ?, updated_at = ? WHERE google_sub = ? AND stock_code = ?",
            [
                (index, datetime.now().isoformat(), google_sub, code)
                for index, code in enumerate(ordered_stock_codes)
            ],
        )
        await db.commit()
    finally:
        await db.close()


async def get_portfolio_groups(google_sub: str) -> list[dict]:
    db = await get_db()
    try:
        await _ensure_default_groups(db, google_sub)
        await db.commit()
        cursor = await db.execute(
            "SELECT group_name, sort_order, is_default FROM portfolio_groups WHERE google_sub = ? ORDER BY sort_order ASC",
            (google_sub,),
        )
        return [dict(row) for row in await cursor.fetchall()]
    finally:
        await db.close()


async def add_portfolio_group(google_sub: str, group_name: str) -> dict:
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT MAX(sort_order) AS mx FROM portfolio_groups WHERE google_sub = ?",
            (google_sub,),
        )
        row = await cursor.fetchone()
        next_order = (row["mx"] or 0) + 1
        await db.execute(
            "INSERT INTO portfolio_groups (google_sub, group_name, sort_order, is_default) VALUES (?, ?, ?, 0)",
            (google_sub, group_name, next_order),
        )
        await db.commit()
        return {"group_name": group_name, "sort_order": next_order, "is_default": 0}
    finally:
        await db.close()


async def rename_portfolio_group(google_sub: str, old_name: str, new_name: str):
    db = await get_db()
    try:
        await db.execute(
            "UPDATE portfolio_groups SET group_name = ? WHERE google_sub = ? AND group_name = ?",
            (new_name, google_sub, old_name),
        )
        await db.execute(
            "UPDATE user_portfolio SET group_name = ? WHERE google_sub = ? AND group_name = ?",
            (new_name, google_sub, old_name),
        )
        await db.commit()
    finally:
        await db.close()


async def delete_portfolio_group(google_sub: str, group_name: str):
    db = await get_db()
    try:
        cursor = await db.execute(
            "SELECT stock_code FROM user_portfolio WHERE google_sub = ? AND group_name = ?",
            (google_sub, group_name),
        )
        items = await cursor.fetchall()
        for item in items:
            default_grp = await _resolve_default_group_name(db, google_sub, item["stock_code"])
            await db.execute(
                "UPDATE user_portfolio SET group_name = ? WHERE google_sub = ? AND stock_code = ?",
                (default_grp, google_sub, item["stock_code"]),
            )
        await db.execute(
            "DELETE FROM portfolio_groups WHERE google_sub = ? AND group_name = ?",
            (google_sub, group_name),
        )
        await db.commit()
    finally:
        await db.close()


async def save_portfolio_groups_order(google_sub: str, group_names: list[str]):
    db = await get_db()
    try:
        await db.executemany(
            "UPDATE portfolio_groups SET sort_order = ? WHERE google_sub = ? AND group_name = ?",
            [(i, google_sub, name) for i, name in enumerate(group_names)],
        )
        await db.commit()
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
