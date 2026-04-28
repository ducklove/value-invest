import aiosqlite
import json
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent / "cache.db"

_conn: aiosqlite.Connection | None = None


async def get_db() -> aiosqlite.Connection:
    global _conn
    if _conn is None:
        _conn = await aiosqlite.connect(DB_PATH)
        _conn.row_factory = aiosqlite.Row
        await _conn.execute("PRAGMA journal_mode=WAL")
        await _conn.execute("PRAGMA foreign_keys=ON")
    return _conn


async def close_db():
    """Shutdown: close the shared connection."""
    global _conn
    if _conn is not None:
        await _conn.close()
        _conn = None


async def init_db():
    db = await get_db()
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

        CREATE TABLE IF NOT EXISTS portfolio_snapshots (
            google_sub TEXT NOT NULL,
            date TEXT NOT NULL,
            total_value REAL NOT NULL DEFAULT 0,
            total_invested REAL NOT NULL DEFAULT 0,
            nav REAL NOT NULL DEFAULT 1000,
            total_units REAL NOT NULL DEFAULT 0,
            PRIMARY KEY (google_sub, date),
            FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS portfolio_cashflows (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            google_sub TEXT NOT NULL,
            date TEXT NOT NULL,
            type TEXT NOT NULL,
            amount REAL NOT NULL,
            nav_at_time REAL,
            units_change REAL,
            memo TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS portfolio_stock_snapshots (
            google_sub TEXT NOT NULL,
            date TEXT NOT NULL,
            stock_code TEXT NOT NULL,
            market_value REAL NOT NULL DEFAULT 0,
            PRIMARY KEY (google_sub, date, stock_code),
            FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_stock_snapshots_sub_date ON portfolio_stock_snapshots(google_sub, date);

        CREATE TABLE IF NOT EXISTS portfolio_intraday (
            google_sub TEXT NOT NULL,
            ts TEXT NOT NULL,
            total_value REAL NOT NULL DEFAULT 0,
            PRIMARY KEY (google_sub, ts),
            FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_intraday_sub_ts ON portfolio_intraday(google_sub, ts);

        CREATE TABLE IF NOT EXISTS nps_holdings (
            date TEXT NOT NULL,
            stock_code TEXT NOT NULL,
            stock_name TEXT NOT NULL,
            shares INTEGER NOT NULL,
            ownership_pct REAL NOT NULL DEFAULT 0,
            price REAL,
            market_value REAL,
            change_pct REAL,
            PRIMARY KEY (date, stock_code)
        );

        CREATE TABLE IF NOT EXISTS nps_snapshots (
            date TEXT NOT NULL PRIMARY KEY,
            total_value REAL NOT NULL DEFAULT 0,
            nav REAL NOT NULL DEFAULT 1000,
            total_count INTEGER NOT NULL DEFAULT 0,
            generated_html TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_nps_holdings_date ON nps_holdings(date);

        CREATE TABLE IF NOT EXISTS user_settings (
            google_sub TEXT NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (google_sub, key),
            FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_portfolio_snapshots_sub_date ON portfolio_snapshots(google_sub, date);
        CREATE INDEX IF NOT EXISTS idx_portfolio_cashflows_sub ON portfolio_cashflows(google_sub, date);

        CREATE TABLE IF NOT EXISTS ticker_map (
            stock_code TEXT PRIMARY KEY,
            resolved_ticker TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_corp_name ON corp_codes(corp_name);
        CREATE INDEX IF NOT EXISTS idx_user_sessions_google_sub ON user_sessions(google_sub);
        CREATE INDEX IF NOT EXISTS idx_user_recent_viewed_at ON user_recent_analyses(google_sub, viewed_at DESC);
        CREATE INDEX IF NOT EXISTS idx_user_stock_prefs_rank ON user_stock_preferences(google_sub, is_pinned DESC, is_starred DESC, updated_at DESC);

        -- Wiki / research-report pipeline.
        -- PDFs are downloaded and parsed in the background. Keyed by content
        -- hash so two URLs pointing at the same file share one row.
        CREATE TABLE IF NOT EXISTS report_pdf_cache (
            pdf_sha1 TEXT PRIMARY KEY,
            stock_code TEXT NOT NULL,
            pdf_url TEXT NOT NULL,
            file_path TEXT,
            file_bytes INTEGER,
            parsed_text TEXT,
            parse_status TEXT NOT NULL,
            parse_error TEXT,
            downloaded_at TEXT,
            parsed_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_pdf_cache_stock ON report_pdf_cache(stock_code);
        CREATE INDEX IF NOT EXISTS idx_pdf_cache_status ON report_pdf_cache(parse_status);

        -- One LLM-generated wiki entry per summarized report.
        CREATE TABLE IF NOT EXISTS stock_wiki_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_ref TEXT NOT NULL,
            report_date TEXT,
            firm TEXT,
            title TEXT,
            recommendation TEXT,
            target_price REAL,
            summary_md TEXT NOT NULL,
            key_points_md TEXT,
            model TEXT,
            tokens_in INTEGER,
            tokens_out INTEGER,
            created_at TEXT NOT NULL,
            UNIQUE(stock_code, source_type, source_ref)
        );
        CREATE INDEX IF NOT EXISTS idx_wiki_stock_date ON stock_wiki_entries(stock_code, report_date DESC);

        -- AI-generated reviews of the latest DART periodic filing. One row
        -- is kept per stock/report so expensive report reads are reusable
        -- across users until the next quarterly/half/annual report arrives.
        CREATE TABLE IF NOT EXISTS dart_report_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code TEXT NOT NULL,
            corp_code TEXT,
            corp_name TEXT,
            rcept_no TEXT NOT NULL,
            report_name TEXT NOT NULL,
            report_date TEXT,
            primary_doc_chars INTEGER NOT NULL DEFAULT 0,
            comparison_reports_json TEXT NOT NULL DEFAULT '[]',
            review_json TEXT NOT NULL,
            review_md TEXT NOT NULL,
            model TEXT,
            tokens_in INTEGER,
            tokens_out INTEGER,
            cost_usd REAL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(stock_code, rcept_no)
        );
        CREATE INDEX IF NOT EXISTS idx_dart_report_reviews_stock
            ON dart_report_reviews(stock_code, report_date DESC);

        -- FTS5 over wiki entries for retrieval. contentless=yes would work
        -- too but keeping it synced to the content table gives us UPDATE
        -- support. Triggers below sync INSERT/UPDATE/DELETE.
        CREATE VIRTUAL TABLE IF NOT EXISTS stock_wiki_fts USING fts5(
            stock_code UNINDEXED,
            title,
            summary_md,
            key_points_md,
            content='stock_wiki_entries',
            content_rowid='id'
        );
        CREATE TRIGGER IF NOT EXISTS stock_wiki_fts_insert
            AFTER INSERT ON stock_wiki_entries BEGIN
            INSERT INTO stock_wiki_fts(rowid, stock_code, title, summary_md, key_points_md)
            VALUES (new.id, new.stock_code, new.title, new.summary_md, new.key_points_md);
        END;
        CREATE TRIGGER IF NOT EXISTS stock_wiki_fts_delete
            AFTER DELETE ON stock_wiki_entries BEGIN
            INSERT INTO stock_wiki_fts(stock_wiki_fts, rowid, stock_code, title, summary_md, key_points_md)
            VALUES ('delete', old.id, old.stock_code, old.title, old.summary_md, old.key_points_md);
        END;
        CREATE TRIGGER IF NOT EXISTS stock_wiki_fts_update
            AFTER UPDATE ON stock_wiki_entries BEGIN
            INSERT INTO stock_wiki_fts(stock_wiki_fts, rowid, stock_code, title, summary_md, key_points_md)
            VALUES ('delete', old.id, old.stock_code, old.title, old.summary_md, old.key_points_md);
            INSERT INTO stock_wiki_fts(rowid, stock_code, title, summary_md, key_points_md)
            VALUES (new.id, new.stock_code, new.title, new.summary_md, new.key_points_md);
        END;

        -- 해외 종목 + 수동 override 배당. 한국 market_data 파이프라인은
        -- 해외 티커를 커버하지 않으므로 yfinance 의 trailingAnnualDividendRate
        -- 를 긁어와 KRW 환산 후 저장. source='manual' 인 row 는 관리자가
        -- 직접 입력한 값이라 auto refresh 가 덮어쓰지 않는다 (ON CONFLICT
        -- ... WHERE source != 'manual' 로 강제).
        CREATE TABLE IF NOT EXISTS foreign_dividends (
            stock_code   TEXT PRIMARY KEY,
            dps_native   REAL,
            currency     TEXT,
            dps_krw      REAL,
            source       TEXT NOT NULL,
            manual_note  TEXT,
            fetched_at   TEXT NOT NULL
        );

        -- 우선주 배당금 — 공유 Google Sheet 의 Data!AI 컬럼 (연간 주당
        -- 배당 원화). market_data 파이프라인은 보통주 코드만 인덱싱하므로
        -- 우선주는 기존엔 보통주 값으로 근사할 수밖에 없었는데, 실제
        -- 우선주 배당은 보통주 + 프리미엄이라 정확한 값이 시트에 들어있음.
        -- preferred_dividends.py 가 주기적으로 fetch 해서 upsert.
        CREATE TABLE IF NOT EXISTS preferred_dividends (
            stock_code          TEXT PRIMARY KEY,
            dividend_per_share  REAL,
            source_name         TEXT,
            common_code         TEXT,
            sheet_year          INTEGER,
            fetched_at          TEXT NOT NULL
        );

        -- Structured event log for in-app observability. Each subsystem
        -- writes short status rows here (snapshot ticks, wiki ingest
        -- results, external API failures, LLM calls, etc.) so the admin
        -- dashboard can show what actually happened without parsing
        -- systemd journal. Capped by TTL + row-count to stay tiny.
        CREATE TABLE IF NOT EXISTS system_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ts          TEXT NOT NULL,
            level       TEXT NOT NULL,        -- 'info' | 'warning' | 'error'
            source      TEXT NOT NULL,        -- 'snapshot_nav' | 'wiki_ingestion' | 'openrouter' | ...
            kind        TEXT NOT NULL,        -- 'tick_ok' | 'api_failure' | 'ingest_summary' | ...
            stock_code  TEXT,                 -- nullable — only for stock-scoped events
            details     TEXT                  -- JSON blob, schema per (source, kind)
        );
        CREATE INDEX IF NOT EXISTS idx_system_events_ts ON system_events(ts DESC);
        CREATE INDEX IF NOT EXISTS idx_system_events_source_ts ON system_events(source, ts DESC);
        CREATE INDEX IF NOT EXISTS idx_system_events_level_ts ON system_events(level, ts DESC);

        -- Runtime admin settings. Secret values are still stored locally, but
        -- API responses must only expose masked metadata. This lets the admin
        -- rotate OpenRouter keys and model profiles without redeploying.
        CREATE TABLE IF NOT EXISTS app_settings (
            key         TEXT PRIMARY KEY,
            value       TEXT NOT NULL,
            is_secret   INTEGER NOT NULL DEFAULT 0,
            updated_by  TEXT,
            updated_at  TEXT NOT NULL
        );

        -- AI usage ledger for cost and latency visibility by feature/model.
        CREATE TABLE IF NOT EXISTS ai_usage_events (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            ts            TEXT NOT NULL,
            google_sub    TEXT,
            feature       TEXT NOT NULL,
            model         TEXT NOT NULL,
            model_profile TEXT,
            input_tokens  INTEGER NOT NULL DEFAULT 0,
            output_tokens INTEGER NOT NULL DEFAULT 0,
            cost_usd      REAL NOT NULL DEFAULT 0,
            latency_ms    INTEGER,
            ok            INTEGER NOT NULL DEFAULT 1,
            error         TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_ai_usage_ts ON ai_usage_events(ts DESC);
        CREATE INDEX IF NOT EXISTS idx_ai_usage_feature_ts ON ai_usage_events(feature, ts DESC);

        -- Daily closing prices for market benchmarks we overlay on the NAV
        -- chart (KOSPI / SP500 / GOLD / ...). Keyed by (code, date) so a
        -- re-download is a no-op upsert. Populated by benchmark_history —
        -- lazy backfill on first query + nightly increment from snapshot_nav.
        CREATE TABLE IF NOT EXISTS benchmark_daily (
            code        TEXT NOT NULL,
            date        TEXT NOT NULL,
            close       REAL NOT NULL,
            source      TEXT NOT NULL,
            fetched_at  TEXT NOT NULL,
            PRIMARY KEY (code, date)
        );
        CREATE INDEX IF NOT EXISTS idx_benchmark_daily_code_date
            ON benchmark_daily(code, date);

        -- Q&A history: audit log + per-user rate limit source.
        CREATE TABLE IF NOT EXISTS stock_qa_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            google_sub TEXT NOT NULL,
            stock_code TEXT NOT NULL,
            question TEXT NOT NULL,
            answer_md TEXT,
            source_ids TEXT,
            model TEXT,
            tokens_in INTEGER,
            tokens_out INTEGER,
            cost_usd REAL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_qa_user_time ON stock_qa_history(google_sub, created_at DESC);

        -- Research insight board: a lightweight journal for backtest results,
        -- strategy experiments, and the author's interpretation. It is not
        -- tied to the built-in backtest menu; external/manual results can be
        -- pasted in as structured summary + optional raw payload.
        CREATE TABLE IF NOT EXISTS insight_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            google_sub TEXT NOT NULL,
            title TEXT NOT NULL,
            insight_md TEXT NOT NULL,
            source_type TEXT NOT NULL DEFAULT 'manual',
            result_summary_json TEXT NOT NULL DEFAULT '{}',
            result_payload_json TEXT,
            tags_json TEXT NOT NULL DEFAULT '[]',
            visibility TEXT NOT NULL DEFAULT 'public',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_insight_posts_created ON insight_posts(created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_insight_posts_user_created ON insight_posts(google_sub, created_at DESC);
    """)
    await _ensure_column(db, "corp_codes", "modify_date", "TEXT")
    await _ensure_column(db, "financial_data", "report_date", "TEXT")
    await _ensure_column(db, "market_data", "dividend_per_share", "REAL")
    await _ensure_column(db, "analysis_meta", "payload_json", "TEXT")
    await _ensure_column(db, "user_stock_preferences", "sort_order", "INTEGER")
    await _ensure_column(db, "user_stock_preferences", "starred_order", "INTEGER")
    await _ensure_column(db, "user_portfolio", "currency", "TEXT DEFAULT 'KRW'")
    await _ensure_column(db, "user_portfolio", "group_name", "TEXT")
    await _ensure_column(db, "user_portfolio", "benchmark_code", "TEXT")
    # 목표가 (수동 override). NULL + target_price_disabled=0 → 자동 계산
    # (우선주 → 본주, 지주사 → NAV per share, 그 외 → avg_price × 1.3).
    # 숫자 → 그 값이 고정 override. target_price_disabled=1 → '-' 로
    # 표시, 자동 계산도 bypass (사용자가 × 버튼으로 명시 비움).
    await _ensure_column(db, "user_portfolio", "target_price", "REAL")
    await _ensure_column(db, "user_portfolio", "target_price_disabled", "INTEGER NOT NULL DEFAULT 0")
    await _ensure_column(db, "users", "is_admin", "INTEGER NOT NULL DEFAULT 0")
    await _ensure_column(db, "portfolio_snapshots", "fx_usdkrw", "REAL")
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
    cursor = await db.execute("SELECT COUNT(*) FROM corp_codes")
    row = await cursor.fetchone()
    return row[0] > 0


async def corp_codes_need_refresh() -> bool:
    db = await get_db()
    cursor = await db.execute(
        "SELECT COUNT(*) AS total, SUM(CASE WHEN modify_date IS NOT NULL AND modify_date != '' THEN 1 ELSE 0 END) AS filled FROM corp_codes"
    )
    row = await cursor.fetchone()
    total = row["total"] or 0
    filled = row["filled"] or 0
    return total > 0 and filled == 0


async def save_corp_codes(codes: list[dict]):
    db = await get_db()
    now = datetime.now().isoformat()
    await db.executemany(
        "INSERT OR REPLACE INTO corp_codes (stock_code, corp_code, corp_name, modify_date, updated_at) VALUES (?, ?, ?, ?, ?)",
        [(c["stock_code"], c["corp_code"], c["corp_name"], c.get("modify_date"), now) for c in codes],
    )
    await db.commit()


async def search_corp(query: str) -> list[dict]:
    db = await get_db()
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


async def get_corp_code(stock_code: str) -> str | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT corp_code FROM corp_codes WHERE stock_code = ?", (stock_code,)
    )
    row = await cursor.fetchone()
    return row["corp_code"] if row else None


async def get_corp_name(stock_code: str) -> str | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT corp_name FROM corp_codes WHERE stock_code = ?", (stock_code,)
    )
    row = await cursor.fetchone()
    return row["corp_name"] if row else None


async def save_financial_data(stock_code: str, data: list[dict]):
    db = await get_db()
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
    db = await get_db()
    cursor = await db.execute(
        "SELECT * FROM financial_data WHERE stock_code = ? ORDER BY year",
        (stock_code,),
    )
    rows = await cursor.fetchall()
    return [dict(r) for r in rows]


async def save_market_data(stock_code: str, data: list[dict]):
    db = await get_db()
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


async def get_market_data(stock_code: str) -> list[dict]:
    db = await get_db()
    cursor = await db.execute(
        "SELECT * FROM market_data WHERE stock_code = ? ORDER BY year",
        (stock_code,),
    )
    rows = await cursor.fetchall()
    return [dict(r) for r in rows]


async def save_analysis_meta(stock_code: str, corp_name: str):
    db = await get_db()
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


async def save_analysis_snapshot(stock_code: str, corp_name: str, payload: dict):
    db = await get_db()
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


async def get_analysis_meta(stock_code: str) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT * FROM analysis_meta WHERE stock_code = ?", (stock_code,)
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def get_analysis_snapshot(stock_code: str) -> dict | None:
    db = await get_db()
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


async def delete_analysis(stock_code: str):
    db = await get_db()
    await db.execute("DELETE FROM financial_data WHERE stock_code = ?", (stock_code,))
    await db.execute("DELETE FROM market_data WHERE stock_code = ?", (stock_code,))
    await db.execute("DELETE FROM analysis_meta WHERE stock_code = ?", (stock_code,))
    await db.execute("DELETE FROM latest_report_cache WHERE stock_code = ?", (stock_code,))
    await db.execute("DELETE FROM report_list_cache WHERE stock_code = ?", (stock_code,))
    await db.execute("DELETE FROM dart_report_reviews WHERE stock_code = ?", (stock_code,))
    await db.commit()


async def upsert_user(user: dict):
    db = await get_db()
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


async def create_user_session(session_token_hash: str, google_sub: str, expires_at: str):
    db = await get_db()
    await db.execute(
        """
        INSERT OR REPLACE INTO user_sessions (session_token_hash, google_sub, created_at, expires_at)
        VALUES (?, ?, ?, ?)
        """,
        (session_token_hash, google_sub, datetime.now().isoformat(), expires_at),
    )
    await db.commit()


async def get_user_by_session(session_token_hash: str) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        """
        SELECT u.google_sub, u.email, u.name, u.picture, u.email_verified, u.is_admin
        FROM user_sessions s
        JOIN users u ON u.google_sub = s.google_sub
        WHERE s.session_token_hash = ? AND s.expires_at > ?
        """,
        (session_token_hash, datetime.now().isoformat()),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def delete_user_session(session_token_hash: str):
    db = await get_db()
    await db.execute(
        "DELETE FROM user_sessions WHERE session_token_hash = ?",
        (session_token_hash,),
    )
    await db.commit()


async def delete_expired_sessions():
    db = await get_db()
    await db.execute(
        "DELETE FROM user_sessions WHERE expires_at <= ?",
        (datetime.now().isoformat(),),
    )
    await db.commit()


async def get_all_users() -> list[dict]:
    db = await get_db()
    cursor = await db.execute(
        "SELECT google_sub, email, name, picture, is_admin, created_at, last_login_at FROM users ORDER BY last_login_at DESC"
    )
    return [dict(row) for row in await cursor.fetchall()]


async def get_db_stats() -> dict:
    db = await get_db()
    tables = {}
    cursor = await db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    for row in await cursor.fetchall():
        tname = row["name"]
        cnt = await db.execute(f"SELECT COUNT(*) as c FROM [{tname}]")
        tables[tname] = (await cnt.fetchone())["c"]
    # DB file size
    import os
    db_size = os.path.getsize(DB_PATH) if DB_PATH.exists() else 0
    return {"tables": tables, "db_size_bytes": db_size}


async def get_app_setting(key: str) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT key, value, is_secret, updated_by, updated_at FROM app_settings WHERE key = ?",
        (key,),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def set_app_setting(key: str, value: str, *, is_secret: bool = False, updated_by: str | None = None):
    db = await get_db()
    await db.execute(
        """
        INSERT INTO app_settings (key, value, is_secret, updated_by, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
            value = excluded.value,
            is_secret = excluded.is_secret,
            updated_by = excluded.updated_by,
            updated_at = excluded.updated_at
        """,
        (key, value, 1 if is_secret else 0, updated_by, datetime.now().isoformat()),
    )
    await db.commit()


async def delete_app_setting(key: str):
    db = await get_db()
    await db.execute("DELETE FROM app_settings WHERE key = ?", (key,))
    await db.commit()


async def insert_ai_usage_event(
    *,
    google_sub: str | None,
    feature: str,
    model: str,
    model_profile: str | None,
    input_tokens: int = 0,
    output_tokens: int = 0,
    cost_usd: float = 0.0,
    latency_ms: int | None = None,
    ok: bool = True,
    error: str | None = None,
):
    db = await get_db()
    await db.execute(
        """
        INSERT INTO ai_usage_events
            (ts, google_sub, feature, model, model_profile, input_tokens, output_tokens, cost_usd, latency_ms, ok, error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            datetime.now().isoformat(),
            google_sub,
            feature,
            model,
            model_profile,
            int(input_tokens or 0),
            int(output_tokens or 0),
            float(cost_usd or 0),
            latency_ms,
            1 if ok else 0,
            (error or "")[:500] if error else None,
        ),
    )
    await db.commit()


async def summarize_ai_usage(days: int = 30) -> dict:
    db = await get_db()
    cursor = await db.execute(
        """
        SELECT feature, model, model_profile,
               COUNT(*) AS calls,
               SUM(input_tokens) AS input_tokens,
               SUM(output_tokens) AS output_tokens,
               SUM(cost_usd) AS cost_usd,
               SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END) AS errors,
               AVG(latency_ms) AS avg_latency_ms,
               MAX(ts) AS latest_at
        FROM ai_usage_events
        WHERE ts >= datetime('now', ?)
        GROUP BY feature, model, model_profile
        ORDER BY cost_usd DESC, calls DESC
        """,
        (f"-{int(days)} days",),
    )
    by_feature = [dict(row) for row in await cursor.fetchall()]
    cursor = await db.execute(
        """
        SELECT ts, feature, model, model_profile, input_tokens, output_tokens, cost_usd, latency_ms, ok, error
        FROM ai_usage_events
        ORDER BY ts DESC, id DESC
        LIMIT 50
        """,
    )
    recent = [dict(row) for row in await cursor.fetchall()]
    return {"days": days, "by_feature": by_feature, "recent": recent}


async def create_insight_post(
    *,
    google_sub: str,
    title: str,
    insight_md: str,
    source_type: str = "manual",
    result_summary: dict | None = None,
    result_payload: dict | list | None = None,
    tags: list[str] | None = None,
    visibility: str = "public",
) -> dict:
    db = await get_db()
    now = datetime.now().isoformat()
    await db.execute(
        """
        INSERT INTO insight_posts
            (google_sub, title, insight_md, source_type, result_summary_json, result_payload_json, tags_json, visibility, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            google_sub,
            title,
            insight_md,
            source_type,
            json.dumps(result_summary or {}, ensure_ascii=False),
            json.dumps(result_payload, ensure_ascii=False) if result_payload is not None else None,
            json.dumps(tags or [], ensure_ascii=False),
            visibility,
            now,
            now,
        ),
    )
    await db.commit()
    cursor = await db.execute("SELECT last_insert_rowid() AS id")
    post_id = int((await cursor.fetchone())["id"])
    post = await get_insight_post(post_id, viewer_google_sub=google_sub)
    return post or {"id": post_id}


def _parse_json_field(value: str | None, fallback):
    if not value:
        return fallback
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return fallback


def _inflate_insight_row(row) -> dict:
    out = dict(row)
    out["tags"] = _parse_json_field(out.pop("tags_json", None), [])
    out["result_summary"] = _parse_json_field(out.pop("result_summary_json", None), {})
    payload_raw = out.pop("result_payload_json", None)
    out["has_result_payload"] = bool(payload_raw)
    if payload_raw is not None:
        out["result_payload"] = _parse_json_field(payload_raw, None)
    return out


async def list_insight_posts(*, viewer_google_sub: str | None = None, limit: int = 50) -> list[dict]:
    db = await get_db()
    limit = max(1, min(int(limit or 50), 100))
    if viewer_google_sub:
        cursor = await db.execute(
            """
            SELECT p.*, u.name AS author_name, u.email AS author_email
            FROM insight_posts p
            JOIN users u ON u.google_sub = p.google_sub
            WHERE p.visibility = 'public' OR p.google_sub = ?
            ORDER BY p.created_at DESC, p.id DESC
            LIMIT ?
            """,
            (viewer_google_sub, limit),
        )
    else:
        cursor = await db.execute(
            """
            SELECT p.*, u.name AS author_name, u.email AS author_email
            FROM insight_posts p
            JOIN users u ON u.google_sub = p.google_sub
            WHERE p.visibility = 'public'
            ORDER BY p.created_at DESC, p.id DESC
            LIMIT ?
            """,
            (limit,),
        )
    rows = [_inflate_insight_row(row) for row in await cursor.fetchall()]
    for row in rows:
        row.pop("result_payload", None)
    return rows


async def get_insight_post(post_id: int, *, viewer_google_sub: str | None = None) -> dict | None:
    db = await get_db()
    if viewer_google_sub:
        cursor = await db.execute(
            """
            SELECT p.*, u.name AS author_name, u.email AS author_email
            FROM insight_posts p
            JOIN users u ON u.google_sub = p.google_sub
            WHERE p.id = ? AND (p.visibility = 'public' OR p.google_sub = ?)
            """,
            (post_id, viewer_google_sub),
        )
    else:
        cursor = await db.execute(
            """
            SELECT p.*, u.name AS author_name, u.email AS author_email
            FROM insight_posts p
            JOIN users u ON u.google_sub = p.google_sub
            WHERE p.id = ? AND p.visibility = 'public'
            """,
            (post_id,),
        )
    row = await cursor.fetchone()
    return _inflate_insight_row(row) if row else None


async def delete_insight_post(post_id: int, *, google_sub: str, is_admin: bool = False) -> bool:
    db = await get_db()
    if is_admin:
        cursor = await db.execute("DELETE FROM insight_posts WHERE id = ?", (post_id,))
    else:
        cursor = await db.execute(
            "DELETE FROM insight_posts WHERE id = ? AND google_sub = ?",
            (post_id, google_sub),
        )
    await db.commit()
    return cursor.rowcount > 0


USER_RECENT_MAX = 20


async def touch_user_recent_analysis(google_sub: str, stock_code: str):
    db = await get_db()
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


async def delete_user_recent_analysis(google_sub: str, stock_code: str):
    db = await get_db()
    await db.execute(
        "DELETE FROM user_recent_analyses WHERE google_sub = ? AND stock_code = ?",
        (google_sub, stock_code),
    )
    await db.commit()


async def get_user_stock_preference(google_sub: str, stock_code: str) -> dict:
    db = await get_db()
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


async def save_user_stock_order(google_sub: str, ordered_stock_codes: list[str]):
    db = await get_db()
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


async def save_starred_order(google_sub: str, ordered_stock_codes: list[str]):
    db = await get_db()
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


async def unstar_stock(google_sub: str, stock_code: str):
    db = await get_db()
    await db.execute(
        """
        UPDATE user_stock_preferences
        SET is_starred = 0, starred_order = NULL, updated_at = ?
        WHERE google_sub = ? AND stock_code = ?
        """,
        (datetime.now().isoformat(), google_sub, stock_code),
    )
    await db.commit()


# --- Portfolio ---

async def resolve_stock_name(stock_code: str) -> str | None:
    name = await get_corp_name(stock_code)
    if name:
        return name
    return None


async def get_portfolio(google_sub: str) -> list[dict]:
    db = await get_db()
    # created_at is surfaced so the UI can show '등록일자' and let the
    # user edit it. It was already stored on every insert but wasn't in
    # the SELECT list — the column existed server-side but was invisible.
    cursor = await db.execute(
        """
        SELECT stock_code, stock_name, quantity, avg_price, sort_order,
               COALESCE(currency, 'KRW') AS currency, group_name, benchmark_code,
               created_at, target_price,
               COALESCE(target_price_disabled, 0) AS target_price_disabled
        FROM user_portfolio
        WHERE google_sub = ?
        ORDER BY CASE WHEN sort_order IS NULL THEN 1 ELSE 0 END, sort_order ASC, created_at ASC
        """,
        (google_sub,),
    )
    return [dict(row) for row in await cursor.fetchall()]


# Korean preferred-stock codes end with a suffix other than '0'. The
# market_data table typically only stores the common stock's dividend
# record, so the preferred stock gets no hit on an exact code match —
# hence the fallback to the common-stock row via _pref_to_common().
_KR_PREF_SUFFIXES = ("5", "7", "8", "9", "K", "L")


def _pref_to_common(code: str) -> str | None:
    """우선주 코드 → 해당 보통주 코드 (e.g. 005935 → 005930, 00088K → 000880).
    우선주가 아니면 None."""
    if len(code) != 6:
        return None
    if code[-1] in _KR_PREF_SUFFIXES:
        return code[:-1] + "0"
    return None


async def get_trailing_dividends(stock_codes: list[str]) -> dict[str, float]:
    """Latest positive `dividend_per_share` per stock across market_data.

    Used for the 배당액 column — the UI multiplies this by the user's
    quantity client-side so edits to quantity reflect instantly. We pull
    the most recent non-zero value because dividend_per_share occasionally
    arrives as 0 for the current year (pre-announcement) and that would
    hide the true trailing yield.

    Excluded: the current calendar year, to avoid the "0 until announced"
    trap the analysis page had (see stock_price.py dividend fallback).

    Preferred-stock fallback: the market_data pipeline (stock_price.py)
    indexes by common-stock code, so a 005935 holding will miss unless
    we also look up its common counterpart 005930. The actual dividend
    paid to preferred holders is usually slightly higher (typical 1%p
    dividend premium) than the common, but the common figure is the
    best automated approximation available until preferred-specific
    data is harvested separately. Without this fallback preferred-stock
    배당액 is silently 0 across the whole table.
    """
    if not stock_codes:
        return {}
    # Build expanded query set = user's codes ∪ their common-stock fallbacks.
    pref_to_common_map: dict[str, str] = {}
    for code in stock_codes:
        common = _pref_to_common(code)
        if common and common != code:
            pref_to_common_map[code] = common
    all_codes = list(set(stock_codes) | set(pref_to_common_map.values()))
    current_year = datetime.now().year
    placeholders = ",".join("?" for _ in all_codes)
    db = await get_db()
    # 0 도 유효한 '무배당 확정' 값으로 취급 — 이전엔 `> 0` 필터로 제외
    # 되어 UI 에서 '-' 로 표시됐는데, 사용자 요청대로 국내 배당 없는 종목
    # 은 '0' 이라 찍어야 정직함. 가장 최근 NOT NULL 연도를 고르므로
    # 과거에 배당이 있었더라도 최신 해에 정책이 바뀌어 0 이 됐으면 그
    # 값이 반영됨 (= 배당 중단 상태 정직 표시).
    cursor = await db.execute(
        f"""SELECT stock_code, dividend_per_share, year
            FROM market_data
            WHERE stock_code IN ({placeholders})
              AND dividend_per_share IS NOT NULL
              AND year < ?
              AND (stock_code, year) IN (
                SELECT stock_code, MAX(year)
                FROM market_data
                WHERE stock_code IN ({placeholders})
                  AND dividend_per_share IS NOT NULL
                  AND year < ?
                GROUP BY stock_code
              )""",
        (*all_codes, current_year, *all_codes, current_year),
    )
    direct_dps = {row["stock_code"]: float(row["dividend_per_share"]) for row in await cursor.fetchall()}

    # Preferred-stock override — a separately-curated Google Sheet carries
    # the authoritative per-year dividend for each preferred (Data!AI =
    # most recent year). When the sheet has an entry — ZERO INCLUDED —
    # that value wins over the common-stock fallback. The curator has
    # explicitly said "sheet 0 means dividend-of-zero, not unknown", so
    # we must not silently substitute the common stock's non-zero number.
    # Only NULL rows (missing) fall through to the common fallback.
    pref_codes = list(pref_to_common_map.keys())
    pref_overrides: dict[str, float] = {}
    if pref_codes:
        pref_placeholders = ",".join("?" for _ in pref_codes)
        cursor = await db.execute(
            f"""SELECT stock_code, dividend_per_share
                FROM preferred_dividends
                WHERE stock_code IN ({pref_placeholders})
                  AND dividend_per_share IS NOT NULL""",
            pref_codes,
        )
        pref_overrides = {row["stock_code"]: float(row["dividend_per_share"]) for row in await cursor.fetchall()}

    # foreign_dividends — yfinance-sourced + admin manual overrides.
    # Single lookup handles everything outside the KR market_data pipeline.
    # We query for all requested codes (not just preferred) because:
    #   - Overseas tickers (AAPL, GOOGL, ...) live here exclusively.
    #   - Admin may occasionally override any code via the manual API
    #     (naming calls this table 'foreign' but practically it's a
    #     generic override lane — market_data still wins when present).
    foreign_placeholders = ",".join("?" for _ in stock_codes)
    cursor = await db.execute(
        f"""SELECT stock_code, dps_krw
            FROM foreign_dividends
            WHERE stock_code IN ({foreign_placeholders})
              AND dps_krw IS NOT NULL""",
        stock_codes,
    )
    foreign_overrides = {row["stock_code"]: float(row["dps_krw"]) for row in await cursor.fetchall()}

    # Resolution order (most-specific first):
    #   (a) market_data 에 양수 dps — 한국 보통주의 authoritative 값
    #   (b) foreign_dividends — yfinance 해외 주식 / 관리자 manual override
    #       이 계층이 (a) 의 '0' 값보다 먼저 체크되는 것이 핵심: 채권
    #       ETF 같은 종목은 market_data 에 dps=0 으로 저장되지만 실제로
    #       분배금이 있으므로 관리자가 override 한 값이 있으면 그걸
    #       사용해야 함. market_data 양수 (a) 는 여전히 최상위라서 일반
    #       한국 주식의 자동 수집 값은 override 에 덮이지 않음.
    #   (c) market_data 의 0 값 — 배당 없는 종목 (ETF 등 포함, override
    #       미등록 시). '-' 가 아니라 '0' 으로 표시됨.
    #   (d) preferred_dividends (curated sheet, 0 은 유효 확정값)
    #   (e) 보통주 market_data fallback — 우선주에만 적용. 보통주가 0
    #       이라도 허용해서 '배당 중단' 상태 반영.
    out: dict[str, float] = {}
    for code in stock_codes:
        direct = direct_dps.get(code)
        if direct is not None and direct > 0:
            out[code] = direct
        elif code in foreign_overrides:
            out[code] = foreign_overrides[code]
        elif direct is not None:
            # market_data dps == 0 and no override
            out[code] = direct
        elif code in pref_overrides:
            out[code] = pref_overrides[code]
        elif code in pref_to_common_map and pref_to_common_map[code] in direct_dps:
            out[code] = direct_dps[pref_to_common_map[code]]
    return out


async def upsert_preferred_dividends(rows: list[dict]) -> int:
    """Bulk upsert from the Google Sheet fetcher.

    rows: [{
        "stock_code": "012205",
        "dividend_per_share": 0.0 | None,
        "source_name": "계양전기우",
        "common_code": "012200",
        "sheet_year": 2025,
    }, ...]

    Returns number of rows written. PK is stock_code so re-imports are
    idempotent — caller doesn't need to clear the table first. Rows with
    missing stock_code are skipped silently.
    """
    if not rows:
        return 0
    now = datetime.now().isoformat(timespec="seconds")
    db = await get_db()
    written = 0
    for r in rows:
        code = (r.get("stock_code") or "").strip()
        if not code:
            continue
        await db.execute(
            """INSERT INTO preferred_dividends
               (stock_code, dividend_per_share, source_name, common_code, sheet_year, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(stock_code) DO UPDATE SET
                   dividend_per_share = excluded.dividend_per_share,
                   source_name = excluded.source_name,
                   common_code = excluded.common_code,
                   sheet_year = excluded.sheet_year,
                   fetched_at = excluded.fetched_at""",
            (
                code,
                r.get("dividend_per_share"),
                r.get("source_name"),
                r.get("common_code"),
                r.get("sheet_year"),
                now,
            ),
        )
        written += 1
    await db.commit()
    return written


async def get_preferred_dividends_count() -> int:
    """Used by admin dashboard — how many preferred rows we have cached."""
    db = await get_db()
    cursor = await db.execute("SELECT COUNT(*) AS n FROM preferred_dividends")
    row = await cursor.fetchone()
    return int(row["n"]) if row else 0


async def list_preferred_dividends() -> list[dict]:
    """Return preferred-dividend sheet cache rows for admin coverage checks."""
    db = await get_db()
    cursor = await db.execute(
        """SELECT stock_code, dividend_per_share, source_name, common_code,
                  sheet_year, fetched_at
           FROM preferred_dividends
           ORDER BY source_name COLLATE NOCASE, stock_code ASC"""
    )
    return [dict(row) for row in await cursor.fetchall()]


# ---------------------------------------------------------------------------
# foreign_dividends — yfinance-sourced + admin-manual overrides
# ---------------------------------------------------------------------------

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
    db = await get_db()
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
    db = await get_db()
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
    db = await get_db()
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
    db = await get_db()
    cursor = await db.execute(
        """SELECT stock_code, dps_native, currency, dps_krw, source,
                  manual_note, fetched_at
           FROM foreign_dividends
           ORDER BY CASE source WHEN 'manual' THEN 0 ELSE 1 END, stock_code ASC"""
    )
    return [dict(row) for row in await cursor.fetchall()]


async def get_foreign_dividends_count() -> int:
    db = await get_db()
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
    db = await get_db()
    cursor = await db.execute(
        "SELECT stock_code, dps_krw, source FROM foreign_dividends WHERE stock_code = ?",
        (code,),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def get_portfolio_item(google_sub: str, stock_code: str) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT stock_code, stock_name, quantity, avg_price, COALESCE(currency, 'KRW') AS currency, group_name FROM user_portfolio WHERE google_sub = ? AND stock_code = ?",
        (google_sub, stock_code),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def update_portfolio_quantity(google_sub: str, stock_code: str, new_quantity: int):
    db = await get_db()
    await db.execute(
        "UPDATE user_portfolio SET quantity = ? WHERE google_sub = ? AND stock_code = ?",
        (new_quantity, google_sub, stock_code),
    )
    await db.commit()


async def add_portfolio_item(
    google_sub: str, stock_code: str, stock_name: str, avg_price: float, quantity: int, currency: str = "KRW",
):
    db = await get_db()
    now = datetime.now().isoformat()
    await db.execute(
        "INSERT INTO user_portfolio (google_sub, stock_code, stock_name, avg_price, quantity, currency, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (google_sub, stock_code, stock_name, avg_price, quantity, currency, now),
    )
    await db.commit()


_TARGET_PRICE_UNCHANGED = object()
_TARGET_DISABLED_UNCHANGED = object()


async def save_portfolio_item(
    google_sub: str, stock_code: str, stock_name: str, quantity: float, avg_price: float,
    currency: str = "KRW", group_name: str | None = None, benchmark_code: str | None = None,
    created_at: str | None = None,
    target_price=_TARGET_PRICE_UNCHANGED,
    target_price_disabled=_TARGET_DISABLED_UNCHANGED,
) -> dict:
    """target_price 인자의 의미:
      - 인자 미전달 (sentinel) → 기존 값 그대로 유지 (수량/매입가만 편집할 때)
      - None              → 자동 계산으로 되돌림 (수동 override 해제)
      - float             → 명시적 수동 고정값

    target_price_disabled:
      - 미전달             → 기존 값 보존
      - True/1             → 자동 계산도 bypass, UI 에서 '-' 로 표시
      - False/0            → 자동 계산 활성화 (기본값)
    """
    db = await get_db()
    now = datetime.now().isoformat()
    # Re-read existing row so we can preserve created_at on simple edits
    # (quantity / avg_price updates shouldn't reset the registration
    # date). Only overwrite created_at when the caller explicitly passes
    # one — that's how the UI's 등록일자 edit gets through.
    cursor = await db.execute(
        "SELECT sort_order, group_name, benchmark_code, created_at, target_price, COALESCE(target_price_disabled, 0) AS target_price_disabled FROM user_portfolio WHERE google_sub = ? AND stock_code = ?",
        (google_sub, stock_code),
    )
    existing = await cursor.fetchone()
    sort_order = existing["sort_order"] if existing else None
    if group_name is None:
        if existing:
            group_name = existing["group_name"]
        else:
            group_name = await _resolve_default_group_name(db, google_sub, stock_code)
    if benchmark_code is None and existing:
        benchmark_code = existing["benchmark_code"]

    # Preserve existing created_at unless overridden; for brand-new rows
    # use `now`. This ordering means an explicit `created_at=None` on an
    # edit leaves the original date untouched, which matches the edit-
    # form contract (leaving the 등록일자 field blank = "no change").
    if created_at is None:
        created_at = existing["created_at"] if existing else now
    # target_price 미전달이면 기존 값 보존, 명시 None 이면 자동계산
    # 으로 되돌림, 숫자면 수동 override 저장.
    if target_price is _TARGET_PRICE_UNCHANGED:
        target_price = existing["target_price"] if existing else None
    if target_price_disabled is _TARGET_DISABLED_UNCHANGED:
        target_price_disabled = int(existing["target_price_disabled"]) if existing else 0
    else:
        target_price_disabled = 1 if target_price_disabled else 0
    # 명시 수동 값을 입력하면 disabled 플래그는 자동 해제 (사용자가
    # 목표가를 넣었다는 건 '표시하고 싶다' 는 의사). 반대로 disabled=1
    # 이면 target_price 는 항상 NULL 로 강제.
    if target_price is not None:
        target_price_disabled = 0
    if target_price_disabled == 1:
        target_price = None

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
        INSERT INTO user_portfolio (google_sub, stock_code, stock_name, quantity, avg_price, sort_order, currency, group_name, benchmark_code, created_at, target_price, target_price_disabled, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(google_sub, stock_code) DO UPDATE SET
            stock_name = excluded.stock_name,
            quantity = excluded.quantity,
            avg_price = excluded.avg_price,
            currency = excluded.currency,
            group_name = excluded.group_name,
            benchmark_code = excluded.benchmark_code,
            created_at = excluded.created_at,
            target_price = excluded.target_price,
            target_price_disabled = excluded.target_price_disabled,
            updated_at = excluded.updated_at
        """,
        (google_sub, stock_code, stock_name, quantity, avg_price, sort_order, currency, group_name, benchmark_code, created_at, target_price, target_price_disabled, now),
    )
    await db.commit()
    return {
        "stock_code": stock_code, "stock_name": stock_name,
        "quantity": quantity, "avg_price": avg_price, "currency": currency,
        "group_name": group_name, "benchmark_code": benchmark_code,
        "created_at": created_at,
        "target_price": target_price,
        "target_price_disabled": target_price_disabled,
    }


async def clear_portfolio(google_sub: str):
    db = await get_db()
    await db.execute("DELETE FROM user_portfolio WHERE google_sub = ?", (google_sub,))
    await db.commit()


async def replace_portfolio(google_sub: str, items: list[dict]):
    """Atomic replace: delete all + insert new in one transaction."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA foreign_keys=ON")
        await db.execute("BEGIN IMMEDIATE")
        try:
            now = datetime.now().isoformat()
            await db.execute("DELETE FROM user_portfolio WHERE google_sub = ?", (google_sub,))
            for i, it in enumerate(items):
                group_name = await _resolve_default_group_name(db, google_sub, it["stock_code"])
                await db.execute(
                    """INSERT INTO user_portfolio (google_sub, stock_code, stock_name, quantity, avg_price, sort_order, currency, group_name, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (google_sub, it["stock_code"], it["stock_name"], it["quantity"], it["avg_price"], i, it.get("currency", "KRW"), group_name, now, now),
                )
        except Exception:
            await db.rollback()
            raise
        await db.commit()


async def delete_portfolio_item(google_sub: str, stock_code: str):
    db = await get_db()
    await db.execute(
        "DELETE FROM user_portfolio WHERE google_sub = ? AND stock_code = ?",
        (google_sub, stock_code),
    )
    await db.commit()


async def update_portfolio_benchmark(google_sub: str, stock_code: str, benchmark_code: str | None):
    db = await get_db()
    await db.execute(
        "UPDATE user_portfolio SET benchmark_code = ?, updated_at = ? WHERE google_sub = ? AND stock_code = ?",
        (benchmark_code, datetime.now().isoformat(), google_sub, stock_code),
    )
    await db.commit()


async def save_portfolio_order(google_sub: str, ordered_stock_codes: list[str]):
    db = await get_db()
    await db.executemany(
        "UPDATE user_portfolio SET sort_order = ?, updated_at = ? WHERE google_sub = ? AND stock_code = ?",
        [
            (index, datetime.now().isoformat(), google_sub, code)
            for index, code in enumerate(ordered_stock_codes)
        ],
    )
    await db.commit()


async def get_portfolio_groups(google_sub: str) -> list[dict]:
    db = await get_db()
    await _ensure_default_groups(db, google_sub)
    await db.commit()
    cursor = await db.execute(
        "SELECT group_name, sort_order, is_default FROM portfolio_groups WHERE google_sub = ? ORDER BY sort_order ASC",
        (google_sub,),
    )
    return [dict(row) for row in await cursor.fetchall()]


async def add_portfolio_group(google_sub: str, group_name: str) -> dict:
    db = await get_db()
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


async def rename_portfolio_group(google_sub: str, old_name: str, new_name: str):
    db = await get_db()
    await db.execute(
        "UPDATE portfolio_groups SET group_name = ? WHERE google_sub = ? AND group_name = ?",
        (new_name, google_sub, old_name),
    )
    await db.execute(
        "UPDATE user_portfolio SET group_name = ? WHERE google_sub = ? AND group_name = ?",
        (new_name, google_sub, old_name),
    )
    await db.commit()


async def delete_portfolio_group(google_sub: str, group_name: str):
    db = await get_db()
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


async def save_portfolio_groups_order(google_sub: str, group_names: list[str]):
    db = await get_db()
    await db.executemany(
        "UPDATE portfolio_groups SET sort_order = ? WHERE google_sub = ? AND group_name = ?",
        [(i, google_sub, name) for i, name in enumerate(group_names)],
    )
    await db.commit()


# --- Portfolio NAV / Snapshots / Cashflows ---

async def get_latest_snapshot(google_sub: str) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT date, total_value, total_invested, nav, total_units FROM portfolio_snapshots WHERE google_sub = ? ORDER BY date DESC LIMIT 1",
        (google_sub,),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def save_snapshot(google_sub: str, date: str, total_value: float, total_invested: float, nav: float, total_units: float, fx_usdkrw: float | None = None):
    db = await get_db()
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
    db = await get_db()
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
    db = await get_db()
    cursor = await db.execute(
        "SELECT date, total_value, total_invested, nav, total_units, fx_usdkrw FROM portfolio_snapshots WHERE google_sub = ? AND date <= ? ORDER BY date DESC LIMIT 1",
        (google_sub, year_end),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def get_nav_history(google_sub: str) -> list[dict]:
    db = await get_db()
    cursor = await db.execute(
        "SELECT date, nav, total_value, total_invested, total_units, fx_usdkrw FROM portfolio_snapshots WHERE google_sub = ? ORDER BY date ASC",
        (google_sub,),
    )
    return [dict(row) for row in await cursor.fetchall()]


async def get_cashflows(google_sub: str) -> list[dict]:
    db = await get_db()
    cursor = await db.execute(
        "SELECT id, date, type, amount, nav_at_time, units_change, memo, created_at FROM portfolio_cashflows WHERE google_sub = ? ORDER BY date DESC, created_at DESC",
        (google_sub,),
    )
    return [dict(row) for row in await cursor.fetchall()]


async def add_cashflow(google_sub: str, date: str, cf_type: str, amount: float, memo: str | None, nav_at_time: float | None, units_change: float | None) -> dict:
    db = await get_db()
    now = datetime.now().isoformat()
    cursor = await db.execute(
        "INSERT INTO portfolio_cashflows (google_sub, date, type, amount, nav_at_time, units_change, memo, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (google_sub, date, cf_type, amount, nav_at_time, units_change, memo, now),
    )
    await db.commit()
    return {"id": cursor.lastrowid, "date": date, "type": cf_type, "amount": amount, "nav_at_time": nav_at_time, "units_change": units_change, "memo": memo, "created_at": now}


async def get_cashflow(google_sub: str, cf_id: int) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT id, type, amount FROM portfolio_cashflows WHERE id = ? AND google_sub = ?",
        (cf_id, google_sub),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def delete_cashflow(google_sub: str, cf_id: int):
    db = await get_db()
    await db.execute("DELETE FROM portfolio_cashflows WHERE id = ? AND google_sub = ?", (cf_id, google_sub))
    await db.commit()


async def get_all_users_with_portfolio() -> list[str]:
    db = await get_db()
    cursor = await db.execute("SELECT DISTINCT google_sub FROM user_portfolio")
    return [row["google_sub"] for row in await cursor.fetchall()]


async def get_pending_cashflows(google_sub: str, date: str) -> list[dict]:
    """Get cashflows for a specific date that haven't been applied to snapshots yet."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT id, type, amount, units_change FROM portfolio_cashflows WHERE google_sub = ? AND date = ?",
        (google_sub, date),
    )
    return [dict(row) for row in await cursor.fetchall()]


async def save_stock_snapshots(google_sub: str, date: str, items: list[dict]):
    """Save per-stock market values for a date. items: [{stock_code, market_value}, ...]"""
    db = await get_db()
    await db.executemany(
        "INSERT OR REPLACE INTO portfolio_stock_snapshots (google_sub, date, stock_code, market_value) VALUES (?, ?, ?, ?)",
        [(google_sub, date, it["stock_code"], it["market_value"]) for it in items],
    )
    await db.commit()


async def get_stock_snapshots_by_date(google_sub: str, date: str) -> list[dict]:
    """Get per-stock snapshots on or before a given date (latest available)."""
    db = await get_db()
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


async def save_intraday_snapshot(google_sub: str, ts: str, total_value: float):
    db = await get_db()
    await db.execute(
        "INSERT OR REPLACE INTO portfolio_intraday (google_sub, ts, total_value) VALUES (?, ?, ?)",
        (google_sub, ts, total_value),
    )
    await db.commit()


async def get_intraday_snapshots(google_sub: str, date: str) -> list[dict]:
    """Get intraday snapshots for a given date (YYYY-MM-DD)."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT ts, total_value FROM portfolio_intraday WHERE google_sub = ? AND ts >= ? AND ts < ? ORDER BY ts ASC",
        (google_sub, date + "T00:00", date + "T99:99"),
    )
    return [dict(r) for r in await cursor.fetchall()]


async def delete_old_intraday(days_to_keep: int = 7):
    """Remove intraday data older than N days."""
    from datetime import date, timedelta
    cutoff = (date.today() - timedelta(days=days_to_keep)).isoformat()
    db = await get_db()
    await db.execute("DELETE FROM portfolio_intraday WHERE ts < ?", (cutoff + "T00:00",))
    await db.commit()


# ---------------------------------------------------------------------------
# NPS (국민연금공단) holdings + snapshots
# ---------------------------------------------------------------------------

async def save_nps_holdings(date: str, items: list[dict]):
    db = await get_db()
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
    db = await get_db()
    cursor = await db.execute(
        "SELECT * FROM nps_holdings WHERE date = ? ORDER BY market_value DESC",
        (date,),
    )
    return [dict(r) for r in await cursor.fetchall()]


async def save_nps_snapshot(date: str, total_value: float, nav: float, count: int, html: str | None = None):
    db = await get_db()
    await db.execute(
        """INSERT OR REPLACE INTO nps_snapshots (date, total_value, nav, total_count, generated_html)
           VALUES (?, ?, ?, ?, ?)""",
        (date, total_value, nav, count, html),
    )
    await db.commit()


async def get_nps_snapshots() -> list[dict]:
    db = await get_db()
    cursor = await db.execute(
        "SELECT date, total_value, nav, total_count FROM nps_snapshots ORDER BY date ASC"
    )
    return [dict(r) for r in await cursor.fetchall()]


async def get_latest_nps_snapshot() -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT date, total_value, nav, total_count FROM nps_snapshots ORDER BY date DESC LIMIT 1"
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def get_latest_nps_html() -> str | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT generated_html FROM nps_snapshots WHERE generated_html IS NOT NULL ORDER BY date DESC LIMIT 1"
    )
    row = await cursor.fetchone()
    return row["generated_html"] if row else None


async def save_latest_report(stock_code: str, report: dict):
    db = await get_db()
    await db.execute(
        "INSERT OR REPLACE INTO latest_report_cache (stock_code, report_json, fetched_at) VALUES (?, ?, ?)",
        (stock_code, json.dumps(report, ensure_ascii=False), datetime.now().isoformat()),
    )
    await db.commit()


async def get_latest_report(stock_code: str, ttl_minutes: int | None = None) -> dict | None:
    db = await get_db()
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


async def save_report_list(stock_code: str, reports: list[dict]):
    db = await get_db()
    await db.execute(
        "INSERT OR REPLACE INTO report_list_cache (stock_code, reports_json, fetched_at) VALUES (?, ?, ?)",
        (stock_code, json.dumps(reports, ensure_ascii=False), datetime.now().isoformat()),
    )
    await db.commit()


async def get_report_list(stock_code: str, ttl_minutes: int | None = None) -> dict | None:
    db = await get_db()
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


async def get_dart_report_review(stock_code: str, rcept_no: str | None = None) -> dict | None:
    db = await get_db()
    if rcept_no:
        cursor = await db.execute(
            "SELECT * FROM dart_report_reviews WHERE stock_code = ? AND rcept_no = ?",
            (stock_code, rcept_no),
        )
    else:
        cursor = await db.execute(
            """
            SELECT * FROM dart_report_reviews
            WHERE stock_code = ?
            ORDER BY COALESCE(report_date, '') DESC, updated_at DESC, id DESC
            LIMIT 1
            """,
            (stock_code,),
        )
    row = await cursor.fetchone()
    if not row:
        return None
    out = dict(row)
    out["comparison_reports"] = json.loads(out.pop("comparison_reports_json") or "[]")
    out["review"] = json.loads(out.pop("review_json") or "{}")
    return out


async def save_dart_report_review(review: dict) -> dict:
    db = await get_db()
    now = datetime.now().isoformat()
    review_payload = review.get("review") or {}
    comparison_reports = review.get("comparison_reports") or []
    await db.execute(
        """
        INSERT INTO dart_report_reviews
            (stock_code, corp_code, corp_name, rcept_no, report_name, report_date,
             primary_doc_chars, comparison_reports_json, review_json, review_md,
             model, tokens_in, tokens_out, cost_usd, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stock_code, rcept_no) DO UPDATE SET
            corp_code = excluded.corp_code,
            corp_name = excluded.corp_name,
            report_name = excluded.report_name,
            report_date = excluded.report_date,
            primary_doc_chars = excluded.primary_doc_chars,
            comparison_reports_json = excluded.comparison_reports_json,
            review_json = excluded.review_json,
            review_md = excluded.review_md,
            model = excluded.model,
            tokens_in = excluded.tokens_in,
            tokens_out = excluded.tokens_out,
            cost_usd = excluded.cost_usd,
            updated_at = excluded.updated_at
        """,
        (
            review.get("stock_code"),
            review.get("corp_code"),
            review.get("corp_name"),
            review.get("rcept_no"),
            review.get("report_name"),
            review.get("report_date"),
            int(review.get("primary_doc_chars") or 0),
            json.dumps(comparison_reports, ensure_ascii=False),
            json.dumps(review_payload, ensure_ascii=False),
            review.get("review_md") or review_payload.get("summary_md") or "",
            review.get("model"),
            int(review.get("tokens_in") or 0),
            int(review.get("tokens_out") or 0),
            float(review.get("cost_usd") or 0),
            review.get("created_at") or now,
            now,
        ),
    )
    await db.commit()
    return await get_dart_report_review(review.get("stock_code"), review.get("rcept_no")) or review


# ---------------------------------------------------------------------------
# User settings (key-value)
# ---------------------------------------------------------------------------

async def get_user_setting(google_sub: str, key: str) -> str | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT value FROM user_settings WHERE google_sub = ? AND key = ?",
        (google_sub, key),
    )
    row = await cursor.fetchone()
    return row["value"] if row else None


async def set_user_setting(google_sub: str, key: str, value: str):
    db = await get_db()
    await db.execute(
        """INSERT INTO user_settings (google_sub, key, value, updated_at)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(google_sub, key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at""",
        (google_sub, key, value, datetime.now().isoformat()),
    )
    await db.commit()


# ---------------------------------------------------------------------------
# Ticker map (foreign stock code → resolved ticker)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Wiki / research-report pipeline
# ---------------------------------------------------------------------------

async def get_pdf_cache_by_sha1(pdf_sha1: str) -> dict | None:
    db = await get_db()
    cursor = await db.execute(
        "SELECT * FROM report_pdf_cache WHERE pdf_sha1 = ?", (pdf_sha1,),
    )
    row = await cursor.fetchone()
    return dict(row) if row else None


async def save_pdf_cache_row(row: dict) -> None:
    """Upsert a row into report_pdf_cache. `row` must contain pdf_sha1."""
    db = await get_db()
    cols = [
        "pdf_sha1", "stock_code", "pdf_url", "file_path", "file_bytes",
        "parsed_text", "parse_status", "parse_error", "downloaded_at", "parsed_at",
    ]
    vals = [row.get(c) for c in cols]
    placeholders = ",".join("?" for _ in cols)
    set_clause = ",".join(f"{c}=excluded.{c}" for c in cols if c != "pdf_sha1")
    await db.execute(
        f"INSERT INTO report_pdf_cache ({','.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT(pdf_sha1) DO UPDATE SET {set_clause}",
        vals,
    )
    await db.commit()


async def pdf_is_already_summarized(stock_code: str, pdf_sha1: str) -> bool:
    """Returns True if a wiki entry for this PDF already exists."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT 1 FROM stock_wiki_entries WHERE stock_code = ? AND source_type = 'broker_report' AND source_ref = ? LIMIT 1",
        (stock_code, pdf_sha1),
    )
    return (await cursor.fetchone()) is not None


async def save_wiki_entry(entry: dict) -> int:
    """Insert a wiki entry. Returns the new row id. Enforces UNIQUE via
    ON CONFLICT — duplicate (stock, source_type, source_ref) replaces
    the prior summary so re-summarizing with a better model overwrites."""
    db = await get_db()
    cols = [
        "stock_code", "source_type", "source_ref", "report_date", "firm",
        "title", "recommendation", "target_price", "summary_md", "key_points_md",
        "model", "tokens_in", "tokens_out", "created_at",
    ]
    vals = [entry.get(c) for c in cols]
    placeholders = ",".join("?" for _ in cols)
    set_clause = ",".join(f"{c}=excluded.{c}" for c in cols if c not in ("stock_code", "source_type", "source_ref"))
    cursor = await db.execute(
        f"INSERT INTO stock_wiki_entries ({','.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT(stock_code, source_type, source_ref) DO UPDATE SET {set_clause}",
        vals,
    )
    await db.commit()
    return cursor.lastrowid


async def get_wiki_entries(stock_code: str, limit: int = 20) -> list[dict]:
    db = await get_db()
    # LEFT JOIN report_pdf_cache so each entry carries the PDF URL it was
    # summarized from. Frontend uses this to attach summaries to matching
    # rows in the broker-report table (and keep the Q&A Retrieval layer
    # unaffected — only an extra string per row).
    cursor = await db.execute(
        """SELECT w.id, w.stock_code, w.source_type, w.source_ref, w.report_date,
                  w.firm, w.title, w.recommendation, w.target_price,
                  w.summary_md, w.key_points_md, w.model, w.created_at,
                  p.pdf_url AS pdf_url
           FROM stock_wiki_entries w
           LEFT JOIN report_pdf_cache p ON p.pdf_sha1 = w.source_ref
           WHERE w.stock_code = ?
           ORDER BY COALESCE(w.report_date, w.created_at) DESC
           LIMIT ?""",
        (stock_code, limit),
    )
    return [dict(r) for r in await cursor.fetchall()]


async def search_wiki(stock_code: str, query: str, limit: int = 5) -> list[dict]:
    """FTS search scoped to one stock. Falls back to recency if FTS returns
    fewer than `limit` matches (e.g. question is too short for meaningful
    tokens, or FTS index is empty). Returned rows are the same shape as
    get_wiki_entries()."""
    db = await get_db()
    # FTS MATCH expects a sanitized query — strip characters FTS treats as
    # operators to avoid "fts5: syntax error near ..." on user input.
    sanitized = _sanitize_fts_query(query)
    rows: list[dict] = []
    if sanitized:
        cursor = await db.execute(
            """SELECT e.id, e.stock_code, e.source_type, e.source_ref, e.report_date,
                      e.firm, e.title, e.recommendation, e.target_price,
                      e.summary_md, e.key_points_md, e.model, e.created_at
               FROM stock_wiki_fts f
               JOIN stock_wiki_entries e ON e.id = f.rowid
               WHERE f.stock_wiki_fts MATCH ? AND e.stock_code = ?
               ORDER BY bm25(stock_wiki_fts) ASC
               LIMIT ?""",
            (sanitized, stock_code, limit),
        )
        rows = [dict(r) for r in await cursor.fetchall()]
    if len(rows) < limit:
        seen = {r["id"] for r in rows}
        extra = await get_wiki_entries(stock_code, limit * 2)
        for e in extra:
            if e["id"] in seen:
                continue
            rows.append(e)
            if len(rows) >= limit:
                break
    return rows[:limit]


def _sanitize_fts_query(q: str) -> str:
    """Strip FTS5 special chars so arbitrary user input doesn't raise.
    Kept simple: keep CJK, ASCII letters/digits, spaces; drop everything
    else. Collapse whitespace."""
    import re
    cleaned = re.sub(r"[^\w\s\uAC00-\uD7AF\u4E00-\u9FFF]+", " ", q)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


async def qa_count_since(google_sub: str, since_iso: str) -> int:
    db = await get_db()
    cursor = await db.execute(
        "SELECT COUNT(*) AS n FROM stock_qa_history WHERE google_sub = ? AND created_at >= ?",
        (google_sub, since_iso),
    )
    row = await cursor.fetchone()
    return int(row["n"]) if row else 0


async def save_qa_entry(entry: dict) -> int:
    db = await get_db()
    cols = [
        "google_sub", "stock_code", "question", "answer_md", "source_ids",
        "model", "tokens_in", "tokens_out", "cost_usd", "created_at",
    ]
    vals = [entry.get(c) for c in cols]
    placeholders = ",".join("?" for _ in cols)
    cursor = await db.execute(
        f"INSERT INTO stock_qa_history ({','.join(cols)}) VALUES ({placeholders})",
        vals,
    )
    await db.commit()
    return cursor.lastrowid


async def get_wiki_stats() -> dict:
    """Aggregate counts for the wiki pipeline — cheap queries, safe to
    call on every page load. Returns {stocks_covered, total_entries,
    pdfs_cached, latest_entry_date}."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT COUNT(DISTINCT stock_code) AS stocks, COUNT(*) AS entries, MAX(COALESCE(report_date, created_at)) AS latest FROM stock_wiki_entries",
    )
    row = await cursor.fetchone()
    cursor2 = await db.execute(
        "SELECT COUNT(*) AS n FROM report_pdf_cache WHERE parse_status = 'parsed'",
    )
    pdf_row = await cursor2.fetchone()
    return {
        "stocks_covered": int(row["stocks"] or 0) if row else 0,
        "total_entries": int(row["entries"] or 0) if row else 0,
        "pdfs_cached": int(pdf_row["n"] or 0) if pdf_row else 0,
        "latest_entry_date": (row["latest"] if row else None) or None,
    }


# ---------------------------------------------------------------------------
# System events (observability)
# ---------------------------------------------------------------------------

async def insert_system_event(
    level: str,
    source: str,
    kind: str,
    *,
    stock_code: str | None = None,
    details: str | None = None,
    ts: str | None = None,
) -> int:
    """Append a structured event row. `details` should be a JSON string —
    callers (via observability.record_event) serialize their payload once
    to avoid a redundant json.loads at read time."""
    db = await get_db()
    if ts is None:
        ts = datetime.now().isoformat(timespec="seconds")
    cursor = await db.execute(
        """INSERT INTO system_events (ts, level, source, kind, stock_code, details)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (ts, level, source, kind, stock_code, details),
    )
    await db.commit()
    return cursor.lastrowid


async def get_system_events(
    *,
    source: str | None = None,
    level: str | None = None,
    stock_code: str | None = None,
    since: str | None = None,
    limit: int = 100,
) -> list[dict]:
    """Filtered newest-first fetch for the admin dashboard."""
    clauses: list[str] = []
    params: list = []
    if source:
        clauses.append("source = ?")
        params.append(source)
    if level:
        clauses.append("level = ?")
        params.append(level)
    if stock_code:
        clauses.append("stock_code = ?")
        params.append(stock_code)
    if since:
        clauses.append("ts >= ?")
        params.append(since)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    # Hard cap — prevent a frontend bug or curl typo from slurping the
    # whole table.
    limit = max(1, min(int(limit), 1000))
    params.append(limit)
    db = await get_db()
    cursor = await db.execute(
        f"SELECT id, ts, level, source, kind, stock_code, details "
        f"FROM system_events {where} ORDER BY ts DESC, id DESC LIMIT ?",
        params,
    )
    return [dict(r) for r in await cursor.fetchall()]


async def summarize_system_events(since_iso: str) -> dict:
    """Aggregate counts by (source, level) since `since_iso`. Used by the
    top-of-dashboard status card so the admin sees failure spikes without
    scrolling through events."""
    db = await get_db()
    cursor = await db.execute(
        """SELECT source, level, COUNT(*) AS n
           FROM system_events WHERE ts >= ?
           GROUP BY source, level""",
        (since_iso,),
    )
    out: dict[str, dict[str, int]] = {}
    for row in await cursor.fetchall():
        out.setdefault(row["source"], {})[row["level"]] = int(row["n"])
    return out


async def prune_system_events(max_age_days: int = 30, max_rows: int = 100_000) -> int:
    """Best-effort cleanup: drop events older than `max_age_days`, then if
    still above `max_rows` trim the oldest until under the cap. Returns
    rows deleted (0 if table was already small)."""
    db = await get_db()
    # Age-based trim.
    cursor = await db.execute(
        "DELETE FROM system_events WHERE ts < datetime('now', ?)",
        (f"-{int(max_age_days)} days",),
    )
    age_deleted = cursor.rowcount or 0
    # Row-count trim as a safety net. Count only if there's risk of being
    # over — skip when obviously fine.
    cursor = await db.execute("SELECT COUNT(*) AS n FROM system_events")
    row = await cursor.fetchone()
    total = int(row["n"]) if row else 0
    overflow_deleted = 0
    if total > max_rows:
        excess = total - max_rows
        cursor = await db.execute(
            "DELETE FROM system_events WHERE id IN "
            "(SELECT id FROM system_events ORDER BY ts ASC, id ASC LIMIT ?)",
            (excess,),
        )
        overflow_deleted = cursor.rowcount or 0
    await db.commit()
    return age_deleted + overflow_deleted


async def get_latest_event(source: str, kind: str | None = None) -> dict | None:
    """Return the most recent matching event. Dashboard uses this to show
    'last successful tick' per subsystem."""
    db = await get_db()
    if kind:
        cursor = await db.execute(
            "SELECT id, ts, level, source, kind, stock_code, details "
            "FROM system_events WHERE source = ? AND kind = ? "
            "ORDER BY ts DESC, id DESC LIMIT 1",
            (source, kind),
        )
    else:
        cursor = await db.execute(
            "SELECT id, ts, level, source, kind, stock_code, details "
            "FROM system_events WHERE source = ? "
            "ORDER BY ts DESC, id DESC LIMIT 1",
            (source,),
        )
    row = await cursor.fetchone()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# Benchmark daily series (KOSPI / SP500 / GOLD / ...)
# ---------------------------------------------------------------------------

async def save_benchmark_rows(code: str, rows: list[dict], source: str = "yfinance") -> int:
    """Upsert daily rows for a benchmark code.

    rows: [{"date": "YYYY-MM-DD", "close": float}, ...]
    Returns number of rows written. Safe to call with overlapping ranges —
    the (code, date) PK makes each call idempotent.
    """
    if not rows:
        return 0
    # Match the rest of this module — plain local-time ISO (no TZ suffix).
    fetched_at = datetime.now().isoformat(timespec="seconds")
    db = await get_db()
    await db.executemany(
        """INSERT INTO benchmark_daily (code, date, close, source, fetched_at)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(code, date) DO UPDATE SET
               close=excluded.close,
               source=excluded.source,
               fetched_at=excluded.fetched_at""",
        [(code, r["date"], float(r["close"]), source, fetched_at) for r in rows if r.get("close") is not None],
    )
    await db.commit()
    return len(rows)


async def get_benchmark_rows(code: str, start: str | None = None, end: str | None = None) -> list[dict]:
    """Read {date, close} rows for a benchmark code, optionally bounded by
    inclusive [start, end] (YYYY-MM-DD). Ordered by date ascending."""
    db = await get_db()
    clauses = ["code = ?"]
    params: list = [code]
    if start:
        clauses.append("date >= ?")
        params.append(start)
    if end:
        clauses.append("date <= ?")
        params.append(end)
    sql = f"SELECT date, close FROM benchmark_daily WHERE {' AND '.join(clauses)} ORDER BY date ASC"
    cursor = await db.execute(sql, params)
    return [{"date": r["date"], "close": r["close"]} for r in await cursor.fetchall()]


async def get_benchmark_last_date(code: str) -> str | None:
    """Return the latest stored date for a code, or None if table is empty
    for that code. Used to compute the incremental fetch window."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT MAX(date) AS d FROM benchmark_daily WHERE code = ?",
        (code,),
    )
    row = await cursor.fetchone()
    return row["d"] if row and row["d"] else None


async def get_benchmark_earliest_date(code: str) -> str | None:
    """Return the earliest stored date for a code. Used by lazy backfill to
    decide whether the request's `start` predates what we have on disk."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT MIN(date) AS d FROM benchmark_daily WHERE code = ?",
        (code,),
    )
    row = await cursor.fetchone()
    return row["d"] if row and row["d"] else None


async def select_wiki_target_stocks(recent_days: int = 30) -> list[str]:
    """Pick which stocks the wiki ingestion pipeline should process.

    Union of four signals:
      (a) currently held in any user portfolio
      (b) starred by any user
      (c) per-user recent analysis (user_recent_analyses) within N days
      (d) any stock whose analysis snapshot (analysis_meta) was touched
          within N days

    (d) was added after a concrete miss: LG화학 had an analysis snapshot
    but no wiki entries, because user_recent_analyses bookkeeping had
    gone missing for that stock (the per-user path depends on a logged-in
    session being correctly threaded through /api/analyze — brittle).
    analysis_meta.analyzed_at is updated on every successful analysis run
    regardless of session state, so folding it in makes "user searched
    for this stock" the robust trigger the pipeline needed.
    """
    db = await get_db()
    cursor = await db.execute(
        f"""SELECT DISTINCT stock_code FROM (
            SELECT stock_code FROM user_portfolio
            UNION
            SELECT stock_code FROM user_stock_preferences WHERE is_starred = 1
            UNION
            SELECT stock_code FROM user_recent_analyses
              WHERE viewed_at >= datetime('now', '-{int(recent_days)} days')
            UNION
            SELECT stock_code FROM analysis_meta
              WHERE analyzed_at >= datetime('now', '-{int(recent_days)} days')
        )
        WHERE stock_code IS NOT NULL""",
    )
    return [r["stock_code"] for r in await cursor.fetchall()]
