import aiosqlite
import json
from pathlib import Path
from datetime import datetime

from cache_layer import CacheEntry, expires_at_for, parse_iso
from services.portfolio.identifiers import is_korean_stock as _is_portfolio_korean_stock

DB_PATH = Path(__file__).parent / "cache.db"

CACHE_NS_LATEST_REPORT = "reports.latest"
CACHE_NS_REPORT_LIST = "reports.list"

_conn: aiosqlite.Connection | None = None
_corp_code_table: dict[str, dict[str, str]] | None = None


async def get_db() -> aiosqlite.Connection:
    global _conn
    if _conn is None:
        _conn = await aiosqlite.connect(DB_PATH)
        _conn.row_factory = aiosqlite.Row
        await _conn.execute("PRAGMA journal_mode=WAL")
        await _conn.execute("PRAGMA busy_timeout=5000")
        await _conn.execute("PRAGMA foreign_keys=ON")
    return _conn


async def close_db():
    """Shutdown: close the shared connection."""
    global _conn, _corp_code_table
    if _conn is not None:
        await _conn.close()
        _conn = None
    _corp_code_table = None


async def _refresh_group_snapshots(db: aiosqlite.Connection, google_sub: str | None = None, snap_date: str | None = None):
    """Rebuild pre-aggregated group weights from per-stock snapshots.

    Group trend reads must stay cheap as history grows, so the expensive
    stock-level GROUP BY happens once at snapshot time (or one-time backfill),
    not on every chart request.
    """
    where = []
    params: list[str] = []
    if google_sub is not None:
        where.append("ps.google_sub = ?")
        params.append(google_sub)
    if snap_date is not None:
        where.append("ps.date = ?")
        params.append(snap_date)
    where_sql = "WHERE " + " AND ".join(where) if where else ""

    if google_sub is not None and snap_date is not None:
        await db.execute(
            "DELETE FROM portfolio_group_snapshots WHERE google_sub = ? AND date = ?",
            (google_sub, snap_date),
        )
    elif google_sub is not None:
        await db.execute("DELETE FROM portfolio_group_snapshots WHERE google_sub = ?", (google_sub,))
    else:
        await db.execute("DELETE FROM portfolio_group_snapshots")

    await db.execute(
        f"""
        WITH stock_rows AS (
            SELECT
                ps.google_sub,
                ps.date,
                COALESCE(ps.group_name, up.group_name, '기타') AS group_name,
                ps.stock_code,
                ps.market_value
            FROM portfolio_stock_snapshots ps
            LEFT JOIN user_portfolio up
              ON up.google_sub = ps.google_sub
             AND up.stock_code = ps.stock_code
            {where_sql}
        ),
        day_totals AS (
            SELECT google_sub, date, SUM(market_value) AS total_value
            FROM stock_rows
            GROUP BY google_sub, date
        ),
        group_rows AS (
            SELECT
                google_sub,
                date,
                group_name,
                SUM(market_value) AS market_value,
                COUNT(DISTINCT stock_code) AS stock_count
            FROM stock_rows
            GROUP BY google_sub, date, group_name
        )
        INSERT OR REPLACE INTO portfolio_group_snapshots
        (google_sub, date, group_name, market_value, stock_count, total_value, weight_pct)
        SELECT
            gr.google_sub,
            gr.date,
            gr.group_name,
            gr.market_value,
            gr.stock_count,
            dt.total_value AS total_value,
            CASE
                WHEN dt.total_value != 0
                THEN gr.market_value * 100.0 / dt.total_value
                ELSE NULL
            END AS weight_pct
        FROM group_rows gr
        JOIN day_totals dt
          ON dt.google_sub = gr.google_sub
         AND dt.date = gr.date
        """,
        tuple(params),
    )


async def _refresh_stock_weight_snapshots(db: aiosqlite.Connection, google_sub: str | None = None, snap_date: str | None = None):
    """Rebuild pre-aggregated per-stock weights for group drill-down charts."""
    where = []
    params: list[str] = []
    if google_sub is not None:
        where.append("ps.google_sub = ?")
        params.append(google_sub)
    if snap_date is not None:
        where.append("ps.date = ?")
        params.append(snap_date)
    where_sql = "WHERE " + " AND ".join(where) if where else ""

    if google_sub is not None and snap_date is not None:
        await db.execute(
            "DELETE FROM portfolio_stock_weight_snapshots WHERE google_sub = ? AND date = ?",
            (google_sub, snap_date),
        )
    elif google_sub is not None:
        await db.execute("DELETE FROM portfolio_stock_weight_snapshots WHERE google_sub = ?", (google_sub,))
    else:
        await db.execute("DELETE FROM portfolio_stock_weight_snapshots")

    await db.execute(
        f"""
        WITH stock_rows AS (
            SELECT
                ps.google_sub,
                ps.date,
                ps.stock_code,
                COALESCE(up.stock_name, ps.stock_code) AS stock_name,
                COALESCE(ps.group_name, up.group_name, '기타') AS group_name,
                ps.market_value
            FROM portfolio_stock_snapshots ps
            LEFT JOIN user_portfolio up
              ON up.google_sub = ps.google_sub
             AND up.stock_code = ps.stock_code
            {where_sql}
        ),
        day_totals AS (
            SELECT google_sub, date, SUM(market_value) AS total_value
            FROM stock_rows
            GROUP BY google_sub, date
        ),
        group_totals AS (
            SELECT google_sub, date, group_name, SUM(market_value) AS group_value
            FROM stock_rows
            GROUP BY google_sub, date, group_name
        )
        INSERT OR REPLACE INTO portfolio_stock_weight_snapshots
        (google_sub, date, group_name, stock_code, stock_name, market_value, group_value, total_value, group_weight_pct, portfolio_weight_pct)
        SELECT
            sr.google_sub,
            sr.date,
            sr.group_name,
            sr.stock_code,
            sr.stock_name,
            sr.market_value,
            gt.group_value,
            dt.total_value,
            CASE
                WHEN gt.group_value != 0
                THEN sr.market_value * 100.0 / gt.group_value
                ELSE NULL
            END AS group_weight_pct,
            CASE
                WHEN dt.total_value != 0
                THEN sr.market_value * 100.0 / dt.total_value
                ELSE NULL
            END AS portfolio_weight_pct
        FROM stock_rows sr
        JOIN group_totals gt
          ON gt.google_sub = sr.google_sub
         AND gt.date = sr.date
         AND gt.group_name = sr.group_name
        JOIN day_totals dt
          ON dt.google_sub = sr.google_sub
         AND dt.date = sr.date
        """,
        tuple(params),
    )


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

        CREATE TABLE IF NOT EXISTS cache_values (
            namespace TEXT NOT NULL,
            key TEXT NOT NULL,
            value_json TEXT NOT NULL,
            cached_at TEXT NOT NULL,
            expires_at TEXT,
            ttl_seconds REAL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (namespace, key)
        );

        CREATE INDEX IF NOT EXISTS idx_cache_values_expires
            ON cache_values(namespace, expires_at);

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

        CREATE TABLE IF NOT EXISTS portfolio_tags (
            google_sub TEXT NOT NULL,
            stock_code TEXT NOT NULL,
            tag TEXT NOT NULL,
            sort_order INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            PRIMARY KEY (google_sub, stock_code, tag),
            FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE,
            FOREIGN KEY (google_sub, stock_code) REFERENCES user_portfolio(google_sub, stock_code) ON DELETE CASCADE
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

        CREATE TABLE IF NOT EXISTS portfolio_group_snapshots (
            google_sub TEXT NOT NULL,
            date TEXT NOT NULL,
            group_name TEXT NOT NULL,
            market_value REAL NOT NULL DEFAULT 0,
            stock_count INTEGER NOT NULL DEFAULT 0,
            total_value REAL NOT NULL DEFAULT 0,
            weight_pct REAL,
            PRIMARY KEY (google_sub, date, group_name),
            FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_group_snapshots_sub_date ON portfolio_group_snapshots(google_sub, date);

        CREATE TABLE IF NOT EXISTS portfolio_stock_weight_snapshots (
            google_sub TEXT NOT NULL,
            date TEXT NOT NULL,
            group_name TEXT NOT NULL,
            stock_code TEXT NOT NULL,
            stock_name TEXT NOT NULL,
            market_value REAL NOT NULL DEFAULT 0,
            group_value REAL NOT NULL DEFAULT 0,
            total_value REAL NOT NULL DEFAULT 0,
            group_weight_pct REAL,
            portfolio_weight_pct REAL,
            PRIMARY KEY (google_sub, date, group_name, stock_code),
            FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_stock_weight_snapshots_sub_group_date ON portfolio_stock_weight_snapshots(google_sub, group_name, date);

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

        -- Cached daily market briefs. Public briefs use google_sub='public';
        -- authenticated rows are per user because the 관심목록/portfolio
        -- evidence bundle is user-specific.
        CREATE TABLE IF NOT EXISTS daily_market_briefs (
            google_sub    TEXT NOT NULL,
            brief_date    TEXT NOT NULL,
            source_hash   TEXT NOT NULL,
            payload_json  TEXT NOT NULL,
            markdown      TEXT NOT NULL,
            model         TEXT,
            tokens_in     INTEGER,
            tokens_out    INTEGER,
            cost_usd      REAL,
            created_at    TEXT NOT NULL,
            updated_at    TEXT NOT NULL,
            PRIMARY KEY (google_sub, brief_date)
        );
        CREATE INDEX IF NOT EXISTS idx_daily_market_briefs_updated
            ON daily_market_briefs(updated_at DESC);

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

        -- Notification channels: per-user delivery targets. channel is e.g.
        -- 'telegram' (and later 'kakao'); config_json holds channel-specific
        -- payload (telegram: {"chat_id":..., "username":...}). verified=1 once
        -- the bot link handshake captured a real chat_id.
        CREATE TABLE IF NOT EXISTS notification_channels (
            google_sub TEXT NOT NULL,
            channel TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            config_json TEXT NOT NULL DEFAULT '{}',
            verified INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (google_sub, channel),
            FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
        );

        -- One-shot codes for the Telegram bot auto-link handshake. The
        -- getUpdates poller maps an incoming `/start <code>` back to the user
        -- who generated it, then deletes the row.
        CREATE TABLE IF NOT EXISTS notification_links (
            code TEXT PRIMARY KEY,
            google_sub TEXT NOT NULL,
            channel TEXT NOT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
        );

        -- Portfolio alert rules + edge-trigger state. scope='stock' uses
        -- stock_code; scope='portfolio' aggregates NAV / daily change. `armed`
        -- prevents re-sending every tick: it drops to 0 when fired and re-arms
        -- to 1 once the condition is no longer met.
        CREATE TABLE IF NOT EXISTS portfolio_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            google_sub TEXT NOT NULL,
            scope TEXT NOT NULL,
            stock_code TEXT,
            alert_type TEXT NOT NULL,
            threshold REAL NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            note TEXT NOT NULL DEFAULT '',
            armed INTEGER NOT NULL DEFAULT 1,
            last_triggered_at TEXT,
            last_value REAL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_portfolio_alerts_user ON portfolio_alerts(google_sub, enabled);
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
    await _ensure_column(db, "user_portfolio", "target_price_formula", "TEXT")
    await _ensure_column(db, "users", "is_admin", "INTEGER NOT NULL DEFAULT 0")
    await _ensure_column(db, "portfolio_snapshots", "fx_usdkrw", "REAL")
    await _ensure_column(db, "portfolio_stock_snapshots", "group_name", "TEXT")
    await _backfill_legacy_cache_values(db)
    await db.execute("CREATE INDEX IF NOT EXISTS idx_stock_snapshots_sub_group_date ON portfolio_stock_snapshots(google_sub, group_name, date)")
    cursor = await db.execute("SELECT COUNT(*) AS n FROM portfolio_group_snapshots")
    group_snapshot_count = (await cursor.fetchone())["n"]
    if group_snapshot_count == 0:
        await _refresh_group_snapshots(db)
    cursor = await db.execute("SELECT COUNT(*) AS n FROM portfolio_stock_weight_snapshots")
    stock_weight_snapshot_count = (await cursor.fetchone())["n"]
    if stock_weight_snapshot_count == 0:
        await _refresh_stock_weight_snapshots(db)
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


async def _backfill_legacy_cache_values(db: aiosqlite.Connection) -> None:
    """Promote older per-feature cache tables into the unified cache table."""
    await db.execute(
        """
        INSERT OR IGNORE INTO cache_values
            (namespace, key, value_json, cached_at, expires_at, ttl_seconds, updated_at)
        SELECT ?, stock_code, report_json, fetched_at, NULL, NULL, fetched_at
        FROM latest_report_cache
        """,
        (CACHE_NS_LATEST_REPORT,),
    )
    await db.execute(
        """
        INSERT OR IGNORE INTO cache_values
            (namespace, key, value_json, cached_at, expires_at, ttl_seconds, updated_at)
        SELECT ?, stock_code, reports_json, fetched_at, NULL, NULL, fetched_at
        FROM report_list_cache
        """,
        (CACHE_NS_REPORT_LIST,),
    )


def _ttl_for_minutes(ttl_minutes: int | None) -> float | None:
    if ttl_minutes is None:
        return None
    return float(ttl_minutes) * 60


def _cache_entry_from_row(
    row: aiosqlite.Row,
    *,
    ttl_seconds: float | None = None,
) -> CacheEntry | None:
    cached_at = parse_iso(row["cached_at"])
    if cached_at is None:
        return None
    stored_ttl = row["ttl_seconds"]
    effective_ttl = ttl_seconds if ttl_seconds is not None else (
        float(stored_ttl) if stored_ttl is not None else None
    )
    expires_at = expires_at_for(cached_at, effective_ttl)
    if expires_at is None:
        expires_at = row["expires_at"]
    stale = False
    expires_at_dt = parse_iso(expires_at)
    if expires_at_dt is not None:
        stale = datetime.now() >= expires_at_dt
    try:
        value = json.loads(row["value_json"])
    except (TypeError, json.JSONDecodeError):
        return None
    return CacheEntry(
        key=row["key"],
        value=value,
        cached_at=row["cached_at"],
        expires_at=expires_at,
        ttl_seconds=effective_ttl,
        stale=stale,
    )


async def set_cache_value(
    namespace: str,
    key: str,
    value,
    *,
    ttl_seconds: float | None = None,
) -> CacheEntry:
    """Store one JSON cache value with explicit cached/expires timestamps."""
    db = await get_db()
    cached_at_dt = datetime.now()
    cached_at = cached_at_dt.isoformat(timespec="seconds")
    expires_at = expires_at_for(cached_at_dt, ttl_seconds)
    await db.execute(
        """
        INSERT INTO cache_values
            (namespace, key, value_json, cached_at, expires_at, ttl_seconds, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(namespace, key) DO UPDATE SET
            value_json = excluded.value_json,
            cached_at = excluded.cached_at,
            expires_at = excluded.expires_at,
            ttl_seconds = excluded.ttl_seconds,
            updated_at = excluded.updated_at
        """,
        (
            namespace,
            key,
            json.dumps(value, ensure_ascii=False),
            cached_at,
            expires_at,
            ttl_seconds,
            cached_at,
        ),
    )
    await db.commit()
    return CacheEntry(
        key=key,
        value=value,
        cached_at=cached_at,
        expires_at=expires_at,
        ttl_seconds=ttl_seconds,
        stale=False,
    )


async def get_cache_value_entry(
    namespace: str,
    key: str,
    *,
    ttl_seconds: float | None = None,
    allow_stale: bool = False,
) -> CacheEntry | None:
    """Read one cache value. A stale row is returned only when allowed."""
    db = await get_db()
    cursor = await db.execute(
        """
        SELECT namespace, key, value_json, cached_at, expires_at, ttl_seconds
        FROM cache_values
        WHERE namespace = ? AND key = ?
        """,
        (namespace, key),
    )
    row = await cursor.fetchone()
    if not row:
        return None
    entry = _cache_entry_from_row(row, ttl_seconds=ttl_seconds)
    if entry is None:
        return None
    if entry.stale and not allow_stale:
        return None
    return entry.with_value_copy()


async def delete_cache_value(namespace: str, key: str) -> None:
    db = await get_db()
    await db.execute(
        "DELETE FROM cache_values WHERE namespace = ? AND key = ?",
        (namespace, key),
    )
    await db.commit()


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
    if _is_portfolio_korean_stock(stock_code):
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
    global _corp_code_table
    db = await get_db()
    now = datetime.now().isoformat()
    await db.executemany(
        "INSERT OR REPLACE INTO corp_codes (stock_code, corp_code, corp_name, modify_date, updated_at) VALUES (?, ?, ?, ?, ?)",
        [(c["stock_code"], c["corp_code"], c["corp_name"], c.get("modify_date"), now) for c in codes],
    )
    await db.commit()
    _corp_code_table = {
        str(c["stock_code"]): {
            "stock_code": str(c["stock_code"]),
            "corp_code": str(c.get("corp_code") or ""),
            "corp_name": str(c.get("corp_name") or ""),
        }
        for c in codes
        if c.get("stock_code") and c.get("corp_name")
    }


_CORP_SEARCH_ALIASES = {
    # DART stores the KCC parent company as the Korean legal name, so a plain
    # "KCC" search otherwise only finds KCC건설.
    "KCC": ["002380"],
}


def _corp_search_alias_codes(query: str) -> list[str]:
    return _CORP_SEARCH_ALIASES.get((query or "").strip().upper(), [])


async def search_corp(query: str) -> list[dict]:
    query = (query or "").strip()
    db = await get_db()
    alias_rows = []
    for stock_code in _corp_search_alias_codes(query):
        cursor = await db.execute(
            "SELECT stock_code, corp_code, corp_name FROM corp_codes WHERE stock_code = ?",
            (stock_code,),
        )
        row = await cursor.fetchone()
        if row:
            alias_rows.append(dict(row))

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
    seen_codes = set()
    exact_name_seen = set()
    for item in [*alias_rows, *(dict(row) for row in rows)]:
        if item["stock_code"] in seen_codes:
            continue
        seen_codes.add(item["stock_code"])
        if item["corp_name"] == query:
            if item["corp_name"] in exact_name_seen:
                continue
            exact_name_seen.add(item["corp_name"])
        results.append(item)
    return results


async def resolve_corp_search_query(query: str) -> dict | None:
    query = (query or "").strip()
    if not query:
        return None
    rows = await search_corp(query)
    if not rows:
        return None
    first = rows[0]
    normalized = query.upper()
    if (
        _corp_search_alias_codes(query)
        or first["stock_code"] == query
        or first["corp_name"] == query
        or first["corp_name"].upper() == normalized
    ):
        return first
    return None


async def get_corp_code(stock_code: str) -> str | None:
    row = (await load_corp_code_table()).get(str(stock_code or "").strip())
    return row["corp_code"] if row else None


async def get_corp_name(stock_code: str) -> str | None:
    row = (await load_corp_code_table()).get(str(stock_code or "").strip())
    return row["corp_name"] if row else None


async def load_corp_code_table(*, force: bool = False) -> dict[str, dict[str, str]]:
    """Return the full internal listed-company code table.

    This is a reference table, not a best-effort quote/name cache. It is
    refreshed from DART at startup and then kept in memory for fast UI paths
    such as benchmark label rendering.
    """
    global _corp_code_table
    if _corp_code_table is not None and not force:
        return _corp_code_table
    db = await get_db()
    cursor = await db.execute(
        "SELECT stock_code, corp_code, corp_name FROM corp_codes WHERE stock_code IS NOT NULL AND stock_code != ''"
    )
    _corp_code_table = {
        row["stock_code"]: {
            "stock_code": row["stock_code"],
            "corp_code": row["corp_code"],
            "corp_name": row["corp_name"],
        }
        for row in await cursor.fetchall()
    }
    return _corp_code_table


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


async def save_latest_report(stock_code: str, report: dict):
    await set_cache_value(CACHE_NS_LATEST_REPORT, stock_code, report)


async def get_latest_report(stock_code: str, ttl_minutes: int | None = None) -> dict | None:
    entry = await get_cache_value_entry(
        CACHE_NS_LATEST_REPORT,
        stock_code,
        ttl_seconds=_ttl_for_minutes(ttl_minutes),
        allow_stale=ttl_minutes is None,
    )
    if entry is None:
        return None
    report = entry.copy_value()
    report["_cached_at"] = entry.cached_at
    report["_expires_at"] = entry.expires_at
    report["_stale"] = entry.stale
    return report


async def save_report_list(stock_code: str, reports: list[dict]):
    await set_cache_value(CACHE_NS_REPORT_LIST, stock_code, reports)


async def get_report_list(stock_code: str, ttl_minutes: int | None = None) -> dict | None:
    entry = await get_cache_value_entry(
        CACHE_NS_REPORT_LIST,
        stock_code,
        ttl_seconds=_ttl_for_minutes(ttl_minutes),
        allow_stale=ttl_minutes is None,
    )
    if entry is None:
        return None
    return {
        "reports": entry.copy_value(),
        "fetched_at": entry.cached_at,
        "expires_at": entry.expires_at,
        "stale": entry.stale,
    }


# ---------------------------------------------------------------------------
# Repository re-exports — see repositories/ package. Imported at the bottom so
# get_db() and shared primitives above are already defined; repositories reach
# the connection via cache.get_db() (no import cycle).
# ---------------------------------------------------------------------------
from repositories.system_events import (  # noqa: E402
    insert_system_event,
    get_system_events,
    summarize_system_events,
    summarize_http_metrics,
    prune_system_events,
    get_latest_event,
)
from repositories.benchmark_daily import (  # noqa: E402
    save_benchmark_rows,
    get_benchmark_rows,
    get_benchmark_last_date,
    get_benchmark_earliest_date,
)
from repositories.ticker_map import (  # noqa: E402
    load_ticker_map,
    save_ticker,
)
from repositories.user_settings import (  # noqa: E402
    get_user_setting,
    set_user_setting,
)
from repositories.nps import (  # noqa: E402
    save_nps_holdings,
    get_nps_holdings,
    save_nps_snapshot,
    get_nps_snapshots,
    get_latest_nps_snapshot,
    get_latest_nps_html,
)
from repositories.dart_review import (  # noqa: E402
    get_dart_report_review,
    save_dart_report_review,
)
from repositories.wiki import (  # noqa: E402
    get_pdf_cache_by_sha1,
    save_pdf_cache_row,
    pdf_is_already_summarized,
    save_wiki_entry,
    get_wiki_entries,
    search_wiki,
    qa_count_since,
    save_qa_entry,
    get_wiki_stats,
    select_wiki_target_stocks,
)
from repositories.foreign_dividends import (  # noqa: E402
    upsert_foreign_dividends_auto,
    upsert_foreign_dividend_manual,
    delete_foreign_dividend,
    list_foreign_dividends,
    get_foreign_dividends_count,
    get_foreign_dividend,
)
from repositories.portfolio import (  # noqa: E402
    resolve_stock_name,
    get_portfolio,
    get_portfolio_tags_for_user,
    get_portfolio_target_metrics,
    get_latest_market_valuation,
    upsert_market_target_metrics,
    get_portfolio_tags,
    get_portfolio_tag_suggestions,
    set_portfolio_tags,
    get_trailing_dividends,
    upsert_preferred_dividends,
    get_preferred_dividends_count,
    list_preferred_dividends,
)
from repositories.portfolio import (  # noqa: E402,F811
    get_portfolio_item,
    update_portfolio_quantity,
    add_portfolio_item,
    save_portfolio_item,
    clear_portfolio,
    replace_portfolio,
    delete_portfolio_item,
    update_portfolio_benchmark,
    save_portfolio_order,
    get_portfolio_groups,
    add_portfolio_group,
    rename_portfolio_group,
    delete_portfolio_group,
    save_portfolio_groups_order,
)
from repositories.snapshots import (  # noqa: E402
    CashflowBalanceError,
    get_latest_snapshot,
    get_snapshot_by_date,
    get_latest_snapshot_before_date,
    save_snapshot,
    get_month_end_snapshot,
    get_year_start_snapshot,
    get_nav_history,
    get_group_weight_history,
    get_group_constituent_history,
    get_cashflows,
    add_cashflow,
    add_cashflow_and_sync_cash,
    delete_cashflow_and_sync_cash,
    get_cashflow,
    delete_cashflow,
    get_all_users_with_portfolio,
    get_pending_cashflows,
    save_stock_snapshots,
    get_stock_snapshots_by_date,
    get_stock_snapshots_before_date,
    save_intraday_snapshot,
    get_intraday_snapshots,
    get_intraday_snapshots_between,
    delete_old_intraday,
)
from repositories.app_settings import (  # noqa: E402
    get_app_setting,
    set_app_setting,
    delete_app_setting,
)
from repositories.ai_usage import (  # noqa: E402
    insert_ai_usage_event,
    summarize_ai_usage,
)
from repositories.market_brief import (  # noqa: E402
    get_daily_market_brief,
    save_daily_market_brief,
)
from repositories.insight_posts import (  # noqa: E402
    create_insight_post,
    list_insight_posts,
    get_insight_post,
    delete_insight_post,
)
from repositories.financial import (  # noqa: E402
    save_financial_data,
    get_financial_data,
    save_market_data,
    upsert_market_dividends,
    get_market_data,
    get_latest_dividend_years,
)
from repositories.analysis import (  # noqa: E402
    save_analysis_meta,
    save_analysis_snapshot,
    get_analysis_meta,
    get_analysis_snapshot,
    delete_analysis,
)
from repositories.users import (  # noqa: E402
    upsert_user,
    create_user_session,
    get_user_by_session,
    delete_user_session,
    delete_expired_sessions,
    get_all_users,
)
from repositories.user_stocks import (  # noqa: E402
    touch_user_recent_analysis,
    delete_user_recent_analysis,
    get_user_stock_preference,
    save_user_stock_preference,
    get_cached_analyses,
    save_user_stock_order,
    save_starred_order,
    unstar_stock,
)
from repositories.notifications import (  # noqa: E402
    get_notification_channel,
    list_notification_channels,
    upsert_notification_channel,
    set_notification_channel_enabled,
    delete_notification_channel,
    create_notification_link,
    pop_notification_link,
    delete_expired_notification_links,
    list_portfolio_alerts,
    get_portfolio_alert,
    create_portfolio_alert,
    update_portfolio_alert,
    delete_portfolio_alert,
    set_portfolio_alert_state,
)
