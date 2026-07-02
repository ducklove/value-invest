import json
from datetime import datetime

import aiosqlite

from cache_layer import (
    CACHE_NS_LATEST_REPORT,
    CACHE_NS_REPORT_LIST,
    CacheEntry,
    expires_at_for,
    parse_iso,
)
from repositories import db as _db
from repositories import schema as schema_repo

# 커넥션 싱글톤의 원본은 repositories/db.py 로 이동했다. 아래는 기존
# ``cache.get_db()`` / ``cache.DB_PATH`` 호출부(레거시 모듈, deploy.sh
# repair 스크립트)를 위한 얇은 재수출. 주의: DB_PATH 는 읽기 전용 별칭
# 이라 테스트/스크립트가 경로를 바꾸려면 repositories.db.DB_PATH 를
# 패치해야 한다 — get_db() 가 호출 시점에 그쪽 전역을 읽는다.
DB_PATH = _db.DB_PATH
get_db = _db.get_db
transaction = _db.transaction

_corp_code_table: dict[str, dict[str, str]] | None = None


async def close_db():
    """Shutdown: close the shared connection (+ cache.py 의 메모리 테이블 리셋)."""
    global _corp_code_table
    await _db.close_db()
    _corp_code_table = None


async def init_db():
    # Migration-only repository helpers (one-time backfills below). Imported
    # locally so cache.py's module surface stays free of repository names.
    from repositories.portfolio import _ensure_default_groups
    from repositories.snapshots import _refresh_group_snapshots, _refresh_stock_weight_snapshots

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
            avg_price_currency TEXT NOT NULL DEFAULT 'KRW',
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
            quantity REAL,
            unit_price REAL,
            avg_price_krw REAL,
            cost_basis REAL,
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

        -- DEPRECATED: 국민연금 데이터는 nps-tracker(별도 정적 대시보드)로 분리됐다.
        -- 아래 두 테이블은 더 이상 기록되지 않으며, 과거 데이터 보존을 위해 drop만
        -- 보류한 상태다(롤백 안전장치). 새 코드는 참조하지 말 것.
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

        CREATE TABLE IF NOT EXISTS portfolio_period_reports (
            google_sub TEXT NOT NULL,
            period_type TEXT NOT NULL,
            period_key TEXT NOT NULL,
            schema_version INTEGER NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            baseline_date TEXT,
            report_json TEXT NOT NULL,
            report_md TEXT NOT NULL DEFAULT '',
            source_hash TEXT NOT NULL,
            generated_at TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (google_sub, period_type, period_key),
            FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_portfolio_period_reports_user_updated
            ON portfolio_period_reports(google_sub, updated_at DESC);

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
            important INTEGER NOT NULL DEFAULT 0,
            armed INTEGER NOT NULL DEFAULT 1,
            last_triggered_at TEXT,
            last_value REAL,
            state_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_portfolio_alerts_user ON portfolio_alerts(google_sub, enabled);

        -- Physical-delivery de-duplication. Alert state is per user/rule, but
        -- migrated or duplicate accounts can point at the same real chat/app.
        -- This suppresses the same semantic alert to the same target per day.
        CREATE TABLE IF NOT EXISTS notification_delivery_dedupe (
            channel TEXT NOT NULL,
            target_key TEXT NOT NULL,
            dedupe_key TEXT NOT NULL,
            sent_date TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (channel, target_key, dedupe_key, sent_date)
        );

        -- 경제캘린더 이벤트 구독: 사용자가 특정 지표 발표(zeroin index_id)에 대해
        -- '결과치(actual) 발표 시 알림'을 신청한 것. fired=1 로 엣지 트리거(결과
        -- 발표 시 1회만 발송). event_date 는 폴링 윈도우·스테일 정리에 쓴다.
        CREATE TABLE IF NOT EXISTS economic_calendar_subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            google_sub TEXT NOT NULL,
            event_id TEXT NOT NULL,
            event_date TEXT NOT NULL,
            event_datetime TEXT,
            country TEXT,
            country_name TEXT,
            event TEXT,
            importance TEXT,
            forecast TEXT,
            previous TEXT,
            fired INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(google_sub, event_id),
            FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_econ_cal_subs_pending ON economic_calendar_subscriptions(fired, event_date);

        -- 리밸런싱 목표 비중: scope='stock'(target_key=종목코드) 또는
        -- scope='group'(target_key=그룹명) 단위 목표 비중(%)과 드리프트 허용
        -- 오차(%p). CRUD 는 repositories/rebalance_targets.py 소유.
        CREATE TABLE IF NOT EXISTS rebalance_targets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            google_sub TEXT NOT NULL,
            scope TEXT NOT NULL,
            target_key TEXT NOT NULL,
            target_weight_pct REAL NOT NULL,
            tolerance_pct REAL NOT NULL DEFAULT 5.0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(google_sub, scope, target_key),
            FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_rebalance_targets_user ON rebalance_targets(google_sub);

        -- 투자 일지: 매수/매도/메모 판단의 '이유'를 기록하고 나중에 결과와
        -- 함께 복기한다. price_at_entry/target_price_at_entry 는 작성 시점
        -- 스냅샷(없으면 NULL). 항목은 append-only — note 만 수정 가능, 삭제
        -- 허용. CRUD 는 repositories/journal.py 소유.
        CREATE TABLE IF NOT EXISTS investment_journal (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            google_sub TEXT NOT NULL,
            stock_code TEXT NOT NULL,
            stock_name TEXT,
            entry_type TEXT NOT NULL,
            note TEXT NOT NULL,
            price_at_entry REAL,
            quantity REAL,
            target_price_at_entry REAL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_investment_journal_user_stock ON investment_journal(google_sub, stock_code);

        -- 포트폴리오 액션 보드 검토 큐: 서버가 매번 재계산하는 액션
        -- (리밸런싱 이탈, 연결 프로젝트 신호 등)에 대해 사용자가
        -- 처리완료/무시/다시검토 상태를 남긴다. 액션 본문은 파생 데이터라
        -- 저장하지 않고 안정 키만 보관한다.
        CREATE TABLE IF NOT EXISTS portfolio_action_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            google_sub TEXT NOT NULL,
            action_key TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'open',
            note TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(google_sub, action_key),
            FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_portfolio_action_reviews_user_status
            ON portfolio_action_reviews(google_sub, status, updated_at DESC);

        CREATE TABLE IF NOT EXISTS portfolio_accounts (
            account_id TEXT PRIMARY KEY,
            google_sub TEXT NOT NULL,
            name TEXT NOT NULL,
            type TEXT NOT NULL DEFAULT 'general',
            sort_order INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(google_sub, name),
            FOREIGN KEY (google_sub) REFERENCES users(google_sub) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_portfolio_accounts_user
            ON portfolio_accounts(google_sub, sort_order);
    """)
    await schema_repo.apply_core_column_migrations(db)
    await db.execute(
        """
        UPDATE users
        SET google_identity_sub = google_sub
        WHERE google_identity_sub IS NULL
          AND google_sub NOT LIKE 'local:%'
        """
    )
    await db.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_google_identity_sub "
        "ON users(google_identity_sub) WHERE google_identity_sub IS NOT NULL"
    )
    await db.execute("CREATE INDEX IF NOT EXISTS idx_users_email_lower ON users(lower(email))")
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
        # Multi-account phase 1: ensure each user has a "default" account and all
        # holdings point at it. Idempotent — skips users who already have an
        # account row. The default account is the single bucket until the user
        # creates more; existing queries (WHERE google_sub = ?) keep working
        # unchanged because account_id is nullable and out of the PK.
        await _ensure_default_account(db, sub)
        await db.execute("""
            UPDATE user_portfolio SET group_name = '기타'
            WHERE google_sub = ? AND group_name IS NULL AND stock_code IN ('KRX_GOLD', 'CRYPTO_BTC', 'CRYPTO_ETH', 'CRYPTO_USDT')
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
    # 구현은 repositories.schema.ensure_column 으로 이관(ST-01). 동작은 동일하되
    # 식별자 allowlist 검증이 추가됐다 — 호출부는 모두 하드코딩 상수이므로 정상
    # 경로에선 영향 없고, 외부 값이 섞여 들어오는 실수를 방어한다.
    from repositories.schema import ensure_column

    await ensure_column(db, table, column, definition)


# 멀티계좌 phase 1 default 계좌 account_id 접두사. 사용자별 고유 id 는
# google_sub 해시 접미사로 만들어 전역 충돌을 피한다.
_DEFAULT_ACCOUNT_PREFIX = "default"


async def _ensure_default_account(db: aiosqlite.Connection, google_sub: str) -> None:
    """사용자에게 기본 계좌 하나를 보장하고, account_id 가 비어 있는 보유
    종목을 모두 그 계좌로 귀속시킨다. 멱등 — 이미 계좌가 있으면 건너뛴다.

    account_id 는 nullable 이고 PK 에서 빠져 있으므로, 이 백핀이 끝나면
    모든 기존 쿼리(WHERE google_sub = ?)가 account_id 없이도 그대로 동작한다.
    """
    import hashlib

    from cache_layer import now_iso

    cursor = await db.execute(
        "SELECT account_id FROM portfolio_accounts WHERE google_sub = ? ORDER BY sort_order LIMIT 1",
        (google_sub,),
    )
    row = await cursor.fetchone()
    if row is not None:
        account_id = row["account_id"]
    else:
        suffix = hashlib.sha256(google_sub.encode("utf-8")).hexdigest()[:12]
        account_id = f"{_DEFAULT_ACCOUNT_PREFIX}-{suffix}"
        now = now_iso()
        await db.execute(
            "INSERT OR IGNORE INTO portfolio_accounts "
            "(account_id, google_sub, name, type, sort_order, created_at, updated_at) "
            "VALUES (?, ?, ?, 'general', 0, ?, ?)",
            (account_id, google_sub, "기본 계좌", now, now),
        )
    # account_id 가 NULL 인 보유 종목을 이 사용자의 default 계좌로 귀속.
    await db.execute(
        "UPDATE user_portfolio SET account_id = ? WHERE google_sub = ? AND account_id IS NULL",
        (account_id, google_sub),
    )


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


async def resolve_stock_name(stock_code: str) -> str | None:
    name = await get_corp_name(stock_code)
    if name:
        return name
    return None


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
    db_size = os.path.getsize(_db.DB_PATH) if _db.DB_PATH.exists() else 0
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
