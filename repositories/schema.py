"""Schema maintenance helpers shared across init_db / migrations.

cache.py 가 소유하던 스키마 보조 함수 중 동적 SQL 이 포함된 것을 분리해
(1) 식별자 allowlist 검증을 명시적으로 두고, (2) 향후 init_db 의 스키마
소유권을 이 모듈로 점진 이관할 수 있는 기반을 만든다.

보안 노트: ``_ensure_column`` 의 동적 SQL 은 테이블/컬럼 식별자를 문자열
보간으로 넣는다. 이 값들은 내부 하드코딩 상수(이 모듈과 cache.py init_db
에서만 호출)이므로 외부 입력이 들어오지 않아 SQL injection 경로는 없다.
그럼에도 식별자를 allowlist 로 제한해 "실수로 외부 값이 들어와도 거부"되게
만든다 — 방어적 품질 향상(ST-01 보조).
"""

from __future__ import annotations

import re

import aiosqlite

from cache_layer import CACHE_NS_LATEST_REPORT, CACHE_NS_REPORT_LIST

ColumnSpec = tuple[str, str, str]

# SQLite 식별자 허용 패턴: ASCII 문자/숫자/밑줄. 한글 식별자는 쓰지 않으므로
# 이 패턴을 벗어나면 ValueError 로 거부한다. 테이블·컬럼명 모두에 적용.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

CORE_SCHEMA_SQL = """
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
"""


async def create_core_schema(db: aiosqlite.Connection) -> None:
    """Create the core application tables and indexes if they do not exist."""
    await db.executescript(CORE_SCHEMA_SQL)

# Columns that were added after the original CREATE TABLE definitions. Keeping
# the list here makes init_db's migration surface explicit while the larger
# table-creation script is still being split out of cache.py.
CORE_COLUMN_MIGRATIONS: tuple[ColumnSpec, ...] = (
    ("corp_codes", "modify_date", "TEXT"),
    ("financial_data", "report_date", "TEXT"),
    ("market_data", "dividend_per_share", "REAL"),
    ("analysis_meta", "payload_json", "TEXT"),
    ("user_stock_preferences", "sort_order", "INTEGER"),
    ("user_stock_preferences", "starred_order", "INTEGER"),
    ("user_portfolio", "currency", "TEXT DEFAULT 'KRW'"),
    ("user_portfolio", "avg_price_currency", "TEXT NOT NULL DEFAULT 'KRW'"),
    ("user_portfolio", "group_name", "TEXT"),
    ("user_portfolio", "benchmark_code", "TEXT"),
    # Manual target override fields.
    ("user_portfolio", "target_price", "REAL"),
    ("user_portfolio", "target_price_disabled", "INTEGER NOT NULL DEFAULT 0"),
    ("user_portfolio", "target_price_formula", "TEXT"),
    ("users", "is_admin", "INTEGER NOT NULL DEFAULT 0"),
    ("users", "password_hash", "TEXT"),
    ("users", "password_updated_at", "TEXT"),
    ("users", "google_identity_sub", "TEXT"),
    # Notification edge-trigger and priority flags.
    ("portfolio_alerts", "state_json", "TEXT NOT NULL DEFAULT '{}'"),
    ("portfolio_alerts", "important", "INTEGER NOT NULL DEFAULT 0"),
    ("portfolio_snapshots", "fx_usdkrw", "REAL"),
    ("portfolio_stock_snapshots", "group_name", "TEXT"),
    ("portfolio_stock_snapshots", "quantity", "REAL"),
    ("portfolio_stock_snapshots", "unit_price", "REAL"),
    ("portfolio_stock_snapshots", "avg_price_krw", "REAL"),
    ("portfolio_stock_snapshots", "cost_basis", "REAL"),
    ("portfolio_groups", "default_type", "TEXT"),
    # Multi-account phase 1. Nullable by design; uniqueness across
    # (google_sub, account_id, stock_code) is deferred to a later table rebuild.
    ("user_portfolio", "account_id", "TEXT"),
)


def _validate_identifier(value: str, *, kind: str) -> str:
    """동적 SQL 에 들어갈 식별자를 allowlist 패턴으로 검증한다.

    허용 패턴을 벗어나면 ValueError — 이 함수의 호출부는 내부 상수만 쓰므로
    정상이라면 절대 발생하지 않는다. 외부 입력이 섞여 들어오는 실수를
    컴파일 타임에 가깝게 막기 위한 방어막.
    """
    if not isinstance(value, str) or not _IDENTIFIER_RE.match(value):
        raise ValueError(f"invalid {kind} identifier: {value!r}")
    return value


async def ensure_column(db: aiosqlite.Connection, table: str, column: str, definition: str) -> None:
    """Add ``column`` to ``table`` if it does not already exist.

    ``table``/``column`` are internal constants; both are validated against an
    identifier allowlist before interpolation (defense-in-depth — no external
    input reaches here). ``definition`` is a type/default fragment like
    ``"TEXT NOT NULL DEFAULT '{}'"`` and is also part of DDL, so it is passed
    through as-is by design (call sites are all hardcoded).

    ``PRAGMA table_info`` 는 각 행이 (cid, name, type, notnull, dflt_value, pk)
    순서의 튜플 또는 Row. row_factory 설정과 무관하게 동작하도록 name 은
    인덱스 1 로 추출한다(aiosqlite Row 도 인덱스 접근 지원).
    """
    table = _validate_identifier(table, kind="table")
    column = _validate_identifier(column, kind="column")
    cursor = await db.execute(f"PRAGMA table_info({table})")
    rows = await cursor.fetchall()
    existing = {row[1] for row in rows}  # row[1] == name
    if column not in existing:
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


async def ensure_columns(db: aiosqlite.Connection, columns: tuple[ColumnSpec, ...]) -> None:
    """Apply a sequence of idempotent ADD COLUMN migrations."""
    for table, column, definition in columns:
        await ensure_column(db, table, column, definition)


async def apply_core_column_migrations(db: aiosqlite.Connection) -> None:
    """Apply init_db's legacy ADD COLUMN migrations."""
    await ensure_columns(db, CORE_COLUMN_MIGRATIONS)


async def backfill_legacy_cache_values(db: aiosqlite.Connection) -> None:
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
