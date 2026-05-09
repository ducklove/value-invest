import asyncio
import importlib.util
import sqlite3
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "import_portfolio_db.py"
spec = importlib.util.spec_from_file_location("import_portfolio_db", SCRIPT_PATH)
import_portfolio_db = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(import_portfolio_db)


def _create_source_db(path: Path) -> None:
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE users (
                google_sub TEXT PRIMARY KEY,
                email TEXT NOT NULL,
                name TEXT NOT NULL,
                picture TEXT,
                email_verified INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                last_login_at TEXT NOT NULL,
                is_admin INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE user_sessions (
                token_hash TEXT PRIMARY KEY,
                google_sub TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            );
            CREATE TABLE user_portfolio (
                google_sub TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                stock_name TEXT NOT NULL,
                quantity REAL NOT NULL,
                avg_price REAL NOT NULL,
                sort_order INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                currency TEXT,
                group_name TEXT,
                benchmark_code TEXT,
                target_price REAL,
                target_price_disabled INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (google_sub, stock_code)
            );
            CREATE TABLE portfolio_groups (
                google_sub TEXT NOT NULL,
                group_name TEXT NOT NULL,
                sort_order INTEGER DEFAULT 0,
                is_default INTEGER DEFAULT 0,
                default_type TEXT,
                PRIMARY KEY (google_sub, group_name)
            );
            CREATE TABLE portfolio_tags (
                google_sub TEXT NOT NULL,
                stock_code TEXT NOT NULL,
                tag TEXT NOT NULL,
                sort_order INTEGER,
                created_at TEXT NOT NULL,
                PRIMARY KEY (google_sub, stock_code, tag)
            );
            CREATE TABLE portfolio_cashflows (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                google_sub TEXT NOT NULL,
                date TEXT NOT NULL,
                type TEXT NOT NULL,
                amount REAL NOT NULL,
                nav_at_time REAL NOT NULL,
                units_change REAL NOT NULL,
                memo TEXT,
                created_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT INTO users VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("prod-sub", "prod@example.com", "Prod User", "", 1, "2026-01-01", "2026-05-09", 1),
        )
        conn.execute(
            "INSERT INTO user_sessions VALUES (?, ?, ?, ?)",
            ("secret-session", "prod-sub", "2026-05-09", "2026-06-09"),
        )
        conn.execute(
            """
            INSERT INTO user_portfolio
            (google_sub, stock_code, stock_name, quantity, avg_price, sort_order, created_at, updated_at, currency, group_name, benchmark_code, target_price, target_price_disabled)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("prod-sub", "005930", "삼성전자", 3, 70000, 0, "2026-01-01", "2026-05-09", "KRW", "국내", "IDX_KOSPI", None, 0),
        )
        conn.execute(
            "INSERT INTO portfolio_groups VALUES (?, ?, ?, ?, ?)",
            ("prod-sub", "국내", 0, 0, None),
        )
        conn.execute(
            "INSERT INTO portfolio_tags VALUES (?, ?, ?, ?, ?)",
            ("prod-sub", "005930", "AI", 0, "2026-05-09"),
        )
        conn.execute(
            "INSERT INTO portfolio_cashflows (google_sub, date, type, amount, nav_at_time, units_change, memo, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("prod-sub", "2026-05-09", "deposit", 1000000, 1000, 1000, "seed", "2026-05-09"),
        )


def test_import_portfolio_rows_copies_portfolio_but_not_sessions(tmp_path):
    source = tmp_path / "prod.db"
    target = tmp_path / "local.db"
    _create_source_db(source)
    asyncio.run(import_portfolio_db._ensure_target_schema(target))

    counts = import_portfolio_db.import_portfolio_rows(
        source,
        target,
        source_email="prod@example.com",
        dest_google_sub="local-sub",
        dest_email="local@example.com",
        dest_name="Local User",
        backup=False,
    )

    assert counts["user_portfolio"] == 1
    assert counts["portfolio_tags"] == 1
    assert counts["portfolio_cashflows"] == 1

    with sqlite3.connect(target) as conn:
        conn.row_factory = sqlite3.Row
        user = conn.execute("SELECT google_sub, email, name FROM users WHERE google_sub = 'local-sub'").fetchone()
        assert dict(user) == {"google_sub": "local-sub", "email": "local@example.com", "name": "Local User"}

        item = conn.execute("SELECT google_sub, stock_code, stock_name FROM user_portfolio").fetchone()
        assert dict(item) == {"google_sub": "local-sub", "stock_code": "005930", "stock_name": "삼성전자"}

        assert conn.execute("SELECT COUNT(*) FROM portfolio_tags WHERE google_sub = 'local-sub'").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM portfolio_cashflows WHERE google_sub = 'local-sub'").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM user_sessions").fetchone()[0] == 0
