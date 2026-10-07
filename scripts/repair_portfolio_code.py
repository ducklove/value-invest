"""Rename a stored Korean Yahoo alias without changing holdings or their history.

Run with --apply after inspecting the default dry run. An online SQLite backup
is saved under .deploy-repairs before the transaction starts. Duplicate holdings
or conflicting historical keys abort the transaction instead of merging values.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path

RELATED_TABLES = (
    "portfolio_tags", "portfolio_stock_snapshots", "portfolio_stock_weight_snapshots",
    "user_stock_preferences", "portfolio_alerts", "investment_journal", "investment_theses",
    "portfolio_income_events", "portfolio_trades", "portfolio_dividend_receipts",
)


def _copy_code(conn: sqlite3.Connection, table: str, user: str, old: str, new: str) -> int:
    columns = [row[1] for row in conn.execute(f"PRAGMA table_info({table})")]
    fields = ",".join(f'"{column}"' for column in columns)
    values = ",".join("?" if column == "stock_code" else f'"{column}"' for column in columns)
    return conn.execute(
        f"INSERT INTO {table} ({fields}) SELECT {values} FROM {table} WHERE google_sub=? AND stock_code=?",
        (new, user, old),
    ).rowcount


def repair_code(conn: sqlite3.Connection, user: str, old: str, new: str) -> dict[str, int]:
    if not re.fullmatch(r"[0-9][0-9A-Z]{5}\.(KS|KQ)", old) or new != old[:-3]:
        raise ValueError("Only a Korean Yahoo suffix may be removed.")
    if not conn.execute("SELECT 1 FROM user_portfolio WHERE google_sub=? AND stock_code=?", (user, old)).fetchone():
        return {}
    if conn.execute("SELECT 1 FROM user_portfolio WHERE google_sub=? AND stock_code=?", (user, new)).fetchone():
        raise ValueError("The native code already exists; holdings were not merged.")
    conn.execute("SAVEPOINT rename_portfolio_code")
    try:
        changed = {"user_portfolio": _copy_code(conn, "user_portfolio", user, old, new)}
        changed["account_holdings"] = _copy_code(conn, "account_holdings", user, old, new)
        for table in RELATED_TABLES:
            changed[table] = conn.execute(
                f"UPDATE {table} SET stock_code=? WHERE google_sub=? AND stock_code=?", (new, user, old),
            ).rowcount
        for column in ("pair_long_code", "benchmark_code"):
            conn.execute(f"UPDATE user_portfolio SET {column}=? WHERE google_sub=? AND {column}=?", (new, user, old))
        # INSERT + DELETE also records a tombstone for the old settlement row
        # key. Updating the primary key alone would leave both holdings active
        # when settlement_versions is replayed after this repair.
        for table in ("account_holdings", "user_portfolio"):
            conn.execute(f"DELETE FROM {table} WHERE google_sub=? AND stock_code=?", (user, old))
        conn.execute("RELEASE rename_portfolio_code")
        return {table: count for table, count in changed.items() if count}
    except BaseException:
        conn.execute("ROLLBACK TO rename_portfolio_code")
        conn.execute("RELEASE rename_portfolio_code")
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=Path("cache.db"))
    parser.add_argument("--from-code", required=True)
    parser.add_argument("--to-code", required=True)
    parser.add_argument("--user", help="Limit the repair to this google_sub.")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    with sqlite3.connect(f"file:{args.db.resolve()}?mode=rw", uri=True, timeout=30) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        users = [row[0] for row in conn.execute(
            "SELECT google_sub FROM user_portfolio WHERE stock_code=? AND (? IS NULL OR google_sub=?)",
            (args.from_code, args.user, args.user),
        )]
        print(json.dumps({"from": args.from_code, "to": args.to_code, "holdings": len(users), "apply": args.apply}))
        if not args.apply or not users:
            return
        backup_dir = args.db.resolve().parent / ".deploy-repairs"
        backup_dir.mkdir(mode=0o700, exist_ok=True)
        backup = backup_dir / f"portfolio-code-{datetime.now():%Y%m%dT%H%M%S%f}.db"
        backup.touch(mode=0o600)
        with sqlite3.connect(backup) as destination:
            conn.backup(destination)
        print(json.dumps({"backup": str(backup)}))
        conn.execute("BEGIN IMMEDIATE")
        for user in users:
            print(json.dumps({"changed": repair_code(conn, user, args.from_code, args.to_code)}))
        conn.commit()


if __name__ == "__main__":
    main()
