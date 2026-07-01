#!/usr/bin/env bash
# Deploy script executed by the self-hosted GitHub Actions runner on the
# home server. Runs as the `cantabile` user. `sudoers` must allow:
#   cantabile ALL=(root) NOPASSWD: /bin/systemctl, /usr/bin/cp, /bin/cp
# The runner's workspace is unused; we operate directly on the production
# checkout at $APP_DIR so that systemd keeps pointing at the same path.

set -euo pipefail

APP_DIR="${APP_DIR:-/home/cantabile/Works/value_invest}"
SERVICE="${SERVICE:-value-invest.service}"
HEALTH_URL="${HEALTH_URL:-https://127.0.0.1:3691/healthz}"
UNIT_DST="${UNIT_DST:-/etc/systemd/system}"

# Units maintained in-repo. Keep value-invest.service last so the main app
# restart picks up any dependency timer changes first.
REPO_UNITS=(
  "value-invest-notify@.service"
  "portfolio-intraday.service"
  "portfolio-intraday.timer"
  "portfolio-snapshot.service"
  "portfolio-snapshot.timer"
  "value-invest-backup.service"
  "value-invest-backup.timer"
  "dart-review-ingestion.service"
  "dart-review-ingestion.timer"
  "wiki-ingestion.service"
  "wiki-ingestion.timer"
  "notify-alerts.service"
  "notify-alerts.timer"
  "notify-calendar.service"
  "notify-calendar.timer"
  "data-quality.service"
  "data-quality.timer"
  "daily-briefing.service"
  "daily-briefing.timer"
  "daily-briefing-market-close.service"
  "daily-briefing-market-close.timer"
  "daily-briefing-night.service"
  "daily-briefing-night.timer"
  "deploy/value-invest.service"
)

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

wait_for_healthz() {
  # -k: cert is for cantabile.tplinkdns.com; localhost check skips name match.
  log "Waiting for healthz"
  for i in {1..20}; do
    if curl -fsSk --max-time 2 "$HEALTH_URL" >/dev/null 2>&1; then
      log "Healthz OK (attempt $i)"
      return 0
    fi
    sleep 1
  done

  log "Healthz did not respond in time; showing recent logs"
  sudo /bin/systemctl --no-pager status "$SERVICE" || true
  return 1
}

cd "$APP_DIR"

log "Fetching latest from origin/master"
git fetch --prune origin
OLD_SHA="$(git rev-parse HEAD)"
git reset --hard origin/master
NEW_SHA="$(git rev-parse HEAD)"

if [[ "$OLD_SHA" == "$NEW_SHA" ]]; then
  log "No new commits ($NEW_SHA). Restarting $SERVICE so the process matches the checkout."
  sudo /bin/systemctl restart "$SERVICE"
  wait_for_healthz
  exit 0
fi

log "Deploying $OLD_SHA -> $NEW_SHA"
git --no-pager log --oneline "$OLD_SHA..$NEW_SHA"

CHANGED_FILES="$(git diff --name-only "$OLD_SHA" "$NEW_SHA")"

# Roll the checkout back to OLD_SHA if anything below fails, so the running
# service keeps matching what's on disk if it restarts for any reason.
rollback() {
  log "Deploy failed — rolling back to $OLD_SHA"
  git reset --hard "$OLD_SHA" >/dev/null
}
trap rollback ERR

# pip flags — Raspberry Pi OS Bookworm enforces PEP 668, so we need
# --break-system-packages to install into the user site where the running
# service (system python3) already picks deps up from.
PIP_FLAGS=(--user --break-system-packages)

# --- Python deps (only if requirements changed) -----------------------------
if grep -qE '^requirements(-dev)?\.txt$' <<<"$CHANGED_FILES"; then
  log "requirements changed — installing"
  python3 -m pip install "${PIP_FLAGS[@]}" --upgrade -r requirements.txt
  # Log resolved versions so drift between machines/days is visible in CI.
  log "installed versions:"
  python3 -m pip list 2>/dev/null | grep -iE '^(fastapi|uvicorn|aiosqlite|httpx|beautifulsoup4|yfinance|google-auth|python-dotenv|websockets)\b' || true
fi

# --- Lint -------------------------------------------------------------------
# Blocking like the tests below. The ruleset lives in pyproject.toml and is
# intentionally conservative (real defects, not style churn); widen it there.
log "Installing dev dependencies"
python3 -m pip install "${PIP_FLAGS[@]}" --quiet -r requirements-dev.txt
log "Running ruff"
python3 -m ruff check .

# --- Tests ------------------------------------------------------------------
# Blocking: a test failure aborts the deploy via the ERR trap, which rolls
# the checkout back to OLD_SHA. The previous non-blocking behaviour was a
# stopgap while the `_conn` singleton leak in the test fixtures made 32
# tests spuriously fail; that's been fixed.
log "Running tests"
python3 -m pytest -q

# --- JS tests ----------------------------------------------------------------
# Blocking when node is available: the jsdom behaviour tests are the growing
# replacement for the Python string-presence checks, so a red run must stop
# the deploy exactly like pytest. If the runner has no node yet, warn loudly
# instead of bricking deploys — install node to turn this into a hard gate.
if command -v node >/dev/null 2>&1 && command -v npm >/dev/null 2>&1; then
  if [[ ! -d node_modules ]] || grep -qE '^package(-lock)?\.json$' <<<"$CHANGED_FILES"; then
    log "Installing JS dev dependencies (npm ci)"
    npm ci --no-audit --no-fund
  fi
  log "Running JS tests"
  npm test
else
  log "WARNING: node/npm not found on runner — JS tests SKIPPED. Install node to enforce this gate."
fi

# Past this point, rolling the checkout back would desync from a restarted
# service, so clear the trap. The health check below has its own explicit
# rollback path instead.
trap - ERR

# --- Retire units no longer maintained in-repo ------------------------------
# Deleting a .timer/.service from the repo doesn't remove an already-installed,
# enabled copy on the host. Explicitly stop+disable+delete retired units so a
# stale timer can't keep POSTing a route that no longer exists (e.g. the NPS
# snapshot timer after the nps-tracker split). Idempotent: a no-op once gone.
RETIRED_UNITS=(
  "nps-snapshot.timer"
  "nps-snapshot.service"
)
RETIRED_ANY=0
for dead in "${RETIRED_UNITS[@]}"; do
  if [[ -f "$UNIT_DST/$dead" ]]; then
    log "Retiring unit: $dead"
    sudo /bin/systemctl disable --now "$dead" 2>/dev/null || true
    sudo rm -f "$UNIT_DST/$dead"
    RETIRED_ANY=1
  fi
done
(( RETIRED_ANY )) && sudo /bin/systemctl daemon-reload

# --- systemd unit sync ------------------------------------------------------
UNITS_TO_RELOAD=()
for src in "${REPO_UNITS[@]}"; do
  [[ -f "$src" ]] || continue
  dst="$UNIT_DST/$(basename "$src")"
  if ! sudo cmp -s "$src" "$dst" 2>/dev/null; then
    log "Unit changed: $(basename "$src")"
    sudo cp "$src" "$dst"
    UNITS_TO_RELOAD+=("$(basename "$src")")
  fi
done

if (( ${#UNITS_TO_RELOAD[@]} > 0 )); then
  log "daemon-reload"
  sudo /bin/systemctl daemon-reload
  for unit in "${UNITS_TO_RELOAD[@]}"; do
    # Re-enable timers so changed schedules take effect
    if [[ "$unit" == *.timer ]]; then
      sudo /bin/systemctl enable --now "$unit"
    fi
  done
fi

# --- Restart main app service ----------------------------------------------
log "Restarting $SERVICE"
sudo /bin/systemctl restart "$SERVICE"

# --- Health check -----------------------------------------------------------
# Blocking: if the new code doesn't come up healthy, roll the checkout back
# to OLD_SHA and restart so the service returns to the last good state
# instead of staying down. pip deps upgraded above are left in place — the
# version ranges that satisfied OLD_SHA still apply, and downgrading live
# site-packages mid-incident is riskier than leaving them.
if ! wait_for_healthz; then
  log "Healthz failed on $NEW_SHA — rolling back to $OLD_SHA and restarting"
  git reset --hard "$OLD_SHA"
  sudo /bin/systemctl restart "$SERVICE"
  wait_for_healthz || log "Healthz still failing after rollback — manual intervention required"
  exit 1
fi

# --- One-time repair --------------------------------------------------------
# 2026-05-18 NAV was previously rerun through the live quote path. Rebuild it
# once with the corrected date-specific snapshot logic, then leave a local
# marker so future deploys do not keep rewriting that historical row.
REPAIR_MARKER=".deploy-repair-2026-05-18-nav-v3.done"
if [[ ! -f "$REPAIR_MARKER" ]]; then
  log "Repairing portfolio NAV snapshot for 2026-05-18"
  python3 snapshot_nav.py 2026-05-18
  touch "$REPAIR_MARKER"
fi

# 2026-05-27 17:00~20:30 KST intraday points were recorded while the quote
# refresh path could fall back to stale per-stock snapshot values for Korean
# holdings. Those rows make the Today sparkline falsely hover near 0 until
# the live point is appended. Remove only that known-bad window; later ticks
# are produced by the corrected strict intraday quote path.
REPAIR_MARKER=".deploy-repair-2026-05-27-intraday-stale-quotes.done"
if [[ ! -f "$REPAIR_MARKER" ]]; then
  log "Removing invalid 2026-05-27 intraday quote-fallback points"
  python3 - <<'PY'
import asyncio
import cache

async def main():
    await cache.init_db()
    db = await cache.get_db()
    await db.execute(
        "DELETE FROM portfolio_intraday WHERE ts >= ? AND ts < ?",
        ("2026-05-27T17:00", "2026-05-27T20:30"),
    )
    await db.commit()
    await cache.close_db()

asyncio.run(main())
PY
  touch "$REPAIR_MARKER"
fi

# Backfill the newly requested cantabile658 account on the production DB. The
# marker keeps this as a one-time data repair while preserving the exact script
# that was run in deploy logs/history.
REPAIR_MARKER=".deploy-repair-cantabile658-backfill-2026-06-v1.done"
if [[ ! -f "$REPAIR_MARKER" ]]; then
  log "Backfilling cantabile658@gmail.com portfolio snapshots from 2026-05-31"
  python3 - <<'PY'
import asyncio
import json
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

from core.config import load_environment

load_environment(Path.cwd(), force=True)

import cache
import snapshot_nav
from repositories import snapshots as snapshots_repo


TARGET_EMAIL = "cantabile658@gmail.com"
TARGET_LOCAL_SUB = "local:cantabile658-gmail-com"
SOURCE_EMAIL = "ducklv@gmail.com"
BASELINE_DATE = "2026-05-31"
BASELINE_CLOSE_DATE = "2026-05-29"
START_DATE = date(2026, 6, 1)
END_DATE = date(2026, 6, 18)
BASE_NAV = 1000.0
COPY_TABLES = (
    "portfolio_groups",
    "user_portfolio",
    "portfolio_tags",
    "user_stock_preferences",
)


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def backup_db() -> str:
    db_path = Path("cache.db").resolve()
    backup_dir = Path("data/db-imports")
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = backup_dir / f"cache.before-cantabile658-live-backfill.{stamp}.db"
    src = sqlite3.connect(db_path)
    dst = sqlite3.connect(backup_path)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    return str(backup_path)


async def fetchone(db, sql: str, params: tuple = ()) -> dict | None:
    cursor = await db.execute(sql, params)
    row = await cursor.fetchone()
    return dict(row) if row else None


async def fetchval(db, sql: str, params: tuple = ()):
    cursor = await db.execute(sql, params)
    row = await cursor.fetchone()
    if not row:
        return None
    return row[0]


async def table_columns(db, table: str) -> list[str]:
    cursor = await db.execute(f"PRAGMA table_info({quote_ident(table)})")
    return [row["name"] for row in await cursor.fetchall()]


async def ensure_target_user(db) -> tuple[str, bool]:
    now = datetime.now().isoformat(timespec="seconds")
    existing = await fetchone(
        db,
        "SELECT * FROM users WHERE lower(email) = lower(?) LIMIT 1",
        (TARGET_EMAIL,),
    )
    if existing:
        return existing["google_sub"], False

    existing_local = await fetchone(
        db,
        "SELECT * FROM users WHERE google_sub = ? LIMIT 1",
        (TARGET_LOCAL_SUB,),
    )
    if existing_local:
        if str(existing_local.get("email") or "").lower() != TARGET_EMAIL.lower():
            raise RuntimeError(f"{TARGET_LOCAL_SUB} already exists with another email")
        return existing_local["google_sub"], False

    columns = await table_columns(db, "users")
    values = {
        "google_sub": TARGET_LOCAL_SUB,
        "email": TARGET_EMAIL,
        "name": "cantabile658",
        "picture": None,
        "email_verified": 1,
        "created_at": now,
        "last_login_at": now,
        "is_admin": 0,
        "password_hash": None,
        "password_updated_at": None,
        "google_identity_sub": None,
    }
    insert_cols = [col for col in columns if col in values]
    placeholders = ",".join("?" for _ in insert_cols)
    await db.execute(
        f"INSERT INTO users ({','.join(quote_ident(c) for c in insert_cols)}) VALUES ({placeholders})",
        tuple(values[col] for col in insert_cols),
    )
    return TARGET_LOCAL_SUB, True


async def find_source_sub(db) -> str:
    source = await fetchone(
        db,
        "SELECT google_sub FROM users WHERE lower(email) = lower(?) LIMIT 1",
        (SOURCE_EMAIL,),
    )
    if source:
        return source["google_sub"]

    source = await fetchone(
        db,
        """
        SELECT google_sub
        FROM user_portfolio
        GROUP BY google_sub
        ORDER BY COUNT(*) DESC
        LIMIT 1
        """,
    )
    if not source:
        raise RuntimeError("No source portfolio exists to copy from")
    return source["google_sub"]


async def copy_table_for_user(db, table: str, source_sub: str, target_sub: str) -> int:
    columns = await table_columns(db, table)
    if "google_sub" not in columns:
        raise RuntimeError(f"{table} does not have google_sub")
    non_user_cols = [col for col in columns if col != "google_sub"]
    insert_cols = ["google_sub", *non_user_cols]
    select_sql = ", ".join(quote_ident(col) for col in non_user_cols)
    if select_sql:
        sql = (
            f"INSERT OR REPLACE INTO {quote_ident(table)} "
            f"({','.join(quote_ident(c) for c in insert_cols)}) "
            f"SELECT ?, {select_sql} FROM {quote_ident(table)} WHERE google_sub = ?"
        )
    else:
        sql = f"INSERT OR REPLACE INTO {quote_ident(table)} (google_sub) SELECT ? FROM {quote_ident(table)} WHERE google_sub = ?"
    cursor = await db.execute(sql, (target_sub, source_sub))
    return int(cursor.rowcount or 0)


async def clear_generated_snapshots(db, target_sub: str) -> None:
    await db.execute(
        "DELETE FROM portfolio_intraday WHERE google_sub = ? AND ts >= ?",
        (target_sub, BASELINE_DATE + "T00:00"),
    )
    for table in (
        "portfolio_stock_weight_snapshots",
        "portfolio_group_snapshots",
        "portfolio_stock_snapshots",
        "portfolio_snapshots",
    ):
        await db.execute(
            f"DELETE FROM {quote_ident(table)} WHERE google_sub = ? AND date >= ?",
            (target_sub, BASELINE_DATE),
        )


async def main() -> None:
    backup_path = backup_db()
    await cache.init_db()
    db = await cache.get_db()

    source_sub = await find_source_sub(db)
    target_sub, created_user = await ensure_target_user(db)
    source_holdings = int(await fetchval(db, "SELECT COUNT(*) FROM user_portfolio WHERE google_sub = ?", (source_sub,)) or 0)
    target_holdings = int(await fetchval(db, "SELECT COUNT(*) FROM user_portfolio WHERE google_sub = ?", (target_sub,)) or 0)
    copied = {}

    if target_holdings == 0:
        if source_holdings == 0:
            raise RuntimeError("Source portfolio has no holdings")
        await db.execute("BEGIN IMMEDIATE")
        try:
            for table in ("portfolio_tags", "user_stock_preferences", "portfolio_groups", "user_portfolio"):
                await db.execute(f"DELETE FROM {quote_ident(table)} WHERE google_sub = ?", (target_sub,))
            for table in COPY_TABLES:
                copied[table] = await copy_table_for_user(db, table, source_sub, target_sub)
            await db.commit()
        except Exception:
            await db.rollback()
            raise
        target_holdings = int(await fetchval(db, "SELECT COUNT(*) FROM user_portfolio WHERE google_sub = ?", (target_sub,)) or 0)

    if target_holdings == 0:
        raise RuntimeError("Target portfolio is empty after preparation")

    await clear_generated_snapshots(db, target_sub)
    await db.commit()

    await snapshot_nav._fetch_fx_usdkrw()
    total_value, total_invested, per_stock = await snapshot_nav._fetch_total_value(target_sub, BASELINE_CLOSE_DATE)
    if total_value <= 0:
        raise RuntimeError("Baseline total value is zero")
    total_units = total_value / BASE_NAV
    await snapshots_repo.save_snapshot(
        target_sub,
        BASELINE_DATE,
        total_value,
        total_invested,
        BASE_NAV,
        total_units,
        snapshot_nav._fx_usdkrw,
    )
    await snapshots_repo.save_stock_snapshots(target_sub, BASELINE_DATE, per_stock)

    written_dates = [BASELINE_DATE]
    current = START_DATE
    while current <= END_DATE:
        if current.weekday() < 5:
            snap_date = current.isoformat()
            await snapshot_nav.take_snapshot(target_sub, snap_date)
            written_dates.append(snap_date)
        current += timedelta(days=1)

    db = await cache.get_db()
    latest = await fetchone(
        db,
        """
        SELECT date, nav, total_value, total_units
        FROM portfolio_snapshots
        WHERE google_sub = ?
        ORDER BY date DESC
        LIMIT 1
        """,
        (target_sub,),
    )
    counts = {}
    for table in (
        "user_portfolio",
        "portfolio_snapshots",
        "portfolio_stock_snapshots",
        "portfolio_group_snapshots",
        "portfolio_stock_weight_snapshots",
    ):
        counts[table] = int(await fetchval(db, f"SELECT COUNT(*) FROM {quote_ident(table)} WHERE google_sub = ?", (target_sub,)) or 0)

    print(json.dumps({
        "ok": True,
        "backup_path": backup_path,
        "source_sub": source_sub,
        "target_sub": target_sub,
        "created_user": created_user,
        "source_holdings": source_holdings,
        "target_holdings": target_holdings,
        "copied": copied,
        "written_dates": written_dates,
        "latest": latest,
        "counts": counts,
    }, ensure_ascii=False, indent=2))
    await cache.close_db()


asyncio.run(main())
PY
  touch "$REPAIR_MARKER"
fi

# 2026-06-30 stock snapshots were created before per-stock quantity metadata
# was added to portfolio_stock_snapshots. The user's 2026-06-30 holdings match
# current quantities, so preserve the historical market_value rows and fill
# quantity/unit-price/cost-basis metadata from the current portfolio. This makes
# v2 period reports use quantity-delta composition changes instead of falling
# back to value-only changes for this boundary snapshot.
REPAIR_MARKER=".deploy-repair-2026-06-30-stock-quantity-metadata-v1.done"
if [[ ! -f "$REPAIR_MARKER" ]]; then
  log "Backfilling 2026-06-30 stock snapshot quantity metadata"
  python3 - <<'PY'
import asyncio
import json
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from core.config import load_environment

load_environment(Path.cwd(), force=True)

import cache
from services.portfolio import fx
from services.portfolio import period_reports

SNAP_DATE = "2026-06-30"
REPORT_PERIODS = (("monthly", "2026-06"),)


def backup_db() -> str:
    db_path = Path("cache.db").resolve()
    backup_dir = Path("data/db-imports")
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = backup_dir / f"cache.before-2026-06-30-quantity-backfill.{stamp}.db"
    src = sqlite3.connect(db_path)
    dst = sqlite3.connect(backup_path)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    return str(backup_path)


async def fetchall(db, sql: str, params: tuple = ()) -> list[dict]:
    cursor = await db.execute(sql, params)
    return [dict(row) for row in await cursor.fetchall()]


async def main() -> None:
    backup_path = backup_db()
    await cache.init_db()
    db = await cache.get_db()
    rows = await fetchall(
        db,
        """
        SELECT
            ps.google_sub,
            u.email,
            ps.stock_code,
            ps.market_value,
            ps.group_name AS snapshot_group_name,
            up.stock_name,
            up.quantity AS current_quantity,
            up.avg_price,
            COALESCE(up.avg_price_currency, 'KRW') AS avg_price_currency,
            up.group_name AS current_group_name
        FROM portfolio_stock_snapshots ps
        LEFT JOIN user_portfolio up
          ON up.google_sub = ps.google_sub
         AND up.stock_code = ps.stock_code
        LEFT JOIN users u
          ON u.google_sub = ps.google_sub
        WHERE ps.date = ?
          AND (ps.quantity IS NULL OR ps.unit_price IS NULL OR ps.avg_price_krw IS NULL OR ps.cost_basis IS NULL)
        ORDER BY ps.google_sub, ps.stock_code
        """,
        (SNAP_DATE,),
    )

    by_user: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_user[row["google_sub"]].append(row)

    prepared: list[dict] = []
    skipped_users: list[dict] = []
    for google_sub, user_rows in by_user.items():
        missing = [row["stock_code"] for row in user_rows if row.get("current_quantity") is None]
        if missing:
            skipped_users.append({
                "google_sub": google_sub,
                "email": user_rows[0].get("email"),
                "reason": "snapshot stock is not in current portfolio",
                "stock_codes": missing,
            })
            continue
        holdings = [
            {
                "avg_price": row.get("avg_price"),
                "avg_price_currency": row.get("avg_price_currency") or "KRW",
            }
            for row in user_rows
        ]
        await fx.annotate_avg_price_krw(holdings)
        for row, holding in zip(user_rows, holdings):
            qty = float(row.get("current_quantity") or 0)
            market_value = float(row.get("market_value") or 0)
            avg_price_krw = float(holding.get("avg_price_krw") or 0)
            prepared.append({
                "google_sub": row["google_sub"],
                "email": row.get("email"),
                "stock_code": row["stock_code"],
                "quantity": qty,
                "unit_price": (market_value / qty) if abs(qty) > 1e-12 else None,
                "avg_price_krw": avg_price_krw,
                "cost_basis": qty * avg_price_krw,
                "group_name": row.get("snapshot_group_name") or row.get("current_group_name"),
            })

    await db.execute("BEGIN IMMEDIATE")
    try:
        for row in prepared:
            await db.execute(
                """
                UPDATE portfolio_stock_snapshots
                   SET quantity = ?,
                       unit_price = ?,
                       avg_price_krw = ?,
                       cost_basis = ?,
                       group_name = COALESCE(group_name, ?)
                 WHERE google_sub = ?
                   AND date = ?
                   AND stock_code = ?
                """,
                (
                    row["quantity"],
                    row["unit_price"],
                    row["avg_price_krw"],
                    row["cost_basis"],
                    row["group_name"],
                    row["google_sub"],
                    SNAP_DATE,
                    row["stock_code"],
                ),
            )
        await db.commit()
    except Exception:
        await db.rollback()
        raise

    impacted_users = sorted({row["google_sub"] for row in prepared})
    regenerated_reports = []
    for google_sub in impacted_users:
        for period_type, period_key in REPORT_PERIODS:
            try:
                saved = await period_reports.generate_and_save_period_report(google_sub, period_type, period_key)
                regenerated_reports.append({
                    "google_sub": google_sub,
                    "email": next((row.get("email") for row in prepared if row["google_sub"] == google_sub), None),
                    "period_type": period_type,
                    "period_key": period_key,
                    "source_hash": saved.get("source_hash"),
                    "warnings": (saved.get("report") or {}).get("data_quality", {}).get("warnings", []),
                })
            except Exception as exc:
                regenerated_reports.append({
                    "google_sub": google_sub,
                    "period_type": period_type,
                    "period_key": period_key,
                    "error": str(exc),
                })

    remaining = await fetchall(
        db,
        """
        SELECT ps.google_sub, u.email, COUNT(*) AS missing_rows
        FROM portfolio_stock_snapshots ps
        LEFT JOIN users u ON u.google_sub = ps.google_sub
        WHERE ps.date = ?
          AND ps.quantity IS NULL
        GROUP BY ps.google_sub, u.email
        ORDER BY missing_rows DESC
        """,
        (SNAP_DATE,),
    )
    print(json.dumps({
        "ok": True,
        "backup_path": backup_path,
        "snapshot_date": SNAP_DATE,
        "updated_rows": len(prepared),
        "updated_users": len(impacted_users),
        "skipped_users": skipped_users,
        "remaining_quantity_null": remaining,
        "regenerated_reports": regenerated_reports,
    }, ensure_ascii=False, indent=2))
    await cache.close_db()


asyncio.run(main())
PY
  touch "$REPAIR_MARKER"
fi

# Regenerate the already-saved 2026-06 monthly period reports with the current
# composition semantics: negative quantities are futures shorts with negative
# weights, and code-only preferred-stock snapshot names are resolved at build
# time. This rewrites only portfolio_period_reports rows that already exist.
REPAIR_MARKER=".deploy-repair-2026-06-period-report-short-names-v1.done"
if [[ ! -f "$REPAIR_MARKER" ]]; then
  log "Regenerating saved 2026-06 period reports for short-position semantics and names"
  python3 - <<'PY'
import asyncio
import json
import sqlite3
from datetime import datetime
from pathlib import Path

from core.config import load_environment

load_environment(Path.cwd(), force=True)

import cache
from services.portfolio import period_reports

PERIOD_TYPE = "monthly"
PERIOD_KEY = "2026-06"


def backup_db() -> str:
    db_path = Path("cache.db").resolve()
    backup_dir = Path("data/db-imports")
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = backup_dir / f"cache.before-2026-06-period-report-regen.{stamp}.db"
    src = sqlite3.connect(db_path)
    dst = sqlite3.connect(backup_path)
    try:
        src.backup(dst)
    finally:
        dst.close()
        src.close()
    return str(backup_path)


async def fetchall(db, sql: str, params: tuple = ()) -> list[dict]:
    cursor = await db.execute(sql, params)
    return [dict(row) for row in await cursor.fetchall()]


async def main() -> None:
    backup_path = backup_db()
    await cache.init_db()
    db = await cache.get_db()
    users = await fetchall(
        db,
        """
        SELECT DISTINCT pr.google_sub, u.email
        FROM portfolio_period_reports pr
        LEFT JOIN users u
          ON u.google_sub = pr.google_sub
        WHERE pr.period_type = ?
          AND pr.period_key = ?
        ORDER BY u.email, pr.google_sub
        """,
        (PERIOD_TYPE, PERIOD_KEY),
    )

    regenerated_reports = []
    for user in users:
        google_sub = user["google_sub"]
        try:
            saved = await period_reports.generate_and_save_period_report(google_sub, PERIOD_TYPE, PERIOD_KEY)
            comp = (saved.get("report") or {}).get("composition_changes") or {}
            regenerated_reports.append({
                "google_sub": google_sub,
                "email": user.get("email"),
                "period_type": PERIOD_TYPE,
                "period_key": PERIOD_KEY,
                "source_hash": saved.get("source_hash"),
                "summary": comp.get("summary") or {},
                "warnings": (saved.get("report") or {}).get("data_quality", {}).get("warnings", []),
            })
        except Exception as exc:
            regenerated_reports.append({
                "google_sub": google_sub,
                "email": user.get("email"),
                "period_type": PERIOD_TYPE,
                "period_key": PERIOD_KEY,
                "error": str(exc),
            })

    errors = [row for row in regenerated_reports if row.get("error")]
    print(json.dumps({
        "ok": not errors,
        "backup_path": backup_path,
        "target_saved_reports": len(users),
        "regenerated_reports": regenerated_reports,
    }, ensure_ascii=False, indent=2))
    await cache.close_db()
    if errors:
        raise SystemExit(1)


asyncio.run(main())
PY
  touch "$REPAIR_MARKER"
fi
