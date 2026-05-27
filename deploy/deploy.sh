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
  "nps-snapshot.service"
  "nps-snapshot.timer"
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

# --- Tests ------------------------------------------------------------------
# Blocking: a test failure aborts the deploy via the ERR trap, which rolls
# the checkout back to OLD_SHA. The previous non-blocking behaviour was a
# stopgap while the `_conn` singleton leak in the test fixtures made 32
# tests spuriously fail; that's been fixed.
log "Running tests"
python3 -m pip install "${PIP_FLAGS[@]}" --quiet -r requirements-dev.txt
python3 -m pytest -q

# Past this point, rolling the checkout back would desync from a restarted
# service, so clear the trap. Health-check failure is reported but the new
# code stays on disk for inspection.
trap - ERR

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
wait_for_healthz

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

# 2026-05-20 NPS NAV was saved from incomplete per-stock close coverage, and
# 2026-05-26/27 reused the last available 2026-05-22 stock closes while KOSPI
# itself had moved on. Rebuild the dates whose exact closes are available and
# remove the known stale rows; route-level chart repair also filters any
# lingering bad embedded JSON from older generated_html.
REPAIR_MARKER=".deploy-repair-2026-05-nps-nav-bad-points.done"
if [[ ! -f "$REPAIR_MARKER" ]]; then
  log "Repairing invalid May 2026 NPS NAV chart points"
  for d in 2026-05-20 2026-05-21 2026-05-22; do
    log "Rebuilding NPS snapshot for $d"
    if ! python3 snapshot_nps.py "$d"; then
      log "NPS repair for $d failed; continuing with remaining dates and route-level chart filtering"
    fi
  done
  python3 - <<'PY'
import asyncio
import cache

async def main():
    await cache.init_db()
    db = await cache.get_db()
    for day in ("2026-05-26", "2026-05-27"):
        await db.execute("DELETE FROM nps_holdings WHERE date = ?", (day,))
        await db.execute("DELETE FROM nps_snapshots WHERE date = ?", (day,))
    await db.commit()
    await cache.close_db()

asyncio.run(main())
PY
  touch "$REPAIR_MARKER"
fi
