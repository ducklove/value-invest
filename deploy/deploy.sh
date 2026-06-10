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
