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
  "nps-snapshot.service"
  "nps-snapshot.timer"
  "portfolio-intraday.service"
  "portfolio-intraday.timer"
  "portfolio-snapshot.service"
  "portfolio-snapshot.timer"
  "deploy/value-invest.service"
)

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

cd "$APP_DIR"

log "Fetching latest from origin/master"
git fetch --prune origin
OLD_SHA="$(git rev-parse HEAD)"
git reset --hard origin/master
NEW_SHA="$(git rev-parse HEAD)"

if [[ "$OLD_SHA" == "$NEW_SHA" ]]; then
  log "No new commits ($NEW_SHA). Skipping deploy."
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

# --- Python deps (only if requirements changed) -----------------------------
if grep -qE '^requirements(-dev)?\.txt$' <<<"$CHANGED_FILES"; then
  log "requirements changed — installing"
  python3 -m pip install --user --upgrade -r requirements.txt
fi

# --- Tests ------------------------------------------------------------------
log "Running tests"
python3 -m pip install --user --quiet -r requirements-dev.txt
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
log "Waiting for healthz"
for i in {1..20}; do
  if curl -fsS --max-time 2 "$HEALTH_URL" >/dev/null 2>&1; then
    log "Healthz OK (attempt $i)"
    exit 0
  fi
  sleep 1
done

log "Healthz did not respond in time; showing recent logs"
sudo /bin/systemctl --no-pager status "$SERVICE" || true
exit 1
