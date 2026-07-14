#!/usr/bin/env bash
# Runs production one-time repair scripts with shared marker and lock handling.

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
APP_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
REPAIR_STATE_DIR="${REPAIR_STATE_DIR:-.deploy-repairs}"
REPAIR_LOCK="${REPAIR_LOCK:-$REPAIR_STATE_DIR/repairs.lock}"
REPAIR_LOCK_TIMEOUT="${REPAIR_LOCK_TIMEOUT:-600}"

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

repair_done() {
  local marker="$1"
  [[ -f "$marker" || -f "$REPAIR_STATE_DIR/$marker" ]]
}

run_repair() {
  local marker="$1"
  local label="$2"
  shift 2

  mkdir -p "$REPAIR_STATE_DIR"
  if repair_done "$marker"; then
    log "Skipping one-time repair: $label"
    return 0
  fi

  (
    flock -w "$REPAIR_LOCK_TIMEOUT" 9
    if repair_done "$marker"; then
      exit 0
    fi
    log "$label"
    "$@"
    touch "$REPAIR_STATE_DIR/$marker" "$marker"
  ) 9>"$REPAIR_LOCK"
}

cd "$APP_ROOT"

run_repair \
  ".deploy-repair-2026-05-18-nav-v3.done" \
  "Repairing portfolio NAV snapshot for 2026-05-18" \
  bash deploy/repairs/repair_2026_05_18_nav_v3.sh

run_repair \
  ".deploy-repair-2026-05-27-intraday-stale-quotes.done" \
  "Removing invalid 2026-05-27 intraday quote-fallback points" \
  python3 deploy/repairs/repair_2026_05_27_intraday_stale_quotes.py

run_repair \
  ".deploy-repair-cantabile658-backfill-2026-06-v1.done" \
  "Backfilling cantabile658@gmail.com portfolio snapshots from 2026-05-31" \
  python3 deploy/repairs/repair_cantabile658_backfill_2026_06_v1.py

run_repair \
  ".deploy-repair-2026-06-30-stock-quantity-metadata-v1.done" \
  "Backfilling 2026-06-30 stock snapshot quantity metadata" \
  python3 deploy/repairs/repair_2026_06_30_stock_quantity_metadata_v1.py

run_repair \
  ".deploy-repair-2026-06-period-report-short-names-v1.done" \
  "Regenerating saved 2026-06 period reports for short-position semantics and names" \
  python3 deploy/repairs/repair_2026_06_period_report_short_names_v1.py

run_repair \
  ".deploy-repair-2026-07-13-nav-stale-fallback-v1.done" \
  "Re-settling 2026-07-13 NAV snapshot polluted by quote-outage fallback" \
  python3 deploy/repairs/repair_2026_07_13_nav_stale_fallback_v1.py
