#!/usr/bin/env bash
# Daily SQLite backup for value-invest cache.db.
# Uses `sqlite3 .backup` (WAL-safe, consistent) rather than file copy.
# Keeps 14 days of daily snapshots, plus the most recent weekly snapshot
# on Sundays (retained for 60 days) so longer-range recovery is possible
# without blowing up disk.

set -euo pipefail

APP_DIR="${APP_DIR:-/home/cantabile/Works/value_invest}"
DB="$APP_DIR/cache.db"
BACKUP_DIR="${BACKUP_DIR:-/home/cantabile/backups/value-invest}"
DAILY_KEEP="${DAILY_KEEP:-14}"
WEEKLY_KEEP_DAYS="${WEEKLY_KEEP_DAYS:-60}"
# Optional off-site sync via rclone. Leave empty to skip. Example:
#   BACKUP_RCLONE_REMOTE=gdrive:value-invest-backup
# To enable, set this in /etc/systemd/system/value-invest-backup.service.d/
# override.conf via `systemctl edit value-invest-backup.service`, add:
#   [Service]
#   Environment="BACKUP_RCLONE_REMOTE=gdrive:value-invest-backup"
BACKUP_RCLONE_REMOTE="${BACKUP_RCLONE_REMOTE:-}"
RCLONE_CONFIG="${RCLONE_CONFIG:-/home/cantabile/.config/rclone/rclone.conf}"

mkdir -p "$BACKUP_DIR/daily" "$BACKUP_DIR/weekly"

if [[ ! -f "$DB" ]]; then
  echo "ERROR: DB not found at $DB" >&2
  exit 1
fi

today="$(date +%F)"
dow="$(date +%u)"   # 1..7 (Mon..Sun)
daily_out="$BACKUP_DIR/daily/cache-$today.db"
weekly_out="$BACKUP_DIR/weekly/cache-$today.db"

# Online backup — works even while the service has the DB open (WAL).
# Writes to a temp file, then renames so partial writes aren't visible.
tmp="$daily_out.tmp"
sqlite3 "$DB" ".backup '$tmp'"
mv -f "$tmp" "$daily_out"

# Compress (gzip; ~3-5x smaller for SQLite). Keep both until retention trims.
gzip -f "$daily_out"
daily_out="$daily_out.gz"

# Sunday → also copy to weekly bucket.
if [[ "$dow" == "7" ]]; then
  cp -f "$daily_out" "$weekly_out.gz"
fi

# Retention.
find "$BACKUP_DIR/daily" -name 'cache-*.db.gz' -mtime +"$DAILY_KEEP" -delete
find "$BACKUP_DIR/weekly" -name 'cache-*.db.gz' -mtime +"$WEEKLY_KEEP_DAYS" -delete

# Integrity check on today's backup — gunzip to memory and run PRAGMA
# integrity_check on a copy. If it fails, the backup is worthless; fail
# loudly so the OnFailure hook (once wired) fires.
check_copy="$(mktemp --suffix=.db)"
gunzip -c "$daily_out" > "$check_copy"
rc=0
out=$(sqlite3 "$check_copy" "PRAGMA integrity_check;" 2>&1) || rc=$?
rm -f "$check_copy"
if [[ $rc -ne 0 || "$out" != "ok" ]]; then
  echo "ERROR: backup integrity check failed: $out" >&2
  exit 2
fi

# Off-site sync (if configured). Fails loudly if rclone is missing or
# remote is misconfigured; local backup still succeeded by this point.
if [[ -n "$BACKUP_RCLONE_REMOTE" ]]; then
  if ! command -v rclone >/dev/null; then
    echo "ERROR: BACKUP_RCLONE_REMOTE set but rclone not installed" >&2
    exit 3
  fi
  # One-way mirror local tree → remote. --fast-list keeps API calls low;
  # --bwlimit caps upload rate so home bandwidth isn't saturated.
  rclone sync "$BACKUP_DIR/" "$BACKUP_RCLONE_REMOTE/" \
    --config "$RCLONE_CONFIG" \
    --fast-list --bwlimit 2M --log-level NOTICE \
    --exclude '*.tmp'
  echo "OK rclone_synced remote=$BACKUP_RCLONE_REMOTE"
fi

# Summary to journald.
size_human=$(du -h "$daily_out" | cut -f1)
total_daily=$(find "$BACKUP_DIR/daily" -name 'cache-*.db.gz' | wc -l)
total_weekly=$(find "$BACKUP_DIR/weekly" -name 'cache-*.db.gz' | wc -l)
echo "OK backup=$daily_out size=$size_human kept_daily=$total_daily kept_weekly=$total_weekly"
