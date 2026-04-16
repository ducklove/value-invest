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

# Summary to journald.
size_human=$(du -h "$daily_out" | cut -f1)
total_daily=$(find "$BACKUP_DIR/daily" -name 'cache-*.db.gz' | wc -l)
total_weekly=$(find "$BACKUP_DIR/weekly" -name 'cache-*.db.gz' | wc -l)
echo "OK backup=$daily_out size=$size_human kept_daily=$total_daily kept_weekly=$total_weekly"
