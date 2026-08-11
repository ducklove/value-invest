#!/usr/bin/env bash
# One-way migration to the single-file config (.env).
#
# Before: settings and secrets were spread across `.env`, `.env.<profile>`,
# `.kis.env`, `keys.txt` and systemd `Environment=` lines. The app now reads
# `.env` only, so any value left behind in a legacy file would silently stop
# being applied. This script folds them into `.env` and moves the originals
# into a timestamped backup directory.
#
# Idempotent: a no-op once no legacy file remains. Runs from deploy.sh BEFORE
# the unit sync/restart so the service comes back up with a complete `.env`.

set -euo pipefail

APP_DIR="${APP_DIR:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)}"
cd "$APP_DIR"

# Merge precedence must reproduce the old load order, where a later loader
# overrode an earlier one and keys.txt only filled gaps:
#   .kis.env > .env.<profile> > .env > keys.txt
LEGACY_LOW_TO_HIGH=("keys.txt" ".env" ".env.production" ".env.development" ".kis.env")
LEGACY_ONLY=("keys.txt" ".env.production" ".env.development" ".kis.env")

found=0
for f in "${LEGACY_ONLY[@]}"; do
  [[ -f "$f" ]] && found=1
done
if (( ! found )); then
  exit 0
fi

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
log "Consolidating legacy config files into .env"

declare -A merged=()
declare -a order=()

absorb() {
  local file="$1"
  [[ -f "$file" ]] || return 0
  local line key value
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line#"${line%%[![:space:]]*}"}"          # ltrim
    [[ -z "$line" || "$line" == \#* || "$line" != *=* ]] && continue
    key="${line%%=*}"
    value="${line#*=}"
    key="${key%"${key##*[![:space:]]}"}"             # rtrim key
    key="${key#export }"
    [[ -z "$key" ]] && continue
    if [[ -z "${merged[$key]+x}" ]]; then
      order+=("$key")
    fi
    merged["$key"]="$value"
  done <"$file"
  echo "    read $(basename "$file")"
}

for f in "${LEGACY_LOW_TO_HIGH[@]}"; do
  absorb "$f"
done

BACKUP_DIR=".config-migrated-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$BACKUP_DIR"
for f in "${LEGACY_LOW_TO_HIGH[@]}"; do
  [[ -f "$f" ]] && cp -p "$f" "$BACKUP_DIR/"
done

{
  echo "# value-invest 단일 설정 파일 — 모든 설정과 시크릿을 여기 한 곳에 둔다."
  echo "# 프로세스 환경변수(systemd Environment=)가 이 파일보다 우선한다."
  echo "# 새 키는 .env.example 에 먼저 문서화한다. 커밋 금지."
  echo "# 이 파일은 레거시 설정 파일 통합으로 생성되었다. 원본 백업: $BACKUP_DIR"
  echo
  for key in "${order[@]}"; do
    printf '%s=%s\n' "$key" "${merged[$key]}"
  done
} >.env.consolidated

mv .env.consolidated .env
chmod 600 .env

for f in "${LEGACY_ONLY[@]}"; do
  [[ -f "$f" ]] && rm -f "$f"
done

log "Consolidated ${#order[@]} key(s) into .env (backup: $BACKUP_DIR)"
