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
  "portfolio-after-close.service"
  "portfolio-after-close.timer"
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
  "linked-projects-sync.service"
  "linked-projects-sync.timer"
  "daily-briefing.service"
  "daily-briefing.timer"
  "daily-briefing-valuation.service"
  "daily-briefing-valuation.timer"
  "daily-briefing-market-close.service"
  "daily-briefing-market-close.timer"
  "daily-briefing-night.service"
  "daily-briefing-night.timer"
  "deploy/value-invest.service"
)

log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }

wait_for_healthz() {
  # -k: cert is for ducklove.duckdns.org; localhost check skips name match.
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
NEW_SHA="$(git rev-parse origin/master)"
STATE_DIR="$APP_DIR/.deploy-state/$(date +%s)-$$"
mkdir -p "$STATE_DIR/units"
chmod 700 "$STATE_DIR"
OLD_VENV="$(readlink "$APP_DIR/.venv-current" || true)"
[[ ! -f .env ]] || cp -p .env "$STATE_DIR/env"

# 설치된 유닛과 타이머 상태를 보존한다. 코드만 되돌려서는 재시작 실패를 복구할 수 없다.
for src in "${REPO_UNITS[@]}"; do
  unit="$(basename "$src")"
  if [[ -f "$UNIT_DST/$unit" ]]; then
    cp -p "$UNIT_DST/$unit" "$STATE_DIR/units/$unit"
  fi
  if [[ "$unit" == *.timer ]]; then
    sudo /bin/systemctl is-enabled "$unit" >"$STATE_DIR/$unit.enabled" 2>/dev/null || true
    sudo /bin/systemctl is-active "$unit" >"$STATE_DIR/$unit.active" 2>/dev/null || true
  fi
done
MUTATED_UNITS=()
RUNTIME_CHANGED=0

rollback() {
  failure="${1:-1}"
  trap - ERR
  set +e
  log "Deploy failed — rolling back to $OLD_SHA"
  cd "$APP_DIR"
  git reset --hard "$OLD_SHA"
  if [[ -n "$OLD_VENV" ]]; then
    ln -sfn "$OLD_VENV" "$APP_DIR/.venv-current.rollback"
    mv -Tf "$APP_DIR/.venv-current.rollback" "$APP_DIR/.venv-current"
  else
    rm -f "$APP_DIR/.venv-current"
  fi
  if [[ -f "$STATE_DIR/env" ]]; then cp -p "$STATE_DIR/env" .env; fi
  for unit in "${MUTATED_UNITS[@]}"; do
    if [[ -f "$STATE_DIR/units/$unit" ]]; then
      sudo cp "$STATE_DIR/units/$unit" "$UNIT_DST/$unit"
    else
      sudo /bin/systemctl disable --now "$unit" 2>/dev/null
      sudo rm -f "$UNIT_DST/$unit"
    fi
  done
  if (( ${#MUTATED_UNITS[@]} > 0 )); then
    sudo /bin/systemctl daemon-reload
    for unit in "${MUTATED_UNITS[@]}"; do
      [[ "$unit" == *.timer ]] || continue
      if grep -qx enabled "$STATE_DIR/$unit.enabled"; then
        sudo /bin/systemctl enable "$unit"
      else
        sudo /bin/systemctl disable "$unit"
      fi
      if grep -qx active "$STATE_DIR/$unit.active"; then
        sudo /bin/systemctl restart "$unit"
      else
        sudo /bin/systemctl stop "$unit"
      fi
    done
  fi
  if (( RUNTIME_CHANGED )); then
    sudo /bin/systemctl restart "$SERVICE"
    wait_for_healthz || log "Rollback health check failed — manual intervention required"
  fi
  exit "$failure"
}
trap 'rollback $?' ERR

# 정상 배포 뒤에만 호출한다. .venvs/<sha> 는 현재(.venv-current 대상=NEW_VENV)와
# 직전(OLD_VENV — 수동 롤백 대상)만 남기고, .deploy-state 는 이번 것과 바로
# 이전 것만 남긴다(.env 사본 보관 개수 제한). 이번 배포보다 나중에 만들어진
# 상태 디렉터리, SHA 형식이 아닌 디렉터리, 심볼릭 링크는 건드리지 않는다.
# 실패해도 배포 결과에는 영향이 없도록 호출부에서 `|| log` 로 감싼다.
prune_deploy_artifacts() {
  local venvs_dir="$APP_DIR/.venvs" state_root="$APP_DIR/.deploy-state"
  local keep_new keep_old keep_cur dir name
  keep_new="$(basename "$NEW_VENV")"
  keep_old="${OLD_VENV:+$(basename "$OLD_VENV")}"
  keep_cur="$(readlink "$APP_DIR/.venv-current" || true)"
  keep_cur="${keep_cur:+$(basename "$keep_cur")}"
  if [[ -d "$venvs_dir" && ! -L "$venvs_dir" ]]; then
    while IFS= read -r dir; do
      name="$(basename "$dir")"
      [[ "$name" =~ ^[0-9a-f]{40}([0-9a-f]{24})?$ ]] || continue
      [[ "$name" == "$keep_new" || "$name" == "$keep_old" || "$name" == "$keep_cur" ]] && continue
      log "Pruning old venv $name"
      rm -rf -- "$dir"
    done < <(find "$venvs_dir" -mindepth 1 -maxdepth 1 -type d)
  fi

  # 스테이징 소스(+node_modules)는 배포가 끝나면 쓰지 않는다(롤백은 git reset).
  rm -rf -- "$STATE_DIR/source"
  [[ -d "$state_root" && ! -L "$state_root" ]] || return 0
  local current_epoch previous="" epoch
  current_epoch="$(basename "$STATE_DIR")"
  current_epoch="${current_epoch%%-*}"
  local older=()
  while IFS= read -r name; do
    [[ "$name" =~ ^([0-9]+)-[0-9]+$ ]] || continue
    [[ "$state_root/$name" == "$STATE_DIR" ]] && continue
    epoch="${BASH_REMATCH[1]}"
    (( epoch <= current_epoch )) || continue
    older+=("$name")
  done < <(find "$state_root" -mindepth 1 -maxdepth 1 -type d -exec basename {} \; | sort -t- -k1,1n -k2,2n)
  if (( ${#older[@]} > 0 )); then
    previous="${older[${#older[@]}-1]}"
    rm -rf -- "$state_root/$previous/source"
    for name in "${older[@]}"; do
      [[ "$name" == "$previous" ]] && continue
      log "Pruning old deploy state $name"
      rm -rf -- "${state_root:?}/$name"
    done
  fi
}

log "Deploying $OLD_SHA -> $NEW_SHA"
STAGED_DIR="$STATE_DIR/source"
mkdir -p "$STAGED_DIR"
git archive "$NEW_SHA" | tar -x -C "$STAGED_DIR"
cd "$STAGED_DIR"

# 운영 site-packages를 수정하지 않는다. 검사와 서비스가 동일한 해시 고정 환경을 쓴다.
NEW_VENV="$APP_DIR/.venvs/$NEW_SHA"
if [[ ! -f "$NEW_VENV/.installed" ]]; then
  python3 -m venv "$NEW_VENV"
  "$NEW_VENV/bin/python" -m pip install --quiet --require-hashes -r requirements-dev.lock
  touch "$NEW_VENV/.installed"
fi
log "Running Python gates"
"$NEW_VENV/bin/python" -m ruff check .
"$NEW_VENV/bin/python" -m pytest -q

if [[ "${SKIP_JS_TESTS:-0}" == "1" ]]; then
  log "SKIP_JS_TESTS=1 — JS tests skipped by explicit request."
elif command -v node >/dev/null 2>&1 && command -v npm >/dev/null 2>&1; then
  npm ci --no-audit --no-fund
  npm test
else
  log "ERROR: node/npm missing — JS gate cannot run"
  rollback 1
fi

cd "$APP_DIR"
git reset --hard "$NEW_SHA"
bash deploy/migrate_env_to_single_file.sh
for src in "${REPO_UNITS[@]}"; do
  [[ -f "$src" ]] || continue
  unit="$(basename "$src")"
  if ! sudo cmp -s "$src" "$UNIT_DST/$unit" 2>/dev/null; then
    MUTATED_UNITS+=("$unit")
    RUNTIME_CHANGED=1
    # Stop active timers before reloading their calendars. Otherwise systemd
    # can dispatch a catch-up against the old web process during deployment.
    # New timers have no loaded unit yet; stopping them makes systemctl fail.
    if [[ "$unit" == *.timer && -f "$UNIT_DST/$unit" ]]; then
      sudo /bin/systemctl stop "$unit"
    fi
    sudo cp "$src" "$UNIT_DST/$unit"
  fi
done
if (( ${#MUTATED_UNITS[@]} > 0 )); then
  sudo /bin/systemctl daemon-reload
  for unit in "${MUTATED_UNITS[@]}"; do
    if [[ "$unit" == *.timer ]]; then
      sudo /bin/systemctl enable "$unit"
    fi
  done
fi

# 환경 선택은 원자적으로 바꾼다. 실패하면 이전 유닛/환경/코드로 함께 복구한다.
ln -sfn "$NEW_VENV" "$APP_DIR/.venv-current.next"
mv -Tf "$APP_DIR/.venv-current.next" "$APP_DIR/.venv-current"
RUNTIME_CHANGED=1
sudo /bin/systemctl restart "$SERVICE"
wait_for_healthz
curl -fsSk --max-time 10 "${HEALTH_URL%/healthz}/readyz" >/dev/null
for unit in "${MUTATED_UNITS[@]}"; do
  if [[ "$unit" == *.timer ]]; then
    sudo /bin/systemctl restart "$unit"
    log "Timer state: $unit"
    sudo /bin/systemctl show "$unit" -p ActiveState -p NextElapseUSecRealtime -p TimersCalendar
  fi
done
trap - ERR
prune_deploy_artifacts || log "WARNING: pruning old venvs/deploy state failed (deploy itself succeeded)"

# 데이터 보정은 되돌릴 수 있는 코드 배포와 별개다. 실패 시 새 서비스는 유지한다.
PATH="$NEW_VENV/bin:$PATH" bash deploy/repairs/run_one_time_repairs.sh
log "Deploy complete: $NEW_SHA"
