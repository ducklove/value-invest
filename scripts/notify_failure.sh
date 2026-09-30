#!/usr/bin/env bash
# systemd OnFailure hook — a failed timer / service must not be silent.
#
# Invoked as: notify_failure.sh <unit-name>
#
# 1) 허브 공용 알림 POST /api/internal/notify (audience=admins) — 브리핑과 같은
#    관리자 채널(텔레그램·카카오)로 간다. 같은 호스트 루프백이라 토큰 없이도
#    통과하고, INTERNAL_API_TOKEN 이 있으면 헤더로도 보낸다.
# 2) 허브가 응답하지 않거나 한 건도 보내지 못했고 NTFY_TOPIC 이 있으면 ntfy.sh 로.
#
# Env (value-invest-notify@.service 가 .env 를 EnvironmentFile 로 읽는다):
#   VALUE_INVEST_NOTIFY_URL (기본 https://127.0.0.1:3691/api/internal/notify)
#   INTERNAL_API_TOKEN, NTFY_TOPIC, NTFY_SERVER (기본 https://ntfy.sh)
#
# Failure in this script itself is ignored — we don't want notification
# failure to cascade into more failures.

set -u

UNIT="${1:-unknown}"
HUB_URL="${VALUE_INVEST_NOTIFY_URL:-https://127.0.0.1:3691/api/internal/notify}"
TOPIC="${NTFY_TOPIC:-}"
SERVER="${NTFY_SERVER:-https://ntfy.sh}"
HOST=$(hostname)

# Pull the last few lines of context so the notification includes WHY.
# %n in OnFailure passes the full unit name including .service, which is what
# journalctl -u expects.
LAST=$(journalctl -u "$UNIT" -n 5 --no-pager 2>/dev/null | tail -n 4 | tr '\n' ' ' | cut -c1-400)
LAST="${LAST:-No recent log lines.}"

hub_delivered=0
PAYLOAD=$(UNIT="$UNIT" HOST="$HOST" LAST="$LAST" python3 -c '
import json, os
print(json.dumps({
    "title": "[%s] %s 실패" % (os.environ["HOST"], os.environ["UNIT"]),
    "text": os.environ["LAST"],
    "source": "systemd",
    "audience": "admins",
}, ensure_ascii=False))
' 2>/dev/null)

if [[ -n "$PAYLOAD" ]]; then
  auth=()
  if [[ -n "${INTERNAL_API_TOKEN:-}" ]]; then
    auth=(-H "X-Internal-Token: ${INTERNAL_API_TOKEN}")
  fi
  RESPONSE=$(curl -fsSk --max-time 15 -X POST \
    -H "Content-Type: application/json" ${auth[@]+"${auth[@]}"} \
    --data "$PAYLOAD" "$HUB_URL" 2>/dev/null) || RESPONSE=""
  if [[ "$RESPONSE" =~ \"sent\":[[:space:]]*([0-9]+) ]] && (( BASH_REMATCH[1] > 0 )); then
    hub_delivered=1
  fi
fi

if (( hub_delivered )); then
  exit 0
fi

if [[ -z "$TOPIC" ]]; then
  echo "hub notify not delivered and NTFY_TOPIC unset — no notification for $UNIT" >&2
  exit 0
fi

curl -fsS --max-time 10 \
  -H "Title: [${HOST}] ${UNIT} failed" \
  -H "Priority: high" \
  -H "Tags: warning" \
  -d "$LAST" \
  "${SERVER}/${TOPIC}" \
  >/dev/null || true
