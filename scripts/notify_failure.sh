#!/usr/bin/env bash
# systemd OnFailure hook. Sends a push notification via ntfy.sh so a
# failed timer / main-service crash isn't silent.
#
# Invoked as: notify_failure.sh <unit-name>
# Requires env var: NTFY_TOPIC (private topic name, e.g. value-invest-ab12xy)
# Optional: NTFY_SERVER (defaults to https://ntfy.sh)
#
# Failure in this script itself is ignored — we don't want notification
# failure to cascade into more failures.

set -u

UNIT="${1:-unknown}"
TOPIC="${NTFY_TOPIC:-}"
SERVER="${NTFY_SERVER:-https://ntfy.sh}"

if [[ -z "$TOPIC" ]]; then
  echo "NTFY_TOPIC unset — skipping notification for $UNIT" >&2
  exit 0
fi

# Pull the last few lines of context so the notification includes WHY.
# Escape for HTTP header-safe single line. %n in OnFailure passes the full
# unit name including .service, which is what journalctl -u expects.
LAST=$(journalctl -u "$UNIT" -n 5 --no-pager 2>/dev/null | tail -n 4 | tr '\n' ' ' | cut -c1-400)
HOST=$(hostname)

curl -fsS --max-time 10 \
  -H "Title: [${HOST}] ${UNIT} failed" \
  -H "Priority: high" \
  -H "Tags: warning" \
  -d "${LAST:-No recent log lines.}" \
  "${SERVER}/${TOPIC}" \
  >/dev/null || true
