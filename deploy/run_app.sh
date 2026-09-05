#!/usr/bin/env bash
set -euo pipefail
APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$APP_DIR"
PYTHON="$APP_DIR/.venv-current/bin/python"
[[ -x "$PYTHON" ]] || PYTHON=/usr/bin/python3
exec "$PYTHON" -m uvicorn main:app --host 0.0.0.0 --port 3691 \
  --ssl-keyfile /etc/letsencrypt/live/cantabile.tplinkdns.com/privkey.pem \
  --ssl-certfile /etc/letsencrypt/live/cantabile.tplinkdns.com/fullchain.pem
