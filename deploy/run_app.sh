#!/usr/bin/env bash
set -euo pipefail
APP_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$APP_DIR"
PYTHON="$APP_DIR/.venv-current/bin/python"
[[ -x "$PYTHON" ]] || PYTHON=/usr/bin/python3
# uvicorn 이 직접 TLS 를 종단한다. 인증서는 certbot(webroot — Caddy 가
# /.well-known/acme-challenge/ 를 /srv/acme 로 서빙)이
# /etc/letsencrypt/live/<도메인>/ 에 자동 갱신한다. 접속 도메인을 옮길 때는
# 새 도메인 인증서를 먼저 발급한 뒤 .env 의 TLS_CERT_NAME(또는 아래 기본값)만
# 바꾸면 된다 — 인증서가 없으면 서비스가 뜨지 않고 deploy.sh 가 롤백한다.
TLS_CERT_NAME="${TLS_CERT_NAME:-ducklove.duckdns.org}"
CERT_DIR="/etc/letsencrypt/live/$TLS_CERT_NAME"
if [[ ! -r "$CERT_DIR/privkey.pem" || ! -r "$CERT_DIR/fullchain.pem" ]]; then
  echo "run_app.sh: TLS 인증서를 찾을 수 없습니다: $CERT_DIR (TLS_CERT_NAME=$TLS_CERT_NAME)" >&2
  exit 1
fi
exec "$PYTHON" -m uvicorn main:app --host 0.0.0.0 --port 3691 \
  --ssl-keyfile "$CERT_DIR/privkey.pem" \
  --ssl-certfile "$CERT_DIR/fullchain.pem"
