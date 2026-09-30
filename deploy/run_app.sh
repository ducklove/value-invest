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
# 재시작 때 브라우저의 실시간 시세 WebSocket·SSE 가 열려 있으면 uvicorn 이 닫힐
# 때까지 무한정 기다려 systemd 가 90초 뒤 SIGKILL 했다(배포마다 실패 알림, 종료
# 정리 누락). 15초 뒤에는 남은 연결을 끊고 lifespan 종료(작업 정리·DB 닫기)로 간다.
exec "$PYTHON" -m uvicorn main:app --host 0.0.0.0 --port 3691 \
  --timeout-graceful-shutdown 15 \
  --ssl-keyfile "$CERT_DIR/privkey.pem" \
  --ssl-certfile "$CERT_DIR/fullchain.pem"
