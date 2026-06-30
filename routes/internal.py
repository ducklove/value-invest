"""Internal endpoints invoked by systemd timers on the same host.

The previous design spawned `python3 snapshot_*.py` as separate processes,
which meant each run started with cold in-memory caches (the warm WS quote,
ticker-map and FX caches) and re-hit every upstream — wasting
KIS/Naver/yfinance rate budget on stocks the web process had just queried
seconds earlier.

These endpoints run the same snapshot logic inside the web process where
those caches are warm. Access is restricted to loopback so there's no
external attack surface.
"""
from __future__ import annotations

import hmac
import logging
import os

from fastapi import APIRouter, Body, HTTPException, Request

from core.errors import AppError

router = APIRouter(prefix="/api/internal", include_in_schema=False)
logger = logging.getLogger(__name__)


def _job_failed(kind: str, exc: Exception) -> AppError:
    """Standardize internal-batch failures into AppError (-> HTTP 500).

    Before this helper, each route repeated the same three lines:
    ``logger.exception(...); raise HTTPException(500, str(exc)) from exc``.
    Centralizing it keeps the log message format consistent and routes
    AppError through the registered exception handler (single response shape).
    The original exception is preserved via ``__cause__`` and logged with full
    traceback, so root-cause visibility is unchanged.
    """
    logger.exception("%s failed", kind)
    return AppError(f"{kind} failed: {exc}")


_LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}


def _require_loopback(request: Request) -> None:
    """Protect internal jobs from public reverse-proxy traffic.

    Direct loopback calls remain supported for systemd timers. If
    INTERNAL_API_TOKEN is configured, callers must send X-Internal-Token.
    """
    expected_token = os.getenv("INTERNAL_API_TOKEN", "").strip()
    provided_token = (
        request.headers.get("x-internal-token")
        or request.headers.get("x-value-invest-internal-token")
        or ""
    ).strip()
    if expected_token:
        if hmac.compare_digest(provided_token, expected_token):
            return
        logger.warning("internal endpoint rejected missing/invalid token")
        raise HTTPException(status_code=403, detail="internal token required")

    client = request.client
    host = client.host if client else ""
    forwarded_for = request.headers.get("x-forwarded-for")
    real_ip = request.headers.get("x-real-ip")
    if host in _LOOPBACK_HOSTS and not forwarded_for and not real_ip:
        return

    logger.warning(
        "internal endpoint rejected host=%s forwarded_for=%s real_ip=%s",
        host,
        forwarded_for,
        real_ip,
    )
    raise HTTPException(status_code=403, detail="internal only")


@router.post("/snapshot/nav")
async def run_nav_snapshot(request: Request):
    _require_loopback(request)
    import snapshot_nav
    try:
        await snapshot_nav.run_all_snapshots(manage_db=False)
        return {"ok": True, "kind": "nav"}
    except Exception as exc:
        raise _job_failed("nav snapshot", exc) from exc


@router.post("/snapshot/intraday")
async def run_intraday_snapshot(request: Request):
    _require_loopback(request)
    import snapshot_intraday
    try:
        await snapshot_intraday.run(manage_db=False)
        return {"ok": True, "kind": "intraday"}
    except Exception as exc:
        raise _job_failed("intraday snapshot", exc) from exc


@router.post("/notifications/evaluate")
async def run_notifications_evaluate(request: Request):
    """Run one portfolio-alert evaluation pass over all users. Loopback-only.

    Driven by notify-alerts.timer (KRX hours). Economic-calendar result alerts
    run on their own timer (`evaluate-calendar`) so each timer drives exactly one
    alert type — no overlap, no duplicate sends.
    """
    _require_loopback(request)
    from services.notifications import engine
    try:
        result = await engine.evaluate_all()
        return {"ok": True, **result}
    except Exception as exc:
        raise _job_failed("notification evaluate", exc) from exc


# 텔레그램 메시지 한도(4096) 아래에서 제목/출처 표기와 채널별 오버헤드 여유분.
_NOTIFY_TEXT_MAX = 3800


@router.post("/notify")
async def send_notification(request: Request, payload: dict = Body(...)):
    """연결 프로젝트 공용 알림 발송 — 채널 설정·토큰은 이 허브 한 곳에만 둔다.

    gold_gap, spac-hunter, nps-tracker, finance-pi 같은 서브프로젝트는
    텔레그램 봇 토큰이나 카카오 OAuth 토큰을 들고 있을 필요 없이 이
    엔드포인트 하나만 호출한다. 카카오 refresh token 은 갱신 시 회전하므로
    여러 프로세스가 같은 토큰을 공유하면 서로를 무효화한다 — 발송 주체를
    허브로 단일화해야 하는 구조적 이유. 같은 호스트는 loopback 으로, 다른
    호스트(finance-pi 등)는 INTERNAL_API_TOKEN + X-Internal-Token 헤더로
    인증한다.

    payload:
      text        필수. 본문.
      title       선택. 첫 줄에 📌 와 함께 표기.
      source      선택. 발신 프로젝트 표기 (마지막 줄 "— <source>").
      google_sub  선택. 지정 시 해당 사용자에게만, 생략 시 활성 채널을 가진
                  전체 사용자에게 보낸다.
    """
    _require_loopback(request)
    text = str((payload or {}).get("text") or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="text is required")

    title = str(payload.get("title") or "").strip()
    source = str(payload.get("source") or "").strip()
    lines = []
    if title:
        lines.append(f"📌 {title}")
    lines.append(text)
    if source:
        lines.append(f"— {source}")
    message = "\n".join(lines)
    if len(message) > _NOTIFY_TEXT_MAX:
        message = message[: _NOTIFY_TEXT_MAX - 1] + "…"

    from repositories import users as users_repo
    from services.notifications import channels

    requested_sub = str(payload.get("google_sub") or "").strip()
    if requested_sub:
        targets = [requested_sub]
    else:
        targets = [u["google_sub"] for u in await users_repo.get_all_users()]

    sent = 0
    notified_users = 0
    for sub in targets:
        # dispatch 는 채널 단위 실패를 삼키고 보낸 건수만 돌려준다 — 한
        # 채널 장애가 다른 수신자/채널을 막지 않는다.
        count = await channels.dispatch(sub, message)
        sent += count
        if count:
            notified_users += 1
    return {"ok": True, "sent": sent, "users": notified_users}


@router.post("/notifications/evaluate-calendar")
async def run_calendar_notifications_evaluate(request: Request):
    """Run one economic-calendar result-alert evaluation pass. Loopback-only.

    Driven by notify-calendar.timer on a broad, around-the-clock schedule, since
    economic results are released at all hours (US evenings/overnight KST, EU
    afternoons, weekends) — unlike the KRX-hours portfolio alert timer. No-ops
    cheaply when no subscriptions are pending.
    """
    _require_loopback(request)
    from services.notifications import engine
    try:
        result = await engine.evaluate_calendar_all()
        return {"ok": True, **result}
    except Exception as exc:
        raise _job_failed("calendar notification evaluate", exc) from exc


@router.post("/data-quality/check")
async def run_data_quality_check(request: Request):
    """데이터 품질 정기 점검 한 사이클 실행. Loopback-only.

    data-quality.timer (매일 20:30 KST — 20:05 NAV 스냅샷/벤치마크 증분이
    끝난 뒤) 가 구동한다. 점검 결과는 system_events(source='data_quality')
    에 기록되고 관리자 패널 '데이터 품질' 카드가 이를 읽는다.
    """
    _require_loopback(request)
    from services import data_quality
    try:
        result = await data_quality.run_all_checks()
        return {"ok": True, **result}
    except Exception as exc:
        raise _job_failed("data quality check", exc) from exc


@router.post("/daily-briefing/send")
async def run_daily_briefing_send(request: Request):
    """AI 데일리 브리핑 배치 발송 한 사이클. Loopback-only.

    daily-briefing*.timer 가 구동한다. kind 기본값은 morning이며,
    각 슬롯의 옵트인(user_settings daily_briefing_*_enabled='true') 사용자에게만 생성·발송하고,
    사용자별 결과는 system_events(source='daily_briefing') 에 남는다.
    """
    _require_loopback(request)
    from services import daily_briefing
    try:
        kind = request.query_params.get("kind") or request.query_params.get("briefing_type")
        result = await daily_briefing.send_briefings(kind)
        return {"ok": True, **result}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise _job_failed("daily briefing send", exc) from exc


@router.post("/wiki/ingest")
async def run_wiki_ingest(request: Request, payload: dict = Body(default={})):
    """Drive the wiki ingestion pipeline. Loopback-only.

    Body is optional JSON of the shape:
        {
          "stock_codes": ["005930", ...],   # optional, defaults to pipeline selector
          "per_stock_limit": 10,             # optional
          "model": "..."                     # optional override
        }
    """
    _require_loopback(request)
    import wiki_ingestion
    body = payload or {}
    codes = body.get("stock_codes") if isinstance(body, dict) else None
    per_stock = body.get("per_stock_limit") if isinstance(body, dict) else None
    model = body.get("model") if isinstance(body, dict) else None
    try:
        result = await wiki_ingestion.run_pipeline(
            stock_codes=codes,
            per_stock_limit=per_stock or wiki_ingestion.DEFAULT_PER_STOCK_LIMIT,
            model=model,
        )
        return {"ok": True, **result}
    except Exception as exc:
        raise _job_failed("wiki ingest", exc) from exc


@router.post("/dart-review/ingest")
async def run_dart_review_ingest(request: Request, payload: dict = Body(default={})):
    """Drive the DART filing AI review pre-generation pipeline."""
    _require_loopback(request)
    import dart_report_review
    body = payload or {}
    codes = body.get("stock_codes") if isinstance(body, dict) else None
    target_limit = body.get("target_limit") if isinstance(body, dict) else None
    force = bool(body.get("force")) if isinstance(body, dict) else False
    try:
        result = await dart_report_review.run_pipeline(
            stock_codes=codes,
            target_limit=target_limit,
            force=force,
        )
        import observability
        failed = int(result.get("failed") or 0)
        await observability.record_event(
            "dart_report_review",
            "ingest_partial" if failed else "ingest_ok",
            level="warning" if failed else "info",
            details={
                "stocks_processed": result.get("stocks_processed", 0),
                "generated": result.get("generated", 0),
                "skipped": result.get("skipped", 0),
                "failed": failed,
                "skipped_by_reason": result.get("skipped_by_reason", {}),
                "failed_by_reason": result.get("failed_by_reason", {}),
                "target_limit": target_limit,
                "force": force,
            },
            wait=True,
        )
        return {"ok": True, **result}
    except Exception as exc:
        raise _job_failed("DART review ingest", exc) from exc
