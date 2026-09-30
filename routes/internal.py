"""Internal endpoints invoked by systemd timers on the same host.

The previous design spawned `python3 snapshot_*.py` as separate processes,
which meant each run started with cold in-memory caches (the warm WS quote,
ticker-map and FX caches) and re-hit every upstream — wasting
KIS/Naver/yfinance rate budget on stocks the web process had just queried
seconds earlier.

These endpoints run the same snapshot logic inside the web process where
those caches are warm.

Access control (``_require_loopback``): a request is accepted when EITHER
  * it carries a valid ``X-Internal-Token`` (== ``INTERNAL_API_TOKEN``,
    constant-time compare) — how cross-host callers (finance-pi, buybacks, …)
    authenticate; OR
  * it comes from a direct loopback peer (127.0.0.1 / ::1) with no
    ``X-Forwarded-For`` / ``X-Real-IP`` / ``Forwarded`` header — the systemd
    timers' plain ``curl https://127.0.0.1:3691/...`` calls.
The loopback path works whether or not ``INTERNAL_API_TOKEN`` is configured,
so setting the token never breaks the timers. Proxied or non-loopback callers
without a valid token get 403.
"""
from __future__ import annotations

import asyncio
import hmac
import logging
import os

from fastapi import APIRouter, Body, HTTPException, Request

from core.errors import AppError
from domain.market_calendar import MarketCalendarUnknown

router = APIRouter(prefix="/api/internal", include_in_schema=False)
logger = logging.getLogger(__name__)
_nav_snapshot_lock = asyncio.Lock()


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
# 이 중 하나라도 있으면 리버스 프록시를 거친 요청으로 보고 loopback 예외를
# 적용하지 않는다(프록시의 peer 주소가 127.0.0.1 이어도).
_PROXY_HEADERS = ("x-forwarded-for", "x-real-ip", "forwarded")
_TOKEN_HEADERS = ("x-internal-token", "x-value-invest-internal-token")


def _provided_token(request: Request) -> str:
    for name in _TOKEN_HEADERS:
        value = (request.headers.get(name) or "").strip()
        if value:
            return value
    return ""


def _token_is_valid(request: Request) -> bool:
    """Constant-time check of X-Internal-Token against INTERNAL_API_TOKEN.

    An unset/blank configured token never matches (not even an empty header).
    """
    expected_token = os.getenv("INTERNAL_API_TOKEN", "").strip()
    provided_token = _provided_token(request)
    if not expected_token or not provided_token:
        return False
    # bytes 로 비교 — 비 ASCII 헤더 값에서 compare_digest(str) 가 TypeError 를
    # 내지 않도록 한다.
    return hmac.compare_digest(
        provided_token.encode("utf-8"), expected_token.encode("utf-8")
    )


def _is_direct_loopback(request: Request) -> bool:
    client = request.client
    host = client.host if client else ""
    if host not in _LOOPBACK_HOSTS:
        return False
    return not any(request.headers.get(name) for name in _PROXY_HEADERS)


def _require_loopback(request: Request) -> None:
    """Accept (valid token) OR (direct loopback peer without proxy headers).

    Direct loopback calls stay supported for the systemd timers whether or
    not INTERNAL_API_TOKEN is configured. Proxied (X-Forwarded-For /
    X-Real-IP / Forwarded) or non-loopback callers need a valid
    X-Internal-Token, compared in constant time.
    """
    if _token_is_valid(request):
        return
    if _is_direct_loopback(request):
        return

    client = request.client
    logger.warning(
        "internal endpoint rejected host=%s forwarded_for=%s real_ip=%s token_sent=%s",
        client.host if client else "",
        request.headers.get("x-forwarded-for"),
        request.headers.get("x-real-ip"),
        bool(_provided_token(request)),
    )
    raise HTTPException(status_code=403, detail="internal only")


@router.post("/snapshot/nav")
async def run_nav_snapshot(request: Request):
    _require_loopback(request)
    from services.portfolio import nav_snapshot as snapshot_nav
    try:
        async with _nav_snapshot_lock:
            await snapshot_nav.run_all_snapshots(manage_db=False, only_missing=True)
        return {"ok": True, "kind": "nav"}
    except Exception as exc:
        raise _job_failed("nav snapshot", exc) from exc


@router.post("/snapshot/after-close")
async def run_after_close_snapshot(request: Request):
    _require_loopback(request)
    from services.portfolio import after_close
    try:
        async with _nav_snapshot_lock:
            await after_close.capture_all()
        return {"ok": True, "kind": "after_close"}
    except Exception as exc:
        raise _job_failed("after close snapshot", exc) from exc


@router.post("/snapshot/intraday")
async def run_intraday_snapshot(request: Request):
    _require_loopback(request)
    from services.portfolio import intraday_snapshot as snapshot_intraday
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
      audience    선택. "admins" 면 관리자(is_admin) 사용자에게만 보낸다 —
                  systemd 실패 알림(scripts/notify_failure.sh)처럼 운영자용 메시지.
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
    audience = str(payload.get("audience") or "").strip().lower()
    if audience not in ("", "all", "admins"):
        raise HTTPException(status_code=400, detail="audience must be 'all' or 'admins'")
    if requested_sub:
        targets = [requested_sub]
    elif audience == "admins":
        targets = [u["google_sub"] for u in await users_repo.get_all_users() if u.get("is_admin")]
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
        from fastapi.responses import JSONResponse

        healthy = result.get("counts", {}).get("error", 0) == 0
        return JSONResponse({"ok": healthy, **result}, status_code=200 if healthy else 503)
    except Exception as exc:
        raise _job_failed("data quality check", exc) from exc


@router.post("/daily-briefing/morning-valuation")
async def run_morning_valuation(request: Request):
    _require_loopback(request)
    from services.portfolio import morning_valuation

    return {"ok": True, **await morning_valuation.capture_enabled()}


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
        if result.get("failed"):
            raise RuntimeError(f"브리핑 {result['failed']}개 발송 보류/실패 — 사용자별 이벤트를 확인하세요.")
        return {"ok": True, **result}
    except MarketCalendarUnknown as exc:
        # 미설정 특수 세션일(수능일 등)·미등록 연도: 잘못된 요청(400)이 아니라
        # 운영자 조치가 필요한 작업 실패 — 로그 + 500 → systemd OnFailure 알림.
        raise _job_failed("daily briefing send", exc) from exc
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
