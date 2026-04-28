"""Internal endpoints invoked by systemd timers on the same host.

The previous design spawned `python3 snapshot_*.py` as separate processes,
which meant each run started with cold in-memory caches
(`routes.portfolio._quote_cache`, `_ticker_map`, `_last_known_quotes`,
`_fx_daily_cache`) and re-hit every upstream — wasting KIS/Naver/yfinance
rate budget on stocks the web process had just queried seconds earlier.

These endpoints run the same snapshot logic inside the web process where
those caches are warm. Access is restricted to loopback so there's no
external attack surface.
"""
from __future__ import annotations

import hmac
import logging
import os

from fastapi import APIRouter, Body, HTTPException, Request

router = APIRouter(prefix="/api/internal", include_in_schema=False)
logger = logging.getLogger(__name__)


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
        logger.exception("nav snapshot failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/snapshot/intraday")
async def run_intraday_snapshot(request: Request):
    _require_loopback(request)
    import snapshot_intraday
    try:
        await snapshot_intraday.run(manage_db=False)
        return {"ok": True, "kind": "intraday"}
    except Exception as exc:
        logger.exception("intraday snapshot failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/snapshot/nps")
async def run_nps_snapshot_ep(request: Request, payload: dict = Body(default={})):
    """NPS daily snapshot. Accepts optional {"date": "YYYY-MM-DD"} so a
    backfill call after a missed run doesn't require SSH'ing into the
    Pi to run the Python CLI."""
    _require_loopback(request)
    import snapshot_nps
    snap_date = None
    if isinstance(payload, dict) and payload.get("date"):
        snap_date = str(payload["date"]).strip()
    try:
        await snapshot_nps.run_nps_snapshot(snap_date=snap_date, manage_db=False)
        return {"ok": True, "kind": "nps", "date": snap_date or "today"}
    except Exception as exc:
        logger.exception("nps snapshot failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


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
        logger.exception("wiki ingest failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc


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
        logger.exception("DART review ingest failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
