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

import logging

from fastapi import APIRouter, HTTPException, Request

router = APIRouter(prefix="/api/internal", include_in_schema=False)
logger = logging.getLogger(__name__)


_LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost"}


def _require_loopback(request: Request) -> None:
    """Reject anything that's not a loopback connection."""
    client = request.client
    host = client.host if client else ""
    if host not in _LOOPBACK_HOSTS:
        logger.warning("internal endpoint rejected non-loopback host=%s", host)
        raise HTTPException(status_code=403, detail="loopback only")


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
async def run_nps_snapshot_ep(request: Request):
    _require_loopback(request)
    import snapshot_nps
    try:
        await snapshot_nps.run_nps_snapshot(manage_db=False)
        return {"ok": True, "kind": "nps"}
    except Exception as exc:
        logger.exception("nps snapshot failed")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
