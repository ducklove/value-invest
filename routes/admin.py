"""Admin API endpoints — batch job monitoring, manual triggers, system stats."""

import asyncio
import json
import logging
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request

import cache
from deps import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/admin", tags=["admin"])


_STANDALONE_MODE = False  # Set True when running as separate admin server


async def _require_admin(request: Request) -> dict:
    if _STANDALONE_MODE:
        return {"google_sub": "admin", "is_admin": True}
    user = await get_current_user(request)
    if not user or not user.get("is_admin"):
        raise HTTPException(status_code=403, detail="관리자 권한이 필요합니다.")
    return user


# ---------------------------------------------------------------------------
# Batch job monitoring
# ---------------------------------------------------------------------------

_TIMERS = [
    {"name": "portfolio-snapshot", "label": "포트폴리오 일일정산", "schedule": "매일 22:00"},
    {"name": "nps-snapshot", "label": "국민연금 일일정산", "schedule": "매일 22:05"},
    {"name": "portfolio-intraday", "label": "포트폴리오 장중", "schedule": "09:00~21:30 (30분)"},
]


def _parse_systemctl_show(output: str) -> dict:
    result = {}
    for line in output.strip().split("\n"):
        if "=" in line:
            k, _, v = line.partition("=")
            result[k] = v
    return result


def _get_timer_status(name: str) -> dict:
    try:
        svc = subprocess.run(
            ["systemctl", "show", f"{name}.service",
             "--property=ActiveState,SubState,ExecMainStartTimestamp,ExecMainExitTimestamp,ExecMainStatus,MainPID"],
            capture_output=True, text=True, timeout=5,
        )
        svc_props = _parse_systemctl_show(svc.stdout)

        timer = subprocess.run(
            ["systemctl", "show", f"{name}.timer",
             "--property=ActiveState,NextElapseUSecRealtime,LastTriggerUSec"],
            capture_output=True, text=True, timeout=5,
        )
        timer_props = _parse_systemctl_show(timer.stdout)

        active = svc_props.get("ActiveState", "unknown")
        sub = svc_props.get("SubState", "unknown")
        exit_code = svc_props.get("ExecMainStatus", "")

        if active == "activating":
            status = "running"
        elif active == "active" and sub == "exited" and exit_code == "0":
            status = "success"
        elif active == "failed" or exit_code not in ("0", ""):
            status = "failed"
        elif active == "inactive" and exit_code == "0":
            status = "success"
        else:
            status = "idle"

        return {
            "service_state": active,
            "sub_state": sub,
            "exit_code": exit_code,
            "status": status,
            "last_start": svc_props.get("ExecMainStartTimestamp", ""),
            "last_exit": svc_props.get("ExecMainExitTimestamp", ""),
            "pid": svc_props.get("MainPID", "0"),
            "timer_active": timer_props.get("ActiveState", "unknown"),
            "next_run": timer_props.get("NextElapseUSecRealtime", ""),
            "last_trigger": timer_props.get("LastTriggerUSec", ""),
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.get("/batch-status")
async def batch_status(request: Request):
    await _require_admin(request)
    jobs = []
    for t in _TIMERS:
        info = _get_timer_status(t["name"])
        jobs.append({**t, **info})
    return jobs


# ---------------------------------------------------------------------------
# Manual trigger
# ---------------------------------------------------------------------------

_running_jobs: dict[str, asyncio.subprocess.Process] = {}

_JOB_SCRIPTS = {
    "portfolio-snapshot": "snapshot_nav.py",
    "nps-snapshot": "snapshot_nps.py",
    "portfolio-intraday": "snapshot_intraday.py",
}


async def _run_snapshot_job(job_name: str, snap_date: str | None = None):
    """Run a snapshot script as a subprocess to avoid DB connection conflicts."""
    script = _JOB_SCRIPTS[job_name]
    cmd = ["/usr/bin/python3", script]
    if snap_date and job_name != "portfolio-intraday":
        cmd.append(snap_date)
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd, cwd=str(Path(__file__).parent.parent),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        _running_jobs[job_name] = proc
        stdout, _ = await proc.communicate()
        if proc.returncode == 0:
            logger.info("Manual job completed: %s (date=%s)", job_name, snap_date)
        else:
            logger.error("Manual job failed: %s exit=%d\n%s", job_name, proc.returncode, stdout.decode()[-500:])
    except Exception as e:
        logger.error("Manual job error: %s: %s", job_name, e)
    finally:
        _running_jobs.pop(job_name, None)


@router.post("/trigger/{job_name}")
async def trigger_job(job_name: str, request: Request):
    await _require_admin(request)
    body = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
    snap_date = body.get("date")

    valid_jobs = {t["name"] for t in _TIMERS}
    if job_name not in valid_jobs:
        raise HTTPException(status_code=400, detail=f"Unknown job: {job_name}")

    proc = _running_jobs.get(job_name)
    if proc and proc.returncode is None:
        raise HTTPException(status_code=409, detail="이미 실행 중입니다.")

    asyncio.create_task(_run_snapshot_job(job_name, snap_date))
    return {"message": f"{job_name} 실행 시작", "date": snap_date}


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

@router.get("/users")
async def list_users(request: Request):
    await _require_admin(request)
    return await cache.get_all_users()


# ---------------------------------------------------------------------------
# Server status
# ---------------------------------------------------------------------------

def _get_server_stats() -> dict:
    import shutil
    # Memory
    mem = {}
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                parts = line.split()
                if parts[0] in ("MemTotal:", "MemAvailable:", "MemFree:"):
                    mem[parts[0].rstrip(":")] = int(parts[1]) * 1024  # kB -> bytes
    except Exception:
        pass

    # Disk
    disk = {}
    try:
        usage = shutil.disk_usage("/")
        disk = {"total": usage.total, "used": usage.used, "free": usage.free}
    except Exception:
        pass

    # Uptime
    uptime = ""
    try:
        with open("/proc/uptime") as f:
            secs = float(f.read().split()[0])
            days = int(secs // 86400)
            hours = int((secs % 86400) // 3600)
            uptime = f"{days}일 {hours}시간"
    except Exception:
        pass

    # CPU temp (Raspberry Pi)
    cpu_temp = None
    try:
        with open("/sys/class/thermal/thermal_zone0/temp") as f:
            cpu_temp = int(f.read().strip()) / 1000
    except Exception:
        pass

    # Load average (kept for reference / debug)
    load_avg = ""
    try:
        with open("/proc/loadavg") as f:
            load_avg = " ".join(f.read().split()[:3])
    except Exception:
        pass

    return {
        "memory": mem,
        "disk": disk,
        "uptime": uptime,
        "cpu_temp": cpu_temp,
        "load_avg": load_avg,
        "now": datetime.now().isoformat(),
    }


def _read_cpu_times() -> tuple[int, int] | None:
    """Return (total_jiffies, idle_jiffies) for the aggregate 'cpu' line in
    /proc/stat. The aggregate already sums every core, so a fully loaded
    quad-core box reaches the 100% scale naturally."""
    try:
        with open("/proc/stat") as f:
            line = f.readline()
        parts = line.split()
        if not parts or parts[0] != "cpu":
            return None
        nums = [int(x) for x in parts[1:]]
        # fields: user, nice, system, idle, iowait, irq, softirq, steal, ...
        idle = nums[3] + (nums[4] if len(nums) > 4 else 0)  # idle + iowait
        total = sum(nums)
        return total, idle
    except Exception:
        return None


async def _measure_cpu_pct(interval: float = 0.2) -> float | None:
    """True CPU utilization percent (0–100) over a short sampling window,
    summed across all cores: 4 fully-pegged cores on this quad-core Pi → 100%."""
    a = _read_cpu_times()
    if a is None:
        return None
    await asyncio.sleep(interval)
    b = _read_cpu_times()
    if b is None:
        return None
    total_delta = b[0] - a[0]
    idle_delta = b[1] - a[1]
    if total_delta <= 0:
        return None
    pct = (1.0 - idle_delta / total_delta) * 100.0
    return max(0.0, min(100.0, pct))


@router.get("/server-stats")
async def server_stats(request: Request):
    await _require_admin(request)
    stats = _get_server_stats()
    stats["cpu_pct"] = await _measure_cpu_pct()
    return stats


# ---------------------------------------------------------------------------
# DB stats
# ---------------------------------------------------------------------------

@router.get("/db-stats")
async def db_stats(request: Request):
    await _require_admin(request)
    return await cache.get_db_stats()


# ---------------------------------------------------------------------------
# Observability: event feed + per-subsystem diagnostics
# ---------------------------------------------------------------------------

def _parse_event_row(row: dict) -> dict:
    """Attach parsed `details_obj` so the frontend doesn't have to JSON-
    decode per row. Keep the raw string too for debugging."""
    out = dict(row)
    raw = out.get("details")
    if raw:
        try:
            out["details_obj"] = json.loads(raw)
        except Exception:
            out["details_obj"] = None
    else:
        out["details_obj"] = None
    return out


@router.get("/events")
async def list_events(
    request: Request,
    source: str | None = None,
    level: str | None = None,
    stock_code: str | None = None,
    hours: float | None = None,
    limit: int = 100,
):
    """Unified event feed. All filters optional; defaults to newest 100.

    Note: query parameters are typed as plain Python defaults (not
    fastapi.Query) so unit tests can call this function directly. FastAPI
    still parses strings via annotations; the hard bounds (limit ≤1000)
    are enforced inside cache.get_system_events.
    """
    await _require_admin(request)
    since = None
    if hours is not None and hours > 0:
        since = (datetime.now() - timedelta(hours=hours)).isoformat(timespec="seconds")
    rows = await cache.get_system_events(
        source=source,
        level=level,
        stock_code=stock_code,
        since=since,
        limit=limit,
    )
    return [_parse_event_row(r) for r in rows]


@router.get("/event-summary")
async def event_summary(request: Request, hours: float = 24):
    """Counts grouped by (source, level) over the last `hours`. Small payload
    for the dashboard top strip — one row per subsystem, failure counts
    highlighted."""
    await _require_admin(request)
    since = (datetime.now() - timedelta(hours=hours)).isoformat(timespec="seconds")
    summary = await cache.summarize_system_events(since)
    # Attach "latest tick" per known subsystem so the card can show "15 min
    # ago" vs a cold "no tick in 3 days" state at a glance.
    known_sources = [
        "snapshot_nav", "snapshot_intraday", "snapshot_nps",
        "wiki_ingestion", "benchmark_history",
    ]
    latest: dict[str, dict | None] = {}
    for src in known_sources:
        row = await cache.get_latest_event(src)
        latest[src] = _parse_event_row(row) if row else None
    return {"hours": hours, "by_source": summary, "latest": latest}


# ---------------------------------------------------------------------------
# Subsystem diagnostics — /diag/<subsystem>
# ---------------------------------------------------------------------------

@router.get("/diag/wiki")
async def diag_wiki(request: Request, code: str = ""):
    """Why isn't this stock's wiki growing? — end-to-end probe that
    answers the question in one call instead of three manual SSH queries.

    Exposes every funnel stage so the operator can pinpoint whether the
    loss is upstream (Naver has no PDF), in our whitelist (PDF exists
    but we reject the host), or downstream (PDF parsed but LLM summary
    failed).
    """
    await _require_admin(request)
    code = code.strip()
    if not code:
        raise HTTPException(status_code=400, detail="code is required")

    import report_client
    from routes.reports import _is_allowed_report_pdf_url

    # --- Naver funnel -----------------------------------------------------
    naver_err: str | None = None
    reports: list[dict] = []
    try:
        reports = await report_client.fetch_reports(code)
    except Exception as exc:
        naver_err = str(exc)[:300]

    with_pdf = [r for r in reports if r.get("pdf_url")]
    allowed = [r for r in with_pdf if _is_allowed_report_pdf_url(r["pdf_url"])]

    sample_limit = 10
    samples = []
    for r in reports[:sample_limit]:
        pdf = r.get("pdf_url")
        if not pdf:
            reason = "no_pdf_url"
        elif not _is_allowed_report_pdf_url(pdf):
            reason = "rejected_by_whitelist"
        else:
            reason = "ok"
        samples.append({
            "date": r.get("date"),
            "firm": r.get("firm"),
            "title": r.get("title"),
            "pdf_url": (pdf or "")[:200] if pdf else None,
            "status": reason,
        })

    # --- DB state ---------------------------------------------------------
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT COUNT(*) AS n FROM stock_wiki_entries WHERE stock_code = ?",
        (code,),
    )
    wiki_count = int((await cursor.fetchone())["n"])

    cursor = await db.execute(
        "SELECT parse_status, COUNT(*) AS n FROM report_pdf_cache WHERE stock_code = ? GROUP BY parse_status",
        (code,),
    )
    pdf_by_status = {r["parse_status"]: int(r["n"]) for r in await cursor.fetchall()}

    # Latest 5 failure messages (if any) — truncated for UI readability.
    cursor = await db.execute(
        """SELECT parse_status, parse_error, pdf_url FROM report_pdf_cache
           WHERE stock_code = ? AND parse_status IN ('download_failed', 'parse_failed')
           ORDER BY downloaded_at DESC LIMIT 5""",
        (code,),
    )
    recent_failures = [
        {"status": r["parse_status"], "error": (r["parse_error"] or "")[:200], "pdf_url": r["pdf_url"]}
        for r in await cursor.fetchall()
    ]

    # --- Verdict ----------------------------------------------------------
    # Plain-language summary so the operator doesn't have to interpret
    # numbers. Prioritize the most common failure mode first.
    if naver_err:
        verdict = f"Naver 응답 실패: {naver_err}"
    elif len(reports) == 0:
        verdict = "Naver 에서 리포트 0건 반환. 종목 코드가 맞는지, 또는 커버리지가 없는 종목인지 확인 필요."
    elif len(with_pdf) == 0:
        verdict = (
            f"Naver 는 {len(reports)}건 주지만 **pdf_url 이 전부 비어있음**. "
            "증권사가 PDF 를 자사 사이트에서만 제공하는 케이스 — 우리 쪽에서 긁을 방법 없음."
        )
    elif len(allowed) == 0:
        verdict = (
            f"{len(with_pdf)}건에 pdf_url 있지만 **화이트리스트에서 모두 탈락** "
            "(stock.pstatic.net/stock-research/*.pdf 만 허용). 화이트리스트 확장 필요."
        )
    elif wiki_count == 0 and pdf_by_status:
        verdict = (
            f"{len(allowed)}건 통과·{sum(pdf_by_status.values())}건 다운로드 시도, "
            f"요약 0건. 실패 원인: {pdf_by_status}"
        )
    elif wiki_count >= len(allowed):
        verdict = f"정상. {wiki_count}건 요약됨 (Naver 가 주는 유효 PDF 수에 도달)."
    else:
        verdict = (
            f"진행 중. Naver 유효 {len(allowed)}건, 요약 {wiki_count}건. "
            "다음 wiki_ingestion tick 에서 차이분이 채워집니다."
        )

    return {
        "stock_code": code,
        "naver": {
            "total": len(reports),
            "has_pdf": len(with_pdf),
            "passes_whitelist": len(allowed),
            "error": naver_err,
            "samples": samples,
        },
        "db": {
            "wiki_entries": wiki_count,
            "pdf_cache_by_status": pdf_by_status,
            "recent_failures": recent_failures,
        },
        "verdict": verdict,
    }
