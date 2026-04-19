"""Admin API endpoints — batch job monitoring, manual triggers, system stats."""

import asyncio
import json
import logging
import os
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path

from fastapi import APIRouter, Body, HTTPException, Query, Request

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


async def _latest_data_date_for(job_name: str) -> str | None:
    """Return the most recent date that actually has data produced by
    this batch job. systemd exit-0 alone doesn't prove the job wrote
    anything — a weekend-skip or a post-failure-retry that gave up can
    both end in exit 0. Querying the downstream table is the only way
    to know whether the operator should actually trust a 'success'."""
    db = await cache.get_db()
    if job_name == "portfolio-snapshot":
        # Any user's most recent NAV snapshot date.
        cursor = await db.execute("SELECT MAX(date) AS d FROM portfolio_snapshots")
        row = await cursor.fetchone()
        return row["d"] if row and row["d"] else None
    if job_name == "nps-snapshot":
        cursor = await db.execute("SELECT MAX(date) AS d FROM nps_snapshots")
        row = await cursor.fetchone()
        return row["d"] if row and row["d"] else None
    if job_name == "portfolio-intraday":
        # Intraday writes ISO timestamps (YYYY-MM-DDTHH:MM); strip to date.
        cursor = await db.execute("SELECT MAX(ts) AS t FROM portfolio_intraday")
        row = await cursor.fetchone()
        if row and row["t"]:
            return row["t"][:10]
        return None
    return None


def _is_trading_day(iso_date: str) -> bool:
    """Weekday check — ignores Korean holidays, which is OK for dashboard
    purposes (we only use this to decide whether a missing date is cause
    for alarm; a false alarm on 신정 is harmless vs. the cost of shipping
    a full holiday calendar)."""
    try:
        return date.fromisoformat(iso_date).weekday() < 5
    except Exception:
        return False


def _compute_staleness(job_name: str, latest_data_date: str | None) -> dict:
    """Classify how fresh this job's data is relative to today.

    Returns:
      level: 'ok' | 'stale' | 'missing'
      expected_latest: the YYYY-MM-DD we'd expect to see for a healthy job
      trading_days_behind: how many weekdays of data are missing (0 when ok)
      note: human-readable short label

    For the intraday job "latest" is the last trading day's intraday
    activity — stale-ness isn't as clean as for daily jobs, so we give
    it a looser treatment.
    """
    today = date.today()
    # Expected latest date = last trading day <= today. The 22:00 KST
    # daily jobs write for the same day, so if today is a weekday after
    # 22:00 we'd expect today; before 22:00 we'd expect yesterday. Use
    # yesterday as the conservative expectation so the dashboard doesn't
    # cry "stale" between 09:00–22:00.
    probe = today - timedelta(days=1) if today.weekday() < 5 else today
    while probe.weekday() >= 5:
        probe -= timedelta(days=1)
    expected = probe.isoformat()

    if not latest_data_date:
        return {"level": "missing", "expected_latest": expected,
                "trading_days_behind": None, "note": "데이터 없음"}

    # Count trading-day gap.
    gap = 0
    cursor = date.fromisoformat(expected)
    try:
        latest_d = date.fromisoformat(latest_data_date)
    except ValueError:
        return {"level": "stale", "expected_latest": expected,
                "trading_days_behind": None, "note": f"날짜 파싱 실패 ({latest_data_date})"}

    while cursor > latest_d:
        if cursor.weekday() < 5:
            gap += 1
        cursor -= timedelta(days=1)

    if gap <= 0:
        return {"level": "ok", "expected_latest": expected,
                "trading_days_behind": 0,
                "note": f"최신 {latest_data_date}"}
    return {"level": "stale", "expected_latest": expected,
            "trading_days_behind": gap,
            "note": f"최신 {latest_data_date} — 거래일 {gap}일 지연"}


@router.get("/batch-status")
async def batch_status(request: Request):
    await _require_admin(request)
    jobs = []
    for t in _TIMERS:
        info = _get_timer_status(t["name"])
        # Augment the systemd snapshot with "did this job actually
        # produce data recently?". Without this, a weekend skip or a
        # silent-failure retry can still paint the card green when the
        # operator cares whether 4/17 got written to nps_snapshots.
        latest_data_date = await _latest_data_date_for(t["name"])
        staleness = _compute_staleness(t["name"], latest_data_date)
        jobs.append({
            **t, **info,
            "latest_data_date": latest_data_date,
            "staleness": staleness,
        })
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
# Deploy status — what commit is this process actually running, and is
# the auto-deploy pipeline healthy? Without this, verifying "did my push
# reach prod" required SSH'ing in. That defeats the whole point of an
# admin dashboard.
# ---------------------------------------------------------------------------

def _load_build_info() -> dict:
    """Read the git commit that was checked out when this module was
    imported. Captures the version actually running in THIS process —
    a later `git pull` won't mutate it, so the dashboard reflects the
    binary's reality, not the filesystem's."""
    info = {"sha": "", "short_sha": "", "subject": "", "committed_at": ""}
    try:
        root = Path(__file__).parent.parent
        result = subprocess.run(
            ["git", "log", "-1", "--format=%H%n%h%n%s%n%cI"],
            cwd=str(root), capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            lines = result.stdout.strip().split("\n")
            if len(lines) >= 4:
                info["sha"] = lines[0]
                info["short_sha"] = lines[1]
                info["subject"] = lines[2]
                info["committed_at"] = lines[3]
    except Exception:
        pass
    return info


_BUILD_INFO = _load_build_info()


def _runner_status() -> dict:
    """Poll systemd for any active `actions.runner.*` service — the
    self-hosted GitHub Actions runner that drives auto-deploy. If this
    is down, no push will actually land on the Pi until it's restarted."""
    try:
        result = subprocess.run(
            ["systemctl", "list-units", "--type=service", "--state=active",
             "actions.runner.*", "--no-legend", "--plain"],
            capture_output=True, text=True, timeout=5,
        )
        for line in result.stdout.strip().split("\n"):
            parts = line.split()
            if parts and parts[0].startswith("actions.runner."):
                return {"active": True, "name": parts[0]}
    except Exception as exc:
        return {"active": False, "name": "", "error": str(exc)[:200]}
    return {"active": False, "name": ""}


def _service_start_timestamp() -> str:
    """Raw ActiveEnterTimestamp for value-invest.service. Same format
    systemctl prints — the frontend parses it as a plain Date string."""
    try:
        r = subprocess.run(
            ["systemctl", "show", "value-invest.service",
             "--property=ActiveEnterTimestamp"],
            capture_output=True, text=True, timeout=5,
        )
        for line in r.stdout.strip().split("\n"):
            if line.startswith("ActiveEnterTimestamp="):
                return line.split("=", 1)[1]
    except Exception:
        pass
    return ""


@router.get("/deploy-status")
async def deploy_status(request: Request):
    """What commit is running, when did the process start, is the
    auto-deploy runner alive. Serves as the 'is my push live yet'
    signal so the operator doesn't need to SSH."""
    await _require_admin(request)
    return {
        "build": _BUILD_INFO,
        "service_started": _service_start_timestamp(),
        "actions_runner": _runner_status(),
    }


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

@router.post("/refresh-foreign-dividends")
async def refresh_foreign_dividends_endpoint(request: Request):
    """yfinance 로부터 포트폴리오 내 해외 종목들의 trailing annual
    dividend 를 일괄 fetch + KRW 환산 + upsert. source='manual' 인
    수동 override 는 건드리지 않음."""
    await _require_admin(request)
    import foreign_dividends
    result = await foreign_dividends.refresh_foreign_dividends()
    result["total_cached"] = await cache.get_foreign_dividends_count()
    return result


@router.get("/foreign-dividends")
async def list_foreign_dividends_endpoint(request: Request):
    """관리자 UI 의 목록 뷰용. 수동 override row 가 먼저 오도록 정렬."""
    await _require_admin(request)
    return await cache.list_foreign_dividends()


@router.post("/foreign-dividend")
async def upsert_foreign_dividend_endpoint(request: Request, payload: dict = Body(...)):
    """관리자 수동 배당 입력. body: {stock_code, dps_krw, note?}.
    source='manual' 로 저장되어 이후 auto refresh 에서 덮어쓰지 않음.
    dps_krw 0 허용 (의도적으로 '배당 없음' 명시)."""
    await _require_admin(request)
    code = str(payload.get("stock_code") or "").strip().upper()
    if not code:
        raise HTTPException(status_code=400, detail="stock_code 는 필수입니다.")
    dps_raw = payload.get("dps_krw")
    if dps_raw is None or dps_raw == "":
        raise HTTPException(status_code=400, detail="dps_krw 는 필수입니다.")
    try:
        dps = float(dps_raw)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="dps_krw 는 숫자여야 합니다.")
    if dps < 0:
        raise HTTPException(status_code=400, detail="dps_krw 는 0 이상이어야 합니다.")
    note = payload.get("note")
    note = str(note).strip() if note else None
    await cache.upsert_foreign_dividend_manual(code, dps, note)
    return {"ok": True, "stock_code": code, "dps_krw": dps, "note": note, "source": "manual"}


@router.delete("/foreign-dividend/{stock_code}")
async def delete_foreign_dividend_endpoint(request: Request, stock_code: str):
    """수동/자동 entry 제거. 이후 auto refresh 에서 다시 채워질 수 있음."""
    await _require_admin(request)
    deleted = await cache.delete_foreign_dividend(stock_code.strip().upper())
    if not deleted:
        raise HTTPException(status_code=404, detail="해당 종목의 배당 entry 가 없습니다.")
    return {"ok": True, "stock_code": stock_code}


@router.post("/refresh-preferred-dividends")
async def refresh_preferred_dividends_endpoint(request: Request):
    """수동 트리거 — Google Sheet Data!AI 컬럼을 즉시 재동기화. 주기
    루프(12h)가 알아서 돌지만, 시트 관리자가 방금 값을 바꾼 직후에는
    기다리지 않고 바로 반영하고 싶을 때 쓰기 위함."""
    await _require_admin(request)
    import preferred_dividends
    result = await preferred_dividends.refresh_preferred_dividends()
    # Attach current cached row count so the dashboard can show before/after.
    result["total_cached"] = await cache.get_preferred_dividends_count()
    return result


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
    # Naver can be slow or unreliable from the Pi; the endpoint used to
    # just await fetch_reports() and would hang until an upstream proxy
    # (or the browser) timed out — the user saw a generic "진단 실패"
    # without any clue that the slowness was upstream. Cap at 12s so we
    # always return a well-formed JSON payload within 15s regardless.
    naver_err: str | None = None
    reports: list[dict] = []
    try:
        reports = await asyncio.wait_for(report_client.fetch_reports(code), timeout=12.0)
    except asyncio.TimeoutError:
        naver_err = "Naver 스크랩 타임아웃 (>12초) — 네트워크 또는 Naver 쪽 지연"
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
