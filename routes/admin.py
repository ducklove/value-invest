"""Admin API endpoints — batch job monitoring, manual triggers, system stats."""

import asyncio
import logging
import os
import subprocess
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request

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
