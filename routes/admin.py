"""Admin API endpoints — batch job monitoring, manual triggers, system stats."""

import asyncio
import html
import ipaddress
import json
import logging
import os
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote, urlparse

from fastapi import APIRouter, Body, HTTPException, Query, Request, Response

import ai_config
import cache
import linked_project_admin
import observability
from deps import TRUSTED_RETURN_ORIGINS, get_current_user
from repositories import foreign_dividends as foreign_dividends_repo
from repositories import portfolio as portfolio_repo
from repositories import system_events as system_events_repo
from repositories import users as users_repo

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/admin", tags=["admin"])


_STANDALONE_MODE = False  # Set True when running as separate admin server
_INTERNAL_PORTFOLIO_BASE_URL = os.getenv(
    "ADMIN_INTERNAL_PORTFOLIO_BASE_URL",
    "https://192.168.68.67:3691",
).rstrip("/")


async def _require_admin(request: Request) -> dict:
    if _STANDALONE_MODE:
        return {"google_sub": "admin", "is_admin": True}
    user = await get_current_user(request)
    if not user or not user.get("is_admin"):
        raise HTTPException(status_code=403, detail="관리자 권한이 필요합니다.")
    return user


_ADMIN_MUTATION_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
_LOCAL_OR_TEST_HOSTS = {"testserver", "testclient", "localhost", "127.0.0.1", "::1"}
_INTERNAL_NETWORKS = tuple(
    ipaddress.ip_network(cidr)
    for cidr in (
        "127.0.0.0/8",
        "10.0.0.0/8",
        "172.16.0.0/12",
        "192.168.0.0/16",
        "169.254.0.0/16",
        "::1/128",
        "fc00::/7",
        "fe80::/10",
    )
)


def _normalize_origin(value: str | None) -> str:
    """Return scheme://host[:port] for Origin/Referer comparisons."""
    if not value:
        return ""
    try:
        parsed = urlparse(value.strip())
    except Exception:
        return ""
    if not parsed.scheme or not parsed.netloc:
        return ""
    return f"{parsed.scheme.lower()}://{parsed.netloc.lower()}"


def _request_origin(request: Request) -> str:
    proto = (
        request.headers.get("x-forwarded-proto", "")
        or request.url.scheme
        or "http"
    ).split(",", 1)[0].strip()
    host = (
        request.headers.get("x-forwarded-host", "")
        or request.headers.get("host", "")
        or request.url.netloc
    ).split(",", 1)[0].strip()
    if not proto or not host:
        return ""
    return f"{proto.lower()}://{host.lower()}"


def _trusted_admin_origins(request: Request) -> set[str]:
    """Static allowlist only — never derived from request headers.

    Trusting a request-derived Origin (Host / X-Forwarded-Host) would let an
    attacker who can spoof those headers add their own origin to the trusted
    set and slip past the CSRF guard. Use the fixed defaults plus the
    ADMIN_ALLOWED_ORIGINS env allowlist instead.
    """
    configured = {
        _normalize_origin(origin)
        for origin in os.getenv("ADMIN_ALLOWED_ORIGINS", "").split(",")
        if origin.strip()
    }
    defaults = {_normalize_origin(origin) for origin in TRUSTED_RETURN_ORIGINS}
    return {origin for origin in (defaults | configured) if origin}


def _is_local_or_test_request(request: Request) -> bool:
    host = (
        request.headers.get("host", "")
        or request.url.hostname
        or ""
    ).split(":", 1)[0].lower()
    client_host = request.client.host if request.client else ""
    return host in _LOCAL_OR_TEST_HOSTS or client_host in _LOCAL_OR_TEST_HOSTS


def _origin_host_is_local(origin: str) -> bool:
    """True if a normalized Origin/Referer points at a loopback/test host.

    Such an origin cannot be produced by a cross-site attacker page (its
    Origin is always the attacker's own public domain), so it is safe to
    trust without consulting any spoofable request header like Host /
    X-Forwarded-Host.
    """
    try:
        host = (urlparse(origin).hostname or "").lower()
    except Exception:
        return False
    return host in _LOCAL_OR_TEST_HOSTS


def _strip_host_port(value: str | None) -> str:
    host = str(value or "").strip()
    if not host:
        return ""
    host = host.split(",", 1)[0].strip()
    if host.startswith("[") and "]" in host:
        return host[1:host.index("]")]
    if host.count(":") == 1 and "." in host:
        return host.split(":", 1)[0]
    return host


def _is_internal_network_host(host: str | None) -> bool:
    host = _strip_host_port(host).lower()
    if host in _LOCAL_OR_TEST_HOSTS:
        return True
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False
    return any(ip in network for network in _INTERNAL_NETWORKS)


def _effective_client_host(request: Request) -> str:
    """Best-effort real client IP behind the local reverse proxy.

    If a proxy provides X-Forwarded-For, prefer its right-most address over
    request.client.host so a public caller proxied through 127.0.0.1 is not
    mistaken for a local request.
    """
    xff = request.headers.get("x-forwarded-for")
    if xff:
        for part in reversed(xff.split(",")):
            host = _strip_host_port(part)
            if host:
                return host
    for header in ("x-real-ip", "cf-connecting-ip"):
        host = _strip_host_port(request.headers.get(header))
        if host:
            return host
    return _strip_host_port(request.client.host if request.client else "")


def _is_internal_network_request(request: Request) -> bool:
    return _is_internal_network_host(_effective_client_host(request))


def _request_host_is_internal_network(request: Request) -> bool:
    host = (
        request.headers.get("x-forwarded-host", "")
        or request.headers.get("host", "")
        or request.url.hostname
        or ""
    )
    return _is_internal_network_host(host)


def _admin_portfolio_url(google_sub: str) -> str:
    encoded_sub = quote(str(google_sub or ""), safe="")
    return f"{_INTERNAL_PORTFOLIO_BASE_URL}/api/admin/users/{encoded_sub}/portfolio.html"


async def _require_internal_admin(request: Request) -> dict:
    user = await _require_admin(request)
    if not _is_internal_network_request(request):
        raise HTTPException(status_code=403, detail="내부 네트워크에서만 열 수 있는 관리자 링크입니다.")
    return user


async def _require_internal_portfolio_viewer(request: Request) -> dict:
    if not _is_internal_network_request(request) or not _request_host_is_internal_network(request):
        raise HTTPException(status_code=403, detail="Internal network portfolio access only.")
    if _STANDALONE_MODE:
        return {"google_sub": "admin", "is_admin": True}
    user = await get_current_user(request)
    if user and user.get("is_admin"):
        return user
    return {"google_sub": "internal-network", "email": "internal-network", "is_admin": False}


async def _require_admin_mutation(request: Request) -> dict:
    """Shared guard for state-changing admin APIs.

    Admin sessions intentionally work across the GitHub Pages/app-server
    flow, so relying on the session cookie alone is not enough. Mutations
    must be JSON fetches from a trusted Origin/Referer.
    """
    user = await _require_admin(request)
    if request.method.upper() not in _ADMIN_MUTATION_METHODS:
        return user

    content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type != "application/json":
        raise HTTPException(status_code=415, detail="관리자 변경 요청은 application/json 만 허용됩니다.")

    origin = _normalize_origin(request.headers.get("origin"))
    if not origin:
        origin = _normalize_origin(request.headers.get("referer"))
    if not origin:
        # No Origin/Referer at all (non-browser client): allow only genuine
        # local/test requests, never a public deployment.
        if _is_local_or_test_request(request):
            return user
        raise HTTPException(status_code=403, detail="허용되지 않은 관리자 요청 출처입니다.")
    if origin in _trusted_admin_origins(request) or _origin_host_is_local(origin):
        return user
    raise HTTPException(status_code=403, detail="허용되지 않은 관리자 요청 출처입니다.")


# ---------------------------------------------------------------------------
# Batch job monitoring
# ---------------------------------------------------------------------------

_TIMERS = [
    {"name": "portfolio-snapshot", "label": "포트폴리오 일일정산", "schedule": "매일 20:05"},
    {"name": "portfolio-intraday", "label": "포트폴리오 장중", "schedule": "08:00~20:00 (10분)"},
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
    # Expected latest date = last trading day <= today. The 20:00 KST
    # daily jobs write for the same day, so if today is a weekday after
    # 20:00 we'd expect today; before 20:00 we'd expect yesterday. Use
    # yesterday as the conservative expectation so the dashboard doesn't
    # cry "stale" between 09:00–20:00.
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
        info = await asyncio.to_thread(_get_timer_status, t["name"])
        # Augment the systemd snapshot with "did this job actually
        # produce data recently?". Without this, a weekend skip or a
        # silent-failure retry can still paint the card green when the
        # operator cares whether 4/17 got written to portfolio_snapshots.
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
    user = await _require_admin_mutation(request)
    body = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
    snap_date = body.get("date")

    valid_jobs = {t["name"] for t in _TIMERS}
    if job_name not in valid_jobs:
        raise HTTPException(status_code=400, detail=f"Unknown job: {job_name}")

    proc = _running_jobs.get(job_name)
    if proc and proc.returncode is None:
        raise HTTPException(status_code=409, detail="이미 실행 중입니다.")

    asyncio.create_task(_run_snapshot_job(job_name, snap_date))
    await observability.record_event(
        "admin",
        "manual_job_triggered",
        level="info",
        details={
            "actor": user.get("email") or user.get("google_sub"),
            "job_name": job_name,
            "date": snap_date,
        },
        wait=True,
    )
    return {"message": f"{job_name} 실행 시작", "date": snap_date}


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

@router.get("/users")
async def list_users(request: Request):
    await _require_admin(request)
    return await users_repo.get_all_users()


def _clean_profile_payload(payload: dict) -> dict:
    email = str((payload or {}).get("email") or "").strip()[:320]
    name = str((payload or {}).get("name") or "").strip()[:120]
    picture = str((payload or {}).get("picture") or "").strip()
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="올바른 이메일을 입력해 주세요.")
    if not name:
        raise HTTPException(status_code=400, detail="이름을 입력해 주세요.")
    if picture and not picture.startswith(("http://", "https://")):
        raise HTTPException(status_code=400, detail="프로필 이미지는 http(s) URL이어야 합니다.")
    return {
        "email": email,
        "name": name,
        "picture": picture[:1000] or None,
        "email_verified": bool((payload or {}).get("email_verified")),
    }


@router.patch("/users/{google_sub}")
async def update_user_profile(google_sub: str, request: Request, payload: dict = Body(...)):
    actor = await _require_admin_mutation(request)
    clean = _clean_profile_payload(payload)
    updated = await users_repo.update_user_profile(google_sub, **clean)
    if not updated:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    await observability.record_event(
        "admin",
        "user_profile_updated",
        level="info",
        details={
            "actor": actor.get("email") or actor.get("google_sub"),
            "target": google_sub,
            "email": clean["email"],
        },
        wait=True,
    )
    return {"ok": True, "user": updated}


@router.put("/users/{google_sub}/role")
async def update_user_role(google_sub: str, request: Request, payload: dict = Body(...)):
    actor = await _require_admin_mutation(request)
    target = await users_repo.get_user(google_sub)
    if not target:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    next_is_admin = bool((payload or {}).get("is_admin"))
    if target.get("is_admin") and not next_is_admin and await users_repo.count_admin_users() <= 1:
        raise HTTPException(status_code=400, detail="마지막 관리자는 일반 사용자로 변경할 수 없습니다.")
    updated = await users_repo.set_user_admin(google_sub, next_is_admin)
    await observability.record_event(
        "admin",
        "user_role_updated",
        level="warning",
        details={
            "actor": actor.get("email") or actor.get("google_sub"),
            "target": google_sub,
            "is_admin": next_is_admin,
        },
        wait=True,
    )
    return {"ok": True, "user": updated}


@router.delete("/users/{google_sub}")
async def delete_user(google_sub: str, request: Request):
    actor = await _require_admin_mutation(request)
    target = await users_repo.get_user(google_sub)
    if not target:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    if google_sub == actor.get("google_sub"):
        raise HTTPException(status_code=400, detail="현재 로그인한 관리자 계정은 삭제할 수 없습니다.")
    if target.get("is_admin") and await users_repo.count_admin_users() <= 1:
        raise HTTPException(status_code=400, detail="마지막 관리자는 삭제할 수 없습니다.")
    deleted = await users_repo.delete_user(google_sub)
    if not deleted:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    await observability.record_event(
        "admin",
        "user_deleted",
        level="warning",
        details={
            "actor": actor.get("email") or actor.get("google_sub"),
            "target": google_sub,
            "target_email": target.get("email"),
        },
        wait=True,
    )
    return {"ok": True}


@router.get("/portfolio-search")
async def search_portfolios(
    request: Request,
    q: str = Query("", min_length=0),
    limit: int = Query(50, ge=1, le=200),
):
    await _require_admin(request)
    rows = await users_repo.search_user_portfolios(q, limit=limit)
    for row in rows:
        row["portfolio_url"] = _admin_portfolio_url(row["google_sub"])
    return {"query": q, "rows": rows}


def _html_page_escape(value) -> str:
    return html.escape(str(value or ""), quote=True)


def _render_admin_portfolio_page(user: dict, items: list[dict], *, actor: dict) -> str:
    safe_name = _html_page_escape(user.get("name") or user.get("email") or user.get("google_sub"))
    total_cost = 0.0
    for item in items:
        try:
            total_cost += float(item.get("quantity") or 0) * float(item.get("avg_price") or 0)
        except (TypeError, ValueError):
            pass
    rows = "\n".join(
        f"""
        <tr>
          <td><strong>{_html_page_escape(item.get("stock_name") or item.get("stock_code"))}</strong><span>{_html_page_escape(item.get("stock_code"))}</span></td>
          <td>{_html_page_escape(item.get("group_name") or "-")}</td>
          <td class="num">{float(item.get("quantity") or 0):,.2f}</td>
          <td class="num">{float(item.get("avg_price") or 0):,.2f} {_html_page_escape(item.get("currency") or "")}</td>
          <td>{_html_page_escape((item.get("created_at") or "")[:10])}</td>
        </tr>
        """
        for item in items
    ) or '<tr><td colspan="5" class="empty">포트폴리오 보유 종목이 없습니다.</td></tr>'
    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{safe_name} 포트폴리오</title>
  <style>
    :root {{ color-scheme: light; --bg:#f6f8fb; --surface:#fff; --line:#dbe2ea; --text:#111827; --muted:#64748b; --accent:#0f766e; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; min-height:100vh; background:var(--bg); color:var(--text); font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif; }}
    main {{ width:min(1120px, calc(100% - 32px)); margin:0 auto; padding:28px 0 48px; }}
    header {{ display:flex; justify-content:space-between; gap:16px; align-items:flex-start; margin-bottom:18px; }}
    h1 {{ margin:0; font-size:24px; line-height:1.2; letter-spacing:0; }}
    .sub {{ color:var(--muted); font-size:13px; margin-top:6px; }}
    .badge {{ display:inline-flex; align-items:center; min-height:24px; padding:0 10px; border:1px solid rgba(15,118,110,.25); border-radius:6px; color:var(--accent); background:rgba(15,118,110,.08); font-size:12px; font-weight:700; }}
    .summary {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:10px; margin-bottom:16px; }}
    .metric {{ background:var(--surface); border:1px solid var(--line); border-radius:8px; padding:14px 16px; }}
    .metric span {{ display:block; color:var(--muted); font-size:12px; margin-bottom:6px; }}
    .metric strong {{ font-size:20px; font-variant-numeric:tabular-nums; }}
    table {{ width:100%; border-collapse:collapse; background:var(--surface); border:1px solid var(--line); border-radius:8px; overflow:hidden; }}
    th,td {{ padding:11px 13px; border-bottom:1px solid var(--line); text-align:left; font-size:13px; }}
    th {{ color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.04em; background:#f8fafc; }}
    td strong {{ display:block; font-size:13px; }}
    td span {{ color:var(--muted); font-size:12px; }}
    .num {{ text-align:right; font-variant-numeric:tabular-nums; }}
    .empty {{ text-align:center; color:var(--muted); padding:28px; }}
    a {{ color:var(--accent); text-decoration:none; font-weight:700; }}
    @media (max-width:720px) {{ main {{ width:calc(100% - 20px); padding-top:16px; }} header,.summary {{ display:block; }} .metric {{ margin-bottom:8px; }} table {{ display:block; overflow:auto; }} }}
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>{safe_name} 포트폴리오</h1>
        <div class="sub">{_html_page_escape(user.get("email"))} · google_sub {_html_page_escape(user.get("google_sub"))}</div>
        <div class="sub">열람 관리자: {_html_page_escape(actor.get("email") or actor.get("google_sub"))}</div>
      </div>
      <div><span class="badge">내부망 전용</span></div>
    </header>
    <section class="summary">
      <div class="metric"><span>보유 종목</span><strong>{len(items):,}</strong></div>
      <div class="metric"><span>매입 원금 합계</span><strong>{total_cost:,.0f}</strong></div>
      <div class="metric"><span>최근 로그인</span><strong>{_html_page_escape((user.get("last_login_at") or "-")[:10])}</strong></div>
    </section>
    <table>
      <thead><tr><th>종목</th><th>그룹</th><th class="num">수량</th><th class="num">평균단가</th><th>등록일</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
    <p class="sub"><a href="/admin.html">관리자 콘솔로 돌아가기</a></p>
  </main>
</body>
</html>"""


@router.get("/users/{google_sub}/portfolio.html", response_class=Response)
async def user_portfolio_page(google_sub: str, request: Request):
    actor = await _require_internal_portfolio_viewer(request)
    user = await users_repo.get_user(google_sub)
    if not user:
        raise HTTPException(status_code=404, detail="사용자를 찾을 수 없습니다.")
    items = await portfolio_repo.get_portfolio(google_sub)
    return Response(
        content=_render_admin_portfolio_page(user, items, actor=actor),
        media_type="text/html; charset=utf-8",
    )


# ---------------------------------------------------------------------------
# Linked project config management
# ---------------------------------------------------------------------------

@router.get("/linked-project-configs")
async def linked_project_configs(request: Request):
    await _require_admin(request)
    return await asyncio.to_thread(linked_project_admin.list_project_configs)


@router.get("/linked-project-configs/{project_key}")
async def linked_project_config(project_key: str, request: Request):
    await _require_admin(request)
    try:
        return await asyncio.to_thread(linked_project_admin.get_project_config, project_key)
    except linked_project_admin.LinkedProjectConfigError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.put("/linked-project-configs/{project_key}")
async def save_linked_project_config(project_key: str, request: Request, payload: dict = Body(...)):
    user = await _require_admin_mutation(request)
    config = payload.get("config") if isinstance(payload, dict) else None
    if config is None:
        raise HTTPException(status_code=400, detail="config payload is required.")
    try:
        saved = linked_project_admin.save_project_config(project_key, config)
    except linked_project_admin.LinkedProjectConfigError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    await observability.record_event(
        "admin",
        "linked_project_config_saved",
        level="info",
        details={
            "actor": user.get("email") or user.get("google_sub"),
            "project_key": project_key,
            "count": saved.get("summary", {}).get("count"),
            "config_path": saved.get("configPath"),
        },
        wait=True,
    )
    return saved


# ---------------------------------------------------------------------------
# AI operations: key status, model registry, usage
# ---------------------------------------------------------------------------

@router.get("/ai-config")
async def ai_admin_config(request: Request, days: int = Query(30, ge=1, le=365)):
    await _require_admin(request)
    return await ai_config.ai_admin_config(days=days)


@router.put("/ai-config/key")
async def save_ai_key(request: Request, payload: dict = Body(...)):
    user = await _require_admin_mutation(request)
    key = str((payload or {}).get("openrouter_api_key") or "").strip()
    try:
        await ai_config.set_openrouter_key(key, user.get("email") or user.get("google_sub"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    await observability.record_event(
        "admin",
        "ai_key_saved",
        level="info",
        details={"actor": user.get("email") or user.get("google_sub"), "provider": "openrouter"},
        wait=True,
    )
    return await ai_config.ai_admin_config()


@router.delete("/ai-config/key")
async def delete_ai_key(request: Request):
    user = await _require_admin_mutation(request)
    await ai_config.delete_openrouter_key()
    await observability.record_event(
        "admin",
        "ai_key_deleted",
        level="warning",
        details={"actor": user.get("email") or user.get("google_sub"), "provider": "openrouter"},
        wait=True,
    )
    return await ai_config.ai_admin_config()


@router.put("/ai-config/models")
async def save_ai_models(request: Request, payload: dict = Body(...)):
    user = await _require_admin_mutation(request)
    models = payload.get("models") if isinstance(payload, dict) else None
    if not isinstance(models, dict):
        raise HTTPException(status_code=400, detail="models object is required.")
    try:
        await ai_config.save_feature_models(models, user.get("email") or user.get("google_sub"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    await observability.record_event(
        "admin",
        "ai_models_saved",
        level="info",
        details={
            "actor": user.get("email") or user.get("google_sub"),
            "features": sorted(models),
        },
        wait=True,
    )
    return await ai_config.ai_admin_config()


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
    service_started, runner = await asyncio.gather(
        asyncio.to_thread(_service_start_timestamp),
        asyncio.to_thread(_runner_status),
    )
    return {
        "build": _BUILD_INFO,
        "service_started": service_started,
        "actions_runner": runner,
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
    are enforced inside system_events_repo.get_system_events.
    """
    await _require_admin(request)
    since = None
    if hours is not None and hours > 0:
        since = (datetime.now() - timedelta(hours=hours)).isoformat(timespec="seconds")
    rows = await system_events_repo.get_system_events(
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
    summary = await system_events_repo.summarize_system_events(since)
    # Attach "latest tick" per known subsystem so the card can show "15 min
    # ago" vs a cold "no tick in 3 days" state at a glance.
    known_sources = [
        "snapshot_nav", "snapshot_intraday",
        "wiki_ingestion", "benchmark_history",
    ]
    latest: dict[str, dict | None] = {}
    for src in known_sources:
        row = await system_events_repo.get_latest_event(src)
        latest[src] = _parse_event_row(row) if row else None
    # 데이터 품질 점검(data-quality.timer)의 최신 요약 이벤트 — details 에
    # 전체 결과(counts + results)가 통째로 들어 있어, 패널은 이 한 행만으로
    # '최근 점검 + 실패 항목'을 그릴 수 있다.
    dq_row = await system_events_repo.get_latest_event("data_quality", "check_summary")
    data_quality = _parse_event_row(dq_row) if dq_row else None
    return {"hours": hours, "by_source": summary, "latest": latest, "data_quality": data_quality}


@router.get("/http-metrics")
async def http_metrics(request: Request, hours: float = 24):
    """Per-endpoint view of recorded slow / 5xx HTTP requests over the last
    `hours`. Source is the latency-observer middleware (observability
    source='http'). Only the problematic tail is recorded, so the UI labels
    these as slow/error counts rather than average response time."""
    await _require_admin(request)
    since = (datetime.now() - timedelta(hours=hours)).isoformat(timespec="seconds")
    endpoints = await system_events_repo.summarize_http_metrics(since)
    return {"hours": hours, "endpoints": endpoints}


@router.get("/timeseries")
async def admin_timeseries(request: Request, hours: float = Query(24, ge=1, le=168)):
    """Chart-friendly hourly observability buckets for the admin console."""
    await _require_admin(request)
    since = (datetime.now() - timedelta(hours=hours)).isoformat(timespec="seconds")
    events, http = await asyncio.gather(
        system_events_repo.summarize_event_timeseries(since),
        system_events_repo.summarize_http_timeseries(since),
    )
    return {"hours": hours, "events": events, "http": http}


# ---------------------------------------------------------------------------
# Subsystem diagnostics — /diag/<subsystem>
# ---------------------------------------------------------------------------

@router.post("/refresh-foreign-dividends")
async def refresh_foreign_dividends_endpoint(request: Request):
    """yfinance 로부터 포트폴리오 내 해외 종목들의 trailing annual
    dividend 를 일괄 fetch + KRW 환산 + upsert. source='manual' 인
    수동 override 는 건드리지 않음."""
    user = await _require_admin_mutation(request)
    import foreign_dividends
    result = await foreign_dividends.refresh_foreign_dividends()
    result["total_cached"] = await foreign_dividends_repo.get_foreign_dividends_count()
    await observability.record_event(
        "admin",
        "foreign_dividends_refreshed",
        level="info",
        details={
            "actor": user.get("email") or user.get("google_sub"),
            "updated": result.get("updated") or result.get("success") or result.get("count"),
            "total_cached": result.get("total_cached"),
        },
        wait=True,
    )
    return result


@router.get("/foreign-dividends")
async def list_foreign_dividends_endpoint(request: Request):
    """관리자 UI 의 목록 뷰용. 수동 override row 가 먼저 오도록 정렬."""
    await _require_admin(request)
    return await foreign_dividends_repo.list_foreign_dividends()


@router.get("/preferred-dividends")
async def list_preferred_dividends_endpoint(request: Request):
    """관리자 UI 의 우선주 배당 시트 캐시 조회용.

    This is intentionally separate from preferredSpread's pair config:
    the dividend sheet can know about a preferred stock before the pair
    config is updated. Showing both lets the admin spot drift immediately.
    """
    await _require_admin(request)
    return await portfolio_repo.list_preferred_dividends()


@router.post("/foreign-dividend")
async def upsert_foreign_dividend_endpoint(request: Request, payload: dict = Body(...)):
    """관리자 수동 배당 입력. body: {stock_code, dps_krw, note?}.
    source='manual' 로 저장되어 이후 auto refresh 에서 덮어쓰지 않음.
    dps_krw 0 허용 (의도적으로 '배당 없음' 명시)."""
    user = await _require_admin_mutation(request)
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
    await foreign_dividends_repo.upsert_foreign_dividend_manual(code, dps, note)
    await observability.record_event(
        "admin",
        "foreign_dividend_upserted",
        level="info",
        stock_code=code,
        details={
            "actor": user.get("email") or user.get("google_sub"),
            "dps_krw": dps,
            "has_note": bool(note),
        },
        wait=True,
    )
    return {"ok": True, "stock_code": code, "dps_krw": dps, "note": note, "source": "manual"}


@router.delete("/foreign-dividend/{stock_code}")
async def delete_foreign_dividend_endpoint(request: Request, stock_code: str):
    """수동/자동 entry 제거. 이후 auto refresh 에서 다시 채워질 수 있음."""
    user = await _require_admin_mutation(request)
    code = stock_code.strip().upper()
    deleted = await foreign_dividends_repo.delete_foreign_dividend(code)
    if not deleted:
        raise HTTPException(status_code=404, detail="해당 종목의 배당 entry 가 없습니다.")
    await observability.record_event(
        "admin",
        "foreign_dividend_deleted",
        level="warning",
        stock_code=code,
        details={"actor": user.get("email") or user.get("google_sub")},
        wait=True,
    )
    return {"ok": True, "stock_code": stock_code}


@router.post("/refresh-preferred-dividends")
async def refresh_preferred_dividends_endpoint(request: Request):
    """수동 트리거 — Google Sheet Data!AI 컬럼을 즉시 재동기화. 주기
    루프(12h)가 알아서 돌지만, 시트 관리자가 방금 값을 바꾼 직후에는
    기다리지 않고 바로 반영하고 싶을 때 쓰기 위함."""
    user = await _require_admin_mutation(request)
    import preferred_dividends
    result = await preferred_dividends.refresh_preferred_dividends()
    # Attach current cached row count so the dashboard can show before/after.
    result["total_cached"] = await portfolio_repo.get_preferred_dividends_count()
    await observability.record_event(
        "admin",
        "preferred_dividends_refreshed",
        level="info",
        details={
            "actor": user.get("email") or user.get("google_sub"),
            "rows": result.get("rows") or result.get("updated") or result.get("count"),
            "total_cached": result.get("total_cached"),
        },
        wait=True,
    )
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
