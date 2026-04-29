import asyncio
import json
import logging
import os
import socket
import time
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

ENV_PATH = Path(__file__).parent / ".kis.env"

# Load deployment env before importing modules that freeze config at import
# time. Lifespan reload is left in place as an idempotent safety net.
if ENV_PATH.exists():
    load_dotenv(ENV_PATH, override=True)

import cache
import ai_config
import dart_client
import integrations
import kis_key_manager
import kis_proxy_client
import kis_ws_manager
from routes import auth_router, analysis_router, reports_router, stocks_router, cache_router, portfolio_router, ws_quotes_router, nps_router, backtest_router, admin_router, insights_router, dart_review_router
from routes import portfolio as portfolio_routes
from routes.internal import router as internal_router
from routes.wiki import router as wiki_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"

# Asset version for cache busting — use short git hash, fall back to timestamp
def _get_asset_version() -> str:
    import subprocess
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=Path(__file__).parent, stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        return str(int(time.time()))

ASSET_VERSION = _get_asset_version()

# --- systemd sd_notify (self-contained, no extra deps) ---
def _sd_notify(msg: str) -> None:
    addr = os.environ.get("NOTIFY_SOCKET")
    if not addr:
        return
    try:
        if addr[0] == "@":
            addr = "\0" + addr[1:]
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as s:
            s.connect(addr)
            s.sendall(msg.encode("utf-8"))
    except Exception:
        pass


# Updated by the watchdog/healthz loop; if this stalls, the loop is blocked.
_last_loop_tick: float = 0.0


async def _watchdog_loop():
    global _last_loop_tick
    interval = 10
    while True:
        _last_loop_tick = time.monotonic()
        _sd_notify("WATCHDOG=1")
        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            break


@asynccontextmanager
async def lifespan(app: FastAPI):
    if ENV_PATH.exists():
        load_dotenv(ENV_PATH, override=True)
    kis_key_manager.load_keys()

    await kis_proxy_client.init_client()
    await cache.init_db()
    await ai_config.migrate_legacy_model_defaults()
    await cache.delete_expired_sessions()

    # Ensure the wiki PDF cache directory exists. Creating it at startup
    # avoids a systemd ReadWritePaths chicken-and-egg (the namespace is
    # set up before the script runs).
    (Path(__file__).parent / "data" / "pdf_cache").mkdir(parents=True, exist_ok=True)
    try:
        needs_corp_refresh = not await cache.is_corp_codes_loaded() or await cache.corp_codes_need_refresh()
    except Exception as e:
        logger.error(f"corp_codes 상태 확인 실패: {e}")
        needs_corp_refresh = False
    if needs_corp_refresh:
        logger.info("corp_codes 테이블을 DART 기준으로 갱신합니다...")
        try:
            codes = await asyncio.wait_for(dart_client.fetch_corp_codes(), timeout=45)
            await cache.save_corp_codes(codes)
            logger.info(f"{len(codes)}개 상장사 코드를 저장했습니다.")
        except asyncio.TimeoutError:
            logger.error("corp_codes 다운로드 타임아웃 — 기존 캐시로 계속 진행합니다.")
        except Exception as e:
            logger.error(f"corp_codes 다운로드 실패: {e}")

    _sd_notify("READY=1")
    watchdog_task = asyncio.create_task(_watchdog_loop())
    insight_warmup_task = None
    if os.environ.get("PORTFOLIO_INSIGHT_STARTUP_WARMUP", "0") == "1":
        insight_warmup_task = asyncio.create_task(
            portfolio_routes.warm_asset_insight_common(
                initial_delay_seconds=float(os.environ.get("PORTFOLIO_INSIGHT_WARMUP_DELAY_S", "90"))
            )
        )

    # Continuous wiki ingestion. Interval is configurable via env (default
    # 30 min) and can be set to 0 to disable entirely (useful in tests /
    # during dev). A short initial delay keeps startup snappy.
    import wiki_ingestion
    wiki_stop = asyncio.Event()
    wiki_interval = float(os.environ.get("WIKI_INGEST_INTERVAL_S", "0"))
    wiki_task: asyncio.Task | None = None
    if wiki_interval > 0:
        wiki_task = asyncio.create_task(
            wiki_ingestion.run_background_loop(
                wiki_stop,
                interval_seconds=wiki_interval,
                initial_delay_seconds=float(os.environ.get("WIKI_INGEST_INITIAL_DELAY_S", "60")),
            )
        )

    # Optional in-process DART review loop. Production normally uses the
    # systemd dart-review-ingestion.timer to call the same internal
    # pipeline; keep this disabled by default to avoid double scheduling.
    import dart_report_review
    dart_review_stop = asyncio.Event()
    dart_review_interval = float(os.environ.get("DART_REVIEW_INTERVAL_S", "0"))
    dart_review_task: asyncio.Task | None = None
    if dart_review_interval > 0:
        dart_review_task = asyncio.create_task(
            dart_report_review.run_background_loop(
                dart_review_stop,
                interval_seconds=dart_review_interval,
                target_limit=int(os.environ.get("DART_REVIEW_TARGET_LIMIT", "12")),
                initial_delay_seconds=float(os.environ.get("DART_REVIEW_INITIAL_DELAY_S", "90")),
            )
        )

    # 우선주 배당 Google Sheet 는 연 1회 공시 기반 + 시트 수동 관리라서
    # 자동 refresh 주기를 돌리지 않는다. /api/admin/refresh-preferred-
    # dividends 수동 엔드포인트로만 동기화 — DB 의 preferred_dividends
    # 테이블은 persist 되므로 한 번 채워두면 계속 유효.

    # Observability event-log pruner. system_events is append-only during
    # normal operation; this loop trims rows older than 30 days every 6
    # hours plus a row-count safety cap. Cheap — most iterations are a
    # no-op DELETE.
    import observability
    obs_stop = asyncio.Event()
    obs_task = asyncio.create_task(
        observability.run_prune_loop(
            obs_stop,
            interval_seconds=float(os.environ.get("OBS_PRUNE_INTERVAL_S", str(6 * 3600))),
            max_age_days=int(os.environ.get("OBS_MAX_AGE_DAYS", "30")),
            max_rows=int(os.environ.get("OBS_MAX_ROWS", "100000")),
        )
    )
    try:
        yield
    finally:
        _sd_notify("STOPPING=1")
        watchdog_task.cancel()
        if insight_warmup_task:
            insight_warmup_task.cancel()
            try:
                await insight_warmup_task
            except (asyncio.CancelledError, Exception):
                pass
        try:
            await watchdog_task
        except (asyncio.CancelledError, Exception):
            pass
        # Stop wiki loop cleanly so an in-flight LLM call can finish.
        wiki_stop.set()
        if wiki_task is not None:
            try:
                await asyncio.wait_for(wiki_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                wiki_task.cancel()
        dart_review_stop.set()
        if dart_review_task is not None:
            try:
                await asyncio.wait_for(dart_review_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                dart_review_task.cancel()
        obs_stop.set()
        try:
            await asyncio.wait_for(obs_task, timeout=2.0)
        except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
            obs_task.cancel()
        await kis_ws_manager.stop_all()
        await kis_proxy_client.close_client()
        await cache.close_db()


app = FastAPI(title="한국 주식 가치투자 분석", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost",
        "http://127.0.0.1",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
        "https://ducklove.github.io",
        "https://cantabile.tplinkdns.com:3691",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)

app.include_router(auth_router)
app.include_router(analysis_router)
app.include_router(reports_router)
app.include_router(stocks_router)
app.include_router(cache_router)
app.include_router(portfolio_router)
app.include_router(ws_quotes_router)
app.include_router(nps_router)
app.include_router(backtest_router)
app.include_router(insights_router)
app.include_router(dart_review_router)
app.include_router(internal_router)
app.include_router(wiki_router)
# Admin dashboard used to live on a separate port (admin_app.py :3692) so
# it could stay localhost-only without auth. That setup had a crucial
# flaw: deploy.sh only restarts value-invest.service, so the admin server
# was silently running stale code on every push — /api/admin/diag/wiki
# returned 404 on a fresh deploy because the standalone process never
# picked up the new route.
#
# Security posture is maintained through _require_admin() (Google OAuth +
# is_admin=1 in the users table), which is strictly stronger than
# "localhost only" anyway once the user hits the box via the public
# hostname. Keep admin_app.py around as an alternate entry point for
# strict-localhost deployments, but fold the router into the main app so
# regular deploys always ship a consistent admin surface.
app.include_router(admin_router)


@app.get("/healthz")
async def healthz():
    # Liveness: this handler runs on the event loop, so a 200 response
    # implies the loop is responsive. Also expose loop tick lag for debugging.
    now = time.monotonic()
    lag = max(0.0, now - _last_loop_tick) if _last_loop_tick else None
    return JSONResponse({"status": "ok", "asset_version": ASSET_VERSION, "loop_lag_s": lag})


@app.get("/")
async def index():
    import re
    html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
    # Append ?v=<hash> to .css and .js references for cache busting
    html = re.sub(
        r'((?:href|src)=["\'])(\./(?:styles\.css|js/[^"\']+\.js))',
        rf'\1\2?v={ASSET_VERSION}',
        html,
    )
    return Response(content=html, media_type="text/html")


# Shareable deep links: `/analysis?code=005930` 같은 URL 을 다른 사이트에
# 서 직접 연결할 수 있도록 path-based 라우트 제공. SPA 이므로 실제 컨텐츠
# 는 동일한 index.html 이고, 클라이언트 (app-main.js::initApp) 가
# window.location.pathname 을 보고 알맞은 뷰를 활성화한다. 화이트리스트
# 방식 — 정확한 path 만 매칭해 /api/* 나 /js/* 와의 충돌을 원천 차단.
@app.get("/analysis")
@app.get("/portfolio")
@app.get("/nps")
@app.get("/labs")
@app.get("/backtest")
@app.get("/insights")
async def spa_pages():
    return await index()


@app.get("/app-config.js")
async def app_config():
    payload = integrations.build_app_config(api_base_url="")
    return Response(
        content=f"window.APP_CONFIG = {json.dumps(payload, ensure_ascii=False)};",
        media_type="application/javascript",
    )


@app.get("/api/integrations")
async def integrations_status():
    return JSONResponse(integrations.build_public_integrations())


@app.get("/styles.css")
async def styles():
    return FileResponse(STATIC_DIR / "styles.css", media_type="text/css")


@app.get("/admin.html")
async def admin_page():
    """Serve the admin dashboard from the main app explicitly.

    Previously admin lived only under /static/admin.html (and the legacy
    admin_app.py :3692 standalone). Whichever path a reverse proxy /
    bookmark / Caddy config pointed at, the moving pieces caused /api/
    admin/diag/wiki to 404 inconsistently. Pinning a canonical
    /admin.html served by the same process that owns /api/admin/*
    guarantees the admin JS and its endpoints always ship together.
    """
    html = (STATIC_DIR / "admin.html").read_text(encoding="utf-8")
    # Same cache-busting pattern as the main index — prevents stale JS
    # from being used after a deploy bumps endpoint contracts.
    import re
    html = re.sub(
        r'((?:href|src)=["\'])(/(?:styles\.css|js/[^"\']+\.js))',
        rf'\1\2?v={ASSET_VERSION}',
        html,
    )
    return Response(content=html, media_type="text/html")


# 정적 파일 서빙 (CSS, JS 등)
app.mount("/js", StaticFiles(directory=str(STATIC_DIR / "js")), name="js")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
