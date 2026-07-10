from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from core.config import AppSettings, get_settings, load_environment
from core.errors import register_exception_handlers
from core.lifespan import app_lifespan
from core.runtime import RuntimeState, get_asset_version
from core.static_routes import register_static_routes


def _docs_path(path: str, settings: AppSettings) -> str | None:
    return path if settings.enable_docs else None


def _register_feature_routers(app: FastAPI) -> None:
    from routes import (
        admin_router,
        analysis_router,
        auth_router,
        cache_router,
        dart_review_router,
        insights_router,
        market_daily_router,
        masters_router,
        notifications_router,
        portfolio_router,
        reports_router,
        screener_router,
        stocks_router,
        ws_quotes_router,
    )
    from routes.accounts import router as accounts_router
    from routes.action_board import router as action_board_router
    from routes.dividend_calendar import router as dividend_calendar_router
    from routes.household_assets import router as household_assets_router
    from routes.internal import router as internal_router
    from routes.journal import router as journal_router
    from routes.portfolio_reports import router as portfolio_reports_router
    from routes.portfolio_risk import router as portfolio_risk_router
    from routes.rebalance import router as rebalance_router
    from routes.wiki import router as wiki_router

    for router in (
        auth_router,
        analysis_router,
        reports_router,
        stocks_router,
        cache_router,
        portfolio_router,
        action_board_router,
        accounts_router,
        portfolio_reports_router,
        portfolio_risk_router,
        rebalance_router,
        dividend_calendar_router,
        household_assets_router,
        journal_router,
        ws_quotes_router,
        insights_router,
        market_daily_router,
        dart_review_router,
        notifications_router,
        internal_router,
        wiki_router,
        screener_router,
        masters_router,
        admin_router,
    ):
        app.include_router(router)


def _slow_request_threshold_ms() -> float:
    """Read the slow-request threshold (ms) from the environment.

    Falls back to 1000ms on missing/garbage values so a typo in the env can
    never disable instrumentation outright."""
    try:
        return float(os.environ.get("SLOW_REQUEST_MS", "1000"))
    except (TypeError, ValueError):
        return 1000.0


_CSP_REPORT_ONLY = (
    "default-src 'self'; "
    "script-src 'self' 'unsafe-inline' 'unsafe-eval' "
    "https://accounts.google.com https://www.googletagmanager.com "
    "https://cdn.jsdelivr.net https://cdnjs.cloudflare.com https://unpkg.com; "
    "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://unpkg.com; "
    "img-src 'self' data: https:; "
    "font-src 'self' data: https:; "
    "connect-src 'self' https: wss: ws:; "
    "frame-src https:; "
    "object-src 'none'; "
    "base-uri 'self'; "
    "form-action 'self'; "
    "frame-ancestors 'none'"
)

_SECURITY_HEADERS = {
    "x-content-type-options": "nosniff",
    "x-frame-options": "DENY",
    "referrer-policy": "strict-origin-when-cross-origin",
    "permissions-policy": "camera=(), microphone=(), geolocation=()",
    "content-security-policy-report-only": _CSP_REPORT_ONLY,
}


class _SecurityHeadersMiddleware:
    """Attach baseline browser security headers without buffering responses."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                existing = {name.lower() for name, _ in message.get("headers", [])}
                headers = list(message.get("headers", []))
                for name, value in _SECURITY_HEADERS.items():
                    raw_name = name.encode("latin-1")
                    if raw_name not in existing:
                        headers.append((raw_name, value.encode("latin-1")))
                if scope.get("scheme") == "https" and b"strict-transport-security" not in existing:
                    headers.append((b"strict-transport-security", b"max-age=31536000; includeSubDomains"))
                message = {**message, "headers": headers}
            await send(message)

        await self.app(scope, receive, send_wrapper)


def _register_security_headers(app: FastAPI) -> None:
    app.add_middleware(_SecurityHeadersMiddleware)


class _RequestLatencyMiddleware:
    """Record slow `/api/*` calls and 5xx responses to the in-app event log.

    This is the data source the admin dashboard and `docs/project-health-review.md`
    ask for: "which endpoint is slow / failing", answered with measured durations
    instead of guesses. Design notes:

    * **Pure ASGI, not BaseHTTPMiddleware.** It only inspects the
      ``http.response.start`` message for the status code and never touches the
      body stream, so SSE / streaming endpoints (``/api/portfolio/quotes``, the
      analysis stream) keep flushing chunk-by-chunk. BaseHTTPMiddleware would
      risk buffering those.
    * **Time-to-first-byte.** Latency is measured up to ``http.response.start``.
      For normal JSON that is effectively the handler time; for a long-lived SSE
      stream it is the time to begin streaming, not the whole connection — so a
      multi-minute stream is not mislabeled "slow".
    * **Only `/api/*` is timed.** Static asset and SPA page serving is noise.
    * **A row is written only when slow (>= ``app.state.slow_request_ms``) or on
      error (5xx / unhandled exception).** Healthy fast traffic writes nothing,
      so a busy quote-polling client does not flood ``system_events``.
    * **Never breaks the request.** The write is fire-and-forget; the original
      response/exception is always what the caller sees.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or not scope.get("path", "").startswith("/api/"):
            await self.app(scope, receive, send)
            return

        started = time.perf_counter()
        captured: dict = {"status": None, "ms": None}

        async def send_wrapper(message):
            if message["type"] == "http.response.start" and captured["status"] is None:
                captured["status"] = message["status"]
                captured["ms"] = (time.perf_counter() - started) * 1000.0
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            # Unhandled — upstream turns it into a 500. Record and re-raise so
            # behavior is unchanged.
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            await self._record(scope, status=500, elapsed_ms=elapsed_ms, errored=True)
            raise

        status = captured["status"] if captured["status"] is not None else 200
        elapsed_ms = captured["ms"] if captured["ms"] is not None else (time.perf_counter() - started) * 1000.0
        await self._record(scope, status=status, elapsed_ms=elapsed_ms, errored=False)

    async def _record(self, scope, *, status: int, elapsed_ms: float, errored: bool) -> None:
        is_error = errored or status >= 500
        state = getattr(scope.get("app"), "state", None)
        threshold = getattr(state, "slow_request_ms", 1000.0)
        if not (is_error or elapsed_ms >= threshold):
            return
        import observability

        await observability.record_event(
            source="http",
            kind="error" if is_error else "slow",
            level="error" if is_error else "warning",
            details={
                "method": scope.get("method", ""),
                "path": scope.get("path", ""),
                "status": status,
                "duration_ms": round(elapsed_ms, 1),
            },
        )


def _register_latency_observer(app: FastAPI) -> None:
    app.state.slow_request_ms = _slow_request_threshold_ms()
    app.add_middleware(_RequestLatencyMiddleware)


def create_app(settings: AppSettings | None = None) -> FastAPI:
    settings = settings or get_settings()
    load_environment(settings.project_root)

    runtime = RuntimeState()
    asset_version = get_asset_version(settings.project_root)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with app_lifespan(app, settings, runtime):
            yield

    app = FastAPI(
        title=settings.app_title,
        lifespan=lifespan,
        docs_url=_docs_path("/docs", settings),
        redoc_url=_docs_path("/redoc", settings),
        openapi_url=_docs_path("/openapi.json", settings),
    )
    app.state.settings = settings
    app.state.runtime = runtime
    app.state.asset_version = asset_version

    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(settings.cors_allowed_origins),
        allow_credentials=True,
        # PATCH: 투자 일지 note 수정(routes/journal.py)이 사용 — 교차 출처
        # 프런트(GitHub Pages 등)의 preflight 가 거부되지 않게 허용한다.
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
    )

    register_exception_handlers(app)
    _register_security_headers(app)
    _register_latency_observer(app)
    _register_feature_routers(app)

    @app.get("/healthz")
    async def healthz():
        now = time.monotonic()
        lag = max(0.0, now - runtime.last_loop_tick) if runtime.last_loop_tick else None
        return JSONResponse({
            "status": "ok",
            "asset_version": asset_version,
            "environment": settings.environment,
            "loop_lag_s": lag,
        })

    app.state.static_handlers = register_static_routes(app, settings, asset_version)
    return app
