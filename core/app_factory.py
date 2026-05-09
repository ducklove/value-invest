from __future__ import annotations

import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from core.config import AppSettings, get_settings, load_environment
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
        backtest_router,
        cache_router,
        dart_review_router,
        insights_router,
        nps_router,
        portfolio_router,
        reports_router,
        stocks_router,
        ws_quotes_router,
    )
    from routes.internal import router as internal_router
    from routes.wiki import router as wiki_router

    for router in (
        auth_router,
        analysis_router,
        reports_router,
        stocks_router,
        cache_router,
        portfolio_router,
        ws_quotes_router,
        nps_router,
        backtest_router,
        insights_router,
        dart_review_router,
        internal_router,
        wiki_router,
        admin_router,
    ):
        app.include_router(router)


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
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Content-Type", "Authorization", "X-Requested-With"],
    )

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

