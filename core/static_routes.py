from __future__ import annotations

import json
import re
from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Response
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

import integrations
from core.config import AppSettings


SPA_PATHS = ("/analysis", "/portfolio", "/nps", "/labs", "/backtest", "/insights")


StaticHandlers = dict[str, Callable[[], Awaitable[Response]]]


def _with_asset_version(html: str, asset_version: str, *, relative: bool) -> str:
    prefix = r"\./" if relative else r"/"
    return re.sub(
        rf'((?:href|src)=["\'])({prefix}(?:styles\.css|js/[^"\']+\.js))',
        rf"\1\2?v={asset_version}",
        html,
    )


def register_static_routes(app: FastAPI, settings: AppSettings, asset_version: str) -> StaticHandlers:
    static_dir = settings.project_root / "static"

    async def index() -> Response:
        html = (static_dir / "index.html").read_text(encoding="utf-8")
        html = _with_asset_version(html, asset_version, relative=True)
        return Response(content=html, media_type="text/html")

    async def spa_pages() -> Response:
        return await index()

    async def app_config() -> Response:
        payload = integrations.build_app_config(api_base_url=settings.public_api_base_url)
        return Response(
            content=f"window.APP_CONFIG = {json.dumps(payload, ensure_ascii=False)};",
            media_type="application/javascript",
        )

    async def integrations_status() -> JSONResponse:
        return JSONResponse(integrations.build_public_integrations())

    async def styles() -> FileResponse:
        return FileResponse(static_dir / "styles.css", media_type="text/css")

    async def admin_page() -> Response:
        html = (static_dir / "admin.html").read_text(encoding="utf-8")
        html = _with_asset_version(html, asset_version, relative=False)
        return Response(content=html, media_type="text/html")

    app.add_api_route("/", index, methods=["GET"])
    for path in SPA_PATHS:
        app.add_api_route(path, spa_pages, methods=["GET"])
    app.add_api_route("/app-config.js", app_config, methods=["GET"])
    app.add_api_route("/api/integrations", integrations_status, methods=["GET"])
    app.add_api_route("/styles.css", styles, methods=["GET"])
    app.add_api_route("/admin.html", admin_page, methods=["GET"])
    app.mount("/js", StaticFiles(directory=str(static_dir / "js")), name="js")
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    return {
        "index": index,
        "spa_pages": spa_pages,
        "app_config": app_config,
        "integrations_status": integrations_status,
        "styles": styles,
        "admin_page": admin_page,
    }

