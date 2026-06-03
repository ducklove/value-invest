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

    # HTML 문서는 캐시하지 않는다(자산은 ?v=<커밋해시>로 캐시버스팅하지만, HTML
    # 자체가 브라우저에 캐시되면 옛 ?v= 를 가리켜 배포가 반영되지 않는다).
    _HTML_NO_CACHE = {"Cache-Control": "no-cache, must-revalidate"}

    async def index() -> Response:
        html = (static_dir / "index.html").read_text(encoding="utf-8")
        html = _with_asset_version(html, asset_version, relative=True)
        return Response(content=html, media_type="text/html", headers=_HTML_NO_CACHE)

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

    async def favicon() -> FileResponse:
        # Single SVG mark serves both /favicon.svg (linked) and /favicon.ico
        # (browsers' default probe) so any page gets the icon without a 404.
        return FileResponse(static_dir / "favicon.svg", media_type="image/svg+xml")

    async def admin_page() -> Response:
        html = (static_dir / "admin.html").read_text(encoding="utf-8")
        html = _with_asset_version(html, asset_version, relative=False)
        return Response(content=html, media_type="text/html", headers=_HTML_NO_CACHE)

    app.add_api_route("/", index, methods=["GET"])
    for path in SPA_PATHS:
        app.add_api_route(path, spa_pages, methods=["GET"])
    app.add_api_route("/app-config.js", app_config, methods=["GET"])
    app.add_api_route("/api/integrations", integrations_status, methods=["GET"])
    app.add_api_route("/styles.css", styles, methods=["GET"])
    app.add_api_route("/favicon.svg", favicon, methods=["GET"])
    app.add_api_route("/favicon.ico", favicon, methods=["GET"])
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

