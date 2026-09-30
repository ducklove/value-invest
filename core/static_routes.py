from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from urllib.parse import parse_qs

from fastapi import FastAPI, Request, Response
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from core.config import AppSettings
from core.runtime import AssetManifest
from services.ecosystem import integrations

# switchView 가 pushState 하는 모든 경로(portfolio-shell.js PF_VIEW_PATHS)는 여기에도
# 있어야 한다 — 빠지면 그 탭에서 새로고침·북마크 진입이 404 JSON 으로 떨어진다.
SPA_PATHS = ("/investing", "/analysis", "/portfolio", "/nps", "/labs", "/tools", "/insights", "/screener", "/masters", "/bonds", "/quant")


StaticHandlers = dict[str, Callable[[], Awaitable[Response]]]


# 버전을 찍는 자산 URL: ./css/*.css, ./js/*.js, ./static/ecosystem/*.{js,css}.
# 이미 쿼리/프래그먼트가 붙은 URL 은 건드리지 않는다(따옴표 직전에서 끝나야 매치).
_ASSET_URL_BODY = r'(?:css/[^"\'?#]+\.css|js/[^"\'?#]+\.js|static/ecosystem/[^"\'?#]+\.(?:css|js))'


def _asset_key(url_path: str) -> str:
    """Map a stamped URL path (``js/a.js``, ``static/ecosystem/x.css``) to its
    manifest key, which is relative to ``static/``."""
    return url_path[len("static/"):] if url_path.startswith("static/") else url_path


def _with_asset_version(
    html: str,
    asset_version: str,
    *,
    relative: bool,
    versions: Mapping[str, str] | None = None,
) -> str:
    """Append ``?v=`` to asset URLs in ``href``/``src``/``data-src`` attributes.

    With ``versions`` (manifest key → content hash) each file gets its own hash
    (F4); files missing from it fall back to ``asset_version`` (the git hash).
    """
    prefix = r"\./" if relative else r"/"
    pattern = re.compile(rf'((?:href|src)=["\'])({prefix})({_ASSET_URL_BODY})(?=["\'])')
    lookup = versions or {}

    def stamp(match: re.Match[str]) -> str:
        version = lookup.get(_asset_key(match.group(3)), asset_version)
        return f"{match.group(1)}{match.group(2)}{match.group(3)}?v={version}"

    return pattern.sub(stamp, html)


# ?v= 가 서버가 지금 찍는 값과 같은 자산은 내용이 바뀌면 URL 도 바뀐다 → 1년 immutable.
_IMMUTABLE_CACHE = "public, max-age=31536000, immutable"


def _query_version(scope) -> str:
    raw = scope.get("query_string") or b""
    try:
        values = parse_qs(raw.decode("latin-1")).get("v") or []
    except UnicodeDecodeError:
        return ""
    return values[0] if values else ""


class VersionedStaticFiles(StaticFiles):
    """StaticFiles that marks current ``?v=``-stamped responses immutable (O1).

    Only a version equal to the manifest's current stamp for that file earns
    ``immutable``; anything else (no ``?v=``, an old deploy's hash, a sibling's
    hand-maintained version) keeps StaticFiles' ETag/Last-Modified revalidation.
    """

    def __init__(self, *, directory: str, manifest: AssetManifest, key_prefix: str = "") -> None:
        super().__init__(directory=directory)
        self._manifest = manifest
        self._key_prefix = key_prefix

    async def get_response(self, path: str, scope) -> Response:
        response = await super().get_response(path, scope)
        version = _query_version(scope)
        if version and response.status_code in (200, 304):
            key = self._key_prefix + path.replace(os.sep, "/")
            if self._manifest.is_current(key, version):
                response.headers["Cache-Control"] = _IMMUTABLE_CACHE
        return response


def _etag_matches(if_none_match: str | None, etag: str) -> bool:
    if not if_none_match:
        return False
    wanted = etag[2:] if etag.startswith("W/") else etag
    for candidate in if_none_match.split(","):
        candidate = candidate.strip()
        if candidate == "*":
            return True
        if (candidate[2:] if candidate.startswith("W/") else candidate) == wanted:
            return True
    return False


class _RenderedHtml:
    """Per-process cache of one HTML shell with its ``?v=`` stamps applied.

    The stamps only change when the manifest does, so production renders each
    page once. In development (``manifest.live``) the manifest and the source
    file are re-checked per request so edits show without a restart.
    """

    def __init__(self, source: Path, manifest: AssetManifest, *, relative: bool) -> None:
        self._source = source
        self._manifest = manifest
        self._relative = relative
        self._key: tuple[int, int] | None = None
        self._html = ""
        self._etag = ""

    def get(self) -> tuple[str, str]:
        if self._manifest.live:
            self._manifest.refresh()
            key = (self._source.stat().st_mtime_ns, self._manifest.generation)
        else:
            key = (0, self._manifest.generation)
        if key != self._key or not self._html:
            html = self._source.read_text(encoding="utf-8")
            html = _with_asset_version(
                html,
                self._manifest.fallback,
                relative=self._relative,
                versions=self._manifest.versions(),
            )
            self._html = html
            # GZip 이 바이트 표현을 바꾸므로 약한 ETag(W/)로 내린다.
            self._etag = 'W/"' + hashlib.sha1(html.encode("utf-8")).hexdigest()[:20] + '"'
            self._key = key
        return self._html, self._etag


def register_static_routes(
    app: FastAPI,
    settings: AppSettings,
    asset_version: str,
    asset_manifest: AssetManifest | None = None,
) -> StaticHandlers:
    static_dir = settings.project_root / "static"
    if asset_manifest is None:
        asset_manifest = AssetManifest(static_dir, asset_version, live=settings.is_development)
    app.state.asset_manifest = asset_manifest
    index_page = _RenderedHtml(static_dir / "index.html", asset_manifest, relative=True)
    admin_html = _RenderedHtml(static_dir / "admin.html", asset_manifest, relative=False)

    # HTML 문서는 캐시하지 않는다(자산은 ?v=<파일별 콘텐츠 해시>로 캐시버스팅하지만,
    # HTML 자체가 브라우저에 캐시되면 옛 ?v= 를 가리켜 배포가 반영되지 않는다).
    # 대신 ETag 로 재검증해 바뀌지 않았으면 304 로 본문 전송을 아낀다.
    _HTML_NO_CACHE = {"Cache-Control": "no-cache, must-revalidate"}

    def _html_response(page: _RenderedHtml, request: Request | None) -> Response:
        html, etag = page.get()
        headers = {**_HTML_NO_CACHE, "ETag": etag}
        if request is not None and _etag_matches(request.headers.get("if-none-match"), etag):
            return Response(status_code=304, headers=headers)
        return Response(content=html, media_type="text/html", headers=headers)

    async def index(request: Request = None) -> Response:  # type: ignore[assignment]
        return _html_response(index_page, request)

    async def spa_pages(request: Request = None) -> Response:  # type: ignore[assignment]
        return await index(request)

    async def app_config() -> Response:
        payload = integrations.build_app_config(api_base_url=settings.public_api_base_url)
        return Response(
            content=f"window.APP_CONFIG = {json.dumps(payload, ensure_ascii=False)};",
            media_type="application/javascript",
        )

    async def integrations_status() -> JSONResponse:
        return JSONResponse(integrations.build_public_integrations())

    async def favicon() -> FileResponse:
        # Single SVG mark serves both /favicon.svg (linked) and /favicon.ico
        # (browsers' default probe) so any page gets the icon without a 404.
        return FileResponse(static_dir / "favicon.svg", media_type="image/svg+xml")

    async def manifest() -> FileResponse:
        return FileResponse(
            static_dir / "manifest.webmanifest",
            media_type="application/manifest+json",
        )

    async def service_worker() -> FileResponse:
        # no-cache so the browser revalidates /sw.js on every check and SW
        # updates land promptly after a deploy; Service-Worker-Allowed lets
        # the root-scope registration control the whole origin.
        return FileResponse(
            static_dir / "sw.js",
            media_type="application/javascript",
            headers={
                "Cache-Control": "no-cache, must-revalidate",
                "Service-Worker-Allowed": "/",
            },
        )

    async def admin_page(request: Request = None) -> Response:  # type: ignore[assignment]
        return _html_response(admin_html, request)

    app.add_api_route("/", index, methods=["GET"])
    for path in SPA_PATHS:
        app.add_api_route(path, spa_pages, methods=["GET"])
    app.add_api_route("/app-config.js", app_config, methods=["GET"])
    app.add_api_route("/api/integrations", integrations_status, methods=["GET"])
    app.add_api_route("/favicon.svg", favicon, methods=["GET"])
    app.add_api_route("/favicon.ico", favicon, methods=["GET"])
    app.add_api_route("/manifest.webmanifest", manifest, methods=["GET"])
    app.add_api_route("/sw.js", service_worker, methods=["GET"])
    app.add_api_route("/admin", admin_page, methods=["GET"])
    app.add_api_route("/admin/", admin_page, methods=["GET"])
    app.add_api_route("/admin.html", admin_page, methods=["GET"])
    app.mount("/js", VersionedStaticFiles(directory=str(static_dir / "js"), manifest=asset_manifest, key_prefix="js/"), name="js")
    app.mount("/css", VersionedStaticFiles(directory=str(static_dir / "css"), manifest=asset_manifest, key_prefix="css/"), name="css")
    app.mount("/static", VersionedStaticFiles(directory=str(static_dir), manifest=asset_manifest), name="static")

    return {
        "index": index,
        "spa_pages": spa_pages,
        "app_config": app_config,
        "integrations_status": integrations_status,
        "manifest": manifest,
        "service_worker": service_worker,
        "admin_page": admin_page,
    }
