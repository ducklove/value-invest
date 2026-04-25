import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from starlette.requests import Request

import deps
from routes import cache_mgmt, auth, internal, portfolio


def _request(path: str = "/") -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": path,
        "headers": [],
        "query_string": b"",
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
        "scheme": "http",
    }
    return Request(scope)


def _request_with_headers(path: str = "/", headers: dict[str, str] | None = None, client_host: str = "127.0.0.1") -> Request:
    encoded_headers = [
        (key.lower().encode("latin-1"), value.encode("latin-1"))
        for key, value in (headers or {}).items()
    ]
    scope = {
        "type": "http",
        "method": "POST",
        "path": path,
        "headers": encoded_headers,
        "query_string": b"",
        "client": (client_host, 12345),
        "server": ("testserver", 80),
        "scheme": "http",
    }
    return Request(scope)


class MainRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_delete_cache_requires_login(self):
        request = _request("/api/cache/005930")
        with patch("routes.cache_mgmt.get_current_user", new=AsyncMock(return_value=None)):
            with self.assertRaises(HTTPException) as exc_info:
                await cache_mgmt.delete_cache("005930", request)
        self.assertEqual(exc_info.exception.status_code, 401)

    async def test_update_cache_order_requires_login(self):
        request = _request("/api/cache/order")
        with patch("routes.cache_mgmt.get_current_user", new=AsyncMock(return_value=None)):
            with self.assertRaises(HTTPException) as exc_info:
                await cache_mgmt.update_cache_order(request, {"stock_codes": ["005930"]})
        self.assertEqual(exc_info.exception.status_code, 401)

    async def test_login_page_redirects_authenticated_user(self):
        request = _request("/login")
        with patch("routes.auth.get_current_user", new=AsyncMock(return_value={"google_sub": "u1"})), \
             patch("routes.auth.auth_service.is_enabled", return_value=True):
            response = await auth.login_page(request)
        self.assertEqual(response.status_code, 303)

    def test_return_to_rejects_scheme_relative_url(self):
        self.assertEqual(auth._normalize_return_to("//evil.example/path"), "/")
        self.assertEqual(auth._normalize_return_to("/portfolio"), "/portfolio")

    def test_internal_rejects_forwarded_loopback_without_token(self):
        request = _request_with_headers(
            "/api/internal/snapshot/nav",
            headers={"X-Forwarded-For": "203.0.113.10"},
            client_host="127.0.0.1",
        )
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(HTTPException) as exc_info:
                internal._require_loopback(request)
        self.assertEqual(exc_info.exception.status_code, 403)

    def test_internal_accepts_configured_token_behind_proxy(self):
        request = _request_with_headers(
            "/api/internal/snapshot/nav",
            headers={"X-Forwarded-For": "203.0.113.10", "X-Internal-Token": "secret"},
            client_host="127.0.0.1",
        )
        with patch.dict("os.environ", {"INTERNAL_API_TOKEN": "secret"}, clear=True):
            internal._require_loopback(request)

    async def test_cashflow_invalid_amount_returns_400(self):
        request = _request_with_headers("/api/portfolio/cashflows")
        with patch("routes.portfolio.get_current_user", new=AsyncMock(return_value={"google_sub": "u1"})):
            with self.assertRaises(HTTPException) as exc_info:
                await portfolio.add_cashflow(request, {"type": "deposit", "amount": "not-a-number"})
        self.assertEqual(exc_info.exception.status_code, 400)

    def test_analysis_snapshot_staleness(self):
        self.assertTrue(deps.analysis_snapshot_is_stale(None))
        self.assertFalse(deps.analysis_snapshot_is_stale("2999-01-01T00:00:00"))

    async def test_spa_pages_serve_index_html(self):
        """Deep-link paths (/analysis, /portfolio, /nps, /backtest, /insights) must serve
        the same index.html the SPA uses so bookmarks and external links
        resolve correctly. Otherwise they'd 404 before the JS can read
        window.location.pathname and pick a tab."""
        import main
        # index() does file IO + regex — call spa_pages which delegates.
        response = await main.spa_pages()
        self.assertEqual(response.media_type, "text/html")
        body = response.body.decode("utf-8") if isinstance(response.body, bytes) else str(response.body)
        # The page shell always contains these anchor IDs.
        self.assertIn("id=\"analysisView\"", body)
        self.assertIn("id=\"insightsView\"", body)
        self.assertIn("switchView", body)

    async def test_spa_routes_registered(self):
        """Belt-and-suspenders: confirm the SPA path-routes are actually
        wired into the FastAPI app. If someone drops a decorator by accident,
        we catch it here before deploy."""
        import main
        registered = {route.path for route in main.app.routes if hasattr(route, "path")}
        for p in ("/analysis", "/portfolio", "/nps", "/backtest", "/insights"):
            self.assertIn(p, registered, f"deep-link route {p} missing")
