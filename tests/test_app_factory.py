import unittest

import httpx

from core.app_factory import create_app
from core.config import PROJECT_ROOT, AppSettings


class AppFactoryTests(unittest.IsolatedAsyncioTestCase):
    def _settings(self, *, environment: str = "production", enable_docs: bool = False) -> AppSettings:
        return AppSettings(
            environment=environment,
            project_root=PROJECT_ROOT,
            app_title="Test Compass",
            public_api_base_url="https://api.example.test",
            cors_allowed_origins=("https://app.example.test",),
            enable_docs=enable_docs,
        )

    async def test_create_app_uses_explicit_settings_for_runtime_routes(self):
        app = create_app(self._settings())

        self.assertEqual(app.title, "Test Compass")
        self.assertEqual(app.state.settings.public_api_base_url, "https://api.example.test")
        self.assertIn("spa_pages", app.state.static_handlers)

        app_config = await app.state.static_handlers["app_config"]()
        body = app_config.body.decode("utf-8")
        self.assertIn('"apiBaseUrl": "https://api.example.test"', body)

    def test_create_app_can_hide_docs_in_production_profile(self):
        app = create_app(self._settings(environment="production", enable_docs=False))
        paths = {route.path for route in app.routes if hasattr(route, "path")}

        self.assertNotIn("/docs", paths)
        self.assertNotIn("/redoc", paths)
        self.assertNotIn("/openapi.json", paths)

    def test_create_app_keeps_docs_available_when_enabled(self):
        app = create_app(self._settings(environment="development", enable_docs=True))
        paths = {route.path for route in app.routes if hasattr(route, "path")}

        self.assertIn("/docs", paths)
        self.assertIn("/redoc", paths)
        self.assertIn("/openapi.json", paths)

    def test_portfolio_order_route_precedes_dynamic_stock_put_route(self):
        app = create_app(self._settings())
        put_paths = [
            route.path
            for route in app.routes
            if hasattr(route, "path") and "PUT" in getattr(route, "methods", set())
        ]

        self.assertLess(
            put_paths.index("/api/portfolio/order"),
            put_paths.index("/api/portfolio/{stock_code}"),
        )

    async def test_security_headers_are_attached_to_http_responses(self):
        app = create_app(self._settings(environment="production", enable_docs=False))
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
        async with httpx.AsyncClient(transport=transport, base_url="https://testserver") as client:
            response = await client.get("/healthz")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")
        self.assertEqual(response.headers["x-frame-options"], "DENY")
        self.assertEqual(response.headers["referrer-policy"], "strict-origin-when-cross-origin")
        self.assertIn("camera=()", response.headers["permissions-policy"])
        self.assertIn("default-src 'self'", response.headers["content-security-policy-report-only"])
        self.assertIn("https://accounts.google.com", response.headers["content-security-policy-report-only"])
        self.assertEqual(
            response.headers["strict-transport-security"],
            "max-age=31536000; includeSubDomains",
        )
