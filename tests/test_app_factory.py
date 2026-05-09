import unittest

from core.app_factory import create_app
from core.config import AppSettings, PROJECT_ROOT


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

