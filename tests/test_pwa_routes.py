"""PWA 정적 라우트(/sw.js, /manifest.webmanifest) 헤더 계약 테스트.

서비스워커는 배포 직후 빠르게 갱신돼야 하므로 no-cache 로 서빙되고,
루트 스코프 제어를 위해 Service-Worker-Allowed: / 를 내려준다.
"""

import unittest

from core.app_factory import create_app
from core.config import PROJECT_ROOT, AppSettings


def _settings() -> AppSettings:
    return AppSettings(
        environment="production",
        project_root=PROJECT_ROOT,
        app_title="Test Compass",
        public_api_base_url="https://api.example.test",
        cors_allowed_origins=("https://app.example.test",),
        enable_docs=False,
    )


class PwaRouteTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.app = create_app(_settings())

    def test_pwa_routes_are_registered(self):
        paths = {route.path for route in self.app.routes if hasattr(route, "path")}
        self.assertIn("/sw.js", paths)
        self.assertIn("/manifest.webmanifest", paths)

    async def test_service_worker_served_no_cache_with_root_scope(self):
        resp = await self.app.state.static_handlers["service_worker"]()

        self.assertEqual(resp.headers["cache-control"], "no-cache, must-revalidate")
        self.assertEqual(resp.headers["service-worker-allowed"], "/")
        self.assertTrue(resp.headers["content-type"].startswith("application/javascript"))
        self.assertTrue(resp.path.as_posix().endswith("static/sw.js"))

    async def test_manifest_served_with_manifest_mime(self):
        resp = await self.app.state.static_handlers["manifest"]()

        self.assertTrue(resp.headers["content-type"].startswith("application/manifest+json"))
        self.assertTrue(resp.path.as_posix().endswith("static/manifest.webmanifest"))
