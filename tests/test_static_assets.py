"""정적 자산 서빙 계약: GZip(O1), immutable 캐시(O1), HTML 렌더 캐시·ETag(O1),
파일별 콘텐츠 해시 ?v= (F4).
"""

from __future__ import annotations

import hashlib
import re
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from fastapi.responses import StreamingResponse

from core.app_factory import create_app
from core.config import PROJECT_ROOT, AppSettings
from core.runtime import AssetManifest
from core.static_routes import _RenderedHtml, _with_asset_version

STATIC = PROJECT_ROOT / "static"
IMMUTABLE = "public, max-age=31536000, immutable"


def _settings(project_root: Path = PROJECT_ROOT, environment: str = "production") -> AppSettings:
    return AppSettings(
        environment=environment,
        project_root=project_root,
        app_title="Test Compass",
        public_api_base_url="https://api.example.test",
        cors_allowed_origins=("https://app.example.test",),
        enable_docs=False,
    )


def _sha10(path: Path) -> str:
    return hashlib.sha1(path.read_bytes()).hexdigest()[:10]


def _stamps(html: str) -> dict[str, str]:
    return dict(re.findall(r'(?:src|href)="\.?/((?:js|css)/[^"?]+)\?v=([^"]+)"', html))


class _AppCase(unittest.IsolatedAsyncioTestCase):
    project_root = PROJECT_ROOT
    environment = "production"

    def setUp(self):
        self.app = create_app(_settings(self.project_root, self.environment))

    def client(self):
        transport = httpx.ASGITransport(app=self.app, raise_app_exceptions=False)
        return httpx.AsyncClient(transport=transport, base_url="https://testserver")


class GzipTests(_AppCase):
    async def test_large_static_asset_is_gzipped_when_accepted(self):
        async with self.client() as client:
            response = await client.get("/js/utils.js", headers={"Accept-Encoding": "gzip"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("content-encoding"), "gzip")
        self.assertIn("accept-encoding", response.headers.get("vary", "").lower())
        # httpx 가 풀어 준 본문 = 원본 파일.
        self.assertEqual(response.content, (STATIC / "js" / "utils.js").read_bytes())

    async def test_identity_when_client_does_not_accept_gzip(self):
        async with self.client() as client:
            response = await client.get("/js/utils.js", headers={"Accept-Encoding": "identity"})
        self.assertNotIn("content-encoding", response.headers)

    async def test_small_responses_stay_uncompressed(self):
        async with self.client() as client:
            response = await client.get("/healthz", headers={"Accept-Encoding": "gzip"})
        self.assertEqual(response.status_code, 200)
        self.assertLess(len(response.content), 1024)
        self.assertNotIn("content-encoding", response.headers)

    async def test_event_stream_is_never_compressed(self):
        chunk = "data: " + ("x" * 4000) + "\n\n"

        async def sse():
            async def gen():
                for _ in range(3):
                    yield chunk

            return StreamingResponse(gen(), media_type="text/event-stream")

        self.app.add_api_route("/__test/sse", sse, methods=["GET"])
        async with self.client() as client:
            response = await client.get("/__test/sse", headers={"Accept-Encoding": "gzip"})
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.headers["content-type"].startswith("text/event-stream"))
        self.assertNotIn("content-encoding", response.headers)
        self.assertEqual(response.text, chunk * 3)

    async def test_security_headers_survive_compression(self):
        async with self.client() as client:
            response = await client.get("/js/utils.js", headers={"Accept-Encoding": "gzip"})
        self.assertEqual(response.headers.get("content-encoding"), "gzip")
        self.assertEqual(response.headers["x-content-type-options"], "nosniff")


class ImmutableCacheTests(_AppCase):
    async def test_current_stamp_is_immutable_on_every_mount(self):
        manifest = self.app.state.asset_manifest
        async with self.client() as client:
            for url, key in [
                ("/js/utils.js", "js/utils.js"),
                ("/css/base.css", "css/base.css"),
                ("/static/js/utils.js", "js/utils.js"),
                ("/static/ecosystem/vc-shell.js", "ecosystem/vc-shell.js"),
            ]:
                response = await client.get(url, params={"v": manifest.version_for(key)})
                self.assertEqual(response.status_code, 200, url)
                self.assertEqual(response.headers.get("cache-control"), IMMUTABLE, url)

    async def test_unversioned_and_foreign_versions_keep_revalidating(self):
        async with self.client() as client:
            plain = await client.get("/js/utils.js")
            stale = await client.get("/js/utils.js", params={"v": "0000000000"})
            # 형제 도구가 레지스트리 버전으로 핫링크하는 URL — 1년 고정되면 안 된다.
            hotlink = await client.get("/js/portfolio-held-badges.js", params={"v": "20260930-vc"})
        for response in (plain, stale, hotlink):
            self.assertEqual(response.status_code, 200)
            self.assertNotIn("immutable", response.headers.get("cache-control", ""))
            self.assertIn("etag", response.headers)

    async def test_conditional_request_on_stamped_asset_keeps_immutable(self):
        version = self.app.state.asset_manifest.version_for("js/utils.js")
        async with self.client() as client:
            first = await client.get("/js/utils.js", params={"v": version})
            again = await client.get(
                "/js/utils.js", params={"v": version}, headers={"If-None-Match": first.headers["etag"]},
            )
        self.assertEqual(again.status_code, 304)
        self.assertEqual(again.headers.get("cache-control"), IMMUTABLE)

    async def test_missing_file_is_404_without_immutable(self):
        async with self.client() as client:
            response = await client.get("/js/does-not-exist.js", params={"v": "abc"})
        self.assertEqual(response.status_code, 404)
        self.assertNotIn("immutable", response.headers.get("cache-control", ""))


class HtmlRenderCacheTests(_AppCase):
    async def test_index_and_admin_carry_etag_and_answer_304(self):
        async with self.client() as client:
            for path in ("/", "/portfolio", "/admin"):
                first = await client.get(path)
                self.assertEqual(first.status_code, 200, path)
                etag = first.headers["etag"]
                self.assertTrue(etag.startswith('W/"'), etag)
                self.assertEqual(first.headers["cache-control"], "no-cache, must-revalidate")
                again = await client.get(path, headers={"If-None-Match": etag})
                self.assertEqual(again.status_code, 304, path)
                self.assertEqual(again.content, b"")
                self.assertEqual(again.headers["etag"], etag)
                other = await client.get(path, headers={"If-None-Match": 'W/"something-else"'})
                self.assertEqual(other.status_code, 200)

    async def test_admin_shell_uses_the_value_compass_brand(self):
        async with self.client() as client:
            admin = (await client.get("/admin")).text
        self.assertIn("<title>Value Compass Admin</title>", admin)
        self.assertNotIn("Value Invest", admin)

    async def test_spa_paths_share_the_index_etag(self):
        async with self.client() as client:
            root = await client.get("/")
            spa = await client.get("/investing")
        self.assertEqual(root.headers["etag"], spa.headers["etag"])
        self.assertEqual(root.text, spa.text)

    async def test_html_is_rendered_once_per_process(self):
        reads = []
        original = Path.read_text

        def counting_read_text(self, *args, **kwargs):
            if self.name in {"index.html", "admin.html"}:
                reads.append(self.name)
            return original(self, *args, **kwargs)

        app = create_app(_settings())
        transport = httpx.ASGITransport(app=app)
        with patch.object(Path, "read_text", counting_read_text):
            async with httpx.AsyncClient(transport=transport, base_url="https://testserver") as client:
                for _ in range(3):
                    await client.get("/")
                    await client.get("/admin")
        self.assertEqual(sorted(reads), ["admin.html", "index.html"])

    async def test_handlers_remain_callable_without_a_request(self):
        # main.py 호환 헬퍼·기존 테스트는 핸들러를 인자 없이 부른다.
        response = await self.app.state.static_handlers["spa_pages"]()
        self.assertEqual(response.status_code, 200)
        self.assertIn("etag", response.headers)


class PerFileVersionTests(_AppCase):
    async def test_each_asset_gets_its_own_content_hash(self):
        async with self.client() as client:
            index = (await client.get("/")).text
            admin = (await client.get("/admin")).text
        stamps = _stamps(index)
        self.assertEqual(stamps["js/utils.js"], _sha10(STATIC / "js" / "utils.js"))
        self.assertEqual(stamps["css/base.css"], _sha10(STATIC / "css" / "base.css"))
        # 서로 다른 파일은 서로 다른 ?v= 를 받는다(저장소 단위 해시가 아니다).
        self.assertNotEqual(stamps["js/utils.js"], stamps["js/app-main.js"])
        self.assertGreater(len(set(stamps.values())), len(stamps) // 2)
        # 지연 로드 data-src 도 같은 파일별 해시를 받는다.
        self.assertIn(f'data-src="./js/masters.js?v={_sha10(STATIC / "js" / "masters.js")}"', index)
        # admin.html(절대 경로) 도 동일 규칙.
        self.assertEqual(_stamps(admin)["js/admin.js"], _sha10(STATIC / "js" / "admin.js"))
        self.assertEqual(_stamps(admin)["js/utils.js"], stamps["js/utils.js"])

    def test_stamping_rules(self):
        html = (
            '<script src="./js/a.js"></script>'
            '<link rel="stylesheet" href="./css/b.css">'
            '<script data-src="./js/lazy.js"></script>'
            '<script src="./static/ecosystem/vc-shell.js"></script>'
            '<script src="./js/unknown.js"></script>'
            '<script src="./js/already.js?v=keep"></script>'
            '<script src="https://cdn.example/js/x.js"></script>'
        )
        versions = {"js/a.js": "aaaaaaaaaa", "css/b.css": "bbbbbbbbbb", "js/lazy.js": "llllllllll",
                    "ecosystem/vc-shell.js": "eeeeeeeeee"}
        out = _with_asset_version(html, "gitsha", relative=True, versions=versions)
        self.assertIn('src="./js/a.js?v=aaaaaaaaaa"', out)
        self.assertIn('href="./css/b.css?v=bbbbbbbbbb"', out)
        self.assertIn('data-src="./js/lazy.js?v=llllllllll"', out)
        self.assertIn('src="./static/ecosystem/vc-shell.js?v=eeeeeeeeee"', out)
        self.assertIn('src="./js/unknown.js?v=gitsha"', out)  # 매니페스트 밖 → git 해시 폴백
        self.assertIn('src="./js/already.js?v=keep"', out)  # 이미 쿼리가 있으면 그대로
        self.assertIn('src="https://cdn.example/js/x.js"', out)
        self.assertEqual(out.count("?v="), 6)


class AssetManifestTests(unittest.TestCase):
    def _tree(self, tmp: Path) -> Path:
        static = tmp / "static"
        for rel, body in {
            "js/a.js": "console.log('a');",
            "js/b.js": "console.log('b');",
            "js/vendor/v.js": "vendor",
            "css/base.css": "body{}",
            "ecosystem/vc-shell.js": "shell",
            "icon.svg": "<svg/>",
        }.items():
            path = static / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(body, encoding="utf-8")
        return static

    def test_hashes_js_css_and_ecosystem_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            static = self._tree(Path(tmp))
            manifest = AssetManifest(static, "gitsha")
            versions = manifest.versions()
            self.assertEqual(
                sorted(versions),
                ["css/base.css", "ecosystem/vc-shell.js", "js/a.js", "js/b.js", "js/vendor/v.js"],
            )
            self.assertEqual(versions["js/a.js"], _sha10(static / "js" / "a.js"))
            self.assertNotEqual(versions["js/a.js"], versions["js/b.js"])
            self.assertEqual(manifest.version_for("icon.svg"), "gitsha")
            self.assertTrue(manifest.is_current("js/a.js", versions["js/a.js"]))
            self.assertFalse(manifest.is_current("js/a.js", "gitsha"))
            self.assertTrue(manifest.is_current("icon.svg", "gitsha"))
            self.assertFalse(manifest.is_current("js/a.js", ""))

    def test_refresh_rehashes_only_changed_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            static = self._tree(Path(tmp))
            manifest = AssetManifest(static, "gitsha")
            before = manifest.versions()
            generation = manifest.generation
            self.assertFalse(manifest.refresh())
            self.assertEqual(manifest.generation, generation)

            (static / "js" / "a.js").write_text("console.log('changed a');", encoding="utf-8")
            self.assertTrue(manifest.refresh())
            after = manifest.versions()
            self.assertNotEqual(after["js/a.js"], before["js/a.js"])
            self.assertEqual(after["js/b.js"], before["js/b.js"])
            self.assertEqual(manifest.generation, generation + 1)

    def test_live_render_picks_up_edits_while_production_render_is_fixed(self):
        with tempfile.TemporaryDirectory() as tmp:
            static = self._tree(Path(tmp))
            (static / "index.html").write_text('<script src="./js/a.js"></script>', encoding="utf-8")
            live = _RenderedHtml(static / "index.html", AssetManifest(static, "g", live=True), relative=True)
            fixed = _RenderedHtml(static / "index.html", AssetManifest(static, "g"), relative=True)
            live_html, live_etag = live.get()
            fixed_html, fixed_etag = fixed.get()
            self.assertEqual(live_html, fixed_html)

            (static / "js" / "a.js").write_text("console.log('edited');", encoding="utf-8")
            new_html, new_etag = live.get()
            self.assertIn(f"?v={_sha10(static / 'js' / 'a.js')}", new_html)
            self.assertNotEqual(new_etag, live_etag)
            self.assertEqual(fixed.get(), (fixed_html, fixed_etag))


class DevelopmentProfileTests(unittest.IsolatedAsyncioTestCase):
    async def test_development_app_restamps_after_an_edit(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            shutil.copytree(STATIC, root / "static")
            app = create_app(_settings(root, "development"))
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="https://testserver") as client:
                before = _stamps((await client.get("/")).text)["js/utils.js"]
                (root / "static" / "js" / "utils.js").write_text("// edited\n", encoding="utf-8")
                after = _stamps((await client.get("/")).text)["js/utils.js"]
        self.assertNotEqual(before, after)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()

