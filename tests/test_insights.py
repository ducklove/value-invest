import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from starlette.requests import Request

import cache
from routes import insights


def _request(method: str = "GET", path: str = "/api/insights") -> Request:
    scope = {
        "type": "http",
        "method": method,
        "path": path,
        "headers": [],
        "query_string": b"",
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
        "scheme": "http",
    }
    return Request(scope)


class InsightRouteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(cache, "DB_PATH", Path(self.tmp.name) / "cache.db")
        self.db_patch.start()
        await cache.close_db()
        await cache.init_db()
        db = await cache.get_db()
        await db.execute(
            """
            INSERT INTO users (google_sub, email, name, picture, email_verified, created_at, last_login_at, is_admin)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("u1", "u1@example.com", "User One", "", 1, "2026-01-01", "2026-01-01", 0),
        )
        await db.execute(
            """
            INSERT INTO users (google_sub, email, name, picture, email_verified, created_at, last_login_at, is_admin)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("u2", "u2@example.com", "User Two", "", 1, "2026-01-01", "2026-01-01", 0),
        )
        await db.commit()

    async def asyncTearDown(self):
        await cache.close_db()
        self.db_patch.stop()
        self.tmp.cleanup()

    async def test_create_and_list_insight_post(self):
        user = {"google_sub": "u1", "email": "u1@example.com", "name": "User One"}
        payload = {
            "title": "외부 백테스트 결과",
            "insight_md": "초과수익은 좋지만 MDD가 커서 조건을 좁혀야 한다.",
            "source_type": "backtest",
            "tags": ["value", "monthly", "value"],
            "result_summary": {"return_pct": 42.5, "benchmark_return_pct": 21.0},
        }
        with patch("routes.insights.get_current_user", AsyncMock(return_value=user)):
            post = await insights.create_insight(_request("POST"), payload)
            rows = await insights.list_insights(_request("GET"))

        self.assertEqual(post["title"], "외부 백테스트 결과")
        self.assertEqual(post["tags"], ["value", "monthly"])
        self.assertTrue(post["can_delete"])
        self.assertEqual(len(rows), 1)
        self.assertNotIn("result_payload", rows[0])
        self.assertEqual(rows[0]["result_summary"]["return_pct"], 42.5)

    async def test_private_post_visible_only_to_author(self):
        await cache.create_insight_post(
            google_sub="u1",
            title="private",
            insight_md="body",
            visibility="private",
        )
        with patch("routes.insights.get_current_user", AsyncMock(return_value={"google_sub": "u2"})):
            rows = await insights.list_insights(_request("GET"))
        self.assertEqual(rows, [])

        with patch("routes.insights.get_current_user", AsyncMock(return_value={"google_sub": "u1"})):
            rows = await insights.list_insights(_request("GET"))
        self.assertEqual(len(rows), 1)

    async def test_create_requires_login_and_body(self):
        with patch("routes.insights.get_current_user", AsyncMock(return_value=None)):
            with self.assertRaises(HTTPException) as exc:
                await insights.create_insight(_request("POST"), {"title": "x", "insight_md": "y"})
        self.assertEqual(exc.exception.status_code, 401)

        with patch("routes.insights.get_current_user", AsyncMock(return_value={"google_sub": "u1"})):
            with self.assertRaises(HTTPException) as exc:
                await insights.create_insight(_request("POST"), {"title": "x"})
        self.assertEqual(exc.exception.status_code, 400)

    async def test_delete_allows_author_only(self):
        post = await cache.create_insight_post(
            google_sub="u1",
            title="delete me",
            insight_md="body",
        )
        with patch("routes.insights.get_current_user", AsyncMock(return_value={"google_sub": "u2"})):
            with self.assertRaises(HTTPException) as exc:
                await insights.delete_insight(post["id"], _request("DELETE"))
        self.assertEqual(exc.exception.status_code, 404)

        with patch("routes.insights.get_current_user", AsyncMock(return_value={"google_sub": "u1"})):
            out = await insights.delete_insight(post["id"], _request("DELETE"))
        self.assertTrue(out["ok"])


if __name__ == "__main__":
    unittest.main()
