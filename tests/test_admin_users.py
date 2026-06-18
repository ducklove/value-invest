import json
import unittest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from starlette.requests import Request

import cache
from _harness import TempDbMixin
from repositories import system_events as system_events_repo
from routes import admin as admin_route


def _request(
    path: str = "/api/admin/users",
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    client_host: str = "127.0.0.1",
) -> Request:
    encoded_headers = [
        (key.lower().encode("latin-1"), value.encode("latin-1"))
        for key, value in (headers or {}).items()
    ]
    return Request({
        "type": "http",
        "method": method,
        "path": path,
        "headers": encoded_headers,
        "query_string": b"",
        "client": (client_host, 12345),
        "server": ("testserver", 80),
        "scheme": "http",
    })


def _mutation_request(path: str = "/api/admin/users/u1") -> Request:
    return _request(
        path,
        method="PATCH",
        headers={
            "Content-Type": "application/json",
            "Origin": "http://localhost:8000",
        },
    )


class AdminUserManagementTests(TempDbMixin):
    async def seed(self):
        db = await cache.get_db()
        await db.executemany(
            """
            INSERT INTO users
              (google_sub, email, name, picture, email_verified, created_at, last_login_at, is_admin)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("admin", "admin@example.com", "Admin", "", 1, "2026-01-01T00:00:00", "2026-01-05T00:00:00", 1),
                ("u2", "user@example.com", "User Two", "", 1, "2026-01-02T00:00:00", "2026-01-04T00:00:00", 0),
            ],
        )
        await db.execute(
            """
            INSERT INTO user_portfolio
              (google_sub, stock_code, stock_name, quantity, avg_price, sort_order, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            ("u2", "005930", "삼성전자", 3, 70000, 0, "2026-01-03T00:00:00", "2026-01-03T00:00:00"),
        )
        await db.commit()

    async def test_list_users_includes_portfolio_count(self):
        with patch("routes.admin.get_current_user", AsyncMock(return_value={"google_sub": "admin", "is_admin": True})):
            rows = await admin_route.list_users(_request())
        user = next(row for row in rows if row["google_sub"] == "u2")
        self.assertEqual(user["portfolio_count"], 1)

    async def test_role_update_and_last_admin_protection(self):
        admin_user = {"google_sub": "admin", "email": "admin@example.com", "is_admin": True}
        with patch("routes.admin.get_current_user", AsyncMock(return_value=admin_user)):
            out = await admin_route.update_user_role(
                "u2",
                _mutation_request("/api/admin/users/u2/role"),
                {"is_admin": True},
            )
        self.assertTrue(out["user"]["is_admin"])

        # Now two admins exist; demote u2 again, then prove the remaining
        # final admin cannot demote itself.
        with patch("routes.admin.get_current_user", AsyncMock(return_value=admin_user)):
            await admin_route.update_user_role(
                "u2",
                _mutation_request("/api/admin/users/u2/role"),
                {"is_admin": False},
            )
            with self.assertRaises(HTTPException) as exc_info:
                await admin_route.update_user_role(
                    "admin",
                    _mutation_request("/api/admin/users/admin/role"),
                    {"is_admin": False},
                )
        self.assertEqual(exc_info.exception.status_code, 400)

    async def test_update_profile_and_delete_user(self):
        admin_user = {"google_sub": "admin", "email": "admin@example.com", "is_admin": True}
        with patch("routes.admin.get_current_user", AsyncMock(return_value=admin_user)):
            updated = await admin_route.update_user_profile(
                "u2",
                _mutation_request("/api/admin/users/u2"),
                {
                    "email": "renamed@example.com",
                    "name": "Renamed",
                    "picture": "https://example.com/p.png",
                    "email_verified": True,
                },
            )
            self.assertEqual(updated["user"]["name"], "Renamed")

            deleted = await admin_route.delete_user(
                "u2",
                _request(
                    "/api/admin/users/u2",
                    method="DELETE",
                    headers={"Content-Type": "application/json", "Origin": "http://localhost:8000"},
                ),
            )
        self.assertTrue(deleted["ok"])
        db = await cache.get_db()
        cursor = await db.execute("SELECT COUNT(*) AS n FROM user_portfolio WHERE google_sub = 'u2'")
        self.assertEqual((await cursor.fetchone())["n"], 0)

    async def test_portfolio_search_returns_internal_link(self):
        with patch("routes.admin.get_current_user", AsyncMock(return_value={"google_sub": "admin", "is_admin": True})):
            out = await admin_route.search_portfolios(_request(), q="삼성", limit=10)
        self.assertEqual(len(out["rows"]), 1)
        self.assertIn("/api/admin/users/u2/portfolio.html", out["rows"][0]["portfolio_url"])

    async def test_internal_portfolio_page_requires_lan_client(self):
        admin_user = {"google_sub": "admin", "email": "admin@example.com", "is_admin": True}
        public_request = _request(
            "/api/admin/users/u2/portfolio.html",
            headers={"X-Forwarded-For": "203.0.113.10"},
            client_host="127.0.0.1",
        )
        with patch("routes.admin.get_current_user", AsyncMock(return_value=admin_user)):
            with self.assertRaises(HTTPException) as exc_info:
                await admin_route.user_portfolio_page("u2", public_request)
        self.assertEqual(exc_info.exception.status_code, 403)

        lan_request = _request(
            "/api/admin/users/u2/portfolio.html",
            headers={"X-Forwarded-For": "192.168.0.25"},
            client_host="127.0.0.1",
        )
        with patch("routes.admin.get_current_user", AsyncMock(return_value=admin_user)):
            response = await admin_route.user_portfolio_page("u2", lan_request)
        body = response.body.decode("utf-8")
        self.assertIn("내부망 전용", body)
        self.assertIn("삼성전자", body)


class AdminTimeseriesTests(TempDbMixin):
    async def test_event_and_http_timeseries_are_hourly(self):
        recent = datetime.now().replace(minute=10, second=0, microsecond=0)
        previous = recent - timedelta(hours=1)
        await system_events_repo.insert_system_event(
            level="info", source="snapshot_nav", kind="tick_ok",
            ts=previous.isoformat(timespec="seconds"),
        )
        await system_events_repo.insert_system_event(
            level="error", source="http", kind="error",
            details=json.dumps({"path": "/api/test", "duration_ms": 1234}),
            ts=recent.isoformat(timespec="seconds"),
        )

        with patch("routes.admin.get_current_user", AsyncMock(return_value={"google_sub": "admin", "is_admin": True})):
            out = await admin_route.admin_timeseries(_request("/api/admin/timeseries"), hours=24)

        self.assertGreaterEqual(len(out["events"]), 2)
        self.assertEqual(out["events"][-1]["error"], 1)
        self.assertEqual(out["http"][-1]["errors"], 1)
        self.assertEqual(out["http"][-1]["max_ms"], 1234.0)


if __name__ == "__main__":
    unittest.main()
