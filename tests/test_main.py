import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from starlette.requests import Request

import main


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


class MainRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_delete_cache_requires_login(self):
        request = _request("/api/cache/005930")
        with patch("main._get_current_user", new=AsyncMock(return_value=None)):
            with self.assertRaises(HTTPException) as exc_info:
                await main.delete_cache("005930", request)
        self.assertEqual(exc_info.exception.status_code, 401)

    async def test_update_cache_order_requires_login(self):
        request = _request("/api/cache/order")
        with patch("main._get_current_user", new=AsyncMock(return_value=None)):
            with self.assertRaises(HTTPException) as exc_info:
                await main.update_cache_order(request, {"stock_codes": ["005930"]})
        self.assertEqual(exc_info.exception.status_code, 401)

    async def test_login_page_redirects_authenticated_user(self):
        request = _request("/login")
        with patch("main._get_current_user", new=AsyncMock(return_value={"google_sub": "u1"})):
            response = await main.login_page(request)
        self.assertEqual(response.status_code, 303)

    def test_analysis_snapshot_staleness(self):
        self.assertTrue(main._analysis_snapshot_is_stale(None))
        self.assertFalse(main._analysis_snapshot_is_stale("2999-01-01T00:00:00"))
