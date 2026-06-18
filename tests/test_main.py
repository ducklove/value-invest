import unittest
from datetime import datetime
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from starlette.requests import Request

import deps
from routes import cache_mgmt, auth, internal, portfolio, admin


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

    async def test_admin_mutation_rejects_untrusted_origin(self):
        request = _request_with_headers(
            "/api/admin/trigger/portfolio-snapshot",
            headers={
                "Host": "cantabile.tplinkdns.com:3691",
                "Content-Type": "application/json",
                "Origin": "https://evil.example",
                "X-Forwarded-Proto": "https",
            },
            client_host="203.0.113.20",
        )
        with patch("routes.admin.get_current_user", new=AsyncMock(return_value={"google_sub": "admin", "is_admin": True})):
            with self.assertRaises(HTTPException) as exc_info:
                await admin._require_admin_mutation(request)
        self.assertEqual(exc_info.exception.status_code, 403)

    async def test_admin_mutation_requires_json_fetch(self):
        request = _request_with_headers(
            "/api/admin/trigger/portfolio-snapshot",
            headers={
                "Host": "cantabile.tplinkdns.com:3691",
                "Content-Type": "text/plain",
                "Origin": "https://cantabile.tplinkdns.com:3691",
                "X-Forwarded-Proto": "https",
            },
            client_host="203.0.113.20",
        )
        with patch("routes.admin.get_current_user", new=AsyncMock(return_value={"google_sub": "admin", "is_admin": True})):
            with self.assertRaises(HTTPException) as exc_info:
                await admin._require_admin_mutation(request)
        self.assertEqual(exc_info.exception.status_code, 415)

    async def test_admin_mutation_accepts_trusted_origin(self):
        request = _request_with_headers(
            "/api/admin/trigger/portfolio-snapshot",
            headers={
                "Host": "cantabile.tplinkdns.com:3691",
                "Content-Type": "application/json",
                "Origin": "https://cantabile.tplinkdns.com:3691",
                "X-Forwarded-Proto": "https",
            },
            client_host="203.0.113.20",
        )
        with patch("routes.admin.get_current_user", new=AsyncMock(return_value={"google_sub": "admin", "is_admin": True})):
            user = await admin._require_admin_mutation(request)
        self.assertTrue(user["is_admin"])

    async def test_cashflow_invalid_amount_returns_400(self):
        request = _request_with_headers("/api/portfolio/cashflows")
        with patch("routes.portfolio.get_current_user", new=AsyncMock(return_value={"google_sub": "u1"})):
            with self.assertRaises(HTTPException) as exc_info:
                await portfolio.add_cashflow(request, {"type": "deposit", "amount": "not-a-number"})
        self.assertEqual(exc_info.exception.status_code, 400)

    async def test_delete_portfolio_item_normalizes_code(self):
        request = _request_with_headers("/api/portfolio/004800")
        deleter = AsyncMock(return_value=True)
        with patch("routes.portfolio.get_current_user", new=AsyncMock(return_value={"google_sub": "u1"})), \
             patch("repositories.portfolio.delete_portfolio_item", new=deleter):
            response = await portfolio.delete_portfolio_item(" 004800 ", request)

        self.assertEqual(response, {"ok": True})
        deleter.assert_awaited_once_with("u1", "004800")

    async def test_delete_portfolio_item_reports_missing_row(self):
        request = _request_with_headers("/api/portfolio/004800")
        with patch("routes.portfolio.get_current_user", new=AsyncMock(return_value={"google_sub": "u1"})), \
             patch("repositories.portfolio.delete_portfolio_item", new=AsyncMock(return_value=False)):
            with self.assertRaises(HTTPException) as exc_info:
                await portfolio.delete_portfolio_item("004800", request)

        self.assertEqual(exc_info.exception.status_code, 404)

    async def test_portfolio_order_saves_full_normalized_code_list(self):
        request = _request_with_headers("/api/portfolio/order")
        saver = AsyncMock()
        with patch("routes.portfolio.get_current_user", new=AsyncMock(return_value={"google_sub": "u1"})), \
             patch("repositories.portfolio.get_portfolio", new=AsyncMock(return_value=[
                 {"stock_code": "BBB"},
                 {"stock_code": "AAA"},
             ])), \
             patch("repositories.portfolio.save_portfolio_order", new=saver):
            response = await portfolio.save_portfolio_order(
                request,
                {"stock_codes": [" bbb ", "aaa"]},
            )

        self.assertEqual(response, {"ok": True, "count": 2})
        saver.assert_awaited_once_with("u1", ["BBB", "AAA"])

    async def test_portfolio_order_rejects_partial_or_unknown_code_list(self):
        request = _request_with_headers("/api/portfolio/order")
        saver = AsyncMock()
        with patch("routes.portfolio.get_current_user", new=AsyncMock(return_value={"google_sub": "u1"})), \
             patch("repositories.portfolio.get_portfolio", new=AsyncMock(return_value=[
                 {"stock_code": "AAA"},
                 {"stock_code": "BBB"},
             ])), \
             patch("repositories.portfolio.save_portfolio_order", new=saver):
            with self.assertRaises(HTTPException) as exc_info:
                await portfolio.save_portfolio_order(
                    request,
                    {"stock_codes": ["BBB", "CCC"]},
                )

        self.assertEqual(exc_info.exception.status_code, 400)
        saver.assert_not_awaited()

    async def test_portfolio_resolve_name_uses_domestic_search_alias_before_foreign(self):
        resolver = AsyncMock(return_value={"stock_code": "002380", "corp_name": "케이씨씨"})
        foreign = AsyncMock(side_effect=AssertionError("KCC alias should not fall through to foreign lookup"))
        with patch("routes.portfolio.cache.resolve_corp_search_query", new=resolver), \
             patch("services.portfolio.foreign.resolve_foreign_reuters", new=foreign):
            response = await portfolio.resolve_name(code="KCC")

        self.assertEqual(response["stock_code"], "002380")
        self.assertEqual(response["stock_name"], "케이씨씨")
        resolver.assert_awaited_once_with("KCC")
        foreign.assert_not_awaited()

    async def test_portfolio_save_canonicalizes_domestic_alias_before_foreign(self):
        request = _request_with_headers("/api/portfolio/KCC")
        alias = {"stock_code": "002380", "corp_name": "케이씨씨"}
        saver = AsyncMock(return_value={"stock_code": "002380"})
        foreign_name = AsyncMock(side_effect=AssertionError("KCC alias should not be saved as a foreign ticker"))
        with patch("routes.portfolio.get_current_user", new=AsyncMock(return_value={"google_sub": "u1"})), \
             patch("routes.portfolio.cache.resolve_corp_search_query", new=AsyncMock(return_value=alias)), \
             patch("repositories.portfolio.save_portfolio_item", new=saver), \
             patch("services.portfolio.foreign.resolve_foreign_name", new=foreign_name), \
             patch("routes.portfolio._fetch_quote", new=AsyncMock(return_value={"price": 1000})):
            await portfolio.save_portfolio_item(
                "KCC",
                request,
                {"stock_name": "", "quantity": 1, "avg_price": 0},
            )

        saver.assert_awaited_once()
        args = saver.await_args.args
        self.assertEqual(args[1], "002380")
        self.assertEqual(args[2], "케이씨씨")
        self.assertEqual(args[5], "KRW")
        foreign_name.assert_not_awaited()

    async def test_portfolio_save_foreign_ticker_uses_fast_currency_inference(self):
        request = _request_with_headers("/api/portfolio/AAPL")
        saver = AsyncMock(return_value={"stock_code": "AAPL"})
        detect_currency = AsyncMock(side_effect=AssertionError("save should not block on currency discovery"))
        with patch("routes.portfolio.get_current_user", new=AsyncMock(return_value={"google_sub": "u1"})), \
             patch("services.portfolio.foreign.resolve_domestic_code_alias", new=AsyncMock(return_value=None)), \
             patch("services.portfolio.foreign.detect_currency", new=detect_currency), \
             patch("repositories.portfolio.save_portfolio_item", new=saver), \
             patch("repositories.foreign_dividends.get_foreign_dividend", new=AsyncMock(return_value={"dps_krw": 0})):
            await portfolio.save_portfolio_item(
                "AAPL",
                request,
                {"stock_name": "Apple Inc.", "quantity": 1, "avg_price": 0},
            )

        saver.assert_awaited_once()
        args = saver.await_args.args
        self.assertEqual(args[1], "AAPL")
        self.assertEqual(args[2], "Apple Inc.")
        self.assertEqual(args[5], "USD")
        detect_currency.assert_not_awaited()

    async def test_portfolio_foreign_search_endpoint_returns_suggestions(self):
        suggestions = [{"stock_code": "AAPL", "stock_name": "Apple Inc.", "currency": "USD"}]
        search = AsyncMock(return_value=suggestions)
        with patch("routes.portfolio.foreign.search_foreign_tickers", new=search):
            response = await portfolio.search_foreign(q="apple", limit=5)

        self.assertEqual(response, suggestions)
        search.assert_awaited_once_with("apple", limit=5)

    def test_portfolio_today_baseline_resets_at_22(self):
        self.assertEqual(
            portfolio._portfolio_today_baseline_date(datetime(2026, 5, 1, 21, 59, 59)),
            "2026-04-30",
        )
        self.assertEqual(
            portfolio._portfolio_today_baseline_date(datetime(2026, 5, 1, 22, 0, 0)),
            "2026-05-01",
        )

    def test_analysis_snapshot_staleness(self):
        self.assertTrue(deps.analysis_snapshot_is_stale(None))
        self.assertFalse(deps.analysis_snapshot_is_stale("2999-01-01T00:00:00"))

    async def test_spa_pages_serve_index_html(self):
        """Deep-link paths (/analysis, /portfolio, /nps, /insights) must serve
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
        for p in ("/analysis", "/portfolio", "/nps", "/insights"):
            self.assertIn(p, registered, f"deep-link route {p} missing")
