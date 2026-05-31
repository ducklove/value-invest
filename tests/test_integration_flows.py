"""End-to-end flow tests that drive the *real* ASGI app through HTTP.

Most existing tests call route handlers directly with a hand-built Request and
mock everything below them. Those are fast but they don't catch breakage in the
wiring: routing precedence, middleware, request/response (de)serialization, and
the handler -> repository -> SQLite path acting together.

These tests boot the app with `create_app()` and talk to it via
`httpx.ASGITransport` against a throwaway SQLite file, so a request travels the
same stack a browser would (minus lifespan, which we deliberately skip — it
would spin up KIS/DART clients). Auth and background warmups are the only seams
stubbed; the DB and the data path are real.

Covers:
* the request-latency observer middleware (slow / error -> system_events),
* the portfolio item GET/DELETE round-trip,
* the cashflow <-> CASH_KRW balance synchronization, including the
  insufficient-balance rejection.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

import cache
import observability
from core import app_factory
from core.app_factory import create_app
from core.config import AppSettings, PROJECT_ROOT
from routes import portfolio as portfolio_route


def _test_settings() -> AppSettings:
    return AppSettings(
        environment="development",
        project_root=PROJECT_ROOT,
        app_title="Test Compass",
        public_api_base_url="https://api.example.test",
        cors_allowed_origins=("https://app.example.test",),
        enable_docs=False,
    )


class IntegrationAppHarness(unittest.IsolatedAsyncioTestCase):
    """Boots a real app over a temp SQLite DB with a logged-in user.

    Subclasses get ``self.client`` (an httpx AsyncClient) and ``self.app``.
    """

    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "cache.db"
        self.db_patch = patch.object(cache, "DB_PATH", self.db_path)
        self.db_patch.start()
        # A prior test may have left cache._conn pointing at a deleted temp DB.
        await cache.close_db()
        await cache.init_db()

        # Background warmups fan out to DART / AI and must not fire in tests.
        portfolio_route._dividend_warmup_last.clear()
        portfolio_route._dividend_warmup_tasks.clear()
        self._patches = [
            patch.object(portfolio_route, "_schedule_portfolio_dividend_warmup", lambda codes: None),
            patch.object(portfolio_route.insights, "schedule_asset_insight_warmup", lambda enriched: None),
        ]
        for p in self._patches:
            p.start()

        db = await cache.get_db()
        await db.execute(
            "INSERT INTO users (google_sub, email, name, picture, email_verified, created_at, last_login_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("u1", "user@example.com", "User", "", 1, "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
        )
        await db.executemany(
            "INSERT INTO corp_codes (stock_code, corp_code, corp_name, updated_at) VALUES (?, ?, ?, ?)",
            [
                ("005930", "00126380", "삼성전자", "2026-01-01"),
                ("000660", "00164779", "SK하이닉스", "2026-01-01"),
            ],
        )
        await db.commit()

        self.user = {"google_sub": "u1", "email": "user@example.com", "name": "User"}
        self.auth_patch = patch.object(
            portfolio_route, "get_current_user", new=AsyncMock(return_value=self.user)
        )
        self.auth_patch.start()

        self.app = create_app(_test_settings())
        # ASGITransport does not run lifespan, so app.state set in create_app
        # (settings, runtime, slow_request_ms, static_handlers) is what we get.
        self.transport = httpx.ASGITransport(app=self.app, raise_app_exceptions=False)
        self.client = httpx.AsyncClient(transport=self.transport, base_url="http://testserver")

    async def asyncTearDown(self) -> None:
        await self.client.aclose()
        self.auth_patch.stop()
        for p in self._patches:
            p.stop()
        await cache.close_db()
        self.db_patch.stop()
        self.temp_dir.cleanup()


class RequestObservabilityTests(IntegrationAppHarness):
    async def test_slow_api_request_records_warning_event(self):
        self.app.state.slow_request_ms = 0.0  # everything counts as slow
        with patch.object(observability, "record_event", new=AsyncMock()) as rec:
            resp = await self.client.get("/api/portfolio/cashflows")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), [])

        rec.assert_awaited_once()
        kwargs = rec.await_args.kwargs
        self.assertEqual(kwargs["source"], "http")
        self.assertEqual(kwargs["kind"], "slow")
        self.assertEqual(kwargs["level"], "warning")
        details = kwargs["details"]
        self.assertEqual(details["path"], "/api/portfolio/cashflows")
        self.assertEqual(details["method"], "GET")
        self.assertEqual(details["status"], 200)
        self.assertIsInstance(details["duration_ms"], (int, float))
        self.assertGreaterEqual(details["duration_ms"], 0.0)

    async def test_fast_request_records_nothing(self):
        self.app.state.slow_request_ms = 10_000_000.0  # nothing is this slow
        with patch.object(observability, "record_event", new=AsyncMock()) as rec:
            resp = await self.client.get("/api/portfolio/cashflows")
        self.assertEqual(resp.status_code, 200)
        rec.assert_not_awaited()

    async def test_non_api_path_is_not_instrumented(self):
        self.app.state.slow_request_ms = 0.0
        with patch.object(observability, "record_event", new=AsyncMock()) as rec:
            resp = await self.client.get("/healthz")
        self.assertEqual(resp.status_code, 200)
        rec.assert_not_awaited()

    async def test_server_error_recorded_even_when_fast(self):
        # High threshold proves the error branch fires independently of latency.
        self.app.state.slow_request_ms = 10_000_000.0
        boom = AsyncMock(side_effect=RuntimeError("boom"))
        with patch.object(portfolio_route, "get_current_user", new=boom), \
             patch.object(observability, "record_event", new=AsyncMock()) as rec:
            resp = await self.client.get("/api/portfolio/cashflows")

        self.assertEqual(resp.status_code, 500)
        rec.assert_awaited_once()
        kwargs = rec.await_args.kwargs
        self.assertEqual(kwargs["kind"], "error")
        self.assertEqual(kwargs["level"], "error")
        self.assertEqual(kwargs["details"]["status"], 500)
        self.assertEqual(kwargs["details"]["path"], "/api/portfolio/cashflows")

    async def test_event_is_persisted_to_system_events(self):
        """No mock on record_event: prove the row actually lands in the table
        the admin dashboard reads. The write is fire-and-forget, so poll."""
        self.app.state.slow_request_ms = 0.0
        resp = await self.client.get("/api/portfolio/cashflows")
        self.assertEqual(resp.status_code, 200)

        events = []
        for _ in range(200):
            events = await cache.get_system_events(source="http")
            if events:
                break
            await asyncio.sleep(0.01)

        self.assertTrue(events, "expected an http event to be persisted")
        latest = events[0]
        self.assertEqual(latest["source"], "http")
        self.assertEqual(latest["kind"], "slow")
        details = json.loads(latest["details"])
        self.assertEqual(details["path"], "/api/portfolio/cashflows")


class PortfolioFlowTests(IntegrationAppHarness):
    async def test_portfolio_item_get_delete_roundtrip(self):
        await cache.save_portfolio_item("u1", "005930", "삼성전자", 100, 65000)

        resp = await self.client.get("/api/portfolio")
        self.assertEqual(resp.status_code, 200)
        items = resp.json()
        samsung = next((it for it in items if it["stock_code"] == "005930"), None)
        self.assertIsNotNone(samsung, "saved item should be returned over HTTP")
        self.assertEqual(samsung["quantity"], 100)
        self.assertEqual(samsung["avg_price"], 65000)
        # GET enriches with the fields the frontend contract depends on.
        self.assertIn("target_metrics", samsung)
        self.assertTrue(samsung.get("benchmark_code"), "default benchmark must be resolved")

        resp = await self.client.delete("/api/portfolio/005930")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"ok": True})

        resp = await self.client.get("/api/portfolio")
        codes = [it["stock_code"] for it in resp.json()]
        self.assertNotIn("005930", codes)

        # Deleting again is a clean 404, not a 500.
        resp = await self.client.delete("/api/portfolio/005930")
        self.assertEqual(resp.status_code, 404)

    async def test_cashflow_deposit_and_withdrawal_sync_cash_krw(self):
        async def cash_krw_balance() -> float | None:
            items = (await self.client.get("/api/portfolio")).json()
            cash = next((it for it in items if it["stock_code"] == "CASH_KRW"), None)
            return None if cash is None else cash["quantity"] * cash["avg_price"]

        # No CASH_KRW until the first deposit.
        self.assertIsNone(await cash_krw_balance())

        resp = await self.client.post(
            "/api/portfolio/cashflows",
            json={"type": "deposit", "amount": 1_000_000, "date": "2026-05-01"},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertTrue(resp.json()["ok"])
        self.assertEqual(await cash_krw_balance(), 1_000_000)

        resp = await self.client.post(
            "/api/portfolio/cashflows",
            json={"type": "withdrawal", "amount": 400_000, "date": "2026-05-02"},
        )
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(await cash_krw_balance(), 600_000)

        # Over-withdrawal is rejected and leaves the balance untouched.
        resp = await self.client.post(
            "/api/portfolio/cashflows",
            json={"type": "withdrawal", "amount": 999_999_999, "date": "2026-05-03"},
        )
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(await cash_krw_balance(), 600_000)

        # Exactly two cashflows were committed (the rejected one rolled back).
        cashflows = (await self.client.get("/api/portfolio/cashflows")).json()
        self.assertEqual(len(cashflows), 2)
        self.assertEqual({cf["type"] for cf in cashflows}, {"deposit", "withdrawal"})


class LatencyMiddlewareStreamingTests(unittest.IsolatedAsyncioTestCase):
    """White-box checks on the ASGI middleware itself — in particular that it
    forwards a streaming body chunk-by-chunk instead of buffering it. This is
    the property that justifies using a pure-ASGI middleware over
    BaseHTTPMiddleware for an app that serves SSE."""

    @staticmethod
    def _scope(path: str = "/api/stream", method: str = "GET", slow_ms: float = 0.0) -> dict:
        state = type("S", (), {"slow_request_ms": slow_ms})()
        fake_app = type("A", (), {"state": state})()
        return {"type": "http", "path": path, "method": method, "app": fake_app}

    async def test_forwards_streaming_body_chunk_by_chunk(self):
        sent: list[dict] = []

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(message):
            sent.append(message)

        async def streaming_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            for chunk in (b"a", b"b", b"c"):
                await send({"type": "http.response.body", "body": chunk, "more_body": True})
            await send({"type": "http.response.body", "body": b"", "more_body": False})

        mw = app_factory._RequestLatencyMiddleware(streaming_app)
        with patch.object(observability, "record_event", new=AsyncMock()) as rec:
            await mw(self._scope(), receive, send)

        # Every downstream message reached the real send, in order, unmodified.
        self.assertEqual(sent[0]["type"], "http.response.start")
        self.assertEqual(sent[0]["status"], 200)
        bodies = [m["body"] for m in sent if m["type"] == "http.response.body"]
        self.assertEqual(bodies, [b"a", b"b", b"c", b""])
        # It still observed the (slow, threshold=0) request.
        rec.assert_awaited_once()
        self.assertEqual(rec.await_args.kwargs["details"]["status"], 200)

    async def test_non_api_scope_passes_through_unobserved(self):
        sent: list[dict] = []

        async def receive():
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(message):
            sent.append(message)

        async def app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok", "more_body": False})

        mw = app_factory._RequestLatencyMiddleware(app)
        with patch.object(observability, "record_event", new=AsyncMock()) as rec:
            await mw(self._scope(path="/healthz"), receive, send)

        self.assertEqual([m["type"] for m in sent], ["http.response.start", "http.response.body"])
        rec.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
