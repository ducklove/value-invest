"""Notification feature tests: alert-rule CRUD over HTTP + engine edge-trigger.

The CRUD tests boot the real ASGI app (httpx.ASGITransport) over a throwaway
SQLite file, like test_integration_flows. The engine tests drive
``services.notifications.engine.evaluate_user`` directly against the same
temp-DB harness, with the quote source and channel dispatch stubbed so the
edge-trigger (fire-once / re-arm) logic is exercised deterministically.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

import cache
from core.app_factory import create_app
from core.config import AppSettings, PROJECT_ROOT
from routes import notifications as notif_route
from services.notifications import channels, engine


def _test_settings() -> AppSettings:
    return AppSettings(
        environment="development",
        project_root=PROJECT_ROOT,
        app_title="Test Compass",
        public_api_base_url="https://api.example.test",
        cors_allowed_origins=("https://app.example.test",),
        enable_docs=False,
    )


async def _seed_user_and_holding(google_sub="u1", code="005930", name="삼성전자"):
    db = await cache.get_db()
    await db.execute(
        "INSERT OR IGNORE INTO users (google_sub, email, name, picture, email_verified, created_at, last_login_at)"
        " VALUES (?, 'e@x', 'U', '', 1, 't', 't')",
        (google_sub,),
    )
    await db.execute(
        "INSERT OR IGNORE INTO user_portfolio (google_sub, stock_code, stock_name, quantity, avg_price, created_at, updated_at)"
        " VALUES (?, ?, ?, 10, 1000, 't', 't')",
        (google_sub, code, name),
    )
    await db.commit()


class NotificationHarness(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "cache.db"
        self.db_patch = patch.object(cache, "DB_PATH", self.db_path)
        self.db_patch.start()
        await cache.close_db()
        await cache.init_db()
        await _seed_user_and_holding()

        self.user = {"google_sub": "u1", "email": "e@x", "name": "U"}
        self.auth_patch = patch.object(
            notif_route, "get_current_user", new=AsyncMock(return_value=self.user)
        )
        self.auth_patch.start()

        self.app = create_app(_test_settings())
        self.transport = httpx.ASGITransport(app=self.app, raise_app_exceptions=False)
        self.client = httpx.AsyncClient(transport=self.transport, base_url="http://testserver")

    async def asyncTearDown(self) -> None:
        await self.client.aclose()
        self.auth_patch.stop()
        await cache.close_db()
        self.db_patch.stop()
        self.temp_dir.cleanup()


class AlertCrudTests(NotificationHarness):
    async def test_channels_status_without_bot(self):
        resp = await self.client.get("/api/notifications/channels")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertFalse(body["bot_configured"])
        self.assertFalse(body["telegram"]["connected"])

    async def test_alert_crud_roundtrip(self):
        # create
        resp = await self.client.post(
            "/api/notifications/alerts",
            json={"alert_type": "price_above", "threshold": 72000, "stock_code": "005930", "note": "목표"},
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        rule = resp.json()
        self.assertEqual(rule["scope"], "stock")
        self.assertEqual(rule["alert_type"], "price_above")
        self.assertEqual(rule["armed"], 1)
        alert_id = rule["id"]

        # list
        resp = await self.client.get("/api/notifications/alerts")
        self.assertEqual(len(resp.json()), 1)

        # toggle off
        resp = await self.client.put(f"/api/notifications/alerts/{alert_id}", json={"enabled": False})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["enabled"], 0)

        # delete
        resp = await self.client.delete(f"/api/notifications/alerts/{alert_id}")
        self.assertEqual(resp.status_code, 200)
        resp = await self.client.get("/api/notifications/alerts")
        self.assertEqual(resp.json(), [])

    async def test_portfolio_alert_does_not_require_stock(self):
        resp = await self.client.post(
            "/api/notifications/alerts",
            json={"alert_type": "nav_above", "threshold": 100000000},
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(resp.json()["scope"], "portfolio")
        self.assertIsNone(resp.json()["stock_code"])

    async def test_price_alert_rejects_unheld_stock(self):
        resp = await self.client.post(
            "/api/notifications/alerts",
            json={"alert_type": "price_below", "threshold": 100, "stock_code": "000660"},
        )
        self.assertEqual(resp.status_code, 400)

    async def test_invalid_alert_type_rejected(self):
        resp = await self.client.post(
            "/api/notifications/alerts",
            json={"alert_type": "moon_phase", "threshold": 1},
        )
        self.assertEqual(resp.status_code, 400)


class AlertEngineHarness(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "cache.db"
        self.db_patch = patch.object(cache, "DB_PATH", self.db_path)
        self.db_patch.start()
        await cache.close_db()
        await cache.init_db()
        await _seed_user_and_holding()
        # A verified, enabled Telegram channel so has_active_channel() is True.
        await cache.upsert_notification_channel(
            "u1", "telegram", config={"chat_id": 123, "username": "t"}, enabled=True, verified=True
        )

    async def asyncTearDown(self) -> None:
        await cache.close_db()
        self.db_patch.stop()
        self.temp_dir.cleanup()

    async def _rule(self, **kw):
        defaults = dict(scope="stock", alert_type="price_above", threshold=72000.0, stock_code="005930")
        defaults.update(kw)
        return await cache.create_portfolio_alert("u1", **defaults)

    async def test_no_dispatch_without_active_channel(self):
        await cache.delete_notification_channel("u1", "telegram")
        await self._rule()
        with patch.object(engine, "_safe_quote_price", new=AsyncMock(return_value=80000.0)), \
             patch.object(channels, "dispatch", new=AsyncMock()) as disp:
            sent = await engine.evaluate_user("u1")
        self.assertEqual(sent, 0)
        disp.assert_not_awaited()

    async def test_edge_trigger_fires_once_then_rearms(self):
        alert_id = await self._rule(threshold=72000.0)
        disp = AsyncMock()
        prices = {"v": 80000.0}  # above threshold

        async def fake_quote(code):
            return prices["v"]

        with patch.object(engine, "_safe_quote_price", new=AsyncMock(side_effect=fake_quote)), \
             patch.object(channels, "dispatch", new=disp):
            # 1) condition met + armed -> fires
            self.assertEqual(await engine.evaluate_user("u1"), 1)
            self.assertEqual(disp.await_count, 1)
            row = await cache.get_portfolio_alert("u1", alert_id)
            self.assertEqual(row["armed"], 0)

            # 2) still met, but disarmed -> no fire
            self.assertEqual(await engine.evaluate_user("u1"), 0)
            self.assertEqual(disp.await_count, 1)

            # 3) drops below -> re-arms, no fire
            prices["v"] = 60000.0
            self.assertEqual(await engine.evaluate_user("u1"), 0)
            row = await cache.get_portfolio_alert("u1", alert_id)
            self.assertEqual(row["armed"], 1)

            # 4) rises above again -> fires again
            prices["v"] = 90000.0
            self.assertEqual(await engine.evaluate_user("u1"), 1)
            self.assertEqual(disp.await_count, 2)

    async def test_message_contains_stock_name(self):
        await self._rule(threshold=100.0)
        captured = {}

        async def capture(google_sub, text):
            captured["text"] = text
            return 1

        with patch.object(engine, "_safe_quote_price", new=AsyncMock(return_value=80000.0)), \
             patch.object(channels, "dispatch", new=capture):
            await engine.evaluate_user("u1")
        self.assertIn("삼성전자", captured["text"])
        self.assertIn("지정가", captured["text"])

    async def test_disabled_rule_not_evaluated(self):
        await self._rule(enabled=False)
        with patch.object(engine, "_safe_quote_price", new=AsyncMock(return_value=80000.0)), \
             patch.object(channels, "dispatch", new=AsyncMock()) as disp:
            self.assertEqual(await engine.evaluate_user("u1"), 0)
        disp.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
