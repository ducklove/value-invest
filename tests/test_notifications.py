"""Notification feature tests: alert-rule CRUD over HTTP, engine edge-trigger,
and the Kakao token-refresh send path.

The CRUD tests boot the real ASGI app (httpx.ASGITransport) over a throwaway
SQLite file, like test_integration_flows. The engine + kakao tests drive the
services directly against the same temp-DB harness, with quote fetching and the
Kakao HTTP calls stubbed so the logic is exercised deterministically.
"""

from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

import cache
from core.app_factory import create_app
from core.config import AppSettings, PROJECT_ROOT
from routes import notifications as notif_route
from services.notifications import channels, engine, kakao


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


async def _set_target_price(google_sub, code, target):
    db = await cache.get_db()
    await db.execute(
        "UPDATE user_portfolio SET target_price = ? WHERE google_sub = ? AND stock_code = ?",
        (target, google_sub, code),
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
    async def test_channels_status_without_config(self):
        resp = await self.client.get("/api/notifications/channels")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertFalse(body["telegram"]["configured"])
        self.assertFalse(body["telegram"]["connected"])
        self.assertFalse(body["kakao"]["configured"])

    async def test_price_alert_crud_roundtrip(self):
        resp = await self.client.post(
            "/api/notifications/alerts",
            json={"alert_type": "price_above", "threshold": 72000, "stock_code": "005930", "note": "목표"},
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        rule = resp.json()
        self.assertEqual(rule["scope"], "stock")
        self.assertEqual(rule["armed"], 1)
        alert_id = rule["id"]

        resp = await self.client.get("/api/notifications/alerts")
        self.assertEqual(len(resp.json()), 1)

        resp = await self.client.put(f"/api/notifications/alerts/{alert_id}", json={"enabled": False})
        self.assertEqual(resp.json()["enabled"], 0)

        resp = await self.client.delete(f"/api/notifications/alerts/{alert_id}")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual((await self.client.get("/api/notifications/alerts")).json(), [])

    async def test_target_reached_needs_no_threshold(self):
        resp = await self.client.post(
            "/api/notifications/alerts",
            json={"alert_type": "target_reached", "stock_code": "005930"},
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        rule = resp.json()
        self.assertEqual(rule["scope"], "stock")
        self.assertEqual(rule["alert_type"], "target_reached")
        self.assertEqual(rule["threshold"], 0)

    async def test_stock_scope_daily_change(self):
        resp = await self.client.post(
            "/api/notifications/alerts",
            json={"alert_type": "daily_change_above", "scope": "stock", "threshold": 5, "stock_code": "005930"},
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(resp.json()["scope"], "stock")
        self.assertEqual(resp.json()["stock_code"], "005930")

    async def test_portfolio_scope_daily_change(self):
        resp = await self.client.post(
            "/api/notifications/alerts",
            json={"alert_type": "daily_change_below", "scope": "portfolio", "threshold": -3},
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(resp.json()["scope"], "portfolio")
        self.assertIsNone(resp.json()["stock_code"])

    async def test_nav_alert_no_stock(self):
        resp = await self.client.post(
            "/api/notifications/alerts",
            json={"alert_type": "nav_above", "threshold": 100000000},
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(resp.json()["scope"], "portfolio")

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
        with patch.object(engine, "_safe_quote", new=AsyncMock(return_value={"price": 80000.0})), \
             patch.object(channels, "dispatch", new=AsyncMock()) as disp:
            self.assertEqual(await engine.evaluate_user("u1"), 0)
        disp.assert_not_awaited()

    async def test_price_edge_trigger_fires_once_then_rearms(self):
        alert_id = await self._rule(threshold=72000.0)
        disp = AsyncMock()
        q = {"price": 80000.0}  # above

        with patch.object(engine, "_safe_quote", new=AsyncMock(side_effect=lambda code: dict(q))), \
             patch.object(channels, "dispatch", new=disp):
            self.assertEqual(await engine.evaluate_user("u1"), 1)
            self.assertEqual(disp.await_count, 1)
            self.assertEqual((await cache.get_portfolio_alert("u1", alert_id))["armed"], 0)

            self.assertEqual(await engine.evaluate_user("u1"), 0)  # still met, disarmed
            self.assertEqual(disp.await_count, 1)

            q["price"] = 60000.0  # below -> re-arm
            self.assertEqual(await engine.evaluate_user("u1"), 0)
            self.assertEqual((await cache.get_portfolio_alert("u1", alert_id))["armed"], 1)

            q["price"] = 90000.0  # above again -> fire
            self.assertEqual(await engine.evaluate_user("u1"), 1)
            self.assertEqual(disp.await_count, 2)

    async def test_target_reached_uses_holding_target(self):
        await _set_target_price("u1", "005930", 70000)
        await self._rule(alert_type="target_reached", threshold=0.0)
        captured = {}

        async def capture(google_sub, text):
            captured["text"] = text
            return 1

        # below target -> no fire
        with patch.object(engine, "_safe_quote", new=AsyncMock(return_value={"price": 60000.0})), \
             patch.object(channels, "dispatch", new=AsyncMock()) as disp:
            self.assertEqual(await engine.evaluate_user("u1"), 0)
            disp.assert_not_awaited()

        # at/above target -> fire with 목표가 message
        with patch.object(engine, "_safe_quote", new=AsyncMock(return_value={"price": 80000.0})), \
             patch.object(channels, "dispatch", new=capture):
            self.assertEqual(await engine.evaluate_user("u1"), 1)
        self.assertIn("목표가 달성", captured["text"])
        self.assertIn("삼성전자", captured["text"])

    async def test_stock_daily_change_uses_change_pct(self):
        alert_id = await self._rule(alert_type="daily_change_above", threshold=5.0)
        disp = AsyncMock()
        q = {"price": 80000.0, "change_pct": 7.0}  # +7% >= 5%

        with patch.object(engine, "_safe_quote", new=AsyncMock(side_effect=lambda code: dict(q))), \
             patch.object(channels, "dispatch", new=disp):
            self.assertEqual(await engine.evaluate_user("u1"), 1)
            q["change_pct"] = 3.0  # below -> re-arm, no fire
            self.assertEqual(await engine.evaluate_user("u1"), 0)
            self.assertEqual((await cache.get_portfolio_alert("u1", alert_id))["armed"], 1)

    async def test_disabled_rule_not_evaluated(self):
        await self._rule(enabled=False)
        with patch.object(engine, "_safe_quote", new=AsyncMock(return_value={"price": 80000.0})), \
             patch.object(channels, "dispatch", new=AsyncMock()) as disp:
            self.assertEqual(await engine.evaluate_user("u1"), 0)
        disp.assert_not_awaited()


class KakaoChannelTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "cache.db"
        self.db_patch = patch.object(cache, "DB_PATH", self.db_path)
        self.db_patch.start()
        await cache.close_db()
        await cache.init_db()
        await _seed_user_and_holding()

    async def asyncTearDown(self) -> None:
        await cache.close_db()
        self.db_patch.stop()
        self.temp_dir.cleanup()

    async def test_send_ok_with_fresh_token(self):
        channel = {
            "enabled": True,
            "config": {"access_token": "a", "access_expires_at": time.time() + 3600, "refresh_token": "r"},
        }
        with patch.object(kakao, "_send_memo", new=AsyncMock(return_value=200)) as send:
            ok = await kakao.send_to_user("u1", channel, "hi")
        self.assertTrue(ok)
        send.assert_awaited_once()

    async def test_refresh_on_401_then_retry(self):
        await cache.upsert_notification_channel(
            "u1", "kakao",
            config={"access_token": "old", "access_expires_at": time.time() + 3600, "refresh_token": "r"},
            enabled=True, verified=True,
        )
        ch = await cache.get_notification_channel("u1", "kakao")
        with patch.object(kakao, "_send_memo", new=AsyncMock(side_effect=[401, 200])), \
             patch.object(kakao, "_refresh_token", new=AsyncMock(return_value={
                 "access_token": "new", "access_expires_at": time.time() + 3600,
             })):
            ok = await kakao.send_to_user("u1", ch, "hi")
        self.assertTrue(ok)
        updated = await cache.get_notification_channel("u1", "kakao")
        self.assertEqual(updated["config"]["access_token"], "new")

    async def test_expired_without_refresh_token_fails(self):
        channel = {"enabled": True, "config": {"access_token": "a", "access_expires_at": time.time() - 10}}
        with patch.object(kakao, "_send_memo", new=AsyncMock(return_value=200)) as send:
            ok = await kakao.send_to_user("u1", channel, "hi")
        self.assertFalse(ok)
        send.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
