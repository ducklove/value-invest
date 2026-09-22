"""Quiet-hour boundaries, HTTP settings and durable, channel-specific replay."""

from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest
import test_notifications as existing

from repositories import bootstrap
from repositories import notifications as repo
from routes import notifications as routes
from services.notifications import alert_delivery as delivery
from services.notifications import channels, engine


@pytest.mark.parametrize("at,expected", [
    ("2026-09-23T20:59:59+09:00", False),
    ("2026-09-23T21:00:00+09:00", True),
    ("2026-09-24T00:00:00+09:00", True),
    ("2026-09-24T07:59:59+09:00", True),
    ("2026-09-24T08:00:00+09:00", False),
    ("2026-09-25T12:00:00+00:00", True),
    ("2026-09-26T08:00:00+09:00", False),  # Saturday still drains
])
def test_overnight_boundaries_use_kst(at, expected):
    assert delivery.is_quiet(delivery.DEFAULT_SETTINGS, datetime.fromisoformat(at)) is expected


@pytest.mark.parametrize("time,expected", [("12:29", False), ("12:30", True), ("13:44", True), ("13:45", False)])
def test_daytime_window(time, expected):
    settings = {**delivery.DEFAULT_SETTINGS, "start": "12:30", "end": "13:45"}
    assert delivery.is_quiet(settings, datetime.fromisoformat(f"2026-09-23T{time}:00+09:00")) is expected


class QuietHoursApiTests(existing.NotificationHarness):
    async def test_settings_roundtrip_and_user_isolation(self):
        self.assertEqual((await self.client.get("/api/notifications/quiet-hours")).json(), delivery.DEFAULT_SETTINGS)
        settings = {"enabled": True, "start": "23:15", "end": "07:45", "mode": "defer"}
        response = await self.client.put("/api/notifications/quiet-hours", json=settings)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual((await self.client.get("/api/notifications/quiet-hours")).json(), settings)
        await existing._seed_user_and_holding("u2")
        self.assertEqual(await delivery.get_settings("u2"), delivery.DEFAULT_SETTINGS)
        settings["enabled"] = False
        self.assertEqual((await self.client.put("/api/notifications/quiet-hours", json=settings)).json(), settings)

    async def test_invalid_payloads_do_not_change_settings(self):
        for change in ({"start": "24:00"}, {"end": "7:00"}, {"start": "08:00"},
                       {"enabled": "false"}, {"mode": "unknown"}, {"end": None}, {"mode": []}):
            response = await self.client.put("/api/notifications/quiet-hours", json={**delivery.DEFAULT_SETTINGS, **change})
            self.assertEqual(response.status_code, 400, response.text)
        self.assertEqual(await delivery.get_settings("u1"), delivery.DEFAULT_SETTINGS)

    async def test_auth_required_for_read_and_write(self):
        with patch.object(routes, "get_current_user", AsyncMock(return_value=None)):
            self.assertEqual((await self.client.get("/api/notifications/quiet-hours")).status_code, 401)
            self.assertEqual((await self.client.put("/api/notifications/quiet-hours", json=delivery.DEFAULT_SETTINGS)).status_code, 401)


class QuietHoursDeliveryTests(existing.NotificationHarness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        await repo.upsert_notification_channel("u1", "telegram", config={"chat_id": "123", "bot_token": "test"})
        self.rule_id = await repo.create_portfolio_alert(
            "u1", scope="stock", alert_type="price_above", threshold=100, stock_code="005930"
        )
        clock = patch.object(delivery, "now_kst", return_value=datetime(2026, 9, 25, 23, 30, tzinfo=delivery.KST))
        self.clock = clock.start()
        self.addCleanup(clock.stop)

    async def defer(self):
        await delivery.save_settings("u1", {**delivery.DEFAULT_SETTINGS, "mode": "defer"})

    def morning(self):
        self.clock.return_value = datetime(2026, 9, 26, 8, tzinfo=delivery.KST)

    async def test_skipped_crossing_is_not_sent_in_morning(self):
        with patch.object(engine, "_safe_quote", AsyncMock(return_value={"price": 110})), \
             patch.object(channels, "dispatch", AsyncMock(return_value=1)) as send:
            self.assertEqual(await engine.evaluate_user("u1"), 0)
            self.assertEqual(await repo.list_pending_portfolio_alerts("u1"), [])
            self.morning()
            self.assertEqual(await engine.evaluate_user("u1"), 0)
            send.assert_not_awaited()
            # A new crossing after quiet hours is still eligible.
            with patch.object(engine, "_safe_quote", AsyncMock(return_value={"price": 90})):
                await engine.evaluate_user("u1")

    async def test_deferred_crossing_survives_restart_and_sends_original_value_once(self):
        await self.defer()
        with patch.object(engine, "_safe_quote", AsyncMock(return_value={"price": 110})), \
             patch.object(channels, "dispatch", AsyncMock(return_value=1)) as send:
            self.assertEqual(await engine.evaluate_user("u1"), 0)
            await engine.evaluate_user("u1")
            send.assert_not_awaited()
            self.assertEqual(len(await repo.list_pending_portfolio_alerts("u1")), 1)
            await bootstrap.close_db()
            await bootstrap.init_db()
            self.morning()
            # The condition has disappeared, but the captured alert is replayed.
            with patch.object(engine, "_safe_quote", AsyncMock(return_value={"price": 90})):
                self.assertEqual(await engine.evaluate_user("u1"), 1)
                self.assertEqual(await engine.evaluate_user("u1"), 0)
            send.assert_awaited_once()
            self.assertIn("110", send.call_args.args[1])
            self.assertIn("09-25 23:30", send.call_args.args[1])
            self.assertEqual(await repo.list_pending_portfolio_alerts("u1"), [])

    async def test_partial_delivery_retries_only_failed_channel(self):
        await self.defer()
        await repo.upsert_notification_channel("u1", "kakao", config={"access_token": "test"})
        await delivery.dispatch("u1", self.rule_id, "night alert")
        self.morning()
        with patch.object(channels.telegram, "send_message", AsyncMock(return_value=True)) as tg, \
             patch.object(channels.kakao, "send_to_user", AsyncMock(side_effect=[False, True])) as kk:
            self.assertEqual(await delivery.flush_pending("u1"), 1)
            pending = await repo.list_pending_portfolio_alerts("u1")
            self.assertEqual([row["channel"] for row in pending], ["kakao"])
            self.assertEqual(await delivery.flush_pending("u1"), 1)
            tg.assert_awaited_once()
            self.assertEqual(kk.await_count, 2)
        self.assertEqual(await repo.list_pending_portfolio_alerts("u1"), [])

    async def test_disabled_quiet_hours_releases_queue_and_allows_night_alerts(self):
        await self.defer()
        await delivery.dispatch("u1", self.rule_id, "queued")
        await delivery.save_settings("u1", {**delivery.DEFAULT_SETTINGS, "enabled": False})
        with patch.object(channels, "dispatch", AsyncMock(return_value=1)) as send:
            self.assertEqual(await delivery.flush_pending("u1"), 1)
            self.assertEqual(await delivery.dispatch("u1", self.rule_id, "live"), 1)
            self.assertEqual(send.await_count, 2)

    async def test_queue_deduplicates_and_rule_edits_cancel_old_alerts(self):
        await self.defer()
        for _ in range(2):
            await delivery.dispatch("u1", self.rule_id, "queued")
        self.assertEqual(len(await repo.list_pending_portfolio_alerts("u1")), 1)
        await repo.update_portfolio_alert("u1", self.rule_id, enabled=False)
        await repo.update_portfolio_alert("u1", self.rule_id, enabled=True)
        self.assertEqual(await repo.list_pending_portfolio_alerts("u1"), [])
        await delivery.dispatch("u1", self.rule_id, "another")
        await repo.delete_portfolio_alert("u1", self.rule_id)
        self.assertEqual(await repo.list_pending_portfolio_alerts("u1"), [])

    async def test_multiple_alerts_are_sent_in_order_at_end_not_before(self):
        await self.defer()
        await delivery.dispatch("u1", self.rule_id, "first")
        await delivery.dispatch("u1", self.rule_id, "second")
        with patch.object(channels, "dispatch", AsyncMock(return_value=1)) as send:
            self.assertEqual(await delivery.flush_pending("u1"), 0)
            send.assert_not_awaited()
            self.morning()
            self.assertEqual(await delivery.flush_pending("u1"), 2)
            self.assertTrue(send.call_args_list[0].args[1].endswith("first"))
            self.assertTrue(send.call_args_list[1].args[1].endswith("second"))
