from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin

from repositories import cache_values
from services.market import indicator_health as health

NOW = datetime(2026, 9, 11, 1, 0, tzinfo=timezone.utc)


class IndicatorHealthTests(TempDbMixin):
    async def test_persistent_failure_notifies_once_and_recovery_notifies_once(self):
        with patch.object(health, "CODES", ["KOSPI"]), \
             patch("market_indicators.fetch_indicators", AsyncMock(return_value={})) as fetch, \
             patch.object(health, "_notify", AsyncMock(return_value=1)) as notify:
            initial = await health.check_once(now=NOW)
            self.assertEqual(initial["status"], "warn")
            notify.assert_not_called()
            await health.check_once(now=NOW + timedelta(minutes=9))
            notify.assert_not_called()
            failed = await health.check_once(now=NOW + timedelta(minutes=10))
            self.assertEqual(failed["status"], "error")
            await health.check_once(now=NOW + timedelta(minutes=11))
            self.assertEqual(notify.await_count, 1)
            # 상태는 DB에 저장돼 재시작 후에도 중복 장애 알림을 보내지 않는다.
            saved = await cache_values.get_cache_value_entry(health.NAMESPACE, health.KEY)
            self.assertTrue(saved.value["notified"])
            fetch.return_value = {"KOSPI": {"value": "6,800.00"}}
            await health.check_once(now=NOW + timedelta(minutes=12))
            await health.check_once(now=NOW + timedelta(minutes=13))
            self.assertEqual(notify.await_count, 2)
            self.assertIn("복구", notify.call_args.args[0])
            summary = await health.summary(now=NOW + timedelta(minutes=13))
            self.assertEqual(summary["status"], "ok")

    async def test_notification_failure_retries_and_total_timeout_counts_as_failure(self):
        with patch.object(health, "CODES", ["KOSPI"]), \
             patch("market_indicators.fetch_indicators", AsyncMock(side_effect=TimeoutError)), \
             patch.object(health, "_notify", AsyncMock(side_effect=[0, 1])) as notify:
            await health.check_once(now=NOW)
            first = await health.check_once(now=NOW + timedelta(minutes=10))
            self.assertFalse(first["notified"])
            second = await health.check_once(now=NOW + timedelta(minutes=11))
            self.assertTrue(second["notified"])
            self.assertEqual(notify.await_count, 2)

    async def test_market_status_and_provider_delay_prevent_closed_market_false_alarm(self):
        yesterday = (NOW - timedelta(days=1)).isoformat()
        self.assertIsNone(health.problem({"value": "100", "market_status": "CLOSE", "as_of": yesterday}, NOW))
        self.assertIsNotNone(health.problem({"value": "100", "market_status": "OPEN", "as_of": yesterday}, NOW))
        delayed = {"value": "100", "market_status": "OPEN", "as_of": (NOW - timedelta(minutes=20)).isoformat(), "delay_minutes": 15}
        self.assertIsNone(health.problem(delayed, NOW))
        self.assertIsNotNone(health.problem({"value": "100", "_degraded": True}, NOW))

    async def test_only_administrators_receive_operational_notifications(self):
        with patch.object(health.users, "get_all_users", AsyncMock(return_value=[{"google_sub": "admin", "is_admin": 1}, {"google_sub": "member", "is_admin": 0}])), \
             patch.object(health.channels, "dispatch", AsyncMock(return_value=1)) as dispatch:
            self.assertEqual(await health._notify("장애", "test"), 1)
            dispatch.assert_awaited_once_with("admin", "장애", dedupe_key="test")

    async def test_monitor_stopped_or_never_started_is_visible_in_quality_check(self):
        self.assertEqual((await health.summary(now=NOW))["status"], "warn")
        await cache_values.set_cache_value(health.NAMESPACE, health.KEY, {"checked_at": NOW.isoformat(), "status": "ok", "issues": {}})
        self.assertEqual((await health.summary(now=NOW + timedelta(minutes=6)))["status"], "warn")
