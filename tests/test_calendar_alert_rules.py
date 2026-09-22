"""Recurring country/importance rules: API, automatic discovery and delivery."""

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, patch

import test_notifications as existing

import economic_calendar
from repositories import bootstrap
from repositories import calendar_rules as rules_repo
from repositories import notifications as repo
from routes import notifications as routes
from services.notifications import alert_delivery, calendar_rules, channels, engine

RULES = [
    {"country": "kr", "min_importance": "all"},
    {"country": "us", "min_importance": "mid"},
    {"country": "jp", "min_importance": "high"},
]


class CalendarRuleTests(existing.NotificationHarness):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        clock = patch.object(alert_delivery, "now_kst", return_value=datetime(2026, 9, 23, 12, tzinfo=alert_delivery.KST))
        self.clock = clock.start()
        self.addCleanup(clock.stop)
        await repo.upsert_notification_channel("u1", "telegram", config={"chat_id": "test"})

    async def save(self, rules=RULES):
        response = await self.client.put("/api/notifications/calendar/rules", json={"rules": rules})
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def event(self, eid="us-high", country="us", importance="high", **fields):
        return {
            "index_id": eid, "country": country, "country_name": country,
            "importance": importance, "date": "2026-09-23", "datetime": "2026-09-23 13:00:00",
            "event": eid, "actual": "2.0%", "forecast": "1.9%", **fields,
        }

    async def evaluate(self, events, sent=1):
        with patch.object(economic_calendar, "fetch_economic_calendar", AsyncMock(return_value={"events": events})) as fetch, \
             patch.object(channels, "dispatch", AsyncMock(return_value=sent)) as send:
            result = await engine.evaluate_calendar_all()
        return result, fetch, send

    async def test_roundtrip_independent_users_and_noop_preserves_start(self):
        self.assertEqual((await self.client.get("/api/notifications/calendar/rules")).json()["rules"], [])
        saved = await self.save()
        await bootstrap.close_db()
        await bootstrap.init_db()
        self.assertEqual((await self.client.get("/api/notifications/calendar/rules")).json(), saved)
        self.assertEqual(len(saved["countries"]), len(economic_calendar.COUNTRY_META))
        self.clock.return_value = datetime(2026, 9, 24, 12, tzinfo=alert_delivery.KST)
        self.assertEqual((await self.save())["rules"], saved["rules"])
        await existing._seed_user_and_holding("u2")
        with patch.object(routes, "get_current_user", AsyncMock(return_value={"google_sub": "u2"})):
            self.assertEqual((await self.client.get("/api/notifications/calendar/rules")).json()["rules"], [])
            await self.save([])
        self.assertEqual(len(await rules_repo.list_rules("u1")), 3)

    async def test_auth_channel_and_validation(self):
        with patch.object(routes, "get_current_user", AsyncMock(return_value=None)):
            self.assertEqual((await self.client.get("/api/notifications/calendar/rules")).status_code, 401)
            self.assertEqual((await self.client.put("/api/notifications/calendar/rules", json={"rules": []})).status_code, 401)
        await self.save()
        for invalid in (None, {}, [RULES[0], RULES[0]], [{"country": "xx", "min_importance": "all"}],
                        [{"country": [], "min_importance": "all"}], [{"country": "us", "min_importance": []}], ["us"]):
            response = await self.client.put("/api/notifications/calendar/rules", json={"rules": invalid})
            self.assertEqual(response.status_code, 400, response.text)
        self.assertEqual(len(await rules_repo.list_rules("u1")), 3)
        await repo.delete_notification_channel("u1", "telegram")
        self.assertEqual((await self.client.put("/api/notifications/calendar/rules", json={"rules": RULES})).status_code, 409)
        await self.save([])  # Can always turn off rules without a channel.

    async def test_country_thresholds_discover_without_manual_subscriptions(self):
        await self.save()
        events = [self.event(f"{country}-{level}", country, level)
                  for country in ("kr", "us", "jp", "gb") for level in ("low", "mid", "high")]
        result, fetch, send = await self.evaluate(events)
        self.assertEqual(result["sent"], 6)
        messages = "\n".join(call.args[1] for call in send.call_args_list)
        for eid in ("kr-low", "kr-mid", "kr-high", "us-mid", "us-high", "jp-high"):
            self.assertIn(eid, messages)
        for eid in ("us-low", "jp-mid", "jp-low", "gb-high"):
            self.assertNotIn(eid, messages)
        self.assertEqual(fetch.call_args.kwargs["countries"], ["jp", "kr", "us"])
        self.assertEqual((await self.evaluate(events))[0]["sent"], 0)

    async def test_future_events_are_enrolled_then_delivered_once_after_restart(self):
        await self.save()
        event = self.event(date="2026-09-24", datetime="2026-09-24 09:00:00", actual="")
        result, _, send = await self.evaluate([event])
        self.assertEqual(result["sent"], 0)
        send.assert_not_awaited()
        self.assertEqual(len(await repo.list_pending_calendar_subscriptions()), 1)
        await bootstrap.close_db()
        await bootstrap.init_db()
        self.clock.return_value = datetime(2026, 9, 24, 9, 10, tzinfo=alert_delivery.KST)
        event["actual"] = "3.0%"
        self.assertEqual((await self.evaluate([event]))[0]["sent"], 1)
        self.assertEqual((await self.evaluate([event]))[0]["sent"], 0)
        # Next week's releases need no browser visit or individual subscription.
        self.clock.return_value = datetime(2026, 9, 30, 12, tzinfo=alert_delivery.KST)
        event.update(index_id="new-week", date="2026-09-30", datetime="2026-09-30 09:00:00")
        self.assertEqual((await self.evaluate([event]))[0]["sent"], 1)

    async def test_new_rule_skips_old_results_but_catches_results_since_save(self):
        await self.save()
        events = [self.event("old", datetime="2026-09-23 11:59:00"), self.event("new")]
        result, _, send = await self.evaluate(events)
        self.assertEqual(result["sent"], 1)
        self.assertIn("new", send.call_args.args[1])
        self.assertNotIn("old", send.call_args.args[1])

    async def test_date_only_event_is_enrolled_before_result_then_sent(self):
        await self.save()
        event = self.event(datetime="", actual="")
        self.assertEqual((await self.evaluate([event]))[0]["sent"], 0)
        event["actual"] = "3"
        self.assertEqual((await self.evaluate([event]))[0]["sent"], 1)
        # Unknown publish time + already released at first observation is skipped.
        self.assertEqual((await self.evaluate([self.event("old-date-only", datetime="")]))[0]["sent"], 0)

    async def test_manual_and_rule_subscription_overlap_does_not_duplicate(self):
        await self.save()
        await repo.upsert_calendar_subscription("u1", "us-high", event_date="2026-09-23", country="us")
        result, _, send = await self.evaluate([self.event()])
        self.assertEqual(result["sent"], 1)
        send.assert_awaited_once()
        self.assertEqual((await self.evaluate([self.event()]))[0]["sent"], 0)

    async def test_narrowing_and_deleting_rules_cancel_only_automatic_pending(self):
        await self.save()
        await self.evaluate([self.event("us-mid", importance="mid", actual=""), self.event(actual="")])
        await repo.upsert_calendar_subscription("u1", "manual", event_date="2026-09-23", country="us", importance="low")
        await self.save([{"country": "us", "min_importance": "high"}])
        self.assertEqual({s["event_id"] for s in await repo.list_pending_calendar_subscriptions()}, {"us-high", "manual"})
        await self.save([])
        self.assertEqual([s["event_id"] for s in await repo.list_pending_calendar_subscriptions()], ["manual"])
        result, _, send = await self.evaluate([self.event("manual", importance="low"), self.event()])
        self.assertEqual(result["sent"], 1)
        self.assertIn("manual", send.call_args.args[1])

    async def test_auto_subscription_can_be_promoted_to_manual(self):
        await self.save()
        await self.evaluate([self.event(actual="")])
        await repo.upsert_calendar_subscription("u1", "us-high", event_date="2026-09-23", country="us")
        await self.save([])
        self.assertEqual(len(await repo.list_pending_calendar_subscriptions()), 1)
        self.assertEqual((await self.evaluate([self.event()]))[0]["sent"], 1)

    async def test_failed_delivery_remains_pending_for_retry(self):
        await self.save()
        self.assertEqual((await self.evaluate([self.event()], sent=0))[0]["sent"], 0)
        self.assertEqual(len(await repo.list_pending_calendar_subscriptions()), 1)
        self.assertEqual((await self.evaluate([self.event()]))[0]["sent"], 1)

    async def test_disconnected_user_does_not_send_but_reconnect_catches_up(self):
        await self.save()
        await repo.set_notification_channel_enabled("u1", "telegram", False)
        result, _, send = await self.evaluate([self.event()])
        self.assertEqual(result["sent"], 0)
        send.assert_not_awaited()
        await repo.set_notification_channel_enabled("u1", "telegram", True)
        self.assertEqual((await self.evaluate([self.event()]))[0]["sent"], 1)

    async def test_rule_deleted_during_fetch_is_not_recreated(self):
        await self.save()
        snapshot = await rules_repo.list_rules()
        await self.save([])
        await calendar_rules.discover(snapshot, [self.event()])
        self.assertEqual(await repo.list_pending_calendar_subscriptions(), [])

    async def test_overlapping_evaluation_does_not_double_send(self):
        await self.save()
        entered, release = asyncio.Event(), asyncio.Event()

        async def slow_fetch(**kwargs):
            entered.set()
            await release.wait()
            return {"events": [self.event()]}

        with patch.object(economic_calendar, "fetch_economic_calendar", slow_fetch), \
             patch.object(channels, "dispatch", AsyncMock(return_value=1)) as send:
            first = asyncio.create_task(engine.evaluate_calendar_all())
            await entered.wait()
            second = await engine.evaluate_calendar_all()
            release.set()
            self.assertEqual((await first)["sent"], 1)
            self.assertEqual(second["skipped"], "already_running")
            send.assert_awaited_once()
