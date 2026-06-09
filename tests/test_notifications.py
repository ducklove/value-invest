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
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

import cache
import economic_calendar
from core.app_factory import create_app
from core.config import AppSettings, PROJECT_ROOT
from routes import notifications as notif_route
from services.notifications import channels, engine, kakao, telegram


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
    async def test_channels_status_initial(self):
        resp = await self.client.get("/api/notifications/channels")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertFalse(body["telegram"]["connected"])
        self.assertFalse(body["kakao"]["connected"])
        # kakao block exposes the redirect_uri the user must register
        self.assertIn("redirect_uri", body["kakao"])

    async def test_telegram_register_manual_chat(self):
        with patch.object(telegram, "get_me", new=AsyncMock(return_value={"username": "mybot"})), \
             patch.object(telegram, "send_message", new=AsyncMock(return_value=True)):
            resp = await self.client.post(
                "/api/notifications/telegram/register",
                json={"bot_token": "123:ABC", "chat_id": "555"},
            )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertTrue(resp.json()["connected"])
        ch = await cache.get_notification_channel("u1", "telegram")
        self.assertEqual(ch["config"]["chat_id"], "555")
        self.assertEqual(ch["config"]["bot_token"], "123:ABC")
        self.assertTrue(ch["verified"])

    async def test_telegram_register_autodetect_chat(self):
        with patch.object(telegram, "get_me", new=AsyncMock(return_value={"username": "mybot"})), \
             patch.object(telegram, "get_recent_chat_id", new=AsyncMock(return_value=("999", "nick"))), \
             patch.object(telegram, "send_message", new=AsyncMock(return_value=True)):
            resp = await self.client.post(
                "/api/notifications/telegram/register", json={"bot_token": "123:ABC"}
            )
        self.assertTrue(resp.json()["connected"])
        self.assertEqual(resp.json()["chat_id"], "999")

    async def test_telegram_register_needs_message_first(self):
        with patch.object(telegram, "get_me", new=AsyncMock(return_value={"username": "mybot"})), \
             patch.object(telegram, "get_recent_chat_id", new=AsyncMock(return_value=None)):
            resp = await self.client.post(
                "/api/notifications/telegram/register", json={"bot_token": "123:ABC"}
            )
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(resp.json()["connected"])
        ch = await cache.get_notification_channel("u1", "telegram")
        self.assertFalse(ch["verified"])  # token stored, awaiting chat_id

    async def test_telegram_register_invalid_token(self):
        with patch.object(telegram, "get_me", new=AsyncMock(return_value=None)):
            resp = await self.client.post(
                "/api/notifications/telegram/register", json={"bot_token": "bad"}
            )
        self.assertEqual(resp.status_code, 400)

    async def test_kakao_connect_returns_authorize_url(self):
        resp = await self.client.post(
            "/api/notifications/kakao/connect", json={"rest_key": "REST123"}
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertIn("REST123", body["authorize_url"])
        self.assertIn("/api/notifications/kakao/callback", body["redirect_uri"])
        ch = await cache.get_notification_channel("u1", "kakao")
        self.assertEqual(ch["config"]["rest_key"], "REST123")
        self.assertFalse(ch["verified"])  # not verified until callback

    async def test_kakao_connect_requires_key(self):
        resp = await self.client.post("/api/notifications/kakao/connect", json={})
        self.assertEqual(resp.status_code, 400)

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

    async def test_important_flag_crud(self):
        # 생성 시 important=True → 저장. 이후 중요 표시만 토글(검증 없이).
        resp = await self.client.post(
            "/api/notifications/alerts",
            json={"alert_type": "price_above", "threshold": 72000, "stock_code": "005930", "important": True},
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        rule = resp.json()
        self.assertEqual(rule["important"], 1)
        alert_id = rule["id"]

        resp = await self.client.put(f"/api/notifications/alerts/{alert_id}", json={"important": False})
        self.assertEqual(resp.json()["important"], 0)
        resp = await self.client.put(f"/api/notifications/alerts/{alert_id}", json={"important": True})
        self.assertEqual(resp.json()["important"], 1)

    async def test_important_defaults_off(self):
        resp = await self.client.post(
            "/api/notifications/alerts", json={"alert_type": "nav_above", "threshold": 100000000}
        )
        self.assertEqual(resp.json()["important"], 0)

    async def test_important_toggle_preserves_armed(self):
        # 발송되어 disarmed 된 규칙을 중요로 토글해도 엣지 상태(armed=0)가 보존되어
        # 같은 날 재발송되지 않는다.
        rid = await cache.create_portfolio_alert(
            "u1", scope="stock", alert_type="price_above", threshold=72000.0, stock_code="005930"
        )
        await cache.set_portfolio_alert_state(rid, armed=False, last_value=80000.0, triggered=True)
        resp = await self.client.put(f"/api/notifications/alerts/{rid}", json={"important": True})
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(resp.json()["important"], 1)
        self.assertEqual(resp.json()["armed"], 0)  # 보존됨

    async def test_analysis_stock_alerts_allow_unheld(self):
        # 분석 화면(source=analysis)은 비보유 종목(000660)도 허용, 4유형 모두.
        for body in (
            {"alert_type": "price_above", "threshold": 50000, "stock_code": "000660", "source": "analysis"},
            {"alert_type": "stock_daily_abs", "threshold": 5, "stock_code": "000660", "source": "analysis"},
            {"alert_type": "disclosure_new", "stock_code": "000660", "source": "analysis"},
            {"alert_type": "report_new", "stock_code": "000660", "source": "analysis"},
        ):
            resp = await self.client.post("/api/notifications/alerts", json=body)
            self.assertEqual(resp.status_code, 200, resp.text)
            self.assertEqual(resp.json()["scope"], "stock")
            self.assertEqual(resp.json()["stock_code"], "000660")

    async def test_unheld_stock_rejected_without_analysis_source(self):
        # source 없으면 기존처럼 보유 종목만 허용 → 비보유(000660) 거부(하위호환).
        resp = await self.client.post(
            "/api/notifications/alerts",
            json={"alert_type": "price_above", "threshold": 50000, "stock_code": "000660"},
        )
        self.assertEqual(resp.status_code, 400)

    async def test_feed_alert_has_no_threshold(self):
        resp = await self.client.post(
            "/api/notifications/alerts",
            json={"alert_type": "disclosure_new", "stock_code": "005930", "source": "analysis"},
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(resp.json()["threshold"], 0)

    async def test_stock_daily_abs_requires_positive(self):
        resp = await self.client.post(
            "/api/notifications/alerts",
            json={"alert_type": "stock_daily_abs", "threshold": 0, "stock_code": "005930", "source": "analysis"},
        )
        self.assertEqual(resp.status_code, 400)

    async def test_per_stock_singleton_vs_price_multi(self):
        # (종목,유형) singleton: stock_daily_abs 재생성은 같은 규칙 갱신.
        first = await self.client.post(
            "/api/notifications/alerts",
            json={"alert_type": "stock_daily_abs", "threshold": 5, "stock_code": "005930", "source": "analysis"},
        )
        rid = first.json()["id"]
        second = await self.client.post(
            "/api/notifications/alerts",
            json={"alert_type": "stock_daily_abs", "threshold": 8, "stock_code": "005930", "source": "analysis"},
        )
        self.assertEqual(second.json()["id"], rid)
        self.assertEqual(second.json()["threshold"], 8)
        # 가격 알림은 한 종목에 여러 개 허용(서로 다른 id).
        p1 = await self.client.post("/api/notifications/alerts", json={"alert_type": "price_above", "threshold": 70000, "stock_code": "005930", "source": "analysis"})
        p2 = await self.client.post("/api/notifications/alerts", json={"alert_type": "price_above", "threshold": 80000, "stock_code": "005930", "source": "analysis"})
        self.assertNotEqual(p1.json()["id"], p2.json()["id"])

    async def test_alerts_filter_by_stock_code(self):
        await self.client.post("/api/notifications/alerts", json={"alert_type": "price_above", "threshold": 70000, "stock_code": "005930", "source": "analysis"})
        await self.client.post("/api/notifications/alerts", json={"alert_type": "price_above", "threshold": 50000, "stock_code": "000660", "source": "analysis"})
        rows = (await self.client.get("/api/notifications/alerts?stock_code=000660")).json()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["stock_code"], "000660")

    async def test_blanket_feed_all_alerts_crud(self):
        # 신규 공시(전체)/리포트(전체): scope=all_stocks, threshold 0, 사용자당 singleton.
        for atype in ("disclosure_new_all", "report_new_all"):
            resp = await self.client.post("/api/notifications/alerts", json={"alert_type": atype})
            self.assertEqual(resp.status_code, 200, resp.text)
            rule = resp.json()
            self.assertEqual(rule["scope"], "all_stocks")
            self.assertEqual(rule["threshold"], 0)
            self.assertIsNone(rule["stock_code"])
            resp2 = await self.client.post("/api/notifications/alerts", json={"alert_type": atype})
            self.assertEqual(resp2.json()["id"], rule["id"])  # singleton upsert

    async def test_analysis_feed_off_creates_disabled_rule(self):
        # 종목 분석 '꺼짐' = enabled:false 개별 규칙(전체를 억제하는 마커).
        resp = await self.client.post(
            "/api/notifications/alerts",
            json={"alert_type": "disclosure_new", "stock_code": "005930", "source": "analysis", "enabled": False},
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(resp.json()["enabled"], 0)

    async def test_target_reached_blanket_singleton(self):
        resp = await self.client.post(
            "/api/notifications/alerts", json={"alert_type": "target_reached"}
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        rule = resp.json()
        self.assertEqual(rule["scope"], "all_stocks")
        self.assertIsNone(rule["stock_code"])
        self.assertEqual(rule["threshold"], 0)
        # creating again upserts the same singleton rule
        resp2 = await self.client.post(
            "/api/notifications/alerts", json={"alert_type": "target_reached"}
        )
        self.assertEqual(resp2.json()["id"], rule["id"])
        self.assertEqual(len((await self.client.get("/api/notifications/alerts")).json()), 1)

    async def test_daily_abs_blanket(self):
        resp = await self.client.post(
            "/api/notifications/alerts", json={"alert_type": "daily_change_abs", "threshold": 5}
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(resp.json()["scope"], "all_stocks")
        self.assertEqual(resp.json()["threshold"], 5)
        self.assertIsNone(resp.json()["stock_code"])

    async def test_daily_abs_requires_positive(self):
        resp = await self.client.post(
            "/api/notifications/alerts", json={"alert_type": "daily_change_abs", "threshold": 0}
        )
        self.assertEqual(resp.status_code, 400)

    async def test_limit_reached_blanket_singleton(self):
        resp = await self.client.post(
            "/api/notifications/alerts", json={"alert_type": "limit_reached"}
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        rule = resp.json()
        self.assertEqual(rule["scope"], "all_stocks")
        self.assertEqual(rule["threshold"], 0)
        self.assertIsNone(rule["stock_code"])
        resp2 = await self.client.post(
            "/api/notifications/alerts", json={"alert_type": "limit_reached"}
        )
        self.assertEqual(resp2.json()["id"], rule["id"])  # singleton upsert

    async def test_portfolio_daily_change(self):
        resp = await self.client.post(
            "/api/notifications/alerts",
            json={"alert_type": "daily_change_below", "threshold": -3},
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

    async def test_calendar_subscription_requires_channel_then_crud(self):
        # 채널이 없으면 구독 시도는 409.
        resp = await self.client.post(
            "/api/notifications/calendar",
            json={"event_id": "777", "event_date": "2026-06-10"},
        )
        self.assertEqual(resp.status_code, 409)

        # 채널 연결 후 구독 → GET event_ids → DELETE.
        await cache.upsert_notification_channel(
            "u1", "telegram", config={"chat_id": 1, "username": "t"}, enabled=True, verified=True
        )
        resp = await self.client.post(
            "/api/notifications/calendar",
            json={
                "event_id": "777", "event_date": "2026-06-10", "country": "us",
                "country_name": "미국", "event": "CPI", "importance": "high",
                "forecast": "3.4%", "previous": "3.6%",
            },
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual((await self.client.get("/api/notifications/calendar")).json()["event_ids"], ["777"])
        resp = await self.client.delete("/api/notifications/calendar/777")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual((await self.client.get("/api/notifications/calendar")).json()["event_ids"], [])

    async def test_calendar_subscription_missing_fields_rejected(self):
        await cache.upsert_notification_channel(
            "u1", "telegram", config={"chat_id": 1}, enabled=True, verified=True
        )
        resp = await self.client.post(
            "/api/notifications/calendar", json={"event_id": "", "event_date": ""}
        )
        self.assertEqual(resp.status_code, 400)

    async def test_calendar_status_diagnostic_reports_state(self):
        await cache.upsert_notification_channel(
            "u1", "telegram", config={"chat_id": 1}, enabled=True, verified=True
        )
        today = date.today().isoformat()
        await cache.upsert_calendar_subscription(
            "u1", "777", event_date=today, country="us", country_name="미국",
            event="CPI", importance="high", forecast="3.4%",
        )
        ev = {"index_id": "777", "actual": "3.2%", "forecast": "3.4%", "country": "us", "event": "CPI"}
        with patch.dict("os.environ", {"NOTIFY_ALERT_INTERVAL_S": "60"}), \
             patch.object(economic_calendar, "fetch_economic_calendar",
                          new=AsyncMock(return_value={"events": [ev]})):
            resp = await self.client.get("/api/notifications/calendar/status")
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertTrue(body["alert_loop_enabled"])
        self.assertTrue(body["has_active_channel"])
        self.assertEqual(body["pending_count"], 1)
        self.assertEqual(len(body["ready_to_fire_now"]), 1)
        self.assertEqual(body["ready_to_fire_now"][0]["actual"], "3.2%")


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
        # feed(공시/리포트) 모듈 전역 TTL 캐시는 테스트 간 공유되므로 초기화.
        engine._disc_cache.clear()
        engine._rep_cache.clear()

    async def asyncTearDown(self) -> None:
        await cache.close_db()
        self.db_patch.stop()
        self.temp_dir.cleanup()

    async def _rule(self, **kw):
        defaults = dict(scope="stock", alert_type="price_above", threshold=72000.0, stock_code="005930")
        defaults.update(kw)
        return await cache.create_portfolio_alert("u1", **defaults)

    async def _add_holding(self, code, name):
        db = await cache.get_db()
        await db.execute(
            "INSERT OR IGNORE INTO user_portfolio (google_sub, stock_code, stock_name, quantity, avg_price, created_at, updated_at)"
            " VALUES ('u1', ?, ?, 10, 1000, 't', 't')",
            (code, name),
        )
        await db.commit()

    async def test_no_dispatch_without_active_channel(self):
        await cache.delete_notification_channel("u1", "telegram")
        await self._rule()
        with patch.object(engine, "_safe_quote", new=AsyncMock(return_value={"price": 80000.0})), \
             patch.object(channels, "dispatch", new=AsyncMock()) as disp:
            self.assertEqual(await engine.evaluate_user("u1"), 0)
        disp.assert_not_awaited()

    async def test_price_fires_at_most_once_per_day(self):
        alert_id = await self._rule(threshold=72000.0)
        disp = AsyncMock()
        q = {"price": 80000.0}  # above
        db = await cache.get_db()

        with patch.object(engine, "_safe_quote", new=AsyncMock(side_effect=lambda code: dict(q))), \
             patch.object(channels, "dispatch", new=disp):
            self.assertEqual(await engine.evaluate_user("u1"), 1)  # fires
            self.assertEqual(disp.await_count, 1)

            self.assertEqual(await engine.evaluate_user("u1"), 0)  # still met, disarmed

            q["price"] = 60000.0  # below -> re-arm
            self.assertEqual(await engine.evaluate_user("u1"), 0)
            self.assertEqual((await cache.get_portfolio_alert("u1", alert_id))["armed"], 1)

            # 같은 날 다시 임계 돌파 -> 하루 1회 상한으로 재발송 안 함
            q["price"] = 90000.0
            self.assertEqual(await engine.evaluate_user("u1"), 0)
            self.assertEqual(disp.await_count, 1)

            # 다음 날(last_triggered_at 이 어제) -> 다시 1회 발송
            await db.execute(
                "UPDATE portfolio_alerts SET last_triggered_at = '2020-01-01T10:00:00' WHERE id = ?",
                (alert_id,),
            )
            await db.commit()
            self.assertEqual(await engine.evaluate_user("u1"), 1)
            self.assertEqual(disp.await_count, 2)

    async def test_important_rule_emphasizes_message(self):
        # 중요 규칙 발화 시 강조 헤더를 덧붙이되 원래 본문은 그대로 포함.
        await self._rule(threshold=72000.0, important=True)
        captured = {}

        async def cap(google_sub, text):
            captured["text"] = text
            return 1

        with patch.object(engine, "_safe_quote", new=AsyncMock(return_value={"price": 80000.0})), \
             patch.object(channels, "dispatch", new=cap):
            self.assertEqual(await engine.evaluate_user("u1"), 1)
        self.assertIn("중요 알림", captured["text"])
        self.assertIn("지정가 알림", captured["text"])

    async def test_normal_rule_not_emphasized(self):
        await self._rule(threshold=72000.0)  # important 기본 False
        captured = {}

        async def cap(google_sub, text):
            captured["text"] = text
            return 1

        with patch.object(engine, "_safe_quote", new=AsyncMock(return_value={"price": 80000.0})), \
             patch.object(channels, "dispatch", new=cap):
            self.assertEqual(await engine.evaluate_user("u1"), 1)
        self.assertNotIn("중요 알림", captured["text"])

    async def test_target_reached_blanket_uses_holding_target(self):
        await _set_target_price("u1", "005930", 70000)
        await self._rule(scope="all_stocks", alert_type="target_reached", threshold=0.0, stock_code=None)
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

    async def test_effective_target_prefers_live_formula(self):
        # 삼성생명류: 수식 목표가가 있으면 매입가×1.3 자동값이 아니라 라이브 수식 결과.
        item = {"stock_code": "005930", "target_price_formula": "BPS*0.5", "avg_price": 200000}
        with patch.object(engine, "resolve_formula_target", new=AsyncMock(return_value=926044.0)):
            self.assertEqual(await engine._effective_target(item, None), 926044.0)
        # 라이브 변수를 못 얻어 수식이 None 이면 매입가×1.3 자동값으로 폴백.
        with patch.object(engine, "resolve_formula_target", new=AsyncMock(return_value=None)):
            self.assertEqual(await engine._effective_target(item, None), 260000.0)

    async def test_daily_abs_blanket_once_per_day(self):
        import json as _json
        rid = await self._rule(scope="all_stocks", alert_type="daily_change_abs", threshold=5.0, stock_code=None)
        disp = AsyncMock()
        q = {"price": 80000.0, "change_pct": 7.0}  # |+7| >= 5 -> fire
        db = await cache.get_db()

        with patch.object(engine, "_safe_quote", new=AsyncMock(side_effect=lambda code: dict(q))), \
             patch.object(channels, "dispatch", new=disp):
            self.assertEqual(await engine.evaluate_user("u1"), 1)
            self.assertEqual(disp.await_count, 1)
            q["change_pct"] = 3.0  # |3| < 5 -> re-arm
            self.assertEqual(await engine.evaluate_user("u1"), 0)
            # 같은 날 다시 ±5% 돌파 -> 하루 1회 상한으로 재발송 안 함
            q["change_pct"] = -8.0
            self.assertEqual(await engine.evaluate_user("u1"), 0)
            self.assertEqual(disp.await_count, 1)
            # 다음 날(fired 가 어제) -> 다시 1회
            await db.execute(
                "UPDATE portfolio_alerts SET state_json = ? WHERE id = ?",
                (_json.dumps({"005930": {"armed": True, "fired": "2020-01-01"}}), rid),
            )
            await db.commit()
            self.assertEqual(await engine.evaluate_user("u1"), 1)
            self.assertEqual(disp.await_count, 2)

    async def test_limit_reached_blanket_once_per_day(self):
        import json as _json
        # 005930(국내). 기준가 10,000 → 상한가 13,000 / 하한가 7,000 (정확 호가단위).
        rid = await self._rule(scope="all_stocks", alert_type="limit_reached", threshold=0.0, stock_code=None)
        disp = AsyncMock()
        q = {"price": 13000.0, "previous_close": 10000.0}  # 상한가 정확 도달
        db = await cache.get_db()

        with patch.object(engine, "_safe_quote", new=AsyncMock(side_effect=lambda code: dict(q))), \
             patch.object(channels, "dispatch", new=disp):
            self.assertEqual(await engine.evaluate_user("u1"), 1)  # 상한가 발화
            q["price"] = 12990.0  # 1틱 아래 → 재무장
            self.assertEqual(await engine.evaluate_user("u1"), 0)
            # 같은 날 하한가 도달해도 하루 1회 상한 → 재발송 안 함
            q["price"] = 7000.0
            self.assertEqual(await engine.evaluate_user("u1"), 0)
            self.assertEqual(disp.await_count, 1)
            # 다음 날 → 다시 1회
            await db.execute(
                "UPDATE portfolio_alerts SET state_json = ? WHERE id = ?",
                (_json.dumps({"005930": {"armed": True, "fired": "2020-01-01"}}), rid),
            )
            await db.commit()
            self.assertEqual(await engine.evaluate_user("u1"), 1)
            self.assertEqual(disp.await_count, 2)

    async def test_limit_reached_not_fired_just_below_limit(self):
        # +29% 수준이어도 상한가(13,000) 미만이면 발화하지 않음 (근사 아님).
        await self._rule(scope="all_stocks", alert_type="limit_reached", threshold=0.0, stock_code=None)
        with patch.object(engine, "_safe_quote", new=AsyncMock(return_value={"price": 12900.0, "previous_close": 10000.0})), \
             patch.object(channels, "dispatch", new=AsyncMock()) as disp:
            self.assertEqual(await engine.evaluate_user("u1"), 0)
        disp.assert_not_awaited()

    async def test_disabled_rule_not_evaluated(self):
        await self._rule(enabled=False)
        with patch.object(engine, "_safe_quote", new=AsyncMock(return_value={"price": 80000.0})), \
             patch.object(channels, "dispatch", new=AsyncMock()) as disp:
            self.assertEqual(await engine.evaluate_user("u1"), 0)
        disp.assert_not_awaited()

    async def test_portfolio_daily_change_uses_total_value_scale(self):
        # 전일 결산: total_value 1억, nav(per-unit) 1,500. 일간등락은 total_value
        # 끼리 비교해야 함(per-unit nav 와 비교하면 수억 % 버그).
        db = await cache.get_db()
        await db.execute(
            "INSERT INTO portfolio_snapshots (google_sub, date, total_value, total_invested, nav, total_units)"
            " VALUES ('u1', '2020-01-01', 100000000, 100000000, 1500, 66666.0)"
        )
        await db.commit()
        await self._rule(scope="portfolio", alert_type="daily_change_above", threshold=1.0, stock_code=None)
        captured = {}

        async def cap(google_sub, text):
            captured["text"] = text
            return 1

        # 오늘 총평가액 1억 100만 → +1.00%
        with patch.object(engine, "_portfolio_nav", new=AsyncMock(return_value=101000000.0)), \
             patch.object(channels, "dispatch", new=cap):
            self.assertEqual(await engine.evaluate_user("u1"), 1)
        self.assertIn("+1.00%", captured["text"])
        self.assertNotIn("e+", captured["text"].lower())  # 비정상 거대값 아님

    async def test_portfolio_daily_change_excludes_cashflow(self):
        db = await cache.get_db()
        await db.execute(
            "INSERT INTO portfolio_snapshots (google_sub, date, total_value, total_invested, nav, total_units)"
            " VALUES ('u1', '2020-01-01', 100000000, 100000000, 1500, 66666.0)"
        )
        # 오늘 1천만원 입금 → 총평가액 +1천만이지만 수익률엔 포함 안 됨.
        await db.execute(
            "INSERT INTO portfolio_cashflows (google_sub, date, type, amount, created_at)"
            " VALUES ('u1', '2020-01-02', 'deposit', 10000000, '2020-01-02T09:00:00')"
        )
        await db.commit()
        await self._rule(scope="portfolio", alert_type="daily_change_above", threshold=1.0, stock_code=None)
        # 오늘 총평가액 = 1억(원금) + 1천만(입금) = 1억1천만. 현금흐름 제외 시 0% → 미발화.
        with patch.object(engine, "_portfolio_nav", new=AsyncMock(return_value=110000000.0)), \
             patch.object(channels, "dispatch", new=AsyncMock()) as disp:
            self.assertEqual(await engine.evaluate_user("u1"), 0)
        disp.assert_not_awaited()

    async def test_stock_daily_abs_fires_once_per_day(self):
        await self._rule(scope="stock", alert_type="stock_daily_abs", threshold=5.0, stock_code="005930")
        disp = AsyncMock()
        q = {"price": 80000.0, "change_pct": 7.0}  # |7| >= 5 → 발화
        with patch.object(engine, "_safe_quote", new=AsyncMock(side_effect=lambda code: dict(q))), \
             patch.object(channels, "dispatch", new=disp):
            self.assertEqual(await engine.evaluate_user("u1"), 1)
            self.assertEqual(disp.await_count, 1)
            q["change_pct"] = 3.0  # |3| < 5 → 재무장
            self.assertEqual(await engine.evaluate_user("u1"), 0)
            q["change_pct"] = -9.0  # 같은 날 재돌파 → 하루1회로 미발송
            self.assertEqual(await engine.evaluate_user("u1"), 0)
            self.assertEqual(disp.await_count, 1)

    async def test_disclosure_new_baseline_then_fire(self):
        await self._rule(scope="stock", alert_type="disclosure_new", threshold=0.0, stock_code="005930")
        disp = AsyncMock()
        box = {"v": {"rcept_no": "111", "report_nm": "주요사항보고서", "rcept_dt": "20260101"}}

        async def fake_feed(kind, code, fc):
            return dict(box["v"]) if box["v"] else None

        with patch.object(engine, "_cached_feed", new=fake_feed), \
             patch.object(channels, "dispatch", new=disp):
            self.assertEqual(await engine.evaluate_user("u1"), 0)  # 첫 틱: baseline만, 미발송
            disp.assert_not_awaited()
            box["v"] = {"rcept_no": "222", "report_nm": "단일판매ㆍ공급계약체결", "rcept_dt": "20260102"}
            self.assertEqual(await engine.evaluate_user("u1"), 1)  # 신규 → 1회
            self.assertEqual(disp.await_count, 1)
            self.assertEqual(await engine.evaluate_user("u1"), 0)  # 동일 유지 → 미발송
            self.assertEqual(disp.await_count, 1)

    async def test_fetch_latest_disclosure_skips_low_signal(self):
        # 최신이 증권발행실적보고서(저신호)면 건너뛰고 다음 실질 공시를 잡는다.
        raw = [
            {"rcept_no": "300", "report_nm": "증권발행실적보고서", "rcept_dt": "20260103", "corp_name": "삼성전자"},
            {"rcept_no": "299", "report_nm": "주요사항보고서(자기주식취득결정)", "rcept_dt": "20260102", "corp_name": "삼성전자"},
        ]
        with patch.object(cache, "get_corp_code", new=AsyncMock(return_value="00126380")), \
             patch("dart_client.fetch_recent_disclosures", new=AsyncMock(return_value=raw)):
            latest = await engine._fetch_latest_disclosure("005930")
        self.assertIsNotNone(latest)
        self.assertEqual(latest["rcept_no"], "299")

    async def test_report_new_baseline_then_fire(self):
        await self._rule(scope="stock", alert_type="report_new", threshold=0.0, stock_code="005930")
        disp = AsyncMock()
        box = {"v": {"sig": "2026-01-01|제목A|미래에셋|http://x/a.pdf", "firm": "미래에셋", "title": "제목A", "target_price": "90000", "recommendation": "매수"}}

        async def fake_feed(kind, code, fc):
            return dict(box["v"]) if box["v"] else None

        with patch.object(engine, "_cached_feed", new=fake_feed), \
             patch.object(channels, "dispatch", new=disp):
            self.assertEqual(await engine.evaluate_user("u1"), 0)  # baseline
            disp.assert_not_awaited()
            box["v"] = {"sig": "2026-01-05|제목B|한국투자|http://x/b.pdf", "firm": "한국투자", "title": "제목B", "target_price": "", "recommendation": ""}
            self.assertEqual(await engine.evaluate_user("u1"), 1)  # 신규 리포트
            self.assertEqual(disp.await_count, 1)

    async def test_evaluate_all_includes_alert_only_user(self):
        # 포트폴리오는 없고 알림 규칙만 있는 사용자(u2)도 평가 대상에 포함.
        db = await cache.get_db()
        await db.execute(
            "INSERT OR IGNORE INTO users (google_sub, email, name, picture, email_verified, created_at, last_login_at)"
            " VALUES ('u2','e2@x','U2','',1,'t','t')"
        )
        await db.commit()
        await cache.upsert_notification_channel("u2", "telegram", config={"chat_id": 1, "username": "t"}, enabled=True, verified=True)
        await cache.create_portfolio_alert("u2", scope="stock", alert_type="price_above", threshold=72000.0, stock_code="005930")
        self.assertIn("u2", await cache.get_all_users_with_alerts())
        with patch.object(engine, "_safe_quote", new=AsyncMock(return_value={"price": 80000.0})), \
             patch.object(channels, "dispatch", new=AsyncMock()) as disp:
            await engine.evaluate_all()
        self.assertGreaterEqual(disp.await_count, 1)  # u2 발화(80000 >= 72000)

    async def test_blanket_disclosure_feed_baseline_then_fire(self):
        await self._add_holding("000660", "SK하이닉스")
        await self._rule(scope="all_stocks", alert_type="disclosure_new_all", threshold=0.0, stock_code=None)
        disp = AsyncMock()
        feed = {
            "005930": {"rcept_no": "A1", "report_nm": "주요사항보고서", "rcept_dt": "20260101"},
            "000660": {"rcept_no": "B1", "report_nm": "주요사항보고서", "rcept_dt": "20260101"},
        }

        async def fake_feed(kind, code, fc):
            v = feed.get(code)
            return dict(v) if v else None

        with patch.object(engine, "_cached_feed", new=fake_feed), \
             patch.object(channels, "dispatch", new=disp):
            self.assertEqual(await engine.evaluate_user("u1"), 0)  # 첫 틱: 두 종목 baseline만
            disp.assert_not_awaited()
            feed["005930"] = {"rcept_no": "A2", "report_nm": "단일판매ㆍ공급계약체결", "rcept_dt": "20260102"}
            self.assertEqual(await engine.evaluate_user("u1"), 1)  # 005930 신규 → 1회
            self.assertEqual(disp.await_count, 1)

    async def test_blanket_feed_respects_per_stock_override(self):
        # 전체 신규공시 ON + 개별: 005930 켜짐(개별 발송), 000660 꺼짐(억제).
        # blanket 은 개별 규칙이 있는 두 종목을 모두 스킵 → 발송은 005930 개별 경로만.
        await self._add_holding("000660", "SK하이닉스")
        await self._rule(scope="all_stocks", alert_type="disclosure_new_all", threshold=0.0, stock_code=None)
        await cache.create_portfolio_alert("u1", scope="stock", alert_type="disclosure_new", threshold=0.0, stock_code="005930", enabled=True)
        await cache.create_portfolio_alert("u1", scope="stock", alert_type="disclosure_new", threshold=0.0, stock_code="000660", enabled=False)
        feed = {
            "005930": {"rcept_no": "A1", "report_nm": "주요사항보고서", "rcept_dt": "20260101"},
            "000660": {"rcept_no": "B1", "report_nm": "주요사항보고서", "rcept_dt": "20260101"},
        }
        calls = []

        async def fake_feed(kind, code, fc):
            v = feed.get(code)
            return dict(v) if v else None

        async def cap(google_sub, text):
            calls.append(text)
            return 1

        with patch.object(engine, "_cached_feed", new=fake_feed), \
             patch.object(channels, "dispatch", new=cap):
            self.assertEqual(await engine.evaluate_user("u1"), 0)  # 첫 틱: 005930 개별 baseline
            feed["005930"] = {"rcept_no": "A2", "report_nm": "단일판매ㆍ공급계약체결", "rcept_dt": "20260102"}
            feed["000660"] = {"rcept_no": "B2", "report_nm": "단일판매ㆍ공급계약체결", "rcept_dt": "20260102"}
            self.assertEqual(await engine.evaluate_user("u1"), 1)  # 005930 개별만
        self.assertTrue(any("삼성전자" in t for t in calls))
        self.assertTrue(all("SK하이닉스" not in t for t in calls))  # 000660(꺼짐) 억제


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
            config={"rest_key": "K", "access_token": "old", "access_expires_at": time.time() + 3600, "refresh_token": "r"},
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


class CalendarAlertEngineHarness(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "cache.db"
        self.db_patch = patch.object(cache, "DB_PATH", self.db_path)
        self.db_patch.start()
        await cache.close_db()
        await cache.init_db()
        await _seed_user_and_holding()
        await cache.upsert_notification_channel(
            "u1", "telegram", config={"chat_id": 1, "username": "t"}, enabled=True, verified=True
        )

    async def asyncTearDown(self) -> None:
        await cache.close_db()
        self.db_patch.stop()
        self.temp_dir.cleanup()

    def _event(self, **kw):
        ev = {
            "index_id": "777", "date": date.today().isoformat(), "time": "22:30",
            "country": "us", "country_name": "미국", "flag": "🇺🇸",
            "event": "CPI (전년비)", "importance": "high", "importance_label": "상",
            "actual": "3.2%", "forecast": "3.4%", "previous": "3.6%",
        }
        ev.update(kw)
        return ev

    async def test_fires_once_when_actual_released(self):
        today = date.today().isoformat()
        await cache.upsert_calendar_subscription(
            "u1", "777", event_date=today, country="us", country_name="미국",
            event="CPI (전년비)", importance="high", forecast="3.4%", previous="3.6%",
        )
        captured = {}

        async def cap(google_sub, text):
            captured["text"] = text
            return 1

        with patch.object(economic_calendar, "fetch_economic_calendar",
                          new=AsyncMock(return_value={"events": [self._event()]})), \
             patch.object(channels, "dispatch", new=cap):
            result = await engine.evaluate_calendar_all()
        self.assertEqual(result["sent"], 1)
        self.assertIn("결과 발표", captured["text"])
        self.assertIn("CPI", captured["text"])
        self.assertIn("예상 하회", captured["text"])  # 3.2 < 3.4

        # fired=1 이후엔 재발송 없음(엣지 트리거).
        with patch.object(economic_calendar, "fetch_economic_calendar",
                          new=AsyncMock(return_value={"events": [self._event()]})), \
             patch.object(channels, "dispatch", new=AsyncMock()) as disp:
            self.assertEqual((await engine.evaluate_calendar_all())["sent"], 0)
        disp.assert_not_awaited()

    async def test_no_fire_when_actual_still_empty(self):
        today = date.today().isoformat()
        await cache.upsert_calendar_subscription("u1", "777", event_date=today, country="us", event="X")
        with patch.object(economic_calendar, "fetch_economic_calendar",
                          new=AsyncMock(return_value={"events": [self._event(actual="")]})), \
             patch.object(channels, "dispatch", new=AsyncMock()) as disp:
            self.assertEqual((await engine.evaluate_calendar_all())["sent"], 0)
        disp.assert_not_awaited()
        # 대기 상태 유지(아직 fired 아님)
        self.assertEqual(len(await cache.list_pending_calendar_subscriptions()), 1)

    async def test_no_fire_without_active_channel(self):
        await cache.delete_notification_channel("u1", "telegram")
        today = date.today().isoformat()
        await cache.upsert_calendar_subscription("u1", "777", event_date=today, country="us", event="X")
        with patch.object(economic_calendar, "fetch_economic_calendar",
                          new=AsyncMock(return_value={"events": [self._event()]})), \
             patch.object(channels, "dispatch", new=AsyncMock()) as disp:
            self.assertEqual((await engine.evaluate_calendar_all())["sent"], 0)
        disp.assert_not_awaited()


class CalendarMessageHelperTests(unittest.TestCase):
    def test_num_parsing(self):
        self.assertEqual(engine._calendar_num("3.2%"), 3.2)
        self.assertEqual(engine._calendar_num("$24.86B"), 24.86)
        self.assertEqual(engine._calendar_num("-2K"), -2.0)
        self.assertIsNone(engine._calendar_num(""))
        self.assertIsNone(engine._calendar_num("-"))

    def test_surprise_direction(self):
        self.assertEqual(engine._calendar_surprise("3.5", "3.4"), "📈 예상 상회")
        self.assertEqual(engine._calendar_surprise("3.2", "3.4"), "📉 예상 하회")
        self.assertEqual(engine._calendar_surprise("3.4", "3.4"), "= 예상 부합")
        self.assertEqual(engine._calendar_surprise("3.4", ""), "")

    def test_message_uses_fresh_event_values(self):
        sub = {"country_name": "미국", "event": "CPI", "forecast": "9.9", "previous": "8.8"}
        ev = {
            "flag": "🇺🇸", "country_name": "미국", "event": "CPI (전년비)",
            "actual": "3.2%", "forecast": "3.4%", "previous": "3.6%",
        }
        msg = engine._format_calendar_message(sub, ev)
        self.assertIn("CPI (전년비)", msg)        # 신선한 이벤트명 우선
        self.assertIn("실제 3.2%", msg)
        self.assertIn("예상 3.4%", msg)
        self.assertIn("이전 3.6%", msg)


if __name__ == "__main__":
    unittest.main()
