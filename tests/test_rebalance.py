"""리밸런싱 도우미 테스트: 저장소 CRUD 왕복, 드리프트 순수 계산(손계산 대조),
허용 오차 경계, 라우트 인증/검증, 알림 엔진 rebalance_drift 엣지 트리거.

DB 는 test_notifications 와 같은 temp-DB 패턴(repositories.db.DB_PATH 패치),
라우트는 실제 ASGI 앱(httpx.ASGITransport) 위에서 검증한다.
"""

from __future__ import annotations

import json
import unittest
from unittest.mock import AsyncMock, patch

import httpx
from _harness import TempDbMixin

from core.app_factory import create_app
from core.config import PROJECT_ROOT, AppSettings
from repositories import db as db_repo
from repositories import notifications as notifications_repo
from repositories import rebalance_targets as targets_repo
from repositories import snapshots as snapshots_repo
from routes import rebalance as rebalance_route
from services.notifications import channels, engine
from services.portfolio import rebalance as rebalance_service


def _test_settings() -> AppSettings:
    return AppSettings(
        environment="development",
        project_root=PROJECT_ROOT,
        app_title="Test Compass",
        public_api_base_url="https://api.example.test",
        cors_allowed_origins=("https://app.example.test",),
        enable_docs=False,
    )


async def _seed_user(google_sub="u1"):
    db = await db_repo.get_db()
    await db.execute(
        "INSERT OR IGNORE INTO users (google_sub, email, name, picture, email_verified, created_at, last_login_at)"
        " VALUES (?, 'e@x', 'U', '', 1, 't', 't')",
        (google_sub,),
    )
    await db.commit()


async def _seed_holding(google_sub, code, name, qty, group):
    db = await db_repo.get_db()
    await db.execute(
        "INSERT OR IGNORE INTO user_portfolio (google_sub, stock_code, stock_name, quantity, avg_price, group_name, created_at, updated_at)"
        " VALUES (?, ?, ?, ?, 1000, ?, 't', 't')",
        (google_sub, code, name, qty, group),
    )
    await db.commit()


async def _seed_snapshot_portfolio(google_sub="u1", date="2026-06-09"):
    """손계산 기준 포트폴리오: 총 1,000,000원.

    * 005930 삼성전자  600,000 (60%)  qty=10 → 주당 60,000 / 한국주식
    * AAPL            300,000 (30%)  qty=2                / 해외주식
    * CASH_KRW        100,000 (10%)  qty=100000           / 기타
    """
    await _seed_holding(google_sub, "005930", "삼성전자", 10, "한국주식")
    await _seed_holding(google_sub, "AAPL", "애플", 2, "해외주식")
    await _seed_holding(google_sub, "CASH_KRW", "원화", 100000, "기타")
    await snapshots_repo.save_stock_snapshots(google_sub, date, [
        {"stock_code": "005930", "market_value": 600000, "group_name": "한국주식"},
        {"stock_code": "AAPL", "market_value": 300000, "group_name": "해외주식"},
        {"stock_code": "CASH_KRW", "market_value": 100000, "group_name": "기타"},
    ])


class TempDbHarness(TempDbMixin):
    async def seed(self) -> None:
        await _seed_user()


# --- 저장소 CRUD -------------------------------------------------------------

class RebalanceTargetsRepoTests(TempDbHarness):
    async def test_upsert_get_list_delete_roundtrip(self):
        await targets_repo.upsert_target("u1", "stock", "005930", 50.0, 5.0)
        await targets_repo.upsert_target("u1", "group", "해외주식", 40.0, 10.0)

        got = await targets_repo.get_target("u1", "stock", "005930")
        self.assertEqual(got["target_weight_pct"], 50.0)
        self.assertEqual(got["tolerance_pct"], 5.0)

        rows = await targets_repo.list_targets("u1")
        self.assertEqual([(r["scope"], r["target_key"]) for r in rows],
                         [("group", "해외주식"), ("stock", "005930")])

        self.assertTrue(await targets_repo.delete_target("u1", "stock", "005930"))
        self.assertFalse(await targets_repo.delete_target("u1", "stock", "005930"))
        self.assertIsNone(await targets_repo.get_target("u1", "stock", "005930"))

    async def test_upsert_same_key_updates_not_duplicates(self):
        await targets_repo.upsert_target("u1", "stock", "005930", 50.0, 5.0)
        await targets_repo.upsert_target("u1", "stock", "005930", 35.0, 3.0)
        rows = await targets_repo.list_targets("u1")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["target_weight_pct"], 35.0)
        self.assertEqual(rows[0]["tolerance_pct"], 3.0)

    async def test_replace_all_targets(self):
        await targets_repo.upsert_target("u1", "stock", "005930", 50.0, 5.0)
        saved = await targets_repo.replace_all_targets("u1", [
            {"scope": "group", "key": "한국주식", "target_weight_pct": 60.0, "tolerance_pct": 4.0},
            {"scope": "stock", "key": "AAPL", "target_weight_pct": 20.0},  # tolerance 기본값
        ])
        self.assertEqual(len(saved), 2)
        by_key = {r["target_key"]: r for r in saved}
        self.assertNotIn("005930", by_key)  # 교체 — 기존 행 제거
        self.assertEqual(by_key["AAPL"]["tolerance_pct"], targets_repo.DEFAULT_TOLERANCE_PCT)

        # 빈 목록 교체 = 전부 삭제
        self.assertEqual(await targets_repo.replace_all_targets("u1", []), [])
        self.assertEqual(await targets_repo.list_targets("u1"), [])

    async def test_targets_are_per_user(self):
        await _seed_user("u2")
        await targets_repo.upsert_target("u1", "stock", "005930", 50.0)
        await targets_repo.upsert_target("u2", "stock", "005930", 30.0)
        self.assertEqual((await targets_repo.get_target("u1", "stock", "005930"))["target_weight_pct"], 50.0)
        self.assertEqual((await targets_repo.get_target("u2", "stock", "005930"))["target_weight_pct"], 30.0)


# --- 순수 드리프트 계산 (손계산 대조) ----------------------------------------

_HOLDINGS = [
    {"stock_code": "005930", "stock_name": "삼성전자", "group_name": "한국주식",
     "market_value": 600000.0, "quantity": 10},
    {"stock_code": "AAPL", "stock_name": "애플", "group_name": "해외주식",
     "market_value": 300000.0, "quantity": 2},
    {"stock_code": "CASH_KRW", "stock_name": "원화", "group_name": "기타",
     "market_value": 100000.0, "quantity": 100000},
]


def _target(scope, key, pct, tol=5.0):
    return {"scope": scope, "target_key": key, "target_weight_pct": pct, "tolerance_pct": tol}


class DriftMathTests(unittest.TestCase):
    def test_stock_overweight_sell_suggestion(self):
        # 현재 60% / 목표 50% → 드리프트 +10%p, 매도 100,000원 ≈ 2주(주당 60,000)
        report = rebalance_service.compute_drift_report(
            [_target("stock", "005930", 50.0, 5.0)], _HOLDINGS, as_of="2026-06-09")
        self.assertEqual(report["total_value"], 1000000.0)
        self.assertEqual(report["as_of"], "2026-06-09")
        item = report["items"][0]
        self.assertEqual(item["label"], "삼성전자")
        self.assertAlmostEqual(item["current_weight_pct"], 60.0)
        self.assertAlmostEqual(item["drift_pct"], 10.0)
        self.assertTrue(item["breached"])
        self.assertEqual(item["action"], "매도")
        self.assertEqual(item["action_amount"], 100000)
        self.assertEqual(item["approx_price"], 60000.0)
        self.assertEqual(item["approx_shares"], 2)  # round(100000/60000)
        self.assertEqual(report["breached_count"], 1)

    def test_group_underweight_buy_suggestion_no_shares(self):
        # 해외주식 30% / 목표 40% → −10%p, 매수 100,000원 (그룹이라 주식 수 없음)
        report = rebalance_service.compute_drift_report(
            [_target("group", "해외주식", 40.0, 5.0)], _HOLDINGS)
        item = report["items"][0]
        self.assertAlmostEqual(item["current_weight_pct"], 30.0)
        self.assertAlmostEqual(item["drift_pct"], -10.0)
        self.assertTrue(item["breached"])
        self.assertEqual(item["action"], "매수")
        self.assertEqual(item["action_amount"], 100000)
        self.assertIsNone(item["approx_shares"])
        self.assertIsNone(item["approx_price"])

    def test_tolerance_edge_is_not_breached(self):
        # |드리프트| == tolerance (정확히 10%p) → 경계값은 미돌파
        report = rebalance_service.compute_drift_report(
            [_target("group", "해외주식", 40.0, 10.0)], _HOLDINGS)
        item = report["items"][0]
        self.assertAlmostEqual(item["drift_pct"], -10.0)
        self.assertFalse(item["breached"])
        # 조정 제안 자체는 여전히 제공
        self.assertEqual(item["action"], "매수")

    def test_zero_drift_has_no_action(self):
        report = rebalance_service.compute_drift_report(
            [_target("group", "기타", 10.0, 5.0)], _HOLDINGS)
        item = report["items"][0]
        self.assertAlmostEqual(item["drift_pct"], 0.0)
        self.assertFalse(item["breached"])
        self.assertIsNone(item["action"])
        self.assertIsNone(item["action_amount"])

    def test_unheld_stock_target_suggests_full_buy(self):
        report = rebalance_service.compute_drift_report(
            [_target("stock", "000660", 10.0, 5.0)], _HOLDINGS)
        item = report["items"][0]
        self.assertAlmostEqual(item["current_weight_pct"], 0.0)
        self.assertAlmostEqual(item["drift_pct"], -10.0)
        self.assertTrue(item["breached"])
        self.assertEqual(item["action"], "매수")
        self.assertEqual(item["action_amount"], 100000)
        self.assertIsNone(item["approx_shares"])  # 미보유 → 근사가 없음

    def test_empty_holdings_returns_targets_without_weights(self):
        report = rebalance_service.compute_drift_report(
            [_target("stock", "005930", 50.0)], [])
        self.assertEqual(report["total_value"], 0)
        item = report["items"][0]
        self.assertIsNone(item["current_weight_pct"])
        self.assertIsNone(item["drift_pct"])
        self.assertFalse(item["breached"])
        self.assertEqual(report["breached_count"], 0)


# --- 라우트 (인증/검증/왕복) --------------------------------------------------

class RebalanceRouteTests(TempDbHarness):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.user = {"google_sub": "u1", "email": "e@x", "name": "U"}
        self.auth_patch = patch.object(
            rebalance_route, "get_current_user", new=AsyncMock(return_value=self.user)
        )
        self.auth_patch.start()
        self.app = create_app(_test_settings())
        self.transport = httpx.ASGITransport(app=self.app, raise_app_exceptions=False)
        self.client = httpx.AsyncClient(transport=self.transport, base_url="http://testserver")

    async def asyncTearDown(self) -> None:
        await self.client.aclose()
        self.auth_patch.stop()
        await super().asyncTearDown()

    async def test_requires_auth(self):
        with patch.object(rebalance_route, "get_current_user", new=AsyncMock(return_value=None)):
            self.assertEqual((await self.client.get("/api/portfolio/rebalance")).status_code, 401)
            resp = await self.client.put("/api/portfolio/rebalance/targets", json={"targets": []})
            self.assertEqual(resp.status_code, 401)
            resp = await self.client.delete(
                "/api/portfolio/rebalance/targets", params={"scope": "stock", "key": "005930"})
            self.assertEqual(resp.status_code, 401)

    async def test_put_then_get_report(self):
        await _seed_snapshot_portfolio()
        resp = await self.client.put("/api/portfolio/rebalance/targets", json={
            "targets": [
                {"scope": "stock", "key": "005930", "target_weight_pct": 50, "tolerance_pct": 5},
                {"scope": "group", "key": "해외주식", "target_weight_pct": 40},
            ]
        })
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(len(resp.json()["targets"]), 2)

        resp = await self.client.get("/api/portfolio/rebalance")
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body["as_of"], "2026-06-09")
        self.assertEqual(body["total_value"], 1000000.0)
        by_key = {i["key"]: i for i in body["items"]}
        self.assertAlmostEqual(by_key["005930"]["drift_pct"], 10.0)
        self.assertTrue(by_key["005930"]["breached"])
        self.assertAlmostEqual(by_key["해외주식"]["current_weight_pct"], 30.0)
        self.assertEqual(by_key["해외주식"]["tolerance_pct"], targets_repo.DEFAULT_TOLERANCE_PCT)

    async def test_get_report_without_targets_or_snapshots(self):
        resp = await self.client.get("/api/portfolio/rebalance")
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(body["items"], [])
        self.assertIsNone(body["as_of"])

    async def test_put_validation_errors(self):
        cases = [
            ({}, "targets 누락"),
            ({"targets": "x"}, "targets 비배열"),
            ({"targets": [{"scope": "sector", "key": "x", "target_weight_pct": 10}]}, "scope 오류"),
            ({"targets": [{"scope": "stock", "key": "", "target_weight_pct": 10}]}, "key 누락"),
            ({"targets": [{"scope": "stock", "key": "005930", "target_weight_pct": "abc"}]}, "비중 비숫자"),
            ({"targets": [{"scope": "stock", "key": "005930", "target_weight_pct": 0}]}, "비중 0"),
            ({"targets": [{"scope": "stock", "key": "005930", "target_weight_pct": 120}]}, "비중 >100"),
            ({"targets": [{"scope": "stock", "key": "005930", "target_weight_pct": 10, "tolerance_pct": 0}]}, "오차 0"),
            ({"targets": [
                {"scope": "stock", "key": "005930", "target_weight_pct": 10},
                {"scope": "stock", "key": "005930", "target_weight_pct": 20},
            ]}, "중복 키"),
            ({"targets": [
                {"scope": "group", "key": "한국주식", "target_weight_pct": 70},
                {"scope": "group", "key": "해외주식", "target_weight_pct": 40},
            ]}, "scope 합 >100"),
        ]
        for payload, label in cases:
            resp = await self.client.put("/api/portfolio/rebalance/targets", json=payload)
            self.assertEqual(resp.status_code, 400, f"{label}: {resp.text}")
        # 잘못된 페이로드는 아무것도 저장하지 않는다
        self.assertEqual(await targets_repo.list_targets("u1"), [])

    async def test_scope_sums_are_independent(self):
        # 종목 합 100% + 그룹 합 100% 은 서로 다른 축이므로 허용
        resp = await self.client.put("/api/portfolio/rebalance/targets", json={
            "targets": [
                {"scope": "stock", "key": "005930", "target_weight_pct": 100},
                {"scope": "group", "key": "한국주식", "target_weight_pct": 100},
            ]
        })
        self.assertEqual(resp.status_code, 200, resp.text)

    async def test_delete_target(self):
        await targets_repo.upsert_target("u1", "group", "해외주식", 40.0)
        resp = await self.client.delete(
            "/api/portfolio/rebalance/targets", params={"scope": "group", "key": "해외주식"})
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(await targets_repo.list_targets("u1"), [])
        resp = await self.client.delete(
            "/api/portfolio/rebalance/targets", params={"scope": "group", "key": "해외주식"})
        self.assertEqual(resp.status_code, 404)
        resp = await self.client.delete(
            "/api/portfolio/rebalance/targets", params={"scope": "bad", "key": "x"})
        self.assertEqual(resp.status_code, 400)


# --- 알림 엔진 (rebalance_drift 엣지 트리거) ----------------------------------

class RebalanceAlertEngineTests(TempDbHarness):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        await _seed_snapshot_portfolio()
        await notifications_repo.upsert_notification_channel(
            "u1", "telegram", config={"chat_id": 123, "username": "t"}, enabled=True, verified=True
        )

    async def _rule(self, **kw):
        defaults = dict(scope="portfolio", alert_type="rebalance_drift", threshold=0.0, stock_code=None)
        defaults.update(kw)
        return await notifications_repo.create_portfolio_alert("u1", **defaults)

    async def test_fires_once_and_rearms_within_tolerance(self):
        # 005930: 현재 60% / 목표 50% / 오차 5%p → 돌파
        await targets_repo.upsert_target("u1", "stock", "005930", 50.0, 5.0)
        alert_id = await self._rule()
        captured = []

        async def cap(google_sub, text):
            captured.append(text)
            return 1

        with patch.object(channels, "dispatch", new=cap):
            self.assertEqual(await engine.evaluate_user("u1"), 1)  # 발화
            self.assertIn("리밸런싱 알림", captured[0])
            self.assertIn("삼성전자 60.0% (목표 50%, +10.0%p)", captured[0])

            self.assertEqual(await engine.evaluate_user("u1"), 0)  # 여전히 돌파, disarm 상태

            # 목표를 58%로 → 드리프트 +2%p (오차 내) → re-arm
            await targets_repo.upsert_target("u1", "stock", "005930", 58.0, 5.0)
            self.assertEqual(await engine.evaluate_user("u1"), 0)
            rule = await notifications_repo.get_portfolio_alert("u1", alert_id)
            state = json.loads(rule["state_json"])
            self.assertTrue(state["stock:005930"]["armed"])

            # 같은 날 다시 돌파 → 하루 1회 상한으로 미발송
            await targets_repo.upsert_target("u1", "stock", "005930", 50.0, 5.0)
            self.assertEqual(await engine.evaluate_user("u1"), 0)
            self.assertEqual(len(captured), 1)

            # 다음 날(fired 가 어제) → 다시 1회 발화
            db = await db_repo.get_db()
            await db.execute(
                "UPDATE portfolio_alerts SET state_json = ? WHERE id = ?",
                (json.dumps({"stock:005930": {"armed": True, "fired": "2020-01-01"}}), alert_id),
            )
            await db.commit()
            self.assertEqual(await engine.evaluate_user("u1"), 1)
            self.assertEqual(len(captured), 2)

    async def test_group_breach_message_has_group_suffix(self):
        # 해외주식 그룹: 현재 30% / 목표 40% / 오차 5%p → −10%p 돌파
        await targets_repo.upsert_target("u1", "group", "해외주식", 40.0, 5.0)
        await self._rule()
        captured = []

        async def cap(google_sub, text):
            captured.append(text)
            return 1

        with patch.object(channels, "dispatch", new=cap):
            self.assertEqual(await engine.evaluate_user("u1"), 1)
        self.assertIn("해외주식 그룹 30.0% (목표 40%, -10.0%p)", captured[0])

    async def test_within_tolerance_does_not_fire(self):
        await targets_repo.upsert_target("u1", "stock", "005930", 58.0, 5.0)  # +2%p
        await self._rule()
        with patch.object(channels, "dispatch", new=AsyncMock()) as disp:
            self.assertEqual(await engine.evaluate_user("u1"), 0)
        disp.assert_not_awaited()

    async def test_multiple_breaches_one_message(self):
        await targets_repo.upsert_target("u1", "stock", "005930", 50.0, 5.0)   # +10%p
        await targets_repo.upsert_target("u1", "group", "해외주식", 45.0, 5.0)  # −15%p
        await self._rule()
        captured = []

        async def cap(google_sub, text):
            captured.append(text)
            return 1

        with patch.object(channels, "dispatch", new=cap):
            self.assertEqual(await engine.evaluate_user("u1"), 1)  # 묶음 1건
        self.assertEqual(len(captured), 1)
        self.assertIn("삼성전자", captured[0])
        self.assertIn("해외주식 그룹", captured[0])

    async def test_important_rule_emphasized(self):
        await targets_repo.upsert_target("u1", "stock", "005930", 50.0, 5.0)
        await self._rule(important=True)
        captured = []

        async def cap(google_sub, text):
            captured.append(text)
            return 1

        with patch.object(channels, "dispatch", new=cap):
            self.assertEqual(await engine.evaluate_user("u1"), 1)
        self.assertIn("중요 알림", captured[0])

    async def test_no_targets_no_dispatch(self):
        await self._rule()
        with patch.object(channels, "dispatch", new=AsyncMock()) as disp:
            self.assertEqual(await engine.evaluate_user("u1"), 0)
        disp.assert_not_awaited()


class RebalanceAlertCrudTests(TempDbHarness):
    """알림 규칙 CRUD 가 rebalance_drift 를 받아들이는지 (scope/임계값/singleton)."""

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        from routes import notifications as notif_route
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
        await super().asyncTearDown()

    async def test_create_rebalance_rule_is_portfolio_singleton(self):
        resp = await self.client.post(
            "/api/notifications/alerts", json={"alert_type": "rebalance_drift"})
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body["scope"], "portfolio")
        self.assertEqual(body["threshold"], 0.0)
        self.assertIsNone(body["stock_code"])

        # 재생성은 기존 규칙 갱신 (사용자당 singleton)
        resp2 = await self.client.post(
            "/api/notifications/alerts", json={"alert_type": "rebalance_drift", "important": True})
        self.assertEqual(resp2.status_code, 200, resp2.text)
        self.assertEqual(resp2.json()["id"], body["id"])
        rules = await notifications_repo.list_portfolio_alerts("u1")
        self.assertEqual(len(rules), 1)


if __name__ == "__main__":
    unittest.main()
