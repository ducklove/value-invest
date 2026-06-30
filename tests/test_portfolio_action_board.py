"""Portfolio action board tests.

The board composes existing rebalance drift data, linked-project signals and
per-user review state into one queue.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
from _harness import TempDbMixin

import cache
from core.app_factory import create_app
from core.config import PROJECT_ROOT, AppSettings
from repositories import action_reviews, rebalance_targets, snapshots
from routes import action_board as action_board_route
from services.portfolio import action_board as action_board_service


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
    db = await cache.get_db()
    await db.execute(
        "INSERT OR IGNORE INTO users (google_sub, email, name, picture, email_verified, created_at, last_login_at)"
        " VALUES (?, 'e@x', 'U', '', 1, 't', 't')",
        (google_sub,),
    )
    await db.commit()


async def _seed_holding(google_sub, code, name, qty, group):
    db = await cache.get_db()
    await db.execute(
        "INSERT OR IGNORE INTO user_portfolio"
        " (google_sub, stock_code, stock_name, quantity, avg_price, group_name, created_at, updated_at)"
        " VALUES (?, ?, ?, ?, 1000, ?, 't', 't')",
        (google_sub, code, name, qty, group),
    )
    await db.commit()


async def _seed_portfolio_state():
    await _seed_holding("u1", "005930", "삼성전자", 10, "한국주식")
    await _seed_holding("u1", "AAPL", "애플", 2, "해외주식")
    await snapshots.save_stock_snapshots("u1", "2026-06-30", [
        {"stock_code": "005930", "market_value": 600000, "group_name": "한국주식"},
        {"stock_code": "AAPL", "market_value": 400000, "group_name": "해외주식"},
    ])


class TempDbHarness(TempDbMixin):
    async def seed(self) -> None:
        await _seed_user()


class ActionReviewRepoTests(TempDbHarness):
    async def test_review_status_roundtrip(self):
        saved = await action_reviews.set_review_status("u1", "signal:preferred:005930", "done")
        self.assertEqual(saved["status"], "done")

        saved = await action_reviews.set_review_status(
            "u1", "signal:preferred:005930", "open", note="다시 확인"
        )
        self.assertEqual(saved["status"], "open")
        self.assertEqual(saved["note"], "다시 확인")

        rows = await action_reviews.list_reviews("u1", ["signal:preferred:005930", "missing"])
        self.assertEqual(rows["signal:preferred:005930"]["status"], "open")
        self.assertNotIn("missing", rows)

    async def test_rejects_unknown_status(self):
        with self.assertRaises(ValueError):
            await action_reviews.set_review_status("u1", "x", "archived")


class ActionBoardRouteTests(TempDbHarness):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.user = {"google_sub": "u1", "email": "e@x", "name": "U"}
        self.auth_patch = patch.object(
            action_board_route, "get_current_user", new=AsyncMock(return_value=self.user)
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
        with patch.object(action_board_route, "get_current_user", new=AsyncMock(return_value=None)):
            self.assertEqual((await self.client.get("/api/portfolio/action-board")).status_code, 401)
            resp = await self.client.put(
                "/api/portfolio/action-board/queue/signal:preferred:005930",
                json={"status": "done"},
            )
            self.assertEqual(resp.status_code, 401)

    async def test_board_combines_rebalance_signal_and_review_state(self):
        await _seed_portfolio_state()
        await rebalance_targets.upsert_target("u1", "stock", "005930", 50.0, 5.0)
        signals = {
            "005930": [{
                "kind": "preferred",
                "title": "삼성전자 우선주 괴리",
                "detail": "우선주 괴리율 36.1%",
                "url": "https://example.test/spread?code=005930",
                "severity": "watch",
                "metric": 36.1,
                "short_label": "우선주",
            }]
        }

        with patch.object(action_board_service.external_tools, "fetch_portfolio_signals", new=AsyncMock(return_value=signals)):
            resp = await self.client.get("/api/portfolio/action-board")
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body["as_of"], "2026-06-30")
        self.assertEqual(body["summary"]["rebalance_breaches"], 1)
        self.assertEqual(body["summary"]["signal_count"], 1)
        self.assertEqual(body["summary"]["open_count"], 2)
        keys = [item["key"] for item in body["queue"]]
        self.assertIn("rebalance:stock:005930", keys)
        self.assertIn("signal:preferred:005930", keys)

        resp = await self.client.put(
            "/api/portfolio/action-board/queue/signal:preferred:005930",
            json={"status": "done"},
        )
        self.assertEqual(resp.status_code, 200, resp.text)

        with patch.object(action_board_service.external_tools, "fetch_portfolio_signals", new=AsyncMock(return_value=signals)):
            resp = await self.client.get("/api/portfolio/action-board")
        body = resp.json()
        by_key = {item["key"]: item for item in body["actions"]}
        self.assertEqual(by_key["signal:preferred:005930"]["status"], "done")
        self.assertEqual(body["summary"]["open_count"], 1)
        self.assertEqual([item["key"] for item in body["queue"]], ["rebalance:stock:005930"])

    async def test_review_validation_errors(self):
        resp = await self.client.put(
            "/api/portfolio/action-board/queue/signal:preferred:005930",
            json={"status": "archived"},
        )
        self.assertEqual(resp.status_code, 400)

        resp = await self.client.put(
            "/api/portfolio/action-board/queue/" + ("x" * 181),
            json={"status": "done"},
        )
        self.assertEqual(resp.status_code, 400)
