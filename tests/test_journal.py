"""투자 일지 테스트: 저장소 CRUD 왕복·소유권 스코프, 라우트 인증/검증,
작성 시점 가격·목표가 스냅샷 캡처(모킹), 작성 후 수익률 계산.

DB 는 test_rebalance 와 같은 temp-DB 패턴(repositories.db.DB_PATH 패치),
라우트는 실제 ASGI 앱(httpx.ASGITransport) 위에서 검증한다. 시세는
services.stock_quotes.get_quote_snapshot 를 AsyncMock 으로 패치 — 실제
업스트림 호출 없이 가격 스냅샷/현재가 경로를 손계산 기대값으로 대조한다.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

import cache
import repositories.db
from core.app_factory import create_app
from core.config import AppSettings, PROJECT_ROOT
from repositories import journal as journal_repo
from routes import journal as journal_route
from services import stock_quotes


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


async def _seed_holding(
    google_sub,
    code,
    name,
    *,
    target_price=None,
    target_price_disabled=0,
):
    db = await cache.get_db()
    await db.execute(
        "INSERT OR IGNORE INTO user_portfolio (google_sub, stock_code, stock_name, quantity, avg_price,"
        " target_price, target_price_disabled, created_at, updated_at)"
        " VALUES (?, ?, ?, 10, 50000, ?, ?, 't', 't')",
        (google_sub, code, name, target_price, target_price_disabled),
    )
    await db.commit()


def _quote_mock(price):
    """get_quote_snapshot 모킹 — stock_to_quote 형태의 dict 를 돌려준다."""
    return AsyncMock(return_value={"code": "X", "price": price, "previous_close": price})


class TempDbHarness(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "cache.db"
        self.db_patch = patch.object(repositories.db, "DB_PATH", self.db_path)
        self.db_patch.start()
        await cache.close_db()
        await cache.init_db()
        await _seed_user("u1")
        await _seed_user("u2")

    async def asyncTearDown(self) -> None:
        await cache.close_db()
        self.db_patch.stop()
        self.temp_dir.cleanup()


# --- 저장소 CRUD -------------------------------------------------------------

class JournalRepoTests(TempDbHarness):
    async def test_insert_list_filter_roundtrip(self):
        first = await journal_repo.insert_entry(
            "u1", "005930", "buy", "PBR 0.9 — 저평가 매수",
            stock_name="삼성전자", price_at_entry=70000.0, quantity=10,
            target_price_at_entry=100000.0,
        )
        second = await journal_repo.insert_entry("u1", "AAPL", "memo", "실적 발표 대기")
        await journal_repo.insert_entry("u2", "005930", "sell", "남의 일지")

        self.assertEqual(first["entry_type"], "buy")
        self.assertEqual(first["price_at_entry"], 70000.0)
        self.assertEqual(first["target_price_at_entry"], 100000.0)
        self.assertEqual(first["stock_name"], "삼성전자")
        self.assertIsNone(second["price_at_entry"])
        self.assertIsNone(second["quantity"])

        # 전체 목록 — 최신 작성순(id DESC tie-break), 남의 항목 비노출.
        rows = await journal_repo.list_entries("u1")
        self.assertEqual([r["id"] for r in rows], [second["id"], first["id"]])

        # 종목 필터.
        rows = await journal_repo.list_entries("u1", "005930")
        self.assertEqual([r["id"] for r in rows], [first["id"]])

    async def test_update_note_is_note_only_and_owner_scoped(self):
        entry = await journal_repo.insert_entry(
            "u1", "005930", "buy", "이전 이유", price_at_entry=70000.0)

        self.assertTrue(await journal_repo.update_note("u1", entry["id"], "수정된 이유"))
        updated = await journal_repo.get_entry("u1", entry["id"])
        self.assertEqual(updated["note"], "수정된 이유")
        # 스냅샷(가격)은 불변.
        self.assertEqual(updated["price_at_entry"], 70000.0)

        # 남의 google_sub 으로는 매칭 자체가 안 된다.
        self.assertFalse(await journal_repo.update_note("u2", entry["id"], "탈취"))
        self.assertIsNone(await journal_repo.get_entry("u2", entry["id"]))

    async def test_delete_owner_scoped(self):
        entry = await journal_repo.insert_entry("u1", "005930", "memo", "삭제 대상")
        self.assertFalse(await journal_repo.delete_entry("u2", entry["id"]))
        self.assertTrue(await journal_repo.delete_entry("u1", entry["id"]))
        self.assertFalse(await journal_repo.delete_entry("u1", entry["id"]))

    async def test_insert_rejects_unknown_entry_type(self):
        with self.assertRaises(ValueError):
            await journal_repo.insert_entry("u1", "005930", "hold", "x")

    async def test_get_holding_snapshot(self):
        await _seed_holding("u1", "005930", "삼성전자", target_price=100000.0)
        snap = await journal_repo.get_holding_snapshot("u1", "005930")
        self.assertEqual(snap["stock_name"], "삼성전자")
        self.assertEqual(snap["target_price"], 100000.0)
        self.assertEqual(snap["target_price_disabled"], 0)
        self.assertIsNone(await journal_repo.get_holding_snapshot("u1", "000660"))


# --- 라우트 (인증/검증/스냅샷 캡처/수익률) ------------------------------------

class JournalRouteTests(TempDbHarness):
    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.user = {"google_sub": "u1", "email": "e@x", "name": "U"}
        self.auth_patch = patch.object(
            journal_route, "get_current_user", new=AsyncMock(return_value=self.user)
        )
        self.auth_patch.start()
        self.app = create_app(_test_settings())
        self.transport = httpx.ASGITransport(app=self.app, raise_app_exceptions=False)
        self.client = httpx.AsyncClient(transport=self.transport, base_url="http://testserver")

    async def asyncTearDown(self) -> None:
        await self.client.aclose()
        self.auth_patch.stop()
        await super().asyncTearDown()

    async def _post(self, payload, price=70000.0):
        with patch.object(stock_quotes, "get_quote_snapshot", new=_quote_mock(price)):
            return await self.client.post("/api/portfolio/journal", json=payload)

    async def test_requires_auth(self):
        with patch.object(journal_route, "get_current_user", new=AsyncMock(return_value=None)):
            self.assertEqual((await self.client.get("/api/portfolio/journal")).status_code, 401)
            resp = await self.client.post(
                "/api/portfolio/journal",
                json={"stock_code": "005930", "entry_type": "buy", "note": "x"})
            self.assertEqual(resp.status_code, 401)
            self.assertEqual(
                (await self.client.patch("/api/portfolio/journal/1", json={"note": "x"})).status_code, 401)
            self.assertEqual(
                (await self.client.delete("/api/portfolio/journal/1")).status_code, 401)

    async def test_post_captures_price_target_and_name_snapshots(self):
        await _seed_holding("u1", "005930", "삼성전자", target_price=100000.0)
        resp = await self._post(
            {"stock_code": "005930", "entry_type": "buy", "note": "BPS 대비 저평가", "quantity": 10},
            price=70000.0,
        )
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body["entry_type"], "buy")
        self.assertEqual(body["price_at_entry"], 70000.0)        # 작성 시점 시세 스냅샷
        self.assertEqual(body["target_price_at_entry"], 100000.0)  # 저장된 목표가 스냅샷
        self.assertEqual(body["stock_name"], "삼성전자")          # 보유 행의 종목명 우선
        self.assertEqual(body["quantity"], 10.0)
        self.assertEqual(body["note"], "BPS 대비 저평가")

    async def test_post_price_capture_is_best_effort_null_on_failure(self):
        failing = AsyncMock(side_effect=RuntimeError("quote upstream down"))
        with patch.object(stock_quotes, "get_quote_snapshot", new=failing):
            resp = await self.client.post(
                "/api/portfolio/journal",
                json={"stock_code": "005930", "entry_type": "memo", "note": "시세 없이도 기록"})
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertIsNone(resp.json()["price_at_entry"])

    async def test_post_target_snapshot_respects_disabled_and_unheld(self):
        # 목표가 비활성(target_price_disabled=1) → 스냅샷 NULL.
        await _seed_holding("u1", "005930", "삼성전자",
                            target_price=100000.0, target_price_disabled=1)
        resp = await self._post({"stock_code": "005930", "entry_type": "sell", "note": "익절"})
        self.assertIsNone(resp.json()["target_price_at_entry"])

        # 미보유 종목 → 목표가 NULL, 종목명은 클라이언트 표시명 폴백.
        resp = await self._post(
            {"stock_code": "AAPL", "entry_type": "memo", "note": "관찰", "stock_name": "Apple"})
        body = resp.json()
        self.assertIsNone(body["target_price_at_entry"])
        self.assertEqual(body["stock_name"], "Apple")

    async def test_post_validation_errors(self):
        cases = [
            ({"entry_type": "buy", "note": "x"}, "stock_code 누락"),
            ({"stock_code": "00 5930", "entry_type": "buy", "note": "x"}, "code 형식"),
            ({"stock_code": "005930", "entry_type": "hold", "note": "x"}, "entry_type 오류"),
            ({"stock_code": "005930", "entry_type": "buy", "note": "  "}, "note 공백"),
            ({"stock_code": "005930", "entry_type": "buy", "note": "x" * 2001}, "note 과대"),
            ({"stock_code": "005930", "entry_type": "buy", "note": "x", "quantity": "abc"}, "수량 비숫자"),
            ({"stock_code": "005930", "entry_type": "buy", "note": "x", "quantity": 0}, "수량 0"),
        ]
        for payload, label in cases:
            resp = await self._post(payload)
            self.assertEqual(resp.status_code, 400, f"{label}: {resp.text}")

    async def test_get_list_computes_since_entry_return(self):
        # 작성 시점 70,000 → 조회 시점 77,000 = +10%.
        await self._post(
            {"stock_code": "005930", "entry_type": "buy", "note": "저평가", "stock_name": "삼성전자"},
            price=70000.0,
        )
        with patch.object(stock_quotes, "get_quote_snapshot", new=_quote_mock(77000.0)):
            resp = await self.client.get("/api/portfolio/journal")
        self.assertEqual(resp.status_code, 200, resp.text)
        entries = resp.json()["entries"]
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["price_at_entry"], 70000.0)
        self.assertEqual(entries[0]["current_price"], 77000.0)
        self.assertEqual(entries[0]["since_entry_return_pct"], 10.0)

    async def test_get_list_handles_missing_prices_and_stock_filter(self):
        # 가격 스냅샷이 없는 항목 + 현재가 조회 실패 → 수익률 NULL.
        await journal_repo.insert_entry("u1", "005930", "memo", "가격 없음")
        await journal_repo.insert_entry("u1", "AAPL", "buy", "분할 매수", price_at_entry=180.0)
        failing = AsyncMock(side_effect=RuntimeError("down"))
        with patch.object(stock_quotes, "get_quote_snapshot", new=failing):
            resp = await self.client.get("/api/portfolio/journal")
        entries = resp.json()["entries"]
        self.assertEqual(len(entries), 2)
        for entry in entries:
            self.assertIsNone(entry["current_price"])
            self.assertIsNone(entry["since_entry_return_pct"])

        # stock_code 필터.
        with patch.object(stock_quotes, "get_quote_snapshot", new=_quote_mock(200.0)):
            resp = await self.client.get("/api/portfolio/journal", params={"stock_code": "AAPL"})
        entries = resp.json()["entries"]
        self.assertEqual([e["stock_code"] for e in entries], ["AAPL"])
        self.assertAlmostEqual(entries[0]["since_entry_return_pct"], 11.11)

    async def test_patch_note_roundtrip_and_validation(self):
        created = (await self._post(
            {"stock_code": "005930", "entry_type": "buy", "note": "이전 이유"})).json()

        resp = await self.client.patch(
            f"/api/portfolio/journal/{created['id']}", json={"note": "수정된 이유"})
        self.assertEqual(resp.status_code, 200, resp.text)
        body = resp.json()
        self.assertEqual(body["note"], "수정된 이유")
        self.assertEqual(body["price_at_entry"], 70000.0)  # 스냅샷 불변

        resp = await self.client.patch(
            f"/api/portfolio/journal/{created['id']}", json={"note": "  "})
        self.assertEqual(resp.status_code, 400)

    async def test_patch_and_delete_other_users_entry_is_404(self):
        other = await journal_repo.insert_entry("u2", "005930", "buy", "남의 기록")
        resp = await self.client.patch(
            f"/api/portfolio/journal/{other['id']}", json={"note": "탈취"})
        self.assertEqual(resp.status_code, 404)
        resp = await self.client.delete(f"/api/portfolio/journal/{other['id']}")
        self.assertEqual(resp.status_code, 404)
        # 원본은 그대로.
        kept = await journal_repo.get_entry("u2", other["id"])
        self.assertEqual(kept["note"], "남의 기록")

    async def test_delete_roundtrip_and_missing_404(self):
        created = (await self._post(
            {"stock_code": "005930", "entry_type": "sell", "note": "리스크 축소"})).json()
        resp = await self.client.delete(f"/api/portfolio/journal/{created['id']}")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"ok": True})
        self.assertEqual(await journal_repo.list_entries("u1"), [])

        resp = await self.client.delete(f"/api/portfolio/journal/{created['id']}")
        self.assertEqual(resp.status_code, 404)


if __name__ == "__main__":
    unittest.main()
