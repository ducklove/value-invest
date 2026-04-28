"""Tests for /api/analysis/{code}/wiki and .../ask routes."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException
from starlette.requests import Request

import cache
from routes import wiki as wiki_route


class WikiListRouteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "cache.db"
        self.db_patch = patch.object(cache, "DB_PATH", self.db_path)
        self.db_patch.start()
        await cache.close_db()
        await cache.init_db()

    async def asyncTearDown(self):
        await cache.close_db()
        self.db_patch.stop()
        self.temp_dir.cleanup()

    async def _seed(self, stock_code: str, n: int = 3):
        for i in range(n):
            await cache.save_wiki_entry({
                "stock_code": stock_code,
                "source_type": "broker_report",
                "source_ref": f"sha-{i}",
                "report_date": f"2026-04-{10+i:02d}",
                "firm": f"증권{i}",
                "title": f"리포트 {i}",
                "recommendation": "Buy",
                "target_price": 90000.0 + i,
                "summary_md": f"summary {i}",
                "key_points_md": f"- point {i}",
                "model": "m",
                "tokens_in": 1,
                "tokens_out": 1,
                "created_at": f"2026-04-{10+i:02d}T00:00:00",
            })

    async def test_empty_wiki(self):
        resp = await wiki_route.get_stock_wiki("005930", limit=10)
        self.assertEqual(resp["count"], 0)
        self.assertEqual(resp["entries"], [])

    async def test_returns_entries_recent_first(self):
        await self._seed("005930", n=3)
        resp = await wiki_route.get_stock_wiki("005930", limit=10)
        self.assertEqual(resp["count"], 3)
        # Newest first.
        dates = [e["report_date"] for e in resp["entries"]]
        self.assertEqual(dates, sorted(dates, reverse=True))

    async def test_limit_enforced(self):
        await self._seed("005930", n=5)
        resp = await wiki_route.get_stock_wiki("005930", limit=2)
        self.assertEqual(resp["count"], 2)
        self.assertEqual(len(resp["entries"]), 2)

    async def test_scoped_to_stock(self):
        await self._seed("005930", n=2)
        await self._seed("000660", n=1)
        resp = await wiki_route.get_stock_wiki("005930", limit=10)
        for e in resp["entries"]:
            self.assertEqual(e["stock_code"], "005930")
        resp2 = await wiki_route.get_stock_wiki("000660", limit=10)
        self.assertEqual(resp2["count"], 1)

    async def test_stats_endpoint_aggregates(self):
        # Empty state.
        stats = await wiki_route.get_wiki_stats()
        self.assertEqual(stats["stocks_covered"], 0)
        self.assertEqual(stats["total_entries"], 0)
        # Seed two stocks.
        await self._seed("005930", n=3)
        await self._seed("000660", n=2)
        stats = await wiki_route.get_wiki_stats()
        self.assertEqual(stats["stocks_covered"], 2)
        self.assertEqual(stats["total_entries"], 5)


def _mk_request() -> Request:
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/analysis/005930/ask",
        "headers": [],
        "query_string": b"",
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
        "scheme": "http",
    }
    return Request(scope)


class WikiAskRouteTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "cache.db"
        self.db_patch = patch.object(cache, "DB_PATH", self.db_path)
        self.db_patch.start()
        await cache.close_db()
        await cache.init_db()
        # Seed a user; Q&A gate needs authenticated.
        db = await cache.get_db()
        await db.execute(
            "INSERT INTO users (google_sub, email, name, picture, email_verified, created_at, last_login_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("u1", "u1@e.com", "U", "", 1, "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
        )
        await db.commit()

    async def asyncTearDown(self):
        await cache.close_db()
        self.db_patch.stop()
        self.temp_dir.cleanup()

    async def test_ask_requires_login(self):
        with patch("routes.wiki.get_current_user", new=AsyncMock(return_value=None)):
            with self.assertRaises(HTTPException) as exc_info:
                await wiki_route.ask_stock("005930", _mk_request(), {"question": "hi"})
        self.assertEqual(exc_info.exception.status_code, 401)

    async def test_ask_rejects_empty_question(self):
        user = {"google_sub": "u1", "is_admin": False}
        with patch("routes.wiki.get_current_user", new=AsyncMock(return_value=user)):
            with self.assertRaises(HTTPException) as exc_info:
                await wiki_route.ask_stock("005930", _mk_request(), {"question": "   "})
        self.assertEqual(exc_info.exception.status_code, 400)

    async def test_ask_enforces_daily_rate_limit(self):
        user = {"google_sub": "u1", "is_admin": False}
        # Pre-seed 20 Q&A rows from today to hit the default cap.
        for i in range(20):
            await cache.save_qa_entry({
                "google_sub": "u1", "stock_code": "005930",
                "question": f"q{i}", "answer_md": "a",
                "source_ids": "[]", "model": "m",
                "tokens_in": 1, "tokens_out": 1, "cost_usd": 0.0,
                "created_at": "2099-01-01T10:00:00",  # anchor to a future-today
            })
        # Monkeypatch _today_kst_iso so the existing rows count as "today".
        with patch("routes.wiki.get_current_user", new=AsyncMock(return_value=user)), \
             patch.object(wiki_route, "_today_kst_iso", return_value="2099-01-01T00:00:00"):
            with self.assertRaises(HTTPException) as exc_info:
                await wiki_route.ask_stock("005930", _mk_request(), {"question": "hi"})
        self.assertEqual(exc_info.exception.status_code, 429)

    def test_build_qa_context_formats_entries(self):
        entries = [
            {
                "id": 1, "firm": "삼성증권", "report_date": "2026-03-10",
                "recommendation": "Buy", "target_price": 90000,
                "title": "HBM 수혜", "key_points_md": "- HBM 출하 증가\n- 업황 반등",
            },
        ]
        block = wiki_route._build_qa_context(entries)
        self.assertIn("2026-03-10", block)
        self.assertIn("삼성증권", block)
        self.assertIn("HBM 수혜", block)
        self.assertIn("Buy", block)
        self.assertIn("90,000", block)

    def test_fmt_krw_scales_by_magnitude(self):
        f = wiki_route._fmt_krw
        self.assertEqual(f(None), "N/A")
        self.assertEqual(f(""), "N/A")
        self.assertEqual(f(5_000), "5,000")
        self.assertTrue(f(1_230_000_000).endswith("억"))
        self.assertTrue(f(1_230_000_000_000).endswith("조"))

    def test_yoy_pct_handles_edges(self):
        y = wiki_route._yoy_pct
        # Not enough rows.
        self.assertEqual(y([{"revenue": 100}], "revenue"), "")
        # Normal growth.
        rows = [{"revenue": 110}, {"revenue": 100}]
        self.assertIn("YoY +10.0%", y(rows, "revenue"))
        # Missing prior year.
        self.assertEqual(y([{"revenue": 100}, {"revenue": None}], "revenue"), "")
        # Division by zero guard.
        self.assertEqual(y([{"revenue": 100}, {"revenue": 0}], "revenue"), "")

    def test_classify_question_routes(self):
        c = wiki_route._classify_question
        # Simple short question → shallow
        p = c("PER 몇이야?")
        self.assertFalse(p["is_deep"])
        self.assertEqual(p["wiki_limit"], 3)
        self.assertFalse(p["include_macro"])
        self.assertFalse(p["include_news"])
        # Deep keyword → deep
        p = c("이 종목 밸류에이션 어떻게 봐?")
        self.assertTrue(p["is_deep"])
        self.assertGreater(p["wiki_limit"], 3)
        self.assertTrue(p["include_news"])
        self.assertTrue(p["include_macro"])
        # Long question → deep regardless of keywords
        p = c("이 종목의 최근 3년간 실적 추이와 경쟁사 대비 상황, 그리고 향후 성장 드라이버와 리스크 요인을 종합적으로 정리해줘")
        self.assertTrue(p["is_deep"])
        # News keyword → news on but macro off (shallow otherwise)
        p = c("최근 뉴스 있어?")
        self.assertTrue(p["include_news"])
        # Macro keyword only
        p = c("환율 영향 어떨까?")
        self.assertTrue(p["include_macro"])

    def test_qa_payload_disables_reasoning_for_final_answer(self):
        payload = wiki_route._qa_chat_payload("moonshotai/kimi-k2.6", "prompt", stream=True, max_tokens=1500)

        self.assertEqual(payload["model"], "moonshotai/kimi-k2.6")
        self.assertTrue(payload["stream"])
        self.assertEqual(payload["reasoning"], {"effort": "none", "exclude": True})
        self.assertFalse(payload["include_reasoning"])

    def test_extract_final_content_handles_openai_text_parts(self):
        data = {
            "choices": [
                {"message": {"content": [{"type": "text", "text": "hello"}, {"type": "text", "text": " world"}]}}
            ]
        }

        self.assertEqual(wiki_route._extract_final_content(data), "hello world")


class ShortcutTests(unittest.IsolatedAsyncioTestCase):
    """Exercise Tier-0 rule-based shortcuts directly — no HTTP stack."""

    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "cache.db"
        self.db_patch = patch.object(cache, "DB_PATH", self.db_path)
        self.db_patch.start()
        await cache.close_db()
        await cache.init_db()
        db = await cache.get_db()
        await db.execute(
            "INSERT INTO corp_codes (stock_code, corp_code, corp_name, updated_at) VALUES (?, ?, ?, ?)",
            ("005930", "00126380", "삼성전자", "2026-01-01"),
        )
        await db.execute(
            """INSERT INTO market_data (stock_code, year, close_price, per, pbr, eps, bps,
                dividend_per_share, dividend_yield, market_cap)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("005930", 2025, 75000, 13.5, 1.25, 5500, 60000, 361.0, 0.48, 460_000_000_000_000),
        )
        await db.commit()

    async def asyncTearDown(self):
        await cache.close_db()
        self.db_patch.stop()
        self.temp_dir.cleanup()

    async def test_shortcut_per_question(self):
        # _fetch_quote returns empty dict via mock so shortcut relies on
        # market_data only.
        with patch("routes.portfolio._fetch_quote", new=AsyncMock(return_value={})):
            ans = await wiki_route._try_shortcut("005930", "PER 몇이야?")
        self.assertIsNotNone(ans)
        self.assertIn("13.5", ans)
        self.assertIn("삼성전자", ans)

    async def test_shortcut_market_cap(self):
        with patch("routes.portfolio._fetch_quote", new=AsyncMock(return_value={})):
            ans = await wiki_route._try_shortcut("005930", "시가총액은?")
        self.assertIsNotNone(ans)
        self.assertIn("조", ans)  # 460조 formatted

    async def test_shortcut_dividend_yield(self):
        with patch("routes.portfolio._fetch_quote", new=AsyncMock(return_value={})):
            ans = await wiki_route._try_shortcut("005930", "배당수익률 얼마?")
        self.assertIsNotNone(ans)
        self.assertIn("0.48", ans)

    async def test_shortcut_current_price_needs_live_quote(self):
        # Without live quote, the 현재가 shortcut can't answer and falls through.
        with patch("routes.portfolio._fetch_quote", new=AsyncMock(return_value={})):
            ans = await wiki_route._try_shortcut("005930", "현재가 얼마?")
        self.assertIsNone(ans)
        # With a live quote it should answer.
        with patch("routes.portfolio._fetch_quote", new=AsyncMock(return_value={
            "price": 80000, "change": 500, "change_pct": 0.63,
        })):
            ans = await wiki_route._try_shortcut("005930", "현재가 얼마?")
        self.assertIsNotNone(ans)
        self.assertIn("80,000", ans)
        self.assertIn("+0.63%", ans)

    async def test_shortcut_misses_complex_question(self):
        # A deep question must fall through to LLM.
        with patch("routes.portfolio._fetch_quote", new=AsyncMock(return_value={"price": 80000})):
            ans = await wiki_route._try_shortcut("005930", "이 종목 앞으로 전망 어때?")
        self.assertIsNone(ans)


if __name__ == "__main__":
    unittest.main()
