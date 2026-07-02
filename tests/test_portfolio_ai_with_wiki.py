"""Verify portfolio AI prompt includes/excludes wiki snippets correctly
and doesn't explode when wiki is empty."""
import json
import unittest
from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin
from fastapi import HTTPException
from starlette.requests import Request

import cache
from core import rate_limit
from repositories import wiki as wiki_repo
from routes import portfolio as pf


def _mk_request() -> Request:
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/portfolio/ai-analysis",
        "headers": [],
        "query_string": b"",
        "client": ("127.0.0.1", 1), "server": ("t", 80), "scheme": "http",
    }
    # is_disconnected() needs a receive callable; return a benign "http.request"
    # every time so the check returns False.
    async def _recv():
        return {"type": "http.request", "body": b"", "more_body": False}
    return Request(scope, receive=_recv)


async def _consume_stream(response) -> tuple[str, list[dict]]:
    """Drain a StreamingResponse body and return (content, done_events)."""
    body_chunks: list[bytes] = []
    async for chunk in response.body_iterator:
        if isinstance(chunk, bytes):
            body_chunks.append(chunk)
        else:
            body_chunks.append(str(chunk).encode())
    body = b"".join(body_chunks).decode("utf-8", errors="replace")
    contents: list[str] = []
    dones: list[dict] = []
    for line in body.splitlines():
        if not line.startswith("data: "):
            continue
        try:
            obj = json.loads(line[6:])
        except Exception:
            continue
        if "content" in obj:
            contents.append(obj["content"])
        if obj.get("done"):
            dones.append(obj)
    return "".join(contents), dones


class PortfolioAIWikiTests(TempDbMixin):
    async def seed(self):
        db = await cache.get_db()
        await db.execute(
            "INSERT INTO users (google_sub, email, name, picture, email_verified, created_at, last_login_at, is_admin) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            ("u1", "u@e.com", "U", "", 1, "2026-01-01T00:00:00", "2026-01-01T00:00:00", 0),
        )
        # Portfolio with one holding.
        await db.execute(
            "INSERT INTO user_portfolio (google_sub, stock_code, stock_name, quantity, avg_price, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("u1", "005930", "삼성전자", 10, 70000, "2026-01-01", "2026-01-01"),
        )
        await db.commit()
        # Force a known OpenRouter key so the early-return guard passes.
        self._orig_key = pf._OPENROUTER_KEY
        pf._OPENROUTER_KEY = "test-key"

    async def asyncTearDown(self):
        rate_limit.reset_rate_limits()
        pf._OPENROUTER_KEY = self._orig_key
        await super().asyncTearDown()

    async def test_ai_analysis_enforces_burst_rate_limit(self):
        user = {"google_sub": "u1", "is_admin": False}
        rate_limit.reset_rate_limits()
        for _ in range(pf.AI_ANALYSIS_BURST_LIMIT):
            rate_limit.enforce_rate_limit(
                _mk_request(),
                scope="portfolio_ai_analysis",
                user=user,
                max_requests=pf.AI_ANALYSIS_BURST_LIMIT,
                window_seconds=pf.AI_ANALYSIS_BURST_WINDOW_SECONDS,
                detail="limited",
            )
        with patch("routes.portfolio.get_current_user", new=AsyncMock(return_value=user)):
            with self.assertRaises(HTTPException) as exc_info:
                await pf.ai_portfolio_analysis(_mk_request(), {})
        self.assertEqual(exc_info.exception.status_code, 429)

    async def _run_and_capture_prompt(self):
        """Invoke ai_portfolio_analysis with an HTTP mock that records the
        prompt and fakes a trivial SSE response. Returns (prompt, done_ev)."""
        captured = {}

        class _FakeStreamCtx:
            def __init__(self, status_code: int, lines: list[str]):
                self.status_code = status_code
                self._lines = lines
            async def __aenter__(self):
                return self
            async def __aexit__(self, *a):
                return False
            async def aiter_lines(self):
                for l in self._lines:
                    yield l
            async def aread(self):
                return b""

        class _FakeClient:
            def __init__(self, *a, **kw):
                pass
            async def __aenter__(self):
                return self
            async def __aexit__(self, *a):
                return False
            def stream(self, method, url, headers=None, json=None, **kw):
                captured["payload"] = json
                # Minimal SSE stream: one delta + [DONE]
                lines = [
                    'data: {"choices":[{"delta":{"content":"요약 "}}],"usage":{"prompt_tokens":100,"completion_tokens":20}}',
                    'data: [DONE]',
                ]
                return _FakeStreamCtx(200, lines)

        user = {"google_sub": "u1", "is_admin": False}
        # Domain logic lives in services.portfolio.ai_analysis, which reaches
        # quote enrichment / market indicators through shared module objects —
        # patch those directly instead of via the routes.portfolio namespace.
        with patch("routes.portfolio.get_current_user", new=AsyncMock(return_value=user)), \
             patch("httpx.AsyncClient", _FakeClient), \
             patch("services.portfolio.quote_service.enrich_with_cached_quotes", new=AsyncMock(return_value=[{
                 "stock_code": "005930", "stock_name": "삼성전자",
                 "quantity": 10, "avg_price": 70000,
                 "quote": {"price": 80000, "change_pct": 1.5},
             }])), \
             patch("market_indicators.fetch_indicators", new=AsyncMock(return_value={})):
            response = await pf.ai_portfolio_analysis(_mk_request(), {})
            # Drain the stream INSIDE the patch context — the generator
            # is iterated lazily, so exiting `with patch(...)` before
            # draining would expose the real httpx.AsyncClient.
            _, dones = await _consume_stream(response)
        return captured.get("payload") or {}, (dones[-1] if dones else {})

    async def test_prompt_skips_wiki_when_empty(self):
        payload, done = await self._run_and_capture_prompt()
        messages = payload.get("messages", [])
        prompt = messages[-1].get("content", "") if messages else ""
        self.assertEqual(payload.get("model"), "~google/gemini-flash-latest")
        self.assertEqual(payload.get("max_tokens"), pf.ai_analysis.AI_MAX_TOKENS)
        self.assertEqual(payload.get("reasoning"), {"effort": "low", "exclude": True})
        self.assertEqual(messages[0].get("role"), "system")
        self.assertNotIn("종목별 리서치 요약", prompt)
        self.assertIn("HTML 태그는 쓰지 말고", prompt)
        self.assertIn("판단 근거", prompt)
        self.assertIn("ASCII 막대그래프", prompt)
        self.assertEqual(done.get("wiki_used"), 0)
        self.assertEqual(done.get("reasoning_effort"), "low")
        self.assertEqual(done.get("context_holdings"), 15)
        self.assertEqual(done.get("context_reports_per_holding"), 3)

    async def test_prompt_includes_wiki_when_entries_exist(self):
        await wiki_repo.save_wiki_entry({
            "stock_code": "005930", "source_type": "broker_report", "source_ref": "sha1",
            "report_date": "2026-03-10", "firm": "삼성증권", "title": "HBM",
            "recommendation": "Buy", "target_price": 90000.0,
            "summary_md": "body", "key_points_md": "- HBM 출하 증가\n- 업황 반등",
            "model": "m", "tokens_in": 1, "tokens_out": 1,
            "created_at": "2026-04-17T00:00:00",
        })
        payload, done = await self._run_and_capture_prompt()
        messages = payload.get("messages", []) if payload else []
        prompt = messages[-1].get("content", "") if messages else ""
        # Debug: if prompt is empty, something broke before the LLM call.
        # Prompt must exist; if empty that means the HTTP mock wasn't
        # exercised (would indicate a regression in the patch target).
        if not prompt:
            self.fail(f"empty prompt, payload={payload!r}, done={done!r}")
        self.assertIn("종목별 리서치 요약", prompt)
        self.assertIn("삼성증권", prompt)
        self.assertIn("HBM", prompt)
        self.assertEqual(done.get("wiki_used"), 1)


if __name__ == "__main__":
    unittest.main()
