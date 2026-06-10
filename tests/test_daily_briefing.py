"""AI 데일리 브리핑 테스트.

* 컨텍스트 조립 — 시드된 임시 DB의 스냅샷/공시 리뷰/위키 항목에서 어제 NAV
  변화·기여 종목·신규 피드가 올바로 모이는지 (LLM 없음, 순수 데이터).
* 템플릿 폴백 — LLM(HTTP) 실패 시에도 발송 가능한 본문이 나오고, usage
  ledger 에 feature='daily_briefing' 실패 행이 남는지 (실제 ai_client 경로,
  httpx 만 모킹).
* 옵트인 필터링 / 사용자별 오류 격리 — send_briefings 배치 시멘틱.
* 내부 엔드포인트 인증 — loopback 전용 가드.
"""

from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from fastapi import HTTPException
from starlette.requests import Request

import cache
from _harness import TempDbMixin
from repositories import app_settings as app_settings_repo
from repositories import snapshots as snapshots_repo
from repositories import system_events as system_events_repo
from repositories import user_settings as user_settings_repo
from repositories import wiki as wiki_repo
from repositories import dart_review as dart_review_repo
from routes import internal
from services import daily_briefing


def _request(path: str = "/", headers: dict[str, str] | None = None, client_host: str = "127.0.0.1") -> Request:
    encoded = [
        (k.lower().encode("latin-1"), v.encode("latin-1"))
        for k, v in (headers or {}).items()
    ]
    scope = {
        "type": "http",
        "method": "POST",
        "path": path,
        "headers": encoded,
        "query_string": b"",
        "client": (client_host, 12345),
        "server": ("testserver", 80),
        "scheme": "http",
    }
    return Request(scope)


class DailyBriefingHarness(TempDbMixin):
    async def _seed_user(self, sub: str = "u1") -> None:
        db = await cache.get_db()
        await db.execute(
            "INSERT OR IGNORE INTO users (google_sub, email, name, picture, email_verified, created_at, last_login_at)"
            " VALUES (?, 'e@x', 'U', '', 1, 't', 't')",
            (sub,),
        )
        for code, name in (("005930", "삼성전자"), ("000660", "SK하이닉스")):
            await db.execute(
                "INSERT OR IGNORE INTO user_portfolio (google_sub, stock_code, stock_name, quantity, avg_price, created_at, updated_at)"
                " VALUES (?, ?, ?, 10, 1000, 't', 't')",
                (sub, code, name),
            )
        await db.commit()

    async def _seed_snapshots(self, sub: str = "u1") -> tuple[str, str]:
        d_prev = (date.today() - timedelta(days=2)).isoformat()
        d_last = (date.today() - timedelta(days=1)).isoformat()
        await snapshots_repo.save_snapshot(sub, d_prev, 1_000_000, 900_000, 1000.0, 1000.0)
        await snapshots_repo.save_snapshot(sub, d_last, 1_050_000, 900_000, 1050.0, 1000.0)
        await snapshots_repo.save_stock_snapshots(sub, d_prev, [
            {"stock_code": "005930", "market_value": 500_000},
            {"stock_code": "000660", "market_value": 300_000},
            {"stock_code": "CASH_KRW", "market_value": 200_000},
        ])
        await snapshots_repo.save_stock_snapshots(sub, d_last, [
            {"stock_code": "005930", "market_value": 570_000},
            {"stock_code": "000660", "market_value": 280_000},
            {"stock_code": "CASH_KRW", "market_value": 200_000},
        ])
        return d_prev, d_last


class BriefingContextTests(DailyBriefingHarness):
    async def test_context_assembly_from_seeded_db(self):
        await self._seed_user()
        d_prev, d_last = await self._seed_snapshots()
        now = datetime.now().isoformat()
        await dart_review_repo.save_dart_report_review({
            "stock_code": "005930",
            "corp_code": "c1",
            "corp_name": "삼성전자",
            "rcept_no": "20260609000001",
            "report_name": "분기보고서 (2026.03)",
            "report_date": d_last,
            "created_at": now,
        })
        await wiki_repo.save_wiki_entry({
            "stock_code": "000660",
            "source_type": "report_pdf",
            "source_ref": "sha-1",
            "report_date": d_last,
            "firm": "한국증권",
            "title": "HBM 사이클 점검",
            "recommendation": "매수",
            "target_price": 300000,
            "summary_md": "s",
            "key_points_md": "k",
            "model": "m",
            "tokens_in": 1,
            "tokens_out": 1,
            "created_at": now,
        })

        calendar_payload = {"events": [{
            "time": "21:30", "country_name": "미국", "flag": "🇺🇸",
            "event": "소비자물가지수(CPI)", "importance": "high",
        }]}
        with patch("economic_calendar.fetch_economic_calendar", new=AsyncMock(return_value=calendar_payload)), \
             patch.object(daily_briefing.ai_analysis, "market_summary_lines", new=AsyncMock(return_value=["- KOSPI: 2900 (+0.5%)"])):
            ctx = await daily_briefing.build_briefing_context("u1")

        # 어제 NAV 변화 (원·%)
        self.assertEqual(ctx["nav"]["date"], d_last)
        self.assertEqual(ctx["nav"]["prev_date"], d_prev)
        self.assertAlmostEqual(ctx["nav"]["change_krw"], 50_000)
        self.assertAlmostEqual(ctx["nav"]["change_pct"], 5.0)
        # 기여 상위/하위 — 현금성 코드 제외
        top_codes = [m["stock_code"] for m in ctx["movers"]["top"]]
        bottom_codes = [m["stock_code"] for m in ctx["movers"]["bottom"]]
        self.assertEqual(top_codes, ["005930"])
        self.assertEqual(bottom_codes, ["000660"])
        self.assertEqual(ctx["movers"]["top"][0]["stock_name"], "삼성전자")
        self.assertAlmostEqual(ctx["movers"]["top"][0]["change_krw"], 70_000)
        self.assertNotIn("CASH_KRW", top_codes + bottom_codes)
        # 신규 공시 리뷰 / 리포트
        self.assertEqual(ctx["filings"][0]["report_name"], "분기보고서 (2026.03)")
        self.assertEqual(ctx["reports"][0]["title"], "HBM 사이클 점검")
        # 오늘 경제 일정 + 시장 지표
        self.assertEqual(ctx["calendar"][0]["event"], "소비자물가지수(CPI)")
        self.assertIn("- KOSPI: 2900 (+0.5%)", ctx["market"])

        # 템플릿 렌더도 핵심 수치를 담는다 (LLM 폴백 본문)
        text = daily_briefing.render_template_briefing(ctx)
        self.assertTrue(text.startswith("🌅 데일리 브리핑"))
        self.assertIn("삼성전자", text)
        self.assertIn("+5.00%", text)

    async def test_context_with_empty_db_is_safe(self):
        await self._seed_user("u-empty")
        with patch("economic_calendar.fetch_economic_calendar", new=AsyncMock(return_value={"events": []})), \
             patch.object(daily_briefing.ai_analysis, "market_summary_lines", new=AsyncMock(return_value=[])):
            ctx = await daily_briefing.build_briefing_context("u-empty")
        self.assertIsNone(ctx["nav"])
        self.assertEqual(ctx["movers"], {"top": [], "bottom": []})
        text = daily_briefing.render_template_briefing(ctx)
        self.assertIn("데일리 브리핑", text)


class GenerateBriefingTests(DailyBriefingHarness):
    def _minimal_context(self) -> dict:
        return {
            "google_sub": "u1",
            "date": date.today().isoformat(),
            "nav": {"date": "2026-06-09", "prev_date": "2026-06-08", "total_value": 1_050_000,
                    "prev_value": 1_000_000, "change_krw": 50_000, "change_pct": 5.0},
            "movers": {"top": [], "bottom": []},
            "filings": [], "reports": [], "calendar": [], "market": [],
        }

    async def _usage_rows(self) -> list[dict]:
        db = await cache.get_db()
        cursor = await db.execute(
            "SELECT feature, model_profile, input_tokens, output_tokens, ok, error FROM ai_usage_events"
        )
        return [dict(r) for r in await cursor.fetchall()]

    async def test_template_fallback_without_api_key(self):
        # 키 미설정 → LLM 호출 전에 실패 → 템플릿 폴백 (발송은 가능해야 한다)
        with patch.object(daily_briefing, "build_briefing_context", new=AsyncMock(return_value=self._minimal_context())), \
             patch.dict("os.environ", {}, clear=True):
            briefing = await daily_briefing.generate_briefing("u1")
        self.assertEqual(briefing["source"], "template")
        self.assertTrue(briefing["text"].startswith("🌅 데일리 브리핑"))
        self.assertIn("+5.00%", briefing["text"])

    async def test_llm_failure_falls_back_and_records_usage(self):
        await app_settings_repo.set_app_setting("OPENROUTER_API_KEY", "sk-or-test", is_secret=True)
        with patch.object(daily_briefing, "build_briefing_context", new=AsyncMock(return_value=self._minimal_context())), \
             patch.object(httpx.AsyncClient, "post", new=AsyncMock(side_effect=httpx.ConnectError("boom"))):
            briefing = await daily_briefing.generate_briefing("u1")
        self.assertEqual(briefing["source"], "template")
        self.assertIn("+5.00%", briefing["text"])
        rows = await self._usage_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["feature"], "daily_briefing")
        self.assertEqual(rows[0]["ok"], 0)

    async def test_llm_success_records_usage_ledger(self):
        await app_settings_repo.set_app_setting("OPENROUTER_API_KEY", "sk-or-test", is_secret=True)
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {
            "model": "test/model",
            "choices": [{"message": {"content": "🌅 데일리 브리핑\n어제 +5.0% 상승했습니다."}}],
            "usage": {"prompt_tokens": 120, "completion_tokens": 45, "cost": 0.0011},
        }
        with patch.object(daily_briefing, "build_briefing_context", new=AsyncMock(return_value=self._minimal_context())), \
             patch.object(httpx.AsyncClient, "post", new=AsyncMock(return_value=resp)):
            briefing = await daily_briefing.generate_briefing("u1")
        self.assertEqual(briefing["source"], "ai")
        self.assertEqual(briefing["model"], "test/model")
        self.assertIn("데일리 브리핑", briefing["text"])
        rows = await self._usage_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["feature"], "daily_briefing")
        self.assertEqual(rows[0]["model_profile"], "daily_briefing")
        self.assertEqual(rows[0]["ok"], 1)
        self.assertEqual(rows[0]["input_tokens"], 120)
        self.assertEqual(rows[0]["output_tokens"], 45)


class SendBriefingsTests(DailyBriefingHarness):
    async def test_opt_in_filtering_default_off(self):
        for sub in ("u1", "u2", "u3"):
            await self._seed_user(sub)
        # u1 만 옵트인. u2 는 명시적 false, u3 는 설정 없음(기본 OFF).
        await daily_briefing.set_enabled("u1", True)
        await daily_briefing.set_enabled("u2", False)

        generate = AsyncMock(return_value={"text": "본문", "source": "template", "model": None})
        dispatch = AsyncMock(return_value=1)
        with patch.object(daily_briefing, "generate_briefing", new=generate), \
             patch.object(daily_briefing.channels, "has_active_channel", new=AsyncMock(return_value=True)), \
             patch.object(daily_briefing.channels, "dispatch", new=dispatch):
            result = await daily_briefing.send_briefings()

        self.assertEqual(result, {"users": 1, "sent": 1, "failed": 0, "skipped": 0})
        generate.assert_awaited_once_with("u1")
        dispatch.assert_awaited_once_with("u1", "본문")
        events = await system_events_repo.get_system_events(source="daily_briefing")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["kind"], "send_ok")

    async def test_no_active_channel_is_skipped_without_generation(self):
        await self._seed_user("u1")
        await daily_briefing.set_enabled("u1", True)
        generate = AsyncMock()
        with patch.object(daily_briefing, "generate_briefing", new=generate), \
             patch.object(daily_briefing.channels, "has_active_channel", new=AsyncMock(return_value=False)):
            result = await daily_briefing.send_briefings()
        self.assertEqual(result["skipped"], 1)
        self.assertEqual(result["sent"], 0)
        generate.assert_not_awaited()  # LLM 비용 없이 건너뛴다

    async def test_per_user_error_isolation(self):
        for sub in ("u1", "u2"):
            await self._seed_user(sub)
            await daily_briefing.set_enabled(sub, True)

        async def _generate(google_sub):
            if google_sub == "u1":
                raise RuntimeError("LLM/DB exploded")
            return {"text": "본문", "source": "ai", "model": "m"}

        dispatch = AsyncMock(return_value=1)
        with patch.object(daily_briefing, "generate_briefing", new=AsyncMock(side_effect=_generate)), \
             patch.object(daily_briefing.channels, "has_active_channel", new=AsyncMock(return_value=True)), \
             patch.object(daily_briefing.channels, "dispatch", new=dispatch):
            result = await daily_briefing.send_briefings()

        # u1 실패가 u2 발송을 막지 않는다
        self.assertEqual(result["sent"], 1)
        self.assertEqual(result["failed"], 1)
        dispatch.assert_awaited_once_with("u2", "본문")
        kinds = {e["kind"] for e in await system_events_repo.get_system_events(source="daily_briefing")}
        self.assertEqual(kinds, {"send_ok", "send_fail"})

    async def test_dispatch_zero_channels_counts_as_failure(self):
        await self._seed_user("u1")
        await daily_briefing.set_enabled("u1", True)
        with patch.object(daily_briefing, "generate_briefing", new=AsyncMock(return_value={"text": "x", "source": "template", "model": None})), \
             patch.object(daily_briefing.channels, "has_active_channel", new=AsyncMock(return_value=True)), \
             patch.object(daily_briefing.channels, "dispatch", new=AsyncMock(return_value=0)):
            result = await daily_briefing.send_briefings()
        self.assertEqual(result["failed"], 1)
        events = await system_events_repo.get_system_events(source="daily_briefing")
        self.assertEqual(events[0]["kind"], "send_fail")


class OptInSettingTests(DailyBriefingHarness):
    async def test_default_off_and_roundtrip(self):
        await self._seed_user("u1")
        self.assertFalse(await daily_briefing.is_enabled("u1"))
        await daily_briefing.set_enabled("u1", True)
        self.assertTrue(await daily_briefing.is_enabled("u1"))
        self.assertEqual(await daily_briefing.opted_in_users(), ["u1"])
        await daily_briefing.set_enabled("u1", False)
        self.assertFalse(await daily_briefing.is_enabled("u1"))
        self.assertEqual(await daily_briefing.opted_in_users(), [])
        # K/V 저장 확인 — 별도 테이블 없이 user_settings 를 쓴다
        raw = await user_settings_repo.get_user_setting("u1", daily_briefing.OPT_IN_KEY)
        self.assertEqual(raw, "false")


class BriefingRouteTests(DailyBriefingHarness):
    """옵트인 GET/PUT 라우트 — 채널 미연결 시 409, 켜고 끄기 왕복."""

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        await self._seed_user("u1")
        from core.app_factory import create_app
        from core.config import AppSettings, PROJECT_ROOT
        from routes import notifications as notif_route

        self.user = {"google_sub": "u1", "email": "e@x", "name": "U"}
        self.auth_patch = patch.object(
            notif_route, "get_current_user", new=AsyncMock(return_value=self.user)
        )
        self.auth_patch.start()
        settings = AppSettings(
            environment="development",
            project_root=PROJECT_ROOT,
            app_title="Test Compass",
            public_api_base_url="https://api.example.test",
            cors_allowed_origins=("https://app.example.test",),
            enable_docs=False,
        )
        self.app = create_app(settings)
        self.transport = httpx.ASGITransport(app=self.app, raise_app_exceptions=False)
        self.client = httpx.AsyncClient(transport=self.transport, base_url="http://testserver")

    async def asyncTearDown(self) -> None:
        await self.client.aclose()
        self.auth_patch.stop()
        await super().asyncTearDown()

    async def _connect_telegram(self) -> None:
        from repositories import notifications as notifications_repo
        await notifications_repo.upsert_notification_channel(
            "u1", "telegram",
            config={"bot_token": "t", "chat_id": "1"},
            enabled=True, verified=True,
        )

    async def test_get_briefing_defaults_off(self):
        resp = await self.client.get("/api/notifications/briefing")
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(resp.json()["enabled"])

    async def test_put_requires_active_channel(self):
        resp = await self.client.put("/api/notifications/briefing", json={"enabled": True})
        self.assertEqual(resp.status_code, 409)
        self.assertFalse(await daily_briefing.is_enabled("u1"))

    async def test_put_roundtrip_with_channel(self):
        await self._connect_telegram()
        resp = await self.client.put("/api/notifications/briefing", json={"enabled": True})
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertTrue(await daily_briefing.is_enabled("u1"))
        # /channels 응답에도 실려 알림 설정 모달이 한 번에 그린다
        channels_resp = await self.client.get("/api/notifications/channels")
        self.assertTrue(channels_resp.json()["daily_briefing"]["enabled"])
        # 끄기는 채널 유무와 무관하게 항상 가능
        resp = await self.client.put("/api/notifications/briefing", json={"enabled": False})
        self.assertEqual(resp.status_code, 200)
        self.assertFalse(await daily_briefing.is_enabled("u1"))


class InternalEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_rejects_forwarded_request_without_token(self):
        request = _request(
            "/api/internal/daily-briefing/send",
            headers={"X-Forwarded-For": "203.0.113.10"},
        )
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(HTTPException) as exc_info:
                await internal.run_daily_briefing_send(request)
        self.assertEqual(exc_info.exception.status_code, 403)

    async def test_rejects_non_loopback_host(self):
        request = _request("/api/internal/daily-briefing/send", client_host="203.0.113.10")
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(HTTPException) as exc_info:
                await internal.run_daily_briefing_send(request)
        self.assertEqual(exc_info.exception.status_code, 403)

    async def test_loopback_runs_batch(self):
        request = _request("/api/internal/daily-briefing/send")
        payload = {"users": 2, "sent": 1, "failed": 0, "skipped": 1}
        with patch.dict("os.environ", {}, clear=True), \
             patch.object(daily_briefing, "send_briefings", new=AsyncMock(return_value=payload)):
            result = await internal.run_daily_briefing_send(request)
        self.assertEqual(result, {"ok": True, **payload})

    async def test_internal_token_accepted_behind_proxy(self):
        request = _request(
            "/api/internal/daily-briefing/send",
            headers={"X-Forwarded-For": "203.0.113.10", "X-Internal-Token": "secret"},
        )
        payload = {"users": 0, "sent": 0, "failed": 0, "skipped": 0}
        with patch.dict("os.environ", {"INTERNAL_API_TOKEN": "secret"}, clear=True), \
             patch.object(daily_briefing, "send_briefings", new=AsyncMock(return_value=payload)):
            result = await internal.run_daily_briefing_send(request)
        self.assertTrue(result["ok"])


if __name__ == "__main__":
    unittest.main()
