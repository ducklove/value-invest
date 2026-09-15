import asyncio
import unittest
from datetime import datetime
from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin

from core.errors import AppError
from repositories import snapshots
from repositories.db import transaction
from services import daily_briefing
from services.portfolio import morning_valuation as morning
from services.portfolio.time_windows import KST


class MorningValuationTests(TempDbMixin):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        morning._capture_lock = asyncio.Lock()
        async with transaction() as db:
            await db.execute("INSERT INTO users (google_sub,email,name,created_at,last_login_at) VALUES ('u','e','U','t','t')")
            for code, name, qty, group in (("005930", "국내", 10, "국내"), ("VOO", "S&P 500", 2, "해외"),
                                            ("GOOGL", "구글", 1, "해외"), ("CASH_KRW", "원화", 100, "현금")):
                await db.execute(
                    "INSERT INTO user_portfolio (google_sub,stock_code,stock_name,quantity,avg_price,group_name,created_at,updated_at) "
                    "VALUES ('u',?,?,?,?,?,'t','t')", (code, name, qty, 1, group),
                )
        await snapshots.save_snapshot("u", "2026-09-15", 1400, 1000, 1400, 1)
        await snapshots.save_stock_snapshots("u", "2026-09-15", [
            {"stock_code": "005930", "market_value": 1000, "quantity": 10, "group_name": "국내"},
            {"stock_code": "VOO", "market_value": 200, "quantity": 2, "group_name": "해외"},
            {"stock_code": "GOOGL", "market_value": 100, "quantity": 1, "group_name": "해외"},
            {"stock_code": "CASH_KRW", "market_value": 100, "quantity": 100, "group_name": "현금"},
        ])
        self.now = datetime(2026, 9, 16, 7, 0, 1, tzinfo=KST)
        self.prices = {"005930": 100, "VOO": 120, "GOOGL": 90, "CASH_KRW": 1}

    async def capture(self, **overrides):
        async def quote(code, **kwargs):
            return overrides.get(code, {"price": self.prices[code]})

        with patch.object(morning.time_windows, "now_kst", return_value=self.now), \
             patch.object(morning.runtime_quotes, "fetch_quote", new=AsyncMock(side_effect=quote)):
            return await morning.capture("u")

    async def test_overnight_total_groups_and_saved_cutoff_survive_resend(self):
        value = await self.capture()
        self.assertEqual(value["as_of"], "2026-09-16T07:00:00+09:00")
        self.assertEqual(value["total_value"], 1430)
        self.assertEqual(value["change_krw"], 30)
        group = value["overseas_groups"][0]
        self.assertEqual(group["market_value"], 330)
        self.assertEqual(group["change_pct"], 10)
        self.assertEqual(group["top"][0]["stock_code"], "VOO")
        self.assertEqual(group["bottom"][0]["change_krw"], -10)
        self.assertEqual((group["up_count"], group["down_count"]), (1, 1))
        self.assertAlmostEqual(group["weight_pct"], 330 / 1430 * 100)
        self.now = self.now.replace(hour=12)
        with patch.object(morning.time_windows, "now_kst", return_value=self.now), \
             patch.object(morning.runtime_quotes, "fetch_quote", new=AsyncMock()) as quote:
            self.assertEqual(await morning.capture("u"), value)
        quote.assert_not_awaited()
        self.assertIsNone(await morning.load("u", "2026-09-17"))
        self.assertEqual((await snapshots.get_latest_snapshot("u"))["date"], "2026-09-15")

    async def test_late_capture_discloses_actual_time_and_missing_quotes_never_become_zero(self):
        self.now = self.now.replace(hour=9, minute=12)
        value = await self.capture(VOO={"price": 120, "_stale": True})
        self.assertEqual(value["source"], "late")
        self.assertIn("T09:12", value["as_of"])
        self.assertIsNone(value["total_value"])
        self.assertIsNone(value["overseas_groups"][0]["market_value"])
        self.assertIn("S&P 500", value["missing"])
        self.assertEqual((await self.capture())["total_value"], 1430)

    async def test_cashflow_is_removed_but_later_cashflow_is_not(self):
        flows = [
            {"type": "deposit", "amount": 20, "created_at": "2026-09-16T06:00:00"},
            {"type": "withdrawal", "amount": 100, "created_at": "2026-09-16T08:00:00"},
        ]
        with patch.object(morning.snapshots, "get_cashflows_created_after", new=AsyncMock(return_value=flows)):
            value = await self.capture()
        self.assertEqual(value["change_krw"], 10)
        self.assertEqual(value["net_cashflow"], 20)

    async def test_quantity_change_does_not_appear_as_group_investment_return(self):
        async with transaction() as db:
            await db.execute("UPDATE user_portfolio SET quantity=3 WHERE stock_code='VOO'")
        group = (await self.capture())["overseas_groups"][0]
        self.assertIsNone(group["change_krw"])
        self.assertIsNone(group["change_pct"])
        self.assertTrue(group["comparison_unavailable"])
        self.assertEqual(group["bottom"][0]["stock_code"], "GOOGL")

    async def test_before_cutoff_cannot_capture_future_value(self):
        self.now = self.now.replace(hour=6, minute=59)
        with self.assertRaises(AppError):
            await self.capture()

    async def test_morning_context_uses_saved_value_and_never_night_snapshot(self):
        value = await self.capture()
        with patch.object(daily_briefing.time_windows, "today_kst_date", return_value=self.now.date()), \
             patch.object(daily_briefing.snapshots_repo, "get_latest_snapshot", new=AsyncMock()) as latest, \
             patch.object(daily_briefing.ai_analysis, "market_summary_lines", new=AsyncMock(return_value=[])), \
             patch.object(daily_briefing, "_fetch_night_futures_block", new=AsyncMock(return_value=None)), \
             patch("economic_calendar.fetch_economic_calendar", new=AsyncMock(return_value={"events": []})):
            context = await daily_briefing.build_briefing_context("u", "morning")
        latest.assert_not_awaited()
        self.assertIsNone(context["nav"])
        self.assertEqual(context["morning_valuation"], value)
        text = daily_briefing.render_template_briefing(context)
        self.assertLess(text.index("해외 그룹 성과"), text.index("총평가"))
        self.assertIn("2026-09-16 07:00 KST", text)
        self.assertIn("S&P 500", text)
        self.assertIn("구글", text)
        self.assertIn("총평가 1,430", text)
        self.assertNotIn("📊 어제", text)

    async def test_missing_today_snapshot_is_explicit(self):
        context = {"date": "2026-09-16", "nav": {"total_value": 1400, "date": "2026-09-15"}}
        text = daily_briefing.render_template_briefing(context)
        self.assertIn("07:00 평가 데이터 미수집", text)
        self.assertNotIn("1,400", text)

    async def test_ai_keeps_exact_valuation_and_overseas_details_at_front(self):
        value = await self.capture()
        context = {"date": value["date"], "briefing_type": "morning", "morning_valuation": value,
                   "overseas_groups": value["overseas_groups"]}
        with patch.object(daily_briefing, "build_briefing_context", new=AsyncMock(return_value=context)), \
             patch.object(daily_briefing.ai_config, "get_model_for_feature", new=AsyncMock(return_value="test/model")), \
             patch.object(daily_briefing.ai_client, "post_chat_completion", new=AsyncMock(return_value={
                 "content": "🌅 모닝 브리핑\n🌐 간밤 시장\n• 오늘 경제 일정을 확인하세요.", "finish_reason": "stop",
             })) as generate:
            result = await daily_briefing.generate_briefing("u", "morning")
        self.assertEqual(generate.await_args.kwargs["payload"]["reasoning"]["effort"], "none")
        self.assertEqual(result["source"], "ai")
        self.assertLess(result["text"].index("해외 그룹 성과"), result["text"].index("총평가"))
        self.assertLess(result["text"].index("총평가"), result["text"].index("간밤 시장"))
        self.assertIn("총평가 1,430", result["text"])
        self.assertIn("상승 기여 S&P 500 +40 (+20.00%)", result["text"])


if __name__ == "__main__":
    unittest.main()
