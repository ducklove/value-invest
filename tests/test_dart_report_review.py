import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

import cache
import dart_report_review
from routes import dart_review as dart_review_route


class DartReportReviewHelperTests(unittest.TestCase):
    def test_normalize_review_accepts_fenced_json(self):
        raw = """```json
        {"summary_md":"# 요약","cards":[{"label":"매출","value":"증가","tone":"good"}]}
        ```"""

        parsed = dart_report_review._normalize_review(raw)

        self.assertEqual(parsed["summary_md"], "# 요약")
        self.assertEqual(parsed["cards"][0]["tone"], "good")

    def test_coerce_number_from_value_or_string(self):
        self.assertEqual(dart_report_review._coerce_number(13.1), 13.1)
        self.assertEqual(dart_report_review._coerce_number("42.8%"), 42.8)
        self.assertEqual(dart_report_review._coerce_number("333.6조"), 333.6)
        self.assertEqual(dart_report_review._coerce_number("1,665"), 1665.0)
        self.assertIsNone(dart_report_review._coerce_number("N/A"))
        self.assertIsNone(dart_report_review._coerce_number(True))

    def test_normalize_metric_trends_parses_and_filters(self):
        raw = {
            "summary_md": "# 요약",
            "metric_trends": [
                {"label": "영업이익률", "unit": "%", "note": "약 3.3배",
                 "before": {"label": "2025 연간", "value": "13.1%"},
                 "after": {"label": "2026 1분기", "value": 42.8}},
                {"label": "매출", "before": {"value": "333.6조"}, "after": {"value": "133.9조"}},
                {"label": "빈 항목", "before": {}, "after": {}},  # 값 없음 → 제외
                "not a dict",  # 비dict → 무시
            ],
        }
        parsed = dart_report_review._normalize_review(json.dumps(raw))
        trends = parsed["metric_trends"]
        self.assertEqual(len(trends), 2)  # 빈 항목/문자열 제거
        self.assertEqual(trends[0]["label"], "영업이익률")
        self.assertEqual(trends[0]["before"]["value"], 13.1)   # "13.1%" → 13.1
        self.assertEqual(trends[0]["after"]["value"], 42.8)
        self.assertEqual(trends[0]["unit"], "%")
        self.assertEqual(trends[1]["before"]["value"], 333.6)

    def test_normalize_review_defaults_metric_trends_to_list(self):
        parsed = dart_report_review._normalize_review('{"summary_md":"x"}')
        self.assertEqual(parsed["metric_trends"], [])

    def test_zip_document_text_extracts_html_body(self):
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr(
                "report.xml",
                "<html><body><h1>분기보고서</h1><p>사업의 내용 매출이 증가했습니다.</p></body></html>",
            )

        text = dart_report_review._zip_document_text(buf.getvalue())

        self.assertIn("분기보고서", text)
        self.assertIn("매출이 증가했습니다", text)

    def test_focus_snippets_prioritizes_business_and_finance_sections(self):
        text = "앞부분 " * 100 + "사업의 내용 주요 제품 설명입니다. " + "중간 " * 100 + "재무에 관한 사항 현금흐름 설명입니다."

        snippet = dart_report_review._focus_snippets(text, limit=800)

        self.assertIn("사업의 내용", snippet)
        self.assertIn("주요 제품", snippet)


class DartReportReviewBrokenDetectionTests(unittest.TestCase):
    def test_truncated_json_blob_is_detected_as_broken(self):
        blob = '{"summary_md": "# 삼성전자 1분기 실적\\n\\n## 요약\\n| 항목 | 값 |", "cards": [{"label":"매출"'
        self.assertTrue(dart_report_review._looks_like_raw_json_blob(blob))
        self.assertTrue(dart_report_review._review_payload_is_broken({"summary_md": blob}))

    def test_normal_markdown_is_not_broken(self):
        md = "# 삼성전자 1분기 실적\n\n## 실적 요약\n| 항목 | 값 |"
        self.assertFalse(dart_report_review._looks_like_raw_json_blob(md))
        self.assertFalse(dart_report_review._review_payload_is_broken({"summary_md": md}, finish_reason="stop"))

    def test_length_finish_reason_marks_payload_broken(self):
        self.assertTrue(
            dart_report_review._review_payload_is_broken({"summary_md": "# 정상"}, finish_reason="length")
        )

    def test_cached_review_broken_uses_review_md_or_summary(self):
        blob = '{"summary_md": "본문", "cards": ['
        self.assertTrue(dart_report_review._cached_review_is_broken({"review": {"summary_md": blob}}))
        self.assertTrue(dart_report_review._cached_review_is_broken({"review_md": blob}))
        self.assertFalse(dart_report_review._cached_review_is_broken({"review": {"summary_md": "# 정상"}, "review_md": "# 정상"}))
        self.assertFalse(dart_report_review._cached_review_is_broken(None))

    def test_finish_reason_extraction(self):
        data = {"choices": [{"finish_reason": "length"}]}
        self.assertEqual(dart_report_review._finish_reason_from_response(data), "length")
        self.assertIsNone(dart_report_review._finish_reason_from_response({}))
        self.assertIsNone(dart_report_review._finish_reason_from_response(None))


class DartReportReviewTruncationTests(unittest.IsolatedAsyncioTestCase):
    BROKEN = (
        {"summary_md": '{"summary_md": "본문", "cards": [{"label":"매출"', "cards": []},
        {"model": "m", "tokens_in": 1, "tokens_out": 2, "cost_usd": 0.0, "finish_reason": "length"},
    )
    GOOD = (
        {
            "summary_md": "# 정상 리뷰",
            "cards": [{"label": "매출", "value": "증가", "tone": "good"}],
            "watch_items": [],
            "comparison_notes": [],
            "source_limits": "",
        },
        {"model": "m", "tokens_in": 3, "tokens_out": 4, "cost_usd": 0.001, "finish_reason": "stop"},
    )

    def _common_patches(self, save_mock):
        filings = [{
            "rcept_no": "r1",
            "report_name": "분기보고서 (2026.03)",
            "report_date": "2026-04-01",
            "viewer_url": "u",
            "kind": "분기",
            "period": "2026.03",
        }]
        return [
            patch.object(cache, "get_corp_code", AsyncMock(return_value="00126380")),
            patch.object(cache, "get_corp_name", AsyncMock(return_value="삼성전자")),
            patch.object(dart_report_review, "fetch_periodic_filings", AsyncMock(return_value=filings)),
            patch.object(dart_report_review, "fetch_document_text", AsyncMock(return_value="원문 텍스트")),
            patch.object(dart_report_review, "_financial_context", AsyncMock(return_value="재무 컨텍스트")),
            patch.object(cache, "save_dart_report_review", save_mock),
        ]

    async def _run_generate(self, call_mock, save_mock):
        import contextlib
        with contextlib.ExitStack() as stack:
            for p in self._common_patches(save_mock):
                stack.enter_context(p)
            stack.enter_context(patch.object(dart_report_review, "_call_openrouter", call_mock))
            return await dart_report_review.generate_review("005930", google_sub=None, force=True)

    async def test_persistent_truncation_retries_then_raises_without_caching(self):
        call = AsyncMock(side_effect=[self.BROKEN, self.BROKEN])
        save = AsyncMock(side_effect=lambda review: review)

        with self.assertRaises(dart_report_review.DartReportReviewError):
            await self._run_generate(call, save)

        self.assertEqual(call.await_count, 2)
        save.assert_not_awaited()
        # retry must escalate the token budget
        self.assertEqual(
            call.await_args_list[1].kwargs.get("max_tokens"),
            dart_report_review.DART_REVIEW_MAX_TOKENS_RETRY,
        )

    async def test_truncation_then_success_caches_good_review(self):
        call = AsyncMock(side_effect=[self.BROKEN, self.GOOD])
        save = AsyncMock(side_effect=lambda review: review)

        saved = await self._run_generate(call, save)

        self.assertEqual(call.await_count, 2)
        save.assert_awaited_once()
        self.assertEqual(saved["review"]["summary_md"], "# 정상 리뷰")
        self.assertEqual(saved["review_md"], "# 정상 리뷰")
        self.assertFalse(saved["cached"])

    async def test_first_attempt_healthy_does_not_retry(self):
        call = AsyncMock(side_effect=[self.GOOD])
        save = AsyncMock(side_effect=lambda review: review)

        saved = await self._run_generate(call, save)

        self.assertEqual(call.await_count, 1)
        save.assert_awaited_once()
        self.assertEqual(saved["review_md"], "# 정상 리뷰")

    async def test_latest_status_treats_broken_cache_as_missing(self):
        broken_cached = {
            "review": {"summary_md": '{"summary_md": "본문", "cards": ['},
            "review_md": '{"summary_md": "본문", "cards": [',
        }
        filings = [{"rcept_no": "r1", "report_name": "분기보고서 (2026.03)", "report_date": "2026-04-01"}]
        with (
            patch.object(cache, "get_corp_code", AsyncMock(return_value="00126380")),
            patch.object(cache, "get_corp_name", AsyncMock(return_value="삼성전자")),
            patch.object(dart_report_review, "fetch_periodic_filings", AsyncMock(return_value=filings)),
            patch.object(cache, "get_dart_report_review", AsyncMock(return_value=broken_cached)),
        ):
            result = await dart_report_review.latest_review_status("005930")

        self.assertEqual(result["status"], "missing")
        self.assertIsNone(result["previous_review"])

    async def test_latest_status_serves_healthy_cache_as_ready(self):
        good_cached = {"review": {"summary_md": "# 정상"}, "review_md": "# 정상"}
        filings = [{"rcept_no": "r1", "report_name": "분기보고서 (2026.03)", "report_date": "2026-04-01"}]
        with (
            patch.object(cache, "get_corp_code", AsyncMock(return_value="00126380")),
            patch.object(cache, "get_corp_name", AsyncMock(return_value="삼성전자")),
            patch.object(dart_report_review, "fetch_periodic_filings", AsyncMock(return_value=filings)),
            patch.object(cache, "get_dart_report_review", AsyncMock(return_value=good_cached)),
        ):
            result = await dart_report_review.latest_review_status("005930")

        self.assertEqual(result["status"], "ready")
        self.assertTrue(result["review"]["cached"])


class DartReportReviewCacheTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(cache, "DB_PATH", Path(self.tmp.name) / "cache.db")
        self.db_patch.start()
        await cache.close_db()
        await cache.init_db()

    async def asyncTearDown(self):
        await cache.close_db()
        self.db_patch.stop()
        self.tmp.cleanup()

    async def test_save_and_get_dart_report_review_roundtrips_json(self):
        saved = await cache.save_dart_report_review({
            "stock_code": "005930",
            "corp_code": "00126380",
            "corp_name": "삼성전자",
            "rcept_no": "20260401000001",
            "report_name": "분기보고서 (2026.03)",
            "report_date": "2026-04-01",
            "primary_doc_chars": 1234,
            "comparison_reports": [{"rcept_no": "20251114000001", "report_name": "분기보고서 (2025.09)"}],
            "review": {"summary_md": "# 리뷰", "cards": [{"label": "리스크", "value": "점검"}]},
            "review_md": "# 리뷰",
            "model": "deepseek/deepseek-v4-flash",
            "tokens_in": 10,
            "tokens_out": 20,
            "cost_usd": 0.001,
        })

        loaded = await cache.get_dart_report_review("005930", "20260401000001")

        self.assertEqual(saved["review"]["summary_md"], "# 리뷰")
        self.assertEqual(loaded["comparison_reports"][0]["rcept_no"], "20251114000001")
        self.assertEqual(loaded["model"], "deepseek/deepseek-v4-flash")


class DartReportReviewPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_pipeline_skips_ready_and_generates_missing(self):
        async def fake_status(stock_code):
            if stock_code == "005930":
                return {"status": "ready", "latest_report": {"rcept_no": "ready-rcept"}}
            return {"status": "missing", "latest_report": {"rcept_no": "missing-rcept"}}

        async def fake_generate(stock_code, *, google_sub, force):
            return {
                "stock_code": stock_code,
                "rcept_no": "missing-rcept",
                "report_date": "2026-04-01",
                "model": "deepseek/deepseek-v4-flash",
            }

        with (
            patch.object(cache, "select_wiki_target_stocks", AsyncMock(return_value=["005930", "000660", "000660"])),
            patch.object(cache, "get_corp_code", AsyncMock(return_value="00126380")),
            patch.object(dart_report_review, "latest_review_status", side_effect=fake_status),
            patch.object(dart_report_review, "generate_review", side_effect=fake_generate),
        ):
            stats = await dart_report_review.run_pipeline(target_limit=1)

        self.assertEqual(stats["stocks_total"], 2)
        self.assertEqual(stats["stocks_processed"], 2)
        self.assertEqual(stats["generated"], 1)
        self.assertEqual(stats["skipped"], 1)
        self.assertEqual(stats["skipped_by_reason"], {"already_ready": 1})

    async def test_generation_limit_counts_new_reviews_not_checked_targets(self):
        async def fake_status(stock_code):
            return {"status": "missing", "latest_report": {"rcept_no": stock_code}}

        async def fake_generate(stock_code, *, google_sub, force):
            return {"stock_code": stock_code, "rcept_no": stock_code, "model": "test-model"}

        with (
            patch.object(cache, "get_corp_code", AsyncMock(return_value="00126380")),
            patch.object(dart_report_review, "latest_review_status", side_effect=fake_status),
            patch.object(dart_report_review, "generate_review", side_effect=fake_generate),
        ):
            stats = await dart_report_review.run_pipeline(
                stock_codes=["000001", "000002", "000003"],
                target_limit=2,
            )

        self.assertEqual(stats["stocks_total"], 3)
        self.assertEqual(stats["stocks_processed"], 3)
        self.assertEqual(stats["generated"], 2)
        self.assertEqual(stats["skipped"], 1)
        self.assertEqual(stats["skipped_by_reason"], {"target_limit_reached": 1})


class DartReportReviewRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_manual_generation_requires_admin(self):
        request = SimpleNamespace(headers={"content-type": "application/json"})
        with patch.object(
            dart_review_route,
            "get_current_user",
            AsyncMock(return_value={"google_sub": "u1", "is_admin": False}),
        ):
            with self.assertRaises(HTTPException) as ctx:
                await dart_review_route.create_filing_review("005930", request, {"force": True})

        self.assertEqual(ctx.exception.status_code, 403)

    async def test_admin_manual_generation_calls_generator(self):
        request = SimpleNamespace(headers={"content-type": "application/json"})
        generated = {
            "stock_code": "005930",
            "rcept_no": "20260401000001",
            "report_name": "분기보고서",
            "review": {"summary_md": "요약", "cards": []},
            "cached": False,
        }
        with (
            patch.object(
                dart_review_route,
                "get_current_user",
                AsyncMock(return_value={"google_sub": "admin-sub", "is_admin": True}),
            ),
            patch.object(dart_review_route.dart_report_review, "generate_review", AsyncMock(return_value=generated)) as gen,
        ):
            result = await dart_review_route.create_filing_review("005930", request, {"force": True})

        gen.assert_awaited_once_with("005930", google_sub="admin-sub", force=True)
        self.assertEqual(result["status"], "ready")
        self.assertTrue(result["generated"])
        self.assertEqual(result["review"]["rcept_no"], "20260401000001")


if __name__ == "__main__":
    unittest.main()
