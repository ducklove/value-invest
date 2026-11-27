import io
import json
import unittest
import zipfile
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin
from fastapi import HTTPException

from repositories import corp_codes
from repositories import dart_review as dart_review_repo
from repositories import wiki as wiki_repo
from routes import dart_review as dart_review_route
from services.dart import report_review as dart_report_review


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


BLOCK_LINES = [
    '{"type":"card","label":"부채비율","value":"7.9%","tone":"good","detail":"전기말 대비 개선"}',
    '{"type":"card","label":"영업수익","value":"9,337억 원","tone":"WATCH","detail":"11.1% 감소"}',
    '{"type":"trend","label":"부채비율","unit":"%","before":{"label":"2025 말","value":"9.01%"},"after":{"label":"2026 3Q","value":7.91}}',
    '{"type":"text","md":"한눈 요약 **핵심**"}',
    '{"type":"section","title":"재무안정성·현금흐름"}',
    '{"type":"table","columns":["구분","2025 말","2026 3Q"],"rows":[["부채총계","2.5조","5.3조","넘치는 셀"],["자본총계",27.9]],"note":"단위 조 원"}',
    '{"type":"section","title":"리스크"}',
    '{"type":"bullets","items":["커머스 매출 감소","","연대보증 ▇▇▇"]}',
    '{"type":"checklist","items":["현금흐름표 확인"]}',
    '{"type":"changes","items":["부채비율 7.5% → 7.9%"]}',
    '{"type":"limits","text":"손익계산서 본문 없음"}',
]


class DartReviewBlockFormatTests(unittest.TestCase):
    """JSON Lines block output: per-block validation, derived legacy fields,
    truncation tolerance."""

    def test_json_lines_become_validated_blocks_with_legacy_fields(self):
        review = dart_report_review._normalize_review("\n".join(BLOCK_LINES))

        self.assertEqual(review["format"], dart_report_review.BLOCKS_FORMAT)
        self.assertEqual(
            [b["type"] for b in review["blocks"]],
            ["card", "card", "trend", "text", "section", "table", "section", "bullets", "checklist", "changes", "limits"],
        )
        self.assertEqual(review["dropped_blocks"], 0)
        self.assertFalse(review["truncated"])
        # tone is normalised; unknown tones fall back to neutral
        self.assertEqual([c["tone"] for c in review["cards"]], ["good", "watch"])
        self.assertNotIn("type", review["cards"][0])
        self.assertEqual(review["metric_trends"][0]["before"]["value"], 9.01)
        self.assertEqual(review["watch_items"], ["현금흐름표 확인"])
        self.assertEqual(review["comparison_notes"], ["부채비율 7.5% → 7.9%"])
        self.assertEqual(review["source_limits"], "손익계산서 본문 없음")
        table = review["blocks"][5]
        # rows are cut or padded to the column count, numbers become strings
        self.assertEqual(table["rows"], [["부채총계", "2.5조", "5.3조"], ["자본총계", "27.9", ""]])
        bullets = review["blocks"][7]
        self.assertEqual(bullets["items"], ["커머스 매출 감소", "연대보증"])
        # summary_md is derived markdown for wiki Q&A and older readers
        self.assertTrue(review["summary_md"].startswith("한눈 요약 **핵심**"))
        self.assertIn("## 재무안정성·현금흐름", review["summary_md"])
        self.assertIn("| 부채총계 | 2.5조 | 5.3조 |", review["summary_md"])
        self.assertIn("## 다음 체크포인트\n- 현금흐름표 확인", review["summary_md"])
        self.assertFalse(dart_report_review._review_payload_is_broken(review, finish_reason="stop"))

    def test_invalid_and_unknown_blocks_are_dropped_one_by_one(self):
        lines = BLOCK_LINES + [
            '{"type":"chart","spec":{}}',
            '{"type":"table","columns":["하나"],"rows":[["a"]]}',
            '{"type":"card"}',
            'not json at all',
        ]
        review = dart_report_review._normalize_review("\n".join(lines))
        self.assertEqual(len(review["blocks"]), len(BLOCK_LINES))
        self.assertEqual(review["dropped_blocks"], 3)

    def test_card_and_trend_counts_are_capped(self):
        card = '{"type":"card","label":"c","value":"v"}'
        review = dart_report_review._normalize_review("\n".join([card] * 12 + BLOCK_LINES[3:5]))
        self.assertEqual(len(review["cards"]), dart_report_review.MAX_CARDS)
        self.assertEqual(review["dropped_blocks"], 12 - dart_report_review.MAX_CARDS)

    def test_cut_off_last_line_keeps_complete_blocks(self):
        text = "\n".join(BLOCK_LINES)
        cut = text[: text.index('{"type":"checklist"') + 25]
        review = dart_report_review._normalize_review(cut)
        self.assertEqual(review["blocks"][-1]["type"], "bullets")
        self.assertEqual(review["watch_items"], [])
        # enough survived: kept, not retried
        self.assertFalse(dart_report_review._review_payload_is_broken(review, finish_reason="length"))

    def test_answer_cut_before_any_body_is_broken(self):
        text = "\n".join(BLOCK_LINES[:3]) + '\n{"type":"text","md":"한눈'
        review = dart_report_review._normalize_review(text)
        self.assertTrue(dart_report_review._review_payload_is_broken(review, finish_reason="length"))

    def test_wrapped_fenced_and_array_answers_are_accepted(self):
        blocks = [json.loads(line) for line in BLOCK_LINES]
        variants = [
            "```json\n" + json.dumps({"blocks": blocks}, ensure_ascii=False, indent=2) + "\n```",
            "[" + ",\n".join(BLOCK_LINES) + "]",
            "다음은 리뷰입니다.\n" + " ".join(BLOCK_LINES),
        ]
        for raw in variants:
            with self.subTest(raw=raw[:30]):
                review = dart_report_review._normalize_review(raw)
                self.assertEqual(len(review["blocks"]), len(BLOCK_LINES))

    def test_truncated_wrapper_still_yields_nested_blocks(self):
        blocks = [json.loads(line) for line in BLOCK_LINES]
        raw = json.dumps({"blocks": blocks}, ensure_ascii=False)
        review = dart_report_review._normalize_review(raw[: raw.index('"checklist"')])
        self.assertEqual(review["format"], dart_report_review.BLOCKS_FORMAT)
        self.assertGreaterEqual(len(review["blocks"]), 8)

    def test_json_without_any_block_is_broken_not_shown_raw(self):
        review = dart_report_review._normalize_review('{"foo": 1}\n{"type": "nope"}')
        self.assertEqual(review["blocks"], [])
        self.assertEqual(review["summary_md"], "")
        self.assertTrue(dart_report_review._review_payload_is_broken(review))

    def test_old_single_object_and_plain_markdown_answers_still_work(self):
        legacy = dart_report_review._normalize_review('{"summary_md": "# 요약", "cards": []}')
        self.assertNotIn("format", legacy)
        self.assertEqual(legacy["summary_md"], "# 요약")
        plain = dart_report_review._normalize_review("## 마크다운 답변\n- 항목")
        self.assertEqual(plain["summary_md"], "## 마크다운 답변\n- 항목")

    def test_prompt_asks_for_json_lines_in_catalog_order(self):
        prompt = dart_report_review._build_prompt(
            stock_code="005930",
            corp_name="삼성전자",
            primary={"report_name": "분기보고서", "report_date": "2026-11-14", "viewer_url": "u"},
            primary_text="원문",
            comparisons=[],
            financial_context="재무",
        )
        self.assertIn("JSON Lines", prompt)
        self.assertNotIn("summary_md", prompt)
        positions = [prompt.index(f'{{"type": "{kind}"') for kind in ("card", "trend", "text", "section", "checklist", "changes", "limits")]
        self.assertEqual(positions, sorted(positions))


class DartReportReviewTruncationTests(TempDbMixin):
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
            patch.object(corp_codes, "get_corp_code", AsyncMock(return_value="00126380")),
            patch.object(corp_codes, "get_corp_name", AsyncMock(return_value="삼성전자")),
            patch.object(dart_report_review, "fetch_periodic_filings", AsyncMock(return_value=filings)),
            patch.object(dart_report_review, "fetch_document_text", AsyncMock(return_value="원문 텍스트")),
            patch.object(dart_report_review, "_financial_context", AsyncMock(return_value="재무 컨텍스트")),
            patch.object(dart_review_repo, "save_dart_report_review", save_mock),
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

    async def test_persistent_truncation_is_remembered_per_filing(self):
        call = AsyncMock(side_effect=[self.BROKEN, self.BROKEN])
        save = AsyncMock(side_effect=lambda review: review)

        with self.assertRaises(dart_report_review.DartReviewOutputError):
            await self._run_generate(call, save)

        failure = await dart_report_review._output_failure("005930", "r1")
        self.assertEqual(failure["attempts"], 1)
        self.assertEqual(failure["finish_reason"], "length")

    async def test_successful_review_clears_remembered_failure(self):
        save = AsyncMock(side_effect=lambda review: review)
        with self.assertRaises(dart_report_review.DartReviewOutputError):
            await self._run_generate(AsyncMock(side_effect=[self.BROKEN, self.BROKEN]), save)

        await self._run_generate(AsyncMock(side_effect=[self.GOOD]), save)

        self.assertIsNone(await dart_report_review._output_failure("005930", "r1"))

    async def test_cut_off_block_review_is_cached_without_retry(self):
        text = "\n".join(BLOCK_LINES)
        partial = dart_report_review._normalize_review(text[: text.index('{"type":"checklist"') + 10])
        partial["truncated"] = True
        usage = {"model": "m", "tokens_in": 3, "tokens_out": 4, "cost_usd": 0.001, "finish_reason": "length"}
        call = AsyncMock(side_effect=[(partial, usage)])
        save = AsyncMock(side_effect=lambda review: review)

        saved = await self._run_generate(call, save)

        self.assertEqual(call.await_count, 1)
        save.assert_awaited_once()
        self.assertTrue(saved["review"]["truncated"])
        self.assertEqual(saved["review_md"], partial["summary_md"])

    async def test_call_marks_cut_off_block_answer_truncated(self):
        post = AsyncMock(return_value={
            "content": "\n".join(BLOCK_LINES),
            "model": "m",
            "input_tokens": 10,
            "output_tokens": 20,
            "cost_usd": 0.001,
            "data": {"choices": [{"finish_reason": "length"}]},
        })
        with (
            patch.object(dart_report_review.ai_config, "get_model_for_feature", AsyncMock(return_value="m")),
            patch.object(dart_report_review.ai_client, "post_chat_completion", post),
        ):
            review, usage = await dart_report_review._call_openrouter("프롬프트", google_sub=None)
        self.assertTrue(review["truncated"])
        self.assertEqual(usage["finish_reason"], "length")

    async def test_request_caps_reasoning_and_leaves_room_for_it(self):
        post = AsyncMock(return_value={
            "content": '{"summary_md": "# 리뷰"}',
            "model": "google/gemini-4-hydrogen",
            "input_tokens": 10,
            "output_tokens": 20,
            "cost_usd": 0.001,
            "data": {"choices": [{"finish_reason": "stop"}]},
        })
        with (
            patch.object(dart_report_review.ai_config, "get_model_for_feature", AsyncMock(return_value="google/gemini-4-hydrogen")),
            patch.object(dart_report_review.ai_client, "post_chat_completion", post),
        ):
            review, usage = await dart_report_review._call_openrouter("프롬프트", google_sub=None)

        payload = post.await_args.kwargs["payload"]
        self.assertEqual(payload["reasoning"], {"effort": "low", "exclude": True})
        # Reasoning tokens count against max_tokens: the cap must sit well above
        # the ~3.5k-token JSON answer, or a reasoning model is cut off every time.
        self.assertGreaterEqual(payload["max_tokens"], 8000)
        self.assertNotIn("retry_without_reasoning", post.await_args.kwargs)
        self.assertEqual(review["summary_md"], "# 리뷰")
        self.assertEqual(usage["finish_reason"], "stop")

    async def test_latest_status_treats_broken_cache_as_missing(self):
        broken_cached = {
            "review": {"summary_md": '{"summary_md": "본문", "cards": ['},
            "review_md": '{"summary_md": "본문", "cards": [',
        }
        filings = [{"rcept_no": "r1", "report_name": "분기보고서 (2026.03)", "report_date": "2026-04-01"}]
        with (
            patch.object(corp_codes, "get_corp_code", AsyncMock(return_value="00126380")),
            patch.object(corp_codes, "get_corp_name", AsyncMock(return_value="삼성전자")),
            patch.object(dart_report_review, "fetch_periodic_filings", AsyncMock(return_value=filings)),
            patch.object(dart_review_repo, "get_dart_report_review", AsyncMock(return_value=broken_cached)),
        ):
            result = await dart_report_review.latest_review_status("005930")

        self.assertEqual(result["status"], "missing")
        self.assertIsNone(result["previous_review"])

    async def test_latest_status_serves_healthy_cache_as_ready(self):
        good_cached = {"review": {"summary_md": "# 정상"}, "review_md": "# 정상"}
        filings = [{"rcept_no": "r1", "report_name": "분기보고서 (2026.03)", "report_date": "2026-04-01"}]
        with (
            patch.object(corp_codes, "get_corp_code", AsyncMock(return_value="00126380")),
            patch.object(corp_codes, "get_corp_name", AsyncMock(return_value="삼성전자")),
            patch.object(dart_report_review, "fetch_periodic_filings", AsyncMock(return_value=filings)),
            patch.object(dart_review_repo, "get_dart_report_review", AsyncMock(return_value=good_cached)),
        ):
            result = await dart_report_review.latest_review_status("005930")

        self.assertEqual(result["status"], "ready")
        self.assertTrue(result["review"]["cached"])


class DartReportReviewCacheTests(TempDbMixin):
    async def test_save_and_get_dart_report_review_roundtrips_json(self):
        saved = await dart_review_repo.save_dart_report_review({
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

        loaded = await dart_review_repo.get_dart_report_review("005930", "20260401000001")

        self.assertEqual(saved["review"]["summary_md"], "# 리뷰")
        self.assertEqual(loaded["comparison_reports"][0]["rcept_no"], "20251114000001")
        self.assertEqual(loaded["model"], "deepseek/deepseek-v4-flash")

    async def test_block_review_grounds_wiki_qa_like_the_old_format(self):
        from routes import wiki as wiki_route

        review = dart_report_review._normalize_review("\n".join(BLOCK_LINES))
        await dart_review_repo.save_dart_report_review({
            "stock_code": "005930",
            "rcept_no": "20261114000001",
            "report_name": "분기보고서 (2026.09)",
            "report_date": "2026-11-14",
            "review": review,
            "review_md": review["summary_md"],
        })

        loaded = await dart_review_repo.get_dart_report_review("005930")
        self.assertEqual(loaded["review"]["blocks"], review["blocks"])
        self.assertFalse(dart_report_review._cached_review_is_broken(loaded))

        context = await wiki_route._load_dart_review_context("005930")
        self.assertIn("- [good] 부채비율: 7.9%", context)
        self.assertIn("| 부채총계 | 2.5조 | 5.3조 |", context)
        self.assertIn("확인할 항목:\n- 현금흐름표 확인", context)
        self.assertIn("원문 범위/한계: 손익계산서 본문 없음", context)


class DartReportReviewPipelineTests(TempDbMixin):
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
            patch.object(wiki_repo, "select_wiki_target_stocks", AsyncMock(return_value=["005930", "000660", "000660"])),
            patch.object(corp_codes, "get_corp_code", AsyncMock(return_value="00126380")),
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
            patch.object(corp_codes, "get_corp_code", AsyncMock(return_value="00126380")),
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


class DartReportReviewFailureBackoffTests(TempDbMixin):
    """A filing whose review output failed must not be re-billed on every run."""

    def _patches(self, generate):
        async def fake_status(stock_code):
            return {"status": "missing", "latest_report": {"rcept_no": "r1"}}

        return (
            patch.object(corp_codes, "get_corp_code", AsyncMock(return_value="00126380")),
            patch.object(dart_report_review, "latest_review_status", side_effect=fake_status),
            patch.object(dart_report_review, "generate_review", generate),
        )

    async def _run(self, generate, **kwargs):
        import contextlib
        with contextlib.ExitStack() as stack:
            for p in self._patches(generate):
                stack.enter_context(p)
            return await dart_report_review.run_pipeline(stock_codes=["005930"], **kwargs)

    async def test_pipeline_skips_filing_during_cooldown(self):
        await dart_report_review._record_output_failure("005930", "r1", {"finish_reason": "length"})
        generate = AsyncMock()

        stats = await self._run(generate)

        generate.assert_not_awaited()
        self.assertEqual(stats["skipped_by_reason"], {"output_failed_cooldown": 1})
        self.assertEqual(stats["failed"], 0)

    async def test_pipeline_retries_once_cooldown_has_passed(self):
        await dart_report_review._record_output_failure("005930", "r1", {"finish_reason": "length"})
        generate = AsyncMock(return_value={"stock_code": "005930", "rcept_no": "r1", "model": "m"})

        with patch.object(dart_report_review, "DART_REVIEW_FAILURE_COOLDOWN_S", 0.0):
            stats = await self._run(generate)

        generate.assert_awaited_once()
        self.assertEqual(stats["generated"], 1)

    async def test_pipeline_gives_up_after_max_failed_attempts(self):
        for _ in range(dart_report_review.DART_REVIEW_MAX_FAILED_ATTEMPTS):
            await dart_report_review._record_output_failure("005930", "r1", {"finish_reason": "length"})
        generate = AsyncMock()

        with patch.object(dart_report_review, "DART_REVIEW_FAILURE_COOLDOWN_S", 0.0):
            stats = await self._run(generate)

        generate.assert_not_awaited()
        self.assertEqual(stats["skipped_by_reason"], {"output_failed_max_attempts": 1})

    async def test_forced_run_ignores_remembered_failure(self):
        await dart_report_review._record_output_failure("005930", "r1", {"finish_reason": "length"})
        generate = AsyncMock(return_value={"stock_code": "005930", "rcept_no": "r1", "model": "m"})

        stats = await self._run(generate, force=True)

        generate.assert_awaited_once()
        self.assertEqual(stats["generated"], 1)

    async def test_truncated_output_is_reported_with_its_own_reason(self):
        generate = AsyncMock(side_effect=dart_report_review.DartReviewOutputError("AI 응답이 잘려"))

        stats = await self._run(generate)

        self.assertEqual(stats["failed_by_reason"], {"output_truncated": 1})

    async def test_other_filing_of_same_stock_is_not_blocked(self):
        await dart_report_review._record_output_failure("005930", "older-rcept", {"finish_reason": "length"})
        generate = AsyncMock(return_value={"stock_code": "005930", "rcept_no": "r1", "model": "m"})

        stats = await self._run(generate)

        generate.assert_awaited_once()
        self.assertEqual(stats["generated"], 1)


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
