import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

import cache
import dart_report_review


class DartReportReviewHelperTests(unittest.TestCase):
    def test_normalize_review_accepts_fenced_json(self):
        raw = """```json
        {"summary_md":"# 요약","cards":[{"label":"매출","value":"증가","tone":"good"}]}
        ```"""

        parsed = dart_report_review._normalize_review(raw)

        self.assertEqual(parsed["summary_md"], "# 요약")
        self.assertEqual(parsed["cards"][0]["tone"], "good")

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


if __name__ == "__main__":
    unittest.main()
