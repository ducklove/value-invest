"""Wiki ingestion unit tests.

Uses the same isolated-DB fixture pattern as test_portfolio.py. All LLM
and HTTP calls are monkeypatched — no network."""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import cache
import wiki_ingestion


SAMPLE_PDF_BYTES = b"%PDF-1.4\n%stub\n1 0 obj <<>> endobj\n%%EOF\n"


class WikiIngestionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "cache.db"
        self.db_patch = patch.object(cache, "DB_PATH", self.db_path)
        self.db_patch.start()
        # Redirect the PDF cache dir to a temp location so tests don't
        # write into the repo.
        self.pdf_dir = Path(self.temp_dir.name) / "pdf_cache"
        self.pdf_dir_patch = patch.object(wiki_ingestion, "PDF_CACHE_DIR", self.pdf_dir)
        self.pdf_dir_patch.start()
        await cache.close_db()
        await cache.init_db()

    async def asyncTearDown(self):
        await cache.close_db()
        self.db_patch.stop()
        self.pdf_dir_patch.stop()
        self.temp_dir.cleanup()

    # -- low-level helpers --

    def test_sha1_hex_stable(self):
        h1 = wiki_ingestion._sha1_hex(b"hello")
        h2 = wiki_ingestion._sha1_hex(b"hello")
        self.assertEqual(h1, h2)
        self.assertEqual(len(h1), 40)

    def test_parse_target_price(self):
        p = wiki_ingestion._parse_target_price
        self.assertEqual(p("50,000"), 50000.0)
        self.assertEqual(p(50000), 50000.0)
        self.assertIsNone(p(""))
        self.assertIsNone(p("없음"))
        self.assertIsNone(p(None))

    def test_split_summary_sections_extracts_bullets(self):
        md = (
            "### 핵심 요약\n본문내용\n\n"
            "### 주요 포인트\n- a\n- b\n- c\n\n"
            "### 밸류에이션 / 목표가 논거\n논거\n"
        )
        full, bullets = wiki_ingestion._split_summary_sections(md)
        self.assertEqual(full, md.strip())
        self.assertIn("- a", bullets)
        self.assertIn("- c", bullets)
        self.assertNotIn("본문내용", bullets)

    def test_build_summary_prompt_includes_metadata(self):
        meta = {
            "stock_code": "005930",
            "stock_name": "삼성전자",
            "firm": "삼성증권",
            "date": "2026-03-10",
            "report_date": "2026-03-10",
            "recommendation": "Buy",
            "target_price": "90,000",
        }
        prompt = wiki_ingestion.build_summary_prompt(meta, "본문 예시 텍스트")
        self.assertIn("삼성전자", prompt)
        self.assertIn("005930", prompt)
        self.assertIn("삼성증권", prompt)
        self.assertIn("Buy", prompt)
        self.assertIn("본문 예시 텍스트", prompt)

    def test_build_summary_prompt_handles_empty_body(self):
        prompt = wiki_ingestion.build_summary_prompt({"stock_code": "X"}, "")
        self.assertIn("본문 텍스트가 비어", prompt)

    def test_build_summary_prompt_truncates_long_body(self):
        long_body = "가" * 50000
        prompt = wiki_ingestion.build_summary_prompt({"stock_code": "X"}, long_body)
        # Body is capped at MAX_PDF_CHARS, so prompt can't contain the
        # whole thing.
        self.assertLess(prompt.count("가"), 50000)
        self.assertGreater(prompt.count("가"), 1000)

    # -- target-stock selection --

    async def test_select_target_stocks_union(self):
        db = await cache.get_db()
        await db.execute(
            "INSERT INTO users (google_sub, email, name, picture, email_verified, created_at, last_login_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("u1", "u1@e.com", "U", "", 1, "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
        )
        # user_recent_analyses has FK → analysis_meta(stock_code); seed it.
        for code in ("035420", "999999"):
            await db.execute(
                "INSERT INTO analysis_meta (stock_code, corp_name, analyzed_at, payload_json) VALUES (?, ?, ?, ?)",
                (code, "C", "2026-01-01T00:00:00", "{}"),
            )
        await db.execute(
            "INSERT INTO user_portfolio (google_sub, stock_code, stock_name, quantity, avg_price, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("u1", "005930", "삼성전자", 10, 70000, "2026-01-01", "2026-01-01"),
        )
        await db.execute(
            "INSERT INTO user_stock_preferences (google_sub, stock_code, is_starred, is_pinned, updated_at) VALUES (?, ?, ?, ?, ?)",
            ("u1", "000660", 1, 0, "2026-01-01"),
        )
        await db.execute(
            "INSERT INTO user_recent_analyses (google_sub, stock_code, viewed_at) VALUES (?, ?, ?)",
            ("u1", "035420", "2026-04-10T00:00:00"),
        )
        await db.execute(
            "INSERT INTO user_recent_analyses (google_sub, stock_code, viewed_at) VALUES (?, ?, ?)",
            # Should be excluded — too old.
            ("u1", "999999", "2020-01-01T00:00:00"),
        )
        await db.commit()

        codes = await cache.select_wiki_target_stocks(recent_days=30)
        self.assertIn("005930", codes)
        self.assertIn("000660", codes)
        self.assertIn("035420", codes)
        self.assertNotIn("999999", codes)

    # -- dedup by sha1 --

    async def test_pdf_cache_keyed_by_sha1(self):
        row = {
            "pdf_sha1": "deadbeef" * 5,
            "stock_code": "005930",
            "pdf_url": "https://stock.pstatic.net/stock-research/abc.pdf",
            "file_path": "data/pdf_cache/deadbeef.pdf",
            "file_bytes": 100,
            "parsed_text": "hello",
            "parse_status": "parsed",
            "parse_error": None,
            "downloaded_at": "2026-04-17T00:00:00",
            "parsed_at": "2026-04-17T00:00:00",
        }
        await cache.save_pdf_cache_row(row)
        # Upsert with different URL but same sha1 → still one row.
        row2 = dict(row, pdf_url="https://stock.pstatic.net/stock-research/different.pdf")
        await cache.save_pdf_cache_row(row2)
        db = await cache.get_db()
        cursor = await db.execute("SELECT COUNT(*) AS n, pdf_url FROM report_pdf_cache WHERE pdf_sha1 = ?", (row["pdf_sha1"],))
        r = await cursor.fetchone()
        self.assertEqual(r["n"], 1)
        self.assertEqual(r["pdf_url"], row2["pdf_url"])  # latest wins

    # -- summarize-already-done shortcut --

    async def test_ingest_skips_already_summarized(self):
        stock = "005930"
        sha = "abcd" * 10
        # Pre-seed PDF cache + wiki entry.
        await cache.save_pdf_cache_row({
            "pdf_sha1": sha, "stock_code": stock,
            "pdf_url": "https://stock.pstatic.net/stock-research/x.pdf",
            "file_path": None, "file_bytes": 1,
            "parsed_text": "t", "parse_status": "parsed",
            "parse_error": None,
            "downloaded_at": "2026-04-17T00:00:00",
            "parsed_at": "2026-04-17T00:00:00",
        })
        await cache.save_wiki_entry({
            "stock_code": stock, "source_type": "broker_report", "source_ref": sha,
            "report_date": "2026-03-10", "firm": "F", "title": "T",
            "recommendation": "Buy", "target_price": 1.0,
            "summary_md": "# s", "key_points_md": "- a",
            "model": "m", "tokens_in": 100, "tokens_out": 50,
            "created_at": "2026-04-17T00:00:00",
        })

        download_mock = AsyncMock(return_value=SAMPLE_PDF_BYTES)
        summarize_mock = AsyncMock()
        with patch.object(wiki_ingestion, "download_pdf", download_mock), \
             patch.object(wiki_ingestion, "summarize_report", summarize_mock):
            # Force the sha1 match so the skipped path triggers.
            orig_sha = wiki_ingestion._sha1_hex
            patched_sha = lambda data: sha if data == SAMPLE_PDF_BYTES else orig_sha(data)
            with patch.object(wiki_ingestion, "_sha1_hex", side_effect=patched_sha):
                result = await wiki_ingestion.ingest_pdf_for_report(stock, {
                    "pdf_url": "https://stock.pstatic.net/stock-research/x.pdf",
                    "date": "2026-03-10", "firm": "F", "title": "T",
                    "recommendation": "Buy", "target_price": "1",
                })
        self.assertEqual(result.get("skipped"), "already_summarized")
        summarize_mock.assert_not_awaited()

    # -- full happy path with all externals mocked --

    async def test_ingest_happy_path(self):
        stock = "005930"
        report = {
            "pdf_url": "https://stock.pstatic.net/stock-research/xyz.pdf",
            "date": "2026-04-01", "firm": "미래에셋", "title": "Q1 리뷰",
            "recommendation": "Buy", "target_price": "95,000",
            "stock_name": "삼성전자",
        }
        fake_summary = {
            "summary_md": "### 핵심 요약\n본문\n### 주요 포인트\n- a\n- b\n",
            "key_points_md": "- a\n- b",
            "tokens_in": 500, "tokens_out": 120, "model": "qwen/qwen-plus",
        }
        with patch.object(wiki_ingestion, "download_pdf", AsyncMock(return_value=SAMPLE_PDF_BYTES)), \
             patch.object(wiki_ingestion, "parse_pdf_bytes", return_value="본문 추출 성공"), \
             patch.object(wiki_ingestion, "summarize_report", AsyncMock(return_value=fake_summary)):
            result = await wiki_ingestion.ingest_pdf_for_report(stock, report)
        self.assertTrue(result.get("ok"), msg=f"unexpected result: {result}")

        entries = await cache.get_wiki_entries(stock, limit=10)
        self.assertEqual(len(entries), 1)
        e = entries[0]
        self.assertEqual(e["stock_code"], stock)
        self.assertEqual(e["firm"], "미래에셋")
        self.assertEqual(e["target_price"], 95000.0)
        self.assertIn("핵심 요약", e["summary_md"])

    async def test_ingest_marks_download_failure(self):
        stock = "005930"
        report = {"pdf_url": "https://stock.pstatic.net/stock-research/a.pdf"}
        with patch.object(wiki_ingestion, "download_pdf", AsyncMock(side_effect=RuntimeError("boom"))):
            result = await wiki_ingestion.ingest_pdf_for_report(stock, report)
        self.assertEqual(result.get("failed"), "download")
        db = await cache.get_db()
        cursor = await db.execute("SELECT parse_status FROM report_pdf_cache WHERE stock_code = ?", (stock,))
        rows = await cursor.fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["parse_status"], "download_failed")

    async def test_ingest_rejects_non_whitelisted_url(self):
        result = await wiki_ingestion.ingest_pdf_for_report(
            "005930",
            {"pdf_url": "https://evil.example.com/x.pdf"},
        )
        # Either skipped (wrong host → no_pdf_url via whitelist) or
        # failed, but must not have summarized anything.
        self.assertNotIn("ok", result)

    # -- FTS search --

    async def test_fts_search_finds_seeded_entry(self):
        stock = "005930"
        await cache.save_wiki_entry({
            "stock_code": stock, "source_type": "broker_report", "source_ref": "sha1",
            "report_date": "2026-03-10", "firm": "F", "title": "HBM 수혜주 전망",
            "recommendation": "Buy", "target_price": 90000.0,
            "summary_md": "HBM 출하 증가로 2H26 업황 반등이 예상된다.",
            "key_points_md": "- HBM 출하 증가\n- 메모리 업황 반등",
            "model": "m", "tokens_in": 1, "tokens_out": 1,
            "created_at": "2026-04-17T00:00:00",
        })
        # FTS query containing a keyword present in summary/key_points.
        rows = await cache.search_wiki(stock, "HBM", limit=3)
        self.assertEqual(len(rows), 1)
        self.assertIn("HBM", rows[0]["summary_md"])

    async def test_search_wiki_fallback_when_fts_empty(self):
        stock = "005930"
        await cache.save_wiki_entry({
            "stock_code": stock, "source_type": "broker_report", "source_ref": "sha1",
            "report_date": "2026-03-10", "firm": "F", "title": "T",
            "recommendation": "Buy", "target_price": 90000.0,
            "summary_md": "본문",
            "key_points_md": "- point",
            "model": "m", "tokens_in": 1, "tokens_out": 1,
            "created_at": "2026-04-17T00:00:00",
        })
        # Query with no matching tokens → FTS returns 0 → fallback to recency.
        rows = await cache.search_wiki(stock, "nonexistentgibberishzzz", limit=3)
        self.assertGreaterEqual(len(rows), 1)


if __name__ == "__main__":
    unittest.main()
