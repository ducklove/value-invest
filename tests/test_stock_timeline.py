import unittest
from unittest.mock import AsyncMock, patch

import httpx

from services.dart import client as dart_client
from services.market import stock_timeline as tl


class StockTimelineRowsTests(unittest.TestCase):
    def test_news_rows_split_datetime_and_dedupe(self):
        rows = tl.news_rows([
            {"title": "A", "outlet": "한경", "published_at": "2026.11.28 09:31", "url": "https://n/1"},
            {"title": "A 중복", "outlet": "한경", "published_at": "2026.11.28 09:31", "url": "https://n/1"},
            {"title": "B", "outlet": "", "published_at": "", "url": ""},
        ])
        self.assertEqual([r["title"] for r in rows], ["A", "B"])
        self.assertEqual((rows[0]["date"], rows[0]["time"]), ("2026-11-28", "09:31"))
        self.assertEqual(rows[1]["date"], "")

    def test_disclosure_rows_have_dart_link_and_date_only(self):
        rows = tl.disclosure_rows([
            {"rcept_no": "20261127000123", "report_nm": "자기주식취득결정", "rcept_dt": "20261127", "flr_nm": "삼성전자"},
            {"rcept_no": "", "report_nm": "빈 접수번호"},
        ])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["date"], "2026-11-27")
        self.assertEqual(rows[0]["time"], "")
        self.assertTrue(rows[0]["url"].endswith("rcpNo=20261127000123"))

    def test_merge_orders_by_date_then_time_with_dateless_last(self):
        news = [{"date": "2026-11-27", "time": "15:00", "title": "n1"}, {"date": "2026-11-28", "time": "09:00", "title": "n2"}]
        disc = [{"date": "2026-11-28", "time": "", "title": "d1"}, {"date": "", "time": "", "title": "x"}]
        self.assertEqual([r["title"] for r in tl.merge_rows(news, disc, limit=10)], ["n2", "d1", "n1", "x"])
        self.assertEqual(len(tl.merge_rows(news, disc, limit=2)), 2)


class StockTimelineSourceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        tl._news_cache.clear()
        tl._disc_cache.clear()

    async def test_disclosures_use_nonessential_budget(self):
        fetch = AsyncMock(return_value=[{"rcept_no": "20261127000001", "report_nm": "공시", "rcept_dt": "20261127"}])
        with patch.object(dart_client, "api_key", return_value="k"), \
             patch.object(tl.corp_codes, "get_corp_code", new=AsyncMock(return_value="00126380")), \
             patch.object(dart_client, "fetch_filing_list", new=fetch):
            src = await tl._disclosure_source("005930")
            again = await tl._disclosure_source("005930")
        self.assertEqual(src["status"], "ok")
        self.assertIs(fetch.await_args.kwargs["essential"], False)
        self.assertEqual(fetch.await_count, 1, "두 번째는 캐시")
        self.assertEqual(again["items"], src["items"])

    async def test_quota_and_errors_are_not_reported_as_empty(self):
        with patch.object(dart_client, "api_key", return_value="k"), \
             patch.object(tl.corp_codes, "get_corp_code", new=AsyncMock(return_value="00126380")), \
             patch.object(dart_client, "fetch_filing_list", new=AsyncMock(side_effect=dart_client.DartQuotaError("budget"))):
            self.assertEqual((await tl._disclosure_source("005930"))["status"], "quota")
        with patch.object(dart_client, "api_key", return_value="k"), \
             patch.object(tl.corp_codes, "get_corp_code", new=AsyncMock(return_value="00126380")), \
             patch.object(dart_client, "fetch_filing_list", new=AsyncMock(side_effect=httpx.ConnectError("down"))):
            self.assertEqual((await tl._disclosure_source("005930"))["status"], "error")
        with patch.object(tl, "_load_news", new=AsyncMock(side_effect=httpx.ConnectError("down"))):
            self.assertEqual((await tl._news_source("005930"))["status"], "error")

    async def test_one_source_failing_keeps_the_other(self):
        news = AsyncMock(return_value=[{"type": "news", "id": "u", "date": "2026-11-28", "time": "10:00", "title": "기사", "source": "", "url": "https://n/u"}])
        with patch.object(tl, "_load_news", new=news), \
             patch.object(tl, "_disclosure_source", new=AsyncMock(return_value={"status": "error", "items": [], "message": "x"})):
            data = await tl.get_stock_timeline("005930")
        self.assertEqual([r["title"] for r in data["items"]], ["기사"])
        self.assertEqual(data["sources"]["disclosure"]["status"], "error")
        self.assertEqual(data["sources"]["news"]["count"], 1)
        self.assertNotIn("items", data["sources"]["news"])

    async def test_limit_applies_per_source_so_filings_survive_busy_news_days(self):
        news = [{"type": "news", "id": str(i), "date": "2026-11-28", "time": f"{i:02d}:00", "title": f"n{i}", "source": "", "url": ""} for i in range(20)]
        disc = [{"type": "disclosure", "id": "d", "date": "2026-11-20", "time": "", "title": "공시", "source": "DART", "url": ""}]
        with patch.object(tl, "_news_source", new=AsyncMock(return_value={"status": "ok", "items": news})), \
             patch.object(tl, "_disclosure_source", new=AsyncMock(return_value={"status": "ok", "items": disc})):
            data = await tl.get_stock_timeline("005930", 5)
        types = [r["type"] for r in data["items"]]
        self.assertEqual(types.count("news"), 5)
        self.assertEqual(types.count("disclosure"), 1)

    async def test_unsupported_codes_skip_upstream(self):
        with patch.object(tl, "_load_news", new=AsyncMock()) as news:
            data = await tl.get_stock_timeline("AAPL")
        news.assert_not_awaited()
        self.assertEqual(data["sources"]["news"]["status"], "unsupported")
