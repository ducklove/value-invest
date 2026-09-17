"""/api/reports 라우트의 캐시 동작.

배경: 2026-09 네이버 리서치 페이지 이전으로 수집기가 빈 목록을 돌려주던
동안 서버 캐시에 [] 가 저장됐고, 수집기를 고친 뒤에도 TTL(60분) 동안
"증권사 리포트가 없습니다" 가 유지됐다. 빈 목록은 캐시 히트로 보지 않는다.
"""
from unittest.mock import AsyncMock, patch

from repositories import cache_values
from routes import reports as reports_route
from tests._harness import TempDbMixin

FAKE_REPORT = {
    "date": "2026-09-07",
    "title": "HBM으로 매크로 우려 극복",
    "firm": "미래에셋증권",
    "firm_short": "미래에셋증권",
    "target_price": "400000",
    "recommendation": "매수",
    "summary": "요약",
    "pdf_url": "https://stock.pstatic.net/stock-research/company/56/x.pdf",
    "source_url": "https://stock.naver.com/research/company/96027",
    "pages": 0,
    "nid": "96027",
    "analyst": "",
}


class ReportsRouteCacheTests(TempDbMixin):
    async def test_empty_cached_list_is_refetched(self):
        await cache_values.save_report_list("005930", [])
        fetch = AsyncMock(return_value=[FAKE_REPORT])
        with patch("report_client.fetch_reports", fetch):
            out = await reports_route.get_reports("005930")
        fetch.assert_awaited_once_with("005930")
        self.assertFalse(out["cached"])
        self.assertEqual([r["title"] for r in out["reports"]], [FAKE_REPORT["title"]])

        # 이제 채워진 목록은 캐시로 서빙되고 수집기는 다시 호출되지 않는다.
        fetch2 = AsyncMock(return_value=[])
        with patch("report_client.fetch_reports", fetch2):
            again = await reports_route.get_reports("005930")
        fetch2.assert_not_awaited()
        self.assertTrue(again["cached"])
        self.assertEqual(len(again["reports"]), 1)

        # 최신 1건 캐시도 함께 채워진다.
        latest = await cache_values.get_latest_report("005930", 60)
        self.assertEqual(latest["nid"], "96027")

    async def test_non_empty_cache_served_without_fetch(self):
        await cache_values.save_report_list("005930", [FAKE_REPORT])
        fetch = AsyncMock(side_effect=AssertionError("should not fetch"))
        with patch("report_client.fetch_reports", fetch):
            out = await reports_route.get_reports("005930")
        self.assertTrue(out["cached"])
        self.assertEqual(out["reports"][0]["nid"], "96027")

    async def test_refresh_bypasses_cache(self):
        await cache_values.save_report_list("005930", [FAKE_REPORT])
        fetch = AsyncMock(return_value=[dict(FAKE_REPORT, title="새 리포트", nid="97000")])
        with patch("report_client.fetch_reports", fetch):
            out = await reports_route.get_reports("005930", refresh=True)
        fetch.assert_awaited_once()
        self.assertFalse(out["cached"])
        self.assertEqual(out["reports"][0]["title"], "새 리포트")

    async def test_fetch_failure_returns_empty_with_error(self):
        with patch("report_client.fetch_reports", AsyncMock(side_effect=RuntimeError("upstream down"))):
            out = await reports_route.get_reports("005930")
        self.assertEqual(out["reports"], [])
        self.assertIn("upstream down", out["error"])
