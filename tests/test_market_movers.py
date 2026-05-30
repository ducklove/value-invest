import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import market_movers
from routes import stocks as stocks_route

FIXTURES = Path(__file__).resolve().parent / "fixtures"


class MarketMoversParserTests(unittest.TestCase):
    def _market_sum_html(self) -> str:
        return (FIXTURES / "naver_sise_market_sum.html").read_text(encoding="utf-8")

    def test_parse_market_cap_table(self):
        rows = market_movers._parse_ranking_table(self._market_sum_html(), "시가총액")
        self.assertEqual(len(rows), 3)
        first = rows[0]
        self.assertEqual(first["rank"], "1")
        self.assertEqual(first["code"], "005930")
        self.assertEqual(first["name"], "삼성전자")
        self.assertEqual(first["price"], "317,000")
        self.assertEqual(first["change_pct"], "+5.84%")
        self.assertEqual(first["direction"], "up")
        self.assertIn("metric", first)  # 시가총액 column surfaced
        # a falling row is detected as down
        down = [r for r in rows if r["change_pct"].startswith("-")]
        self.assertTrue(down and down[0]["direction"] == "down")

    def test_parse_without_metric_header_omits_metric(self):
        rows = market_movers._parse_ranking_table(self._market_sum_html(), None)
        self.assertTrue(rows)
        self.assertNotIn("metric", rows[0])

    def test_parse_empty_or_missing_table(self):
        self.assertEqual(market_movers._parse_ranking_table("", "시가총액"), [])
        self.assertEqual(market_movers._parse_ranking_table("<div>no table</div>", None), [])


class SectorParserTests(unittest.TestCase):
    def _sector_html(self) -> str:
        return (FIXTURES / "naver_sise_group.html").read_text(encoding="utf-8")

    def test_parse_sector_table(self):
        rows = market_movers._parse_sector_table(self._sector_html())
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["name"], "전자제품")
        self.assertEqual(rows[0]["change_pct"], "+29.19%")
        self.assertEqual(rows[0]["direction"], "up")

    def test_sector_direction_from_sign(self):
        html = (
            '<table class="type_1"><tr><th>업종명</th><th>전일대비</th></tr>'
            '<tr><td><a href="/sise/sise_group_detail.naver?no=1">철강</a></td>'
            '<td class="number">-2.50%</td></tr>'
            '<tr><td><a href="/sise/sise_group_detail.naver?no=2">은행</a></td>'
            '<td class="number">0.00%</td></tr></table>'
        )
        rows = market_movers._parse_sector_table(html)
        self.assertEqual(rows[0]["direction"], "down")
        self.assertEqual(rows[1]["direction"], "flat")

    def test_parse_empty_sector_table(self):
        self.assertEqual(market_movers._parse_sector_table(""), [])
        self.assertEqual(market_movers._parse_sector_table("<div>nope</div>"), [])


class MarketMoversEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_endpoint_returns_items_and_clamps_limit(self):
        fake = AsyncMock(return_value=[{"rank": "1", "code": "005930", "name": "삼성전자",
                                        "price": "317,000", "change_pct": "+5.84%", "direction": "up"}])
        # The endpoint does `import market_movers` internally, so patching the
        # module's fetch is what intercepts it.
        with patch.object(market_movers, "fetch_market_movers", new=fake):
            result = await stocks_route.get_market_movers(kind="market_cap", market="kospi", limit=999)
        self.assertEqual(result["kind"], "market_cap")
        self.assertEqual(result["market"], "kospi")
        self.assertEqual(result["items"][0]["code"], "005930")
        # limit clamped to <= 30
        self.assertLessEqual(fake.await_args.args[2], 30)

    async def test_sectors_endpoint_returns_items_and_clamps_limit(self):
        fake = AsyncMock(return_value=[{"name": "전자제품", "change_pct": "+29.19%", "direction": "up"}])
        with patch.object(market_movers, "fetch_sectors", new=fake):
            result = await stocks_route.get_market_sectors(limit=999)
        self.assertEqual(result["sectors"][0]["name"], "전자제품")
        self.assertLessEqual(fake.await_args.args[0], 40)


if __name__ == "__main__":
    unittest.main()
