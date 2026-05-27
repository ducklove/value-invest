import unittest
from unittest.mock import AsyncMock, patch

import nps_scraper
import snapshot_nps


class NpsPublicDataTests(unittest.TestCase):
    def test_public_csv_parser_reads_official_universe(self):
        csv_text = (
            "번호,종목명,평가액(억 원),자산군 내 비중(퍼센트),지분율(퍼센트)\r\n"
            "1,삼성전자,230421,16.7,7.26\r\n"
            "5,현대차,33529,2.43,7.55\r\n"
        )
        with patch.object(
            nps_scraper,
            "_download_public_csv",
            return_value=(csv_text.encode("cp949"), "2024-12-31"),
        ):
            rows = nps_scraper.fetch_public_nps_holdings()

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1]["name"], "현대차")
        self.assertEqual(rows[1]["source"], "data.go.kr")
        self.assertEqual(rows[1]["source_market_value"], 3_352_900_000_000)
        self.assertEqual(rows[1]["ownership_pct"], 7.55)

    def test_fnguide_parser_uses_report_date_column(self):
        html = """
        <table><tr>
          <td>1</td><td>신세계</td><td>1,272,156</td><td>0</td>
          <td>0</td><td>13.47</td><td>2026.03.27</td>
        </tr></table>
        """.encode("utf-8")
        completed = type("Completed", (), {"stdout": html})()
        with patch.object(nps_scraper.subprocess, "run", return_value=completed):
            rows = nps_scraper.fetch_fnguide_nps_holdings()

        self.assertEqual(rows[0]["ownership_pct"], 13.47)
        self.assertEqual(rows[0]["total_ownership_pct"], 13.47)
        self.assertEqual(rows[0]["report_date"], "2026-03-27")


class NpsResolutionTests(unittest.IsolatedAsyncioTestCase):
    async def test_public_data_alias_resolves_hyundai_motor(self):
        holdings = [{"name": "현대차"}]
        with patch("cache.load_corp_code_table", new=AsyncMock(return_value={})), \
             patch("cache.search_corp", new=AsyncMock(return_value=[])):
            rows = await nps_scraper.resolve_stock_codes(holdings)

        self.assertEqual(rows[0]["stock_code"], "005380")

    async def test_public_data_market_value_derives_estimated_shares(self):
        holdings = [{
            "name": "현대차",
            "stock_code": "005380",
            "shares": 0,
            "source_date": "2024-12-31",
            "source_market_value": 3_352_900_000_000,
        }]

        async def fake_prices(tickers, *, since, until, fields=None):
            self.assertEqual(until, "2024-12-31")
            return {"005380": [{"date": "2024-12-30", "close": 212000}]}

        with patch.object(snapshot_nps.close_price_client, "get_daily_prices_batch", new=fake_prices):
            await snapshot_nps._derive_public_data_shares(holdings)

        self.assertEqual(holdings[0]["shares"], 15_815_566)
        self.assertEqual(holdings[0]["shares_source"], "public_value_div_year_end_close")

    async def test_snapshot_quotes_require_exact_snapshot_date(self):
        holdings = [{
            "name": "삼성전자",
            "stock_code": "005930",
            "shares": 10,
            "source_market_value": 2_000_000,
        }]

        async def fake_prices(tickers, *, since, until, fields=None):
            self.assertEqual(until, "2026-05-26")
            return {"005930": [{"date": "2026-05-22", "close": 200000}]}

        with patch.object(snapshot_nps.close_price_client, "get_daily_prices_batch", new=fake_prices):
            enriched = await snapshot_nps._fetch_quotes_for_holdings(holdings, "2026-05-26")

        self.assertIsNone(enriched[0]["price"])
        self.assertIsNone(enriched[0]["market_value"])
        self.assertFalse(enriched[0]["_has_exact_price"])
        self.assertEqual(enriched[0]["_price_date"], "2026-05-22")

    async def test_snapshot_price_coverage_rejects_stale_top_holdings(self):
        holdings = [
            {
                "name": f"Top {i}",
                "stock_code": f"00000{i}",
                "shares": 10,
                "source_market_value": 1000 - i,
                "_has_exact_price": i >= 3,
                "_price_date": "2026-05-22" if i < 3 else "2026-05-26",
                "market_value": 1000 if i >= 3 else None,
            }
            for i in range(20)
        ]

        with self.assertRaises(snapshot_nps.NpsSnapshotIncomplete):
            snapshot_nps._validate_price_coverage(holdings, "2026-05-26")
