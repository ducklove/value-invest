"""Broker forecasts must come from marked annual per-share rows, not ratios."""

import asyncio
import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from services import research_valuation as rv

TODAY = date(2026, 10, 28)

# Numeric excerpts from real Naver-distributed broker PDFs checked 2026-10-28.
# DS: https://stock.pstatic.net/stock-research/company/66/20261012_company_563668000.pdf
# Same report has one table on each half of the financial-statement page.
DS_EXCERPT = """삼성전자 005930
(십억원) 2023 2024 2025 2026F 2027F
EPS(원) 2,225 5,433 7,065 53,991 91,008
주요투자지표 (원, 배)
2023 2024 2025 2026F 2027F 2023 2024 2025 2026F 2027F
주당지표(원)
현금의 증가 19,400 -15,375 16,179 250,634 403,665 EPS 2,225 5,433 7,065 53,991 91,008
기초현금 49,681 69,081 53,706 69,885 320,519 BPS 52,002 57,663 60,317 109,052 176,130
"""

# Hana's consensus rows deliberately differ from the broker's own forecasts.
HANA_EXCERPT = """삼성전자 (005930)
Consensus Data
2026 2027
EPS(원) 45,340 67,565
BPS(원) 108,719 167,470
Financial Data (십억원, %, 배, 원)
투자지표 2024 2025 2026F 2027F
EPS 4,950 6,564 44,558 62,991
증감율 132.29 32.61 578.82 41.37
PER 10.75 18.27 6.03 4.26
PBR 0.92 1.87 2.37 1.54
EV/EBITDA 3.60 7.67 3.37 1.73
ROE 9.03 10.85 51.10 44.41
BPS 57,930 63,976 113,340 174,682
투자지표 현금흐름표 (단위:십억원)
2024 2025 2026F 2027F 2028F 2024 2025 2026F 2027F 2028F
주당지표(원)
EPS 4,950 6,564 44,558 62,991 104,792 당기순이익 34,451.4 45,206.8 298,596.1 420,888.8 700,194.6
BPS 57,930 63,976 113,340 174,682 277,824 조정 40,098.8 49,722.2 92,966.3 41,437.5 33,817.5
"""


class ExtractForwardInputsTests(unittest.TestCase):
    def test_real_ds_report_uses_annual_forecast_and_twin_header(self):
        result = rv.extract_forward_inputs(DS_EXCERPT, today=TODAY)
        self.assertEqual(result, {"fiscal_year": 2026, "period": "2026E", "eps": 53991.0, "bps": 109052.0})

    def test_real_hana_report_ignores_unmarked_consensus(self):
        result = rv.extract_forward_inputs(HANA_EXCERPT, today=TODAY)
        self.assertEqual(result["eps"], 44558)
        self.assertEqual(result["bps"], 113340)

    def test_real_eugene_report_ignores_modified_eps(self):
        text = """(단위:십억원) 2024A 2025A 2026F 2027F 2028F
EPS(원) 5,632 7,477 51,925 88,945 108,078
수정EPS(원) 5,632 7,477 50,127 86,736 105,717
2024A 2025A 2026F 2027F 2028F
주당지표(원)
EPS 5,632 7,477 51,925 88,945 108,078
BPS 57,663 62,995 108,859 174,333 253,212
"""
        result = rv.extract_forward_inputs(text, today=TODAY)
        self.assertEqual(result["eps"], 51925)
        self.assertEqual(result["bps"], 108859)

    def test_ignores_historical_and_quarterly_multiples(self):
        for text in (
            "2024 2025\nEPS(원) 100 200\nBPS(원) 1000 2000",
            "1Q26 2Q26 3Q26E 4Q26E 2026E 2027E\nEPS(원) 10 20 30 40 100 200",
            "2026E 2027E\nPER(배) 10 8\nPBR(배) 1.5 1.4",
            "2025/12 2026/12E\nEPS(원) 100 200",
        ):
            with self.subTest(text=text):
                self.assertIsNone(rv.extract_forward_inputs(text, today=TODAY))

    def test_missing_or_negative_eps_keeps_supported_bps(self):
        for eps in ("-", "(100)", "-100", "0", "N/A"):
            result = rv.extract_forward_inputs(f"2025A 2026E\nEPS(원) 50 {eps}\nBPS(원) 1000 1200", today=TODAY)
            self.assertIsNone(result["eps"])
            self.assertEqual(result["bps"], 1200)

    def test_rejects_column_mismatch_conflicts_and_growth_rates(self):
        self.assertIsNone(rv.extract_forward_inputs("2024A 2025A 2026E\nEPS(원) 10 20", today=TODAY))
        self.assertIsNone(rv.extract_forward_inputs("2025A 2026E\nEPS(원) 100 200\nEPS(원) 100 300", today=TODAY))
        self.assertIsNone(rv.extract_forward_inputs("2025A 2026E\n주당지표(원)\n성장성 (%)\nEPS 30 80", today=TODAY))

    def test_requires_won_unit_and_does_not_accept_target_assumption(self):
        for text in ("2025A 2026E\nEPS($) 100 200", "2025A 2026E\nEPS 100 200", "(십억원) 2025A 2026E\nEPS 100 200", "2026년 EPS(원) 200, 목표 PER 10배"):
            self.assertIsNone(rv.extract_forward_inputs(text, today=TODAY))

    def test_modified_and_growth_rows_cannot_conflict_with_per_share_inputs(self):
        text = """2025A 2026E
EPS(원) 100 200
수정 EPS(원) 100 190
Adjusted EPS(원) 100 180
성장성 (%)
매출액 5 10
영업이익 20 30
당기순이익 30 50
EPS 30 80
주당지표(원)
BPS 1000 1200
"""
        result = rv.extract_forward_inputs(text, today=TODAY)
        self.assertEqual(result["eps"], 200)
        self.assertEqual(result["bps"], 1200)

    def test_rolls_period_at_new_year_and_does_not_reuse_old_year(self):
        result = rv.extract_forward_inputs(DS_EXCERPT, today=date(2027, 1, 1))
        self.assertEqual(result["period"], "2027E")
        self.assertEqual(result["eps"], 91008)
        self.assertIsNone(rv.extract_forward_inputs(DS_EXCERPT, today=date(2028, 1, 1)))


def _report(nid="96634", **updates):
    return {
        "date": "2026-10-12", "firm": "DS투자증권", "title": "삼성전자 전망",
        "pdf_url": f"https://stock.pstatic.net/stock-research/company/66/{nid}.pdf",
        "source_url": f"https://stock.naver.com/research/company/{nid}", **updates,
    }


class FetchForwardValuationTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        rv._cache.clear()

    async def test_returns_inputs_source_and_posted_date_without_report_price(self):
        client = SimpleNamespace(get=AsyncMock(return_value=SimpleNamespace(status_code=200, content=b"%PDF-stub")))
        with patch.object(rv, "_today", return_value=TODAY), \
             patch.object(rv.cache_values, "get_report_list", AsyncMock(return_value=None)), \
             patch.object(rv.report_client, "fetch_reports", AsyncMock(return_value=[_report()])), \
             patch.object(rv, "get_http_client", AsyncMock(return_value=client)), \
             patch.object(rv, "_pdf_text", return_value=DS_EXCERPT):
            result = await rv.fetch_forward_valuation("005930")
        self.assertEqual(result["eps"], 53991)
        self.assertEqual(result["source"], "DS투자증권")
        self.assertEqual(result["as_of"], "2026-10-12")
        self.assertEqual(result["as_of_basis"], "report_posted_date")
        self.assertEqual(result["basis"], "broker_report_fiscal_year")
        self.assertNotIn("per", result)
        self.assertNotIn("price", result)

    async def test_reuses_report_list_and_skips_stale_unsafe_or_other_share_class_pdf(self):
        reports = [_report("1", date="2025-12-01"), _report("2", pdf_url="https://example.com/x.pdf"), _report("3")]
        client = SimpleNamespace(get=AsyncMock(return_value=SimpleNamespace(status_code=200, content=b"%PDF-stub")))
        fetch = AsyncMock()
        with patch.object(rv, "_today", return_value=TODAY), \
             patch.object(rv.cache_values, "get_report_list", AsyncMock(return_value={"reports": reports})), \
             patch.object(rv.report_client, "fetch_reports", fetch), \
             patch.object(rv, "get_http_client", AsyncMock(return_value=client)), \
             patch.object(rv, "_pdf_text", return_value=DS_EXCERPT):
            self.assertIsNone(await rv.fetch_forward_valuation("005935"))
        fetch.assert_not_awaited()
        self.assertEqual(client.get.await_count, 1)

    async def test_prefers_newest_supported_report_and_supports_partial_metrics(self):
        old = {"eps": 100, "bps": 1000, "as_of": "2026-10-01"}
        new = {"eps": None, "bps": 1100, "as_of": "2026-10-20"}
        async def inputs(report, today, stock_code):
            return {"3": new, "2": None, "1": old}[report["nid"]]
        reports = [dict(_report("1", date="2026-10-01"), nid="1"), dict(_report("2", date="2026-10-25"), nid="2"), dict(_report("3", date="2026-10-20"), nid="3")]
        with patch.object(rv.cache_values, "get_report_list", AsyncMock(return_value={"reports": reports})), \
             patch.object(rv, "_report_inputs", side_effect=inputs):
            self.assertEqual(await rv._load("005930", TODAY), new)

    async def test_failure_and_no_data_are_cached_and_parallel_loads_share_one_call(self):
        loader = AsyncMock(return_value=None)
        with patch.object(rv, "_today", return_value=TODAY), patch.object(rv, "_load", loader):
            self.assertEqual(await asyncio.gather(rv.fetch_forward_valuation("005930"), rv.fetch_forward_valuation("005930")), [None, None])
            self.assertIsNone(await rv.fetch_forward_valuation("005930"))
        self.assertEqual(loader.await_count, 1)
        rv._cache.clear()
        with patch.object(rv, "_load", AsyncMock(side_effect=RuntimeError("offline"))):
            self.assertIsNone(await rv.fetch_forward_valuation("005930"))

    async def test_rejects_non_domestic_code_before_network(self):
        loader = AsyncMock()
        with patch.object(rv, "_load", loader):
            for code in ("GOOGL", "", "../../foo", "005930.KS"):
                self.assertIsNone(await rv.fetch_forward_valuation(code))
        loader.assert_not_awaited()

    async def test_bad_pdf_and_invalid_or_future_dates_fail_closed(self):
        client = SimpleNamespace(get=AsyncMock(return_value=SimpleNamespace(status_code=200, content=b"<html>no PDF</html>")))
        with patch.object(rv, "get_http_client", AsyncMock(return_value=client)), \
             patch.object(rv, "_pdf_text") as parse:
            for report in (_report(), _report(date="bad"), _report(date="2026-12-01")):
                self.assertIsNone(await rv._report_inputs(report, TODAY, "005930"))
        self.assertEqual(client.get.await_count, 1)
        parse.assert_not_called()

    async def test_rejects_another_issuers_forecast_inside_sector_report(self):
        text = "삼성전자 (005930) 산업 리포트\nSK하이닉스 (000660)\n2025A 2026E\n주당지표(원)\nEPS 10000 62000\nBPS 100000 180000"
        client = SimpleNamespace(get=AsyncMock(return_value=SimpleNamespace(status_code=200, content=b"%PDF-stub")))
        with patch.object(rv, "get_http_client", AsyncMock(return_value=client)), patch.object(rv, "_pdf_text", return_value=text):
            self.assertIsNone(await rv._report_inputs(_report(), TODAY, "005930"))
        self.assertEqual(rv._issuer_codes("삼성전자 (005930)\n현재주가 268500\nEPS 100000\nBPS 180000"), {"005930"})

    async def test_accepts_domestic_alphanumeric_preferred_code(self):
        loader = AsyncMock(return_value={"eps": 100, "period": "2026E"})
        with patch.object(rv, "_load", loader):
            result = await rv.fetch_forward_valuation("00088K")
        self.assertEqual(result["eps"], 100)
        self.assertEqual(loader.await_args.args[0], "00088K")
