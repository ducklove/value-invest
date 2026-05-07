import unittest

import dart_client


class DartDividendParserTests(unittest.TestCase):
    def test_parse_common_stock_cash_dividends_from_annual_report(self):
        payload = {
            "status": "000",
            "list": [
                {
                    "se": "주당 현금배당금(원)",
                    "stock_knd": "보통주",
                    "stlm_dt": "2025-12-31",
                    "thstrm": "400",
                    "frmtrm": "200",
                    "lwfr": "100",
                },
                {
                    "se": "주당 현금배당금(원)",
                    "stock_knd": "우선주",
                    "stlm_dt": "2025-12-31",
                    "thstrm": "450",
                },
            ],
        }

        result = dart_client.parse_dividend_per_share_by_year(payload, 2023, 2025)

        self.assertEqual(result, {2023: 100.0, 2024: 200.0, 2025: 400.0})

    def test_parse_voting_common_stock_kind_used_by_kcc(self):
        payload = {
            "status": "000",
            "list": [
                {
                    "se": "주당 현금배당금(원)",
                    "stock_knd": "의결권 있는 주식",
                    "stlm_dt": "2025-12-31",
                    "thstrm": "15,000",
                    "frmtrm": "10,000",
                    "lwfr": "8,000",
                },
                {
                    "se": "주당 현금배당금(원)",
                    "stock_knd": "의결권 없는 주식",
                    "stlm_dt": "2025-12-31",
                    "thstrm": "15,050",
                },
            ],
        }

        result = dart_client.parse_dividend_per_share_by_year(payload, 2025, 2025)

        self.assertEqual(result, {2025: 15000.0})

    def test_parse_ignores_total_dividends_paid_rows(self):
        payload = {
            "status": "000",
            "list": [
                {
                    "se": "현금배당금총액(백만원)",
                    "stock_knd": "",
                    "stlm_dt": "2025-12-31",
                    "thstrm": "73,541",
                },
            ],
        }

        self.assertEqual(dart_client.parse_dividend_per_share_by_year(payload), {})
