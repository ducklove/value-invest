import unittest

from services.dart import client as dart_client


class DartDividendParserTests(unittest.TestCase):
    def test_parse_common_stock_share_status_excludes_treasury(self):
        payload = {
            "status": "000",
            "list": [
                {
                    "se": "보통주",
                    "istc_totqy": "22,960,000",
                    "tesstk_co": "1,230,520",
                    "distb_stock_co": "21,729,480",
                    "stlm_dt": "2025-12-31",
                },
                {
                    "se": "합계",
                    "istc_totqy": "22,960,000",
                    "tesstk_co": "1,230,520",
                    "distb_stock_co": "21,729,480",
                    "stlm_dt": "2025-12-31",
                },
            ],
        }

        result = dart_client.parse_common_stock_share_status(payload)

        self.assertEqual(result["issued_shares"], 22_960_000)
        self.assertEqual(result["treasury_shares"], 1_230_520)
        self.assertEqual(result["distributed_shares"], 21_729_480)

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


class DartCorpCodesZipTests(unittest.TestCase):
    """DART 가 zip 대신 오류 본문을 줘도 시작 경로가 잡는 도메인 오류로 끝나야 한다."""

    def test_error_body_raises_external_service_error(self):
        from core.errors import ExternalServiceError

        body = b'{"status":"020","message":"\xec\x9a\x94\xec\xb2\xad \xec\xa0\x9c\xed\x95\x9c"}'
        with self.assertRaises(ExternalServiceError):
            dart_client._parse_corp_codes_zip(body)

    def test_valid_zip_parses_listed_companies_only(self):
        import io
        import zipfile

        xml = (
            "<result><list><corp_code>00126380</corp_code><corp_name>삼성전자</corp_name>"
            "<stock_code>005930</stock_code><modify_date>20260101</modify_date></list>"
            "<list><corp_code>00000001</corp_code><corp_name>비상장</corp_name>"
            "<stock_code> </stock_code><modify_date>20260101</modify_date></list></result>"
        )
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("CORPCODE.xml", xml)
        codes = dart_client._parse_corp_codes_zip(buf.getvalue())
        self.assertEqual(
            codes,
            [{"corp_code": "00126380", "corp_name": "삼성전자", "stock_code": "005930", "modify_date": "20260101"}],
        )

    def test_broken_xml_raises_external_service_error(self):
        import io
        import zipfile

        from core.errors import ExternalServiceError

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("CORPCODE.xml", "<result><list>")
        with self.assertRaises(ExternalServiceError):
            dart_client._parse_corp_codes_zip(buf.getvalue())
