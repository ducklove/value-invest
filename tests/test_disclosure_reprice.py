"""공시 → 목표가 재가격(Pro-forma) & 주가 반응 시차 단위·통합 테스트."""

from __future__ import annotations

import asyncio
import io
import json
import unittest
import zipfile
import zlib
from datetime import date
from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin, seed_corp_codes, seed_user
from fastapi import FastAPI
from fastapi.testclient import TestClient

from repositories import cache_values
from repositories import db as db_repo
from repositories import notifications as notifications_repo
from routes import stocks as stocks_routes
from services.dart import client as dart_client
from services.market import stock_timeline
from services.notifications import channels, engine
from services.portfolio import disclosure_reprice as dr
from services.portfolio import valuation as valuation_service

AS_OF = date(2026, 11, 25)


def _make_zip(xml_body: str, filename: str = "20261120000123.xml") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(filename, xml_body.encode("utf-8"))
    return buf.getvalue()


def _make_multi_zip(entries: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, data in entries.items():
            zf.writestr(name, data)
    return buf.getvalue()


RETIREMENT_XML = """<?xml version="1.0" encoding="utf-8"?>
<DOCUMENT>
<BODY>
<TABLE>
  <TBODY>
    <TR>
      <TD ROWSPAN="2">1. 소각할 주식의 수</TD>
      <TD>보통주식</TD>
      <TD>620,000 (주1)</TD>
    </TR>
    <TR>
      <TD>종류주식</TD>
      <TD>-</TD>
    </TR>
    <TR>
      <TD ROWSPAN="2">2. 발행주식총수</TD>
      <TD>보통주식</TD>
      <TD>10,000,000</TD>
    </TR>
    <TR>
      <TD>종류주식</TD>
      <TD>-</TD>
    </TR>
    <TR>
      <TD COLSPAN="2">4. 소각예정금액(원)</TD>
      <TD>48,360,000,000</TD>
    </TR>
    <TR>
      <TD COLSPAN="2">6. 소각할 주식의 취득방법</TD>
      <TD>기취득 자기주식</TD>
    </TR>
    <TR>
      <TD COLSPAN="2">7. 소각예정일</TD>
      <TD>2026년 12월 10일</TD>
    </TR>
  </TBODY>
</TABLE>
</BODY>
</DOCUMENT>
"""

AMENDED_RETIREMENT_XML = """
<html><body>
<table>
  <tr><th>항목</th><th>정정전</th><th>정정후</th></tr>
  <tr><td>1. 소각할 주식의 수 (보통주식)</td><td>500,000</td><td>620,000</td></tr>
</table>
""" + RETIREMENT_XML + "</body></html>"

PREFERRED_ONLY_RETIREMENT_XML = RETIREMENT_XML.replace(
    "<TD>보통주식</TD>\n      <TD>620,000 (주1)</TD>",
    "<TD>보통주식</TD>\n      <TD>-</TD>",
).replace(
    "<TD>종류주식</TD>\n      <TD>-</TD>",
    "<TD>종류주식</TD>\n      <TD>100,000</TD>",
    1,
)

DIVIDEND_XML = """<?xml version="1.0" encoding="utf-8"?>
<DOCUMENT>
<BODY>
<TABLE>
  <TBODY>
    <TR><TD COLSPAN="2">1. 배당구분</TD><TD>결산배당</TD></TR>
    <TR><TD COLSPAN="2">2. 배당종류</TD><TD>현금배당</TD></TR>
    <TR>
      <TD ROWSPAN="3">3. 1주당 배당금(원)</TD>
      <TD>보통주식</TD>
      <TD>1,500</TD>
    </TR>
    <TR>
      <TD>종류주식</TD>
      <TD>1,550</TD>
    </TR>
    <TR>
      <TD>차등배당 여부</TD>
      <TD>미해당</TD>
    </TR>
    <TR>
      <TD ROWSPAN="2">4. 시가배당율(%)</TD>
      <TD>보통주식</TD>
      <TD>3.2</TD>
    </TR>
    <TR>
      <TD>종류주식</TD>
      <TD>4.1</TD>
    </TR>
    <TR><TD COLSPAN="2">6. 배당기준일</TD><TD>2026-12-31</TD></TR>
  </TBODY>
</TABLE>
</BODY>
</DOCUMENT>
"""

QUARTERLY_DIVIDEND_XML = DIVIDEND_XML.replace("결산배당", "분기배당").replace("1,500", "361")
MISSING_CATEGORY_DIVIDEND_XML = DIVIDEND_XML.replace("<TR><TD COLSPAN=\"2\">1. 배당구분</TD><TD>결산배당</TD></TR>", "")

AMBIGUOUS_DIVIDEND_XML = """
<table>
  <tr><td>1. 배당구분</td><td>결산배당</td></tr>
  <tr><td>3. 1주당 배당금(원)</td><td>보통주식</td><td>일반주주 1,500 / 최대주주 1,000</td></tr>
</table>
"""


def _issued_denominator_basis() -> dict:
    """finance-pi 가 발행주식총수 분모(treasury_shares_excluded=False)로 산출한 보통주 단독 기초재무."""
    return {
        "applicable": True,
        "source": "internal_fundamentals",
        "bps": 100_000.0,
        "eps": 10_000.0,
        "bpsTreasuryExcluded": False,
        "epsTreasuryExcluded": False,
        "shareComposition": "common_only",
        "issuedShares": 10_000_000,
        "treasuryShares": 620_000,
        "outstandingShares": 9_380_000,
    }


def _outstanding_denominator_basis(treasury: int = 800_000) -> dict:
    """finance-pi 가 이미 자기주식을 차감한 유통주식수 분모(treasury_shares_excluded=True) 기초재무."""
    return {
        "applicable": True,
        "source": "internal_fundamentals",
        "bps": 106_610.0,
        "eps": 10_661.0,
        "bpsTreasuryExcluded": True,
        "epsTreasuryExcluded": True,
        "shareComposition": "common_only",
        "issuedShares": 10_000_000,
        "treasuryShares": treasury,
        "outstandingShares": 10_000_000 - treasury,
    }


class DisclosureClassificationAndParsingTests(unittest.TestCase):
    def test_classify_reprice_disclosure_matches_exact_two_types_and_excludes_subsidiaries(self):
        self.assertEqual(dr.classify_reprice_disclosure("주식소각결정"), dr.STOCK_RETIREMENT)
        self.assertEqual(dr.classify_reprice_disclosure("[기재정정]주요사항보고서(주식소각결정)"), dr.STOCK_RETIREMENT)
        self.assertEqual(dr.classify_reprice_disclosure("자기주식소각결정"), dr.STOCK_RETIREMENT)
        self.assertEqual(dr.classify_reprice_disclosure("현금ㆍ현물배당결정"), dr.CASH_DIVIDEND)
        self.assertEqual(dr.classify_reprice_disclosure("[기재정정]현금·현물배당결정"), dr.CASH_DIVIDEND)
        self.assertEqual(dr.classify_reprice_disclosure("현금배당결정"), dr.CASH_DIVIDEND)

        self.assertIsNone(dr.classify_reprice_disclosure("현금ㆍ현물배당결정(자회사의주요경영사항)"))
        self.assertIsNone(dr.classify_reprice_disclosure("주식소각결정(종속회사의주요경영사항)"))
        self.assertIsNone(dr.classify_reprice_disclosure("주식소각결정철회"))
        self.assertIsNone(dr.classify_reprice_disclosure("현금ㆍ현물배당결정예고"))
        self.assertIsNone(dr.classify_reprice_disclosure("자기주식취득결정"))

    def test_parse_stock_retirement_and_dividend_xml(self):
        parsed = dr.parse_reprice_document_zip(_make_zip(AMENDED_RETIREMENT_XML), dr.STOCK_RETIREMENT, "20261120000123")
        self.assertEqual(parsed["parse_status"], "ok")
        ext = parsed["extracted"]
        self.assertEqual(ext["retired_shares_common"], 620_000)
        self.assertIsNone(ext["retired_shares_preferred"])
        self.assertEqual(ext["issued_shares_common"], 10_000_000)
        self.assertEqual(ext["planned_amount_krw"], 48_360_000_000.0)
        self.assertEqual(ext["acquisition_method"], "기취득 자기주식")
        self.assertEqual(ext["scheduled_date"], "2026-12-10")

        div_parsed = dr.parse_reprice_document_zip(_make_zip(DIVIDEND_XML), dr.CASH_DIVIDEND, "20261120000999")
        self.assertEqual(div_parsed["parse_status"], "ok")
        self.assertEqual(div_parsed["extracted"]["dividend_category"], "결산배당")
        self.assertEqual(div_parsed["extracted"]["dps_common"], 1500.0)
        self.assertEqual(div_parsed["extracted"]["dps_preferred"], 1550.0)
        self.assertEqual(div_parsed["extracted"]["record_date"], "2026-12-31")

    def test_zip_limits_and_corrupt_or_internal_parser_errors_return_unparseable(self):
        # 1) 엔트리 수 상한(50개) 초과
        many = _make_multi_zip({f"doc_{i}.xml": b"<p>x</p>" for i in range(55)})
        self.assertEqual(dr.parse_reprice_document_zip(many, dr.STOCK_RETIREMENT)["parse_status"], "unparseable")

        # 2) 압축 해제 합계 상한 초과
        big_xml = ("<p>" + ("A" * 2048) + "</p>").encode("utf-8")
        with patch.object(dr, "MAX_ZIP_UNCOMPRESSED_BYTES", 1024):
            res_big = dr.parse_reprice_document_zip(
                _make_multi_zip({"a.xml": big_xml}), dr.STOCK_RETIREMENT
            )
        self.assertEqual(res_big["parse_status"], "unparseable")
        self.assertIn("상한", res_big["parse_message"])

        # 3) 손상된 압축 / zlib / EOF / RuntimeError / NotImplementedError 및 파서 내부 예외
        self.assertEqual(dr.parse_reprice_document_zip(b"not-a-zip", dr.STOCK_RETIREMENT)["parse_status"], "unparseable")
        for zip_exc in (zlib.error("crc"), EOFError(), RuntimeError("encrypted"), NotImplementedError("alg")):
            with patch.object(dr, "extract_xml_documents_from_zip", side_effect=zip_exc):
                out = dr.parse_reprice_document_zip(b"PK\x03\x04", dr.STOCK_RETIREMENT)
                self.assertEqual(out["parse_status"], "unparseable")

        for xml_exc in (IndexError("idx"), AttributeError("attr"), RecursionError("deep")):
            with patch.object(dr, "parse_stock_retirement_xml", side_effect=xml_exc):
                out = dr.parse_reprice_document_zip(_make_zip(RETIREMENT_XML), dr.STOCK_RETIREMENT)
                self.assertEqual(out["parse_status"], "unparseable")

        amb = dr.parse_reprice_document_zip(_make_zip(AMBIGUOUS_DIVIDEND_XML), dr.CASH_DIVIDEND)
        self.assertEqual(amb["parse_status"], "unparseable")

    def test_valuation_basis_exposes_treasury_excluded_flags_and_share_composition(self):
        common_only = {
            "fiscal_year": 2025,
            "per_share": {
                "eps_ttm": {"value": 8000, "treasury_shares_excluded": True},
                "bps": {
                    "value": 90000,
                    "treasury_shares_excluded": True,
                    "components": [
                        {"stock_kind": "보통주", "issued_shares": 1000, "treasury_shares": 100, "outstanding_shares": 900}
                    ],
                },
            },
        }
        b1 = valuation_service._basis_from_fundamental("005930", common_only)
        self.assertIs(b1["bpsTreasuryExcluded"], True)
        self.assertIs(b1["epsTreasuryExcluded"], True)
        self.assertEqual(b1["shareComposition"], "common_only")

        mixed = {
            "fiscal_year": 2025,
            "per_share": {
                "bps": {
                    "value": 90000,
                    "treasury_shares_excluded": True,
                    "components": [
                        {"stock_kind": "보통주", "issued_shares": 1000, "treasury_shares": 100, "outstanding_shares": 900},
                        {"stock_kind": "우선주", "issued_shares": 200, "treasury_shares": 0, "outstanding_shares": 200},
                    ],
                },
            },
        }
        b2 = valuation_service._basis_from_fundamental("005930", mixed)
        self.assertEqual(b2["shareComposition"], "mixed")

    def test_timeline_disclosure_rows_tag_reprice_type_only_when_matched(self):
        rows = stock_timeline.disclosure_rows([
            {"rcept_no": "1", "report_nm": "주식소각결정", "rcept_dt": "20261120"},
            {"rcept_no": "2", "report_nm": "자기주식취득결정", "rcept_dt": "20261119"},
        ])
        self.assertEqual(rows[0].get("reprice_type"), dr.STOCK_RETIREMENT)
        self.assertNotIn("reprice_type", rows[1])


class ProFormaFreshnessDenominatorAndVerdictTests(unittest.TestCase):
    def test_existing_treasury_retirement_distinguishes_issued_vs_outstanding_vs_unknown_denominator(self):
        parsed = dr.parse_reprice_document_zip(_make_zip(RETIREMENT_XML), dr.STOCK_RETIREMENT)

        # 1) 분모 미확정(market_data_cache 등) → uncalculable + 사실(수량·발행비율·금액·예정일) 유지
        unknown_pf = dr.compute_pro_forma(
            dr.STOCK_RETIREMENT,
            parsed,
            stock_code="005930",
            basis={"source": "market_data_cache", "bps": 100_000.0, "outstandingShares": 9_380_000},
            trailing_dps=None,
            rcept_dt="2026-11-20",
            as_of=AS_OF,
        )
        self.assertEqual(unknown_pf["status"], "uncalculable")
        self.assertEqual(unknown_pf["reason"], dr.UNCALCULABLE_TREASURY_REASON)
        self.assertIn("보통주 소각 620,000주(발행보통주 대비 6.20%)", unknown_pf["facts_summary"])
        self.assertIn("483.6억원", unknown_pf["facts_summary"])
        self.assertIn("2026-12-10", unknown_pf["facts_summary"])

        # 2) 분모=유통주식수이고 기취득 자기주식(treasuryShares >= 소각수) → no_impact
        out_pf = dr.compute_pro_forma(
            dr.STOCK_RETIREMENT,
            parsed,
            stock_code="005930",
            basis=_outstanding_denominator_basis(treasury=800_000),
            trailing_dps=None,
            rcept_dt="2026-11-20",
            as_of=AS_OF,
        )
        self.assertEqual(out_pf["status"], "no_impact")
        out_tr = dr.compute_target_reprice(
            "005930",
            {"stock_code": "005930", "target_price_formula": "BPS * 0.78"},
            disclosure_type=dr.STOCK_RETIREMENT,
            pre_variables={"BPS": 106_610.0},
            pro_forma=out_pf,
        )
        self.assertEqual(out_tr["status"], "no_impact")
        self.assertEqual(out_tr["delta_krw"], 0.0)

        # 3) 분모=발행주식수 확정 + 보통주 단독 구성 + 기취득 자기주식 → Pro-forma 계산(ok)
        iss_pf = dr.compute_pro_forma(
            dr.STOCK_RETIREMENT,
            parsed,
            stock_code="005930",
            basis=_issued_denominator_basis(),
            trailing_dps=None,
            rcept_dt="2026-11-20",
            as_of=AS_OF,
        )
        self.assertEqual(iss_pf["status"], "ok")
        self.assertAlmostEqual(iss_pf["per_share_uplift_pct"], 6.61, places=2)
        iss_tr = dr.compute_target_reprice(
            "005930",
            {"stock_code": "005930", "target_price_formula": "BPS * 0.78"},
            disclosure_type=dr.STOCK_RETIREMENT,
            pre_variables={"BPS": 100_000.0},
            pro_forma=iss_pf,
        )
        self.assertEqual(iss_tr["status"], "ok")
        self.assertEqual(round(iss_tr["pre_target"]), 78_000)
        self.assertEqual(round(iss_tr["post_target"]), 83_156)

    def test_mixed_share_composition_and_preferred_retirement_do_not_touch_common_bps(self):
        parsed = dr.parse_reprice_document_zip(_make_zip(RETIREMENT_XML), dr.STOCK_RETIREMENT)
        mixed_basis = {**_issued_denominator_basis(), "shareComposition": "mixed"}
        mixed_pf = dr.compute_pro_forma(
            dr.STOCK_RETIREMENT, parsed, stock_code="005930", basis=mixed_basis, trailing_dps=None, rcept_dt="2026-11-20", as_of=AS_OF
        )
        self.assertEqual(mixed_pf["status"], "uncalculable")

        pref_parsed = dr.parse_reprice_document_zip(_make_zip(PREFERRED_ONLY_RETIREMENT_XML), dr.STOCK_RETIREMENT)
        pref_pf = dr.compute_pro_forma(
            dr.STOCK_RETIREMENT, pref_parsed, stock_code="005930", basis=_issued_denominator_basis(), trailing_dps=None, rcept_dt="2026-11-20", as_of=AS_OF
        )
        self.assertEqual(pref_pf["status"], "uncalculable")
        self.assertIn("종류주식 소각", pref_pf["reason"])

    def test_stale_when_older_than_7_days_or_effective_date_passed(self):
        parsed = dr.parse_reprice_document_zip(_make_zip(RETIREMENT_XML), dr.STOCK_RETIREMENT)
        # 접수일 15일 경과 → stale
        old_pf = dr.compute_pro_forma(
            dr.STOCK_RETIREMENT, parsed, stock_code="005930", basis=_issued_denominator_basis(), trailing_dps=None, rcept_dt="2026-11-05", as_of=AS_OF
        )
        self.assertEqual(old_pf["status"], "stale")
        self.assertEqual(old_pf["reason"], dr.STALE_REASON)

        # 접수일은 최근이어도 소각예정일이 이미 도래 → stale
        passed_pf = dr.compute_pro_forma(
            dr.STOCK_RETIREMENT,
            parsed,
            stock_code="005930",
            basis=_issued_denominator_basis(),
            trailing_dps=None,
            rcept_dt="2026-12-09",
            as_of=date(2026, 12, 10),
        )
        self.assertEqual(passed_pf["status"], "stale")

    def test_dividend_requires_explicit_year_end_category(self):
        parsed_ok = dr.parse_reprice_document_zip(_make_zip(DIVIDEND_XML), dr.CASH_DIVIDEND)
        pf_ok = dr.compute_pro_forma(
            dr.CASH_DIVIDEND, parsed_ok, stock_code="005935", basis={}, trailing_dps=1250.0, rcept_dt="2026-11-20", as_of=AS_OF
        )
        self.assertEqual(pf_ok["status"], "ok")
        self.assertTrue(pf_ok["dividend_category_confirmed"])
        self.assertEqual(pf_ok["post_dps"], 1550.0)

        # 배당구분 누락 시 결산배당으로 가정하지 않고 uncalculable
        parsed_missing = dr.parse_reprice_document_zip(_make_zip(MISSING_CATEGORY_DIVIDEND_XML), dr.CASH_DIVIDEND)
        pf_missing = dr.compute_pro_forma(
            dr.CASH_DIVIDEND, parsed_missing, stock_code="005930", basis={}, trailing_dps=1200.0, rcept_dt="2026-11-20", as_of=AS_OF
        )
        self.assertEqual(pf_missing["status"], "uncalculable")
        self.assertFalse(pf_missing["dividend_category_confirmed"])

        # 분기배당 단건도 uncalculable
        parsed_q = dr.parse_reprice_document_zip(_make_zip(QUARTERLY_DIVIDEND_XML), dr.CASH_DIVIDEND)
        pf_q = dr.compute_pro_forma(
            dr.CASH_DIVIDEND, parsed_q, stock_code="005930", basis={}, trailing_dps=1444.0, rcept_dt="2026-11-20", as_of=AS_OF
        )
        self.assertEqual(pf_q["status"], "uncalculable")

    def test_auto_target_and_explicit_price_reaction_labels(self):
        holding_auto = {"stock_code": "005930", "avg_price": 60_000, "target_price": None, "target_price_formula": None}
        tr = dr.compute_target_reprice(
            "005930", holding_auto, disclosure_type=dr.STOCK_RETIREMENT, pre_variables={}, pro_forma={"status": "ok", "post_bps": 110_000.0}
        )
        self.assertEqual(tr["status"], "no_impact")
        self.assertEqual(tr["reason"], "영향 없음(목표가가 평단 기준)")

        closes = [
            {"date": "2026-11-02", "close": 50_000.0},
            {"date": "2026-11-03", "close": 50_400.0},
            {"date": "2026-11-04", "close": 50_500.0},
            {"date": "2026-11-05", "close": 50_600.0},
            {"date": "2026-11-06", "close": 50_700.0},
            {"date": "2026-11-09", "close": 50_500.0},
        ]
        pr = dr.compute_price_reaction(closes, "2026-11-03")
        self.assertEqual(pr["basis_label"], "접수일 직전 거래일 종가 기준")
        self.assertEqual(pr["day5"]["label"], "공시일 포함 5거래일")
        self.assertEqual(pr["day20"]["label"], "공시일 포함 20거래일")
        self.assertAlmostEqual(pr["day5"]["return_pct"], 1.00)

        under = dr.compute_verdict({"status": "ok", "delta_pct": 6.6}, pr)
        self.assertEqual(under["window_label"], "공시일 포함 5거래일")
        self.assertEqual(under["text"], "목표가 상향폭 대비 주가 반응이 작음")


class RepriceCacheAuthAmendmentAndPushIsolationTests(TempDbMixin):
    async def seed(self):
        dr._doc_mem_cache.clear()
        dart_client._quota.reset()
        engine._disc_cache.clear()
        await seed_user("u1")
        await seed_corp_codes([("005930", "00126380", "삼성전자", "2026-01-01")])
        db = await db_repo.get_db()
        await db.execute(
            "INSERT INTO user_portfolio (google_sub, stock_code, stock_name, quantity, avg_price, target_price_formula, created_at, updated_at)"
            " VALUES ('u1', '005930', '삼성전자', 100, 60000, 'BPS * 0.78', 't', 't')"
        )
        await db.commit()
        await notifications_repo.upsert_notification_channel(
            "u1", "telegram", config={"chat_id": "123", "bot_token": "T"}, enabled=True, verified=True
        )

    async def test_v2_cache_persists_only_ok_and_uses_short_ttl_for_failures(self):
        self.assertEqual(dr.CACHE_NS_REPRICE_DOC, "dart.reprice_doc.v2")

        # 1) 파싱 실패 문서는 SQLite cache_values 에 영속 저장하지 않고 메모리 1시간 TTL만 적용
        bad_doc = AsyncMock(return_value=_make_zip(AMBIGUOUS_DIVIDEND_XML))
        with patch.object(dart_client, "fetch_document_xml_bytes", new=bad_doc):
            failed = await dr._fetch_and_parse_document("20261120000888", dr.CASH_DIVIDEND)
        self.assertEqual(failed["parse_status"], "unparseable")
        self.assertIsNone(
            await cache_values.get_cache_value_entry(dr.CACHE_NS_REPRICE_DOC, f"{dr.CASH_DIVIDEND}:20261120000888")
        )
        mem_entry = dr._doc_mem_cache.get_entry(f"{dr.CASH_DIVIDEND}:20261120000888")
        self.assertEqual(mem_entry.ttl_seconds, dr.DOC_FAILURE_TTL_SECONDS)

        # 2) 성공(ok) 문서만 SQLite cache_values(v2) 에 영속 저장
        ok_doc = AsyncMock(return_value=_make_zip(RETIREMENT_XML))
        with patch.object(dart_client, "fetch_document_xml_bytes", new=ok_doc):
            ok_res = await dr._fetch_and_parse_document("20261120000123", dr.STOCK_RETIREMENT)
            dr._doc_mem_cache.clear()
            cached_res = await dr._fetch_and_parse_document("20261120000123", dr.STOCK_RETIREMENT)
        self.assertEqual(ok_res["parse_status"], "ok")
        self.assertEqual(cached_res["extracted"]["retired_shares_common"], 620_000)
        self.assertEqual(ok_doc.await_count, 1)

    async def test_guest_never_downloads_document_xml_and_amendment_supersedes_original(self):
        app = FastAPI()
        app.include_router(stocks_routes.router)
        filings = [
            {"rcept_no": "20261124000200", "report_nm": "[기재정정]주식소각결정", "rcept_dt": "20261124"},
            {"rcept_no": "20261122000100", "report_nm": "주식소각결정", "rcept_dt": "20261122"},
        ]
        closes = [
            {"date": "2026-11-20", "close": 70_000.0},
            {"date": "2026-11-23", "close": 70_200.0},
            {"date": "2026-11-24", "close": 70_500.0},
        ]
        doc_mock = AsyncMock(return_value=_make_zip(AMENDED_RETIREMENT_XML))

        with patch.object(dart_client, "api_key", return_value="k"), \
             patch.object(dart_client, "fetch_filing_list", new=AsyncMock(return_value=filings)), \
             patch.object(dart_client, "fetch_document_xml_bytes", new=doc_mock), \
             patch.object(dr, "today_kst", return_value=AS_OF), \
             patch.object(dr, "fetch_valuation_basis", new=AsyncMock(return_value=_issued_denominator_basis())), \
             patch.object(dr.close_price_client, "get_daily_closes", new=AsyncMock(return_value=closes)):
            # 1) 비로그인 최초 호출 → 원문 다운로드 0회(cache_miss), 정정된 원본은 superseded
            with patch.object(stocks_routes, "get_current_user", new=AsyncMock(return_value=None)):
                with TestClient(app) as client:
                    guest_before = client.get("/api/stocks/005930/reprice").json()
            self.assertEqual(doc_mock.await_count, 0)
            self.assertEqual(guest_before["items"][0]["parse_status"], "cache_miss")
            self.assertTrue(guest_before["items"][1]["superseded"])
            self.assertEqual(guest_before["items"][1]["parse_status"], "superseded")

            # 2) 로그인 호출 → 정정본(20261124000200)만 1회 원문 다운로드·계산, 원본은 다운로드 생략
            with patch.object(stocks_routes, "get_current_user", new=AsyncMock(return_value={"google_sub": "u1"})):
                with TestClient(app) as client:
                    auth_resp = client.get("/api/stocks/005930/reprice").json()
            self.assertEqual(doc_mock.await_count, 1)
            self.assertEqual(doc_mock.await_args.args[0], "20261124000200")
            self.assertEqual(auth_resp["items"][0]["target_reprice"]["status"], "ok")
            self.assertEqual(round(auth_resp["items"][0]["target_reprice"]["post_target"]), 83_156)
            self.assertEqual(auth_resp["items"][1]["pro_forma"]["status"], "superseded")
            self.assertEqual(auth_resp["items"][1]["target_reprice"]["status"], "superseded")

            # 3) 캐시 적재 뒤 비로그인 호출 → 추가 다운로드 없이 캐시된 Pro-forma 제공(target_reprice는 None)
            with patch.object(stocks_routes, "get_current_user", new=AsyncMock(return_value=None)):
                with TestClient(app) as client:
                    guest_after = client.get("/api/stocks/005930/reprice").json()
            self.assertEqual(doc_mock.await_count, 1)
            self.assertEqual(guest_after["items"][0]["pro_forma"]["status"], "ok")
            self.assertIsNone(guest_after["items"][0]["target_reprice"])

    async def test_push_only_calculates_latest_amendment_and_isolates_parser_exceptions(self):
        rule_id = await notifications_repo.create_portfolio_alert(
            "u1", scope="all_stocks", alert_type="disclosure_new_all", threshold=0.0
        )
        sent_messages: list[str] = []

        async def capture_dispatch(google_sub, text, **_):
            sent_messages.append(text)
            return 1

        box = {"v": {"rcept_no": "20261119000001", "report_nm": "기타공시", "rcept_dt": "20261119"}}

        async def fake_feed(kind, code, fc):
            return dict(box["v"])

        # 1) 원본(100)과 기재정정(200)이 한 패스에 함께 감지되면 정정본(200)에만 목표가 줄이 붙는다.
        with patch.object(engine, "_cached_feed", new=fake_feed), \
             patch.object(channels, "dispatch", new=capture_dispatch), \
             patch.object(dr, "today_kst", return_value=AS_OF), \
             patch.object(dart_client, "fetch_document_xml_bytes", new=AsyncMock(return_value=_make_zip(AMENDED_RETIREMENT_XML))), \
             patch.object(dr, "fetch_valuation_basis", new=AsyncMock(return_value=_issued_denominator_basis())):
            self.assertEqual(await engine.evaluate_user("u1"), 0)
            recent_batch = [
                {"rcept_no": "20261124000200", "report_nm": "[기재정정]주식소각결정", "rcept_dt": "20261124"},
                {"rcept_no": "20261122000100", "report_nm": "주식소각결정", "rcept_dt": "20261122"},
            ]
            box["v"] = {**recent_batch[0], "_recent": recent_batch}
            self.assertEqual(await engine.evaluate_user("u1"), 2)

        self.assertEqual(len(sent_messages), 2)
        self.assertNotIn("목표가 ", sent_messages[0])  # 정정된 원본은 재가격 줄 생략
        self.assertIn("목표가 78,000→83,156원(+6.6%)", sent_messages[1])  # 정정본에만 첨부

        # 2) 파서·래퍼 내부에서 예기치 못한 예외가 터져도 본 공시 알림은 정상 발송되고 baseline 이 갱신된다.
        sent_messages.clear()
        with patch.object(engine, "_cached_feed", new=fake_feed), \
             patch.object(channels, "dispatch", new=capture_dispatch), \
             patch.object(dr, "_fetch_and_parse_document", new=AsyncMock(side_effect=RuntimeError("unexpected parser crash"))):
            box["v"] = {
                "rcept_no": "20261125000300",
                "report_nm": "현금ㆍ현물배당결정",
                "rcept_dt": "20261125",
                "_recent": [{"rcept_no": "20261125000300", "report_nm": "현금ㆍ현물배당결정", "rcept_dt": "20261125"}],
            }
            self.assertEqual(await engine.evaluate_user("u1"), 1)

        self.assertEqual(len(sent_messages), 1)
        self.assertIn("현금ㆍ현물배당결정", sent_messages[0])
        updated_rule = await notifications_repo.get_portfolio_alert("u1", rule_id)
        self.assertEqual(json.loads(updated_rule["state_json"])["005930"], "20261125000300")

        # 3) 타임아웃 시에도 빈 문자열 반환
        async def slow_doc(*_a, **_kw):
            await asyncio.sleep(0.2)
            return _make_zip(DIVIDEND_XML)

        with patch.object(dart_client, "fetch_document_xml_bytes", new=slow_doc):
            line = await dr.format_notification_reprice_line(
                "005930",
                {"rcept_no": "20261125000777", "report_nm": "현금ㆍ현물배당결정", "rcept_dt": "20261125"},
                {"stock_code": "005930", "target_price_formula": "DPS * 20"},
                timeout=0.01,
                as_of=AS_OF,
            )
        self.assertEqual(line, "")


if __name__ == "__main__":
    unittest.main()
