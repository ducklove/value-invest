"""공시 → 목표가 재가격(Pro-forma) & 주가 반응 시차 단위·통합 테스트."""

from __future__ import annotations

import asyncio
import io
import unittest
import zipfile
from unittest.mock import AsyncMock, MagicMock, patch

from _harness import TempDbMixin, seed_corp_codes, seed_user
from fastapi import FastAPI
from fastapi.testclient import TestClient

from repositories import db as db_repo
from repositories import notifications as notifications_repo
from routes import stocks as stocks_routes
from services.dart import client as dart_client
from services.market import stock_timeline
from services.notifications import channels, engine
from services.portfolio import disclosure_reprice as dr


def _make_zip(xml_body: str, filename: str = "20261120000123.xml") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(filename, xml_body.encode("utf-8"))
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

AMBIGUOUS_DIVIDEND_XML = """
<table>
  <tr><td>1. 배당구분</td><td>결산배당</td></tr>
  <tr><td>3. 1주당 배당금(원)</td><td>보통주식</td><td>일반주주 1,500 / 최대주주 1,000</td></tr>
</table>
"""


class DisclosureClassificationAndParsingTests(unittest.TestCase):
    def test_classify_reprice_disclosure_matches_exact_two_types_and_excludes_subsidiaries(self):
        self.assertEqual(dr.classify_reprice_disclosure("주식소각결정"), dr.STOCK_RETIREMENT)
        self.assertEqual(dr.classify_reprice_disclosure("[기재정정]주요사항보고서(주식소각결정)"), dr.STOCK_RETIREMENT)
        self.assertEqual(dr.classify_reprice_disclosure("자기주식소각결정"), dr.STOCK_RETIREMENT)
        self.assertEqual(dr.classify_reprice_disclosure("현금ㆍ현물배당결정"), dr.CASH_DIVIDEND)
        self.assertEqual(dr.classify_reprice_disclosure("[기재정정]현금·현물배당결정"), dr.CASH_DIVIDEND)
        self.assertEqual(dr.classify_reprice_disclosure("현금배당결정"), dr.CASH_DIVIDEND)

        # 자회사 공시·철회·취소·예고·타 유형 제외
        self.assertIsNone(dr.classify_reprice_disclosure("현금ㆍ현물배당결정(자회사의주요경영사항)"))
        self.assertIsNone(dr.classify_reprice_disclosure("주식소각결정(종속회사의주요경영사항)"))
        self.assertIsNone(dr.classify_reprice_disclosure("주식소각결정철회"))
        self.assertIsNone(dr.classify_reprice_disclosure("현금ㆍ현물배당결정예고"))
        self.assertIsNone(dr.classify_reprice_disclosure("자기주식취득결정"))
        self.assertIsNone(dr.classify_reprice_disclosure("단일판매ㆍ공급계약체결"))

    def test_parse_stock_retirement_xml_expands_rowspan_and_handles_amendment_diff_table(self):
        parsed = dr.parse_reprice_document_zip(_make_zip(AMENDED_RETIREMENT_XML), dr.STOCK_RETIREMENT, "20261120000123")
        self.assertEqual(parsed["parse_status"], "ok")
        ext = parsed["extracted"]
        self.assertEqual(ext["retired_shares_common"], 620_000)
        self.assertIsNone(ext["retired_shares_preferred"])
        self.assertEqual(ext["issued_shares_common"], 10_000_000)
        self.assertEqual(ext["acquisition_method"], "기취득 자기주식")
        self.assertEqual(ext["scheduled_date"], "2026-12-10")

    def test_parse_cash_dividend_xml_extracts_common_and_preferred_dps(self):
        parsed = dr.parse_reprice_document_zip(_make_zip(DIVIDEND_XML), dr.CASH_DIVIDEND, "20261120000999")
        self.assertEqual(parsed["parse_status"], "ok")
        ext = parsed["extracted"]
        self.assertEqual(ext["dividend_category"], "결산배당")
        self.assertEqual(ext["dps_common"], 1500.0)
        self.assertEqual(ext["dps_preferred"], 1550.0)
        self.assertEqual(ext["yield_common_pct"], 3.2)
        self.assertEqual(ext["record_date"], "2026-12-31")

    def test_ambiguous_or_corrupt_xml_never_invents_numbers(self):
        amb = dr.parse_reprice_document_zip(_make_zip(AMBIGUOUS_DIVIDEND_XML), dr.CASH_DIVIDEND)
        self.assertEqual(amb["parse_status"], "unparseable")
        self.assertIn("수치 확인 불가", amb["parse_message"])

        bad = dr.parse_reprice_document_zip(b"not-a-zip", dr.STOCK_RETIREMENT)
        self.assertEqual(bad["parse_status"], "unparseable")
        self.assertIn("수치 확인 불가", bad["parse_message"])

    def test_timeline_disclosure_rows_tag_reprice_type_only_when_matched(self):
        rows = stock_timeline.disclosure_rows([
            {"rcept_no": "1", "report_nm": "주식소각결정", "rcept_dt": "20261120"},
            {"rcept_no": "2", "report_nm": "자기주식취득결정", "rcept_dt": "20261119"},
        ])
        self.assertEqual(rows[0].get("reprice_type"), dr.STOCK_RETIREMENT)
        self.assertNotIn("reprice_type", rows[1])


class ProFormaAndVerdictMathTests(unittest.TestCase):
    def test_retirement_pro_forma_and_formula_target_reprice(self):
        parsed = dr.parse_reprice_document_zip(_make_zip(RETIREMENT_XML), dr.STOCK_RETIREMENT)
        basis = {"bps": 100_000.0, "eps": 10_000.0, "outstandingShares": 10_000_000, "issuedShares": 10_000_000}
        pf = dr.compute_pro_forma(
            dr.STOCK_RETIREMENT, parsed, stock_code="005930", basis=basis, trailing_dps=None
        )
        self.assertEqual(pf["status"], "ok")
        # 10,000,000 / 9,380,000 = 1.066098... → +6.61%
        self.assertAlmostEqual(pf["per_share_uplift_pct"], 6.61, places=2)
        self.assertAlmostEqual(pf["post_bps"], 106609.81, places=2)

        holding = {"stock_code": "005930", "target_price_formula": "BPS * 0.78"}
        tr = dr.compute_target_reprice(
            "005930",
            holding,
            disclosure_type=dr.STOCK_RETIREMENT,
            pre_variables={"BPS": 100_000.0},
            pro_forma=pf,
        )
        self.assertEqual(tr["status"], "ok")
        self.assertEqual(round(tr["pre_target"]), 78_000)
        self.assertEqual(round(tr["post_target"]), 83_156)
        self.assertAlmostEqual(tr["delta_pct"], 6.61, places=2)

    def test_dividend_pro_forma_handles_preferred_stock_and_rejects_quarterly_substitution(self):
        parsed = dr.parse_reprice_document_zip(_make_zip(DIVIDEND_XML), dr.CASH_DIVIDEND)
        pf_pref = dr.compute_pro_forma(
            dr.CASH_DIVIDEND, parsed, stock_code="005935", basis={}, trailing_dps=1250.0
        )
        self.assertEqual(pf_pref["status"], "ok")
        self.assertEqual(pf_pref["post_dps"], 1550.0)
        self.assertAlmostEqual(pf_pref["dps_delta_pct"], 24.0, places=2)

        holding = {"stock_code": "005935", "target_price_formula": "DPS * 20"}
        tr = dr.compute_target_reprice(
            "005935",
            holding,
            disclosure_type=dr.CASH_DIVIDEND,
            pre_variables={"DPS": 1250.0},
            pro_forma=pf_pref,
        )
        self.assertEqual(tr["status"], "ok")
        self.assertEqual(tr["pre_target"], 25000.0)
        self.assertEqual(tr["post_target"], 31000.0)
        self.assertEqual(tr["delta_krw"], 6000.0)

        # 분기배당 단건 공시는 연간 DPS 수식에 직접 대입하지 않고 미계산 처리
        q_parsed = dr.parse_reprice_document_zip(_make_zip(QUARTERLY_DIVIDEND_XML), dr.CASH_DIVIDEND)
        q_pf = dr.compute_pro_forma(
            dr.CASH_DIVIDEND, q_parsed, stock_code="005930", basis={}, trailing_dps=1444.0
        )
        self.assertEqual(q_pf["status"], "uncalculable")
        q_tr = dr.compute_target_reprice(
            "005930",
            {"stock_code": "005930", "target_price_formula": "DPS * 25"},
            disclosure_type=dr.CASH_DIVIDEND,
            pre_variables={"DPS": 1444.0},
            pro_forma=q_pf,
        )
        self.assertEqual(q_tr["status"], "uncalculable")
        self.assertIn("분기배당", q_tr["reason"])

    def test_auto_target_reports_no_impact_avg_price_basis(self):
        pf = {"status": "ok", "post_bps": 110_000.0}
        holding_auto = {"stock_code": "005930", "avg_price": 60_000, "target_price": None, "target_price_formula": None}
        tr = dr.compute_target_reprice(
            "005930", holding_auto, disclosure_type=dr.STOCK_RETIREMENT, pre_variables={}, pro_forma=pf
        )
        self.assertEqual(tr["status"], "no_impact")
        self.assertEqual(tr["reason"], "영향 없음(목표가가 평단 기준)")

    def test_price_reaction_and_verdict_windows(self):
        closes = [
            {"date": "2026-11-02", "close": 50_000.0},  # 공시 전 종가
            {"date": "2026-11-03", "close": 50_400.0},  # 당일 (+0.80%)
            {"date": "2026-11-04", "close": 50_500.0},
            {"date": "2026-11-05", "close": 50_600.0},
            {"date": "2026-11-06", "close": 50_700.0},
            {"date": "2026-11-09", "close": 50_500.0},  # 5거래일 (+1.00%)
        ]
        pr = dr.compute_price_reaction(closes, "2026-11-03")
        self.assertEqual(pr["status"], "ok")
        self.assertEqual(pr["base_date"], "2026-11-02")
        self.assertAlmostEqual(pr["day0"]["return_pct"], 0.80)
        self.assertAlmostEqual(pr["day5"]["return_pct"], 1.00)
        self.assertEqual(pr["day20"]["status"], "in_progress")

        under = dr.compute_verdict({"status": "ok", "delta_pct": 6.6}, pr)
        self.assertEqual(under["code"], "underreacted")
        self.assertEqual(under["text"], "목표가 상향폭 대비 주가 반응이 작음")

        priced_in_pr = {**pr, "day5": {"status": "ok", "date": "2026-11-09", "close": 56_000.0, "return_pct": 12.0}}
        priced = dr.compute_verdict({"status": "ok", "delta_pct": 5.0}, priced_in_pr)
        self.assertEqual(priced["code"], "priced_in")
        self.assertEqual(priced["text"], "주가가 먼저 반영")


class RepriceQuotaCacheRouteAndNotificationTests(TempDbMixin):
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

    async def test_document_xml_respects_nonessential_budget_and_db_cache(self):
        dart_client._quota._roll()
        dart_client._quota.calls = dart_client.NONESSENTIAL_BUDGET
        with patch.object(dart_client, "api_key", return_value="k"):
            with self.assertRaises(dart_client.DartQuotaError):
                await dart_client.fetch_document_xml_bytes("20261120000123", essential=False)

        # 예산 내 1회 다운로드 후 메모리 캐시를 비워도 DB cache_values 에서 히트해 재호출하지 않는다.
        dart_client._quota.reset()
        fake_client = MagicMock()
        fake_client.get = AsyncMock(return_value=MagicMock(status_code=200, content=_make_zip(RETIREMENT_XML)))
        with patch.object(dart_client, "api_key", return_value="k"), \
             patch.object(dart_client, "get_http_client", new=AsyncMock(return_value=fake_client)):
            first = await dr._fetch_and_parse_document("20261120000123", dr.STOCK_RETIREMENT)
            dr._doc_mem_cache.clear()
            second = await dr._fetch_and_parse_document("20261120000123", dr.STOCK_RETIREMENT)
        self.assertEqual(first["parse_status"], "ok")
        self.assertEqual(second["extracted"]["retired_shares_common"], 620_000)
        self.assertEqual(fake_client.get.await_count, 1)

    async def test_reprice_route_logged_in_vs_guest(self):
        app = FastAPI()
        app.include_router(stocks_routes.router)
        filings = [{"rcept_no": "20261120000123", "report_nm": "주식소각결정", "rcept_dt": "20261120"}]
        closes = [
            {"date": "2026-11-19", "close": 70_000.0},
            {"date": "2026-11-20", "close": 70_500.0},
        ]
        basis = {"applicable": True, "bps": 100_000.0, "eps": 8_000.0, "outstandingShares": 10_000_000}

        with patch.object(dart_client, "api_key", return_value="k"), \
             patch.object(dart_client, "fetch_filing_list", new=AsyncMock(return_value=filings)), \
             patch.object(dart_client, "fetch_document_xml_bytes", new=AsyncMock(return_value=_make_zip(RETIREMENT_XML))), \
             patch.object(dr, "fetch_valuation_basis", new=AsyncMock(return_value=basis)), \
             patch.object(dr.close_price_client, "get_daily_closes", new=AsyncMock(return_value=closes)):
            with patch.object(stocks_routes, "get_current_user", new=AsyncMock(return_value={"google_sub": "u1"})):
                with TestClient(app) as client:
                    auth_resp = client.get("/api/stocks/005930/reprice").json()
            with patch.object(stocks_routes, "get_current_user", new=AsyncMock(return_value=None)):
                with TestClient(app) as client:
                    guest_resp = client.get("/api/stocks/005930/reprice").json()

        self.assertTrue(auth_resp["authenticated"])
        self.assertTrue(auth_resp["held"])
        self.assertEqual(auth_resp["items"][0]["target_reprice"]["status"], "ok")
        self.assertEqual(round(auth_resp["items"][0]["target_reprice"]["pre_target"]), 78_000)
        self.assertEqual(round(auth_resp["items"][0]["target_reprice"]["post_target"]), 83_156)
        self.assertEqual(auth_resp["items"][0]["verdict"]["text"], "목표가 상향폭 대비 주가 반응이 작음")

        self.assertFalse(guest_resp["authenticated"])
        self.assertFalse(guest_resp["held"])
        self.assertIsNone(guest_resp["items"][0]["target_reprice"])
        self.assertEqual(guest_resp["items"][0]["pro_forma"]["status"], "ok")

    async def test_disclosure_push_appends_target_reprice_line_and_skips_on_timeout(self):
        await notifications_repo.create_portfolio_alert(
            "u1", scope="all_stocks", alert_type="disclosure_new_all", threshold=0.0
        )
        sent_messages: list[str] = []

        async def capture_dispatch(google_sub, text, **_):
            sent_messages.append(text)
            return 1

        box = {"v": {"rcept_no": "20261119000001", "report_nm": "기타공시", "rcept_dt": "20261119"}}

        async def fake_feed(kind, code, fc):
            return dict(box["v"])

        basis = {"applicable": True, "bps": 100_000.0, "outstandingShares": 10_000_000}
        with patch.object(engine, "_cached_feed", new=fake_feed), \
             patch.object(channels, "dispatch", new=capture_dispatch), \
             patch.object(dart_client, "fetch_document_xml_bytes", new=AsyncMock(return_value=_make_zip(RETIREMENT_XML))), \
             patch.object(dr, "fetch_valuation_basis", new=AsyncMock(return_value=basis)):
            # 1) 첫 관측: baseline 기록만
            self.assertEqual(await engine.evaluate_user("u1"), 0)
            # 2) 주식소각결정 신규 공시 발화 → 목표가 라인 첨부
            box["v"] = {"rcept_no": "20261120000123", "report_nm": "주식소각결정", "rcept_dt": "20261120"}
            self.assertEqual(await engine.evaluate_user("u1"), 1)

        self.assertEqual(len(sent_messages), 1)
        self.assertIn("목표가 78,000→83,156원(+6.6%)", sent_messages[0])

        # 3) 원문 조회가 지연(Timeout)되어도 공시 알림 자체는 정상 발송(재가격 줄만 생략)
        dr._doc_mem_cache.clear()

        async def slow_doc(*_a, **_kw):
            await asyncio.sleep(0.2)
            return _make_zip(DIVIDEND_XML)

        with patch.object(engine, "_cached_feed", new=fake_feed), \
             patch.object(channels, "dispatch", new=capture_dispatch), \
             patch.object(dr, "NOTIFICATION_REPRICE_TIMEOUT_S", 0.01), \
             patch.object(dart_client, "fetch_document_xml_bytes", new=slow_doc):
            line = await dr.format_notification_reprice_line(
                "005930",
                {"rcept_no": "20261121000777", "report_nm": "현금ㆍ현물배당결정"},
                {"stock_code": "005930", "target_price_formula": "DPS * 20"},
                timeout=0.01,
            )
        self.assertEqual(line, "")

    async def test_unexpected_internal_bug_propagates_through_narrowed_handlers(self):
        class _UnexpectedBug(Exception):
            pass

        with patch.object(
            dart_client,
            "fetch_document_xml_bytes",
            new=AsyncMock(side_effect=_UnexpectedBug("internal bug")),
        ):
            with self.assertRaises(_UnexpectedBug):
                await dr._fetch_and_parse_document("20261122000111", dr.STOCK_RETIREMENT)


if __name__ == "__main__":
    unittest.main()
