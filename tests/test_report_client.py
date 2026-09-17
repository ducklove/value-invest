"""Tests for report_client (Naver 증권 리서치 JSON API 클라이언트).

네트워크는 타지 않는다 — get_http_client 를 가짜 클라이언트로 바꿔서
2026-09 에 관찰한 stock.naver.com 응답 모양을 그대로 재현한다. 회귀 배경:
네이버가 구 finance.naver.com HTML 페이지를 302 로 폐지하면서 종목분석 탭의
증권사 리포트가 통째로 비어 보였다.
"""
import unittest
from unittest.mock import AsyncMock, patch

import report_client
from report_client import (
    NAVER_RESEARCH_API,
    _dedupe_reports,
    _detail_fields,
    _normalize_target_price,
    _parse_api_item,
    fetch_latest_report,
    fetch_reports,
)


def _list_item(nid, title="리포트", date="2026-09-07", firm="미래에셋증권", goal="400000", opinion="매수"):
    return {
        "nid": str(nid),
        "title": title,
        "content": "<p><strong>요약</strong></p><p><br>본문&amp;내용</p>",
        "brokerName": firm,
        "brokerCode": "56",
        "writeDate": date,
        "readCount": "44208",
        "itemCode": "005930",
        "itemName": "삼성전자",
        "goalPrice": goal,
        "opinionText": opinion,
        "opinionType": "buy",
    }


def _detail_body(nid, pdf="https://stock.pstatic.net/stock-research/company/56/x.pdf"):
    return dict(
        _list_item(nid),
        attachUrl=pdf,
        attachName="x.pdf",
        prevGoalPrice="370000",
        priceAtWriteDate="255500",
    )


class _FakeResponse:
    def __init__(self, payload, status_code=200, raw=None):
        self._payload = payload
        self.status_code = status_code
        self._raw = raw

    def json(self):
        if self._raw is not None:
            raise ValueError("not json")
        return self._payload


class _FakeClient:
    """URL(+params) 별로 준비된 응답을 돌려주고 호출을 기록한다."""

    def __init__(self, list_pages=None, details=None, list_status=200, detail_status=200):
        self.list_pages = list_pages or {}
        self.details = details or {}
        self.list_status = list_status
        self.detail_status = detail_status
        self.calls = []

    async def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append((url, dict(params or {}), headers))
        if url == NAVER_RESEARCH_API:
            index = int((params or {}).get("index", "0"))
            page = self.list_pages.get(index)
            if page is None:
                return _FakeResponse({"hasNext": False, "totalCount": "0", "items": [], "index": str(index)})
            if page == "invalid":
                return _FakeResponse(None, raw="<html>")
            return _FakeResponse(page, status_code=self.list_status)
        nid = url.rsplit("/", 1)[-1]
        detail = self.details.get(nid)
        if detail is None:
            return _FakeResponse({"detailCode": "HttpError", "message": "Not Found"}, status_code=404)
        return _FakeResponse(detail, status_code=self.detail_status)


def _patch_client(client):
    return patch.object(report_client, "get_http_client", AsyncMock(return_value=client))


class ParseApiItemTests(unittest.TestCase):
    def test_maps_list_item_to_legacy_schema(self):
        out = _parse_api_item(_list_item(96027, title="HBM으로 매크로 우려 극복"))
        self.assertEqual(out["date"], "2026-09-07")
        self.assertEqual(out["title"], "HBM으로 매크로 우려 극복")
        self.assertEqual(out["firm"], "미래에셋증권")
        self.assertEqual(out["firm_short"], "미래에셋증권")
        self.assertEqual(out["target_price"], "400000")
        self.assertEqual(out["recommendation"], "매수")
        self.assertEqual(out["summary"], "요약 본문&내용")
        self.assertEqual(out["pdf_url"], "")  # 목록에는 PDF 링크가 없다
        self.assertEqual(out["source_url"], "https://stock.naver.com/research/company/96027")
        self.assertEqual(out["nid"], "96027")
        self.assertEqual(out["pages"], 0)
        self.assertEqual(out["analyst"], "")

    def test_returns_none_without_nid_or_title(self):
        self.assertIsNone(_parse_api_item({"title": "x"}))
        self.assertIsNone(_parse_api_item({"nid": "1", "title": ""}))
        self.assertIsNone(_parse_api_item("not a dict"))

    def test_accepts_research_id_alias_and_legacy_date(self):
        out = _parse_api_item({"researchId": 96216, "title": "T", "writeDate": "26.09.17"})
        self.assertEqual(out["nid"], "96216")
        self.assertEqual(out["date"], "2026-09-17")

    def test_missing_goal_and_opinion_become_empty(self):
        item = _list_item(1)
        item["goalPrice"] = None
        item["opinionText"] = "없음"
        out = _parse_api_item(item)
        self.assertEqual(out["target_price"], "")
        self.assertEqual(out["recommendation"], "")


class NormalizeTargetPriceTests(unittest.TestCase):
    def test_variants(self):
        self.assertEqual(_normalize_target_price("400000"), "400000")
        self.assertEqual(_normalize_target_price("150,000원"), "150000")
        self.assertEqual(_normalize_target_price(250000), "250000")
        self.assertEqual(_normalize_target_price("없음"), "")
        self.assertEqual(_normalize_target_price("0"), "")
        self.assertEqual(_normalize_target_price(None), "")
        self.assertEqual(_normalize_target_price(""), "")


class DetailFieldsTests(unittest.TestCase):
    def test_flat_detail_body(self):
        out = _detail_fields(_detail_body(96027))
        self.assertEqual(out["pdf_url"], "https://stock.pstatic.net/stock-research/company/56/x.pdf")
        self.assertEqual(out["prev_target_price"], "370000")
        self.assertEqual(out["price_at_write_date"], "255500")
        self.assertEqual(out["target_price"], "400000")
        self.assertNotIn("analyst", out)  # 빈 값은 제외 — 목록값을 덮어쓰지 않는다

    def test_wrapped_research_content_body(self):
        out = _detail_fields({"researchContent": _detail_body(96027), "researchSummaries": []})
        self.assertEqual(out["pdf_url"], "https://stock.pstatic.net/stock-research/company/56/x.pdf")

    def test_garbage_payload(self):
        self.assertEqual(_detail_fields(None), {})
        self.assertEqual(_detail_fields({"detailCode": "HttpError"}), {})


class FetchLatestReportTests(unittest.IsolatedAsyncioTestCase):
    async def test_returns_first_item_enriched_with_pdf(self):
        client = _FakeClient(
            list_pages={0: {"hasNext": True, "totalCount": "1233", "items": [_list_item(96027)]}},
            details={"96027": _detail_body(96027)},
        )
        with _patch_client(client):
            report = await fetch_latest_report("005930")

        self.assertEqual(report["nid"], "96027")
        self.assertEqual(report["pdf_url"], "https://stock.pstatic.net/stock-research/company/56/x.pdf")
        self.assertEqual(report["target_price"], "400000")
        list_call = client.calls[0]
        self.assertEqual(list_call[0], NAVER_RESEARCH_API)
        self.assertEqual(list_call[1]["itemCodes"], "005930")
        self.assertEqual(list_call[1]["size"], "1")
        self.assertEqual(list_call[1]["index"], "0")
        self.assertEqual(client.calls[1][0], f"{NAVER_RESEARCH_API}/96027")

    async def test_none_when_no_items(self):
        client = _FakeClient(list_pages={})
        with _patch_client(client):
            self.assertIsNone(await fetch_latest_report("999999"))

    async def test_none_on_http_error(self):
        client = _FakeClient(list_pages={0: {"items": [_list_item(1)]}}, list_status=503)
        with _patch_client(client):
            self.assertIsNone(await fetch_latest_report("005930"))

    async def test_survives_detail_failure(self):
        client = _FakeClient(list_pages={0: {"items": [_list_item(5)]}}, details={})
        with _patch_client(client):
            report = await fetch_latest_report("005930")
        self.assertEqual(report["nid"], "5")
        self.assertEqual(report["pdf_url"], "")


class FetchReportsTests(unittest.IsolatedAsyncioTestCase):
    async def test_pages_until_has_next_false_and_enriches_each(self):
        client = _FakeClient(
            list_pages={
                0: {"hasNext": True, "items": [_list_item(3, date="2026-09-07"), _list_item(2, date="2026-08-01")]},
                1: {"hasNext": False, "items": [_list_item(1, date="2026-07-01")]},
            },
            details={str(n): _detail_body(n, pdf=f"https://stock.pstatic.net/stock-research/company/56/{n}.pdf") for n in (1, 2, 3)},
        )
        with _patch_client(client):
            reports = await fetch_reports("005930", max_pages=5, per_page=2)

        self.assertEqual([r["nid"] for r in reports], ["3", "2", "1"])
        self.assertTrue(all(r["pdf_url"].endswith(f"/{r['nid']}.pdf") for r in reports))
        list_calls = [c for c in client.calls if c[0] == NAVER_RESEARCH_API]
        self.assertEqual([c[1]["index"] for c in list_calls], ["0", "1"])
        self.assertTrue(all(c[1]["itemCodes"] == "005930" for c in list_calls))
        self.assertTrue(all(c[1]["startDate"].endswith("-01-01") for c in list_calls))
        self.assertTrue(all(c[1]["size"] == "2" for c in list_calls))

    async def test_stops_at_max_pages(self):
        client = _FakeClient(
            list_pages={i: {"hasNext": True, "items": [_list_item(100 + i, title=f"R{i}")]} for i in range(5)},
            details={},
        )
        with _patch_client(client):
            reports = await fetch_reports("005930", max_pages=2, per_page=1)
        self.assertEqual(len(reports), 2)
        self.assertEqual(len([c for c in client.calls if c[0] == NAVER_RESEARCH_API]), 2)

    async def test_stops_at_three_year_cutoff(self):
        client = _FakeClient(
            list_pages={
                0: {"hasNext": True, "items": [_list_item(2, date="2026-01-01"), _list_item(1, date="2019-12-31")]},
                1: {"hasNext": True, "items": [_list_item(0, date="2019-06-01")]},
            },
            details={},
        )
        with _patch_client(client):
            reports = await fetch_reports("005930", max_pages=5, per_page=2)
        self.assertEqual([r["nid"] for r in reports], ["2"])
        self.assertEqual(len([c for c in client.calls if c[0] == NAVER_RESEARCH_API]), 1)

    async def test_size_is_capped_at_naver_max(self):
        client = _FakeClient(list_pages={0: {"hasNext": False, "items": [_list_item(1)]}})
        with _patch_client(client):
            await fetch_reports("005930", max_pages=1, per_page=500)
        self.assertEqual(client.calls[0][1]["size"], "50")

    async def test_empty_on_invalid_json_or_http_error(self):
        with _patch_client(_FakeClient(list_pages={0: "invalid"})):
            self.assertEqual(await fetch_reports("005930"), [])
        with _patch_client(_FakeClient(list_pages={0: {"items": [_list_item(1)]}}, list_status=500)):
            self.assertEqual(await fetch_reports("005930"), [])

    async def test_dedupes_rows_repeated_across_pages(self):
        client = _FakeClient(
            list_pages={
                0: {"hasNext": True, "items": [_list_item(1, title="R1"), _list_item(2, title="R2")]},
                1: {"hasNext": False, "items": [_list_item(1, title="R1")]},
            },
        )
        with _patch_client(client):
            reports = await fetch_reports("058650", max_pages=5, per_page=2)
        self.assertEqual([r["title"] for r in reports], ["R1", "R2"])


class DedupeReportsTests(unittest.TestCase):
    def test_keeps_unique_rows(self):
        rows = [
            {"date": "2026-03-06", "firm": "신한투자증권", "title": "A", "pdf_url": None},
            {"date": "2026-02-10", "firm": "신한투자증권", "title": "B", "pdf_url": None},
            {"date": "2025-12-24", "firm": "한국IR협의회", "title": "C", "pdf_url": "https://x"},
        ]
        out = _dedupe_reports(rows)
        self.assertEqual(len(out), 3)
        self.assertEqual([r["title"] for r in out], ["A", "B", "C"])

    def test_drops_exact_duplicates(self):
        rows = [
            {"date": "2026-03-06", "firm": "신한투자증권", "title": "리포트 1"},
            {"date": "2025-12-24", "firm": "신한투자증권", "title": "리포트 2"},
            {"date": "2026-03-06", "firm": "신한투자증권", "title": "리포트 1"},  # dup of 1
            {"date": "2025-12-24", "firm": "신한투자증권", "title": "리포트 2"},  # dup of 2
        ]
        out = _dedupe_reports(rows)
        self.assertEqual(len(out), 2)

    def test_first_occurrence_wins(self):
        rows = [
            {"date": "2025-08-08", "firm": "A", "title": "T", "pdf_url": "https://first"},
            {"date": "2025-08-08", "firm": "A", "title": "T", "pdf_url": "https://second"},
        ]
        out = _dedupe_reports(rows)
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["pdf_url"], "https://first")

    def test_distinguishes_same_title_different_firm(self):
        rows = [
            {"date": "2026-03-06", "firm": "신한투자증권", "title": "반도체 수급 전망"},
            {"date": "2026-03-06", "firm": "미래에셋증권", "title": "반도체 수급 전망"},
        ]
        out = _dedupe_reports(rows)
        self.assertEqual(len(out), 2)

    def test_distinguishes_same_title_different_date(self):
        rows = [
            {"date": "2026-03-06", "firm": "X", "title": "연간 전망"},
            {"date": "2025-03-06", "firm": "X", "title": "연간 전망"},
        ]
        out = _dedupe_reports(rows)
        self.assertEqual(len(out), 2)

    def test_handles_missing_fields(self):
        rows = [
            {},
            {"date": None},
            {"firm": ""},
            {"date": "", "firm": "", "title": ""},
        ]
        out = _dedupe_reports(rows)
        self.assertEqual(len(out), 1)

    def test_058650_regression(self):
        pairs = [
            ("2026-03-06", "신한투자증권", "R1"),
            ("2025-12-24", "신한투자증권", "R2"),
            ("2025-08-27", "신한투자증권", "R3"),
            ("2025-08-08", "한국IR협의회", "R4"),
            ("2025-06-30", "신한투자증권", "R5"),
        ]
        rows = [{"date": d, "firm": f, "title": t} for d, f, t in pairs * 2]
        out = _dedupe_reports(rows)
        self.assertEqual(len(out), 5)


if __name__ == "__main__":
    unittest.main()
