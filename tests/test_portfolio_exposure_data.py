"""X-Ray 데이터 로더·KIND 파서·조립·라우트 테스트."""

import json
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from _harness import TempDbMixin, seed_user
from fastapi import HTTPException
from starlette.requests import Request

from repositories import accounts as accounts_repo
from repositories import cache_values
from repositories import db as db_repo
from routes import portfolio_exposure as exposure_route
from services.market.sources import kind
from services.portfolio import exposure_data, exposure_service

ETF_PAYLOAD = {
    "generatedAt": "2026-09-30T20:54:16.743Z",  # KST 로는 10-01 05:54
    "etfs": [
        {
            "id": "069500",
            "shortName": " KODEX 200 ",
            "name": "KODEX 200",
            "assetClass": "주식",
            "currency": "KRW",
            "holdings": [
                {"ticker": "005930", "name": "삼성전자", "weight": 34.5},
                {"ticker": None, "name": "2026-12 코스피200지수선물", "weight": None},
            ],
            "sparkline": [1, 2, 3],
        },
        {"id": "voo", "name": "Vanguard S&P 500", "assetClass": "주식", "holdings": []},
        "garbage",
    ],
}

CORP_LIST_HTML = """
<html><body><table>
  <tr><th>회사명</th><th>시장구분</th><th>종목코드</th><th>업종</th><th>주요제품</th><th>상장일</th></tr>
  <tr>
    <td>삼성전자</td>
    <td>

        유가

    </td>
    <td style="mso-number-format:'@';text-align:center;">005930</td>
    <td>통신 및 방송 장비 제조업</td>
    <td>반도체</td><td>1975-06-11</td>
  </tr>
  <tr><td>KB제34호스팩</td><td>코스닥</td><td>0209J0</td><td>금융 지원 서비스업</td><td>기업인수합병</td><td>2026-09-22</td></tr>
  <tr><td>이상한 행</td><td>코스닥</td><td>12345</td><td>업종</td><td>-</td><td>-</td></tr>
  <tr><td>짧은 행</td></tr>
</table></body></html>
"""


def _response(status: int, *, content: bytes = b"", etag: str | None = None) -> httpx.Response:
    headers = {"etag": etag} if etag else {}
    return httpx.Response(status, content=content, headers=headers, request=httpx.Request("GET", "https://x/"))


class _Client:
    """응답을 순서대로 돌려주고 요청 헤더를 기록하는 가짜 httpx 클라이언트."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []

    async def get(self, url, **kwargs):
        self.requests.append((url, kwargs))
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


# ---------------------------------------------------------------------------
# 순수 변환
# ---------------------------------------------------------------------------

def test_compact_etf_snapshot_keeps_only_weighted_rows_and_needed_fields():
    value = exposure_data.compact_etf_snapshot(json.dumps(ETF_PAYLOAD).encode())
    assert value["available"] is True
    assert value["as_of"] == "2026-10-01"  # UTC 시각을 KST 날짜로 바꾼다.
    assert value["generated_at"] == ETF_PAYLOAD["generatedAt"]
    kodex = value["etfs"]["069500"]
    assert kodex["name"] == "KODEX 200"
    assert kodex["full_name"] == "KODEX 200"
    assert kodex["rows"] == [["005930", "삼성전자", 34.5]]
    assert kodex["unweighted"] == 1
    assert "sparkline" not in kodex
    assert value["etfs"]["VOO"]["rows"] == []


@pytest.mark.parametrize("raw", [b"{}", b'{"etfs": []}', b'{"etfs": ["x"]}', b"not json"])
def test_compact_etf_snapshot_rejects_bad_shapes(raw):
    with pytest.raises(ValueError):
        exposure_data.compact_etf_snapshot(raw)


def test_normalize_holdco_payload_indexes_by_holding_code():
    value = exposure_data.normalize_holdco_payload({
        "generatedAt": "2026-09-14T09:01:07+09:00",
        "items": [{
            "holdingCode": "000670",
            "holdingName": "영풍",
            "holdingAdjustedShares": 17874450,
            "holdingTotalShares": 18077950,
            "subsidiaries": [{"code": "010130", "name": "고려아연", "sharesHeld": 5262450, "ticker": "010130.KS"}],
        }],
    })
    assert value["as_of"] == "2026-09-14"
    assert value["holdcos"]["000670"]["adjusted_shares"] == 17874450
    assert value["holdcos"]["000670"]["subsidiaries"] == [{"code": "010130", "name": "고려아연", "shares_held": 5262450}]
    with pytest.raises(ValueError):
        exposure_data.normalize_holdco_payload({"items": []})


def test_parse_corp_list_finds_columns_by_header_name():
    companies = kind.parse_corp_list(CORP_LIST_HTML)
    assert set(companies) == {"005930", "0209J0"}  # 알파벳 코드도 받고 형식이 어긋난 행은 버린다.
    assert companies["005930"] == {"name": "삼성전자", "market": "유가", "sector": "통신 및 방송 장비 제조업"}


def test_parse_corp_list_survives_column_reorder_and_rejects_other_pages():
    # 시장구분 열이 없던 예전 형식도 머리글로 찾아 읽는다.
    old = (
        "<table><tr><th>회사명</th><th>종목코드</th><th>업종</th></tr>"
        "<tr><td>삼성전자</td><td>005930</td><td>반도체</td></tr></table>"
    )
    assert kind.parse_corp_list(old)["005930"] == {"name": "삼성전자", "market": "", "sector": "반도체"}
    assert kind.parse_corp_list("<html>로그인 페이지</html>") == {}
    assert kind.parse_corp_list("<table><tr><th>전혀</th><th>다른</th></tr></table>") == {}
    assert kind.parse_corp_list("") == {}


class KindFetchTests(TempDbMixin):
    async def test_fetch_decodes_cp949_and_rejects_short_lists(self):
        rows = "".join(
            f"<tr><td>회사{i}</td><td>코스닥</td><td>{i:06d}</td><td>업종</td></tr>" for i in range(100000, 101000)
        )
        html = f"<table><tr><th>회사명</th><th>시장구분</th><th>종목코드</th><th>업종</th></tr>{rows}</table>"
        client = _Client(_response(200, content=html.encode("cp949")))
        with patch.object(kind, "get_http_client", new=AsyncMock(return_value=client)):
            companies = await kind.fetch_listed_companies()
        self.assertEqual(len(companies), 1000)
        self.assertEqual(companies["100000"]["name"], "회사100000")

        short = _Client(_response(200, content=CORP_LIST_HTML.encode("cp949")))
        with patch.object(kind, "get_http_client", new=AsyncMock(return_value=short)):
            with self.assertRaises(kind.KindError):
                await kind.fetch_listed_companies()


# ---------------------------------------------------------------------------
# 영속 캐시
# ---------------------------------------------------------------------------

class PersistedLoaderTests(TempDbMixin):
    async def seed(self):
        exposure_data.reset()
        self.addCleanup(exposure_data.reset)

    async def test_etf_loader_parses_once_and_survives_restart_without_network(self):
        client = _Client(_response(200, content=json.dumps(ETF_PAYLOAD).encode(), etag='"v1"'))
        with patch.object(exposure_data, "get_http_client", new=AsyncMock(return_value=client)):
            first = await exposure_data.load_etf_holdings()
            again = await exposure_data.load_etf_holdings()
        self.assertTrue(first["available"])
        self.assertEqual(first["etag"], '"v1"')
        self.assertIs(first, again)  # 메모리 원본을 공유해 큰 값을 복사하지 않는다.
        self.assertEqual(len(client.requests), 1)

        # 재시작(메모리 비움) — DB 값이 아직 신선하면 네트워크에 가지 않는다.
        exposure_data.reset()
        offline = _Client()
        with patch.object(exposure_data, "get_http_client", new=AsyncMock(return_value=offline)):
            restored = await exposure_data.load_etf_holdings()
        self.assertEqual(restored["etfs"]["069500"]["rows"], [["005930", "삼성전자", 34.5]])
        self.assertEqual(offline.requests, [])

    async def test_etf_loader_sends_etag_and_keeps_value_on_304(self):
        previous = exposure_data.compact_etf_snapshot(json.dumps(ETF_PAYLOAD).encode())
        previous["etag"] = '"v1"'
        await cache_values.set_cache_value("portfolio.exposure", "etf_holdings", previous, ttl_seconds=-1)
        client = _Client(_response(304))
        parse = patch.object(exposure_data, "compact_etf_snapshot", side_effect=AssertionError("다시 파싱하면 안 된다"))
        with patch.object(exposure_data, "get_http_client", new=AsyncMock(return_value=client)), parse:
            value = await exposure_data.load_etf_holdings()
        self.assertEqual(client.requests[0][1]["headers"]["If-None-Match"], '"v1"')
        self.assertEqual(value["etfs"].keys(), previous["etfs"].keys())
        self.assertNotIn("stale", value)
        entry = await cache_values.get_cache_value_entry("portfolio.exposure", "etf_holdings")
        self.assertIsNotNone(entry)  # TTL 이 다시 연장됐다.

    async def test_etf_loader_serves_stale_value_when_refresh_fails(self):
        previous = exposure_data.compact_etf_snapshot(json.dumps(ETF_PAYLOAD).encode())
        await cache_values.set_cache_value("portfolio.exposure", "etf_holdings", previous, ttl_seconds=-1)
        client = _Client(httpx.ConnectError("down"))
        with patch.object(exposure_data, "get_http_client", new=AsyncMock(return_value=client)):
            value = await exposure_data.load_etf_holdings()
            again = await exposure_data.load_etf_holdings()
        self.assertTrue(value["available"])
        self.assertTrue(value["stale"])
        self.assertEqual(value["as_of"], "2026-10-01")
        self.assertIs(value, again)  # 실패 직후 요청마다 재시도하지 않는다.
        self.assertEqual(len(client.requests), 1)

    async def test_loader_reports_unavailable_without_previous_value(self):
        client = _Client(_response(500))
        with patch.object(exposure_data, "get_http_client", new=AsyncMock(return_value=client)):
            value = await exposure_data.load_holdcos()
        self.assertFalse(value["available"])
        self.assertIsNone(value["as_of"])
        self.assertIsNone(await cache_values.get_cache_value_entry("portfolio.exposure", "holdcos", allow_stale=True))

    async def test_company_loader_persists_for_a_long_time(self):
        fetch = AsyncMock(return_value={"005930": {"name": "삼성전자", "market": "유가", "sector": "반도체"}})
        with patch.object(exposure_data.kind, "fetch_listed_companies", new=fetch):
            value = await exposure_data.load_listed_companies()
            exposure_data.reset()
            again = await exposure_data.load_listed_companies()
        self.assertEqual(fetch.await_count, 1)
        self.assertEqual(again["companies"], value["companies"])
        self.assertTrue(value["as_of"])

    async def test_preferred_pairs_accept_both_summary_and_legacy_spelling(self):
        config = [
            {"commonTicker": "005930.KS", "preferredTicker": "005935.KS"},
            {"commonTicker": "000880", "preferredTicker": "00088K"},
            {"commonTicker": "", "preferredTicker": "X"},
            "garbage",
        ]
        load = AsyncMock(return_value=({"lastUpdated": "2026-11-07 09:38:24"}, config))
        with patch.object(exposure_data.external_tools, "load_spread_pairs", new=load):
            value = await exposure_data.load_preferred_pairs()
            await exposure_data.load_preferred_pairs()
        self.assertEqual(value["pairs"], {"005935": "005930", "00088K": "000880"})
        self.assertEqual(value["as_of"], "2026-11-07")
        self.assertEqual(load.await_count, 1)

    async def test_instrument_types_fetch_only_unknown_symbols_and_persist(self):
        async def fetch(symbol, **_kwargs):
            return {"meta": {"instrumentType": {"VWO": "ETF", "AGNC": "EQUITY"}.get(symbol)}}

        fetch_mock = AsyncMock(side_effect=fetch)
        with patch.object(exposure_data.yahoo, "fetch_close_series", new=fetch_mock):
            first = await exposure_data.load_instrument_types(["VWO", "AGNC", "NOPE"])
            # 실패한 심볼만 다시 묻는다.
            second = await exposure_data.load_instrument_types(["VWO", "NOPE"])
            exposure_data.reset()  # 재시작 — DB 값으로 복원한다.
            third = await exposure_data.load_instrument_types(["AGNC"])
            self.assertEqual(await exposure_data.load_instrument_types([]), {})
        self.assertEqual(first, {"VWO": "ETF", "AGNC": "EQUITY"})
        self.assertEqual(second, {"VWO": "ETF"})
        self.assertEqual(third, {"AGNC": "EQUITY"})
        self.assertEqual([call.args[0] for call in fetch_mock.await_args_list], ["VWO", "AGNC", "NOPE", "NOPE"])

    async def test_instrument_type_timeout_is_not_fatal(self):
        with patch.object(exposure_data.yahoo, "fetch_close_series", new=AsyncMock(side_effect=TimeoutError)):
            self.assertEqual(await exposure_data.load_instrument_types(["VWO"]), {})

    async def test_preferred_pairs_failure_is_not_fatal(self):
        load = AsyncMock(side_effect=httpx.ConnectError("down"))
        with patch.object(exposure_data.external_tools, "load_spread_pairs", new=load):
            value = await exposure_data.load_preferred_pairs()
        self.assertFalse(value["available"])


# ---------------------------------------------------------------------------
# 조립·계좌 필터·라우트
# ---------------------------------------------------------------------------

def _request(query: bytes = b"") -> Request:
    return Request({
        "type": "http",
        "method": "GET",
        "path": "/api/portfolio/exposure",
        "headers": [],
        "query_string": query,
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
        "scheme": "http",
    })


SOURCES = {
    "load_etf_holdings": {"available": True, "as_of": "2026-09-30", "stale": False, "etfs": {
        "069500": {"name": "KODEX 200", "asset_class": "주식", "unweighted": 0, "rows": [["005930", "삼성전자", 50.0]]},
    }},
    "load_holdcos": {"available": True, "as_of": "2026-09-14", "holdcos": {
        "000670": {"name": "영풍", "adjusted_shares": 1000,
                   "subsidiaries": [{"code": "010130", "name": "고려아연", "shares_held": 100}]},
    }},
    "load_preferred_pairs": {"available": True, "as_of": "2026-11-07", "pairs": {"005935": "005930"}},
    "load_listed_companies": {"available": True, "as_of": "2026-11-08", "companies": {
        "005930": {"name": "삼성전자", "market": "유가", "sector": "통신 및 방송 장비 제조업"},
        "000670": {"name": "영풍", "market": "유가", "sector": "1차 비철금속 제조업"},
        "010130": {"name": "고려아연", "market": "유가", "sector": "1차 비철금속 제조업"},
    }},
}
PRICES = {"005935": 1000.0, "069500": 2000.0, "000670": 500.0, "010130": 10000.0, "CASH_KRW": 1.0}
SUB = "u1"


class ExposureServiceTests(TempDbMixin):
    async def seed(self):
        exposure_service.reset()
        exposure_data.reset()
        self.addCleanup(exposure_service.reset)
        self.addCleanup(exposure_data.reset)
        await seed_user()
        db = await db_repo.get_db()
        await db.execute(
            "INSERT INTO portfolio_accounts (account_id, google_sub, name, sort_order, created_at, updated_at) "
            "VALUES ('acc-a', ?, '계좌 A', 1, '2026-01-01', '2026-01-01'), ('acc-b', ?, '계좌 B', 2, '2026-01-01', '2026-01-01')",
            (SUB, SUB),
        )
        await db.execute("INSERT OR IGNORE INTO account_holdings_initialized VALUES (?)", (SUB,))
        holdings = [
            ("acc-a", "005935", "삼성전자우", 10),
            ("acc-a", "069500", "KODEX 200", 5),
            ("acc-a", "CASH_KRW", "원화", 1000),
            ("acc-b", "000670", "영풍", 20),
        ]
        for account, code, name, quantity in holdings:
            await db.execute(
                "INSERT INTO account_holdings VALUES (?,?,?,?,?,?,?,?,?,?)",
                (SUB, account, code, name, quantity, 1.0, "KRW", "KRW", "2026-01-01", "2026-01-01"),
            )
            await db.execute(
                "INSERT OR REPLACE INTO user_portfolio (google_sub, stock_code, stock_name, quantity, avg_price, "
                "created_at, updated_at, currency, avg_price_currency) VALUES (?,?,?,?,?,?,?,?,?)",
                (SUB, code, name, quantity, 1.0, "2026-01-01", "2026-01-01", "KRW", "KRW"),
            )
        await db.commit()

    def _patches(self, quotes=None, closes=None):
        patches = [
            patch.object(exposure_data, name, new=AsyncMock(return_value=value)) for name, value in SOURCES.items()
        ]

        async def complete_quotes(codes):
            source = PRICES if quotes is None else quotes
            return {code: {"price": source[code]} for code in codes if code in source}

        patches.append(patch.object(exposure_service.quote_service, "complete_quotes", new=complete_quotes))
        closes_mock = AsyncMock(return_value={} if closes is None else closes)
        patches.append(patch.object(exposure_service.close_price, "get_daily_prices_batch", new=closes_mock))
        self.closes_mock = closes_mock
        return patches

    async def _report(self, account_id=None, **kwargs):
        patches = self._patches(**kwargs)
        for item in patches:
            item.start()
        try:
            return await exposure_service.get_exposure_report(SUB, account_id)
        finally:
            for item in patches:
                item.stop()

    async def test_all_accounts_combined_report(self):
        report = await self._report()
        totals = report["totals"]
        # 우선주 10주 × 1000 + ETF 5주 × 2000 + 영풍 20주 × 500 + 현금 1000.
        self.assertEqual(totals["total_value"], 31000)
        self.assertEqual(totals["cash_value"], 1000)
        self.assertEqual(totals["invested_value"], 30000)
        rows = {row["code"]: row for row in report["exposures"]["rows"]}
        self.assertEqual(rows["005930"]["direct_value"], 10000)
        self.assertEqual(rows["005930"]["etf_value"], 5000)
        self.assertEqual(report["holdcos"]["rows"][0]["look_through_value"], 20000)  # 20/1000×100×10000
        self.assertIsNone(report["account_id"])
        self.assertTrue(report["computed_at"])
        ids = {row["id"]: row for row in report["sources"]}
        self.assertEqual(ids["eiayn"]["as_of"], "2026-09-30")
        self.assertTrue(ids["eiayn"]["stale"])
        self.assertFalse(ids["finance_pi"]["available"])  # 일봉 없음은 숨기지 않는다.
        self.assertFalse(report["risk"]["available"])

    async def test_account_filter_uses_that_accounts_quantities_and_caches_separately(self):
        report_a = await self._report("acc-a")
        self.assertEqual(report_a["account_id"], "acc-a")
        self.assertEqual(report_a["totals"]["total_value"], 21000)
        self.assertEqual(report_a["holdcos"]["rows"], [])

        report_b = await self._report("acc-b")
        self.assertEqual(report_b["totals"]["total_value"], 10000)
        self.assertEqual(report_b["totals"]["cash_value"], 0)
        self.assertEqual(report_b["holdcos"]["rows"][0]["code"], "000670")

        # 캐시 키는 사용자·계좌별이다. 같은 계좌를 다시 부르면 다시 계산하지 않는다.
        changed = dict(PRICES, **{"005935": 9999999.0})
        cached_a = await self._report("acc-a", quotes=changed)
        self.assertEqual(cached_a["totals"]["total_value"], 21000)

    async def test_unknown_account_is_rejected(self):
        with self.assertRaises(accounts_repo.AccountError):
            await self._report("not-mine")

    async def test_daily_closes_request_only_domestic_codes_and_feed_risk(self):
        dates = [f"2026-03-{day:02d}" for day in range(1, 26)]
        closes = {
            "005935": [{"date": d, "close": 100.0 + (i % 3)} for i, d in enumerate(dates)],
            "069500": [{"date": d, "close": 200.0 - (i % 2)} for i, d in enumerate(dates)],
            "000670": [{"date": d, "close": 50.0 + (i % 4)} for i, d in enumerate(dates)],
        }
        report = await self._report(closes=closes)
        args = self.closes_mock.await_args
        self.assertEqual(sorted(args.args[0]), ["000670", "005935", "069500"])
        self.assertEqual(args.kwargs["fields"], ("close",))
        self.assertTrue(report["risk"]["available"])
        self.assertEqual(report["risk"]["sample_returns"], 24)
        self.assertAlmostEqual(sum(r["risk_contribution_pct"] for r in report["risk"]["rows"]), 100.0, places=2)
        ids = {row["id"]: row for row in report["sources"]}
        self.assertEqual(ids["finance_pi"]["as_of"], "2026-03-25")

    async def test_foreign_instrument_type_reaches_classification(self):
        db = await db_repo.get_db()
        await db.execute(
            "INSERT INTO account_holdings VALUES (?,?,?,?,?,?,?,?,?,?)",
            (SUB, "acc-a", "VWO", "신흥국 주식", 2, 1.0, "USD", "USD", "2026-01-01", "2026-01-01"),
        )
        await db.execute(
            "INSERT OR REPLACE INTO user_portfolio (google_sub, stock_code, stock_name, quantity, avg_price, "
            "created_at, updated_at, currency, avg_price_currency) VALUES (?,?,?,?,?,?,?,?,?)",
            (SUB, "VWO", "신흥국 주식", 2, 1.0, "2026-01-01", "2026-01-01", "USD", "USD"),
        )
        await db.commit()
        fetch = AsyncMock(return_value={"meta": {"instrumentType": "ETF"}})
        with patch.object(exposure_data.yahoo, "fetch_close_series", new=fetch):
            report = await self._report(quotes=dict(PRICES, VWO=70000.0))
        self.assertEqual(fetch.await_args.args[0], "VWO")
        rows = {row["code"]: row for row in report["etfs"]["rows"]}
        self.assertEqual(rows["VWO"]["status"], "not_in_universe")
        self.assertEqual(rows["VWO"]["coverage_pct"], 0)
        self.assertEqual(report["exposures"]["etf_unknown_value"], 145000)  # VWO 14만 + KODEX 미공개 절반 5천

    async def test_holdco_block_carries_its_file_date(self):
        report = await self._report()
        self.assertTrue(report["holdcos"]["estimated"])
        self.assertEqual(report["holdcos"]["as_of"], "2026-09-14")
        labels = {row["id"]: row["label"] for row in report["sources"]}
        self.assertEqual(labels["holding_value"], "지주사 지분(설정값)")

    async def test_closes_failure_leaves_risk_null(self):
        patches = self._patches()
        for item in patches:
            item.start()
        self.closes_mock.side_effect = exposure_service.close_price.ClosePriceClientError("down")
        try:
            report = await exposure_service.get_exposure_report(SUB)
        finally:
            for item in patches:
                item.stop()
        self.assertFalse(report["risk"]["available"])
        self.assertIsNone(report["risk"]["portfolio_volatility_pct"])
        self.assertEqual(report["totals"]["total_value"], 31000)

    async def test_snapshot_price_fills_missing_quote_and_is_marked_stale(self):
        db = await db_repo.get_db()
        await db.execute(
            "INSERT INTO portfolio_snapshots (google_sub, date, total_value, total_invested, nav, total_units) "
            "VALUES (?, '2026-11-06', 1, 1, 1000, 1)", (SUB,),
        )
        await db.execute(
            "INSERT INTO portfolio_stock_snapshots (google_sub, date, stock_code, market_value, quantity) "
            "VALUES (?, '2026-11-06', '000670', 40000, 20)", (SUB,),
        )
        await db.commit()
        quotes = {code: price for code, price in PRICES.items() if code != "000670"}
        report = await self._report(quotes=quotes)
        self.assertEqual(report["totals"]["unpriced_count"], 0)
        self.assertEqual(report["totals"]["stale_quotes"], 1)
        rows = {row["code"]: row for row in report["exposures"]["rows"]}
        self.assertEqual(rows["000670"]["direct_value"], 40000)  # 스냅샷 주당 2000 × 20주

    async def test_empty_portfolio_returns_empty_state(self):
        db = await db_repo.get_db()
        await db.execute("DELETE FROM user_portfolio")
        await db.execute("DELETE FROM account_holdings")
        await db.commit()
        report = await self._report()
        self.assertFalse(report["available"])
        self.assertEqual(report["totals"]["position_count"], 0)


class ExposureRouteTests(TempDbMixin):
    async def test_requires_login(self):
        with patch.object(exposure_route, "get_current_user", new=AsyncMock(return_value=None)):
            with self.assertRaises(HTTPException) as ctx:
                await exposure_route.get_portfolio_exposure(_request(), account_id=None)
        self.assertEqual(ctx.exception.status_code, 401)

    async def test_passes_user_and_trimmed_account_to_service(self):
        service = AsyncMock(return_value={"available": True})
        user = {"google_sub": SUB}
        with patch.object(exposure_route, "get_current_user", new=AsyncMock(return_value=user)), \
                patch.object(exposure_route.exposure_service, "get_exposure_report", new=service):
            result = await exposure_route.get_portfolio_exposure(_request(), account_id=" acc-a ")
            await exposure_route.get_portfolio_exposure(_request(), account_id="   ")
        self.assertEqual(result, {"available": True})
        self.assertEqual(service.await_args_list[0].args, (SUB, "acc-a"))
        self.assertEqual(service.await_args_list[1].args, (SUB, None))

    async def test_route_is_registered_on_the_app(self):
        from core.app_factory import create_app

        paths = {getattr(route, "path", None) for route in create_app().routes}
        self.assertIn("/api/portfolio/exposure", paths)
