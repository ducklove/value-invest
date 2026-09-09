"""외부 원본 날짜 의미, 영속 캐시, 제한 시간과 장애 격리."""

import asyncio
import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import httpx
from _harness import TempDbMixin

from repositories.cache_values import get_cache_value_entry, set_cache_value
from services import dividend_sources as sources

AGNC_HTML = """<table><thead><tr><th>Month/Quarter</th><th>Declaration Date</th><th>Ex-Dividend Date</th><th>Record Date</th><th>Payment Date</th><th>Dividend Per Share</th></tr></thead><tbody><tr><td>August 2026</td><td>08/12/26</td><td>08/31/26</td><td>08/31/26</td><td>09/10/26</td><td>$0.12</td></tr></tbody></table>"""
SCHP_HTML = """<table><thead><tr><th></th><th></th><th>Ex-Date</th><th>Record Date</th><th>Payable Date</th><th>Income</th><th>Total Distribution</th></tr></thead><tbody><tr><th></th><td></td><td>09/01/2026</td><td>09/01/2026</td><td>09/08/2026</td><td>0.0773</td><td>0.077300000</td></tr><tr><th></th><td></td><td>02/02/2026</td><td>02/02/2026</td><td>02/06/2026</td><td>0</td><td>0</td></tr></tbody></table>"""


class ParserTests(unittest.TestCase):
    def test_kis_record_cash_payment_and_stock_dividend_are_not_interchangeable(self):
        rows = {"items": [{"sht_cd": "005930", "record_date": "20250630", "divi_pay_dt": "2025/08/20", "per_sto_divi_amt": "367"},
                          {"sht_cd": "005930", "record_date": "20260331", "stk_div_pay_dt": "2026/05/20", "per_sto_divi_amt": ""},
                          {"sht_cd": "005935", "record_date": "20260331", "per_sto_divi_amt": "368"}]}
        events = sources.parse_kis_dividends(rows, "005930")
        self.assertEqual(len(events), 2)
        self.assertEqual((events[0]["record_date"], events[0]["pay_date"], events[0]["amount_per_share"]), ("2025-06-30", "2025-08-20", 367))
        self.assertIsNone(events[1]["pay_date"])
        self.assertIsNone(events[1]["amount_per_share"])

    def test_agnc_payment_and_rights_dates_are_distinct(self):
        event = sources.parse_official_html(AGNC_HTML, "AGNC")[0]
        self.assertEqual((event["ex_date"], event["record_date"], event["pay_date"], event["declaration_date"]),
                         ("2026-08-31", "2026-08-31", "2026-09-10", "2026-08-12"))
        self.assertEqual(event["amount_per_share"], 0.12)

    def test_schwab_hidden_columns_duplicate_tables_and_zero_distribution(self):
        events = sources.parse_official_html(SCHP_HTML * 2, "SCHP")
        self.assertEqual(len(events), 2)
        self.assertEqual((events[0]["pay_date"], events[0]["amount_per_share"]), ("2026-09-08", 0.0773))
        self.assertEqual(events[1]["amount_per_share"], 0)

    def test_changed_html_is_failure_not_an_empty_dividend_history(self):
        with self.assertRaises(ValueError):
            sources.parse_official_html("<html>Unavailable</html>", "AGNC")

    def test_yahoo_timestamp_is_local_ex_date_never_payment(self):
        stamp = datetime(2026, 9, 1, 1, tzinfo=timezone.utc).timestamp()
        payload = {"chart": {"result": [{"meta": {"currency": "USD", "gmtoffset": -14400},
                                          "events": {"dividends": {"x": {"date": stamp, "amount": 0.12}, "bad": {"date": stamp, "amount": "NaN"}}}}]}}
        events = sources.parse_yahoo_chart(payload, "AGNC")
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["ex_date"], "2026-08-31")
        self.assertIsNone(events[0]["pay_date"])

    def test_empty_but_valid_yahoo_response_and_unavailable_are_different(self):
        self.assertEqual(sources.parse_yahoo_chart({"chart": {"result": [{"meta": {"currency": "USD"}}]}}, "TEST"), [])
        with self.assertRaises(ValueError):
            sources.parse_yahoo_chart({"chart": {"result": None}}, "TEST")


class CacheTests(TempDbMixin):
    async def test_success_is_persistent_and_reused(self):
        result = {"events": sources.parse_official_html(AGNC_HTML, "AGNC"), "fetched_at": "2026-09-10T00:00:00Z", "status": "fresh"}
        with patch.object(sources, "fetch_history", AsyncMock(return_value=result)) as fetch:
            first = await sources.get_histories(["AGNC", "AGNC.O"])
            second = await sources.get_histories(["AGNC"])
        self.assertEqual(fetch.await_count, 1)
        self.assertEqual(first["AGNC"], first["AGNC.O"])
        self.assertEqual(second["AGNC"], result)
        self.assertIsNotNone(await get_cache_value_entry(sources.NAMESPACE, "AGNC"))

    async def test_outage_keeps_original_timestamp_and_does_not_erase_history(self):
        old = {"events": sources.parse_official_html(AGNC_HTML, "AGNC"), "fetched_at": "2026-09-01T00:00:00Z", "status": "fresh"}
        await set_cache_value(sources.NAMESPACE, "AGNC", old, ttl_seconds=-1)
        with patch.object(sources, "fetch_history", AsyncMock(side_effect=httpx.ConnectError("offline"))) as fetch:
            first = await sources.get_histories(["AGNC", "SCHP"])
            second = await sources.get_histories(["AGNC", "SCHP"])
        self.assertEqual(first["AGNC"]["events"], old["events"])
        self.assertEqual(first["AGNC"]["fetched_at"], old["fetched_at"])
        self.assertEqual(first["AGNC"]["status"], "stale")
        self.assertEqual(first["SCHP"]["status"], "unavailable")
        self.assertEqual(first, second)
        self.assertEqual(fetch.await_count, 2)

    async def test_slow_ticker_does_not_discard_completed_ticker(self):
        async def fetch(ticker):
            if ticker == "SLOW":
                await asyncio.sleep(1)
            return {"events": [], "status": "fresh"}
        with patch.object(sources, "fetch_history", side_effect=fetch), patch.object(sources, "BATCH_TIMEOUT", 0.05):
            result = await sources.get_histories(["FAST", "SLOW"])
        self.assertEqual(result["FAST"]["status"], "fresh")
        self.assertEqual(result["SLOW"]["status"], "unavailable")

    async def test_kosdaq_fallback_after_ks_404(self):
        paths = []
        def handler(request):
            paths.append(request.url.path)
            if request.url.path.endswith(".KS"):
                return httpx.Response(404)
            return httpx.Response(200, json={"chart": {"result": [{"meta": {"currency": "KRW"}}]}})
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with patch.object(sources, "get_http_client", AsyncMock(return_value=client)), patch.object(sources.kis_proxy_client, "get_dividends", AsyncMock(return_value={"items": []})):
                result = await sources.fetch_history("123456.KS")
        self.assertEqual(paths, ["/v8/finance/chart/123456.KS", "/v8/finance/chart/123456.KQ"])
        self.assertEqual(result["events"], [])

    async def test_existing_payment_dates_survive_kis_fallback_to_ex_date_history(self):
        old = {"events": [{"pay_date": "2026-08-20"}], "official": True, "fetched_at": "2026-09-01", "status": "fresh"}
        await set_cache_value(sources.NAMESPACE, "005930.KS", old, ttl_seconds=-1)
        with patch.object(sources, "fetch_history", AsyncMock(return_value={"events": [], "official": False, "status": "fresh"})):
            result = await sources.get_histories(["005930"])
        self.assertEqual(result["005930"]["events"], old["events"])
        self.assertEqual(result["005930"]["status"], "stale")
