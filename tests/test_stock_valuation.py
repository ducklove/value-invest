import copy
import unittest
from datetime import date
from unittest.mock import AsyncMock, Mock, patch

from services import stock_valuation


def quarterly_payload(periods=None, eps=None, bps=None):
    periods = periods or [("202506", "N"), ("202509", "N"), ("202512", "N"), ("202603", "N"), ("202606", "N"), ("202609", "Y")]
    eps = eps or {"202506": "999", "202509": "1,783", "202512": "2,864", "202603": "6,993", "202606": "10,718", "202609": "13,504"}
    bps = bps or {"202506": "58,135", "202509": "60,632", "202512": "63,997", "202603": "71,907", "202606": "86,052", "202609": "100,000"}
    return {
        "itemCode": "005930",
        "financePeriodType": "quarter",
        "financeInfo": {
            "itemCode": "005930",
            "trTitleList": [{"key": key, "isConsensus": flag} for key, flag in periods],
            "rowList": [
                {"title": "EPS", "columns": {key: {"value": value} for key, value in eps.items()}},
                {"title": "BPS", "columns": {key: {"value": value} for key, value in bps.items()}},
                {"title": "PER", "columns": {key: {"value": "999"} for key, _ in periods}},
            ],
        },
    }


class QuarterlyValuationTests(unittest.TestCase):
    def parse(self, payload):
        return stock_valuation.parse_quarterly_valuation(payload, "005930", today=date(2026, 10, 28))

    def test_four_actual_quarters_are_summed_and_latest_bps_is_one_point(self):
        result = self.parse(quarterly_payload())
        self.assertEqual(result["status"], "ready")
        self.assertEqual(result["eps_ttm"], 22358)
        self.assertEqual(result["bps"], 86052)
        self.assertEqual(result["earnings"]["period_start"], "2025-07-01")
        self.assertEqual(result["earnings"]["period_end"], "2026-06-30")
        self.assertEqual(result["equity"]["period_end"], "2026-06-30")
        self.assertEqual(result["earnings"]["quarter_count"], 4)
        self.assertEqual(result["earnings_basis"], "ttm")
        self.assertEqual(result["equity_basis"], "latest")
        self.assertIsNone(result["available_date"])
        self.assertIsNone(result["statement_basis"])

    def test_zero_and_loss_quarters_are_valid_inputs(self):
        payload = quarterly_payload()
        columns = payload["financeInfo"]["rowList"][0]["columns"]
        for key, value in {"202509": "0", "202512": "-100", "202603": "50", "202606": "-20"}.items():
            columns[key]["value"] = value
        result = self.parse(payload)
        self.assertEqual(result["eps_ttm"], -70)
        self.assertEqual(result["status"], "ready")

    def test_missing_latest_eps_does_not_select_an_older_complete_window(self):
        payload = quarterly_payload()
        payload["financeInfo"]["rowList"][0]["columns"]["202606"]["value"] = "-"
        result = self.parse(payload)
        self.assertIsNone(result["eps_ttm"])
        self.assertEqual(result["bps"], 86052)
        self.assertEqual(result["status"], "partial")

    def test_latest_missing_bps_does_not_fall_back_to_annual_or_older_quarter(self):
        payload = quarterly_payload()
        payload["financeInfo"]["rowList"][1]["columns"]["202606"]["value"] = "-"
        result = self.parse(payload)
        self.assertEqual(result["eps_ttm"], 22358)
        self.assertIsNone(result["bps"])
        self.assertEqual(result["reason"], "latest_reported_bps_unavailable")

    def test_missing_quarter_blocks_ttm(self):
        payload = quarterly_payload()
        payload["financeInfo"]["trTitleList"] = [column for column in payload["financeInfo"]["trTitleList"] if column["key"] != "202603"]
        result = self.parse(payload)
        self.assertIsNone(result["eps_ttm"])
        self.assertEqual(result["bps"], 86052)

    def test_forecasts_and_unknown_flags_are_excluded_even_when_numeric(self):
        payload = quarterly_payload()
        payload["financeInfo"]["trTitleList"][-2]["isConsensus"] = None
        result = self.parse(payload)
        self.assertEqual(result["period_end"], "2026-03-31")
        self.assertEqual(result["eps_ttm"], 999 + 1783 + 2864 + 6993)
        self.assertEqual(result["bps"], 71907)

    def test_future_period_marked_actual_is_excluded(self):
        payload = quarterly_payload()
        payload["financeInfo"]["trTitleList"].append({"key": "202612", "isConsensus": "N"})
        result = self.parse(payload)
        self.assertEqual(result["period_end"], "2026-06-30")

    def test_non_calendar_quarters_work_across_year_boundary(self):
        periods = [("202510", "N"), ("202601", "N"), ("202604", "N"), ("202607", "N")]
        values = {key: "100" for key, _ in periods}
        result = self.parse(quarterly_payload(periods=periods, eps=values, bps=values))
        self.assertEqual(result["eps_ttm"], 400)
        self.assertEqual(result["earnings"]["period_start"], "2025-08-01")
        self.assertEqual(result["earnings"]["period_end"], "2026-07-31")

    def test_annual_payload_is_not_accepted_as_ttm(self):
        payload = quarterly_payload()
        payload["financePeriodType"] = "annual"
        result = self.parse(payload)
        self.assertIsNone(result["eps_ttm"])
        self.assertIsNone(result["bps"])

    def test_old_provider_results_are_not_current(self):
        periods = [("202503", "N"), ("202506", "N"), ("202509", "N"), ("202512", "N")]
        result = self.parse(quarterly_payload(periods=periods))
        self.assertEqual(result["reason"], "reported_quarters_stale")
        self.assertIsNone(result["eps_ttm"])
        self.assertIsNone(result["bps"])

    def test_only_three_quarters_still_exposes_latest_bps(self):
        payload = quarterly_payload()
        payload["financeInfo"]["trTitleList"] = payload["financeInfo"]["trTitleList"][2:]
        result = self.parse(payload)
        self.assertEqual(result["bps"], 86052)
        self.assertIsNone(result["eps_ttm"])

    def test_symbol_mismatch_and_ambiguous_rows_fail_closed(self):
        for kind in ("symbol", "duplicate_row", "duplicate_column"):
            with self.subTest(kind=kind):
                payload = quarterly_payload()
                if kind == "symbol":
                    payload["itemCode"] = "000660"
                elif kind == "duplicate_row":
                    payload["financeInfo"]["rowList"].append(copy.deepcopy(payload["financeInfo"]["rowList"][0]))
                else:
                    payload["financeInfo"]["trTitleList"].append(copy.deepcopy(payload["financeInfo"]["trTitleList"][0]))
                result = self.parse(payload)
                self.assertEqual(result["status"], "unavailable")
                self.assertIsNone(result["eps_ttm"])
                self.assertIsNone(result["bps"])


class CurrentValuationFetchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        stock_valuation._cache.clear()

    async def test_fetch_is_cached_and_copies_values(self):
        response = Mock()
        response.json.return_value = quarterly_payload()
        client = Mock(get=AsyncMock(return_value=response))
        with patch.object(stock_valuation, "_today", return_value=date(2026, 10, 28)), patch.object(stock_valuation, "get_http_client", AsyncMock(return_value=client)):
            first = await stock_valuation.fetch_current_valuation_basis("005930")
            first["eps_ttm"] = 999
            second = await stock_valuation.fetch_current_valuation_basis("005930")
        self.assertEqual(second["eps_ttm"], 22358)
        self.assertIsNotNone(second["fetched_at"])
        client.get.assert_awaited_once()

    async def test_fetch_failure_is_unavailable_without_annual_fallback(self):
        with patch.object(stock_valuation, "_today", return_value=date(2026, 10, 28)), patch.object(stock_valuation, "get_http_client", AsyncMock(side_effect=RuntimeError("down"))):
            result = await stock_valuation.fetch_current_valuation_basis("005930")
        self.assertEqual(result["status"], "unavailable")
        self.assertIsNone(result["eps_ttm"])
        self.assertEqual(result["reason"], "quarterly_provider_unavailable")

    async def test_alphanumeric_preferred_code_is_normalized_and_supported(self):
        payload = quarterly_payload()
        payload["itemCode"] = payload["financeInfo"]["itemCode"] = "00088K"
        response = Mock()
        response.json.return_value = payload
        client = Mock(get=AsyncMock(return_value=response))
        with patch.object(stock_valuation, "_today", return_value=date(2026, 10, 28)), patch.object(stock_valuation, "get_http_client", AsyncMock(return_value=client)):
            result = await stock_valuation.fetch_current_valuation_basis(" 00088k ")
        self.assertEqual(result["stock_code"], "00088K")
        self.assertEqual(result["status"], "ready")
        self.assertIn("/00088K/", client.get.await_args.args[0])

    async def test_historical_cutoff_is_rejected_without_network(self):
        with patch.object(stock_valuation, "_today", return_value=date(2026, 10, 28)), patch.object(stock_valuation, "get_http_client", AsyncMock()) as get_client:
            result = await stock_valuation.fetch_current_valuation_basis("005930", as_of="2025-10-28")
        self.assertEqual(result["reason"], "historical_point_in_time_unavailable")
        get_client.assert_not_awaited()
