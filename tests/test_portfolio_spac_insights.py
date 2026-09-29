import copy
import unittest
from contextlib import ExitStack
from datetime import date
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import Response

import external_tools
from routes import portfolio as pf
from services.portfolio import spac

ITEM = {
    "code": "0209J0", "name": "KB제34호스팩", "ipoPrice": 2000,
    "currentPrice": 1900, "listingDate": "2026-09-22",
    "liquidationValuePerShare": 2200, "payoutDate": "2027-09-29",
    "escrowRatePeriods": [{"startDate": "2026-09-15", "ratePct": 2.38}],
    "valuationBasis": {"trustStartDate": "2026-09-15", "trustFeePct": 0, "interestTaxPct": 15.4},
}
DATA = {"spacs": [ITEM], "lastUpdated": "2026-09-29 09:00 KST", "valuationAssumptions": {}}


class SpacValuationTests(unittest.TestCase):
    def test_current_value_is_accrued_not_future_payout_and_uses_popup_price(self):
        context = {"item": ITEM, "assumptions": {}, "updatedAt": DATA["lastUpdated"]}
        result = spac.build_spac_insight(context, {"price": 1882}, as_of=date(2026, 9, 29))
        current = 2000 * (1 + 0.0238 * 0.846 * 14 / 365)
        self.assertEqual(result["currentLiquidationValue"], round(current, 2))
        self.assertEqual(result["liquidationDiscountPct"], round((current - 1882) / current * 100, 2))
        self.assertEqual(result["annualizedReturnPct"], round((2200 / 1882 - 1) * 100, 2))
        self.assertEqual(result["listingDate"], "2026-09-22")

    def test_rate_changes_rollovers_and_disclosed_anchor(self):
        item = {
            "ipoPrice": 2000,
            "valuationBasis": {"trustStartDate": "2023-01-01", "trustFeePct": 0, "interestTaxPct": 0},
            "escrowRatePeriods": [
                {"startDate": "2023-01-01", "ratePct": 5},
                {"startDate": "2024-07-01", "ratePct": 3},
            ],
        }
        expected = 2000 * 1.05 * (1 + .05 * 182 / 365) * (1 + .03 * 184 / 365)
        self.assertAlmostEqual(spac.current_liquidation_value(item, date(2025, 1, 1), {}), expected)
        item["valuationBasis"]["anchor"] = {"date": "2024-07-01", "valuePerShare": 2150}
        self.assertAlmostEqual(spac.current_liquidation_value(item, date(2025, 1, 1), {}),
                               2150 * (1 + .03 * 184 / 365))
        # A future anchor and rate change cannot affect an earlier valuation.
        self.assertAlmostEqual(spac.current_liquidation_value(item, date(2024, 1, 1), {}), 2100)

    def test_missing_rates_and_expired_payout_do_not_invent_values(self):
        item = copy.deepcopy(ITEM)
        item["escrowRatePeriods"] = []
        result = spac.build_spac_insight({"item": item, "assumptions": {}}, {"price": 0},
                                         as_of=date(2027, 9, 29))
        self.assertIsNone(result["currentLiquidationValue"])
        self.assertIsNone(result["liquidationDiscountPct"])
        self.assertIsNone(result["annualizedReturnPct"])
        self.assertEqual(result["price"], 1900)
        self.assertEqual(result["listingDate"], "2026-09-22")
        item["escrowRatePeriods"] = [{"startDate": "2028-01-01", "ratePct": 5}]
        self.assertIsNone(spac.current_liquidation_value(item, date(2027, 9, 29), {}))


class SpacSourceTests(unittest.IsolatedAsyncioTestCase):
    def tearDown(self):
        external_tools._raw_cache.clear()

    async def test_fetch_cached_full_data_and_retry_after_failure(self):
        external_tools._raw_cache.clear()
        with patch.object(external_tools, "_get_json", new=AsyncMock(side_effect=[httpx.ConnectError("offline"), DATA])) as fetch:
            self.assertEqual(await external_tools.fetch_spac_data(), {})
            self.assertEqual(await external_tools.fetch_spac_data(), DATA)
            self.assertEqual(await external_tools.fetch_spac_data(), DATA)
        self.assertEqual(fetch.await_count, 2)
        self.assertTrue(fetch.await_args.args[0].endswith("/spac-hunter/main/data.json"))

    async def test_stale_fallback_retains_source_timestamp(self):
        external_tools._raw_cache.set("spac-hunter/data", DATA, ttl_seconds=-1)
        with patch.object(external_tools, "_get_json", new=AsyncMock(side_effect=httpx.ConnectError("offline"))):
            self.assertEqual(await external_tools.fetch_spac_data(), DATA)

    async def test_non_spac_skips_fetch_and_unknown_spac_keeps_empty_cards(self):
        with patch.object(external_tools, "fetch_spac_data", new=AsyncMock(return_value=DATA)) as fetch:
            self.assertIsNone(await spac.fetch_spac_context("005930", "삼성전자"))
            fetch.assert_not_awaited()
            context = await spac.fetch_spac_context("0209J0", "KB제34호스팩")
            self.assertEqual(context["item"]["code"], "0209J0")
            context = await spac.fetch_spac_context("123450", "신규스팩")
            result = spac.build_spac_insight(context, None)
            self.assertTrue(result["applicable"])
            self.assertIsNone(result["currentLiquidationValue"])

    async def test_endpoint_replaces_only_spac_fundamentals(self):
        for code, name, is_spac in [("0209J0", "KB제34호스팩", True), ("005930", "삼성전자", False)]:
            item = {"stock_code": code, "stock_name": name, "currency": "KRW", "quantity": 10, "avg_price": 1900}
            with ExitStack() as stack:
                def mock(obj, attr, value):
                    return stack.enter_context(patch.object(obj, attr, new=AsyncMock(return_value=value)))
                mock(pf, "get_current_user", {"google_sub": "test"})
                mock(pf.portfolio_repo, "get_portfolio", [item])
                mock(pf.portfolio_repo, "get_portfolio_tags", [])
                mock(pf.portfolio_repo, "get_portfolio_tag_suggestions", [])
                mock(pf.fx, "annotate_avg_price_krw", None)
                mock(pf.insights, "fetch_quote_for_insight", {"price": 1882})
                mock(pf.insights, "asset_history_for_insight", {"rows": [], "currency": "KRW"})
                mock(pf.insights, "resolve_insight_benchmark", "IDX_KOSPI")
                mock(pf.insights, "benchmark_history_for_insight", [])
                mock(pf.insights, "fetch_insight_indicators", {})
                basis = mock(pf.insights, "fetch_insight_valuation_basis", {"applicable": True, "eps": 100, "bps": 2000})
                mock(pf, "_fetch_benchmark_quote", {})
                mock(pf, "_resolve_benchmark_name", "코스피")
                source = mock(external_tools, "fetch_spac_data", DATA)
                mock(external_tools, "etf_link_for", None)
                stack.enter_context(patch.object(pf.insights, "gold_gap_for_asset", return_value=None))
                stack.enter_context(patch.object(pf.insights, "holding_context_for_asset", return_value=None))
                result = await pf.asset_insight(code, object(), Response())
            self.assertEqual(result["profile"]["isSpac"], is_spac)
            if is_spac:
                basis.assert_not_awaited()
                source.assert_awaited_once()
                self.assertFalse(result["valuation"]["applicable"])
                self.assertEqual(result["spac"]["listingDate"], "2026-09-22")
            else:
                basis.assert_awaited_once()
                source.assert_not_awaited()
                self.assertIsNone(result["spac"])
                self.assertEqual(result["valuation"]["per"], 18.82)
