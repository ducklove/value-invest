import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from repositories import portfolio as portfolio_repo
from routes import portfolio as pf
from services.market.sources import close_price as close_price_client
from services.market.sources import kis_proxy as kis_proxy_client
from services.portfolio import insights
from services.portfolio import valuation as valuation_service


def reported_basis(eps=6000, bps=80000, roe=12.5):
    period = {"period_start": "2025-07-01", "period_end": "2026-06-30", "quarter_count": 4}
    return {
        "eps_ttm": eps, "bps": bps, "roe_ttm": roe,
        "earnings_basis": "ttm", "equity_basis": "latest", "roe_basis": "ttm",
        "period_end": "2026-06-30", "source": "NAVER Finance",
        "earnings": period, "equity": {"period_end": "2026-06-30"}, "roe": period,
    }


class AssetInsightHistorySourceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        insights.asset_history_cache.clear()
        valuation_service._insight_basis_cache.clear()

    async def asyncTearDown(self):
        insights.asset_history_cache.clear()
        valuation_service._insight_basis_cache.clear()

    async def test_korean_history_prefers_internal_close_api(self):
        local_rows = [{"date": "2026-05-07", "close": 12660.0}]

        with (
            patch.object(close_price_client, "get_daily_closes", new=AsyncMock(return_value=local_rows)) as local,
            patch.object(kis_proxy_client, "get_history", new=AsyncMock()) as kis,
        ):
            result = await insights.download_korean_history("003200", period_days=30)

        self.assertEqual(result["currency"], "KRW")
        self.assertEqual(result["rows"], [{"date": "2026-05-07", "close": 12660.0}])
        local.assert_awaited_once()
        kis.assert_not_awaited()

    async def test_korean_history_falls_back_to_kis_when_internal_api_is_empty(self):
        kis_payload = {"items": [{"stck_bsop_date": "20260507", "stck_clpr": "12660"}]}

        with (
            patch.object(close_price_client, "get_daily_closes", new=AsyncMock(return_value=[])),
            patch.object(kis_proxy_client, "get_history", new=AsyncMock(return_value=kis_payload)) as kis,
        ):
            result = await insights.download_korean_history("003200", period_days=30)

        self.assertEqual(result["rows"], [{"date": "2026-05-07", "close": 12660.0}])
        kis.assert_awaited_once()

    async def test_kospi_benchmark_history_prefers_internal_macro_api(self):
        local_rows = [{"date": "2026-05-07", "close": 3200.5}]

        with (
            patch.object(close_price_client, "get_macro_index", new=AsyncMock(return_value=local_rows)) as local,
            patch.object(insights, "download_yfinance_history", new=AsyncMock()) as yahoo,
        ):
            rows = await insights.benchmark_history_for_insight("IDX_KOSPI")

        self.assertEqual(rows, [{"date": "2026-05-07", "close": 3200.5}])
        local.assert_awaited_once()
        yahoo.assert_not_awaited()

    async def test_kosdaq_and_sp500_benchmark_history_use_internal_macro_api(self):
        local_rows = [{"date": "2026-05-07", "close": 1000.0}]

        for benchmark_code, series_id in (("IDX_KOSDAQ", "KOSDAQ"), ("IDX_SP500", "SP500")):
            insights.asset_history_cache.clear()
            with (
                patch.object(close_price_client, "get_macro_index", new=AsyncMock(return_value=local_rows)) as local,
                patch.object(insights, "download_yfinance_history", new=AsyncMock()) as yahoo,
            ):
                rows = await insights.benchmark_history_for_insight(benchmark_code)

            self.assertEqual(rows, local_rows)
            self.assertEqual(local.await_args.kwargs.get("since") is not None, True)
            self.assertEqual(local.await_args.args[0], series_id)
            yahoo.assert_not_awaited()

    async def test_local_benchmark_history_falls_back_when_internal_macro_api_is_empty(self):
        yf_rows = [{"date": "2026-05-07", "close": 3201.0}]

        with (
            patch.object(close_price_client, "get_macro_index", new=AsyncMock(return_value=[])),
            patch.object(insights, "download_yfinance_history", new=AsyncMock(return_value={"rows": yf_rows})),
        ):
            rows = await insights.benchmark_history_for_insight("IDX_KOSPI")

        self.assertEqual(rows, yf_rows)

    async def test_benchmark_quotes_child_cancellation_returns_stale_quote(self):
        pf._benchmark_quote_cache.clear()
        pf._benchmark_quote_cache.set("IDX_KOSPI", {"change_pct": 1.2})

        with (
            patch.object(pf, "get_current_user", new=AsyncMock(return_value={"google_sub": "u1"})),
            patch.object(portfolio_repo, "get_portfolio", new=AsyncMock(return_value=[
                {"stock_code": "005930", "stock_name": "삼성전자", "benchmark_code": "IDX_KOSPI"}
            ])),
            patch.object(pf.corp_codes, "load_corp_code_table", new=AsyncMock(return_value={})),
            patch.object(pf, "_fetch_benchmark_quote", new=AsyncMock(side_effect=asyncio.CancelledError())),
        ):
            result = await pf.get_benchmark_quotes(object())

        self.assertEqual(result["IDX_KOSPI"]["change_pct"], 1.2)
        self.assertEqual(result["IDX_KOSPI"]["name"], pf._resolve_benchmark_name_fast("IDX_KOSPI"))

    async def test_insight_valuation_uses_common_stock_fundamentals_for_preferred(self):
        fundamentals = {
            "005930": {
                "fiscal_year": 2025,
                "as_of": "2026-05-10",
                "metrics": {
                    "net_income": {"amount": 44_000_000},
                    "equity": {"amount": 400_000_000},
                },
                "per_share": {
                    "eps_ttm": {"value": 5000, "treasury_shares_excluded": True},
                    "bps": {
                        "value": 50000,
                        "treasury_shares_excluded": True,
                        "components": [
                            {
                                "stock_kind": "보통주",
                                "issued_shares": 1000,
                                "treasury_shares": 100,
                                "outstanding_shares": 900,
                            },
                            {
                                "stock_kind": "우선주",
                                "issued_shares": 200,
                                "treasury_shares": 20,
                                "outstanding_shares": 180,
                            },
                        ],
                    },
                },
            }
        }

        with (
            patch.object(valuation_service, "fetch_current_valuation_basis", new=AsyncMock(return_value=reported_basis())) as quarterly,
            patch.object(close_price_client, "get_basic_fundamentals", new=AsyncMock(return_value=fundamentals)) as local,
            patch.object(portfolio_repo, "get_latest_market_valuation", new=AsyncMock(return_value={})),
            patch.object(portfolio_repo, "upsert_market_target_metrics", new=AsyncMock(return_value=None)) as write,
        ):
            basis = await insights.fetch_insight_valuation_basis("005935")
            valuation = insights.build_insight_valuation({"price": 40000}, basis)

        local.assert_awaited_once()
        self.assertEqual(local.await_args.args[0], ["005930"])
        self.assertEqual(valuation["sourceCode"], "005930")
        self.assertEqual(quarterly.await_args.args[0], "005930")
        self.assertEqual(valuation["per"], 6.67)
        self.assertEqual(valuation["pbr"], 0.5)
        self.assertEqual(valuation["roe"], 12.5)
        self.assertEqual(valuation["eps"], 6000)
        self.assertEqual(valuation["bps"], 80000)
        self.assertEqual(valuation["earningsPeriod"]["period_end"], "2026-06-30")
        self.assertEqual(valuation["treasuryFiscalYear"], 2025)
        write.assert_not_awaited()
        self.assertEqual(valuation["treasuryShareRatioPct"], 10.0)
        self.assertEqual(valuation["treasuryShares"], 120)
        self.assertEqual(valuation["issuedShares"], 1200)
        self.assertEqual(valuation["outstandingShares"], 1080)

    async def test_insight_valuation_prefers_quarterly_inputs_over_annual_fundamentals_and_cache(self):
        fundamentals = {
            "009770": {
                "fiscal_year": 2025,
                "as_of": "2026-05-10",
                "metrics": {
                    "net_income": {"amount": 250_000_000},
                    "equity": {"amount": 180_000_000_000},
                },
                "per_share": {
                    "eps_ttm": {"value": 100, "treasury_shares_excluded": True},
                    "bps": {"value": 72000, "treasury_shares_excluded": True},
                },
            }
        }
        cached = {"year": 2024, "eps": 1, "bps": 1, "net_income": 1, "total_equity": 1}

        cache_lookup = AsyncMock(return_value=cached)
        with (
            patch.object(valuation_service, "fetch_current_valuation_basis", new=AsyncMock(return_value=reported_basis(eps=200, bps=100000))),
            patch.object(close_price_client, "get_basic_fundamentals", new=AsyncMock(return_value=fundamentals)),
            patch.object(portfolio_repo, "get_latest_market_valuation", new=cache_lookup),
            patch.object(portfolio_repo, "upsert_market_target_metrics", new=AsyncMock(return_value=None)),
        ):
            basis = await insights.fetch_insight_valuation_basis("009770")
            valuation = insights.build_insight_valuation({"price": 36000}, basis)

        cache_lookup.assert_not_awaited()
        self.assertEqual(valuation["bps"], 100000)
        self.assertEqual(valuation["eps"], 200)
        self.assertEqual(valuation["pbr"], 0.36)
        self.assertEqual(valuation["per"], 180.0)
        self.assertEqual(valuation["source"], "NAVER Finance")

    async def test_quarterly_failure_does_not_fall_back_to_annual_or_cached_ratios(self):
        annual = {"005930": {"per_share": {"eps_annual": {"value": 5000}, "bps": {"value": 50000}}}}
        with (
            patch.object(valuation_service, "fetch_current_valuation_basis", new=AsyncMock(side_effect=RuntimeError("offline"))),
            patch.object(close_price_client, "get_basic_fundamentals", new=AsyncMock(return_value=annual)),
            patch.object(portfolio_repo, "get_latest_market_valuation", new=AsyncMock(return_value={"per": 10, "pbr": 1})) as cache,
        ):
            basis = await insights.fetch_insight_valuation_basis("005930")
            valuation = insights.build_insight_valuation({"price": 50000}, basis)
        self.assertIsNone(valuation["per"])
        self.assertIsNone(valuation["pbr"])
        self.assertIsNone(valuation["roe"])
        cache.assert_not_awaited()

    async def test_current_financials_survive_treasury_provider_failure(self):
        with (
            patch.object(valuation_service, "fetch_current_valuation_basis", new=AsyncMock(return_value=reported_basis())),
            patch.object(close_price_client, "get_basic_fundamentals", new=AsyncMock(side_effect=RuntimeError("offline"))),
        ):
            valuation = insights.build_insight_valuation({"price": 40000}, await insights.fetch_insight_valuation_basis("005930"))
        self.assertEqual(valuation["per"], 6.67)
        self.assertEqual(valuation["roe"], 12.5)
        self.assertIsNone(valuation["treasuryShareRatioPct"])

    async def test_foreign_asset_skips_domestic_financial_providers(self):
        with patch.object(valuation_service, "fetch_current_valuation_basis", new=AsyncMock()) as fetch:
            self.assertEqual(await insights.fetch_insight_valuation_basis("AAPL"), {"applicable": False})
        fetch.assert_not_awaited()

    def test_legacy_annual_fields_and_eps_bps_roe_approximation_are_rejected(self):
        annual = {"applicable": True, "eps": 100, "bps": 1000, "netIncome": 100, "equity": 1000,
                  "perFallback": 10, "pbrFallback": 1, "closePrice": 1000, "roe": 10}
        valuation = insights.build_insight_valuation({"price": 1000}, annual)
        self.assertIsNone(valuation["per"])
        self.assertIsNone(valuation["pbr"])
        self.assertIsNone(valuation["roe"])
        current = {"applicable": True, "eps": 100, "bps": 1000, "earningsBasis": "ttm", "equityBasis": "latest"}
        valuation = insights.build_insight_valuation({"price": 1000}, current)
        self.assertEqual(valuation["per"], 10)
        self.assertIsNone(valuation["roe"])
