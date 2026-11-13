"""스크리너 히스토리 테스트.

* ``domain/screener_history`` — 연도별 시계열 → 파생 지표(순수 규칙).
* ``services/screener_history`` — finance-pi 배치 수집, 배당 행 선택,
  백그라운드 수집·DB 캐시 수명주기. finance-pi 호출은 전부 mock 이다.
"""

from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin

from domain.screener_history import derive_history_metrics, most_common_latest_year
from repositories import cache_values
from services import screener_history
from services.market.sources import close_price, finance_pi
from services.market.sources.close_price import ClosePriceClientError


def _year(revenue=None, operating_profit=None, net_income=None, equity=None, liabilities=None, dps=None) -> dict:
    return {
        "revenue": revenue, "operating_profit": operating_profit, "net_income": net_income,
        "equity": equity, "liabilities": liabilities, "dps": dps,
    }


class DeriveHistoryMetricsTests(unittest.TestCase):
    def test_empty_series_yields_blank_metrics(self):
        out = derive_history_metrics({})
        self.assertIsNone(out["history_fy"])
        self.assertIsNone(out["turnaround_op"])
        self.assertIsNone(out["dps"])
        self.assertEqual(out["history"], [])

    def test_turnaround_is_loss_then_profit_and_growth_is_blank_on_negative_base(self):
        out = derive_history_metrics({
            2024: _year(revenue=100, operating_profit=-10, net_income=-20, equity=200, liabilities=100),
            2025: _year(revenue=120, operating_profit=5, net_income=3, equity=210, liabilities=90),
        })
        self.assertEqual(out["history_fy"], 2025)
        self.assertIs(out["turnaround_op"], True)
        self.assertIs(out["turnaround_net"], True)
        # 적자 → 흑자 구간의 증가율은 의미가 없어 비운다.
        self.assertIsNone(out["op_profit_growth"])
        self.assertAlmostEqual(out["revenue_growth"], 20.0)

    def test_staying_profitable_or_staying_in_loss_is_not_a_turnaround(self):
        profit = derive_history_metrics({
            2024: _year(operating_profit=10, net_income=5), 2025: _year(operating_profit=12, net_income=6),
        })
        self.assertIs(profit["turnaround_op"], False)
        loss = derive_history_metrics({
            2024: _year(operating_profit=-10, net_income=-5), 2025: _year(operating_profit=-2, net_income=-1),
        })
        self.assertIs(loss["turnaround_op"], False)
        # 직전 연도 자료가 없으면 판단하지 않는다.
        alone = derive_history_metrics({2025: _year(operating_profit=12, net_income=6)})
        self.assertIsNone(alone["turnaround_op"])

    def test_streaks_need_consecutive_years(self):
        series = {
            2021: _year(revenue=80, operating_profit=8, net_income=5),
            2022: _year(revenue=90, operating_profit=9, net_income=6),
            2023: _year(revenue=100, operating_profit=10, net_income=7),
            2024: _year(revenue=110, operating_profit=9, net_income=8),
            2025: _year(revenue=130, operating_profit=12, net_income=9),
        }
        out = derive_history_metrics(series)
        self.assertEqual(out["revenue_growth_years"], 4)
        self.assertEqual(out["op_profit_growth_years"], 1)  # 2024 에 꺾였다
        self.assertEqual(out["profit_years"], 5)
        self.assertAlmostEqual(out["revenue_cagr_3y"], ((130 / 90) ** (1 / 3) - 1) * 100)

        # 중간 연도(2023)가 비면 거기서 멈춘다 — 2022 와 건너뛰어 비교하지 않는다.
        gapped = {fy: values for fy, values in series.items() if fy != 2023}
        out = derive_history_metrics(gapped)
        self.assertEqual(out["revenue_growth_years"], 1)
        self.assertEqual(out["profit_years"], 2)
        # 3년 CAGR 은 양 끝 연도(2022·2025)만 쓰므로 그대로 계산된다.
        self.assertAlmostEqual(out["revenue_cagr_3y"], ((130 / 90) ** (1 / 3) - 1) * 100)

    def test_ratio_changes_and_streaks(self):
        out = derive_history_metrics({
            2023: _year(revenue=100, operating_profit=5, net_income=5, equity=100, liabilities=150),
            2024: _year(revenue=100, operating_profit=8, net_income=8, equity=100, liabilities=120),
            2025: _year(revenue=100, operating_profit=12, net_income=12, equity=100, liabilities=90),
        })
        self.assertAlmostEqual(out["fy_roe"], 12.0)
        self.assertAlmostEqual(out["roe_change"], 4.0)
        self.assertEqual(out["roe_up_years"], 2)
        self.assertAlmostEqual(out["op_margin_change"], 4.0)
        self.assertEqual(out["op_margin_up_years"], 2)
        self.assertAlmostEqual(out["debt_ratio_change"], -30.0)
        self.assertEqual(out["debt_ratio_down_years"], 2)

    def test_non_positive_equity_blanks_roe_and_debt_ratio(self):
        out = derive_history_metrics({
            2024: _year(net_income=5, equity=10, liabilities=50),
            2025: _year(net_income=5, equity=-3, liabilities=80),
        })
        self.assertIsNone(out["fy_roe"])
        self.assertIsNone(out["fy_debt_ratio"])
        self.assertIsNone(out["roe_change"])
        self.assertEqual(out["roe_up_years"], 0)

    def test_dividend_growth_and_no_cut_streaks(self):
        out = derive_history_metrics({
            2021: _year(net_income=1, dps=100),
            2022: _year(net_income=1, dps=100),
            2023: _year(net_income=1, dps=120),
            2024: _year(net_income=1, dps=150),
            2025: _year(net_income=1, dps=180),
        })
        self.assertEqual(out["dps"], 180)
        self.assertEqual(out["dps_fy"], 2025)
        self.assertAlmostEqual(out["dps_growth"], 20.0)
        self.assertEqual(out["dps_growth_years"], 3)  # 2022→2023 부터 3번 연속 인상
        self.assertEqual(out["dividend_no_cut_years"], 4)  # 동결(2021→2022)도 무삭감

    def test_dividend_cut_breaks_both_streaks(self):
        out = derive_history_metrics({
            2023: _year(net_income=1, dps=200),
            2024: _year(net_income=1, dps=250),
            2025: _year(net_income=1, dps=100),
        })
        self.assertEqual(out["dps_growth_years"], 0)
        self.assertEqual(out["dividend_no_cut_years"], 0)
        self.assertAlmostEqual(out["dps_growth"], -60.0)

    def test_no_dividend_in_latest_fiscal_year_means_not_paying(self):
        out = derive_history_metrics({
            2024: _year(net_income=1, dps=200),
            2025: _year(net_income=1),
        })
        self.assertIsNone(out["dps"])
        self.assertEqual(out["dps_growth_years"], 0)
        self.assertEqual(out["dividend_no_cut_years"], 0)

    def test_dividend_started_this_year_is_not_dividend_growth(self):
        out = derive_history_metrics({
            2024: _year(net_income=1),
            2025: _year(net_income=1, dps=300),
        })
        self.assertEqual(out["dps"], 300)
        self.assertIsNone(out["dps_growth"])
        self.assertEqual(out["dps_growth_years"], 0)

    def test_dividend_only_series_uses_latest_dividend_year(self):
        # 우선주: 재무는 없고 배당만 있다.
        series = {2023: _year(dps=1445), 2024: _year(dps=1447), 2025: _year(dps=1669)}
        out = derive_history_metrics(series, reference_year=2025)
        self.assertIsNone(out["history_fy"])
        self.assertEqual(out["dps"], 1669)
        self.assertEqual(out["dps_growth_years"], 2)
        self.assertEqual([h["fy"] for h in out["history"]], [2023, 2024, 2025])
        # 오래전에 끊긴 배당은 현재 배당으로 치지 않는다.
        stale = derive_history_metrics({2021: _year(dps=500), 2022: _year(dps=600)}, reference_year=2025)
        self.assertIsNone(stale["dps"])

    def test_stale_financials_are_not_reported_as_recent(self):
        series = {2021: _year(operating_profit=-5, net_income=-5), 2022: _year(operating_profit=9, net_income=9)}
        out = derive_history_metrics(series, reference_year=2025)
        self.assertIsNone(out["history_fy"])
        self.assertIsNone(out["turnaround_op"])
        # 한 해 늦는 것(3월 결산 등)은 허용한다.
        late = derive_history_metrics(
            {2023: _year(operating_profit=-5, net_income=-5), 2024: _year(operating_profit=9, net_income=9)},
            reference_year=2025,
        )
        self.assertEqual(late["history_fy"], 2024)
        self.assertIs(late["turnaround_op"], True)

    def test_history_table_shows_the_last_five_years(self):
        series = {fy: _year(revenue=fy, net_income=1) for fy in range(2019, 2026)}
        out = derive_history_metrics(series)
        self.assertEqual([h["fy"] for h in out["history"]], [2021, 2022, 2023, 2024, 2025])

    def test_most_common_latest_year(self):
        data = {
            "A": {2024: _year(net_income=1), 2025: _year(net_income=1)},
            "B": {2025: _year(net_income=1)},
            "C": {2026: _year(net_income=1)},  # 일찍 공시한 소수
            "D": {2025: _year(dps=100)},  # 배당만 있는 종목은 세지 않는다
        }
        self.assertEqual(most_common_latest_year(data), 2025)
        self.assertIsNone(most_common_latest_year({}))


def _metric(amount):
    return {"amount": amount, "fiscal_year": 2025}


class CollectHistoryTests(unittest.IsolatedAsyncioTestCase):
    def test_pick_dps_uses_the_latest_filing_per_fiscal_year(self):
        rows = [
            {"fiscal_year": 2024, "available_date": "2025-03-11", "cash_dividend_per_share": 1446.0},
            {"fiscal_year": 2024, "available_date": "2026-03-10", "cash_dividend_per_share": 1450.0},
            {"fiscal_year": 2025, "available_date": "2026-03-10", "cash_dividend_per_share": 1668.0},
            {"fiscal_year": 2023, "available_date": "2026-03-10", "cash_dividend_per_share": None},
            {"fiscal_year": "bad", "cash_dividend_per_share": 1.0},
        ]
        self.assertEqual(screener_history.pick_dps_by_year(rows), {2024: 1450.0, 2025: 1668.0, 2023: None})

    async def test_collects_financials_per_year_and_dividends_in_batches(self):
        async def fake_basic(tickers, *, fiscal_year=None, timeout=None, **_):
            return {
                code: {"metrics": {
                    "revenue": _metric(fiscal_year * 10), "net_income": _metric(fiscal_year),
                    "operating_profit": _metric(None), "assets": _metric(999),
                }}
                for code in tickers if code != "000003"
            }

        async def fake_dividends(tickers, *, start_year, end_year, timeout=None):
            return {"000001": [
                {"fiscal_year": 2025, "available_date": "2026-03-10", "cash_dividend_per_share": 500.0},
            ]} if "000001" in tickers else {}

        basic = AsyncMock(side_effect=fake_basic)
        dividends = AsyncMock(side_effect=fake_dividends)
        with patch.object(close_price, "get_basic_fundamentals", basic), \
             patch.object(close_price, "get_dividend_history", dividends), \
             patch.object(screener_history, "_BATCH_SIZE", 2), \
             patch.object(screener_history, "_BATCH_PAUSE_SECONDS", 0):
            data = await screener_history.collect_history(["000001", "000002", "000003"], [2024, 2025])

        # 3종목을 2개씩 → 2묶음 × 2개 연도, 배당은 묶음마다 한 번.
        self.assertEqual(basic.await_count, 4)
        self.assertEqual(dividends.await_count, 2)
        self.assertEqual(basic.await_args_list[0].args[0], ["000001", "000002"])
        self.assertEqual(basic.await_args_list[0].kwargs["fiscal_year"], 2024)
        self.assertEqual(dividends.await_args_list[0].kwargs, {"start_year": 2024, "end_year": 2025, "timeout": 60.0})
        self.assertEqual(data["000001"][2025]["revenue"], 20250)
        self.assertEqual(data["000001"][2025]["dps"], 500.0)
        self.assertIsNone(data["000001"][2025]["operating_profit"])
        self.assertNotIn("assets", data["000001"][2025])
        self.assertNotIn("dps", data["000002"][2025])
        self.assertNotIn("000003", data)

    async def test_collection_stops_when_finance_pi_is_cooling_down(self):
        basic = AsyncMock(return_value={})
        with patch.object(close_price, "get_basic_fundamentals", basic), \
             patch.object(finance_pi, "cooldown_active", return_value=True):
            with self.assertRaises(ClosePriceClientError):
                await screener_history.collect_history(["000001"], [2025])
        basic.assert_not_awaited()


class HistoryLifecycleTests(TempDbMixin):
    """get_history — 백그라운드 수집, DB 캐시, 만료 뒤 재수집, 실패 처리."""

    DATA = {"000001": {2025: {"revenue": 10.0, "operating_profit": 1.0, "net_income": 1.0,
                              "equity": 5.0, "liabilities": 2.0, "dps": 100.0}}}

    async def test_disabled_finance_pi_reports_unavailable_without_collecting(self):
        collect = AsyncMock(return_value=self.DATA)
        with patch.object(close_price, "ENABLED", False), patch.object(screener_history, "collect_history", collect):
            state = await screener_history.get_history(["000001"])
        self.assertEqual(state.status, "unavailable")
        collect.assert_not_awaited()

    async def test_first_call_builds_in_background_then_serves_from_cache(self):
        collect = AsyncMock(return_value=self.DATA)
        with patch.object(close_price, "ENABLED", True), patch.object(screener_history, "collect_history", collect):
            first = await screener_history.get_history(["000001"])
            self.assertEqual(first.status, "building")
            self.assertIsNone(first.data)
            await screener_history.wait_for_build()
            ready = await screener_history.get_history(["000001"])
            self.assertEqual(ready.status, "ready")
            self.assertEqual(ready.data, self.DATA)
            self.assertFalse(ready.refreshing)
            self.assertIsNotNone(ready.built_at)

            # 프로세스 재시작(메모리 비움) 뒤에도 DB 캐시에서 바로 읽는다 — 다시 모으지 않는다.
            screener_history.reset_state()
            again = await screener_history.get_history(["000001"])
            self.assertEqual(again.status, "ready")
            self.assertEqual(again.data, self.DATA)
            self.assertEqual(again.built_at, ready.built_at)
        collect.assert_awaited_once()
        # 올해 포함 7개 사업연도를 묻는다.
        years = collect.await_args.args[1]
        self.assertEqual(len(years), screener_history.FISCAL_YEAR_SPAN)
        self.assertEqual(years, sorted(years))

    async def test_expired_cache_is_served_while_refreshing(self):
        await cache_values.set_cache_value(
            screener_history._CACHE_NS, screener_history._CACHE_KEY,
            {"built_at": "2026-11-01T06:00:00+09:00", "fiscal_years": [2025],
             "tickers": {"000001": {"2025": [1.0, 1.0, 1.0, 1.0, 1.0, 50.0]}}},
            ttl_seconds=-1,
        )
        collect = AsyncMock(return_value=self.DATA)
        with patch.object(close_price, "ENABLED", True), patch.object(screener_history, "collect_history", collect):
            stale = await screener_history.get_history(["000001"])
            self.assertEqual(stale.status, "ready")
            self.assertTrue(stale.refreshing)
            self.assertEqual(stale.data["000001"][2025]["dps"], 50.0)
            await screener_history.wait_for_build()
            fresh = await screener_history.get_history(["000001"])
        self.assertEqual(fresh.data["000001"][2025]["dps"], 100.0)
        self.assertFalse(fresh.refreshing)
        collect.assert_awaited_once()

    async def test_failed_or_empty_collection_is_not_cached_and_backs_off(self):
        for collect in (
            AsyncMock(side_effect=ClosePriceClientError("boom")),
            AsyncMock(return_value={}),
        ):
            screener_history.reset_state()
            with patch.object(close_price, "ENABLED", True), patch.object(screener_history, "collect_history", collect):
                await screener_history.get_history(["000001"])
                await screener_history.wait_for_build()
                after = await screener_history.get_history(["000001"])
                self.assertEqual(after.status, "unavailable")
                # 재시도 간격 안에는 다시 모으지 않는다.
                await screener_history.get_history(["000001"])
                await screener_history.wait_for_build()
            collect.assert_awaited_once()
            self.assertIsNone(
                await cache_values.get_cache_value_entry(
                    screener_history._CACHE_NS, screener_history._CACHE_KEY, allow_stale=True
                )
            )


class DividendHistoryClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_requests_year_range_and_returns_rows_by_requested_code(self):
        get_json = AsyncMock(return_value={"dividends": {
            "005930": [{"fiscal_year": 2025, "cash_dividend_per_share": 1668.0}],
            "000660": [],
        }})
        with patch.object(close_price, "ENABLED", True), patch.object(close_price, "_get_json", get_json), \
             patch.object(finance_pi, "cooldown_active", return_value=False):
            rows = await close_price.get_dividend_history(["005930", "000660"], start_year=2020, end_year=2026)
        self.assertEqual(list(rows), ["005930"])
        path, params = get_json.await_args.args
        self.assertEqual(path, "/api/fundamentals/dividends")
        self.assertEqual(params, {"tickers": "005930,000660", "start_year": 2020, "end_year": 2026})

    async def test_http_failure_becomes_client_error(self):
        import httpx

        with patch.object(close_price, "ENABLED", True), \
             patch.object(close_price, "_get_json", AsyncMock(side_effect=httpx.ConnectError("down"))), \
             patch.object(finance_pi, "cooldown_active", return_value=False):
            with self.assertRaises(ClosePriceClientError):
                await close_price.get_dividend_history(["005930"], start_year=2020, end_year=2026)

    async def test_disabled_client_returns_nothing(self):
        get_json = AsyncMock()
        with patch.object(close_price, "ENABLED", False), patch.object(close_price, "_get_json", get_json):
            self.assertEqual(await close_price.get_dividend_history(["005930"], start_year=2020, end_year=2026), {})
        get_json.assert_not_awaited()
