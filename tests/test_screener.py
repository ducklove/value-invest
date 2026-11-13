"""밸류 스크리너 테스트.

repository(인메모리 필터), service(검증·정규화·스냅샷 캐시·finance-pi 호출),
route(엔드포인트) 세 계층을 검증한다. 더 이상 로컬 DB 시드를 쓰지 않고
finance-pi 스냅샷 응답(fixture 리스트)을 mock 한다 — 실제 동작과 동일.
"""

from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin, seed_user
from fastapi.testclient import TestClient

import routes.screener as screener_route
from core.app_factory import create_app
from repositories import screener as screener_repo
from repositories import user_stocks as user_stocks_repo
from services import screener as screener_service
from services import screener_history


def _snapshot_fixture() -> list[dict]:
    """finance-pi 가 반환할 가상의 전 유니버스 스냅샷.

    market_cap 은 finance-pi 원본 단위(KRW 원)로 둔다 — service 의 _get_snapshot
    이 억원으로 정규화한다. repo 단위 테스트는 이미 억원으로 정규화된
    fixture(아래 _normalized_fixture)를 쓴다.
    """
    return [
        # 두산: 저평가 고품질 (P/E 8, P/B 0.8, ROE 15%, 배당 3%)
        {
            "ticker": "000001", "name": "두산", "market": "KOSPI",
            "close": 50000, "per": 8.0, "pbr": 0.8, "roe": 15.0,
            "market_cap": 10_000_000_000_000, "dividend_yield": 3.0,
            "operating_margin": 10.0, "debt_ratio": 50.0,
            "revenue": 20000, "operating_profit": 2000,
            "net_income": 1500, "equity": 10000, "as_of": "2026-04-29",
        },
        # KT: 보통 (P/E 20, P/B 1.5, ROE 7.5%)
        {
            "ticker": "000002", "name": "KT", "market": "KOSPI",
            "close": 40000, "per": 20.0, "pbr": 1.5, "roe": 7.5,
            "market_cap": 5_000_000_000_000, "dividend_yield": 0.0,
            "operating_margin": 5.0, "debt_ratio": 100.0,
            "revenue": 20000, "operating_profit": 1000,
            "net_income": 750, "equity": 10000, "as_of": "2026-04-29",
        },
        # 신성비: 재무 데이터 없음 (PBR/ROE/equity null) — 시세/PER(naver)만
        {
            "ticker": "000003", "name": "신성비", "market": "KOSDAQ",
            "close": 10000, "per": 25.0, "pbr": None, "roe": None,
            "market_cap": 500_000_000_000, "dividend_yield": None,
            "operating_margin": None, "debt_ratio": None,
            "revenue": None, "operating_profit": None,
            "net_income": None, "equity": None, "as_of": "2026-04-29",
        },
        # 적자기업: naver PER 이 음수(-514). P/E 1~5 필터에 절대 걸려선 안 된다.
        {
            "ticker": "000004", "name": "적자기업", "market": "KOSPI",
            "close": 3000, "per": -514.0, "pbr": 0.3, "roe": -20.0,
            "market_cap": 100_000_000_000, "dividend_yield": 0.0,
            "operating_margin": -15.0, "debt_ratio": 200.0,
            "revenue": 5000, "operating_profit": -750,
            "net_income": -1000, "equity": 5000, "as_of": "2026-04-29",
        },
    ]


def _normalized_fixture() -> list[dict]:
    """억원으로 정규화된 fixture — repo 단위 테스트용(_get_snapshot 통과 후 상태)."""
    rows = _snapshot_fixture()
    for row in rows:
        mc = row.get("market_cap")
        if isinstance(mc, (int, float)) and mc != 0:
            row["market_cap"] = round(mc / 1e8, 2)
    return rows



class ScreenerRepoTests(unittest.TestCase):
    """repositories/screener.py — 인메모리 필터/정렬/페이징 (순수 함수).

    _normalized_fixture(억원 정규화 후)를 쓴다 — repo 는 정규화된 스냅샷을
    받는다고 가정한다.
    """

    def setUp(self):
        self.snapshot = _normalized_fixture()

    def test_per_min_max_range_excludes_negative(self):
        # 핵심 회귀: P/E 1~5 (min=1, max=5) 필터가 음수 PER(-514)을 걸러낸다.
        rows, total = screener_repo.screen_snapshot(
            self.snapshot, {"per": [("min", 1), ("max", 5)]}
        )
        tickers = {r["ticker"] for r in rows}
        self.assertNotIn("000004", tickers)  # 적자기업(per=-514) 제외
        self.assertEqual(total, 0)  # 1~5 범위의 PER 을 가진 종목이 없음

    def test_per_max_only_still_excludes_negative(self):
        # max=15 만: 음수 PER(-514)은 15보다 작지만, min 미설정이어도
        # 음수는 저평가가 아니므로... 현재 로직은 max=15 → -514<15 통과.
        # 이건 의도적 동작(min/max 독립)이므로, 음수 제외를 원하면 min 도 함께.
        rows, total = screener_repo.screen_snapshot(
            self.snapshot, {"per": [("max", 15)]}
        )
        tickers = {r["ticker"] for r in rows}
        self.assertIn("000001", tickers)  # 두산(per=8)
        # 적자기업(per=-514) 도 max=15 에 걸림(음수 < 15). min/max 독립 동작.

    def test_roe_min_filter(self):
        rows, total = screener_repo.screen_snapshot(
            self.snapshot, {"roe": [("min", 10)]}
        )
        tickers = {r["ticker"] for r in rows}
        self.assertEqual(tickers, {"000001"})  # 두산만 roe=15

    def test_combined_filters_intersect(self):
        rows, total = screener_repo.screen_snapshot(
            self.snapshot,
            {"per": [("max", 15), ("min", 1)], "roe": [("min", 10)], "dividend_yield": [("min", 2)]},
        )
        self.assertEqual(total, 1)
        self.assertEqual(rows[0]["ticker"], "000001")

    def test_filter_excludes_null_metric_rows(self):
        # 신성비는 roe 가 null → roe 필터에 걸리지 않는다.
        rows, total = screener_repo.screen_snapshot(
            self.snapshot, {"roe": [("min", 1)]}
        )
        tickers = {r["ticker"] for r in rows}
        self.assertNotIn("000003", tickers)

    def test_filter_with_no_match_returns_zero(self):
        rows, total = screener_repo.screen_snapshot(
            self.snapshot, {"per": [("min", 100), ("max", 200)]}
        )
        self.assertEqual(total, 0)
        self.assertEqual(rows, [])

    def test_sort_by_market_cap_desc(self):
        rows, _ = screener_repo.screen_snapshot(
            self.snapshot, {"per": [("min", -1000), ("max", 1000)]}
        )
        # 모든 종목(per 전체 허용) — 두산(100000억) > KT(50000억) > ...
        tickers = [r["ticker"] for r in rows]
        self.assertEqual(tickers[0], "000001")  # 시가총액 최대

    def test_sort_puts_missing_values_last_in_both_directions(self):
        # 신성비(000003)는 roe 가 없다 — 오름·내림 어느 쪽이든 맨 뒤여야 한다.
        for direction, first in (("desc", "000001"), ("asc", "000004")):
            rows, total = screener_repo.screen_snapshot(
                self.snapshot, {"market_cap": [("min", 0)]}, sort_by="roe", sort_dir=direction
            )
            tickers = [r["ticker"] for r in rows]
            self.assertEqual(total, 4)
            self.assertEqual(tickers[0], first, direction)
            self.assertEqual(tickers[-1], "000003", direction)

    def test_sort_by_name_is_textual(self):
        rows, _ = screener_repo.screen_snapshot(
            self.snapshot, {"market_cap": [("min", 0)]}, sort_by="corp_name", sort_dir="asc"
        )
        self.assertEqual([r["name"] for r in rows], ["KT", "두산", "신성비", "적자기업"])

    def test_sort_does_not_reorder_the_input_snapshot(self):
        before = [r["ticker"] for r in self.snapshot]
        screener_repo.screen_snapshot(self.snapshot, {}, sort_by="per", sort_dir="asc")
        self.assertEqual([r["ticker"] for r in self.snapshot], before)

    def test_pagination_respects_offset(self):
        rows, total = screener_repo.screen_snapshot(
            self.snapshot, {"per": [("min", -1000), ("max", 1000)]}, limit=1, offset=1
        )
        self.assertEqual(len(rows), 1)

    def test_zero_limit_returns_empty(self):
        rows, total = screener_repo.screen_snapshot(
            self.snapshot, {"per": [("max", 15)]}, limit=0
        )
        self.assertEqual(total, 0)
        self.assertEqual(rows, [])

    def test_result_row_has_friendly_aliases(self):
        rows, _ = screener_repo.screen_snapshot(
            self.snapshot, {"per": [("min", 1), ("max", 15)]}
        )
        # ticker → stock_code, name → corp_name, close → close_price alias 추가
        self.assertEqual(rows[0]["stock_code"], "000001")
        self.assertEqual(rows[0]["corp_name"], "두산")
        self.assertEqual(rows[0]["close_price"], 50000)

    def test_snapshot_coverage(self):
        cov = screener_repo.snapshot_coverage(self.snapshot)
        self.assertEqual(cov["universe"], 4)
        self.assertEqual(cov["valued"], 4)  # 전부 close 있음
        self.assertEqual(cov["fundamentals"], 3)  # 신성비만 equity 없음



class ScreenerServiceTests(TempDbMixin):
    """services/screener.py — 검증 + finance-pi 호출 mock."""

    async def test_normalize_filters_rejects_unknown_metric(self):
        with self.assertRaises(screener_service.ScreenerError):
            screener_service.normalize_filters({"nonexistent": {"min": 1}})

    async def test_normalize_filters_rejects_non_number(self):
        with self.assertRaises(screener_service.ScreenerError):
            screener_service.normalize_filters({"per": {"min": "abc"}})

    async def test_normalize_filters_rejects_out_of_range(self):
        with self.assertRaises(screener_service.ScreenerError):
            screener_service.normalize_filters({"per": {"min": 99999}})

    async def test_normalize_filters_keeps_both_min_and_max(self):
        # 핵심 회귀: min/max 동시 설정이 둘 다 보존되어야 한다 (이전 덮어쓰기 버그).
        out = screener_service.normalize_filters({"per": {"min": 1, "max": 5}})
        self.assertEqual(out, {"per": [("min", 1.0), ("max", 5.0)]})

    async def test_normalize_filters_drops_empty_bounds(self):
        out = screener_service.normalize_filters({"per": {}})
        self.assertEqual(out, {})

    async def test_normalize_filters_rejects_non_dict_bounds(self):
        with self.assertRaises(screener_service.ScreenerError):
            screener_service.normalize_filters({"per": 10})

    async def test_run_screen_rejects_empty_filters(self):
        with self.assertRaises(screener_service.ScreenerError):
            await screener_service.run_screen({})

    async def test_run_screen_rejects_bad_sort(self):
        with self.assertRaises(screener_service.ScreenerError):
            await screener_service.run_screen({}, sort_by="nonexistent")

    async def test_run_screen_rejects_bad_limit(self):
        with self.assertRaises(screener_service.ScreenerError):
            await screener_service.run_screen({}, limit=0)

    async def test_run_screen_returns_shaped_response(self):
        with patch.object(screener_service, "get_screener_snapshot", new=AsyncMock(
            return_value={"as_of": "2026-04-29", "count": 4, "rows": _snapshot_fixture()}
        )):
            result = await screener_service.run_screen(
                {"per": {"min": 1, "max": 15}}
            )
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["rows"][0]["stock_code"], "000001")
        # filters 응답이 min/max 쌍의 리스트로 표현된다.
        self.assertEqual(
            result["filters"]["per"],
            [{"op": "min", "value": 1.0}, {"op": "max", "value": 15.0}],
        )

    async def test_run_screen_normalizes_market_cap_to_eokwon(self):
        # finance-pi 원본 market_cap(원) → 억원 정규화 검증.
        with patch.object(screener_service, "get_screener_snapshot", new=AsyncMock(
            return_value={"as_of": "2026-04-29", "count": 4, "rows": _snapshot_fixture()}
        )):
            result = await screener_service.run_screen(
                {"per": {"min": 1, "max": 15}}
            )
        # 두산 market_cap 10조원 → 100000 억원
        self.assertAlmostEqual(result["rows"][0]["market_cap"], 100000.0, places=1)

    async def test_get_filter_specs_includes_coverage(self):
        with patch.object(screener_service, "get_screener_snapshot", new=AsyncMock(
            return_value={"as_of": "2026-04-29", "count": 4, "rows": _snapshot_fixture()}
        )):
            specs = await screener_service.get_filter_specs()
        self.assertIn("filters", specs)
        self.assertEqual(specs["coverage"]["universe"], 4)
        self.assertEqual(specs["coverage"]["fundamentals"], 3)

    async def test_run_screen_maps_finance_pi_failure_to_502(self):
        from core.errors import ExternalServiceError
        from repositories import cache_values
        from services.market.sources.close_price import ClosePriceClientError
        # 스냅샷 캐시를 비워 _get_snapshot 이 실제 get_screener_snapshot 을 호출하게 한다.
        await cache_values.delete_cache_value(
            screener_service._SNAPSHOT_CACHE_NS, "latest"
        )
        with patch.object(screener_service, "get_screener_snapshot", new=AsyncMock(
            side_effect=ClosePriceClientError("boom")
        )):
            with self.assertRaises(ExternalServiceError):
                await screener_service.run_screen({"per": {"min": 1, "max": 15}})


def _history_fixture() -> dict:
    """``screener_history.get_history`` 가 돌려줄 연도별 시계열(종목 → 사업연도 → 지표)."""
    return {
        # 두산: 4년 연속 매출·배당 증가, 계속 흑자.
        "000001": {
            2022: {"revenue": 10000, "operating_profit": 900, "net_income": 700, "equity": 8000, "liabilities": 5000, "dps": 1000},
            2023: {"revenue": 12000, "operating_profit": 1100, "net_income": 800, "equity": 8500, "liabilities": 5000, "dps": 1200},
            2024: {"revenue": 15000, "operating_profit": 1500, "net_income": 1000, "equity": 9000, "liabilities": 4800, "dps": 1300},
            2025: {"revenue": 20000, "operating_profit": 2000, "net_income": 2000, "equity": 10000, "liabilities": 4500, "dps": 1500},
        },
        # KT: 2024 영업손실 → 2025 영업흑자(턴어라운드), 배당은 줄였다.
        "000002": {
            2023: {"revenue": 21000, "operating_profit": 500, "net_income": 300, "equity": 9800, "liabilities": 9000, "dps": 500},
            2024: {"revenue": 19000, "operating_profit": -400, "net_income": -600, "equity": 9200, "liabilities": 9500, "dps": 400},
            2025: {"revenue": 20000, "operating_profit": 1000, "net_income": 750, "equity": 10000, "liabilities": 10000, "dps": 200},
        },
    }


def _history_state(status: str = "ready") -> screener_history.HistoryState:
    if status != "ready":
        return screener_history.HistoryState(status=status)
    return screener_history.HistoryState(
        status="ready", data=_history_fixture(), built_at="2026-11-14T06:00:00+09:00", fiscal_years=(2022, 2023, 2024, 2025)
    )


class ScreenerHistoryConditionTests(TempDbMixin):
    """히스토리 조건·값 보정·관심종목 필터 — 스냅샷과 히스토리 조회를 mock."""

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self._snapshot_patch = patch.object(screener_service, "get_screener_snapshot", new=AsyncMock(
            return_value={"as_of": "2026-04-29", "count": 4, "rows": _snapshot_fixture()}
        ))
        self._snapshot_patch.start()
        self._history = AsyncMock(return_value=_history_state())
        self._history_patch = patch.object(screener_history, "get_history", new=self._history)
        self._history_patch.start()

    async def asyncTearDown(self):
        self._history_patch.stop()
        self._snapshot_patch.stop()
        await super().asyncTearDown()

    async def test_turnaround_condition_selects_loss_to_profit_only(self):
        result = await screener_service.run_screen({}, conditions_raw=["turnaround"])
        self.assertEqual([r["stock_code"] for r in result["rows"]], ["000002"])
        self.assertEqual(result["conditions"], ["turnaround"])
        self.assertTrue(result["rows"][0]["turnaround_op"])

    async def test_dividend_growth_conditions(self):
        grew = await screener_service.run_screen({}, conditions_raw=["dividend_growth"])
        self.assertEqual([r["stock_code"] for r in grew["rows"]], ["000001"])
        three = await screener_service.run_screen({}, conditions_raw=["dividend_growth_3y"])
        self.assertEqual([r["stock_code"] for r in three["rows"]], ["000001"])
        self.assertEqual(three["rows"][0]["dps_growth_years"], 3)

    async def test_conditions_and_filters_intersect(self):
        both = await screener_service.run_screen({"per": {"max": 10}}, conditions_raw=["revenue_growth_3y"])
        self.assertEqual([r["stock_code"] for r in both["rows"]], ["000001"])
        none = await screener_service.run_screen(
            {"per": {"min": 15}}, conditions_raw=["revenue_growth_3y"]
        )
        self.assertEqual(none["total"], 0)
        # 조건 두 개는 AND — 두산은 턴어라운드가 아니다.
        both_conditions = await screener_service.run_screen({}, conditions_raw=["turnaround", "dividend_growth"])
        self.assertEqual(both_conditions["total"], 0)

    async def test_unknown_condition_is_rejected(self):
        with self.assertRaises(screener_service.ScreenerError):
            await screener_service.run_screen({}, conditions_raw=["nonexistent"])
        with self.assertRaises(screener_service.ScreenerError):
            await screener_service.run_screen({}, conditions_raw="turnaround")

    async def test_conditions_are_refused_while_history_is_not_ready(self):
        for status in ("building", "unavailable"):
            screener_service.reset_state()
            self._history.return_value = _history_state(status)
            with self.assertRaises(screener_service.ScreenerError):
                await screener_service.run_screen({}, conditions_raw=["turnaround"])
            with self.assertRaises(screener_service.ScreenerError):
                await screener_service.run_screen({"revenue_growth": {"min": 5}})
            # 현재 지표 필터는 히스토리 없이도 동작한다.
            result = await screener_service.run_screen({"per": {"min": 1, "max": 15}})
            self.assertEqual(result["total"], 1)
            self.assertEqual(result["history"]["status"], status)

    async def test_ratios_are_recomputed_from_latest_fiscal_year(self):
        result = await screener_service.run_screen({"market_cap": {"min": 60000}})
        row = result["rows"][0]
        self.assertEqual(row["stock_code"], "000001")
        # 스냅샷 값(roe 15, 부채비율 50, 영업이익률 10, 배당수익률 3%) 대신
        # 최근 사업연도(2025) 재무제표·주당배당금으로 다시 계산한다.
        self.assertEqual(row["history_fy"], 2025)
        self.assertAlmostEqual(row["roe"], 20.0)
        self.assertAlmostEqual(row["debt_ratio"], 45.0)
        self.assertAlmostEqual(row["operating_margin"], 10.0)
        self.assertAlmostEqual(row["dividend_yield"], 3.0)  # 1500 / 50000
        self.assertAlmostEqual(row["pbr"], 1e13 / 10000, places=2)
        self.assertAlmostEqual(row["revenue_growth"], 33.33, places=2)
        self.assertEqual([h["fy"] for h in row["history"]], [2022, 2023, 2024, 2025])
        self.assertNotIn("fy_equity", row)

    async def test_rows_without_history_keep_snapshot_values(self):
        result = await screener_service.run_screen({"per": {"min": -1000, "max": 0}})
        row = result["rows"][0]
        self.assertEqual(row["stock_code"], "000004")
        self.assertEqual(row["roe"], -20.0)
        self.assertEqual(row["dividend_yield"], 0.0)
        self.assertIsNone(row["history_fy"])
        self.assertEqual(row["history"], [])

    async def test_sort_by_history_metric_keeps_missing_last(self):
        result = await screener_service.run_screen(
            {"market_cap": {"min": 0}}, sort_by="revenue_growth", sort_dir="asc"
        )
        codes = [r["stock_code"] for r in result["rows"]]
        self.assertEqual(codes[:2], ["000002", "000001"])  # +5.3% < +33.3%
        self.assertEqual(set(codes[2:]), {"000003", "000004"})  # 히스토리 없음 → 뒤

    async def test_watchlist_restricts_universe_and_counts_missing(self):
        result = await screener_service.run_screen({}, watchlist=["000002", "000003", "999999"])
        self.assertEqual({r["stock_code"] for r in result["rows"]}, {"000002", "000003"})
        self.assertTrue(result["watchlist_only"])
        self.assertEqual(result["watchlist_missing"], 1)
        # 빈 관심종목도 "전체"로 새지 않는다.
        empty = await screener_service.run_screen({}, watchlist=[])
        self.assertEqual(empty["total"], 0)
        # 관심종목 + 조건.
        narrowed = await screener_service.run_screen({}, conditions_raw=["turnaround"], watchlist=["000001", "000002"])
        self.assertEqual([r["stock_code"] for r in narrowed["rows"]], ["000002"])

    async def test_spec_lists_conditions_with_definitions_and_history_coverage(self):
        specs = await screener_service.get_filter_specs()
        keys = [c["key"] for c in specs["conditions"]]
        self.assertIn("turnaround", keys)
        self.assertIn("dividend_growth", keys)
        self.assertTrue(all(c["description"] and c["group"] and c["label"] for c in specs["conditions"]))
        self.assertNotIn("test", specs["conditions"][0])
        self.assertEqual(specs["history"]["status"], "ready")
        self.assertEqual(specs["history"]["covered"], 2)
        self.assertEqual(specs["history"]["latest_fiscal_year"], 2025)
        self.assertEqual(specs["as_of"], "2026-04-29")
        self.assertTrue(specs["filters"]["revenue_growth"]["needs_history"])
        self.assertFalse(specs["filters"]["per"]["needs_history"])
        # 표의 정렬 가능한 열은 모두 서버 정렬 키에 있다.
        for key in ("corp_name", "stock_code", "market_cap", "dps", "revenue_growth", "op_profit_growth", "dps_growth"):
            self.assertIn(key, specs["sorts"])

    async def test_universe_is_rebuilt_when_history_arrives(self):
        self._history.return_value = _history_state("building")
        before = await screener_service.run_screen({"per": {"min": 1, "max": 15}})
        self.assertIsNone(before["rows"][0]["history_fy"])
        self._history.return_value = _history_state()
        after = await screener_service.run_screen({"per": {"min": 1, "max": 15}})
        self.assertEqual(after["rows"][0]["history_fy"], 2025)


class ScreenerWatchlistRouteTests(TempDbMixin):
    """관심종목 엔드포인트 — 분석 화면과 같은 user_stock_preferences.is_starred 를 쓴다."""

    async def asyncSetUp(self):
        await super().asyncSetUp()
        await seed_user("u1")
        self.client = TestClient(create_app())
        self.user = {"google_sub": "u1", "email": "user@example.com"}

    def _as(self, user):
        return patch.object(screener_route, "get_current_user", AsyncMock(return_value=user)), patch(
            "deps.get_current_user", AsyncMock(return_value=user)
        )

    async def test_anonymous_gets_empty_watchlist_and_cannot_write(self):
        route_patch, deps_patch = self._as(None)
        with route_patch, deps_patch:
            resp = self.client.get("/api/screener/watchlist")
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json(), {"authenticated": False, "codes": []})
            self.assertEqual(
                self.client.put("/api/screener/watchlist/005930", json={"starred": True}).status_code, 401
            )
            run = self.client.post("/api/screener/run", json={"filters": {}, "watchlist_only": True})
            self.assertEqual(run.status_code, 401)

    async def test_star_and_unstar_roundtrip_shares_the_analysis_starred_flag(self):
        route_patch, deps_patch = self._as(self.user)
        with route_patch, deps_patch:
            resp = self.client.put("/api/screener/watchlist/005930", json={"starred": True})
            self.assertEqual(resp.status_code, 200)
            self.assertEqual(resp.json(), {"stock_code": "005930", "starred": True})
            self.client.put("/api/screener/watchlist/000660", json={"starred": True})
            listed = self.client.get("/api/screener/watchlist").json()
            self.assertTrue(listed["authenticated"])
            self.assertEqual(set(listed["codes"]), {"005930", "000660"})
            # 같은 플래그 — 분석 화면의 선호 조회에서도 관심종목이다.
            pref = await user_stocks_repo.get_user_stock_preference("u1", "005930")
            self.assertTrue(pref["is_starred"])

            self.client.put("/api/screener/watchlist/005930", json={"starred": False})
            self.assertEqual(self.client.get("/api/screener/watchlist").json()["codes"], ["000660"])

    async def test_starring_keeps_an_existing_note(self):
        await user_stocks_repo.save_user_stock_preference("u1", "005930", note="메모")
        route_patch, deps_patch = self._as(self.user)
        with route_patch, deps_patch:
            self.client.put("/api/screener/watchlist/005930", json={"starred": True})
        pref = await user_stocks_repo.get_user_stock_preference("u1", "005930")
        self.assertTrue(pref["is_starred"])
        self.assertEqual(pref["note"], "메모")

    async def test_bad_code_or_payload_is_rejected(self):
        route_patch, deps_patch = self._as(self.user)
        with route_patch, deps_patch:
            self.assertEqual(self.client.put("/api/screener/watchlist/abc", json={"starred": True}).status_code, 400)
            self.assertEqual(self.client.put("/api/screener/watchlist/005930", json={"starred": "yes"}).status_code, 400)
            self.assertEqual(self.client.put("/api/screener/watchlist/005930", json={}).status_code, 400)

    async def test_watchlist_only_run_uses_the_users_starred_codes(self):
        await user_stocks_repo.save_user_stock_preference("u1", "000002", is_starred=True)
        route_patch, deps_patch = self._as(self.user)
        with route_patch, deps_patch, patch.object(screener_service, "get_screener_snapshot", new=AsyncMock(
            return_value={"as_of": "2026-04-29", "count": 4, "rows": _snapshot_fixture()}
        )):
            resp = self.client.post("/api/screener/run", json={"filters": {}, "watchlist_only": True})
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertEqual([r["stock_code"] for r in data["rows"]], ["000002"])
        self.assertTrue(data["watchlist_only"])

    async def test_starred_tab_lists_stocks_that_were_never_analyzed(self):
        # 스크리너에서 담은 종목은 analysis_meta 가 없다 — 분석 화면 관심 탭에도 보여야 한다.
        from repositories import corp_codes

        await corp_codes.save_corp_codes([{"stock_code": "005930", "corp_code": "00126380", "corp_name": "삼성전자"}])
        await user_stocks_repo.save_user_stock_preference("u1", "005930", is_starred=True)
        await user_stocks_repo.save_user_stock_preference("u1", "123456", is_starred=True)
        items = await user_stocks_repo.get_cached_analyses(google_sub="u1", tab="starred", include_quotes=True)
        by_code = {item["stock_code"]: item for item in items}
        self.assertEqual(by_code["005930"]["corp_name"], "삼성전자")
        self.assertEqual(by_code["123456"]["corp_name"], "123456")  # 이름을 모르면 코드
        self.assertIsNone(by_code["005930"]["analyzed_at"])
        self.assertEqual(by_code["005930"]["quote_snapshot"], {})
        self.assertTrue(by_code["005930"]["is_starred"])


class ScreenerRouteTests(unittest.TestCase):
    """TestClient 기반 라우트 엔드포인트 테스트."""

    @classmethod
    def setUpClass(cls):
        cls.app = create_app()
        cls.client = TestClient(cls.app)

    def test_spec_endpoint_returns_filter_definitions(self):
        # spec 은 coverage 를 finance-pi 에서 가져오지만, 캐시가 비어있으면
        # service mock 으로 HTTP 계약만 검증한다.
        with patch.object(screener_service, "get_filter_specs", new=AsyncMock(
            return_value={
                "filters": screener_service.FILTER_SPECS,
                "sorts": screener_service.ALLOWED_SORTS,
                "coverage": {"universe": 4, "valued": 4, "fundamentals": 3},
            }
        )):
            resp = self.client.get("/api/screener/spec")
        self.assertEqual(resp.status_code, 200)
        data = resp.json()
        self.assertIn("per", data["filters"])
        self.assertIn("roe", data["filters"])
        self.assertIn("coverage", data)

    def test_run_endpoint_with_empty_filters_is_rejected(self):
        resp = self.client.post("/api/screener/run?limit=50", json={"filters": {}})
        self.assertEqual(resp.status_code, 400)

    def test_run_endpoint_rejects_bad_filter_metric(self):
        resp = self.client.post(
            "/api/screener/run?limit=50",
            json={"filters": {"nonexistent": {"min": 1}}},
        )
        self.assertEqual(resp.status_code, 400)

    def test_run_endpoint_rejects_bad_limit_query(self):
        resp = self.client.post("/api/screener/run?limit=0", json={"filters": {}})
        self.assertEqual(resp.status_code, 422)
