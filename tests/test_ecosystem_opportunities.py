"""기회 보드·종목 360°·액션 보드 백분위 (services/ecosystem/opportunities.py) — 소스 폴백과 도구별 실패 격리."""

import copy
import json
import os
import unittest
from datetime import date, datetime
from unittest.mock import AsyncMock, patch

import httpx
from test_external_tools_ecosystem import _Base, all_summaries, data, reset_caches, summary

from domain.timeutil import KST
from routes import stocks as stocks_route
from services import stock_quotes
from services.ecosystem import envelope, external_tools, live_cards, opportunities, siblings, signals

# 픽스처의 기준일(2026-09-23~28)에 맞춘 고정 '오늘'.
TODAY = date(2026, 9, 30)
NOW = datetime(2026, 9, 30, 18, 0, tzinfo=KST)

SIGNAL_KEYS = {"kind", "title", "detail", "url", "severity", "metric", "short_label"}


def republished(tool_id: str, strip: tuple[str, ...]) -> dict:
    """새 선택 필드가 아직 없는 현재 형제 summary(해시 재계산)."""
    env = summary(tool_id)
    for pair in env["data"].get("pairs") or []:
        for key in strip:
            pair.pop(key, None)
    for key in strip:
        env["data"].pop(key, None)
    env["contentHash"] = envelope.vc_publish().content_hash(env["data"])
    return env


CPS_FIELDS = ("spreadPct1y", "spreadPct3y", "spreadPctEnd", "attractiveness",
              "commonAvgTradedValue20", "preferredAvgTradedValue20")
HOLDING_FIELDS = ("ratioPct1y", "ratioPct3y", "ratioPctEnd")
NPS_FIELDS = ("positionsAsOf", "weights", "ownership", "ownershipChange")


def distribution(values: list[float]) -> dict:
    return {"date": "2026-09-23", "pctile3yValues": values, "pctile1yValues": values}


def dashboards() -> dict[str, object]:
    """형제가 지금 공개하는 대시보드 파일(data/summary.json)의 필요한 부분."""
    return {
        siblings.data_url(signals.TOOL_PREFERRED, "dashboard-summary"): {"pairs": [{
            "id": "samsung_elec",
            "attractiveness": {"total": 76.5},
            "current": {"commonAvgTradedValue20": 4e11, "preferredAvgTradedValue20": 1e10},
            "spreadPercentiles": distribution([float(v) for v in range(1, 101)]),  # 22.51 → 22
        }, {
            "id": "doosan_fuelcell",
            "attractiveness": {"total": 47.0},
            "current": {"preferredAvgTradedValue20": 55349725.0},
            "spreadPercentiles": distribution([float(v) for v in range(1, 91)]),   # 86.28 → 96
        }]},
        siblings.data_url(signals.TOOL_HOLDING, "dashboard-summary"): {"pairs": [{
            "id": "youngpoong_koreazinc",
            "current": {"pctile3y": 1},  # 멈춘 저장값
            "percentileHistory": {
                "dates": [f"2026-0{m}-{d:02d}" for m in range(1, 10) for d in range(1, 11)],
                "ratio": [float(100 + 5 * i) for i in range(90)],  # 전부 현재 비율(760.56) 아래
            },
        }]},
    }


class _Board(_Base):
    def setUp(self):
        super().setUp()
        self.quotes = {}
        self.today = patch.object(opportunities, "today_kst", return_value=TODAY)
        self.now = patch.object(opportunities, "now_kst", return_value=NOW)
        self.cross = patch.object(signals, "MIN_CROSS_SECTION", 3)  # 픽스처 종목 수가 적다
        self.domestic = patch.object(live_cards, "domestic_quotes", new=AsyncMock(side_effect=self._quotes))
        # 네트워크·실제 DB 를 타지 않게 막는다(자사주 summary 의 ratios 에는 이름이 없어 코드표를 조회한다).
        self.single_quote = patch.object(stock_quotes, "get_quote_snapshot", new=AsyncMock(return_value={}))
        self.names = patch.object(external_tools, "_buyback_stock_name", new=AsyncMock(return_value="허브종목명"))
        self.stubs = (self.today, self.now, self.cross, self.domestic, self.single_quote, self.names)
        for p in self.stubs:
            p.start()
        self.addCleanup(self._stop)

    def _stop(self):
        for p in self.stubs:
            p.stop()

    async def _quotes(self, codes):
        return {code: self.quotes[code] for code in codes if code in self.quotes}

    def summaries(self):
        return self.patches(summaries=True, summary_http=all_summaries)

    async def board(self, stack=None):
        return await self.run_with(stack or self.summaries(), opportunities.fetch_opportunity_board)

    @staticmethod
    def tools(board: dict) -> dict:
        return {t["tool"]: t for t in board["tools"]}

    @staticmethod
    def row(board: dict, code: str) -> dict:
        return next(r for r in board["rows"] if r["code"] == code)


class BoardFromSummaryTests(_Board):
    async def test_published_fields_rank_preferred_and_holding_candidates(self):
        board = await self.board()
        self.assertEqual(self.legacy_calls, [])  # 새 필드가 있으면 대시보드 파일을 받지 않는다
        codes = [r["code"] for r in board["rows"]]
        self.assertIn("005935", codes)  # 3년 백분위 91
        self.assertIn("000670", codes)  # 3년 백분위 93
        self.assertNotIn("036710", codes)  # 3년 백분위 62 < 80
        self.assertEqual([r["rank"] for r in board["rows"]], list(range(1, len(codes) + 1)))
        self.assertEqual(board["minScore"], signals.BOARD_MIN_SCORE)

    async def test_every_row_carries_liquidity_and_as_of(self):
        self.quotes = {"000670": {"trade_value": 2.5e9, "date": "2026-09-30"}}
        board = await self.board()
        preferred = self.row(board, "005935")
        self.assertEqual(preferred["liquidity"]["value"], 9876543210.0)
        self.assertEqual(preferred["liquidity"]["basis"], "avg20d")
        holding = self.row(board, "000670")
        self.assertEqual(holding["liquidity"], {"value": 2.5e9, "basis": "session", "asOf": "2026-09-30", "low": False})
        for row in board["rows"]:
            self.assertIn("value", row["liquidity"])
            self.assertTrue(row["primary"]["asOf"])
            self.assertTrue(row["primary"]["url"].startswith("https://"))

    async def test_failed_quotes_leave_liquidity_unknown_not_zero(self):
        with patch.object(live_cards, "domestic_quotes", new=AsyncMock(side_effect=httpx.ConnectError("offline"))):
            board = await self.board()
        row = self.row(board, "000670")
        self.assertIsNone(row["liquidity"]["value"])
        self.assertIsNone(row["liquidity"]["low"])

    async def test_ranking_ignores_who_holds_what(self):
        board = await self.board()
        self.assertNotIn("heldCodes", board)  # 보유 정보는 순위 계산에 들어가지 않는다

    async def test_tool_status_reports_counts_and_as_of(self):
        board = await self.board()
        tools = self.tools(board)
        self.assertEqual(set(tools), set(opportunities.TOOL_ORDER))
        self.assertEqual(tools[signals.TOOL_PREFERRED]["status"], "ok")
        self.assertEqual(tools[signals.TOOL_PREFERRED]["distributionEnd"], "2026-09-23")
        self.assertEqual(tools[signals.TOOL_HOLDING]["asOf"], "2026-09-26")
        self.assertEqual(tools[signals.TOOL_BUYBACKS]["asOf"], "2025-12-31")
        self.assertEqual(tools[signals.TOOL_NPS]["asOf"], "2024-12-31")

    async def test_nps_real_change_can_rank_but_level_alone_cannot(self):
        env = summary("nps-tracker")
        env["data"]["ownershipChange"]["changes"] = {"005930": 0.42, "000660": 1.5, "402340": 2.0}
        env["contentHash"] = envelope.vc_publish().content_hash(env["data"])

        async def http(url, etag):
            if url == siblings.summary_url("nps-tracker"):
                return 200, env, None
            return await all_summaries(url, etag)

        board = await self.board(self.patches(summaries=True, summary_http=http))
        codes = [r["code"] for r in board["rows"] if r["primary"]["tool"] == signals.TOOL_NPS]
        self.assertEqual(codes, ["402340"])  # 변화가 큰 종목만, 지분율 수준만 있는 009150 은 후보가 아니다
        self.assertEqual(self.row(board, "402340")["primary"]["score"], 100.0)

    async def test_same_file_nps_data_is_context_only_and_says_why(self):
        env = summary("nps-tracker")
        env["data"]["ownershipChange"] = {"from": "2024-12-31", "to": "2025-12-31", "comparable": False, "changes": {}}
        env["contentHash"] = envelope.vc_publish().content_hash(env["data"])

        async def http(url, etag):
            if url == siblings.summary_url("nps-tracker"):
                return 200, env, None
            return await all_summaries(url, etag)

        board = await self.board(self.patches(summaries=True, summary_http=http))
        self.assertFalse([r for r in board["rows"] if r["primary"]["tool"] == signals.TOOL_NPS])
        self.assertIn("지분율이 같아", self.tools(board)[signals.TOOL_NPS]["warning"])
        context = [s for s in self.row(board, "005935")["others"] if s["tool"] == signals.TOOL_NPS]
        self.assertEqual(context[0]["code"], "005930")  # 우선주 행에 보통주의 국민연금 문맥이 붙는다

    async def test_tool_warning_keeps_every_reason(self):
        """오래된 기준일이 다른 사유(백분위 없음·변화 없음)를 가리지 않는다."""
        env = summary("nps-tracker")
        env["data"]["ownershipChange"] = {"from": "2024-12-31", "to": "2025-12-31", "comparable": False, "changes": {}}
        env["contentHash"] = envelope.vc_publish().content_hash(env["data"])

        async def http(url, etag):
            if url == siblings.summary_url("nps-tracker"):
                return 200, env, None
            return await all_summaries(url, etag)

        with patch.object(opportunities, "today_kst", return_value=date(2026, 12, 31)):
            board = await self.board(self.patches(summaries=True, summary_http=http))
        warning = self.tools(board)[signals.TOOL_NPS]["warning"]
        self.assertIn("일 지난", warning)
        self.assertIn("지분율이 같아", warning)

    async def test_stale_sources_warn_at_tool_and_row_level(self):
        with patch.object(opportunities, "today_kst", return_value=date(2026, 10, 15)):
            board = await self.board()
        tools = self.tools(board)
        self.assertIn("거래일", tools[signals.TOOL_PREFERRED]["warning"])
        self.assertTrue(self.row(board, "005935")["primary"]["stale"])

    async def test_board_is_cached_between_requests(self):
        await self.board()
        first = opportunities._board_cache.get("board")
        self.assertIsNotNone(first)
        with patch.object(opportunities, "_build_board", new=AsyncMock(side_effect=AssertionError("cache miss"))):
            second = await opportunities.fetch_opportunity_board()
        self.assertEqual([r["code"] for r in second["rows"]], [r["code"] for r in first["rows"]])


class BoardFallbackTests(_Board):
    def current_summaries(self):
        """형제가 아직 새 필드를 발행하지 않은 지금 상태."""

        async def http(url, etag):
            if url == siblings.summary_url(signals.TOOL_PREFERRED):
                return 200, republished(signals.TOOL_PREFERRED, CPS_FIELDS), None
            if url == siblings.summary_url(signals.TOOL_HOLDING):
                return 200, republished(signals.TOOL_HOLDING, HOLDING_FIELDS), None
            if url == siblings.summary_url(signals.TOOL_NPS):
                return 200, republished(signals.TOOL_NPS, NPS_FIELDS), None
            return await all_summaries(url, etag)

        return self.patches(summaries=True, summary_http=http)

    async def test_dashboard_files_fill_in_until_the_siblings_publish(self):
        self.urls.update(dashboards())
        board = await self.board(self.current_summaries())
        self.assertEqual(self.tools(board)[signals.TOOL_PREFERRED]["status"], "ok")
        row = self.row(board, "33626K")  # doosan_fuelcell 우선주
        self.assertEqual(row["primary"]["score"], 96)
        self.assertEqual(row["liquidity"]["value"], 55349725.0)
        self.assertTrue(row["liquidity"]["low"])
        self.assertEqual(self.row(board, "000670")["primary"]["score"], 100)  # 멈춘 저장값 1이 아니라 현재 비율

    async def test_a_failed_dashboard_file_only_drops_percentiles(self):
        board = await self.board(self.current_summaries())  # dashboard URL 은 404
        tools = self.tools(board)
        self.assertEqual(tools[signals.TOOL_PREFERRED]["status"], "partial")
        self.assertIn("현재 값만", tools[signals.TOOL_PREFERRED]["warning"])
        self.assertEqual(tools[signals.TOOL_PREFERRED]["rankable"], 0)
        self.assertGreater(tools[signals.TOOL_PREFERRED]["count"], 0)
        self.assertFalse([r for r in board["rows"] if r["primary"]["tool"] == signals.TOOL_PREFERRED])
        self.assertFalse([r for r in board["rows"] if r["primary"]["tool"] == signals.TOOL_HOLDING])

    async def test_legacy_only_mode_still_builds_the_board(self):
        self.urls.update(dashboards())
        board = await self.board(self.patches(summaries=False))
        self.assertEqual(self.row(board, "33626K")["primary"]["score"], 96)
        tools = self.tools(board)
        self.assertEqual(tools[signals.TOOL_SPAC]["status"], "ok")
        self.assertEqual(tools[signals.TOOL_NPS]["status"], "ok")
        self.assertIn("변화 데이터가 없어", tools[signals.TOOL_NPS]["warning"])  # current.json 에는 변화가 없다

    async def test_nps_positions_fall_back_to_current_json(self):
        self.urls.update(dashboards())
        out = await self.run_with(self.current_summaries(), lambda: opportunities.fetch_stock_360("005930"))
        nps = next(s for s in out["signals"] if s["tool"] == signals.TOOL_NPS)
        self.assertEqual({m["label"] for m in nps["metrics"]} & {"포트폴리오 비중(추정 수량 기반)"},
                         {"포트폴리오 비중(추정 수량 기반)"})
        self.assertIn(siblings.data_url(signals.TOOL_NPS, "current"), self.legacy_calls)


class BoardFailureIsolationTests(_Board):
    async def test_one_failed_tool_leaves_the_others_visible(self):
        with patch.object(opportunities, "_load_spac_source", new=AsyncMock(side_effect=ValueError("boom"))):
            board = await self.board()
        tools = self.tools(board)
        self.assertEqual(tools[signals.TOOL_SPAC]["status"], "failed")
        self.assertIn("받지 못했습니다", tools[signals.TOOL_SPAC]["warning"])
        self.assertEqual(tools[signals.TOOL_PREFERRED]["status"], "ok")
        self.assertIn("005935", [r["code"] for r in board["rows"]])

    async def test_each_tool_can_fail_alone(self):
        loaders = {
            signals.TOOL_PREFERRED: "_load_spread_source",
            signals.TOOL_HOLDING: "_load_holding_source",
            signals.TOOL_SPAC: "_load_spac_source",
            signals.TOOL_NPS: "_load_nps_source",
        }
        for tool_id, name in loaders.items():
            with self.subTest(tool=tool_id):
                reset_caches()
                opportunities.reset()
                with patch.object(opportunities, name, new=AsyncMock(side_effect=httpx.ConnectError("offline"))):
                    board = await self.board()
                tools = self.tools(board)
                self.assertEqual(tools[tool_id]["status"], "failed")
                self.assertEqual(sum(1 for t in tools.values() if t["status"] == "failed"), 1)

    async def test_buybacks_failure_is_isolated_too(self):
        with patch.object(external_tools, "_buybacks_data", new=AsyncMock(side_effect=KeyError("byCode"))):
            board = await self.board()
        self.assertEqual(self.tools(board)[signals.TOOL_BUYBACKS]["status"], "failed")
        self.assertTrue(board["rows"])

    async def test_all_tools_failing_returns_an_empty_board_with_reasons(self):
        async def down(url, etag):
            raise httpx.ConnectError("offline")

        self.urls = {}
        board = await self.board(self.patches(summaries=True, summary_http=down))
        self.assertEqual(board["rows"], [])
        self.assertTrue(all(t["status"] == "failed" for t in board["tools"]))

    async def test_all_tools_failing_serves_the_last_good_board(self):
        good = await self.board()
        opportunities._board_cache.set("board", good, ttl_seconds=-1)  # 만료

        async def down(url, etag):
            raise httpx.ConnectError("offline")

        self.urls = {}
        opportunities._sources.clear()
        external_tools._raw_cache.clear()
        siblings.reset()
        board = await self.board(self.patches(summaries=True, summary_http=down))
        self.assertEqual([r["code"] for r in board["rows"]], [r["code"] for r in good["rows"]])

    async def test_a_transient_failure_keeps_the_last_source_within_a_day(self):
        await self.board()
        opportunities._sources.set("spac", opportunities._sources.get("spac"), ttl_seconds=-1)
        opportunities._board_cache.clear()
        with patch.object(opportunities, "_load_spac_source", new=AsyncMock(side_effect=ValueError("boom"))):
            board = await self.board()
        self.assertNotEqual(self.tools(board)[signals.TOOL_SPAC]["status"], "failed")


class Stock360Tests(_Board):
    async def stock(self, code: str, stack=None) -> dict:
        return await self.run_with(stack or self.summaries(), lambda: opportunities.fetch_stock_360(code))

    async def test_preferred_code_shows_its_pair_with_percentiles_and_average_liquidity(self):
        out = await self.stock("005935")
        pair = next(s for s in out["signals"] if s["tool"] == signals.TOOL_PREFERRED)
        self.assertEqual(pair["code"], "005935")
        self.assertEqual(pair["score"], 91)
        self.assertEqual(out["liquidity"]["basis"], "avg20d")
        self.assertEqual(out["failedTools"], [])
        self.assertTrue(pair["url"].endswith("?code=005935"))

    async def test_preferred_code_includes_the_common_stock_signals(self):
        out = await self.stock("005935")
        by_code = {(s["tool"], s["code"]) for s in out["signals"]}
        self.assertEqual(by_code, {
            (signals.TOOL_PREFERRED, "005935"),
            (signals.TOOL_BUYBACKS, "005930"),
            (signals.TOOL_NPS, "005930"),
        })
        self.assertEqual(out["signals"][0]["code"], "005935")  # 이 코드가 거래 대상인 신호가 먼저
        # 같은 보통주의 다른 우선주 쌍은 섞지 않는다.
        hyundai = await self.stock("005387")
        self.assertEqual({s["code"] for s in hyundai["signals"] if s["tool"] == signals.TOOL_PREFERRED}, {"005387"})

    async def test_preferred_failure_still_returns_the_other_tools(self):
        with patch.object(opportunities, "_load_spread_source", new=AsyncMock(side_effect=ValueError("boom"))):
            out = await self.stock("115310")
        self.assertEqual([t["tool"] for t in out["failedTools"]], [signals.TOOL_PREFERRED])
        self.assertIn(signals.TOOL_BUYBACKS, {s["tool"] for s in out["signals"]})

    async def test_common_code_shows_every_preferred_pair_and_company_level_signals(self):
        out = await self.stock("005380")  # 현대차: 우선주 3종
        preferred = [s for s in out["signals"] if s["tool"] == signals.TOOL_PREFERRED]
        self.assertEqual({s["code"] for s in preferred}, {"005387", "005385", "005389"})
        out = await self.stock("005930")
        tools = {s["tool"] for s in out["signals"]}
        self.assertEqual(tools, {signals.TOOL_PREFERRED, signals.TOOL_BUYBACKS, signals.TOOL_NPS})
        self.assertEqual(next(s for s in out["signals"] if s["tool"] == signals.TOOL_NPS)["asOf"], "2024-12-31")

    async def test_holding_company_shows_ratio_percentile(self):
        with patch.object(stock_quotes, "get_quote_snapshot",
                          new=AsyncMock(return_value={"trade_value": 3e9, "date": "2026-09-30"})):
            out = await self.stock("000670")
        holding = next(s for s in out["signals"] if s["tool"] == signals.TOOL_HOLDING)
        self.assertEqual(holding["score"], 93)
        self.assertEqual(out["liquidity"], {"value": 3e9, "basis": "session", "asOf": "2026-09-30", "low": False})

    async def test_quote_liquidity_matches_the_board_source(self):
        self.quotes = {"000670": {"trade_value": 7e9, "date": "2026-09-30"}}
        with patch.object(stock_quotes, "get_quote_snapshot", new=AsyncMock(side_effect=AssertionError("single quote"))):
            out = await self.stock("000670")
        self.assertEqual(out["liquidity"]["value"], 7e9)

    async def test_missing_names_are_filled_for_the_same_stock(self):
        out = await self.stock("000670")
        self.assertEqual({s["name"] for s in out["signals"]}, {"영풍"})
        named = await self.stock("000660")  # 자사주 ratios 에는 이름이 없고 국민연금 top 에는 있다
        self.assertEqual({s["tool"] for s in named["signals"]}, {signals.TOOL_BUYBACKS, signals.TOOL_NPS})
        self.assertEqual({s["name"] for s in named["signals"]}, {"SK하이닉스"})
        lookup = await self.stock("035420")  # 어느 쪽에도 이름이 없으면 허브 코드표를 본다
        self.assertEqual({s["name"] for s in lookup["signals"]}, {"허브종목명"})

    async def test_spac_code_shows_liquidation_numbers(self):
        code = data("spac-hunter")["spacs"][0]["code"]
        out = await self.stock(code)
        spac_signal = next(s for s in out["signals"] if s["tool"] == signals.TOOL_SPAC)
        self.assertIn("연환산 기대수익(거래비용 차감 전)", {m["label"] for m in spac_signal["metrics"]})
        self.assertIsNone(out["liquidity"]["value"])

    async def test_etf_code_links_to_eiayn(self):
        out = await self.stock("069500")
        etf = next(s for s in out["signals"] if s["tool"] == signals.TOOL_ETF)
        self.assertEqual(etf["url"], "https://ducklove.github.io/eiayn/?code=069500")
        self.assertIsNone(etf["score"])

    async def test_unrelated_and_invalid_codes_are_empty(self):
        with patch.object(stock_quotes, "get_quote_snapshot", new=AsyncMock(side_effect=AssertionError("no quote"))):
            out = await self.stock("999990")
        self.assertEqual(out["signals"], [])
        self.assertIsNone(out["liquidity"]["value"])
        for bad in ("", "   ", "../etc", "VOO"):
            self.assertEqual((await self.stock(bad))["signals"], [])

    async def test_failed_tool_is_named_and_the_rest_stay(self):
        with patch.object(opportunities, "_load_holding_source", new=AsyncMock(side_effect=ValueError("boom"))):
            out = await self.stock("005935")
        self.assertEqual([t["tool"] for t in out["failedTools"]], [signals.TOOL_HOLDING])
        self.assertTrue(out["signals"])

    async def test_failed_quote_does_not_break_the_panel(self):
        with patch.object(stock_quotes, "get_quote_snapshot", new=AsyncMock(side_effect=httpx.ConnectError("offline"))):
            out = await self.stock("000670")
        self.assertTrue(out["signals"])
        self.assertIsNone(out["liquidity"]["value"])

    async def test_code_is_normalized(self):
        out = await self.stock(" 005935 ")
        self.assertEqual(out["code"], "005935")
        self.assertTrue(out["signals"])


class ActionBoardSeverityTests(_Board):
    async def signals_for(self, codes, stack=None):
        return await self.run_with(stack or self.summaries(), lambda: external_tools.fetch_portfolio_signals(codes))

    async def test_percentile_replaces_the_fixed_threshold(self):
        out = await self.signals_for(["005930", "005380", "000670", "036710"])
        by = {code: {s["kind"]: s for s in sigs} for code, sigs in out.items()}
        # 삼성전자: 괴리 22.5%(고정 50 미만)지만 자기 분포 91 → high
        self.assertEqual(by["005930"]["preferred"]["severity"], "high")
        self.assertIn("자기 3년 백분위 91", by["005930"]["preferred"]["detail"])
        # 현대차: 괴리 50.25%(고정 50 이상)지만 자기 분포 88 → watch
        self.assertEqual(by["005380"]["preferred"]["severity"], "watch")
        # 심텍홀딩스: 비율 792%(고정 150 이상)지만 자기 분포 62 → watch
        self.assertEqual(by["036710"]["holding"]["severity"], "watch")
        self.assertEqual(by["000670"]["holding"]["severity"], "high")

    async def test_metric_and_shape_do_not_change(self):
        out = await self.signals_for(["005930", "000670", "115310"])
        for sigs in out.values():
            for signal in sigs:
                self.assertEqual(set(signal), SIGNAL_KEYS)
        preferred = next(s for s in out["005930"] if s["kind"] == "preferred")
        self.assertEqual(preferred["metric"], 22.51)  # metric 은 백분위가 아니라 원래 값이다
        self.assertEqual(preferred["short_label"], "우선주")
        self.assertTrue(preferred["detail"].startswith("우선주 괴리율 22.5%"))

    async def test_missing_percentile_falls_back_to_the_fixed_threshold(self):
        out = await self.signals_for(["005389", "363280"])  # 픽스처에서 백분위가 null 인 쌍
        by = {code: {s["kind"]: s for s in sigs} for code, sigs in out.items()}
        self.assertEqual(by["005389"]["preferred"]["severity"], "high")  # 51.04 ≥ 50
        self.assertNotIn("백분위", by["005389"]["preferred"]["detail"])
        self.assertEqual(by["363280"]["holding"]["severity"], "high")  # 447.94 ≥ 150

    async def test_percentile_outage_keeps_the_action_board_working(self):
        with patch.object(opportunities, "_load_spread_source", new=AsyncMock(side_effect=ValueError("boom"))), \
             patch.object(opportunities, "_load_holding_source", new=AsyncMock(side_effect=ValueError("boom"))):
            out = await self.signals_for(["005930", "000670"])
        by = {code: {s["kind"]: s for s in sigs} for code, sigs in out.items()}
        self.assertEqual(by["005930"]["preferred"]["severity"], "watch")  # 22.51 < 50
        self.assertEqual(by["000670"]["holding"]["severity"], "high")     # 760 ≥ 150
        self.assertNotIn("백분위", by["000670"]["holding"]["detail"])

    async def test_buybacks_keep_the_fixed_threshold(self):
        out = await self.signals_for(["115310", "000660"])
        by = {code: {s["kind"]: s for s in sigs} for code, sigs in out.items()}
        self.assertEqual(by["115310"]["buybacks"]["severity"], "high")   # 54.18 ≥ 20
        self.assertEqual(by["000660"]["buybacks"]["severity"], "watch")  # 3.61 < 20


class RouteTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        reset_caches()
        opportunities.reset()

    async def test_board_route_adds_held_codes_for_a_logged_in_user(self):
        board = {"rows": [], "tools": []}
        with patch.object(opportunities, "fetch_opportunity_board", new=AsyncMock(return_value=copy.deepcopy(board))), \
             patch.object(stocks_route, "get_current_user", new=AsyncMock(return_value={"google_sub": "u1"})), \
             patch("repositories.portfolio.get_portfolio",
                   new=AsyncMock(return_value=[{"stock_code": " 005935 "}, {"stock_code": None}])):
            out = await stocks_route.external_opportunities(object())
        self.assertEqual(out["heldCodes"], ["005935"])

    async def test_board_route_is_public_and_hides_nothing_for_guests(self):
        with patch.object(opportunities, "fetch_opportunity_board", new=AsyncMock(return_value={"rows": [1]})), \
             patch.object(stocks_route, "get_current_user", new=AsyncMock(return_value=None)):
            out = await stocks_route.external_opportunities(object())
        self.assertEqual(out, {"rows": [1], "heldCodes": []})

    async def test_held_code_lookup_failure_does_not_break_the_board(self):
        import sqlite3

        with patch.object(opportunities, "fetch_opportunity_board", new=AsyncMock(return_value={"rows": []})), \
             patch.object(stocks_route, "get_current_user", new=AsyncMock(return_value={"google_sub": "u1"})), \
             patch("repositories.portfolio.get_portfolio", new=AsyncMock(side_effect=sqlite3.OperationalError("locked"))):
            out = await stocks_route.external_opportunities(object())
        self.assertEqual(out["heldCodes"], [])

    async def test_stock_360_route_passes_the_code(self):
        fake = AsyncMock(return_value={"code": "005930", "signals": []})
        with patch.object(opportunities, "fetch_stock_360", new=fake):
            out = await stocks_route.external_stock_360("005930")
        self.assertEqual(out["code"], "005930")
        self.assertEqual(fake.await_args.args[0], "005930")

    async def test_route_paths_do_not_shadow_the_stock_links_route(self):
        paths = {route.path for route in stocks_route.router.routes}
        self.assertLessEqual(
            {"/api/external/opportunities", "/api/external/stock/{code}", "/api/external/stock/{code}/360"}, paths,
        )


class RegistryAndSchemaTests(unittest.TestCase):
    def test_dashboard_files_come_from_the_registry(self):
        for tool_id in (signals.TOOL_PREFERRED, signals.TOOL_HOLDING):
            url = siblings.data_url(tool_id, "dashboard-summary")
            self.assertTrue(url.startswith(f"https://ducklove.github.io/{tool_id}/data/summary.json"), url)

    def test_env_override_moves_the_dashboard_file(self):
        with patch.dict(os.environ, {"HOLDING_VALUE_BASE_URL": "https://mirror.example/hv/"}):
            self.assertEqual(siblings.data_url(signals.TOOL_HOLDING, "dashboard-summary"),
                             "https://mirror.example/hv/data/summary.json")

    def test_fixtures_publish_the_fields_the_signals_read(self):
        pairs = {p["id"]: p for p in data(signals.TOOL_PREFERRED)["pairs"]}
        signal = signals.preferred_signal(pairs["samsung_elec"], None, TODAY)
        self.assertEqual(signal["score"], 91)
        self.assertEqual(signal["averageTradedValue20"], pairs["samsung_elec"]["preferredAvgTradedValue20"])
        holding = {p["id"]: p for p in data(signals.TOOL_HOLDING)["pairs"]}["youngpoong_koreazinc"]
        self.assertEqual(signals.holding_signal(holding, "2026-09-26", TODAY)["score"], 93)
        nps = data(signals.TOOL_NPS)
        self.assertEqual(nps["ownershipChange"]["comparable"], True)
        json.dumps(nps)


if __name__ == "__main__":
    unittest.main()
