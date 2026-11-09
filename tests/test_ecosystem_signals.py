"""형제 도구 신호 정규화 순수 함수 (services/ecosystem/signals.py). 네트워크·DB·시계 없음."""

import unittest
from datetime import date, timedelta

from services.ecosystem import signals

# 2026-11-09(월)을 기준으로 삼는다. 직전 거래일은 2026-11-06(금)이다.
TODAY = date(2026, 11, 9)


def labels(signal: dict) -> dict:
    return {item["label"]: item["value"] for item in signal["metrics"]}


def preferred_pair(**over) -> dict:
    pair = {
        "id": "samsung_elec", "name": "삼성전자", "preferredName": "삼성전자우",
        "commonCode": "005930", "preferredCode": "005935",
        "spread": 19.59, "spreadChange": -0.81, "commonPrice": 193500, "preferredPrice": 155600,
        "date": "2026-11-06", "spreadPct1y": 40, "spreadPct3y": 92, "spreadPctEnd": "2026-11-06",
        "attractiveness": 63.5, "commonAvgTradedValue20": 5e11, "preferredAvgTradedValue20": 3e10,
    }
    pair.update(over)
    return pair


def holding_pair(**over) -> dict:
    pair = {
        "id": "youngpoong_koreazinc", "name": "영풍→고려아연", "holdingName": "영풍", "code": "000670",
        "ratio": 917.34, "ratioChange": -23.45, "holdingValue": 50256.4, "marketCap": 5478.5,
        "ratioPct1y": 90, "ratioPct3y": 93, "ratioPctEnd": "2026-11-06",
    }
    pair.update(over)
    return pair


def spac(code: str, annualized, **over) -> dict:
    item = {
        "code": code, "name": f"{code}스팩", "currentPrice": 2000, "annualizedReturn": annualized,
        "liquidationDiscountPct": 2.0, "currentLiquidationValue": 2040.0, "payoutDate": "2027-03-11",
        "mergerStatus": None, "status": "일반",
    }
    item.update(over)
    return item


class PercentileTests(unittest.TestCase):
    def test_rank_counts_samples_at_or_below_and_matches_the_sibling_cards(self):
        values = list(range(1, 101))
        self.assertEqual(signals.percentile_rank(values, 92), 92)
        self.assertEqual(signals.percentile_rank(values, 92.5), 92)
        self.assertEqual(signals.percentile_rank(values, 0), 0)
        self.assertEqual(signals.percentile_rank(values, 1000), 100)

    def test_rank_rounds_half_up_like_js_math_round(self):
        # 200개 중 101개 = 50.5% → JS Math.round 는 51(Python round 는 50).
        self.assertEqual(signals.percentile_rank([1.0] * 101 + [9.0] * 99, 1.0), 51)

    def test_rank_needs_thirty_valid_samples_and_a_current_value(self):
        self.assertIsNone(signals.percentile_rank([1.0] * 29, 1.0))
        self.assertEqual(signals.percentile_rank([1.0] * 30, 1.0), 100)
        self.assertIsNone(signals.percentile_rank([1.0] * 30 + [None] * 5, None))
        self.assertIsNone(signals.percentile_rank([None, float("nan"), True, "9"] * 10, 1.0))
        self.assertIsNone(signals.percentile_rank(None, 1.0))

    def test_holding_percentile_uses_the_current_ratio_not_the_frozen_value(self):
        first = date(2026, 1, 1)
        dates = [(first + timedelta(days=i)).isoformat() for i in range(100)]
        history = {"dates": dates, "ratio": [float(i) for i in range(1, 101)]}
        out = signals.holding_percentiles(history, 80.0, "2026-05-01T13:26:57+09:00")
        self.assertEqual(out["ratioPct3y"], 80)  # 스냅샷 표본까지 101개 중 81개 이하
        self.assertEqual(out["ratioPct1y"], 80)
        self.assertEqual(out["ratioPctEnd"], dates[-1])  # 넣기 전 분포의 마지막 날짜

    def test_holding_percentile_replaces_a_same_day_sample(self):
        first = date(2026, 1, 1)
        dates = [(first + timedelta(days=i)).isoformat() for i in range(100)]
        history = {"dates": dates, "ratio": [float(i) for i in range(1, 101)]}
        out = signals.holding_percentiles(history, 1.0, dates[-1])
        self.assertEqual(out["ratioPct3y"], 2)  # {1, 1(교체)} ≤ 1 → 2/100

    def test_holding_percentile_never_moves_the_window_for_an_old_snapshot(self):
        history = {"dates": [f"2026-01-{d:02d}" for d in range(1, 31)], "ratio": [float(d) for d in range(1, 31)]}
        out = signals.holding_percentiles(history, 15.0, "2025-12-31")
        self.assertEqual(out["ratioPct3y"], 50)  # 분포보다 오래된 스냅샷은 표본으로 넣지 않는다

    def test_holding_percentile_is_null_without_history_or_ratio(self):
        empty = {"ratioPct1y": None, "ratioPct3y": None, "ratioPctEnd": None}
        self.assertEqual(signals.holding_percentiles(None, 1.0, "2026-01-01"), empty)
        self.assertEqual(signals.holding_percentiles({"dates": 1, "ratio": 2}, 1.0, "2026-01-01"), empty)
        history = {"dates": [f"2026-01-{d:02d}" for d in range(1, 31)], "ratio": [1.0] * 30}
        self.assertEqual(
            signals.holding_percentiles(history, None, "2026-02-01"),
            {"ratioPct1y": None, "ratioPct3y": None, "ratioPctEnd": "2026-01-30"},
        )

    def test_spread_percentile_prefers_the_distribution_over_the_stored_value(self):
        stats = {
            "date": "2026-11-06", "pctile1y": 1, "pctile3y": 2,
            "pctile1yValues": [float(v) for v in range(1, 41)],
            "pctile3yValues": [float(v) for v in range(1, 101)],
        }
        self.assertEqual(signals.spread_percentiles(stats, 22.5),
                         {"spreadPct1y": 55, "spreadPct3y": 22, "spreadPctEnd": "2026-11-06"})

    def test_spread_percentile_falls_back_to_stored_only_without_a_distribution(self):
        stored = {"date": "2026-11-06", "pctile3y": 77}
        self.assertEqual(signals.spread_percentiles(stored, 10.0)["spreadPct3y"], 77)
        short = {"date": "2026-11-06", "pctile3y": 77, "pctile3yValues": [1.0] * 5}
        self.assertIsNone(signals.spread_percentiles(short, 10.0)["spreadPct3y"])
        self.assertIsNone(signals.spread_percentiles(None, 10.0)["spreadPct3y"])

    def test_cross_section_score_needs_enough_names(self):
        population = signals.sorted_population([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, None, "x"])
        self.assertEqual(signals.cross_section_score(population, 10), 100.0)
        self.assertEqual(signals.cross_section_score(population, 5.5), 50.0)
        self.assertEqual(signals.cross_section_score(population, 0), 0.0)
        self.assertIsNone(signals.cross_section_score(population, None))
        self.assertIsNone(signals.cross_section_score(population[:9], 5))


class FreshnessTests(unittest.TestCase):
    def test_trading_days_skip_weekends(self):
        self.assertEqual(signals.trading_days_behind(date(2026, 11, 6), date(2026, 11, 8)), 0)  # 금 → 일
        self.assertEqual(signals.trading_days_behind(date(2026, 11, 6), TODAY), 1)              # 금 → 월
        self.assertEqual(signals.trading_days_behind(date(2026, 11, 2), date(2026, 11, 6)), 4)
        self.assertEqual(signals.trading_days_behind(TODAY, TODAY), 0)
        self.assertEqual(signals.trading_days_behind(date(2026, 11, 10), TODAY), 0)  # 미래 기준일

    def test_trading_days_skip_krx_holidays(self):
        # 2026-10-03(개천절 토)·05(추석 연휴 대체)처럼 달력 휴장일은 세지 않는다.
        self.assertLess(signals.trading_days_behind(date(2026, 9, 30), date(2026, 10, 15)), 11)

    def test_long_gaps_are_estimated_without_walking_the_calendar(self):
        self.assertEqual(signals.trading_days_behind(date(2020, 1, 1), date(2020, 7, 1)), 182 * 5 // 7)

    def test_warning_follows_the_tool_tolerance(self):
        self.assertIsNone(signals.freshness_warning(signals.TOOL_PREFERRED, "2026-11-06", TODAY))
        self.assertIsNone(signals.freshness_warning(signals.TOOL_PREFERRED, "2026-11-09T14:58:05+09:00", TODAY))
        stale = signals.freshness_warning(signals.TOOL_PREFERRED, "2026-11-02", TODAY)
        self.assertIn("2026-11-02", stale)
        self.assertIn("거래일", stale)

    def test_yearly_filings_are_not_stale_until_the_next_filing_is_due(self):
        self.assertIsNone(signals.freshness_warning(signals.TOOL_BUYBACKS, "2025-12-31", TODAY))
        self.assertIsNone(signals.freshness_warning(signals.TOOL_NPS, "2025-12-31", TODAY))
        self.assertIn("일 지난", signals.freshness_warning(signals.TOOL_BUYBACKS, "2025-03-31", TODAY))

    def test_unknown_as_of_is_a_warning(self):
        self.assertIn("확인할 수 없", signals.freshness_warning(signals.TOOL_SPAC, None, TODAY))
        self.assertIn("확인할 수 없", signals.freshness_warning(signals.TOOL_SPAC, "not-a-date", TODAY))

    def test_history_score_keeps_a_lagging_distribution_with_a_warning(self):
        self.assertEqual(signals.history_score(92, "2026-11-06", TODAY), (92, None))
        score, warning = signals.history_score(92, "2026-10-07", TODAY)
        self.assertEqual(score, 92)
        self.assertIn("2026-10-07", warning)
        self.assertIn("거래일 지연", warning)

    def test_history_score_drops_a_distribution_older_than_ninety_days(self):
        score, warning = signals.history_score(98, "2026-07-10", TODAY)
        self.assertIsNone(score)
        self.assertIn("2026-07-10", warning)

    def test_history_score_without_percentile_or_end_date(self):
        self.assertIsNone(signals.history_score(None, "2026-11-06", TODAY)[0])
        self.assertIsNone(signals.history_score(92, None, TODAY)[0])


class SeverityTests(unittest.TestCase):
    def test_percentile_overrides_the_fixed_threshold_in_both_directions(self):
        self.assertEqual(signals.history_severity(92, 19.6, 50), "high")   # 낮은 괴리지만 자기 역사 상단
        self.assertEqual(signals.history_severity(85, 83.8, 50), "watch")  # 높은 괴리지만 자기 역사 중간
        self.assertEqual(signals.history_severity(90, 1.0, 50), "high")
        self.assertEqual(signals.history_severity(89, 99.0, 50), "watch")

    def test_fixed_threshold_is_only_the_fallback(self):
        self.assertEqual(signals.history_severity(None, 50.0, 50), "high")
        self.assertEqual(signals.history_severity(None, 49.9, 50), "watch")
        self.assertEqual(signals.history_severity(None, None, 50), "watch")


class PreferredSignalTests(unittest.TestCase):
    def test_trade_target_is_the_preferred_stock_and_numbers_pass_through(self):
        signal = signals.preferred_signal(preferred_pair(), "2026-11-09T14:58:05+09:00", TODAY)
        self.assertEqual((signal["code"], signal["relatedCode"]), ("005935", "005930"))
        self.assertEqual(signal["name"], "삼성전자우")
        self.assertEqual((signal["score"], signal["scoreBasis"]), (92, signals.BASIS_HISTORY))
        self.assertTrue(signal["rankable"])
        self.assertFalse(signal["stale"])
        self.assertEqual(signal["asOf"], "2026-11-06")
        self.assertEqual(signal["distributionEnd"], "2026-11-06")
        self.assertEqual(signal["averageTradedValue20"], 3e10)
        shown = labels(signal)
        self.assertEqual(shown["괴리율"], 19.59)
        self.assertEqual(shown["3년 백분위"], 92)
        self.assertEqual(shown["1년 백분위"], 40)
        self.assertEqual(shown["cps 매력도"], 63.5)

    def test_unknown_numbers_are_left_out_instead_of_zeroed(self):
        signal = signals.preferred_signal(
            preferred_pair(spreadPct1y=None, spreadPct3y=None, spreadPctEnd=None, attractiveness=None,
                           preferredAvgTradedValue20=None, commonAvgTradedValue20=None),
            "2026-11-06", TODAY,
        )
        self.assertIsNone(signal["score"])
        self.assertFalse(signal["rankable"])
        self.assertIsNone(signal["averageTradedValue20"])
        shown = labels(signal)
        for missing in ("3년 백분위", "1년 백분위", "cps 매력도", "보통주 20일 평균 거래대금"):
            self.assertNotIn(missing, shown)
        self.assertEqual(shown["괴리율"], 19.59)
        self.assertTrue(signal["stale"])  # 백분위가 없다는 경고가 붙는다

    def test_no_spread_means_no_signal(self):
        self.assertIsNone(signals.preferred_signal(preferred_pair(spread=None), "2026-11-06", TODAY))
        self.assertIsNone(signals.preferred_signal(preferred_pair(preferredCode=""), "2026-11-06", TODAY))

    def test_old_quote_date_warns(self):
        signal = signals.preferred_signal(preferred_pair(date="2026-11-02"), "2026-11-02", TODAY)
        self.assertTrue(signal["stale"])
        self.assertTrue(any("2026-11-02" in w for w in signal["warnings"]))

    def test_a_frozen_distribution_hides_every_percentile(self):
        signal = signals.preferred_signal(preferred_pair(spreadPctEnd="2026-07-01"), "2026-11-06", TODAY)
        self.assertIsNone(signal["score"])
        self.assertFalse(signal["rankable"])
        self.assertNotIn("3년 백분위", labels(signal))
        self.assertNotIn("1년 백분위", labels(signal))


class HoldingSignalTests(unittest.TestCase):
    def test_holding_signal_keeps_the_current_ratio_with_its_distribution_date(self):
        signal = signals.holding_signal(holding_pair(), "2026-11-09T13:26:57+09:00", TODAY)
        self.assertEqual((signal["code"], signal["name"], signal["pairName"]), ("000670", "영풍", "영풍→고려아연"))
        self.assertEqual(signal["score"], 93)
        self.assertEqual(signal["asOf"], "2026-11-09")
        self.assertEqual(signal["warnings"], [])
        self.assertEqual(labels(signal)["보유가치/시총"], 917.34)
        self.assertEqual(labels(signal)["시가총액"], 5478.5)

    def test_lagging_daily_history_is_surfaced_on_the_signal(self):
        signal = signals.holding_signal(holding_pair(ratioPctEnd="2026-10-07"), "2026-11-09", TODAY)
        self.assertEqual(signal["score"], 93)
        self.assertTrue(signal["rankable"])
        self.assertTrue(signal["stale"])
        self.assertEqual(signal["distributionEnd"], "2026-10-07")
        self.assertIn("2026-10-07", signal["warnings"][0])

    def test_missing_ratio_means_no_signal(self):
        self.assertIsNone(signals.holding_signal(holding_pair(ratio=None), "2026-11-09", TODAY))


class SpacSignalTests(unittest.TestCase):
    def universe(self) -> list[dict]:
        return [spac(f"47000{i}", float(i)) for i in range(10)]

    def test_score_is_rank_among_spacs_and_published_values_are_not_changed(self):
        items = {s["code"]: s for s in signals.spac_signals(self.universe(), "2026-11-06", TODAY)}
        top = items["470009"]
        self.assertEqual((top["score"], top["scoreBasis"]), (100.0, signals.BASIS_CROSS_SECTION))
        self.assertTrue(top["rankable"])
        self.assertEqual(labels(top)["연환산 기대수익(거래비용 차감 전)"], 9.0)
        self.assertEqual(labels(top)["청산가 괴리"], 2.0)
        self.assertIn("10개", top["scoreNote"])

    def test_merger_spacs_leave_the_ranking_and_the_population(self):
        items = self.universe() + [spac("470100", 99.0, mergerStatus="합병 확정", status="합병 확정")]
        out = {s["code"]: s for s in signals.spac_signals(items, "2026-11-06", TODAY)}
        merger = out["470100"]
        self.assertIsNone(merger["score"])
        self.assertFalse(merger["rankable"])
        self.assertTrue(any("합병 확정" in w for w in merger["warnings"]))
        self.assertEqual(labels(merger)["상태"], "합병 확정")
        self.assertEqual(out["470009"]["score"], 100.0)  # 합병 스팩 값이 모집단에 들어가지 않았다

    def test_too_few_spacs_or_missing_return_have_no_score(self):
        few = signals.spac_signals(self.universe()[:9], "2026-11-06", TODAY)
        self.assertTrue(all(s["score"] is None for s in few))
        out = {s["code"]: s for s in signals.spac_signals(self.universe() + [spac("470101", None)], "2026-11-06", TODAY)}
        self.assertIsNone(out["470101"]["score"])
        self.assertNotIn("연환산 기대수익(거래비용 차감 전)", labels(out["470101"]))

    def test_stale_valuation_date_warns_on_every_spac(self):
        out = signals.spac_signals(self.universe(), "2026-10-01", TODAY)
        self.assertTrue(all(s["stale"] for s in out))

    def test_no_net_of_cost_number_is_invented(self):
        signal = signals.spac_signals(self.universe(), "2026-11-06", TODAY)[0]
        self.assertEqual(set(labels(signal)), {
            "연환산 기대수익(거래비용 차감 전)", "청산가 괴리", "현재가", "현재 청산가", "수령 예정일", "상태",
        })


class BuybackSignalTests(unittest.TestCase):
    def by_code(self) -> dict:
        return {f"00000{i}": {"name": f"회사{i}", "asOf": "2025-12-31", "treasuryRatioPct": float(i)} for i in range(10)}

    def test_cross_section_score_and_zero_is_not_a_candidate(self):
        out = {s["code"]: s for s in signals.buyback_signals(self.by_code(), TODAY)}
        self.assertEqual(out["000009"]["score"], 100.0)
        self.assertTrue(out["000009"]["rankable"])
        self.assertEqual(out["000000"]["score"], 10.0)
        self.assertFalse(out["000000"]["rankable"])  # 자사주 0%는 후보가 아니다
        self.assertEqual(labels(out["000000"])["자사주 비율"], 0.0)  # 그러나 실제 0은 그대로 표시한다

    def test_codes_limit_the_output_but_not_the_population(self):
        out = signals.buyback_signals(self.by_code(), TODAY, codes=["000009"])
        self.assertEqual([s["code"] for s in out], ["000009"])
        self.assertEqual(out[0]["score"], 100.0)

    def test_old_snapshot_warns(self):
        rows = self.by_code()
        rows["000009"]["asOf"] = "2025-03-31"
        out = {s["code"]: s for s in signals.buyback_signals(rows, TODAY)}
        self.assertTrue(out["000009"]["stale"])
        self.assertFalse(out["000008"]["stale"])


class NpsSignalTests(unittest.TestCase):
    def source(self, change) -> dict:
        weights = {f"00000{i}": 0.1 * i for i in range(10)}
        ownership = {f"00000{i}": 5.0 + i for i in range(10)}
        return {"weights": weights, "ownership": ownership, "change": change, "positionsAsOf": "2025-12-31",
                "priceAsOf": "2026-11-06", "names": {"000001": "회사1"}}

    def test_same_file_republished_yields_no_change_and_no_ranking(self):
        change = {"from": "2024-12-31", "to": "2025-12-31", "comparable": False, "changes": {}}
        out = signals.nps_signals(self.source(change), TODAY)
        self.assertEqual(len(out), 10)
        self.assertTrue(all(s["score"] is None and not s["rankable"] for s in out))
        first = next(s for s in out if s["code"] == "000001")
        self.assertNotIn("지분율 변화", labels(first))
        self.assertEqual(labels(first)["공시 지분율"], 6.0)
        self.assertIn("지분율이 같아", first["warnings"][0])

    def test_only_real_increases_are_ranked(self):
        changes = {f"00000{i}": i - 4.0 for i in range(10)}  # -4 ~ +5
        change = {"from": "2024-12-31", "to": "2025-12-31", "comparable": True, "changes": changes}
        out = {s["code"]: s for s in signals.nps_signals(self.source(change), TODAY)}
        self.assertEqual(out["000009"]["score"], 100.0)
        self.assertTrue(out["000009"]["rankable"])
        self.assertIsNone(out["000000"]["score"])  # 줄어든 종목은 기회 신호가 아니다
        self.assertEqual(labels(out["000000"])["지분율 변화"], -4.0)
        self.assertIsNone(out["000004"]["score"])  # 0 변화도 마찬가지
        self.assertEqual(out["000009"]["warnings"], [])

    def test_a_code_missing_from_changes_is_unknown_not_zero(self):
        change = {"from": "2024-12-31", "to": "2025-12-31", "comparable": True, "changes": {"000001": 1.0}}
        out = {s["code"]: s for s in signals.nps_signals(self.source(change), TODAY)}
        self.assertNotIn("지분율 변화", labels(out["000002"]))

    def test_no_change_data_says_so(self):
        out = signals.nps_signals(self.source(None), TODAY)
        self.assertIn("없습니다", out[0]["warnings"][0])

    def test_codes_filter_and_legacy_source_shape(self):
        current = {
            "source": "data.go.kr(2025-12-31)",
            "summary": {"asOf": "2026-11-06"},
            "holdings": [
                {"stock_code": "005930", "stock_name": "삼성전자", "weight": 17.8, "ownership_pct": 7.26},
                {"stock_code": "000020", "stock_name": "동화약품", "weight": None, "ownership_pct": 0.0},
                {"stock_code": None},
                "junk",
            ],
        }
        source = signals.nps_source_from_current(current)
        self.assertEqual(source["weights"], {"005930": 17.8})
        self.assertEqual(source["ownership"], {"005930": 7.26})  # 0.0은 공시 공란
        self.assertEqual(source["positionsAsOf"], "2025-12-31")
        self.assertEqual(source["priceAsOf"], "2026-11-06")
        self.assertIsNone(source["change"])
        out = signals.nps_signals(source, TODAY, codes=["005930"])
        self.assertEqual([s["name"] for s in out], ["삼성전자"])


class DashboardShapeTests(unittest.TestCase):
    """폴백 경로가 발행 필드와 같은 모양을 만들어 정규화가 소스를 구분하지 않는지."""

    def test_spread_pairs_join_prices_config_and_dashboard(self):
        current = {"prices": {"samsung_elec": {"spread": 22.5, "spreadChange": -1.0, "commonPrice": 2, "preferredPrice": 1,
                                               "date": "2026-11-06"}}}
        config = [{"id": "samsung_elec", "name": "삼성전자", "commonTicker": "005930.KS", "preferredTicker": "005935.KS",
                   "preferredName": "삼성전자우"}, "junk"]
        dashboard = {"pairs": [{
            "id": "samsung_elec",
            "attractiveness": {"total": 61.5},
            "current": {"commonAvgTradedValue20": 5e11, "preferredAvgTradedValue20": 3e10},
            "spreadPercentiles": {"date": "2026-11-06", "pctile3yValues": [float(v) for v in range(1, 101)]},
        }]}
        pair = signals.spread_pairs(current, config, dashboard)[0]
        self.assertEqual((pair["commonCode"], pair["preferredCode"]), ("005930", "005935"))
        self.assertEqual(pair["spreadPct3y"], 22)
        self.assertEqual(pair["attractiveness"], 61.5)
        self.assertEqual(signals.preferred_signal(pair, None, TODAY)["averageTradedValue20"], 3e10)

    def test_spread_pairs_without_dashboard_keep_current_values(self):
        current = {"prices": {"a": {"spread": 22.5, "date": "2026-11-06"}}}
        config = [{"id": "a", "name": "A", "commonTicker": "000010", "preferredTicker": "000015"}]
        for dashboard in (None, {"pairs": "nope"}, ["junk"]):
            pair = signals.spread_pairs(current, config, dashboard)[0]
            self.assertEqual(pair["spread"], 22.5)
            self.assertIsNone(pair["spreadPct3y"])

    def test_holding_pairs_use_the_snapshot_ratio(self):
        first = date(2026, 1, 1)
        dates = [(first + timedelta(days=i)).isoformat() for i in range(100)]
        current = {"generatedAt": "2026-11-09T13:26:57+09:00",
                   "pairs": [{"id": "a", "ratio": 80.0, "ratioChange": 1.0, "holdingValue": 2, "marketCap": 1}]}
        config = [{"id": "a", "name": "A→B", "holdingName": "A", "holdingTicker": "000010.KS"}]
        dashboard = {"pairs": [{"id": "a", "percentileHistory": {"dates": dates, "ratio": [float(i) for i in range(1, 101)]},
                                "current": {"pctile3y": 1}}]}  # 멈춘 저장값은 쓰지 않는다
        pair = signals.holding_pairs(current, config, dashboard)[0]
        self.assertEqual(pair["code"], "000010")
        self.assertEqual(pair["ratioPct3y"], 80)
        self.assertEqual(pair["ratioPctEnd"], dates[-1])

    def test_percentile_by_code_follows_the_first_pair_rule(self):
        items = [
            {"code": "005387", "relatedCode": "005380", "score": 50},
            {"code": "005385", "relatedCode": "005380", "score": 95},
            {"code": "000010", "relatedCode": None, "score": None},
        ]
        out = signals.percentile_by_code(items, "code", "relatedCode")
        self.assertEqual(out, {"005387": 50, "005380": 50, "005385": 95})


class LiquidityTests(unittest.TestCase):
    def test_tool_average_comes_first(self):
        info = signals.liquidity_info(5e7, "2026-11-06", {"trade_value": 9e9, "date": "2026-11-09"})
        self.assertEqual(info, {"value": 5e7, "basis": "avg20d", "asOf": "2026-11-06", "low": True})

    def test_shared_quote_is_the_fallback_with_its_own_date(self):
        info = signals.liquidity_info(None, None, {"trade_value": 9e9, "date": "2026-11-06"})
        self.assertEqual(info, {"value": 9e9, "basis": "session", "asOf": "2026-11-06", "low": False})

    def test_unknown_liquidity_is_none_not_zero(self):
        empty = {"value": None, "basis": None, "asOf": None, "low": None}
        self.assertEqual(signals.liquidity_info(), empty)
        self.assertEqual(signals.liquidity_info(None, None, {}), empty)
        self.assertEqual(signals.liquidity_info(None, None, {"trade_value": None}), empty)
        self.assertEqual(signals.liquidity_info(None, None, {"trade_value": "1,000"}), empty)

    def test_intraday_accumulation_is_not_called_low(self):
        info = signals.liquidity_info(None, None, {"trade_value": 1e7, "date": "2026-11-09"}, session_complete=False)
        self.assertIsNone(info["low"])
        self.assertTrue(signals.liquidity_info(None, None, {"trade_value": 1e7}, session_complete=True)["low"])


def ranked(code: str, tool: str, score, basis=signals.BASIS_HISTORY, **over) -> dict:
    return {"tool": tool, "code": code, "name": code, "score": score, "scoreBasis": basis,
            "rankable": score is not None, "relatedCode": None, "metrics": [], "warnings": [],
            "averageTradedValue20": None, "asOf": "2026-11-06", **over}


class RankingTests(unittest.TestCase):
    def test_bands_come_first_then_own_history_then_score(self):
        rows, _ = signals.select_candidates([
            ranked("C1", signals.TOOL_BUYBACKS, 99.9, signals.BASIS_CROSS_SECTION),
            ranked("H1", signals.TOOL_HOLDING, 91),
            ranked("P1", signals.TOOL_PREFERRED, 96),
            ranked("S1", signals.TOOL_SPAC, 89.0, signals.BASIS_CROSS_SECTION),
            ranked("P2", signals.TOOL_PREFERRED, 85),
        ])
        self.assertEqual([r["code"] for r in rows], ["P1", "H1", "C1", "P2", "S1"])

    def test_scores_below_the_floor_and_non_rankable_signals_are_left_out(self):
        rows, eligible = signals.select_candidates([
            ranked("A", signals.TOOL_PREFERRED, 79),
            ranked("B", signals.TOOL_PREFERRED, 80),
            ranked("C", signals.TOOL_SPAC, 99.0, signals.BASIS_CROSS_SECTION, rankable=False),
            ranked("D", signals.TOOL_NPS, None, None),
        ])
        self.assertEqual([r["code"] for r in rows], ["B"])
        self.assertEqual(eligible, {signals.TOOL_PREFERRED: 1})

    def test_per_tool_cap_keeps_the_best_and_reports_how_many_were_cut(self):
        items = [ranked(f"C{i:03d}", signals.TOOL_BUYBACKS, 99.0 - i / 10, signals.BASIS_CROSS_SECTION) for i in range(30)]
        items.append(ranked("P1", signals.TOOL_PREFERRED, 85))
        rows, eligible = signals.select_candidates(items, per_tool=3)
        self.assertEqual(eligible[signals.TOOL_BUYBACKS], 30)
        self.assertEqual([r["code"] for r in rows], ["C000", "C001", "C002", "P1"])

    def test_one_row_per_code_with_the_other_tools_as_context(self):
        rows, _ = signals.select_candidates([
            ranked("000670", signals.TOOL_HOLDING, 93),
            ranked("000670", signals.TOOL_BUYBACKS, 95.0, signals.BASIS_CROSS_SECTION),
            ranked("000670", signals.TOOL_NPS, None, None),
        ])
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["primary"]["tool"], signals.TOOL_HOLDING)  # 같은 구간이면 자기 역사 기준이 대표
        self.assertEqual([s["tool"] for s in row["others"]], [signals.TOOL_BUYBACKS, signals.TOOL_NPS])

    def test_preferred_row_gets_the_common_stock_context(self):
        rows, _ = signals.select_candidates([
            ranked("005935", signals.TOOL_PREFERRED, 92, relatedCode="005930"),
            ranked("005930", signals.TOOL_BUYBACKS, 50.0, signals.BASIS_CROSS_SECTION),
        ])
        self.assertEqual([s["code"] for s in rows[0]["others"]], ["005930"])

    def test_ties_go_to_the_more_liquid_row_and_unknown_liquidity_sorts_last(self):
        rows, _ = signals.select_candidates([
            ranked("A", signals.TOOL_PREFERRED, 96),
            ranked("B", signals.TOOL_PREFERRED, 96, averageTradedValue20=5e9),
            ranked("C", signals.TOOL_PREFERRED, 96),
        ])
        quotes = {"C": {"trade_value": 1e9, "date": "2026-11-06"}}
        ordered = signals.rank_rows(rows, quotes)
        self.assertEqual([(r["rank"], r["code"]) for r in ordered], [(1, "B"), (2, "C"), (3, "A")])
        self.assertIsNone(ordered[2]["liquidity"]["value"])

    def test_liquidity_never_changes_a_higher_score_band(self):
        rows, _ = signals.select_candidates([
            ranked("LOW", signals.TOOL_PREFERRED, 99, averageTradedValue20=1e6),
            ranked("BIG", signals.TOOL_PREFERRED, 90, averageTradedValue20=1e12),
        ])
        self.assertEqual([r["code"] for r in signals.rank_rows(rows, {})], ["LOW", "BIG"])

    def test_a_score_of_100_shares_the_top_band(self):
        rows, _ = signals.select_candidates([
            ranked("S", signals.TOOL_SPAC, 100.0, signals.BASIS_CROSS_SECTION),
            ranked("P", signals.TOOL_PREFERRED, 90),
        ])
        self.assertEqual([r["code"] for r in rows], ["P", "S"])


if __name__ == "__main__":
    unittest.main()
