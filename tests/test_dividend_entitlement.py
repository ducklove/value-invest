"""배당 권리 기준 시점(시장 시간대·국내 T+2)과 일별 정산 보유 판정의 순수 규칙."""

import unittest
from datetime import date

from domain.dividend_entitlement import (
    HoldingHistory,
    entitlement,
    holding_identity,
    krx_record_entitlement_day,
    market_side,
    reference_point,
)
from domain.market_calendar import is_trading_day


def rows(table: dict) -> list[dict]:
    """{날짜: {코드: 수량}} → 정산 행. 수량 None은 수량 기록 전 정산(평가액만 있음)."""
    return [{"date": day, "stock_code": code, "quantity": qty, "market_value": 1000.0}
            for day, holdings in table.items() for code, qty in holdings.items()]


class MarketTests(unittest.TestCase):
    def test_market_side_by_code(self):
        for code in ("005930", "0074K0.KS", "005930.KQ"):
            self.assertEqual(market_side(code), "KR", code)
        for code in ("0700.HK", "7203.T", "A200.AX", "600519.SS", "D05.SI", "2330.TW", "FUEVFVND.HM", "FUEVFVND.VN", "PTT.BK"):
            self.assertEqual(market_side(code), "east", code)
        for code in ("AAPL", "GOOGL.O", "SCHP.K", "BRK.B", "BRK-B", "EUN2.DE", "BP.L", "SHOP.TO", "ewm"):
            self.assertEqual(market_side(code), "west", code)

    def test_identity_joins_code_spellings(self):
        self.assertEqual(holding_identity("GOOGL.O"), holding_identity("GOOGL"))
        self.assertEqual(holding_identity("SCHP.K"), "SCHP")
        self.assertEqual(holding_identity("0074K0.KS"), "0074K0")
        self.assertEqual(holding_identity("ewm"), "EWM")
        self.assertEqual(holding_identity("BRK.B"), holding_identity("BRK-B"))
        self.assertNotEqual(holding_identity("AAA.AX"), holding_identity("AAA"))

    def test_trading_day_helper_uses_krx_calendar(self):
        self.assertTrue(is_trading_day(date(2026, 9, 23)))
        self.assertFalse(is_trading_day(date(2026, 9, 24)))   # 추석
        self.assertFalse(is_trading_day(date(2026, 9, 26)))   # 토요일
        self.assertFalse(is_trading_day(date(2026, 12, 31)))  # 연말 휴장
        self.assertTrue(is_trading_day(date(2026, 11, 19)))   # 수능일 후보: 시각만 바뀌는 거래일
        self.assertIsNone(is_trading_day(date(2025, 6, 2)))   # 달력이 없는 연도


class ReferencePointTests(unittest.TestCase):
    def test_same_ex_date_differs_by_market_timezone(self):
        ex = {"ex_date": "2026-09-15"}
        self.assertEqual(reference_point(ex, "AAPL")["date"], date(2026, 9, 15))      # 미국: KST 오후 정산이 전 거래일까지
        self.assertEqual(reference_point(ex, "EUN2.DE")["date"], date(2026, 9, 15))
        self.assertEqual(reference_point(ex, "0700.HK")["date"], date(2026, 9, 14))   # 홍콩: 그날 정산이 배당락 거래를 담는다
        self.assertEqual(reference_point(ex, "005930")["date"], date(2026, 9, 14))
        self.assertEqual(reference_point(ex, "AAPL")["rule"], "ex_date_same_day")
        self.assertEqual(reference_point(ex, "0700.HK")["rule"], "ex_date_prev_day")

    def test_krx_record_date_is_two_trading_days_back_across_holiday_and_weekend(self):
        # 2026-09-28(월) 기준일: 9/24·25 추석, 9/26·27 주말 → 9/23, 9/22.
        self.assertEqual(krx_record_entitlement_day(date(2026, 9, 28)), (date(2026, 9, 22), False))
        # 기준일이 휴장(12/31)이면 그 이하 마지막 거래일(12/30)에서 2거래일 전.
        self.assertEqual(krx_record_entitlement_day(date(2026, 12, 31)), (date(2026, 12, 28), False))
        point = reference_point({"record_date": "2026-09-28", "pay_date": "2026-11-20"}, "005930")
        self.assertEqual((point["date"], point["rule"], point["approximate"]), (date(2026, 9, 22), "krx_record_t2", False))
        # 휴장일 달력이 없는 연도는 평일로 근사한다.
        self.assertEqual(krx_record_entitlement_day(date(2025, 12, 31)), (date(2025, 12, 29), True))

    def test_overseas_record_and_payment_only_are_approximate(self):
        point = reference_point({"record_date": "2026-09-15"}, "AAPL")
        self.assertEqual((point["date"], point["rule"], point["approximate"]), (date(2026, 9, 15), "record_date", True))
        point = reference_point({"pay_date": "2026-09-15"}, "AAPL")
        self.assertEqual((point["date"], point["rule"], point["approximate"]), (date(2026, 9, 14), "pay_date", True))
        self.assertIsNone(reference_point({}, "AAPL"))


class HoldingHistoryTests(unittest.TestCase):
    def test_latest_snapshot_on_or_before_bound(self):
        history = HoldingHistory(rows({"2026-09-14": {"AAPL": 3}, "2026-09-16": {"AAPL": 5}}))
        point = history.at("AAPL", date(2026, 9, 15))
        self.assertEqual((point["held"], point["quantity"], point["basis"], point["as_of"]), (True, 3.0, "snapshot", "2026-09-14"))
        self.assertFalse(history.at("MSFT", date(2026, 9, 15))["held"])

    def test_bound_before_first_snapshot_uses_first_snapshot(self):
        history = HoldingHistory(rows({"2026-03-31": {"AAPL": 3}, "2026-04-01": {"AAPL": 3, "MSFT": 1}}))
        point = history.at("AAPL", date(2025, 12, 1))
        self.assertEqual((point["held"], point["basis"], point["as_of"]), (True, "earliest_snapshot", "2026-03-31"))
        self.assertFalse(history.at("MSFT", date(2025, 12, 1))["held"])  # 첫 기록에 없음 = 나중에 샀다

    def test_short_and_zero_positions_are_not_held(self):
        history = HoldingHistory(rows({"2026-09-14": {"000880": -1000, "005930": 0}}))
        self.assertFalse(history.at("000880", date(2026, 9, 14))["held"])
        self.assertFalse(history.at("005930", date(2026, 9, 14))["held"])

    def test_single_snapshot_gap_is_filled_with_previous_quantity(self):
        # 운영 사례: 9/14 하루만 빠졌다가 9/15에 같은 수량으로 다시 보인다.
        history = HoldingHistory(rows({"2026-09-11": {"003200": 3000, "X": 1}, "2026-09-14": {"X": 1},
                                       "2026-09-15": {"003200": 3000, "X": 1}}))
        point = history.at("003200", date(2026, 9, 14))
        self.assertEqual((point["held"], point["quantity"], point["gap_filled"], point["quantity_as_of"]),
                         (True, 3000.0, True, "2026-09-11"))
        # 오래 빠진 종목(매도 후 재매수)은 메우지 않는다.
        long_gap = {f"2026-05-{d:02d}": {"X": 1} for d in (11, 12, 13, 14, 15, 18, 19, 20, 21)}
        long_gap["2026-05-11"]["35320K"] = 1
        long_gap["2026-05-21"]["35320K"] = 1
        self.assertFalse(HoldingHistory(rows(long_gap)).at("35320K", date(2026, 5, 15))["held"])

    def test_legacy_rows_without_quantity_take_nearest_known_quantity(self):
        history = HoldingHistory(rows({"2026-06-26": {"SCHP.O": None}, "2026-06-29": {"SCHP.O": None},
                                       "2026-06-30": {"SCHP.O": 120}, "2026-09-22": {"SCHP": 100}}))
        point = history.at("SCHP", date(2026, 6, 27))
        self.assertEqual((point["held"], point["quantity"], point["as_of"], point["quantity_as_of"]),
                         (True, 120.0, "2026-06-26", "2026-06-30"))
        self.assertEqual(history.at("SCHP", date(2026, 9, 22))["quantity"], 100.0)  # 코드 표기 변경도 같은 종목
        # 평가액이 없는 수량 미기록 행은 보유가 아니다.
        empty = HoldingHistory([{"date": "2026-06-26", "stock_code": "SCHP", "quantity": None, "market_value": 0}])
        self.assertFalse(empty.at("SCHP", date(2026, 6, 26))["held"])


class EntitlementTests(unittest.TestCase):
    TODAY = date(2026, 10, 1)

    def setUp(self):
        self.history = HoldingHistory(rows({"2026-09-01": {"GOOGL": 10}, "2026-09-30": {"GOOGL": 15}}))

    def test_past_reference_uses_snapshot_and_future_or_estimated_use_current(self):
        past = entitlement("GOOGL", {"ex_date": "2026-09-08"}, self.TODAY, self.history, 15.0)
        self.assertEqual((past["held"], past["quantity"], past["holding_basis"], past["holding_as_of"], past["reference_date"]),
                         (True, 10.0, "snapshot", "2026-09-01", "2026-09-08"))
        future = entitlement("GOOGL", {"ex_date": "2026-10-01"}, self.TODAY, self.history, 15.0)
        self.assertEqual((future["quantity"], future["holding_basis"]), (15.0, "current"))
        estimated = entitlement("GOOGL", {"pay_date": "2026-09-01", "estimated": True}, self.TODAY, self.history, 15.0)
        self.assertEqual((estimated["quantity"], estimated["holding_basis"], estimated["reference_date"]), (15.0, "current", None))

    def test_not_held_reasons_and_no_records_fallback(self):
        dropped = entitlement("AAPL", {"ex_date": "2026-09-08"}, self.TODAY, self.history, 4.0)
        self.assertEqual((dropped["held"], dropped["excluded_reason"]), (False, "not_held_at_reference"))
        early = entitlement("AAPL", {"ex_date": "2026-08-01"}, self.TODAY, self.history, 4.0)
        self.assertEqual((early["held"], early["excluded_reason"]), (False, "absent_from_first_record"))
        sold_future = entitlement("O", {"ex_date": "2026-10-15"}, self.TODAY, self.history, None)
        self.assertEqual((sold_future["held"], sold_future["excluded_reason"]), (False, "not_held_now"))
        fallback = entitlement("AAPL", {"ex_date": "2026-09-08"}, self.TODAY, HoldingHistory([]), 4.0)
        self.assertEqual((fallback["held"], fallback["quantity"], fallback["holding_basis"]), (True, 4.0, "current_fallback"))


if __name__ == "__main__":
    unittest.main()
