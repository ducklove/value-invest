import unittest
from datetime import date
from unittest.mock import patch

import stock_price
from stock_price import (
    _safe_float, _safe_div, _get_first,
    _group_close_by_year, _group_dividends_by_year,
    _forward_fill_year_values, _parse_date, _parse_year,
    _estimate_price_adjustment_factors,
    _adjust_dividends_by_price_factors,
    _build_dividend_events,
    _get_history_close_series,
    _naver_quote_from_block,
    _parse_naver_bulk_entry,
    pd,
)


class KrxLimitTests(unittest.TestCase):
    """상한가/하한가는 base×1.30/×0.70 을 해당 가격대 호가단위로 내림/올림한
    '정확한' 호가여야 한다(근사 ±30% 아님)."""

    def test_tick_sizes(self):
        from services.krx_limits import krx_tick_size
        self.assertEqual(krx_tick_size(1999), 1)
        self.assertEqual(krx_tick_size(2000), 5)
        self.assertEqual(krx_tick_size(5000), 10)
        self.assertEqual(krx_tick_size(20000), 50)
        self.assertEqual(krx_tick_size(50000), 100)
        self.assertEqual(krx_tick_size(200000), 500)
        self.assertEqual(krx_tick_size(500000), 1000)

    def test_clean_multiples_are_exactly_30pct(self):
        from services.krx_limits import krx_upper_limit, krx_lower_limit
        for base in (1000, 10000, 50000, 100000, 200000, 500000):
            self.assertEqual(krx_upper_limit(base), base * 1.3)
            self.assertEqual(krx_lower_limit(base), base * 0.7)

    def test_tick_rounding_band_crossing(self):
        from services.krx_limits import krx_upper_limit, krx_lower_limit
        # 기준가 89,600 → 상한가 116,400(=tick100 내림, +29.91%), 하한가 62,800.
        self.assertEqual(krx_upper_limit(89600), 116400)
        self.assertEqual(krx_lower_limit(89600), 62800)
        # base tick(10) ≠ 상한가대 tick(50) 경계: 19,010 → 상한가 24,700.
        self.assertEqual(krx_upper_limit(19010), 24700)

    def test_none_for_invalid_base(self):
        from services.krx_limits import krx_upper_limit, krx_lower_limit
        self.assertIsNone(krx_upper_limit(None))
        self.assertIsNone(krx_lower_limit(0))


class NaverBulkMarketSelectionTests(unittest.TestCase):
    """Outside regular KRX hours the bulk (Naver) path must mirror the live KIS
    market (active_market_code) and surface the NXT after-hours price; during
    regular hours it uses the KRX regular-session price. Keeps the initial
    portfolio paint from flickering KRX↔NXT against the live stream in 시간외."""

    ENTRY = {
        "itemCode": "005930",
        "closePrice": "360,500",
        "compareToPreviousPrice": {"code": "2"},
        "compareToPreviousClosePrice": "11,500",
        "fluctuationsRatio": "3.30",
        "localTradedAt": "2026-06-02T15:30:00+09:00",
        "overMarketPriceInfo": {
            "overMarketStatus": "CLOSE",  # NXT 마감 후에도 NX면 NXT가를 써야
            "overPrice": "362,500",
            "compareToPreviousPrice": {"code": "2"},
            "compareToPreviousClosePrice": "13,500",
            "fluctuationsRatio": "3.87",
            "localTradedAt": "2026-06-02T20:00:00.000000+09:00",
        },
    }

    def test_after_hours_uses_nxt_overprice(self):
        with patch.object(stock_price.kis_ws_manager, "active_market_code", return_value="NX"):
            code, q = _parse_naver_bulk_entry(self.ENTRY)
        self.assertEqual(code, "005930")
        self.assertEqual(q["price"], 362500)
        self.assertAlmostEqual(q["change_pct"], 3.87, places=2)

    def test_regular_hours_uses_krx_close(self):
        with patch.object(stock_price.kis_ws_manager, "active_market_code", return_value="J"):
            _, q = _parse_naver_bulk_entry(self.ENTRY)
        self.assertEqual(q["price"], 360500)
        self.assertAlmostEqual(q["change_pct"], 3.30, places=2)

    def test_nxt_unavailable_falls_back_to_krx(self):
        entry = {k: v for k, v in self.ENTRY.items() if k != "overMarketPriceInfo"}
        with patch.object(stock_price.kis_ws_manager, "active_market_code", return_value="NX"):
            _, q = _parse_naver_bulk_entry(entry)
        self.assertEqual(q["price"], 360500)  # NXT 미거래 → KRX 폴백


class NaverQuoteSignTests(unittest.TestCase):
    """Naver returns compareToPreviousClosePrice / fluctuationsRatio already
    SIGNED. The parser must not multiply by the direction code again (that
    flipped every DOWN stock to a positive change — e.g. -7.6% → +8.9%)."""

    @staticmethod
    def _block(price, change, sign_code, ratio):
        return {
            "compareToPreviousPrice": {"code": sign_code},
            "compareToPreviousClosePrice": change,
            "fluctuationsRatio": ratio,
            "accumulatedTradingVolume": "1,000",
        }

    def test_down_stock_keeps_negative_change(self):
        # 두산퓨얼셀 사례: 종가 82,800, 전일대비 -6,800(signCode 5=하락), -7.59%.
        q = _naver_quote_from_block("closePrice", {
            "closePrice": "82,800", **self._block("82,800", "-6,800", "5", "-7.59"),
        })
        self.assertEqual(q["change"], -6800)
        self.assertEqual(q["previous_close"], 89600)
        self.assertAlmostEqual(q["change_pct"], -7.59, places=2)

    def test_up_stock_stays_positive(self):
        q = _naver_quote_from_block("closePrice", {
            "closePrice": "116,300", **self._block("116,300", "100", "2", "0.09"),
        })
        self.assertEqual(q["change"], 100)
        self.assertGreater(q["change_pct"], 0)

    def test_unsigned_magnitude_still_resolves_direction(self):
        # 원천이 부호 없는 크기로 와도 signCode로 방향을 맞춘다.
        q = _naver_quote_from_block("closePrice", {
            "closePrice": "82,800", **self._block("82,800", "6,800", "5", "7.59"),
        })
        self.assertEqual(q["change"], -6800)
        self.assertLess(q["change_pct"], 0)

    def test_ratio_fallback_when_change_missing(self):
        q = _naver_quote_from_block("closePrice", {
            "closePrice": "82,800", **self._block("82,800", None, "5", "-7.59"),
        })
        self.assertAlmostEqual(q["change_pct"], -7.59, places=2)


class SafeFloatTests(unittest.TestCase):
    def test_normal(self):
        self.assertAlmostEqual(_safe_float("123.45"), 123.45)

    def test_none(self):
        self.assertIsNone(_safe_float(None))

    def test_empty_string(self):
        self.assertIsNone(_safe_float(""))

    def test_zero_as_none(self):
        self.assertIsNone(_safe_float(0, zero_as_none=True))

    def test_zero_kept(self):
        self.assertAlmostEqual(_safe_float(0, zero_as_none=False), 0.0)

    def test_invalid_string(self):
        self.assertIsNone(_safe_float("abc"))

    def test_comma_separated(self):
        self.assertAlmostEqual(_safe_float("1,234.5"), 1234.5)


class SafeDivTests(unittest.TestCase):
    def test_normal(self):
        self.assertAlmostEqual(_safe_div(10, 2), 5.0)

    def test_zero_denominator(self):
        self.assertIsNone(_safe_div(10, 0))

    def test_none_values(self):
        self.assertIsNone(_safe_div(None, 2))
        self.assertIsNone(_safe_div(10, None))

    def test_multiply(self):
        self.assertAlmostEqual(_safe_div(1, 4, multiply=100), 25.0)


class GetFirstTests(unittest.TestCase):
    def test_found(self):
        self.assertEqual(_get_first({"a": 1, "b": 2}, "a", "b"), 1)

    def test_fallback(self):
        self.assertEqual(_get_first({"b": 2}, "a", "b"), 2)

    def test_none_mapping(self):
        self.assertIsNone(_get_first(None, "a"))

    def test_skip_none_values(self):
        self.assertEqual(_get_first({"a": None, "b": 2}, "a", "b"), 2)

    def test_skip_empty_string(self):
        self.assertEqual(_get_first({"a": "", "b": "ok"}, "a", "b"), "ok")


class GroupCloseByYearTests(unittest.TestCase):
    def test_normal(self):
        items = [
            {"stck_bsop_date": "20230615", "stck_clpr": "50000"},
            {"stck_bsop_date": "20231229", "stck_clpr": "55000"},
            {"stck_bsop_date": "20240615", "stck_clpr": "60000"},
        ]
        result = _group_close_by_year(items)
        self.assertAlmostEqual(result[2023], 55000.0)
        self.assertAlmostEqual(result[2024], 60000.0)

    def test_alternative_keys(self):
        items = [
            {"date": "2023-06-15", "close": "50000"},
            {"date": "2023-12-29", "close": "55000"},
        ]
        result = _group_close_by_year(items)
        self.assertAlmostEqual(result[2023], 55000.0)

    def test_empty(self):
        self.assertEqual(_group_close_by_year([]), {})

    def test_none(self):
        self.assertEqual(_group_close_by_year(None), {})


@unittest.skipIf(pd is None, "pandas is not installed")
class HistoryCloseSeriesTests(unittest.TestCase):
    def test_prefers_close_over_negative_adj_close(self):
        history = pd.DataFrame({
            "Close": [84525.0],
            "Adj Close": [-35781.39],
        })

        result = _get_history_close_series(history)

        self.assertEqual(float(result.iloc[0]), 84525.0)


class GroupDividendsByYearTests(unittest.TestCase):
    def test_sums_within_year(self):
        items = [
            {"record_date": "20230315", "per_sto_divi_amt": "500"},
            {"record_date": "20230915", "per_sto_divi_amt": "500"},
            {"record_date": "20240315", "per_sto_divi_amt": "600"},
        ]
        result = _group_dividends_by_year(items)
        self.assertAlmostEqual(result[2023], 1000.0)
        self.assertAlmostEqual(result[2024], 600.0)

    def test_preserves_explicit_zero_dividend(self):
        items = [
            {"record_date": "20021231", "per_sto_divi_amt": "0", "face_val": "5000"},
        ]

        self.assertEqual(_group_dividends_by_year(items), {2002: 0.0})

    def test_empty(self):
        self.assertEqual(_group_dividends_by_year([]), {})

    def test_none(self):
        self.assertEqual(_group_dividends_by_year(None), {})


class BuildDividendEventsTests(unittest.TestCase):
    def test_sorted_output(self):
        items = [
            {"date": "2024-06-15", "dividend_per_share": "600"},
            {"date": "2023-03-15", "dividend_per_share": "500"},
        ]
        result = _build_dividend_events(items)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][0], date(2023, 3, 15))
        self.assertEqual(result[1][0], date(2024, 6, 15))


class EstimatePriceAdjustmentFactorsTests(unittest.TestCase):
    def test_normal(self):
        adjusted = {2023: 50000.0, 2024: 60000.0}
        raw = {2023: 100000.0, 2024: 60000.0}
        result = _estimate_price_adjustment_factors(adjusted, raw)
        self.assertAlmostEqual(result[2023], 2.0)
        self.assertAlmostEqual(result[2024], 1.0)

    def test_missing_raw(self):
        adjusted = {2023: 50000.0}
        raw = {}
        result = _estimate_price_adjustment_factors(adjusted, raw)
        self.assertEqual(result, {})


class AdjustDividendsByPriceFactorsTests(unittest.TestCase):
    def test_applies_factor(self):
        dividends = {2022: 1000.0, 2023: 500.0}
        factors = {2022: 2.0, 2023: 1.0}
        result = _adjust_dividends_by_price_factors(dividends, factors)
        self.assertAlmostEqual(result[2022], 500.0)
        self.assertAlmostEqual(result[2023], 500.0)

    def test_no_factor(self):
        dividends = {2023: 500.0}
        factors = {}
        result = _adjust_dividends_by_price_factors(dividends, factors)
        self.assertAlmostEqual(result[2023], 500.0)


class ForwardFillTests(unittest.TestCase):
    def test_fills_gaps(self):
        values = {2020: 100.0, 2022: 200.0}
        result = _forward_fill_year_values(values, [2020, 2021, 2022, 2023])
        self.assertEqual(result[2020], 100.0)
        self.assertEqual(result[2021], 100.0)
        self.assertEqual(result[2022], 200.0)
        self.assertEqual(result[2023], 200.0)

    def test_empty(self):
        self.assertEqual(_forward_fill_year_values({}, [2020, 2021]), {})

    def test_no_fill_before_first(self):
        values = {2022: 100.0}
        result = _forward_fill_year_values(values, [2020, 2021, 2022])
        self.assertNotIn(2020, result)
        self.assertNotIn(2021, result)
        self.assertEqual(result[2022], 100.0)


class ParseTests(unittest.TestCase):
    def test_parse_date_yyyymmdd(self):
        self.assertEqual(_parse_date("20230615"), date(2023, 6, 15))

    def test_parse_date_iso(self):
        self.assertEqual(_parse_date("2023-06-15"), date(2023, 6, 15))

    def test_parse_date_slash(self):
        self.assertEqual(_parse_date("2023/06/15"), date(2023, 6, 15))

    def test_parse_date_none(self):
        self.assertIsNone(_parse_date(None))

    def test_parse_date_empty(self):
        self.assertIsNone(_parse_date(""))

    def test_parse_year_valid(self):
        self.assertEqual(_parse_year("2023"), 2023)

    def test_parse_year_from_date(self):
        self.assertEqual(_parse_year("20230615"), 2023)

    def test_parse_year_none(self):
        self.assertIsNone(_parse_year(None))

    def test_parse_year_empty(self):
        self.assertIsNone(_parse_year(""))
