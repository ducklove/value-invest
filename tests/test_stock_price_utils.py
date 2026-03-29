import unittest
from datetime import date, datetime
from stock_price import (
    _safe_float, _safe_div, _get_first,
    _group_close_by_year, _group_dividends_by_year,
    _forward_fill_year_values, _parse_date, _parse_year,
    _estimate_price_adjustment_factors,
    _adjust_dividends_by_price_factors,
    _build_dividend_events,
)


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
