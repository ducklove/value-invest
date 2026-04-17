import unittest
from analyzer import analyze, _safe_div, _to_eok_won


class SafeDivTests(unittest.TestCase):
    def test_normal_division(self):
        self.assertAlmostEqual(_safe_div(10, 2), 5.0)

    def test_zero_denominator(self):
        self.assertIsNone(_safe_div(10, 0))

    def test_none_numerator(self):
        self.assertIsNone(_safe_div(None, 2))

    def test_none_denominator(self):
        self.assertIsNone(_safe_div(10, None))

    def test_multiply(self):
        self.assertAlmostEqual(_safe_div(1, 2, multiply=100), 50.0)


class ToEokWonTests(unittest.TestCase):
    def test_normal(self):
        self.assertAlmostEqual(_to_eok_won(100_000_000), 1.0)

    def test_none(self):
        self.assertIsNone(_to_eok_won(None))

    def test_zero(self):
        self.assertAlmostEqual(_to_eok_won(0), 0.0)


class AnalyzeTests(unittest.TestCase):
    def test_empty_input(self):
        result = analyze([], [], [])
        self.assertIn("years", result)
        self.assertIn("indicators", result)
        self.assertEqual(result["years"], [])

    def test_single_year(self):
        fin = [{"year": 2023, "revenue": 1_000_000_000, "operating_profit": 100_000_000,
                "net_income": 80_000_000, "total_equity": 500_000_000,
                "total_assets": 1_000_000_000, "total_liabilities": 500_000_000}]
        mkt = [{"year": 2023, "close_price": 50000, "eps": 1000, "bps": 5000,
                "dividend_per_share": 500, "dividend_yield": 1.0,
                "market_cap": 5_000_000_000_000, "per": 50.0, "pbr": 10.0,
                "shares_outstanding": 100_000_000}]
        result = analyze(fin, mkt)
        self.assertEqual(result["years"], [2023])
        indicators = result["indicators"]
        # ROE = net_income / equity * 100 = 80M / 500M * 100 = 16.0
        roe_values = indicators["ROE (%)"]
        self.assertEqual(len(roe_values), 1)
        self.assertAlmostEqual(roe_values[0]["value"], 16.0)
        # Debt ratio = liabilities / equity * 100 = 500M / 500M * 100 = 100.0
        debt_values = indicators["부채비율 (%)"]
        self.assertEqual(len(debt_values), 1)
        self.assertAlmostEqual(debt_values[0]["value"], 100.0)

    def test_per_pbr_from_market_data(self):
        fin = [{"year": 2023, "revenue": 1e9, "operating_profit": 1e8,
                "net_income": 5e7, "total_equity": 3e8,
                "total_assets": 6e8, "total_liabilities": 3e8}]
        mkt = [{"year": 2023, "close_price": 10000, "eps": 500, "bps": 3000,
                "dividend_per_share": 0, "dividend_yield": 0,
                "market_cap": 1e12, "per": 20.0, "pbr": 3.33,
                "shares_outstanding": 1e8}]
        result = analyze(fin, mkt)
        per_vals = result["indicators"]["PER"]
        self.assertAlmostEqual(per_vals[0]["value"], 20.0)

    def test_weekly_indicators(self):
        fin = [{"year": 2023, "revenue": 1e9, "operating_profit": 1e8,
                "net_income": 5e7, "total_equity": 3e8,
                "total_assets": 6e8, "total_liabilities": 3e8}]
        mkt = [{"year": 2023, "close_price": 10000, "eps": 500, "bps": 3000,
                "dividend_per_share": 100, "dividend_yield": 1.0,
                "market_cap": 1e12, "per": 20.0, "pbr": 3.33,
                "shares_outstanding": 1e8}]
        weekly = [{"date": "2023-12-29", "close_price": 10000, "per": 20.0, "pbr": 3.33, "dividend_yield": 1.0}]
        result = analyze(fin, mkt, weekly)
        self.assertIn("weekly_indicators", result)
        weekly_price = result["weekly_indicators"]["주가"]
        self.assertEqual(len(weekly_price), 1)
        self.assertEqual(weekly_price[0]["value"], 10000)

    def test_operating_margin(self):
        fin = [{"year": 2023, "revenue": 1_000_000, "operating_profit": 200_000,
                "net_income": 100_000, "total_equity": 500_000,
                "total_assets": 800_000, "total_liabilities": 300_000}]
        result = analyze(fin, [])
        margins = result["indicators"]["영업이익률 (%)"]
        self.assertEqual(len(margins), 1)
        self.assertAlmostEqual(margins[0]["value"], 20.0)

    def test_market_only_years(self):
        """Market data years without matching financial data should still appear."""
        mkt = [{"year": 2022, "close_price": 5000, "eps": 100, "bps": 1000,
                "dividend_per_share": 0, "dividend_yield": 0,
                "market_cap": 1e11, "per": 50.0, "pbr": 5.0,
                "shares_outstanding": 2e7}]
        result = analyze([], mkt)
        self.assertIn(2022, result["years"])
        self.assertEqual(len(result["indicators"]["주가 (원)"]), 1)
