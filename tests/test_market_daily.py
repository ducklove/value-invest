import unittest

import market_daily


class MarketDailyRuleTests(unittest.TestCase):
    def test_relative_move_uses_two_percent_threshold_when_kospi_is_quiet(self):
        result = market_daily.detect_relative_move(3.1, 0.8)

        self.assertTrue(result["is_notable"])
        self.assertEqual(result["move_type"], "급등")
        self.assertEqual(result["threshold_pct"], 2.0)
        self.assertEqual(result["relative_pct"], 2.3)

    def test_relative_move_uses_kospi_move_when_market_is_wide(self):
        result = market_daily.detect_relative_move(-5.8, -3.0)

        self.assertFalse(result["is_notable"])
        self.assertEqual(result["threshold_pct"], 3.0)
        self.assertEqual(result["relative_pct"], -2.8)

    def test_gemini35_flash_cost_estimate(self):
        self.assertAlmostEqual(
            market_daily.estimate_gemini35_flash_cost(12_000, 1_200),
            0.0288,
        )
