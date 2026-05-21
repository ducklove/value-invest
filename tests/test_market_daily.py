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

    def test_market_tape_prioritizes_breaking_events(self):
        payload = {
            "market": [
                {"code": "KOSPI", "label": "KOSPI", "value": "2,700.00", "change_pct": -2.3},
                {"code": "USD_KRW", "label": "달러/원", "value": "1,350.00", "change_pct": 0.2},
            ],
            "moves": [
                {
                    "stock_code": "005930",
                    "stock_name": "삼성전자",
                    "price": 80000,
                    "change_pct": 4.5,
                    "relative_pct": 6.8,
                    "move_type": "급등",
                    "is_notable": True,
                },
            ],
            "disclosures": [
                {
                    "stock_code": "000660",
                    "stock_name": "SK하이닉스",
                    "report_name": "단일판매ㆍ공급계약체결",
                    "rcept_no": "202605200001",
                    "is_material": True,
                    "material_reason": "공급계약",
                    "url": "https://dart.example.test",
                },
            ],
            "news": [
                {
                    "stock_code": "005930",
                    "title": "삼성전자 장중 강세",
                    "outlet": "테스트뉴스",
                    "published_at": "2026.05.20 10:00",
                },
            ],
        }

        events = market_daily.build_market_tape_events(payload)

        self.assertGreaterEqual(len(events), 4)
        self.assertEqual(events[0]["type"], "disclosure")
        self.assertEqual(events[0]["severity"], "breaking")
        self.assertEqual(events[1]["type"], "stock_move")
        self.assertEqual(events[1]["badge"], "급등")
        kospi = next(event for event in events if event["label"] == "KOSPI")
        self.assertEqual(kospi["severity"], "breaking")

    def test_market_tape_omits_flat_watchlist_noise(self):
        events = market_daily.build_market_tape_events({
            "market": [],
            "moves": [
                {
                    "stock_code": "005930",
                    "stock_name": "삼성전자",
                    "change_pct": 0.3,
                    "relative_pct": 0.1,
                    "is_notable": False,
                },
            ],
        })

        self.assertEqual(events, [])

    def test_market_tape_omits_non_material_disclosure_noise(self):
        events = market_daily.build_market_tape_events({
            "disclosures": [
                {
                    "stock_code": "005930",
                    "stock_name": "\uc0bc\uc131\uc804\uc790",
                    "report_name": "\uae30\uc5c5\uc124\uba85\ud68c(IR)\uac1c\ucd5c",
                    "rcept_no": "202605210001",
                    "is_material": False,
                },
            ],
        })

        self.assertEqual(events, [])

    def test_market_tape_filters_routine_securities_disclosure(self):
        events = market_daily.build_market_tape_events({
            "disclosures": [
                {
                    "stock_code": "005940",
                    "stock_name": "NH\ud22c\uc790\uc99d\uad8c",
                    "report_name": "\uc77c\uad04\uc2e0\uace0\ucd94\uac00\uc11c\ub958(\ud30c\uc0dd\uacb0\ud569\uc99d\uad8c)",
                    "rcept_no": "202605210002",
                    "is_material": True,
                    "material_reason": "\uc720\uc0c1\uc99d\uc790",
                },
            ],
        })

        self.assertEqual(events, [])

    def test_market_tape_keeps_major_securities_disclosure(self):
        events = market_daily.build_market_tape_events({
            "disclosures": [
                {
                    "stock_code": "039490",
                    "stock_name": "\ud0a4\uc6c0\uc99d\uad8c",
                    "report_name": "\ubc30\ub2f9\uacb0\uc815",
                    "rcept_no": "202605210003",
                },
            ],
        })

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["type"], "disclosure")
        self.assertEqual(events[0]["severity"], "breaking")
        self.assertIn("\ubc30\ub2f9", events[0]["text"])

    def test_market_tape_news_uses_stock_name_instead_of_code_when_needed(self):
        events = market_daily.build_market_tape_events({
            "news": [
                {
                    "stock_code": "005930",
                    "stock_name": "\uc0bc\uc131\uc804\uc790",
                    "title": "\ubc18\ub3c4\uccb4 \ud22c\uc790 \ud655\ub300",
                    "outlet": "\uc5f0\ud569\ub274\uc2a4",
                    "published_at": "2026.05.21 10:00",
                },
            ],
        })

        self.assertEqual(events[0]["text"], "[\uc0bc\uc131\uc804\uc790] \ubc18\ub3c4\uccb4 \ud22c\uc790 \ud655\ub300 \u00b7 \uc5f0\ud569\ub274\uc2a4")
        self.assertNotIn("005930", events[0]["text"])

    def test_market_tape_news_does_not_repeat_stock_name_in_title(self):
        events = market_daily.build_market_tape_events({
            "news": [
                {
                    "stock_code": "005930",
                    "stock_name": "\uc0bc\uc131\uc804\uc790",
                    "title": "\uc0bc\uc131\uc804\uc790 \uc2e0\uc81c\ud488 \uacf5\uac1c",
                    "outlet": "\uc5f0\ud569\ub274\uc2a4",
                    "published_at": "2026.05.21 10:00",
                },
            ],
        })

        self.assertEqual(events[0]["text"], "\uc0bc\uc131\uc804\uc790 \uc2e0\uc81c\ud488 \uacf5\uac1c \u00b7 \uc5f0\ud569\ub274\uc2a4")
        self.assertNotIn("[\uc0bc\uc131\uc804\uc790]", events[0]["text"])
