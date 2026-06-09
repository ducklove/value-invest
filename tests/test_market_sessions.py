"""Tests for the market-session clock (open_markets / US Eastern DST).

All instants are injected so the suite is deterministic and tz-database free.
2026-06-09 is a Tuesday; 2026-06-13 a Saturday.
"""

import unittest
from datetime import date, datetime

import market_sessions
from market_sessions import KST


class MarketSessionsTest(unittest.TestCase):
    def test_korea_and_japan_open_midmorning(self):
        # 평일 KST 10:00 — 한국·일본 개장, 홍콩·중국(10:30~)·미국은 닫힘.
        now = datetime(2026, 6, 9, 10, 0, tzinfo=KST)
        self.assertEqual(market_sessions.open_markets(now), {"KR", "JP"})

    def test_all_asia_open_late_morning(self):
        # 평일 KST 11:00 — 한·일·홍콩·중국 모두 개장.
        now = datetime(2026, 6, 9, 11, 0, tzinfo=KST)
        self.assertEqual(market_sessions.open_markets(now), {"KR", "JP", "HK", "CN"})

    def test_us_open_overnight_kst_winter(self):
        # 겨울(EST): 미국 09:30 = KST 23:30. 평일 KST 23:45 → 미국만.
        now = datetime(2026, 1, 6, 23, 45, tzinfo=KST)
        self.assertEqual(market_sessions.open_markets(now), {"US"})

    def test_us_open_overnight_kst_summer(self):
        # 여름(EDT): 미국 09:30 = KST 22:30. 평일 KST 22:45 → 미국만.
        now = datetime(2026, 7, 7, 22, 45, tzinfo=KST)
        self.assertEqual(market_sessions.open_markets(now), {"US"})

    def test_weekend_all_closed(self):
        now = datetime(2026, 6, 13, 11, 0, tzinfo=KST)  # 토요일
        self.assertEqual(market_sessions.open_markets(now), set())

    def test_predawn_weekday_all_closed(self):
        # 평일 KST 07:00 — 아시아 개장 전, 미국은 이미 마감.
        now = datetime(2026, 6, 9, 7, 0, tzinfo=KST)
        self.assertEqual(market_sessions.open_markets(now), set())

    def test_us_dst_boundaries(self):
        # 2026: 2nd Sun Mar = 03-08, 1st Sun Nov = 11-01.
        self.assertFalse(market_sessions.us_eastern_is_dst(date(2026, 3, 7)))
        self.assertTrue(market_sessions.us_eastern_is_dst(date(2026, 3, 8)))
        self.assertTrue(market_sessions.us_eastern_is_dst(date(2026, 10, 31)))
        self.assertFalse(market_sessions.us_eastern_is_dst(date(2026, 11, 1)))


if __name__ == "__main__":
    unittest.main()
