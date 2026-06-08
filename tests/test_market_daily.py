import unittest
from unittest.mock import AsyncMock, patch

import pytest

import market_daily


@pytest.mark.asyncio
async def test_quote_moves_uses_portfolio_runtime_quote_boundary():
    with patch.object(
        market_daily.portfolio_quotes,
        "fetch_quote",
        new=AsyncMock(return_value={
            "price": 70000,
            "date": "2026-05-28",
            "change": 700,
            "change_pct": 1.0,
            "trade_value": 123000,
        }),
    ) as fetch_quote:
        rows = await market_daily._quote_moves(
            [{"stock_code": "AAPL", "stock_name": "Apple", "sources": ["watch"]}],
            0.2,
        )

    fetch_quote.assert_awaited_once_with("AAPL")
    self_row = rows[0]
    assert self_row["stock_code"] == "AAPL"
    assert self_row["price"] == 70000
    assert self_row["change_pct"] == 1.0


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

    def test_portfolio_summary_aggregates_only_held_positions(self):
        moves = [
            {"stock_code": "A", "stock_name": "에이", "sources": ["portfolio"],
             "quantity": 10, "price": 1000, "change": -50, "change_pct": -4.76},
            {"stock_code": "B", "stock_name": "비", "sources": ["portfolio"],
             "quantity": 5, "price": 2000, "change": 100, "change_pct": 5.26},
            {"stock_code": "C", "stock_name": "씨", "sources": ["starred"],
             "quantity": None, "price": 3000, "change": 10, "change_pct": 0.3},
        ]
        summary = market_daily._portfolio_summary(moves)
        self.assertEqual(summary["holding_count"], 2)  # starred w/o quantity excluded
        self.assertEqual(summary["up"], 1)
        self.assertEqual(summary["down"], 1)
        # mv(A)=mv(B)=10,000 → weighted avg of -4.76 and +5.26 = +0.25
        self.assertEqual(summary["weighted_day_return_pct"], 0.25)
        self.assertEqual(summary["top_detractors"][0]["stock_code"], "A")
        self.assertEqual(summary["top_detractors"][0]["day_pl"], -500)
        self.assertEqual(summary["top_contributors"][0]["stock_code"], "B")

    def test_portfolio_summary_none_without_quantified_holdings(self):
        self.assertIsNone(
            market_daily._portfolio_summary([{"stock_code": "C", "sources": ["starred"]}])
        )

    def test_parse_investor_flow_reads_signed_institution_and_foreign(self):
        html = (
            '<table class="type2">'
            '<tr><th>날짜</th><th>종가</th></tr>'
            '<tr>'
            '<td>2026.06.08</td><td>295,500</td><td>33,500</td><td>-10.18%</td>'
            '<td>38,467,019</td><td>-3,937,194</td><td>-1,174,306</td>'
            '<td>2,789,250,329</td><td>47.71%</td>'
            '</tr></table>'
        )
        flow = market_daily._parse_investor_flow(html)
        self.assertEqual(flow["date"], "2026.06.08")
        self.assertEqual(flow["institution_net"], -3937194)
        self.assertEqual(flow["foreign_net"], -1174306)

    def test_parse_investor_flow_returns_none_without_full_row(self):
        html = '<table class="type2"><tr><td>2026.06.08</td><td>1</td><td>2</td></tr></table>'
        self.assertIsNone(market_daily._parse_investor_flow(html))

    def test_map_news_item_adds_snippet_and_outlet(self):
        item = {
            "titleFull": "삼성전자 HBM4 공급 확대",
            "title": "삼성전자 HBM4",
            "officeId": "117",
            "articleId": "0004072607",
            "officeName": "테스트뉴스",
            "datetime": "202606081931",
            "body": "삼성전자가 HBM4 공급 계약을 확대했다고 밝혔다.",
            "mobileNewsUrl": "https://m.stock.naver.com/news/x",
        }
        mapped = market_daily._map_news_item(item, "005930")
        self.assertEqual(mapped["title"], "삼성전자 HBM4 공급 확대")
        self.assertEqual(mapped["outlet"], "테스트뉴스")
        self.assertEqual(mapped["published_at"], "2026.06.08 19:31")
        self.assertTrue(mapped["snippet"].startswith("삼성전자가 HBM4"))
        self.assertEqual(mapped["url"], "https://m.stock.naver.com/news/x")

    def test_map_news_item_builds_url_and_skips_empty_title(self):
        mapped = market_daily._map_news_item(
            {"title": "t", "officeId": "117", "articleId": "A", "datetime": "x"}, "005930"
        )
        self.assertIn("article_id=A", mapped["url"])
        self.assertIn("office_id=117", mapped["url"])
        self.assertIsNone(market_daily._map_news_item({"body": "x"}, "005930"))

    def test_map_news_item_truncates_long_snippet(self):
        mapped = market_daily._map_news_item({"title": "t", "body": "가" * 200}, "005930")
        self.assertTrue(mapped["snippet"].endswith("…"))
        self.assertLessEqual(len(mapped["snippet"]), 161)

    def test_flatten_news_payload_handles_flat_and_grouped(self):
        self.assertEqual(market_daily._flatten_news_payload([{"id": 1}]), [{"id": 1}])
        self.assertEqual(market_daily._flatten_news_payload([{"items": [{"id": 1}]}]), [{"id": 1}])
        self.assertEqual(market_daily._flatten_news_payload({"items": [{"id": 1}]}), [{"id": 1}])
        self.assertEqual(market_daily._flatten_news_payload(None), [])

    def test_extract_upcoming_dividend_picks_nearest_future_in_window(self):
        from datetime import date

        today = date(2026, 6, 8)
        rows = [
            {"record_date": "20260630", "per_sto_divi_amt": "361"},
            {"record_date": "20251231"},  # past
            {"record_date": "20261231"},  # beyond window
        ]
        event = market_daily._extract_upcoming_dividend(rows, today, 45)
        self.assertEqual(event, {"date": "2026-06-30", "amount": 361.0})
        # nearest wins
        nearer = market_daily._extract_upcoming_dividend(
            [{"record_date": "20260701"}, {"record_date": "20260615"}], today, 45
        )
        self.assertEqual(nearer["date"], "2026-06-15")
        # no parseable date → None (fail-safe, never fabricates)
        self.assertIsNone(market_daily._extract_upcoming_dividend([{"foo": "bar"}], today, 45))

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
