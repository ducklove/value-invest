import unittest
from unittest.mock import AsyncMock, patch

import external_tools
from routes import stocks as stocks_route


class ExternalSummaryTests(unittest.TestCase):
    def test_code_helper(self):
        self.assertEqual(external_tools._code("005930.KS"), "005930")
        self.assertEqual(external_tools._code("000670.KQ"), "000670")
        self.assertEqual(external_tools._code(""), "")

    def test_summarize_holding_sorts_by_ratio_and_joins_name_code(self):
        current = {
            "summary": {"averageRatio": 215.6, "pairCount": 2},
            "lastUpdated": "2026-05-30 10:32:01",
            "pairs": [
                {"id": "a", "ratio": 120.0, "ratioChange": 1.0},
                {"id": "b", "ratio": 781.87, "ratioChange": 3.05},
                {"id": "c"},  # ratio 없음 → 제외
            ],
        }
        config = [
            {"id": "a", "name": "A지주", "holdingTicker": "111111.KS"},
            {"id": "b", "name": "영풍→고려아연", "holdingTicker": "000670.KS"},
        ]
        out = external_tools._summarize_holding(current, config)
        self.assertEqual(out["averageRatio"], 215.6)
        self.assertEqual([r["name"] for r in out["top"]], ["영풍→고려아연", "A지주"])
        self.assertEqual(out["top"][0]["code"], "000670")
        self.assertEqual(out["url"], external_tools.SITE["holding"])

    def test_summarize_spread_keeps_only_config_pairs(self):
        current = {
            "averageSpread": 48.28,
            "averageSpreadChange": 0.8,
            "prices": {
                "samsung_elec": {"spread": 36.12, "spreadChange": -0.14},
                "doosan_fc": {"spread": 88.8, "spreadChange": 0.22},
                "doosan_fc_pref": {"spread": 88.3},  # config에 없음 → 제외
            },
        }
        config = [
            {"id": "samsung_elec", "name": "삼성전자", "commonTicker": "005930.KS"},
            {"id": "doosan_fc", "name": "두산퓨얼셀", "commonTicker": "336260.KS"},
        ]
        out = external_tools._summarize_spread(current, config)
        names = [r["name"] for r in out["top"]]
        self.assertEqual(names, ["두산퓨얼셀", "삼성전자"])  # spread 내림차순
        self.assertNotIn("doosan_fc_pref", [r.get("code") for r in out["top"]])
        self.assertEqual(out["top"][0]["code"], "336260")

    def test_summarize_gold_latest_gap_and_links(self):
        data = {
            "updated_at": "2026-05-30 16:34 KST",
            "gold": {"dates": ["2026-05-30", "2026-05-31"], "gap_pct": [-3.0, -2.81]},
            "bitcoin": {"dates": ["2026-05-31"], "gap_pct": [-1.63]},
            "usdt": {"gap_pct": []},  # 빈 → 제외
        }
        out = external_tools._summarize_gold(data)
        keys = [a["key"] for a in out["assets"]]
        self.assertEqual(keys, ["gold", "bitcoin"])
        gold = out["assets"][0]
        self.assertEqual(gold["gap"], -2.81)  # 최신값
        self.assertEqual(gold["date"], "2026-05-31")
        self.assertIn("asset=gold", gold["link"])


class ExternalEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_insights_endpoint_passthrough(self):
        payload = {"holding": {"top": []}, "spread": {"top": []}, "goldGap": {"assets": []}}
        fake = AsyncMock(return_value=payload)
        with patch.object(external_tools, "fetch_external_insights", new=fake):
            result = await stocks_route.external_insights()
        self.assertEqual(result, payload)

    async def test_insights_partial_failure_keeps_others(self):
        # 한 도구 fetch가 실패해도 나머지는 살아남는다.
        external_tools._cache.clear()
        with patch.object(external_tools, "_holding_summary", new=AsyncMock(side_effect=RuntimeError("boom"))), \
             patch.object(external_tools, "_spread_summary", new=AsyncMock(return_value={"top": [], "url": "u"})), \
             patch.object(external_tools, "_gold_summary", new=AsyncMock(return_value={"assets": [], "url": "u"})):
            out = await external_tools.fetch_external_insights()
        self.assertNotIn("holding", out)
        self.assertIn("spread", out)
        self.assertIn("goldGap", out)


if __name__ == "__main__":
    unittest.main()
