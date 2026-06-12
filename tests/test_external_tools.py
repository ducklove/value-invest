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

    def test_summarize_nps_sorts_by_weight(self):
        current = {
            "lastUpdated": "2026-06-05 22:07:00",
            "asOf": "2026-06-05",
            "summary": {"totalValue": 476000000, "nav": 2040.52, "count": 2, "todayPct": -5.51},
            "holdings": [
                {"stock_code": "000660", "stock_name": "SK하이닉스", "weight": 8.2, "market_value": 100, "change_pct": 1.1},
                {"stock_code": "005930", "stock_name": "삼성전자", "weight": 31.7, "market_value": 200, "change_pct": -6.4},
                {"stock_code": "035720", "stock_name": "카카오"},  # weight 없음 → 뒤로
            ],
        }
        out = external_tools._summarize_nps(current)
        self.assertEqual(out["nav"], 2040.52)
        self.assertEqual(out["count"], 2)
        # weight 내림차순, 값 없는 항목은 뒤로.
        self.assertEqual([r["code"] for r in out["top"]], ["005930", "000660", "035720"])
        self.assertEqual(out["top"][0]["name"], "삼성전자")
        self.assertEqual(out["url"], external_tools.SITE["nps"])

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

    def test_summarize_spac_sorts_by_current_price(self):
        current = {
            "lastUpdated": "2026-06-01 14:58:47 KST",
            "summary": {"averageAnnualizedReturn": 1.69, "belowIpoCount": 19, "totalCount": 73},
            "prices": {
                "474660": {"name": "신한제12호스팩", "currentPrice": 2010, "ipoPrice": 2000, "annualizedReturn": 6.05, "ratio": 1.025},
                "0131D0": {"name": "키움히어로제2호스팩", "currentPrice": 1975, "ipoPrice": 2000, "annualizedReturn": 3.1, "ratio": 0.9875},
                "0072Z0": {"name": "KB제33호스팩", "ratio": 0.9945},  # 현재가 없음 → 제외
            },
        }
        out = external_tools._summarize_spac(current)
        # 현재가 낮은 순: 키움(1975) < 신한(2010)
        self.assertEqual([r["name"] for r in out["top"]], ["키움히어로제2호스팩", "신한제12호스팩"])
        self.assertEqual(out["top"][0]["code"], "0131D0")
        self.assertEqual(out["top"][0]["currentPrice"], 1975)
        self.assertEqual(out["averageAnnualizedReturn"], 1.69)
        self.assertEqual(out["belowIpoCount"], 19)
        self.assertEqual(out["url"], external_tools.SITE["spac"])

    def test_summarize_spread_dedupes_multi_preferred_by_max_spread(self):
        # 같은 보통주(336260)에 우선주 2개 → 괴리율 큰 것 하나만 노출.
        current = {
            "prices": {
                "doosan_fc_1": {"spread": 87.0, "spreadChange": 0.1},
                "doosan_fc_2": {"spread": 89.0, "spreadChange": 0.2},
            },
        }
        config = [
            {"id": "doosan_fc_1", "name": "두산퓨얼셀/1우", "commonTicker": "336260.KS", "preferredTicker": "33626K.KS"},
            {"id": "doosan_fc_2", "name": "두산퓨얼셀/2우B", "commonTicker": "336260.KS", "preferredTicker": "33626L.KS"},
        ]
        out = external_tools._summarize_spread(current, config)
        self.assertEqual(len(out["top"]), 1)
        self.assertEqual(out["top"][0]["name"], "두산퓨얼셀/2우B")
        self.assertEqual(out["top"][0]["spread"], 89.0)
        self.assertEqual(out["top"][0]["code"], "336260")

    def test_summarize_gold_latest_gap_and_links(self):
        data = {
            "updated_at": "2026-05-30 16:34 KST",
            "gold": {"dates": ["2026-05-30", "2026-05-31"], "gap_pct": [-3.0, -2.81]},
            "bitcoin": {"dates": ["2026-05-31"], "gap_pct": [-1.63]},
            "eth": {"dates": ["2026-05-31"], "gap_pct": [-0.99]},
            "usdt": {"gap_pct": []},  # 빈 → 제외
        }
        out = external_tools._summarize_gold(data)
        keys = [a["key"] for a in out["assets"]]
        self.assertEqual(keys, ["gold", "bitcoin", "eth"])
        gold = out["assets"][0]
        self.assertEqual(gold["gap"], -2.81)  # 최신값
        self.assertEqual(gold["date"], "2026-05-31")
        self.assertIn("asset=gold", gold["link"])
        eth = out["assets"][2]
        self.assertEqual(eth["label"], "이더리움")
        self.assertIn("asset=eth", eth["link"])

    def test_summarize_etf_picks_daily_seed_stable_and_rank_sorted(self):
        data = {
            "count": 100,
            "generatedAt": "2026-06-11T23:03:12.120Z",
            "etfs": [
                {"rank": i, "ticker": f"T{i:03d}", "shortName": f"ETF{i}",
                 "market": "국내", "aiynScore": 100 - i,
                 "link": f"https://ducklove.github.io/eiayn/?code=T{i:03d}"}
                for i in range(1, 101)
            ],
        }
        out1 = external_tools._summarize_etf_picks(data, "2026-06-12")
        out2 = external_tools._summarize_etf_picks(data, "2026-06-12")
        # 같은 날짜 시드 → 항상 같은 5개 (캐시 만료 후 재계산에도 불변).
        self.assertEqual(len(out1["top"]), 5)
        self.assertEqual([r["code"] for r in out1["top"]], [r["code"] for r in out2["top"]])
        # 노출은 rank 오름차순.
        ranks = [r["rank"] for r in out1["top"]]
        self.assertEqual(ranks, sorted(ranks))
        self.assertEqual(out1["universe"], 100)
        self.assertEqual(out1["url"], external_tools.SITE["etf"])
        # 다른 날짜 시드 → 다른 조합 (시드 고정이라 이 비교 결과도 결정적).
        out3 = external_tools._summarize_etf_picks(data, "2026-06-13")
        self.assertNotEqual([r["code"] for r in out1["top"]], [r["code"] for r in out3["top"]])

    def test_summarize_etf_picks_small_universe(self):
        data = {"etfs": [{"rank": 1, "ticker": "A", "shortName": "A", "aiynScore": 9}]}
        out = external_tools._summarize_etf_picks(data, "2026-06-12")
        self.assertEqual(len(out["top"]), 1)
        self.assertEqual(external_tools._summarize_etf_picks({}, "2026-06-12")["top"], [])


class StockLinkMatchTests(unittest.TestCase):
    SPREAD_CUR = {"prices": {"samsung_elec": {"spread": 36.12, "spreadChange": -0.14,
                                              "commonPrice": 317000, "preferredPrice": 202500}}}
    SPREAD_CFG = [{"id": "samsung_elec", "name": "삼성전자", "commonTicker": "005930.KS",
                   "preferredTicker": "005935.KS", "preferredName": "삼성전자우"}]
    HOLD_CUR = {"pairs": [{"id": "yp", "ratio": 781.87, "ratioChange": 3.05,
                           "holdingValue": 72253.4, "marketCap": 9241.1}]}
    HOLD_CFG = [{"id": "yp", "name": "영풍→고려아연", "holdingTicker": "000670.KS"}]

    def test_match_preferred_by_common_or_preferred_code(self):
        for code in ("005930", "005935"):  # 보통주/우선주 코드 둘 다 매칭
            m = external_tools._match_preferred(code, self.SPREAD_CUR, self.SPREAD_CFG)
            self.assertIsNotNone(m)
            self.assertEqual(m["spread"], 36.12)
            self.assertEqual(m["preferredName"], "삼성전자우")
        self.assertIsNone(external_tools._match_preferred("035720", self.SPREAD_CUR, self.SPREAD_CFG))

    def test_match_holding_with_code_deeplink(self):
        m = external_tools._match_holding("000670", self.HOLD_CUR, self.HOLD_CFG)
        self.assertIsNotNone(m)
        self.assertEqual(m["ratio"], 781.87)
        self.assertIn("?code=000670", m["url"])
        self.assertIsNone(external_tools._match_holding("999999", self.HOLD_CUR, self.HOLD_CFG))


class StockLinkFetchTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_stock_links_combines_tools_and_etf(self):
        external_tools._raw_cache.clear()

        async def fake_load(repo):
            if repo == "common_preferred_spread":
                return StockLinkMatchTests.SPREAD_CUR, StockLinkMatchTests.SPREAD_CFG
            return StockLinkMatchTests.HOLD_CUR, StockLinkMatchTests.HOLD_CFG

        # ETF universe 는 eiayn 원격 파일이므로 네트워크 없이 고정 집합으로 패치.
        with patch.object(external_tools, "_load_pair", new=AsyncMock(side_effect=fake_load)), \
             patch.object(external_tools, "fetch_etf_universe", new=AsyncMock(return_value={"069500", "VOO"})):
            pref = await external_tools.fetch_stock_links("005930")
            self.assertIn("preferred", pref)
            self.assertNotIn("holding", pref)
            self.assertNotIn("etf", pref)
            hold = await external_tools.fetch_stock_links("000670")
            self.assertIn("holding", hold)
            # 국내 ETF 코드 → etf 링크만
            kr_etf = await external_tools.fetch_stock_links("069500")
            self.assertIn("etf", kr_etf)
            self.assertEqual(kr_etf["etf"]["url"], "https://ducklove.github.io/eiayn/?code=069500")
            # 해외 ETF 티커(소문자) → 정규화 후 매칭
            us_etf = await external_tools.fetch_stock_links("voo")
            self.assertEqual(us_etf["etf"]["code"], "VOO")
            empty = await external_tools.fetch_stock_links("035720")
            self.assertEqual(empty, {})
            self.assertEqual(await external_tools.fetch_stock_links(""), {})


class EtfLinkTests(unittest.IsolatedAsyncioTestCase):
    async def test_etf_link_for_matches_and_normalizes(self):
        with patch.object(external_tools, "fetch_etf_universe", new=AsyncMock(return_value={"069500", "VOO"})):
            self.assertEqual(
                (await external_tools.etf_link_for("069500"))["url"],
                "https://ducklove.github.io/eiayn/?code=069500",
            )
            self.assertEqual((await external_tools.etf_link_for("voo"))["code"], "VOO")
            # 거래소 접미사가 붙어도 앞부분으로 매칭
            self.assertEqual((await external_tools.etf_link_for("VOO.US"))["code"], "VOO")
            self.assertIsNone(await external_tools.etf_link_for("005930"))
            self.assertIsNone(await external_tools.etf_link_for(""))

    async def test_etf_link_for_empty_universe(self):
        with patch.object(external_tools, "fetch_etf_universe", new=AsyncMock(return_value=set())):
            self.assertIsNone(await external_tools.etf_link_for("VOO"))


class ExternalEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_insights_endpoint_passthrough(self):
        payload = {"holding": {"top": []}, "spread": {"top": []}, "goldGap": {"assets": []}}
        fake = AsyncMock(return_value=payload)
        with patch.object(external_tools, "fetch_external_insights", new=fake):
            result = await stocks_route.external_insights()
        self.assertEqual(result, payload)

    async def test_stock_links_endpoint_passthrough(self):
        fake = AsyncMock(return_value={"preferred": {"spread": 36.12}})
        with patch.object(external_tools, "fetch_stock_links", new=fake):
            result = await stocks_route.external_stock_links("005930")
        self.assertEqual(result["preferred"]["spread"], 36.12)
        self.assertEqual(fake.await_args.args[0], "005930")

    async def test_insights_partial_failure_keeps_others(self):
        # 한 도구 fetch가 실패해도 나머지는 살아남는다.
        external_tools._cache.clear()
        with patch.object(external_tools, "_holding_summary", new=AsyncMock(side_effect=RuntimeError("boom"))), \
             patch.object(external_tools, "_spread_summary", new=AsyncMock(return_value={"top": [], "url": "u"})), \
             patch.object(external_tools, "_gold_summary", new=AsyncMock(return_value={"assets": [], "url": "u"})), \
             patch.object(external_tools, "_spac_summary", new=AsyncMock(return_value={"top": [], "url": "u"})), \
             patch.object(external_tools, "_nps_summary", new=AsyncMock(return_value={"top": [], "url": "u"})), \
             patch.object(external_tools, "_etf_picks_summary", new=AsyncMock(return_value={"top": [], "url": "u"})):
            out = await external_tools.fetch_external_insights()
        self.assertNotIn("holding", out)
        self.assertIn("spread", out)
        self.assertIn("goldGap", out)
        self.assertIn("spac", out)
        self.assertIn("nps", out)
        self.assertIn("etfPicks", out)


if __name__ == "__main__":
    unittest.main()
