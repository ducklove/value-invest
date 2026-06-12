import unittest
from unittest.mock import AsyncMock, patch

import pytest

import market_daily


def _llm_env_patches(post_mock):
    """_call_openrouter 호출에 필요한 외부 의존(모델 설정·키·LLM)을 한 번에 패치."""
    return (
        patch.object(market_daily.ai_config, "get_model_for_feature", new=AsyncMock(return_value="test-model")),
        patch.object(market_daily.ai_config, "openrouter_reasoning_controls", return_value={}),
        patch.object(market_daily.ai_client, "require_openrouter_key", new=AsyncMock()),
        patch.object(market_daily.ai_client, "post_chat_completion", new=post_mock),
    )


@pytest.mark.asyncio
async def test_call_openrouter_retries_transient_failure_then_succeeds():
    ok = {"content": "## 시황", "model": "test-model", "input_tokens": 10, "output_tokens": 20, "cost_usd": 0.001}
    post = AsyncMock(side_effect=[RuntimeError("timeout"), ok])
    p1, p2, p3, p4 = _llm_env_patches(post)
    with p1, p2, p3, p4:
        result = await market_daily._call_openrouter({"market": [], "movers": [], "market_news": []}, None)
    assert result["llm_ok"] is True
    assert result["markdown"] == "## 시황"
    assert post.await_count == 2


@pytest.mark.asyncio
async def test_call_openrouter_falls_back_after_all_attempts():
    payload = {
        "market": [{"code": "KOSPI", "label": "KOSPI", "value": "8,414.04", "change_pct": 8.37}],
        "movers": [],
        "market_news": [],
    }
    post = AsyncMock(side_effect=RuntimeError("down"))
    p1, p2, p3, p4 = _llm_env_patches(post)
    with p1, p2, p3, p4:
        result = await market_daily._call_openrouter(payload, None)
    assert result["llm_ok"] is False
    assert "KOSPI" in result["markdown"]  # fallback 은 수집 근거를 보여준다
    assert post.await_count == market_daily.MARKET_DAILY_LLM_ATTEMPTS


@pytest.mark.asyncio
async def test_call_openrouter_retries_empty_content():
    ok = {"content": "## 본문", "model": "test-model", "input_tokens": 1, "output_tokens": 1, "cost_usd": 0.0}
    post = AsyncMock(side_effect=[{**ok, "content": ""}, ok])
    p1, p2, p3, p4 = _llm_env_patches(post)
    with p1, p2, p3, p4:
        result = await market_daily._call_openrouter({"market": [], "movers": [], "market_news": []}, None)
    assert result["llm_ok"] is True
    assert result["markdown"] == "## 본문"


@pytest.mark.asyncio
async def test_build_daily_market_brief_is_market_wide_without_portfolio():
    with patch.object(market_daily, "_market_snapshot", new=AsyncMock(
            return_value=([{"code": "KOSPI", "label": "KOSPI", "value": "8,000", "change_pct": 1.0}], 1.0))), \
         patch.object(market_daily, "_tape_movers", new=AsyncMock(return_value=[
             {"stock_code": "900110", "stock_name": "이슈", "price": 1000,
              "change_pct": 12.0, "direction": "up", "bucket": "급등", "market": "kosdaq"},
             {"stock_code": "005930", "stock_name": "삼성전자", "price": 80000,
              "change_pct": 1.0, "direction": "up", "bucket": "시총", "market": "kospi"},
         ])), \
         patch.object(market_daily.market_movers, "fetch_sectors", new=AsyncMock(
             return_value=[{"name": "반도체", "change_pct": "+3.00%", "direction": "up"}])), \
         patch.object(market_daily.market_movers, "fetch_investor_flows", new=AsyncMock(
             return_value={"kospi": {"date": "26.06.12"}})), \
         patch.object(market_daily.market_news, "fetch_market_news", new=AsyncMock(
             return_value=[{"title": "코스피 급등", "source": "테스트"}])), \
         patch.object(market_daily, "_fetch_dart_disclosures", new=AsyncMock(return_value=([], []))) as disc, \
         patch.object(market_daily, "_news_for_focus_codes", new=AsyncMock(return_value=[])), \
         patch.object(market_daily, "_investor_flows_for_codes", new=AsyncMock(return_value=[])), \
         patch.object(market_daily, "_call_openrouter", new=AsyncMock(return_value={
             "markdown": "# x", "model": "m", "tokens_in": 1, "tokens_out": 1,
             "cost_usd": 0.0, "llm_ok": True, "error": None})):
        brief = await market_daily.build_daily_market_brief(google_sub="user-1")

    payload = brief["payload"]
    # 포트폴리오/관심종목 흔적이 없어야 한다 — 시장 전체 기준 공용 브리프.
    assert "portfolio_summary" not in payload
    assert "moves" not in payload
    assert "interest_count" not in payload
    assert "upcoming_events" not in payload
    assert payload["scope"] == "market"
    assert payload["movers"][0]["stock_code"] == "900110"
    assert payload["sectors"][0]["name"] == "반도체"
    assert payload["market_investor_flows"]["kospi"]["date"] == "26.06.12"
    assert payload["market_news"][0]["title"] == "코스피 급등"
    # 공시·뉴스 시드는 이슈 종목(급등락)만 — 시총상위(005930)는 제외.
    seeded = disc.await_args.args[0]
    assert [s["stock_code"] for s in seeded] == ["900110"]


@pytest.mark.asyncio
async def test_build_daily_market_brief_survives_source_failures():
    with patch.object(market_daily, "_market_snapshot", new=AsyncMock(side_effect=RuntimeError("boom"))), \
         patch.object(market_daily, "_tape_movers", new=AsyncMock(return_value=[])), \
         patch.object(market_daily.market_movers, "fetch_sectors", new=AsyncMock(side_effect=RuntimeError("boom"))), \
         patch.object(market_daily.market_movers, "fetch_investor_flows", new=AsyncMock(return_value={})), \
         patch.object(market_daily.market_news, "fetch_market_news", new=AsyncMock(return_value=[])), \
         patch.object(market_daily, "_call_openrouter", new=AsyncMock(return_value={
             "markdown": "# x", "model": "m", "tokens_in": 1, "tokens_out": 1,
             "cost_usd": 0.0, "llm_ok": True, "error": None})):
        brief = await market_daily.build_daily_market_brief()

    payload = brief["payload"]
    assert payload["market"] == []
    assert payload["sectors"] == []
    assert any("market" in w for w in payload["source_warnings"])


@pytest.mark.asyncio
async def test_build_daily_market_brief_times_out_hung_source():
    import asyncio

    async def _hang(*_args, **_kwargs):
        await asyncio.sleep(60)

    with patch.object(market_daily, "BRIEF_SOURCE_TIMEOUT_S", 0.05), \
         patch.object(market_daily, "_market_snapshot", new=_hang), \
         patch.object(market_daily, "_tape_movers", new=AsyncMock(return_value=[])), \
         patch.object(market_daily.market_movers, "fetch_sectors", new=AsyncMock(return_value=[])), \
         patch.object(market_daily.market_movers, "fetch_investor_flows", new=AsyncMock(return_value={})), \
         patch.object(market_daily.market_news, "fetch_market_news", new=AsyncMock(return_value=[])), \
         patch.object(market_daily, "_call_openrouter", new=AsyncMock(return_value={
             "markdown": "# x", "model": "m", "tokens_in": 1, "tokens_out": 1,
             "cost_usd": 0.0, "llm_ok": True, "error": None})):
        brief = await asyncio.wait_for(market_daily.build_daily_market_brief(), timeout=5)

    # 매달린 소스는 타임아웃으로 비우고 나머지로 브리프를 만든다(전체 hang 금지).
    assert brief["payload"]["market"] == []
    assert any("market" in w for w in brief["payload"]["source_warnings"])


class MarketDailyRuleTests(unittest.TestCase):
    def test_build_prompt_is_market_wide(self):
        prompt = market_daily._build_prompt({
            "market": [], "movers": [], "sectors": [], "market_investor_flows": {},
            "market_news": [], "news": [], "disclosures": [], "investor_flows": [],
        })
        # 시장 전체 관점 + 포트폴리오 관점 금지 문구.
        self.assertIn("시장 전체", prompt)
        self.assertIn("포트폴리오·보유종목·관심종목 관점은 절대 넣지 마세요", prompt)
        # 옛 포트폴리오 evidence 키가 프롬프트 지침에 남아 있으면 안 된다.
        self.assertNotIn("portfolio_summary", prompt)
        self.assertNotIn("weighted_day_return_pct", prompt)
        self.assertNotIn("upcoming_events", prompt)

    def test_fallback_markdown_is_market_wide(self):
        payload = {
            "market": [{"code": "KOSPI", "label": "KOSPI", "value": "8,414.04", "change_pct": 8.37}],
            "movers": [
                {"stock_code": "1", "stock_name": "상한이슈", "change_pct": 29.9, "bucket": "상한가"},
                {"stock_code": "2", "stock_name": "시총만", "change_pct": 0.5, "bucket": "시총"},
            ],
            "market_news": [{"title": "코스피 급등 마감", "source": "테스트뉴스"}],
        }
        md = market_daily._fallback_markdown(payload, "메시지")
        self.assertIn("+8.37%", md)
        self.assertIn("상한이슈", md)
        self.assertNotIn("시총만", md)  # 이슈(급등락·상하한가)만 노출
        self.assertIn("코스피 급등 마감", md)

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

    def test_market_tape_excludes_kospi200(self):
        events = market_daily.build_market_tape_events({
            "market": [
                {"code": "KOSPI", "label": "KOSPI", "value": "2,700.00", "change_pct": -2.3},
                {"code": "KOSPI200", "label": "KOSPI200", "value": "360.00", "change_pct": -2.1},
            ],
        })

        labels = [event["label"] for event in events]
        self.assertIn("KOSPI", labels)
        self.assertNotIn("KOSPI200", labels)

    def test_tape_index_codes_features_open_markets_only(self):
        # 전 시장 마감: 홈 지수(KOSPI/KOSDAQ) + 24h 매크로만.
        self.assertEqual(
            market_daily._tape_index_codes(set()),
            ["KOSPI", "KOSDAQ", "USD_KRW", "OIL_CL"],
        )
        # 미국장 열림: 미국 지수 추가.
        self.assertEqual(
            market_daily._tape_index_codes({"US"}),
            ["KOSPI", "KOSDAQ", "SPX", "IXIC", "DJI", "USD_KRW", "OIL_CL"],
        )
        # 아시아장 열림: 닛케이·항셍·상해 추가.
        self.assertEqual(
            market_daily._tape_index_codes({"JP", "HK", "CN"}),
            ["KOSPI", "KOSDAQ", "NI225", "HSI", "SHC", "USD_KRW", "OIL_CL"],
        )

    def test_mover_bucket_classifies_limit_and_moves(self):
        self.assertEqual(market_daily._mover_bucket("rising", 29.9), "상한가")
        self.assertEqual(market_daily._mover_bucket("rising", 7.2), "급등")
        self.assertEqual(market_daily._mover_bucket("falling", -30.0), "하한가")
        self.assertEqual(market_daily._mover_bucket("falling", -6.0), "급락")
        # 시총상위는 변동폭과 무관하게 항상 유지.
        self.assertEqual(market_daily._mover_bucket("market_cap", 0.3), "시총")
        # ±5% 미만 급상승/급하락은 노이즈 → 버림(None).
        self.assertIsNone(market_daily._mover_bucket("rising", 1.2))
        self.assertIsNone(market_daily._mover_bucket("falling", -3.0))
        self.assertIsNone(market_daily._mover_bucket("rising", None))

    def test_market_tape_movers_become_stock_events(self):
        events = market_daily.build_market_tape_events({
            "movers": [
                {"stock_code": "005930", "stock_name": "삼성전자", "price": 80000,
                 "change_pct": 1.2, "direction": "up", "bucket": "시총"},
                {"stock_code": "900110", "stock_name": "이슈상한", "price": 13000,
                 "change_pct": 29.9, "direction": "up", "bucket": "상한가"},
            ],
        })

        # 상한가(breaking)가 시총(watch)보다 먼저.
        self.assertEqual(events[0]["badge"], "상한가")
        self.assertEqual(events[0]["severity"], "breaking")
        self.assertEqual(events[0]["type"], "stock_move")
        self.assertEqual(events[0]["stock_code"], "900110")
        cap = next(e for e in events if e["badge"] == "시총")
        self.assertEqual(cap["severity"], "watch")
        self.assertIn("삼성전자", cap["text"])

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

    def test_market_tape_news_drops_stock_name_prefix(self):
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

        # \ub274\uc2a4 \uc81c\ubaa9 \uc55e [\uc885\ubaa9\uba85] \uba38\ub9ac\ud45c\ub294 \uc81c\uac70 \u2014 \uc81c\ubaa9+\ucd9c\ucc98\ub9cc.
        self.assertEqual(events[0]["text"], "\ubc18\ub3c4\uccb4 \ud22c\uc790 \ud655\ub300 \u00b7 \uc5f0\ud569\ub274\uc2a4")
        self.assertNotIn("[\uc0bc\uc131\uc804\uc790]", events[0]["text"])
        self.assertNotIn("005930", events[0]["text"])
        # \uc885\ubaa9 \uc815\ubcf4 \uc790\uccb4\ub294 \uba54\ud0c0\ub85c \uc720\uc9c0(\ud074\ub9ad \uc2dc \uc885\ubaa9 \uc774\ub3d9).
        self.assertEqual(events[0]["stock_code"], "005930")

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


class DailyBriefRouteTests(unittest.IsolatedAsyncioTestCase):
    """브리프 라우트: 공용 캐시 키 + 실패한 생성은 캐시에 저장하지 않는다."""

    GEN_OK = {
        "brief_date": "2026-06-12", "source_hash": "h", "payload": {"scope": "market"},
        "markdown": "# 시황", "model": "m", "tokens_in": 1, "tokens_out": 2,
        "cost_usd": 0.0, "llm_ok": True, "error": None,
    }

    async def test_failed_generation_is_not_cached(self):
        from routes import market_daily as route

        gen = {**self.GEN_OK, "markdown": "fallback", "llm_ok": False, "error": "timeout"}
        with patch.object(route, "get_current_user", new=AsyncMock(return_value=None)), \
             patch.object(route.market_brief_repo, "get_daily_market_brief", new=AsyncMock(return_value=None)), \
             patch.object(route.market_brief_repo, "save_daily_market_brief", new=AsyncMock()) as save, \
             patch.object(route.market_daily, "build_daily_market_brief", new=AsyncMock(return_value=gen)):
            result = await route.get_daily_market_brief(request=None, refresh=False)
        save.assert_not_awaited()  # 실패 본문이 30분 캐시를 점유하면 안 된다
        self.assertFalse(result["llm_ok"])
        self.assertEqual(result["markdown"], "fallback")
        self.assertEqual(result["error"], "timeout")

    async def test_brief_is_shared_under_public_key(self):
        from routes import market_daily as route

        user = {"google_sub": "user-123"}
        saved = {"markdown": "# 시황", "payload": {}}
        with patch.object(route, "get_current_user", new=AsyncMock(return_value=user)), \
             patch.object(route.market_brief_repo, "get_daily_market_brief", new=AsyncMock(return_value=None)) as get, \
             patch.object(route.market_brief_repo, "save_daily_market_brief", new=AsyncMock(return_value=saved)) as save, \
             patch.object(route.market_daily, "build_daily_market_brief", new=AsyncMock(return_value=dict(self.GEN_OK))) as build:
            result = await route.get_daily_market_brief(request=None, refresh=False)
        # 캐시 조회·저장 모두 사용자와 무관한 public 키.
        self.assertEqual(get.await_args.args[0], "public")
        self.assertEqual(save.await_args.kwargs["google_sub"], "public")
        # 생성(비용 귀속)에는 요청 사용자를 넘긴다.
        self.assertEqual(build.await_args.kwargs["google_sub"], "user-123")
        self.assertTrue(result["llm_ok"])

    async def test_cached_brief_short_circuits(self):
        from routes import market_daily as route

        cached = {"markdown": "# 캐시", "payload": {}}
        with patch.object(route, "get_current_user", new=AsyncMock(return_value=None)), \
             patch.object(route.market_brief_repo, "get_daily_market_brief", new=AsyncMock(return_value=cached)), \
             patch.object(route.market_daily, "build_daily_market_brief", new=AsyncMock()) as build:
            result = await route.get_daily_market_brief(request=None, refresh=False)
        build.assert_not_awaited()
        self.assertTrue(result["cached"])
