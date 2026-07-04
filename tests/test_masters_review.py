"""대가의 시선 포트폴리오 진단(masters review) 테스트.

- 자산군 휴리스틱 분류(classify_holding)와 비중/갭 산식은 순수 함수로 검증.
- generate_review 는 저장소·시세·LLM 경계를 전부 mock — 프롬프트에 철학과
  보유 비중이 실리는지, 응답 필드가 UI 계약을 지키는지 본다.
- HTTP 계층: 로그인 없으면 401, 빈 포트폴리오 400, 정상 경로 200.
"""

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from core.app_factory import create_app
from core.config import PROJECT_ROOT, AppSettings
from services import investment_masters as im
from services.portfolio import masters_review as mr


def _enriched(name: str, code: str, qty: float, price: float, currency: str = "KRW") -> dict:
    return {
        "stock_code": code,
        "stock_name": name,
        "quantity": qty,
        "currency": currency,
        "quote": {"price": price},
    }


class ClassifyTests(unittest.TestCase):
    def test_name_patterns_map_to_asset_classes(self):
        cases = [
            ("KODEX 국고채30년액티브", "439870", "bond_long"),
            ("KIWOOM 국고채10년", "148070", "bond_mid"),
            ("KODEX 단기채권", "153130", "bond_short"),
            ("ACE KRX금현물", "411060", "gold"),
            ("TIGER 리츠부동산인프라", "329200", "reits"),
            ("TIGER 원유선물Enhanced(H)", "130680", "commodities"),
            ("TIGER 미국S&P500", "360750", "equity_global"),
            ("삼성전자", "005930", "equity_kr"),
        ]
        for name, code, expected in cases:
            self.assertEqual(mr.classify_holding(name, code), expected, msg=name)

    def test_foreign_ticker_or_currency_maps_to_global_equity(self):
        self.assertEqual(mr.classify_holding("Apple Inc", "AAPL"), "equity_global")
        self.assertEqual(mr.classify_holding("어떤해외펀드", "012345", currency="USD"), "equity_global")

    def test_special_asset_codes_classify_by_code_not_name(self):
        # 실포트폴리오에서 실제로 깨졌던 케이스들 — 코드 기반 확정 분류.
        cases = [
            ("원화", "CASH_KRW", "bond_short"),
            ("미국 달러", "CASH_USD", "bond_short"),
            ("베트남 동", "CASH_VND", "bond_short"),
            ("KRX 금현물", "KRX_GOLD", "gold"),
            ("비트코인", "CRYPTO_BTC", "crypto"),
            ("이더리움", "CRYPTO_ETH", "crypto"),
        ]
        for name, code, expected in cases:
            self.assertEqual(mr.classify_holding(name, code), expected, msg=code)

    def test_extended_name_patterns_cover_real_holdings(self):
        cases = [
            ("KIWOOM 물가채KIS", "430500", "bond_mid"),
            ("Schwab US TIPS ETF", "SCHP.K", "bond_mid"),
            ("abrdn Physical Silver Shares ET", "SIVR", "commodities"),
            ("abrdn Physical Platinum Shares", "PPLT", "commodities"),
            ("ICBC CSOP FTSE Chinese Gv&Plcy Bnk Bd Idx ETF", "83199.HK", "equity_global"),
        ]
        for name, code, expected in cases:
            self.assertEqual(mr.classify_holding(name, code), expected, msg=code)


class BreakdownTests(unittest.TestCase):
    def setUp(self):
        self.enriched = [
            _enriched("삼성전자", "005930", 100, 70_000),          # 7.0M equity_kr
            _enriched("TIGER 미국S&P500", "360750", 100, 20_000),  # 2.0M equity_global
            _enriched("KODEX 단기채권", "153130", 10, 100_000),    # 1.0M bond_short
            _enriched("시세없는종목", "999999", 10, 0),             # unpriced
        ]

    def test_breakdown_weights_and_concentration(self):
        b = mr.portfolio_breakdown(self.enriched)
        self.assertEqual(b["holdings_count"], 3)
        self.assertEqual(b["total_value"], 10_000_000)
        self.assertEqual(b["holdings"][0]["weight"], 70.0)
        self.assertEqual(b["top3_weight"], 100.0)
        self.assertEqual(b["unpriced"], ["시세없는종목"])
        weights = {r["asset"]: r["weight"] for r in b["asset_weights"]}
        self.assertEqual(weights, {"equity_kr": 70.0, "equity_global": 20.0, "bond_short": 10.0})

    def test_short_position_keeps_negative_weight_and_reduces_total(self):
        enriched = [
            _enriched("삼성전자", "005930", 100, 70_000),        # +7.0M
            _enriched("미래에셋증권", "006800", -100, 20_000),   # -2.0M 숏
        ]
        b = mr.portfolio_breakdown(enriched)
        self.assertEqual(b["total_value"], 5_000_000)
        by_code = {r["code"]: r for r in b["holdings"]}
        self.assertEqual(by_code["005930"]["weight"], 140.0)
        self.assertEqual(by_code["006800"]["weight"], -40.0)

    def test_allocation_gap_against_strategy(self):
        b = mr.portfolio_breakdown(self.enriched)
        buffett = next(s for s in im.load_catalog()["strategies"] if s["id"] == "buffett")
        gap = {r["asset"]: r for r in (mr.allocation_gap(b, buffett))}
        # 버핏 예시: equity_global 90 / bond_short 10.
        self.assertEqual(gap["equity_global"]["diff"], 20.0 - 90.0)
        self.assertEqual(gap["bond_short"]["diff"], 0.0)
        self.assertEqual(gap["equity_kr"]["target"], 0.0)

    def test_prompt_carries_philosophy_holdings_and_guardrails(self):
        b = mr.portfolio_breakdown(self.enriched)
        buffett = next(s for s in im.load_catalog()["strategies"] if s["id"] == "buffett")
        prompt = mr.build_review_prompt(buffett, b, mr.allocation_gap(b, buffett))
        self.assertIn("워런 버핏", prompt)
        self.assertIn("경제적 해자", prompt)
        self.assertIn("삼성전자 (005930): 비중 70.0%", prompt)
        self.assertIn("매수/매도를 단정하지 마세요", prompt)
        self.assertIn("근사치", prompt)


class EnrichWithQuotesTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_cached_quotes_fall_back_to_bulk_then_individual_fetch(self):
        items = [
            {"stock_code": "005930", "stock_name": "삼성전자", "quantity": 10},
            {"stock_code": "430500", "stock_name": "KIWOOM 물가채KIS", "quantity": 10},
            {"stock_code": "VOO", "stock_name": "Vanguard S&P 500 ETF", "quantity": 5},
            {"stock_code": "DEAD", "stock_name": "조회실패", "quantity": 1},
        ]

        async def _cached(passed):
            out = []
            for it in passed:
                enriched = dict(it)
                # 캐시에는 국내 대형주만 있다 — 콜드 상태 재현.
                enriched["quote"] = {"price": 70_000} if it["stock_code"] == "005930" else {}
                out.append(enriched)
            return out

        async def _fetch(code):
            if code == "VOO":
                return {"price": 800_000}  # KRW 환산 시세
            # 개별 조회 취소/실패 — 배치 전체를 무너뜨리면 안 된다.
            raise asyncio.CancelledError()

        bulk_mock = AsyncMock(return_value={"430500": {"price": 100_000}})
        with patch.object(mr.quote_service, "enrich_with_cached_quotes", AsyncMock(side_effect=_cached)), \
             patch.object(mr.stock_quotes, "get_bulk_quote_snapshots", bulk_mock), \
             patch.object(mr.quote_service, "fetch_quote", AsyncMock(side_effect=_fetch)):
            enriched = await mr.enrich_with_quotes(items)

        # 국내 미싱 코드는 벌크 한 번으로 해결 — 개별 경로로 새지 않는다.
        bulk_mock.assert_awaited_once_with(["430500"])
        quotes = {it["stock_code"]: (it.get("quote") or {}).get("price") for it in enriched}
        self.assertEqual(quotes["005930"], 70_000)
        self.assertEqual(quotes["430500"], 100_000)
        self.assertEqual(quotes["VOO"], 800_000)
        self.assertIsNone(quotes["DEAD"])
        # breakdown 에서 실패 종목은 unpriced 로 분리된다.
        b = mr.portfolio_breakdown(enriched)
        self.assertEqual(b["unpriced"], ["조회실패"])
        self.assertEqual(b["holdings_count"], 3)


class GenerateReviewTests(unittest.IsolatedAsyncioTestCase):
    def _patches(self, items, llm_result=None):
        llm_result = llm_result or {
            "content": "## 총평\n- 좋은 습관입니다.",
            "model": "test/model",
            "finish_reason": "stop",
            "input_tokens": 100,
            "output_tokens": 50,
            "cost_usd": 0.001,
        }
        return (
            patch.object(mr.portfolio_repo, "get_portfolio", AsyncMock(return_value=items)),
            patch.object(mr.quote_service, "enrich_with_cached_quotes", AsyncMock(side_effect=lambda x: x)),
            patch.object(mr.quote_service, "fetch_quote", AsyncMock(return_value={})),
            patch.object(mr.ai_config, "get_model_for_feature", AsyncMock(return_value="test/model")),
            patch.object(mr.ai_client, "post_chat_completion", AsyncMock(return_value=llm_result)),
        )

    async def test_generate_review_returns_markdown_and_deterministic_context(self):
        items = [
            _enriched("삼성전자", "005930", 100, 70_000),
            _enriched("KODEX 단기채권", "153130", 30, 100_000),
        ]
        p1, p2, p3, p4, p5 = self._patches(items)
        with p1, p2, p3, p4, p5 as llm_mock:
            out = await mr.generate_review({"strategy_id": "buffett"}, {"google_sub": "u1"})
        self.assertEqual(out["strategy"]["id"], "buffett")
        self.assertIn("총평", out["markdown"])
        self.assertIn("참고용", out["disclaimer"])
        self.assertEqual(out["breakdown"]["holdings_count"], 2)
        self.assertFalse(out["truncated"])
        gap_assets = {r["asset"] for r in out["gap"]}
        self.assertIn("equity_kr", gap_assets)
        # LLM 프롬프트에 대가 이름과 사용자 비중이 실제로 실렸는지.
        sent_payload = llm_mock.call_args.kwargs["payload"]
        user_msg = sent_payload["messages"][1]["content"]
        self.assertIn("워런 버핏", user_msg)
        self.assertIn("삼성전자", user_msg)
        self.assertEqual(llm_mock.call_args.kwargs["feature"], "masters_review")

    async def test_truncated_flag_follows_finish_reason(self):
        items = [_enriched("삼성전자", "005930", 10, 70_000)]
        llm_result = {
            "content": "잘림", "model": "m", "finish_reason": "length",
            "input_tokens": 1, "output_tokens": 1, "cost_usd": 0.0,
        }
        p1, p2, p3, p4, p5 = self._patches(items, llm_result)
        with p1, p2, p3, p4, p5:
            out = await mr.generate_review({"strategy_id": "dalio"}, {"google_sub": "u1"})
        self.assertTrue(out["truncated"])

    async def test_empty_or_unpriced_portfolio_raises(self):
        for items in ([], [_enriched("시세없음", "999999", 10, 0)]):
            p1, p2, p3, p4, p5 = self._patches(items)
            with p1, p2, p3, p4, p5:
                with self.assertRaises(mr.EmptyPortfolioError):
                    await mr.generate_review({"strategy_id": "buffett"}, {"google_sub": "u1"})

    async def test_unknown_strategy_raises_masters_error(self):
        with self.assertRaises(im.MastersError):
            await mr.generate_review({"strategy_id": "nobody"}, {"google_sub": "u1"})


class ReviewApiTests(unittest.IsolatedAsyncioTestCase):
    def _settings(self) -> AppSettings:
        return AppSettings(
            environment="production",
            project_root=PROJECT_ROOT,
            app_title="Test Compass",
            public_api_base_url="https://api.example.test",
            cors_allowed_origins=("https://app.example.test",),
            enable_docs=False,
        )

    async def _post(self, body: dict, user: dict | None):
        app = create_app(self._settings())
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
        with patch("routes.masters.get_current_user", AsyncMock(return_value=user)):
            async with httpx.AsyncClient(transport=transport, base_url="https://testserver") as client:
                return await client.post("/api/masters/review", json=body)

    async def test_review_requires_login(self):
        resp = await self._post({"strategy_id": "buffett"}, user=None)
        self.assertEqual(resp.status_code, 401)

    async def test_review_happy_path_delegates_to_service(self):
        fake = {"disclaimer": "참고용", "strategy": {"id": "buffett"}, "markdown": "## ok"}
        with patch("routes.masters.masters_review.generate_review", AsyncMock(return_value=fake)):
            resp = await self._post({"strategy_id": "buffett"}, user={"google_sub": "u1"})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json()["markdown"], "## ok")

    async def test_review_maps_empty_portfolio_to_400(self):
        with patch(
            "routes.masters.masters_review.generate_review",
            AsyncMock(side_effect=mr.EmptyPortfolioError("empty")),
        ):
            resp = await self._post({"strategy_id": "buffett"}, user={"google_sub": "u1"})
        self.assertEqual(resp.status_code, 400)


if __name__ == "__main__":
    unittest.main()
