"""market_data 연간 캐시 무결성 테스트.

2026-08 사고 회귀 테스트: 포트폴리오 valuation 캐시 경로가 "최신 연도"
close_price 를 회계연도 행에 upsert 해 행 내부(per×eps vs close)가 모순인
오염 행이 생겼고, 종목분석 카드 PBR 이 0.5 로 왜곡됐다(현대차 실측 0.91).

- ``stock_price._market_row_is_inconsistent`` / ``market_data_needs_refresh``:
  오염 행을 감지해 전체 재계산을 트리거(캐시 자가치유).
- ``portfolio_repo.upsert_market_target_metrics``: close_price 를 받지 않고,
  기존값 우선(빈 칸 채우기)이라 fetch_market_data 가 쓴 행을 훼손하지 않는다.
- ``valuation._persist_market_target_metrics``: closePrice 를 repo 로 넘기지
  않는다.
"""

from datetime import datetime
from unittest.mock import AsyncMock, patch

from _harness import TempDbMixin

import stock_price
from repositories import financial as financial_repo
from repositories import portfolio as portfolio_repo
from services.portfolio import valuation


def _consistent_rows():
    current_year = datetime.now().year
    return [
        {
            "year": 1989,
            "close_price": 1000.0,
            "per": 10.0,
            "eps": 100.0,
            "pbr": 1.0,
            "bps": 1000.0,
            "dividend_per_share": 10.0,
            "dividend_yield": 1.0,
            "market_cap": 1.0,
        },
        {
            "year": current_year - 1,
            "close_price": 296500.0,
            "per": 8.36,
            "eps": 35464.0,   # 8.36 × 35,464 = 296,479 ≈ close
            "pbr": 0.67,
            "bps": 442537.0,  # 0.67 × 442,537 = 296,500 = close
            "dividend_per_share": 10000.0,
            "dividend_yield": 3.37,
            "market_cap": 1.0,
        },
    ]


class MarketRowConsistencyTests(TempDbMixin):
    def test_consistent_dataset_does_not_need_refresh(self):
        self.assertFalse(stock_price.market_data_needs_refresh(_consistent_rows()))

    def test_cross_year_close_poisoning_triggers_refresh(self):
        # 현대차 2025 행 실사례: 비율은 연말 종가(296,500)로 계산됐는데
        # close_price 만 2026년 3월 시세(538,000)로 덮어써진 상태.
        rows = _consistent_rows()
        rows[-1]["close_price"] = 538000.0
        self.assertTrue(stock_price._market_row_is_inconsistent(rows[-1]))
        self.assertTrue(stock_price.market_data_needs_refresh(rows))

    def test_row_without_ratio_or_close_is_not_flagged(self):
        # 배당 전용 행(close 없음), 비율 없는 행, 적자(음수 eps) 행은 판정 제외.
        self.assertFalse(stock_price._market_row_is_inconsistent({"year": 2025, "dividend_per_share": 100.0}))
        self.assertFalse(stock_price._market_row_is_inconsistent({"year": 2025, "close_price": 1000.0}))
        self.assertFalse(stock_price._market_row_is_inconsistent(
            {"year": 2025, "close_price": 1000.0, "per": -5.0, "eps": -200.0}
        ))

    def test_tiny_ratio_is_exempt_from_rounding_noise(self):
        # per 저장값은 소수 2자리 반올림이라 0.05 미만 배수는 반올림만으로
        # 30% 넘게 어긋날 수 있다 — 판정 제외 가드.
        self.assertFalse(stock_price._market_row_is_inconsistent(
            {"year": 2025, "close_price": 149.0, "per": 0.01, "eps": 10000.0}
        ))


class UpsertTargetMetricsTests(TempDbMixin):
    async def seed(self):
        await financial_repo.save_market_data("005380", [
            {
                "year": 2025,
                "close_price": 296500.0,
                "per": 8.36,
                "pbr": 0.67,
                "eps": 36088.0,
                "bps": 442537.0,
                "dividend_per_share": 10000.0,
                "dividend_yield": 3.37,
                "market_cap": 1.0,
            },
        ])

    async def test_upsert_never_touches_existing_row_fields(self):
        await portfolio_repo.upsert_market_target_metrics([
            {"stock_code": "005380", "year": 2025, "eps": 99999.0, "bps": 88888.0, "close_price": 538000.0},
        ])

        rows = await financial_repo.get_market_data("005380")
        row = next(r for r in rows if r["year"] == 2025)
        self.assertEqual(row["close_price"], 296500.0)
        self.assertEqual(row["eps"], 36088.0)
        self.assertEqual(row["bps"], 442537.0)
        self.assertEqual(row["per"], 8.36)
        self.assertEqual(row["pbr"], 0.67)

    async def test_upsert_fills_missing_fields_and_new_years(self):
        db_rows = await financial_repo.get_market_data("005380")
        self.assertEqual(len(db_rows), 1)

        # 새 연도 행 생성 — close_price 는 NULL 로 남는다(소유자는 fetch_market_data).
        await portfolio_repo.upsert_market_target_metrics([
            {"stock_code": "005380", "year": 2024, "eps": 46042.0, "bps": 412360.0},
        ])
        # 기존 행의 빈 칸(eps NULL)만 채운다.
        await financial_repo.save_market_data("005380", [
            {"year": 2023, "close_price": 179152.0},
        ])
        await portfolio_repo.upsert_market_target_metrics([
            {"stock_code": "005380", "year": 2023, "eps": 43589.0, "bps": 351279.0},
        ])

        by_year = {r["year"]: r for r in await financial_repo.get_market_data("005380")}
        self.assertEqual(by_year[2024]["eps"], 46042.0)
        self.assertIsNone(by_year[2024]["close_price"])
        self.assertEqual(by_year[2023]["close_price"], 179152.0)
        self.assertEqual(by_year[2023]["eps"], 43589.0)
        self.assertEqual(by_year[2023]["bps"], 351279.0)


class PersistTargetMetricsTests(TempDbMixin):
    async def test_persist_drops_echoed_close_price(self):
        basis = {
            "fiscalYear": 2025,
            "eps": 35000.0,
            "bps": 440000.0,
            # _apply_market_cache_fallback 이 "최신 연도" 행에서 에코한 값 —
            # 회계연도 행에 기록되면 교차연도 오염이므로 repo 로 넘기면 안 된다.
            "closePrice": 538000.0,
        }
        with patch.object(
            portfolio_repo, "upsert_market_target_metrics", new=AsyncMock(return_value=1)
        ) as upsert:
            await valuation._persist_market_target_metrics("005380", basis)

        upsert.assert_awaited_once()
        (rows,) = upsert.await_args.args
        self.assertEqual(rows, [{"stock_code": "005380", "year": 2025, "eps": 35000.0, "bps": 440000.0}])

    async def test_persist_skips_when_no_metrics(self):
        with patch.object(
            portfolio_repo, "upsert_market_target_metrics", new=AsyncMock(return_value=1)
        ) as upsert:
            await valuation._persist_market_target_metrics("005380", {"fiscalYear": 2025, "closePrice": 538000.0})

        upsert.assert_not_awaited()
