"""target_resolver: 목표가 수식을 라이브 데이터로 평가."""

from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

from services.portfolio import target_resolver as tr


class ResolveFormulaTargetTests(unittest.IsolatedAsyncioTestCase):
    async def test_bps_formula(self):
        with patch.object(tr, "fetch_valuation_basis",
                          new=AsyncMock(return_value={"applicable": True, "bps": 900000})):
            value = await tr.resolve_formula_target("005930", "BPS*0.5", 100000)
        self.assertEqual(value, 450000)

    async def test_avg_price_variable(self):
        # 매입가만 쓰는 수식은 추가 조회 없이 평가된다.
        value = await tr.resolve_formula_target("005930", "매입가*1.5", 100000)
        self.assertEqual(value, 150000)

    async def test_missing_variable_returns_none(self):
        # BPS 를 못 얻으면 None → 호출측이 폴백.
        with patch.object(tr, "fetch_valuation_basis",
                          new=AsyncMock(return_value={"applicable": False})):
            value = await tr.resolve_formula_target("005930", "BPS*0.5", 100000)
        self.assertIsNone(value)

    async def test_no_formula_returns_none(self):
        self.assertIsNone(await tr.resolve_formula_target("005930", "", 100000))

    async def test_holding_value_per_share(self):
        # 보유지분 수식: 자회사 지분가치 합 / 유통주식수.
        meta = {"holdingValue": {"meta": {"032830": {
            "subsidiaries": [{"code": "005930", "sharesHeld": 100}],
            "totalShares": 10, "treasuryShares": 0,
        }}}}
        with patch.object(tr.integrations, "build_public_integrations", return_value=meta), \
             patch.object(tr, "_quote_price", new=AsyncMock(return_value=70000.0)):
            # 보유지분/주 = 100*70000 / 10 = 700,000.  수식 보유지분*1.0
            value = await tr.resolve_formula_target("032830", "보유지분", 100000)
        self.assertEqual(value, 700000)


if __name__ == "__main__":
    unittest.main()
