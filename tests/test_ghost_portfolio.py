"""유령 포트폴리오("가만히 있었다면") 및 매도 후 추적 단위·통합 테스트."""

from __future__ import annotations

import json
from datetime import date, timedelta
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from _harness import seed_corp_codes, seed_user
from fastapi import FastAPI

from domain import ghost_portfolio as rules
from repositories import db as db_repo
from repositories import snapshots as snapshots_repo
from routes import investment_insights as routes
from services.portfolio import ghost as service


def _make_bars(start_iso: str, closes: list[float], *, shares: float | None = None) -> list[dict]:
    """주말을 건너뛰며 평일 일봉 목록을 만든다."""
    cur = date.fromisoformat(start_iso)
    out = []
    for close in closes:
        while cur.weekday() >= 5:
            cur += timedelta(days=1)
        row = {
            "date": cur.isoformat(),
            "open": close,
            "high": close * 1.02,
            "low": close * 0.98,
            "close": close,
            "currency": "KRW",
        }
        if shares is not None:
            row["listed_shares"] = shares
        out.append(row)
        cur += timedelta(days=1)
    return out


def test_ghost_portfolio_zero_trades_gives_zero_effect():
    nav_points = [
        {"date": "2025-12-30", "total_value": 10_000_000, "fx_usdkrw": 1400.0},
        {"date": "2026-06-01", "total_value": 11_200_000, "fx_usdkrw": 1410.0},
        {"date": "2026-11-27", "total_value": 12_500_000, "fx_usdkrw": 1420.0},
    ]
    baseline_stocks = [
        {"stock_code": "005930", "stock_name": "삼성전자", "quantity": 100, "unit_price": 80_000, "market_value": 8_000_000},
        {"stock_code": "CASH_KRW", "stock_name": "원화", "quantity": 2_000_000, "unit_price": 1, "market_value": 2_000_000},
    ]
    snapshot_prices = {
        "2025-12-30": {
            "005930": {"unit_price": 80_000, "quantity": 100, "market_value": 8_000_000, "priced_from_fallback": 0},
            "CASH_KRW": {"unit_price": 1, "quantity": 2_000_000, "market_value": 2_000_000, "priced_from_fallback": 0},
        },
        "2026-06-01": {
            "005930": {"unit_price": 92_000, "quantity": 100, "market_value": 9_200_000, "priced_from_fallback": 0},
            "CASH_KRW": {"unit_price": 1, "quantity": 2_000_000, "market_value": 2_000_000, "priced_from_fallback": 0},
        },
        "2026-11-27": {
            "005930": {"unit_price": 105_000, "quantity": 100, "market_value": 10_500_000, "priced_from_fallback": 0},
            "CASH_KRW": {"unit_price": 1, "quantity": 2_000_000, "market_value": 2_000_000, "priced_from_fallback": 0},
        },
    }
    res = rules.compute_ghost_portfolio(
        nav_points=nav_points,
        baseline_stocks=baseline_stocks,
        snapshot_prices_by_date=snapshot_prices,
        external_krw_closes={},
        split_flags={},
        cashflows=[],
    )
    assert res["available"] is True
    assert res["scope"] == "complete"
    assert res["ghost_end_value"] == 12_500_000
    assert res["effect_krw"] == 0
    assert res["effect_pct_p"] == 0
    assert [pt["diff_krw"] for pt in res["series"]] == [0, 0, 0]


def test_ghost_portfolio_cashflow_proportional_assumption_and_modified_dietz():
    nav_points = [
        {"date": "2025-12-30", "total_value": 6_000_000, "fx_usdkrw": 1400.0},
        {"date": "2026-06-30", "total_value": 10_500_000, "fx_usdkrw": 1400.0},
        {"date": "2026-12-29", "total_value": 11_480_000, "fx_usdkrw": 1400.0},
    ]
    baseline_stocks = [
        {"stock_code": "005930", "stock_name": "삼성전자", "quantity": 100, "unit_price": 50_000, "market_value": 5_000_000},
        {"stock_code": "CASH_KRW", "stock_name": "원화", "quantity": 1_000_000, "unit_price": 1, "market_value": 1_000_000},
    ]
    ext_closes = {
        "005930": [("2025-12-30", 50_000.0), ("2026-06-30", 54_000.0), ("2026-12-29", 60_000.0)],
    }
    cashflows = [
        {"date": "2026-06-29", "applied_snapshot_date": "2026-06-30", "type": "deposit", "amount": 4_000_000, "units_change": 4000.0},
        {"date": "2026-12-29", "applied_snapshot_date": None, "type": "deposit", "amount": 9_999_999, "units_change": None},
    ]
    res = rules.compute_ghost_portfolio(
        nav_points=nav_points,
        baseline_stocks=baseline_stocks,
        snapshot_prices_by_date={},
        external_krw_closes=ext_closes,
        split_flags={},
        cashflows=cashflows,
    )
    assert res["scope"] == "complete"
    assert res["net_cashflow_krw"] == 4_000_000
    assert res["ghost_end_value"] == 11_000_000
    assert res["actual_end_value"] == 11_480_000
    assert res["effect_krw"] == 480_000
    assert res["ghost_return_pct"] == 12.5
    assert res["actual_return_pct"] == 18.5
    assert res["effect_pct_p"] == 6.0


@pytest.mark.parametrize("uncalc_end_mv", [5_200_000, 2_800_000])
def test_scoped_comparison_zero_trades_with_uncalculated_surge_or_crash_has_zero_effect(uncalc_end_mv):
    """리뷰 항목 1: 매매가 없으면 미계산 종목이 급등(+30%)·급락(-30%)해도 매매 효과는 0이어야 한다."""
    # 기준일: 005930 500만(50%) + 현금 100만(10%) [계산 가능 60%] + AAPL 400만(40%, 분할 미확인 미계산) = 1,000만
    # 종료일: 005930 550만(+10%), 현금 100만, AAPL은 급등(520만) 또는 급락(280만) — 매매 없음
    baseline_stocks = [
        {"stock_code": "005930", "stock_name": "삼성전자", "quantity": 100, "unit_price": 50_000, "market_value": 5_000_000},
        {"stock_code": "AAPL", "stock_name": "애플", "quantity": 20, "unit_price": 200_000, "market_value": 4_000_000},
        {"stock_code": "CASH_KRW", "stock_name": "원화", "quantity": 1_000_000, "unit_price": 1, "market_value": 1_000_000},
    ]
    nav_points = [
        {"date": "2026-01-02", "total_value": 10_000_000, "fx_usdkrw": 1400.0},
        {"date": "2026-06-01", "total_value": 5_500_000 + 1_000_000 + uncalc_end_mv, "fx_usdkrw": 1400.0},
    ]
    snapshot_prices = {
        "2026-01-02": {
            "005930": {"unit_price": 50_000, "quantity": 100, "market_value": 5_000_000, "priced_from_fallback": 0},
            "AAPL": {"unit_price": 200_000, "quantity": 20, "market_value": 4_000_000, "priced_from_fallback": 0},
            "CASH_KRW": {"unit_price": 1, "quantity": 1_000_000, "market_value": 1_000_000, "priced_from_fallback": 0},
        },
        "2026-06-01": {
            "005930": {"unit_price": 55_000, "quantity": 100, "market_value": 5_500_000, "priced_from_fallback": 0},
            "AAPL": {"unit_price": uncalc_end_mv / 20, "quantity": 20, "market_value": uncalc_end_mv, "priced_from_fallback": 0},
            "CASH_KRW": {"unit_price": 1, "quantity": 1_000_000, "market_value": 1_000_000, "priced_from_fallback": 0},
        },
    }
    res = rules.compute_ghost_portfolio(
        nav_points=nav_points,
        baseline_stocks=baseline_stocks,
        snapshot_prices_by_date=snapshot_prices,
        external_krw_closes={"005930": [("2026-01-02", 50_000.0), ("2026-06-01", 55_000.0)]},
        split_flags={"AAPL": "분할 여부 확인 전 (해외 차트 미확보)"},
        cashflows=[],
    )
    assert res["scope"] == "partial"
    assert res["coverage"]["calculable_weight_pct"] == 60.0
    assert res["baseline_value"] == 6_000_000
    assert res["actual_end_value"] == 6_500_000
    assert res["ghost_end_value"] == 6_500_000
    assert res["effect_krw"] == 0
    assert res["effect_pct_p"] == 0.0


def test_reviewer_repro_half_weight_uncalculated_surge_without_trades_is_zero():
    """리뷰어 재현: 비중 50% 미계산 종목 +30%, 매매 없음 → effect 0 (이전 구현은 +15.0%p)."""
    day0 = {
        "005930": {"unit_price": 50_000, "quantity": 100, "market_value": 5_000_000, "priced_from_fallback": 0},
        "BTC": {"unit_price": 50_000_000, "quantity": 0.1, "market_value": 5_000_000, "priced_from_fallback": 0},
    }
    day1 = {
        "005930": {"unit_price": 50_000, "quantity": 100, "market_value": 5_000_000, "priced_from_fallback": 0},
        "BTC": {"unit_price": 65_000_000, "quantity": 0.1, "market_value": 6_500_000, "priced_from_fallback": 0},
    }
    res = rules.compute_ghost_portfolio(
        nav_points=[
            {"date": "2026-01-02", "total_value": 10_000_000},
            {"date": "2026-06-01", "total_value": 11_500_000},
        ],
        baseline_stocks=[{"stock_code": c, "stock_name": c, **v} for c, v in day0.items()],
        snapshot_prices_by_date={"2026-01-02": day0, "2026-06-01": day1},
        external_krw_closes={},
        split_flags={"BTC": "특수자산 과거 종가 미제공"},
        cashflows=[],
    )
    assert res["scope"] == "partial"
    assert res["coverage"]["calculable_weight_pct"] == 50.0
    assert res["effect_krw"] == 0
    assert res["effect_pct_p"] == 0.0


def test_scoped_invariant_survives_residual_drift_fallback_rows_and_uncalculated_sale():
    """무매매(계산 범위) 불변식: 정산 잔차 변동·직전 값 대체 행·미계산 종목의 현금 전환이 있어도 effect 0."""
    # 기준일 잔차 3,000원(0.5% 미만 → 표시 안 함), 다음 날 잔차 4,000원으로 변동
    day0 = {
        "005930": {"unit_price": 50_000, "quantity": 100, "market_value": 5_000_000, "priced_from_fallback": 0},
        "AAPL": {"unit_price": 200_000, "quantity": 10, "market_value": 2_000_000, "priced_from_fallback": 0},
        "CASH_KRW": {"unit_price": 1, "quantity": 3_000_000, "market_value": 3_000_000, "priced_from_fallback": 0},
    }
    # 1일차: 005930은 시세 실패로 직전 값 대체(수량 동일), AAPL 일부(5주) 매도 기록 있음 → 현금 +120만
    day1 = {
        "005930": {"unit_price": 50_000, "quantity": 100, "market_value": 5_000_000, "priced_from_fallback": 1},
        "AAPL": {"unit_price": 260_000, "quantity": 5, "market_value": 1_300_000, "priced_from_fallback": 0},
        "CASH_KRW": {"unit_price": 1, "quantity": 4_200_000, "market_value": 4_200_000, "priced_from_fallback": 0},
    }
    # 2일차: AAPL 나머지 5주가 기록 없이 소멸(현금 +130만)
    day2 = {
        "005930": {"unit_price": 52_000, "quantity": 100, "market_value": 5_200_000, "priced_from_fallback": 0},
        "CASH_KRW": {"unit_price": 1, "quantity": 5_500_000, "market_value": 5_500_000, "priced_from_fallback": 0},
    }
    res = rules.compute_ghost_portfolio(
        nav_points=[
            {"date": "2026-01-02", "total_value": 10_003_000},
            {"date": "2026-03-02", "total_value": 5_000_000 + 1_300_000 + 4_200_000 + 4_000},
            {"date": "2026-06-01", "total_value": 5_200_000 + 5_500_000 + 4_000},
        ],
        baseline_stocks=[{"stock_code": c, "stock_name": c, **v} for c, v in day0.items()],
        snapshot_prices_by_date={"2026-01-02": day0, "2026-03-02": day1, "2026-06-01": day2},
        external_krw_closes={"005930": [("2026-01-02", 50_000.0), ("2026-03-02", 51_000.0), ("2026-06-01", 52_000.0)]},
        split_flags={"AAPL": "분할 여부 확인 전 (해외 차트 미확보)"},
        cashflows=[],
        uncalc_krw_trade_flows={"AAPL": [("2026-02-20", 1_200_000.0)]},
    )
    assert res["scope"] == "partial"
    assert [pt["diff_krw"] for pt in res["series"]] == [0, 0, 0]
    assert res["effect_krw"] == 0
    assert res["effect_pct_p"] == 0.0
    assert res["baseline_value"] == 8_000_000


def test_scoped_comparison_with_trades_isolates_calculable_alpha_and_blocks_under_50_pct():
    """리뷰 항목 1: 매매가 있을 때는 계산 가능 범위 효과만 반영하고, 계산 가능 비중 < 50%면 미계산 처리한다."""
    # 기준일: 005930 600만(60%) + 현금 200만(20%) [계산 가능 80% = 800만] + AAPL 200만(20%, 미계산) = 1,000만
    # 실제 매매: 005930 전량 매도 후 000660 교체 → 000660 평가액 720만, 현금 200만 (실제 계산범위 = 920만)
    # 방치 시: 005930 종가 상승으로 660만 + 현금 200만 (방치 계산범위 = 860만)
    # 미계산 AAPL은 200만 → 300만(+50% 급등)했지만 양쪽에서 제외되므로 매매 효과는 정확히 +60만(+7.50%p = 60만/800만)
    baseline_stocks = [
        {"stock_code": "005930", "stock_name": "삼성전자", "quantity": 100, "unit_price": 60_000, "market_value": 6_000_000},
        {"stock_code": "AAPL", "stock_name": "애플", "quantity": 10, "unit_price": 200_000, "market_value": 2_000_000},
        {"stock_code": "CASH_KRW", "stock_name": "원화", "quantity": 2_000_000, "unit_price": 1, "market_value": 2_000_000},
    ]
    nav_points = [
        {"date": "2026-01-02", "total_value": 10_000_000, "fx_usdkrw": 1400.0},
        {"date": "2026-06-01", "total_value": 7_200_000 + 2_000_000 + 3_000_000, "fx_usdkrw": 1400.0},
    ]
    snapshot_prices = {
        "2026-01-02": {
            "005930": {"unit_price": 60_000, "quantity": 100, "market_value": 6_000_000, "priced_from_fallback": 0},
            "AAPL": {"unit_price": 200_000, "quantity": 10, "market_value": 2_000_000, "priced_from_fallback": 0},
            "CASH_KRW": {"unit_price": 1, "quantity": 2_000_000, "market_value": 2_000_000, "priced_from_fallback": 0},
        },
        "2026-06-01": {
            "000660": {"unit_price": 180_000, "quantity": 40, "market_value": 7_200_000, "priced_from_fallback": 0},
            "AAPL": {"unit_price": 300_000, "quantity": 10, "market_value": 3_000_000, "priced_from_fallback": 0},
            "CASH_KRW": {"unit_price": 1, "quantity": 2_000_000, "market_value": 2_000_000, "priced_from_fallback": 0},
        },
    }
    res = rules.compute_ghost_portfolio(
        nav_points=nav_points,
        baseline_stocks=baseline_stocks,
        snapshot_prices_by_date=snapshot_prices,
        external_krw_closes={"005930": [("2026-01-02", 60_000.0), ("2026-06-01", 66_000.0)]},
        split_flags={"AAPL": "분할 여부 확인 전 (해외 차트 미확보)"},
        cashflows=[],
    )
    assert res["scope"] == "partial"
    assert res["coverage"]["calculable_weight_pct"] == 80.0
    assert res["actual_end_value"] == 9_200_000
    assert res["ghost_end_value"] == 8_600_000
    assert res["effect_krw"] == 600_000
    assert res["effect_pct_p"] == 7.5

    # 계산 가능 비중이 50% 미만(40% 계산 가능, 60% 미계산)이면 헤드라인 수치를 내지 않고 미계산 반환
    under_half = rules.compute_ghost_portfolio(
        nav_points=[
            {"date": "2026-01-02", "total_value": 10_000_000},
            {"date": "2026-06-01", "total_value": 13_000_000},
        ],
        baseline_stocks=[
            {"stock_code": "005930", "stock_name": "삼성전자", "quantity": 40, "unit_price": 100_000, "market_value": 4_000_000},
            {"stock_code": "AAPL", "stock_name": "애플", "quantity": 30, "unit_price": 200_000, "market_value": 6_000_000},
        ],
        snapshot_prices_by_date={},
        external_krw_closes={"005930": [("2026-01-02", 100_000.0), ("2026-06-01", 110_000.0)]},
        split_flags={"AAPL": "분할 여부 확인 전"},
        cashflows=[],
    )
    assert under_half["scope"] == "uncalculated"
    assert "계산 가능 비중 부족" in (under_half["reason"] or "")
    assert under_half["effect_krw"] is None
    assert under_half["effect_pct_p"] is None


def test_split_verification_failure_and_snapshot_quantity_jump_and_foreign_cash_fx():
    """리뷰 항목 2·3: 일봉 누락 분할 미확인, 무거래 스냅샷 수량 급변, 환율 없는 외화 예수금은 모두 미계산."""
    # 1) 국내 일봉이 비어 있으면 분할 여부 확인 전 사유 반환
    assert "분할 여부 확인 전" in (rules.detect_krx_series_split_or_mismatch(50_000, "2026-01-02", "2026-06-01", []) or "")
    # 사후 5:1 수정주가 불일치 및 무수정 일봉 급변
    assert rules.detect_krx_series_split_or_mismatch(50_000, "2026-01-02", "2026-01-10", _make_bars("2026-01-02", [10_000, 10_200])) is not None
    assert rules.detect_krx_series_split_or_mismatch(50_000, "2026-01-02", "2026-01-10", _make_bars("2026-01-02", [50_000, 10_000])) is not None

    # 2) 스냅샷 수량이 거래 없이 100 → 120주(20% 무상증자, 단가 100,000 → 83,333)로 변하면 분할 의심 감지
    flags = rules.detect_snapshot_quantity_split_flags(
        stock_rows=[
            {"date": "2026-01-02", "stock_code": "005930", "quantity": 100, "unit_price": 100_000, "market_value": 10_000_000},
            {"date": "2026-01-02", "stock_code": "CASH_KRW", "quantity": 5_000_000, "unit_price": 1, "market_value": 5_000_000},
            {"date": "2026-03-02", "stock_code": "005930", "quantity": 120, "unit_price": 83_333, "market_value": 10_000_000},
            {"date": "2026-03-02", "stock_code": "CASH_KRW", "quantity": 5_000_000, "unit_price": 1, "market_value": 5_000_000},
        ],
        manual_trades=[],
        nh_rows=[],
        cashflows=[],
        baseline_date="2026-01-02",
        ending_date="2026-03-02",
    )
    assert "005930" in flags

    # 반면 현금이 실제로 차감된 정상 추가 매수(100→150주, 단가 100,000 유지, 현금 500만→0)는 분할로 오탐하지 않는다.
    normal_buy_flags = rules.detect_snapshot_quantity_split_flags(
        stock_rows=[
            {"date": "2026-01-02", "stock_code": "005930", "quantity": 100, "unit_price": 100_000, "market_value": 10_000_000},
            {"date": "2026-01-02", "stock_code": "CASH_KRW", "quantity": 5_000_000, "unit_price": 1, "market_value": 5_000_000},
            {"date": "2026-03-02", "stock_code": "005930", "quantity": 150, "unit_price": 100_000, "market_value": 15_000_000},
            {"date": "2026-03-02", "stock_code": "CASH_KRW", "quantity": 0, "unit_price": 1, "market_value": 0},
        ],
        manual_trades=[],
        nh_rows=[],
        cashflows=[],
        baseline_date="2026-01-02",
        ending_date="2026-03-02",
    )
    assert "005930" not in normal_buy_flags

    # 3) 환율 소스가 없는 외화 현금(CASH_EUR)은 기준일 값 고정 대신 uncalculated("환율 미확보") 처리
    fx_res = rules.compute_ghost_portfolio(
        nav_points=[
            {"date": "2026-01-02", "total_value": 10_000_000},
            {"date": "2026-06-01", "total_value": 10_900_000},
        ],
        baseline_stocks=[
            {"stock_code": "005930", "stock_name": "삼성전자", "quantity": 80, "unit_price": 100_000, "market_value": 8_000_000},
            {"stock_code": "CASH_EUR", "stock_name": "유로", "quantity": 1_300, "unit_price": 1_538.46, "market_value": 2_000_000},
        ],
        snapshot_prices_by_date={
            "2026-01-02": {
                "005930": {"unit_price": 100_000, "quantity": 80, "market_value": 8_000_000, "priced_from_fallback": 0},
                "CASH_EUR": {"unit_price": 1_538.46, "quantity": 1_300, "market_value": 2_000_000, "priced_from_fallback": 0},
            },
            "2026-06-01": {
                "005930": {"unit_price": 110_000, "quantity": 80, "market_value": 8_800_000, "priced_from_fallback": 0},
                "CASH_EUR": {"unit_price": None, "quantity": 1_300, "market_value": 2_100_000, "priced_from_fallback": 1},
            },
        },
        external_krw_closes={"005930": [("2026-01-02", 100_000.0), ("2026-06-01", 110_000.0)]},
        split_flags={},
        cashflows=[],
    )
    assert fx_res["uncalculated"][0]["stock_code"] == "CASH_EUR"
    assert "환율 미확보" in fx_res["uncalculated"][0]["reason"]
    assert fx_res["effect_krw"] == 0


def test_post_sale_manual_recorded_date_priority_and_overseas_bar_matching():
    """리뷰 항목 4·5: 기록일 기준 수동 매매 표기/인접일 우선순위 및 해외 직전 거래일 봉 고저 범위 매칭."""
    # 항목 4: 기록일 기준(date_is_recorded_at=True) 수동 매매가 스냅샷 소멸일(2026-03-05)과 인접(2026-03-07)하면 스냅샷 소멸 우선,
    #         일부 매도처럼 스냅샷 소멸이 없으면 "기록일 기준(체결일과 다를 수 있음)" 주석 유지
    events = rules.collect_sale_events(
        nh_rows=[],
        manual_trades=[
            {
                "side": "sell", "trade_date": "2026-03-07", "date_is_recorded_at": True,
                "stock_code": "005380", "stock_name": "현대차", "currency": "KRW",
                "quantity": 3, "price": 240_000, "gross_amount": 720_000, "quantity_after": 0,
            },
            {
                "side": "sell", "trade_date": "2026-03-10", "date_is_recorded_at": True,
                "stock_code": "000660", "stock_name": "SK하이닉스", "currency": "KRW",
                "quantity": 2, "price": 190_000, "gross_amount": 380_000, "quantity_after": 5,
            },
        ],
        stock_snapshots=[
            {"date": "2026-03-04", "stock_code": "005380", "quantity": 3, "market_value": 720_000},
            {"date": "2026-03-04", "stock_code": "000660", "quantity": 7, "market_value": 1_330_000},
            {"date": "2026-03-05", "stock_code": "000660", "quantity": 7, "market_value": 1_330_000},
        ],
        effective_since="2026-01-01",
    )
    by_code = {e["stock_code"]: e for e in events}
    assert by_code["005380"]["source"] == "snapshot_disappearance"
    assert by_code["005380"]["trade_date"] == "2026-03-05"
    assert by_code["000660"]["source"] == "manual_trade"
    assert by_code["000660"]["date_is_recorded_at"] is True
    assert by_code["000660"]["date_note"] == "기록일 기준(체결일과 다를 수 있음)"

    # 항목 5: 해외 매도(KST 2026-05-13 새벽 체결 = 미국 현지 2026-05-12 봉 고저 범위 [212, 218], 5/13 봉은 [225, 230] 갭상승)
    us_bars = [
        {"date": "2026-05-12", "open": 214.0, "high": 218.0, "low": 212.0, "close": 216.0, "currency": "USD"},
        {"date": "2026-05-13", "open": 226.0, "high": 230.0, "low": 225.0, "close": 228.0, "currency": "USD"},
    ] + [
        {"date": (date(2026, 5, 14) + timedelta(days=i)).isoformat(), "open": 220.0, "high": 222.0, "low": 219.0, "close": 220.0, "currency": "USD"}
        for i in range(30)
        if (date(2026, 5, 14) + timedelta(days=i)).weekday() < 5
    ]
    matched = rules.evaluate_post_sales(
        sale_events=[{
            "trade_date": "2026-05-13", "stock_code": "AAPL", "stock_name": "애플",
            "currency": "USD", "source": "nh_realized_pnl", "sale_type": "closed",
            "price_basis": "execution", "quantity": 10, "sell_price": 215.0, "sell_amount": 2150.0, "status": "ok",
        }],
        daily_bars={"AAPL": us_bars},
        corporate_events={"AAPL": {"verified": True, "ex_dates": set(), "split_dates": set()}},
        since="2026-01-01",
    )
    assert matched["items"][0]["status"] == "ok"
    assert matched["items"][0]["matched_bar_date"] == "2026-05-12"

    # 당일·직전 거래일 어느 봉의 고저 범위에도 들지 않으면 미계산 처리
    unmatched = rules.evaluate_post_sales(
        sale_events=[{
            "trade_date": "2026-05-13", "stock_code": "AAPL", "stock_name": "애플",
            "currency": "USD", "source": "nh_realized_pnl", "sale_type": "closed",
            "price_basis": "execution", "quantity": 10, "sell_price": 310.0, "sell_amount": 3100.0, "status": "ok",
        }],
        daily_bars={"AAPL": us_bars},
        corporate_events={"AAPL": {"verified": True, "ex_dates": set(), "split_dates": set()}},
        since="2026-01-01",
    )
    assert unmatched["items"][0]["status"] == "uncalculated"
    assert "고저 범위 불일치" in unmatched["items"][0]["reason"]


def test_post_sale_sources_priority_dedup_and_horizons():
    bars_a = _make_bars("2026-03-02", [100_000 - i * 1_000 for i in range(25)])
    bars_b = _make_bars("2026-03-02", [50_000 + i * 1_000 for i in range(25)])
    bars_c = _make_bars("2026-03-02", [30_000 + i * 500 for i in range(25)])

    sale_day_a = bars_a[0]["date"]
    sale_day_b = bars_b[1]["date"]
    prev_day_c = bars_c[2]["date"]
    gone_day_c = bars_c[3]["date"]
    recent_day = bars_a[22]["date"]

    nh_rows = [
        {"market": "domestic", "trade_date": sale_day_a, "stock_code": "005930", "stock_name": "삼성전자", "currency": "KRW", "sell_quantity": 4, "sell_krw": 396_000, "issues": []},
        {"market": "domestic", "trade_date": sale_day_a, "stock_code": "005930", "stock_name": "삼성전자", "currency": "KRW", "sell_quantity": 6, "sell_krw": 604_000, "issues": []},
        {"market": "domestic", "trade_date": "2026-03-31", "stock_code": "035420", "stock_name": "NAVER", "currency": "KRW", "sell_quantity": 5, "sell_krw": 900_000, "issues": ["detail_pending"]},
    ]
    manual_trades = [
        {"side": "sell", "trade_date": sale_day_a, "stock_code": "005930", "stock_name": "삼성전자", "currency": "KRW", "quantity": 10, "price": 100_000, "gross_amount": 1_000_000, "quantity_after": 0},
        {"side": "sell", "trade_date": sale_day_b, "stock_code": "000660", "stock_name": "SK하이닉스", "currency": "KRW", "quantity": 5, "price": 51_000, "gross_amount": 255_000, "quantity_after": 3},
        {"side": "sell", "trade_date": recent_day, "stock_code": "005930", "stock_name": "삼성전자", "currency": "KRW", "quantity": 2, "price": 78_000, "gross_amount": 156_000, "quantity_after": 0},
    ]
    stock_snapshots = [
        {"date": prev_day_c, "stock_code": "005380", "quantity": 3, "market_value": 93_000},
        {"date": gone_day_c, "stock_code": "000660", "quantity": 3, "market_value": 159_000},
    ]

    events = rules.collect_sale_events(
        nh_rows=nh_rows,
        manual_trades=manual_trades,
        stock_snapshots=stock_snapshots,
        effective_since="2026-01-01",
    )
    assert len(events) == 5

    post = rules.evaluate_post_sales(
        sale_events=events,
        daily_bars={"005930": bars_a, "000660": bars_b, "005380": bars_c},
        corporate_events={
            "005930": {"verified": True, "ex_dates": set(), "split_dates": set()},
            "000660": {"verified": True, "ex_dates": set(), "split_dates": set()},
            "005380": {"verified": True, "ex_dates": set(), "split_dates": set()},
        },
        since="2026-01-01",
    )
    summary = post["summary"]
    assert summary["total_count"] == 5
    assert summary["uncalculated_sale_count"] == 1
    assert summary["matured_20d_count"] == 3
    assert summary["calculable_20d_count"] == 3
    assert summary["pending_20d_count"] == 1
    assert summary["avoided_drop_20d_count"] == 1
    assert summary["rose_after_20d_count"] == 2


def test_post_sale_marks_ex_dividend_split_and_unverified_as_uncalculated():
    bars = _make_bars("2026-04-01", [50_000 + i * 200 for i in range(25)])
    sale_day = bars[0]["date"]
    ex_day_10 = bars[10]["date"]

    res_ex = rules.evaluate_post_sales(
        sale_events=[{
            "trade_date": sale_day, "stock_code": "005930", "stock_name": "삼성전자",
            "currency": "KRW", "source": "nh_realized_pnl", "sale_type": "closed",
            "price_basis": "execution", "quantity": 10, "sell_price": 50_000, "sell_amount": 500_000, "status": "ok",
        }],
        daily_bars={"005930": bars},
        corporate_events={"005930": {"verified": True, "ex_dates": {ex_day_10}, "split_dates": set()}},
        since="2026-01-01",
    )
    row_ex = res_ex["items"][0]
    assert row_ex["day5"]["status"] == "ok"
    assert row_ex["day20"]["status"] == "uncalculated"
    assert "배당락" in row_ex["day20"]["reason"]
    assert row_ex["current"]["status"] == "uncalculated"

    res_unverified = rules.evaluate_post_sales(
        sale_events=[{
            "trade_date": sale_day, "stock_code": "005930", "stock_name": "삼성전자",
            "currency": "KRW", "source": "nh_realized_pnl", "sale_type": "closed",
            "price_basis": "execution", "quantity": 10, "sell_price": 50_000, "sell_amount": 500_000, "status": "ok",
        }],
        daily_bars={"005930": bars},
        corporate_events={"005930": {"verified": False, "ex_dates": set(), "split_dates": set()}},
        since="2026-01-01",
    )
    assert res_unverified["items"][0]["day5"]["status"] == "uncalculated"
    assert "확인 전" in res_unverified["items"][0]["day5"]["reason"]

    res_split = rules.evaluate_post_sales(
        sale_events=[{
            "trade_date": sale_day, "stock_code": "005930", "stock_name": "삼성전자",
            "currency": "KRW", "source": "nh_realized_pnl", "sale_type": "closed",
            "price_basis": "execution", "quantity": 10, "sell_price": 250_000, "sell_amount": 2_500_000, "status": "ok",
        }],
        daily_bars={"005930": bars},
        corporate_events={"005930": {"verified": True, "ex_dates": set(), "split_dates": set()}},
        since="2026-01-01",
    )
    assert res_split["items"][0]["status"] == "uncalculated"
    assert "불일치" in res_split["items"][0]["reason"]


@pytest.mark.asyncio
async def test_ghost_api_endpoint_auth_validation_caching_and_db_roundtrip(temp_db):
    await seed_user("u1")
    await seed_corp_codes([
        ("005930", "00126380", "삼성전자", "2026-01-01"),
        ("000660", "00164779", "SK하이닉스", "2026-01-01"),
    ])

    await snapshots_repo.save_snapshot("u1", "2025-12-30", 10_000_000, 10_000_000, 1000.0, 10_000.0, 1400.0)
    await snapshots_repo.save_stock_snapshots("u1", "2025-12-30", [
        {"stock_code": "005930", "market_value": 8_000_000, "quantity": 100, "unit_price": 80_000, "currency": "KRW", "fx_rate": 1.0},
        {"stock_code": "CASH_KRW", "market_value": 2_000_000, "quantity": 2_000_000, "unit_price": 1, "currency": "KRW", "fx_rate": 1.0},
    ])
    await snapshots_repo.save_snapshot("u1", "2026-01-15", 10_800_000, 10_000_000, 1080.0, 10_000.0, 1405.0)
    await snapshots_repo.save_stock_snapshots("u1", "2026-01-15", [
        {"stock_code": "000660", "market_value": 8_800_000, "quantity": 50, "unit_price": 176_000, "currency": "KRW", "fx_rate": 1.0},
        {"stock_code": "CASH_KRW", "market_value": 2_000_000, "quantity": 2_000_000, "unit_price": 1, "currency": "KRW", "fx_rate": 1.0},
    ])
    await snapshots_repo.save_snapshot("u1", "2026-02-10", 11_500_000, 10_000_000, 1150.0, 10_000.0, 1410.0)
    await snapshots_repo.save_stock_snapshots("u1", "2026-02-10", [
        {"stock_code": "000660", "market_value": 9_500_000, "quantity": 50, "unit_price": 190_000, "currency": "KRW", "fx_rate": 1.0},
        {"stock_code": "CASH_KRW", "market_value": 2_000_000, "quantity": 2_000_000, "unit_price": 1, "currency": "KRW", "fx_rate": 1.0},
    ])

    bars_005930 = _make_bars("2025-12-29", [80_000 + i * 500 for i in range(35)])
    bars_000660 = _make_bars("2025-12-29", [170_000 + i * 600 for i in range(35)])
    sale_date = bars_005930[3]["date"]
    sale_price = bars_005930[3]["close"]
    async with db_repo.transaction() as db:
        # 체결일(date) 없이 기록일(created_at)만 저장된 일반 수동 매매(일부 매도)도 함께 검증
        await db.execute(
            "INSERT INTO portfolio_trades (google_sub, request_id, stock_code, fingerprint, result_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                "u1",
                "req-1",
                "005930",
                "fp-1",
                json.dumps({
                    "stock_code": "005930",
                    "stock_name": "삼성전자",
                    "side": "sell",
                    "currency": "KRW",
                    "quantity": 100,
                    "price": sale_price,
                    "gross_amount": sale_price * 100,
                    "quantity_after": 0,
                    "date": sale_date,
                }),
                f"{sale_date}T02:00:00+00:00",
            ),
        )
        part_date = bars_000660[15]["date"]
        part_price = bars_000660[15]["close"]
        await db.execute(
            "INSERT INTO portfolio_trades (google_sub, request_id, stock_code, fingerprint, result_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                "u1",
                "req-2",
                "000660",
                "fp-2",
                json.dumps({
                    "stock_code": "000660",
                    "stock_name": "SK하이닉스",
                    "side": "sell",
                    "currency": "KRW",
                    "quantity": 5,
                    "price": part_price,
                    "gross_amount": part_price * 5,
                    "quantity_after": 45,
                }),
                f"{part_date}T04:00:00+00:00",
            ),
        )

    app = FastAPI()
    app.include_router(routes.router)

    batch_mock = AsyncMock(return_value={"005930": bars_005930, "000660": bars_000660})
    div_mock = AsyncMock(return_value={
        "005930": {"status": "fresh", "events": []},
        "000660": {"status": "fresh", "events": []},
    })

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        with patch.object(routes, "get_current_user", AsyncMock(return_value=None)):
            unauth = await client.get("/api/portfolio/ghost")
            assert unauth.status_code == 401

        with (
            patch.object(routes, "get_current_user", AsyncMock(return_value={"google_sub": "u1"})),
            patch.object(service.close_price_client, "get_daily_prices_batch", batch_mock),
            patch.object(service.dividend_sources, "get_histories", div_mock),
            patch.object(service, "today_kst_date", return_value=date(2026, 2, 13)),
        ):
            bad = await client.get("/api/portfolio/ghost?since=not-a-date")
            assert bad.status_code == 400
            future = await client.get("/api/portfolio/ghost?since=2099-01-01")
            assert future.status_code == 400

            resp = await client.get("/api/portfolio/ghost")
            assert resp.status_code == 200
            body = resp.json()
            assert body["available"] is True
            assert body["baseline_date"] == "2025-12-30"
            assert body["ghost"]["scope"] == "complete"
            assert body["ghost"]["actual_end_value"] == 11_500_000
            assert body["post_sale"]["summary"]["total_count"] == 2
            by_code = {it["stock_code"]: it for it in body["post_sale"]["items"]}
            assert by_code["005930"]["date_note"] is None
            assert by_code["000660"]["date_is_recorded_at"] is True
            assert by_code["000660"]["date_note"] == "기록일 기준(체결일과 다를 수 있음)"
            assert batch_mock.await_count == 1

            # 더 짧은 기간(since=2026-01-15) 조회 시에도 이미 넓게 받은 일봉 캐시를 잘라 써서 외부 배치를 재호출하지 않는다.
            resp_short = await client.get("/api/portfolio/ghost?since=2026-01-15")
            assert resp_short.status_code == 200
            assert batch_mock.await_count == 1


@pytest.mark.asyncio
async def test_service_marks_foreign_yahoo_failure_as_split_unverified(temp_db):
    """리뷰 항목 2: 해외 종목 Yahoo 차트 실패 시 스냅샷 단가가 있어도 분할 미확인으로 미계산 처리한다."""
    await seed_user("u1")
    await seed_corp_codes([("005930", "00126380", "삼성전자", "2026-01-01")])

    await snapshots_repo.save_snapshot("u1", "2026-01-02", 10_000_000, 10_000_000, 1000.0, 10_000.0, 1400.0)
    await snapshots_repo.save_stock_snapshots("u1", "2026-01-02", [
        {"stock_code": "005930", "market_value": 7_000_000, "quantity": 100, "unit_price": 70_000, "currency": "KRW", "fx_rate": 1.0},
        {"stock_code": "AAPL", "market_value": 3_000_000, "quantity": 10, "unit_price": 300_000, "currency": "USD", "fx_rate": 1400.0},
    ])
    await snapshots_repo.save_snapshot("u1", "2026-03-02", 11_600_000, 10_000_000, 1160.0, 10_000.0, 1400.0)
    await snapshots_repo.save_stock_snapshots("u1", "2026-03-02", [
        {"stock_code": "005930", "market_value": 7_700_000, "quantity": 100, "unit_price": 77_000, "currency": "KRW", "fx_rate": 1.0},
        {"stock_code": "AAPL", "market_value": 3_900_000, "quantity": 10, "unit_price": 390_000, "currency": "USD", "fx_rate": 1400.0},
    ])

    kr_bars = [
        {"date": "2026-01-02", "open": 70_000, "high": 71_000, "low": 69_000, "close": 70_000},
        {"date": "2026-03-02", "open": 77_000, "high": 78_000, "low": 76_000, "close": 77_000},
    ]
    with (
        patch.object(service.close_price_client, "get_daily_prices_batch", AsyncMock(return_value={"005930": kr_bars})),
        patch.object(service.yahoo, "fetch_chart_json", AsyncMock(side_effect=httpx.ConnectError("yahoo down"))),
        patch.object(service.yahoo, "fetch_close_series", AsyncMock(return_value={"rows": [], "currency": None})),
    ):
        report = await service.build_ghost_report("u1", today=date(2026, 3, 3))
        assert report["ghost"]["scope"] == "partial"
        assert report["ghost"]["uncalculated"][0]["stock_code"] == "AAPL"
        assert "분할 여부 확인 전" in report["ghost"]["uncalculated"][0]["reason"]
        # AAPL이 +30%(300만→390만) 올랐지만 매매가 없으므로 계산 범위 한정 비교에서 매매 효과는 0원!
        assert report["ghost"]["effect_krw"] == 0
        assert report["ghost"]["effect_pct_p"] == 0.0


@pytest.mark.asyncio
async def test_default_since_advances_past_legacy_null_quantity_snapshots(temp_db):
    await seed_user("u1")
    await seed_corp_codes([("005930", "00126380", "삼성전자", "2026-01-01")])

    await snapshots_repo.save_snapshot("u1", "2025-12-30", 8_000_000, 8_000_000, 1000.0, 8_000.0, 1400.0)
    await snapshots_repo.save_stock_snapshots("u1", "2025-12-30", [
        {"stock_code": "005930", "market_value": 8_000_000, "quantity": None, "unit_price": None},
    ])
    await snapshots_repo.save_snapshot("u1", "2026-06-30", 9_000_000, 8_000_000, 1125.0, 8_000_000, 1400.0)
    await snapshots_repo.save_stock_snapshots("u1", "2026-06-30", [
        {"stock_code": "005930", "market_value": 9_000_000, "quantity": 100, "unit_price": 90_000, "currency": "KRW", "fx_rate": 1.0},
    ])
    await snapshots_repo.save_snapshot("u1", "2026-09-30", 10_000_000, 8_000_000, 1250.0, 8_000_000, 1400.0)
    await snapshots_repo.save_stock_snapshots("u1", "2026-09-30", [
        {"stock_code": "005930", "market_value": 10_000_000, "quantity": 100, "unit_price": 100_000, "currency": "KRW", "fx_rate": 1.0},
    ])

    bars = [
        {"date": "2026-06-30", "open": 90_000, "high": 91_000, "low": 89_000, "close": 90_000},
        {"date": "2026-09-30", "open": 100_000, "high": 101_000, "low": 99_000, "close": 100_000},
    ]
    with patch.object(service.close_price_client, "get_daily_prices_batch", AsyncMock(return_value={"005930": bars})):
        default_report = await service.build_ghost_report("u1", today=date(2026, 10, 1))
        assert default_report["baseline_date"] == "2026-06-30"
        assert default_report["baseline_mode"] == "first_quantity_recorded"
        assert "2026-06-30" in (default_report["baseline_note"] or "")
        assert default_report["ghost"]["scope"] == "complete"
        assert default_report["ghost"]["effect_krw"] == 0

        explicit_report = await service.build_ghost_report("u1", since="2025-12-30", today=date(2026, 10, 1))
        assert explicit_report["baseline_date"] == "2025-12-30"
        assert explicit_report["ghost"]["scope"] == "uncalculated"
        assert explicit_report["ghost"]["uncalculated"][0]["reason"] == "기준일 보유 수량 미기록"
