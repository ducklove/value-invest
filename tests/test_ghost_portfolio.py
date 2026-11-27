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
    # d0 = 2025-12-30 (6,000,000 = 005930 100주×50,000 + 현금 1,000,000)
    # 2026-06-30(182/364 = 정확히 중간일)에 +4,000,000 입금 정산 → 평균 투입 자본 D = 6,000,000 + 4,000,000 * 0.5 = 8,000,000.
    # 미반영(applied_snapshot_date=None, units_change=None) 입출금은 무시되어야 한다.
    # 종료일(2026-12-29) 005930 종가 60,000 (전량 매도 후 외부 종가로 재평가):
    #   방치 평가액 = 6,000,000(주식) + 1,000,000(기준일 현금) + 4,000,000(순입출금) = 11,000,000 (손익 +1,000,000 → +12.50%)
    #   실제 평가액 = 11,480,000 (손익 +1,480,000 → +18.50%)
    #   매매 효과 = +480,000원, +6.00%p
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


def test_ghost_portfolio_discloses_uncalculated_holdings_and_avoids_fake_all_zero_series():
    nav_points = [
        {"date": "2026-01-02", "total_value": 10_000_000, "fx_usdkrw": 1400.0},
        {"date": "2026-06-01", "total_value": 11_300_000, "fx_usdkrw": 1400.0},
    ]
    baseline_stocks = [
        {"stock_code": "005930", "stock_name": "삼성전자", "quantity": 70, "unit_price": 100_000, "market_value": 7_000_000},
        {"stock_code": "CRYPTO_BTC", "stock_name": "비트코인", "quantity": 0.02, "unit_price": 100_000_000, "market_value": 2_000_000},
        {"stock_code": "CASH_KRW", "stock_name": "원화", "quantity": 1_000_000, "unit_price": 1, "market_value": 1_000_000},
    ]
    # 삼성전자는 100,000 → 110,000(+700,000), 비트코인은 중간 매도 후 일봉 미제공
    res = rules.compute_ghost_portfolio(
        nav_points=nav_points,
        baseline_stocks=baseline_stocks,
        snapshot_prices_by_date={},
        external_krw_closes={"005930": [("2026-01-02", 100_000.0), ("2026-06-01", 110_000.0)]},
        split_flags={},
        cashflows=[],
    )
    assert res["scope"] == "partial"
    assert res["coverage"]["uncalculated_holdings"] == 1
    assert res["coverage"]["uncalculated_weight_pct"] == 20.0
    assert res["coverage"]["calculable_weight_pct"] == 80.0
    assert res["uncalculated"][0]["stock_code"] == "CRYPTO_BTC"
    assert "특수자산" in res["uncalculated"][0]["reason"]
    assert res["ghost_end_value"] == 10_700_000  # 7.7M + 2.0M(미계산 원금 유지) + 1.0M(현금)

    # 모든 위험자산이 미계산이면 가짜 0% 시리즈 대신 미계산(None)으로 반환한다.
    all_uncalc = rules.compute_ghost_portfolio(
        nav_points=nav_points,
        baseline_stocks=[baseline_stocks[1], baseline_stocks[2]],
        snapshot_prices_by_date={},
        external_krw_closes={},
        split_flags={},
        cashflows=[],
    )
    assert all_uncalc["scope"] == "uncalculated"
    assert all_uncalc["ghost_end_value"] is None
    assert all_uncalc["effect_krw"] is None
    assert all_uncalc["effect_pct_p"] is None


def test_detect_krx_series_split_or_mismatch():
    bars_normal = _make_bars("2026-01-02", [50_000, 51_000, 52_000], shares=1_000_000)
    assert rules.detect_krx_series_split_or_mismatch(50_000, "2026-01-02", "2026-01-10", bars_normal) is None
    # 사후 5:1 액면분할로 과거 수정주가가 10,000원으로 내려간 경우
    assert rules.detect_krx_series_split_or_mismatch(50_000, "2026-01-02", "2026-01-10", _make_bars("2026-01-02", [10_000, 10_200])) is not None
    # 무수정 일봉에서 하루 -80% 급락한 경우
    assert rules.detect_krx_series_split_or_mismatch(50_000, "2026-01-02", "2026-01-10", _make_bars("2026-01-02", [50_000, 10_000])) is not None


def test_post_sale_sources_priority_dedup_and_horizons():
    # 25개 거래일 생성: 2026-03-02 시작
    bars_a = _make_bars("2026-03-02", [100_000 - i * 1_000 for i in range(25)])  # 하락 회피 종목
    bars_b = _make_bars("2026-03-02", [50_000 + i * 1_000 for i in range(25)])   # 매도 후 상승 종목
    bars_c = _make_bars("2026-03-02", [30_000 + i * 500 for i in range(25)])     # 스냅샷 소멸 종목

    sale_day_a = bars_a[0]["date"]  # 2026-03-02
    sale_day_b = bars_b[1]["date"]  # 2026-03-03
    prev_day_c = bars_c[2]["date"]  # 2026-03-04
    gone_day_c = bars_c[3]["date"]  # 2026-03-05
    recent_day = bars_a[22]["date"]  # 만기 전(남은 거래일 2일)

    nh_rows = [
        # 같은 날 분할 체결 2건 → 10주 합산, 평균 단가 100,000원
        {"market": "domestic", "trade_date": sale_day_a, "stock_code": "005930", "stock_name": "삼성전자", "currency": "KRW", "sell_quantity": 4, "sell_krw": 396_000, "issues": []},
        {"market": "domestic", "trade_date": sale_day_a, "stock_code": "005930", "stock_name": "삼성전자", "currency": "KRW", "sell_quantity": 6, "sell_krw": 604_000, "issues": []},
        # 상세 미확보 행은 미계산으로 남는다
        {"market": "domestic", "trade_date": "2026-03-31", "stock_code": "035420", "stock_name": "NAVER", "currency": "KRW", "sell_quantity": 5, "sell_krw": 900_000, "issues": ["detail_pending"]},
    ]
    manual_trades = [
        # NH와 같은 날·같은 종목은 중복 제거
        {"side": "sell", "trade_date": sale_day_a, "stock_code": "005930", "stock_name": "삼성전자", "currency": "KRW", "quantity": 10, "price": 100_000, "gross_amount": 1_000_000, "quantity_after": 0},
        # 수동 매매 단독 건 (일부 매도)
        {"side": "sell", "trade_date": sale_day_b, "stock_code": "000660", "stock_name": "SK하이닉스", "currency": "KRW", "quantity": 5, "price": 51_000, "gross_amount": 255_000, "quantity_after": 3},
        # 최근 매도 건 (5·20거래일 미도래)
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
    assert len(events) == 5  # NH 삼성전자 + NH NAVER(미계산) + 수동 SK하이닉스 + 수동 최근 삼성전자 + 스냅샷 소멸 현대차(005380)

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

    by_key = {(it["stock_code"], it["trade_date"]): it for it in post["items"]}
    item_a = by_key[("005930", sale_day_a)]
    assert item_a["source"] == "nh_realized_pnl"
    assert item_a["sell_price"] == 100_000
    assert item_a["day5"]["price"] == 95_000
    assert item_a["day5"]["change_pct"] == -5.0
    assert item_a["day20"]["price"] == 80_000
    assert item_a["day20"]["change_pct"] == -20.0
    assert item_a["day20"]["diff_amount"] == -200_000
    assert item_a["day20"]["outcome"] == "avoided_drop"

    item_b = by_key[("000660", sale_day_b)]
    assert item_b["source"] == "manual_trade"
    assert item_b["sale_type"] == "partial"
    assert item_b["day20"]["outcome"] == "rose_after"

    item_c = by_key[("005380", gone_day_c)]
    assert item_c["source"] == "snapshot_disappearance"
    assert item_c["price_basis"] == "close_estimate"
    assert item_c["sell_price"] == bars_c[3]["close"]

    item_recent = by_key[("005930", recent_day)]
    assert item_recent["day5"]["status"] == "pending"
    assert item_recent["day20"]["status"] == "pending"
    assert item_recent["current"]["status"] == "ok"


def test_post_sale_marks_ex_dividend_split_and_unverified_as_uncalculated():
    bars = _make_bars("2026-04-01", [50_000 + i * 200 for i in range(25)])
    sale_day = bars[0]["date"]
    ex_day_10 = bars[10]["date"]  # +5거래일 이후, +20거래일 이전 배당락

    # 1) 10거래일째 배당락: +5거래일은 계산 가능, +20거래일·현재가는 미계산(배당락 구간)
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
    assert res_ex["summary"]["matured_20d_count"] == 1
    assert res_ex["summary"]["calculable_20d_count"] == 0
    assert res_ex["summary"]["uncalculated_20d_count"] == 1

    # 2) 배당·분할 미확인(verified=False) → 확인 전 미계산
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

    # 3) 매도 체결가가 당일 일봉 범위와 동떨어진 경우(사후 분할 수정주가 불일치) → 전체 미계산
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

    # 정산 스냅샷 2개 + 종목별 스냅샷 + 수동 매도 1건 시드
    await snapshots_repo.save_snapshot("u1", "2025-12-30", 10_000_000, 10_000_000, 1000.0, 10_000.0, 1400.0)
    await snapshots_repo.save_stock_snapshots("u1", "2025-12-30", [
        {"stock_code": "005930", "market_value": 8_000_000, "quantity": 100, "unit_price": 80_000, "currency": "KRW", "fx_rate": 1.0},
        {"stock_code": "CASH_KRW", "market_value": 2_000_000, "quantity": 2_000_000, "unit_price": 1, "currency": "KRW", "fx_rate": 1.0},
    ])
    await snapshots_repo.save_snapshot("u1", "2026-02-10", 11_500_000, 10_000_000, 1150.0, 10_000.0, 1410.0)
    await snapshots_repo.save_stock_snapshots("u1", "2026-02-10", [
        {"stock_code": "000660", "market_value": 9_500_000, "quantity": 50, "unit_price": 190_000, "currency": "KRW", "fx_rate": 1.0},
        {"stock_code": "CASH_KRW", "market_value": 2_000_000, "quantity": 2_000_000, "unit_price": 1, "currency": "KRW", "fx_rate": 1.0},
    ])

    bars_005930 = _make_bars("2025-12-29", [80_000 + i * 500 for i in range(35)])
    sale_date = bars_005930[3]["date"]
    sale_price = bars_005930[3]["close"]
    async with db_repo.transaction() as db:
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

    app = FastAPI()
    app.include_router(routes.router)

    batch_mock = AsyncMock(return_value={"005930": bars_005930})
    div_mock = AsyncMock(return_value={"005930": {"status": "fresh", "events": []}})

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        # 1) 비로그인 → 401
        with patch.object(routes, "get_current_user", AsyncMock(return_value=None)):
            unauth = await client.get("/api/portfolio/ghost")
            assert unauth.status_code == 401

        with (
            patch.object(routes, "get_current_user", AsyncMock(return_value={"google_sub": "u1"})),
            patch.object(service.close_price_client, "get_daily_prices_batch", batch_mock),
            patch.object(service.dividend_sources, "get_histories", div_mock),
        ):
            # 2) 잘못된 날짜 형식 / 미래 날짜 → 400
            bad = await client.get("/api/portfolio/ghost?since=not-a-date")
            assert bad.status_code == 400
            future = await client.get("/api/portfolio/ghost?since=2099-01-01")
            assert future.status_code == 400

            # 3) 기본 조회 (YTD)
            resp = await client.get("/api/portfolio/ghost")
            assert resp.status_code == 200
            body = resp.json()
            assert body["available"] is True
            assert body["baseline_date"] == "2025-12-30"
            assert body["ghost"]["scope"] == "complete"
            assert body["ghost"]["actual_end_value"] == 11_500_000
            assert body["post_sale"]["summary"]["total_count"] == 1
            assert body["post_sale"]["items"][0]["source"] == "manual_trade"
            assert body["post_sale"]["items"][0]["day20"]["status"] == "ok"
            assert batch_mock.await_count == 1

            # 4) 동일 상태 재호출 시 인메모리 캐시 적중 (외부 배치 재호출 없음)
            resp2 = await client.get("/api/portfolio/ghost")
            assert resp2.status_code == 200
            assert batch_mock.await_count == 1


@pytest.mark.asyncio
async def test_default_since_advances_past_legacy_null_quantity_snapshots(temp_db):
    await seed_user("u1")
    await seed_corp_codes([("005930", "00126380", "삼성전자", "2026-01-01")])

    # 연초 스냅샷은 quantity=None(구버전), 2026-06-30부터 quantity 기록 시작
    await snapshots_repo.save_snapshot("u1", "2025-12-30", 8_000_000, 8_000_000, 1000.0, 8_000.0, 1400.0)
    await snapshots_repo.save_stock_snapshots("u1", "2025-12-30", [
        {"stock_code": "005930", "market_value": 8_000_000, "quantity": None, "unit_price": None},
    ])
    await snapshots_repo.save_snapshot("u1", "2026-06-30", 9_000_000, 8_000_000, 1125.0, 8_000.0, 1400.0)
    await snapshots_repo.save_stock_snapshots("u1", "2026-06-30", [
        {"stock_code": "005930", "market_value": 9_000_000, "quantity": 100, "unit_price": 90_000, "currency": "KRW", "fx_rate": 1.0},
    ])
    await snapshots_repo.save_snapshot("u1", "2026-09-30", 10_000_000, 8_000_000, 1250.0, 8_000.0, 1400.0)
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

        # 사용자가 명시적으로 since=2025-12-30을 지정하면 임의 이동 없이 미계산(수량 미기록)으로 투명하게 반환한다.
        explicit_report = await service.build_ghost_report("u1", since="2025-12-30", today=date(2026, 10, 1))
        assert explicit_report["baseline_date"] == "2025-12-30"
        assert explicit_report["ghost"]["scope"] == "uncalculated"
        assert explicit_report["ghost"]["uncalculated"][0]["reason"] == "기준일 보유 수량 미기록"
