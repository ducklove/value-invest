from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

import snapshot_nav

ROOT = Path(__file__).resolve().parents[1]


def test_snapshot_nav_does_not_import_portfolio_route_private_helpers():
    source = (ROOT / "snapshot_nav.py").read_text(encoding="utf-8")

    assert "from routes.portfolio import" not in source


@pytest.mark.asyncio
async def test_fetch_total_value_forces_rest_for_korean_stocks():
    today = snapshot_nav._today_kst().isoformat()
    with patch.object(
        snapshot_nav.portfolio_repo,
        "get_portfolio",
        new=AsyncMock(return_value=[
            {"stock_code": "005930", "quantity": 2, "avg_price": 1000, "group_name": "KR"},
        ]),
    ), patch.object(
        snapshot_nav.snapshots_repo,
        "get_stock_snapshots_before_date",
        new=AsyncMock(return_value=[]),
    ), patch.object(
        snapshot_nav.portfolio_quotes,
        "is_korean_stock",
        return_value=True,
    ), patch.object(
        snapshot_nav.portfolio_quotes,
        "fetch_quote",
        new=AsyncMock(return_value={"price": 2000}),
    ) as fetch_quote:
        total_value, total_invested, per_stock = await snapshot_nav._fetch_total_value("u1", today)

    fetch_quote.assert_awaited_once_with(
        "005930",
        force_refresh=True,
        use_ws_cache=False,
    )
    assert total_value == 4000
    assert total_invested == 2000
    assert per_stock == [{
        "stock_code": "005930",
        "market_value": 4000,
        "group_name": "KR",
        "quantity": 2,
        "unit_price": 2000,
        "avg_price_krw": 1000,
        "cost_basis": 2000,
        "priced_from_fallback": False,
    }]


@pytest.mark.asyncio
async def test_fetch_total_value_converts_avg_price_currency_to_krw():
    today = snapshot_nav._today_kst().isoformat()
    with patch.object(
        snapshot_nav.portfolio_repo,
        "get_portfolio",
        new=AsyncMock(return_value=[
            {"stock_code": "AAPL", "quantity": 2, "avg_price": 100, "avg_price_currency": "USD", "group_name": "US"},
        ]),
    ), patch.object(
        snapshot_nav.snapshots_repo,
        "get_stock_snapshots_before_date",
        new=AsyncMock(return_value=[]),
    ), patch.object(
        snapshot_nav.portfolio_quotes,
        "is_korean_stock",
        return_value=False,
    ), patch.object(
        snapshot_nav.portfolio_quotes,
        "fetch_quote",
        new=AsyncMock(return_value={"price": 150000}),
    ), patch.object(
        snapshot_nav.fx,
        "price_to_krw",
        new=AsyncMock(return_value=140000),
    ) as price_to_krw:
        total_value, total_invested, per_stock = await snapshot_nav._fetch_total_value("u1", today)

    price_to_krw.assert_awaited_once_with(100, "USD")
    assert total_value == 300000
    assert total_invested == 280000
    assert per_stock == [{
        "stock_code": "AAPL",
        "market_value": 300000,
        "group_name": "US",
        "quantity": 2,
        "unit_price": 150000,
        "avg_price_krw": 140000,
        "cost_basis": 280000,
        "priced_from_fallback": False,
    }]


@pytest.mark.asyncio
async def test_fetch_total_value_uses_historical_close_for_past_korean_snapshot():
    with patch.object(
        snapshot_nav.portfolio_repo,
        "get_portfolio",
        new=AsyncMock(return_value=[
            {"stock_code": "005930", "quantity": 2, "avg_price": 1000, "group_name": "KR"},
        ]),
    ), patch.object(
        snapshot_nav.snapshots_repo,
        "get_stock_snapshots_before_date",
        new=AsyncMock(return_value=[]),
    ), patch.object(
        snapshot_nav.close_price_client,
        "get_daily_prices",
        new=AsyncMock(return_value=[{"date": "2026-05-18", "close": 1800}]),
    ) as daily_prices, patch.object(
        snapshot_nav.portfolio_quotes,
        "is_korean_stock",
        return_value=True,
    ), patch.object(
        snapshot_nav.portfolio_quotes,
        "fetch_quote",
        new=AsyncMock(return_value={"price": 9999}),
    ) as fetch_quote:
        total_value, _total_invested, per_stock = await snapshot_nav._fetch_total_value("u1", "2026-05-18")

    daily_prices.assert_awaited_once()
    fetch_quote.assert_not_awaited()
    assert total_value == 3600
    assert per_stock[0]["market_value"] == 3600
    assert per_stock[0]["priced_from_fallback"] is False


@pytest.mark.asyncio
async def test_fetch_total_value_uses_prior_date_snapshot_only_as_fallback():
    get_before = AsyncMock(return_value=[{"stock_code": "005930", "market_value": 1234}])
    with patch.object(
        snapshot_nav.portfolio_repo,
        "get_portfolio",
        new=AsyncMock(return_value=[
            {"stock_code": "005930", "quantity": 2, "avg_price": 1000, "group_name": "KR"},
        ]),
    ), patch.object(
        snapshot_nav.snapshots_repo,
        "get_stock_snapshots_before_date",
        new=get_before,
    ), patch.object(
        snapshot_nav.portfolio_quotes,
        "is_korean_stock",
        return_value=True,
    ), patch.object(
        snapshot_nav.close_price_client,
        "get_daily_prices",
        new=AsyncMock(return_value=[]),
    ), patch.object(
        snapshot_nav.kis_proxy_client,
        "get_history",
        new=AsyncMock(return_value={"items": []}),
    ), patch.object(
        snapshot_nav.portfolio_quotes,
        "fetch_quote",
        new=AsyncMock(return_value={"price": 9999, "_stale": True}),
    ):
        total_value, _total_invested, per_stock = await snapshot_nav._fetch_total_value("u1", "2026-05-18")

    get_before.assert_awaited_once_with("u1", "2026-05-18")
    assert total_value == 1234
    assert per_stock[0]["market_value"] == 1234
    assert per_stock[0]["priced_from_fallback"] is True


@pytest.mark.asyncio
async def test_fetch_total_value_refuses_avg_price_fallback_without_snapshot():
    with patch.object(
        snapshot_nav.portfolio_repo,
        "get_portfolio",
        new=AsyncMock(return_value=[
            {"stock_code": "005930", "quantity": 2, "avg_price": 1000, "group_name": "KR"},
        ]),
    ), patch.object(
        snapshot_nav.snapshots_repo,
        "get_stock_snapshots_before_date",
        new=AsyncMock(return_value=[]),
    ), patch.object(
        snapshot_nav.portfolio_quotes,
        "is_korean_stock",
        return_value=True,
    ), patch.object(
        snapshot_nav.close_price_client,
        "get_daily_prices",
        new=AsyncMock(return_value=[]),
    ), patch.object(
        snapshot_nav.kis_proxy_client,
        "get_history",
        new=AsyncMock(return_value={"items": []}),
    ), patch.object(
        snapshot_nav.portfolio_quotes,
        "fetch_quote",
        new=AsyncMock(return_value={}),
    ):
        with pytest.raises(snapshot_nav.SnapshotIncomplete):
            await snapshot_nav._fetch_total_value("u1", "2026-05-18")


@pytest.mark.asyncio
async def test_take_snapshot_rerun_preserves_existing_units():
    with patch.object(
        snapshot_nav,
        "_fetch_total_value",
        new=AsyncMock(return_value=(12000, 8000, [{"stock_code": "005930", "market_value": 12000}])),
    ), patch.object(
        snapshot_nav.snapshots_repo,
        "get_snapshot_by_date",
        new=AsyncMock(return_value={"date": "2026-05-18", "nav": 1000, "total_units": 10}),
    ), patch.object(
        snapshot_nav.snapshots_repo,
        "get_latest_snapshot_before_date",
        new=AsyncMock(),
    ) as get_before, patch.object(
        snapshot_nav.snapshots_repo,
        "get_pending_cashflows",
        new=AsyncMock(),
    ) as get_cashflows, patch.object(
        snapshot_nav.snapshots_repo,
        "save_snapshot",
        new=AsyncMock(),
    ) as save_snapshot, patch.object(
        snapshot_nav.snapshots_repo,
        "save_stock_snapshots",
        new=AsyncMock(),
    ):
        await snapshot_nav.take_snapshot("u1", "2026-05-18")

    get_before.assert_not_awaited()
    get_cashflows.assert_not_awaited()
    save_snapshot.assert_awaited_once_with("u1", "2026-05-18", 12000, 8000, 1200, 10, snapshot_nav._fx_usdkrw)


@pytest.mark.asyncio
async def test_take_snapshot_applies_same_day_cashflow_units_to_nav_denominator():
    fake_db = AsyncMock()
    with patch.object(
        snapshot_nav,
        "_fetch_total_value",
        new=AsyncMock(return_value=(12000, 8000, [{"stock_code": "005930", "market_value": 12000}])),
    ), patch.object(
        snapshot_nav.snapshots_repo,
        "get_snapshot_by_date",
        new=AsyncMock(return_value=None),
    ), patch.object(
        snapshot_nav.snapshots_repo,
        "get_latest_snapshot_before_date",
        new=AsyncMock(return_value={"date": "2026-05-17", "nav": 1000, "total_units": 10}),
    ), patch.object(
        snapshot_nav.snapshots_repo,
        "get_pending_cashflows",
        new=AsyncMock(return_value=[
            {"id": 1, "type": "deposit", "amount": 2000, "units_change": None},
            {"id": 2, "type": "withdrawal", "amount": 1000, "units_change": None},
        ]),
    ), patch.object(
        snapshot_nav.db_repo,
        "get_db",
        new=AsyncMock(return_value=fake_db),
    ), patch.object(
        snapshot_nav.snapshots_repo,
        "save_snapshot",
        new=AsyncMock(),
    ) as save_snapshot, patch.object(
        snapshot_nav.snapshots_repo,
        "save_stock_snapshots",
        new=AsyncMock(),
    ):
        await snapshot_nav.take_snapshot("u1", "2026-05-18")

    assert fake_db.execute.await_count == 2
    fake_db.execute.assert_any_await(
        "UPDATE portfolio_cashflows SET nav_at_time = ?, units_change = ? WHERE id = ?",
        (1000, 2.0, 1),
    )
    fake_db.execute.assert_any_await(
        "UPDATE portfolio_cashflows SET nav_at_time = ?, units_change = ? WHERE id = ?",
        (1000, -1.0, 2),
    )
    save_snapshot.assert_awaited_once_with("u1", "2026-05-18", 12000, 8000, 12000 / 11, 11, snapshot_nav._fx_usdkrw)


@pytest.mark.asyncio
async def test_take_snapshot_returns_fallback_holding_count():
    """정산이 이전 값 폴백으로 채운 종목 수를 반환해 상위 집계가 성공을
    무조건 초록으로 칠하지 않게 한다."""
    per_stock = [
        {"stock_code": "005930", "market_value": 6000, "priced_from_fallback": True},
        {"stock_code": "000660", "market_value": 6000, "priced_from_fallback": False},
    ]
    with patch.object(
        snapshot_nav,
        "_fetch_total_value",
        new=AsyncMock(return_value=(12000, 8000, per_stock)),
    ), patch.object(
        snapshot_nav.snapshots_repo,
        "get_snapshot_by_date",
        new=AsyncMock(return_value={"date": "2026-05-18", "nav": 1000, "total_units": 10}),
    ), patch.object(
        snapshot_nav.snapshots_repo,
        "save_snapshot",
        new=AsyncMock(),
    ), patch.object(
        snapshot_nav.snapshots_repo,
        "save_stock_snapshots",
        new=AsyncMock(),
    ) as save_stock_snapshots:
        fallback_count = await snapshot_nav.take_snapshot("u1", "2026-05-18")

    assert fallback_count == 1
    # 폴백 플래그가 저장 경로까지 그대로 전달돼 신선도 점검이 읽을 수 있어야 한다.
    saved_rows = save_stock_snapshots.await_args.args[2]
    assert [r["priced_from_fallback"] for r in saved_rows] == [True, False]


@pytest.mark.asyncio
async def test_run_all_snapshots_degrades_tick_when_holdings_fall_back():
    """한 종목이라도 이전값 폴백이면 tick_ok 가 아니라 tick_partial/warning 으로
    기록하고, 폴백 사용자/종목 수를 details 에 노출한다."""
    recorded: list[dict] = []

    async def _record(source, kind, **kwargs):
        recorded.append({"source": source, "kind": kind, **kwargs})

    with patch.object(
        snapshot_nav.snapshots_repo,
        "get_all_users_with_portfolio",
        new=AsyncMock(return_value=["userAAAA1111", "userBBBB2222"]),
    ), patch.object(
        snapshot_nav,
        "take_snapshot",
        # 첫 사용자는 종목 2건 폴백, 둘째 사용자는 정상(0).
        new=AsyncMock(side_effect=[2, 0]),
    ), patch.object(
        snapshot_nav,
        "_fetch_fx_usdkrw",
        new=AsyncMock(),
    ), patch.object(
        snapshot_nav,
        "_save_gold_close",
        new=AsyncMock(),
    ), patch.object(
        snapshot_nav,
        "_update_benchmark_history",
        new=AsyncMock(),
    ), patch("observability.record_event", new=_record):
        await snapshot_nav.run_all_snapshots("2026-07-13", manage_db=False)

    tick = next(e for e in recorded if e["source"] == "snapshot_nav" and e["kind"] in ("tick_ok", "tick_partial"))
    assert tick["kind"] == "tick_partial"
    assert tick["level"] == "warning"
    assert tick["details"]["fallback_users"] == ["userAAAA"]
    assert tick["details"]["fallback_holdings"] == 2
    assert tick["details"]["users_failed"] == []


@pytest.mark.asyncio
async def test_run_all_snapshots_reports_tick_ok_when_all_fresh():
    """폴백이 하나도 없으면 종전대로 tick_ok/info."""
    recorded: list[dict] = []

    async def _record(source, kind, **kwargs):
        recorded.append({"source": source, "kind": kind, **kwargs})

    with patch.object(
        snapshot_nav.snapshots_repo,
        "get_all_users_with_portfolio",
        new=AsyncMock(return_value=["userAAAA1111"]),
    ), patch.object(
        snapshot_nav,
        "take_snapshot",
        new=AsyncMock(return_value=0),
    ), patch.object(
        snapshot_nav,
        "_fetch_fx_usdkrw",
        new=AsyncMock(),
    ), patch.object(
        snapshot_nav,
        "_save_gold_close",
        new=AsyncMock(),
    ), patch.object(
        snapshot_nav,
        "_update_benchmark_history",
        new=AsyncMock(),
    ), patch("observability.record_event", new=_record):
        await snapshot_nav.run_all_snapshots("2026-07-13", manage_db=False)

    tick = next(e for e in recorded if e["source"] == "snapshot_nav" and e["kind"] in ("tick_ok", "tick_partial"))
    assert tick["kind"] == "tick_ok"
    assert tick["level"] == "info"
    assert tick["details"]["fallback_holdings"] == 0
