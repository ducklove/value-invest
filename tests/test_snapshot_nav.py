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
    assert per_stock == [{"stock_code": "005930", "market_value": 4000, "group_name": "KR"}]


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
