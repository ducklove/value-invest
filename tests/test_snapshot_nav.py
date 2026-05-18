from unittest.mock import AsyncMock, patch

import pytest

import snapshot_nav


@pytest.mark.asyncio
async def test_fetch_total_value_forces_rest_for_korean_stocks():
    with patch.object(
        snapshot_nav.cache,
        "get_portfolio",
        new=AsyncMock(return_value=[
            {"stock_code": "005930", "quantity": 2, "avg_price": 1000, "group_name": "KR"},
        ]),
    ), patch.object(
        snapshot_nav.cache,
        "get_stock_snapshots_before_date",
        new=AsyncMock(return_value=[]),
    ), patch(
        "routes.portfolio._is_korean_stock",
        return_value=True,
    ), patch(
        "routes.portfolio._fetch_quote",
        new=AsyncMock(return_value={"price": 2000}),
    ) as fetch_quote:
        total_value, total_invested, per_stock = await snapshot_nav._fetch_total_value("u1", "2026-05-18")

    fetch_quote.assert_awaited_once_with(
        "005930",
        force_refresh=True,
        use_ws_cache=False,
    )
    assert total_value == 4000
    assert total_invested == 2000
    assert per_stock == [{"stock_code": "005930", "market_value": 4000, "group_name": "KR"}]


@pytest.mark.asyncio
async def test_fetch_total_value_uses_prior_date_snapshot_only_as_fallback():
    get_before = AsyncMock(return_value=[{"stock_code": "005930", "market_value": 1234}])
    with patch.object(
        snapshot_nav.cache,
        "get_portfolio",
        new=AsyncMock(return_value=[
            {"stock_code": "005930", "quantity": 2, "avg_price": 1000, "group_name": "KR"},
        ]),
    ), patch.object(
        snapshot_nav.cache,
        "get_stock_snapshots_before_date",
        new=get_before,
    ), patch(
        "routes.portfolio._is_korean_stock",
        return_value=True,
    ), patch(
        "routes.portfolio._fetch_quote",
        new=AsyncMock(return_value={}),
    ):
        total_value, _total_invested, per_stock = await snapshot_nav._fetch_total_value("u1", "2026-05-18")

    get_before.assert_awaited_once_with("u1", "2026-05-18")
    assert total_value == 1234
    assert per_stock[0]["market_value"] == 1234
