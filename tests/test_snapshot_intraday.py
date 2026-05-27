from unittest.mock import AsyncMock, patch

import pytest

import snapshot_intraday


@pytest.mark.asyncio
async def test_fetch_total_value_uses_prior_stock_snapshot_for_non_korean_quote_missing():
    fetch_quote = AsyncMock(return_value={})
    with patch.object(
        snapshot_intraday.cache,
        "get_portfolio",
        new=AsyncMock(return_value=[
            {"stock_code": "AAPL", "quantity": 2, "avg_price": 1000},
        ]),
    ), patch.object(
        snapshot_intraday.cache,
        "get_stock_snapshots_by_date",
        new=AsyncMock(return_value=[
            {"stock_code": "AAPL", "market_value": 1234},
        ]),
    ), patch(
        "routes.portfolio._is_korean_stock",
        return_value=False,
    ), patch(
        "routes.portfolio._fetch_quote",
        new=fetch_quote,
    ):
        total = await snapshot_intraday._fetch_total_value("u1", "2026-05-18")

    fetch_quote.assert_awaited_once_with("AAPL")
    assert total == 1234


@pytest.mark.asyncio
async def test_fetch_total_value_refuses_korean_stock_snapshot_fallback_when_quote_missing():
    fetch_quote = AsyncMock(return_value={})
    with patch.object(
        snapshot_intraday.cache,
        "get_portfolio",
        new=AsyncMock(return_value=[
            {"stock_code": "005930", "quantity": 2, "avg_price": 1000},
        ]),
    ), patch.object(
        snapshot_intraday.cache,
        "get_stock_snapshots_by_date",
        new=AsyncMock(return_value=[
            {"stock_code": "005930", "market_value": 1234},
        ]),
    ), patch(
        "routes.portfolio._is_korean_stock",
        return_value=True,
    ), patch(
        "routes.portfolio._fetch_quote",
        new=fetch_quote,
    ):
        with pytest.raises(snapshot_intraday.IntradaySnapshotIncomplete):
            await snapshot_intraday._fetch_total_value("u1", "2026-05-18")

    fetch_quote.assert_awaited_once_with(
        "005930",
        force_refresh=True,
        use_ws_cache=False,
    )


@pytest.mark.asyncio
async def test_fetch_total_value_ignores_stale_quote_when_snapshot_fallback_exists():
    with patch.object(
        snapshot_intraday.cache,
        "get_portfolio",
        new=AsyncMock(return_value=[
            {"stock_code": "CASH_USD", "quantity": 10, "avg_price": 1400},
        ]),
    ), patch.object(
        snapshot_intraday.cache,
        "get_stock_snapshots_by_date",
        new=AsyncMock(return_value=[
            {"stock_code": "CASH_USD", "market_value": 15190},
        ]),
    ), patch(
        "routes.portfolio._is_korean_stock",
        return_value=False,
    ), patch(
        "routes.portfolio._fetch_quote",
        new=AsyncMock(return_value={"price": 9999, "_stale": True}),
    ):
        total = await snapshot_intraday._fetch_total_value("u1", "2026-05-18")

    assert total == 15190


@pytest.mark.asyncio
async def test_fetch_total_value_refuses_avg_price_fallback_without_snapshot():
    with patch.object(
        snapshot_intraday.cache,
        "get_portfolio",
        new=AsyncMock(return_value=[
            {"stock_code": "005930", "quantity": 2, "avg_price": 1000},
        ]),
    ), patch.object(
        snapshot_intraday.cache,
        "get_stock_snapshots_by_date",
        new=AsyncMock(return_value=[]),
    ), patch(
        "routes.portfolio._is_korean_stock",
        return_value=True,
    ), patch(
        "routes.portfolio._fetch_quote",
        new=AsyncMock(return_value={}),
    ):
        with pytest.raises(snapshot_intraday.IntradaySnapshotIncomplete):
            await snapshot_intraday._fetch_total_value("u1", "2026-05-18")
