import asyncio
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from services import stock_quotes


ROOT = Path(__file__).resolve().parents[1]


def setup_function():
    stock_quotes._stock_cache.clear()
    stock_quotes._dead_stock_cache.clear()
    stock_quotes._last_known.clear()
    stock_quotes._locks.clear()


def test_runtime_quote_callers_use_stock_quotes_service_boundary():
    allowed = {
        ROOT / "stock_price.py",  # low-level REST/WS implementation
        ROOT / "services" / "stock_quotes.py",  # public current-price service
    }
    offenders = []
    for path in ROOT.rglob("*.py"):
        if any(part in {".venv", ".claude", "__pycache__", "tests"} for part in path.parts):
            continue
        if path in allowed:
            continue
        source = path.read_text(encoding="utf-8")
        if "fetch_quote_snapshot(" in source:
            offenders.append(str(path.relative_to(ROOT)))

    assert offenders == []


@pytest.mark.asyncio
async def test_get_stock_prefers_recent_websocket_cache():
    with patch.object(stock_quotes.kis_ws_manager, "ws_cache_matches_rest_market", return_value=True), \
         patch.object(stock_quotes.kis_ws_manager, "get_cached_quote", return_value={
             "code": "005930",
             "price": 70000,
             "previous_close": 69500,
             "volume": 1234,
             "source": "ws",
             "market": "J",
             "ts": datetime.now().timestamp(),
         }), \
         patch.object(stock_quotes.stock_price, "fetch_quote_snapshot", new=AsyncMock()) as rest:
        stock = await stock_quotes.get_stock("005930")

    rest.assert_not_awaited()
    assert stock is not None
    assert stock.code == "005930"
    assert stock.current_price == 70000
    assert stock.previous_close == 69500
    assert stock.volume == 1234
    assert stock.source == "ws"
    assert stock.market == "J"


@pytest.mark.asyncio
async def test_get_stock_uses_rest_when_websocket_unavailable_and_caches_result():
    rest_quote = {
        "price": 70100,
        "previous_close": 69500,
        "volume": 9876,
        "source": "rest",
        "market": "J",
        "fetched_at": "2026-05-28T09:02:00",
    }
    with patch.object(stock_quotes.kis_ws_manager, "ws_cache_matches_rest_market", return_value=False), \
         patch.object(stock_quotes.stock_price, "fetch_quote_snapshot", new=AsyncMock(return_value=rest_quote)) as rest:
        first = await stock_quotes.get_stock("005930")
        second = await stock_quotes.get_stock("005930")

    rest.assert_awaited_once_with(
        "005930",
        use_ws_cache=True,
        max_ws_age_seconds=stock_quotes.stock_price.WS_QUOTE_MAX_AGE_SECONDS,
    )
    assert first is not None
    assert first.current_price == 70100
    assert first.volume == 9876
    assert second == first


@pytest.mark.asyncio
async def test_get_stock_treats_alphanumeric_krx_etf_as_domestic_rest_quote():
    rest_quote = {
        "price": 21480,
        "previous_close": 21300,
        "source": "rest",
        "market": "J",
        "fetched_at": "2026-05-28T09:02:00",
    }
    with patch.object(stock_quotes.kis_ws_manager, "ws_cache_matches_rest_market", return_value=False), \
         patch.object(stock_quotes.stock_price, "fetch_quote_snapshot", new=AsyncMock(return_value=rest_quote)) as rest:
        stock = await stock_quotes.get_stock("0074K0")

    rest.assert_awaited_once_with(
        "0074K0",
        use_ws_cache=True,
        max_ws_age_seconds=stock_quotes.stock_price.WS_QUOTE_MAX_AGE_SECONDS,
    )
    assert stock is not None
    assert stock.current_price == 21480
    assert stock.market == "J"


@pytest.mark.asyncio
async def test_get_stock_rejects_lower_rank_history_after_rest_quote():
    rest_quote = {
        "price": 70100,
        "previous_close": 69500,
        "source": "rest",
        "fetched_at": "2026-05-28T09:02:00",
    }
    history_quote = {
        "price": 69900,
        "previous_close": 69500,
        "source": "history",
        "fetched_at": "2026-05-28T09:03:00",
    }
    with patch.object(stock_quotes.kis_ws_manager, "ws_cache_matches_rest_market", return_value=False), \
         patch.object(stock_quotes.stock_price, "fetch_quote_snapshot", new=AsyncMock(side_effect=[rest_quote, history_quote])):
        first = await stock_quotes.get_stock("005930", force_refresh=True, use_ws_cache=False)
        second = await stock_quotes.get_stock("005930", force_refresh=True, use_ws_cache=False)

    assert first is not None
    assert second is not None
    assert first.current_price == 70100
    assert second.current_price == 70100


@pytest.mark.asyncio
async def test_get_stock_negative_caches_empty_quote():
    with patch.object(stock_quotes.kis_ws_manager, "ws_cache_matches_rest_market", return_value=False), \
         patch.object(stock_quotes.stock_price, "fetch_quote_snapshot", new=AsyncMock(return_value={})) as rest:
        first = await stock_quotes.get_stock("005930")
        second = await stock_quotes.get_stock("005930")

    assert first is None
    assert second is None
    rest.assert_awaited_once()


@pytest.mark.asyncio
async def test_get_quote_snapshot_derives_change_from_previous_close():
    stock = stock_quotes.Stock(
        code="005930",
        current_price=70100,
        previous_close=69500,
        volume=100,
        created_at=datetime(2026, 5, 28, 9, 2),
        source="rest",
    )
    with patch.object(stock_quotes, "get_stock", new=AsyncMock(return_value=stock)):
        quote = await stock_quotes.get_quote_snapshot("005930")

    assert quote["price"] == 70100
    assert quote["change"] == 600
    assert quote["change_pct"] == 0.86
    assert quote["volume"] == 100


@pytest.mark.asyncio
async def test_get_stock_cont_calls_back_once_immediately_even_without_change():
    stock = stock_quotes.Stock(
        code="005930",
        current_price=70100,
        previous_close=69500,
        volume=100,
        created_at=datetime(2026, 5, 28, 9, 2),
    )
    calls = []
    called = asyncio.Event()

    def callback(value):
        calls.append(value)
        called.set()

    with patch.object(stock_quotes, "get_stock", new=AsyncMock(return_value=stock)):
        sub = stock_quotes.get_stock_cont("005930", callback, interval_seconds=0.2)
        await called.wait()
        await asyncio.sleep(0.25)
        sub.cancel()

    assert calls == [stock]
