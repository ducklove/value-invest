import asyncio
from datetime import datetime, timedelta
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
        if "fetch_quote_snapshot(" in source or "fetch_bulk_quotes_kr(" in source:
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
    # 랭크 보호는 현재 시세가 신선할 때(QUOTE_RANK_PROTECT_SECONDS 이내)의
    # 레이스 방지 장치다 — 직후 도착한 비-stale history 가 REST 를 덮지 못한다.
    rest_quote = {
        "price": 70100,
        "previous_close": 69500,
        "source": "rest",
        "fetched_at": datetime.now().isoformat(),
    }
    history_quote = {
        "price": 69900,
        "previous_close": 69500,
        "source": "history",
        "fetched_at": datetime.now().isoformat(),
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
async def test_get_stock_marks_last_known_fallback_stale_after_fetch_failure():
    stock_quotes._last_known["005930"] = stock_quotes.Stock(
        code="005930",
        current_price=70100,
        previous_close=69500,
        volume=None,
        created_at=datetime(2026, 5, 28, 9, 2),
        source="rest",
    )
    with patch.object(stock_quotes.kis_ws_manager, "ws_cache_matches_rest_market", return_value=False), \
         patch.object(stock_quotes.stock_price, "fetch_quote_snapshot", new=AsyncMock(return_value={})):
        stock = await stock_quotes.get_stock("005930", force_refresh=True, use_ws_cache=False)

    assert stock is not None
    assert stock.current_price == 70100
    assert stock.stale is True
    assert stock_quotes._last_known["005930"].stale is False
    assert stock_quotes.stock_to_quote(stock)["_stale"] is True


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
async def test_get_bulk_quote_snapshots_caches_results_for_single_code_path():
    bulk_quote = {
        "price": 318500,
        "previous_close": 299500,
        "source": "naver",
        "date": "2026-06-11",
        "fetched_at": "2026-06-11T10:00:00",
    }
    with patch.object(
        stock_quotes.stock_price,
        "fetch_bulk_quotes_kr",
        new=AsyncMock(return_value={"005930": bulk_quote}),
    ) as bulk:
        results = await stock_quotes.get_bulk_quote_snapshots(["005930", "005930", " ", "000660"])

    bulk.assert_awaited_once_with(["005930", "000660"])
    assert results["005930"]["price"] == 318500
    assert results["005930"]["change"] == 19000
    # 벌크로 받은 시세는 단건 캐시에도 기록돼 이후 cached 조회와 일관돼야 한다.
    cached = stock_quotes.get_stock_cached("005930", allow_stale=False)
    assert cached is not None and cached.current_price == 318500
    # 업스트림이 해석 못 한 코드는 결과에서 빠진다(호출자가 개별 폴백).
    assert "000660" not in results


@pytest.mark.asyncio
async def test_get_bulk_quote_snapshots_skips_upstream_for_empty_input():
    with patch.object(
        stock_quotes.stock_price,
        "fetch_bulk_quotes_kr",
        new=AsyncMock(),
    ) as bulk:
        assert await stock_quotes.get_bulk_quote_snapshots(["", "  "]) == {}
    bulk.assert_not_awaited()


def _naver_bulk_quote(price: float) -> dict:
    return {
        "price": price,
        "previous_close": 69000,
        "source": "naver",
        "date": datetime.now().date().isoformat(),
        "fetched_at": datetime.now().isoformat(),
    }


@pytest.mark.asyncio
async def test_bulk_refresh_unfreezes_quote_after_ws_ticks_stop():
    # WS 틱이 끊긴 종목: 마지막 ws 시세가 보호 시간을 넘기면 폴링이 가져온
    # 네이버 벌크 시세가 같은 날이라도 받아들여져야 한다 (동결 버그 회귀 방지).
    stock_quotes._last_known["005930"] = stock_quotes.Stock(
        code="005930",
        current_price=70000,
        previous_close=69000,
        volume=None,
        created_at=datetime.now() - timedelta(seconds=120),
        source="ws",
    )
    with patch.object(
        stock_quotes.stock_price,
        "fetch_bulk_quotes_kr",
        new=AsyncMock(return_value={"005930": _naver_bulk_quote(70500)}),
    ):
        results = await stock_quotes.get_bulk_quote_snapshots(["005930"])

    assert results["005930"]["price"] == 70500
    assert stock_quotes._last_known["005930"].current_price == 70500


@pytest.mark.asyncio
async def test_bulk_refresh_keeps_fresh_ws_quote_within_protect_window():
    # 보호 시간 안의 ws 시세는 낮은 랭크 벌크 시세가 덮지 못한다 (레이스 방지).
    stock_quotes._last_known["005930"] = stock_quotes.Stock(
        code="005930",
        current_price=70000,
        previous_close=69000,
        volume=None,
        created_at=datetime.now() - timedelta(seconds=5),
        source="ws",
    )
    with patch.object(
        stock_quotes.stock_price,
        "fetch_bulk_quotes_kr",
        new=AsyncMock(return_value={"005930": _naver_bulk_quote(70500)}),
    ):
        results = await stock_quotes.get_bulk_quote_snapshots(["005930"])

    assert results["005930"]["price"] == 70000
    assert stock_quotes._last_known["005930"].current_price == 70000


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
