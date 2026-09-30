"""runtime_quotes.fetch_quote_map / kr_trading_day (O10a/X11).

배치 패스(장중 스냅샷)가 공유하는 시세 맵: 국내 벌크 1회 + 결측·해외는 bounded
gather, 휴장일에는 국내 REST 강제 조회를 하지 않는다.
"""

from __future__ import annotations

import asyncio
from datetime import date
from unittest.mock import AsyncMock, patch

import pytest

from services.portfolio import runtime_quotes


class _Provider:
    def __init__(self, *, bulk=None, quotes=None, delay=0.0):
        self.bulk = bulk if bulk is not None else {}
        self.quotes = quotes or {}
        self.delay = delay
        self.bulk_calls: list[list[str]] = []
        self.quote_calls: list[tuple[str, dict]] = []
        self.active = 0
        self.max_active = 0

    async def fetch_quote(self, stock_code, *, force_refresh=False, use_ws_cache=True):
        kwargs = {"force_refresh": force_refresh, "use_ws_cache": use_ws_cache}
        self.quote_calls.append((stock_code, kwargs))
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(self.delay)
            value = self.quotes.get(stock_code, {})
            if isinstance(value, BaseException):
                raise value
            return dict(value)
        finally:
            self.active -= 1

    async def fetch_bulk_kr_quotes(self, stock_codes):
        self.bulk_calls.append(list(stock_codes))
        if isinstance(self.bulk, BaseException):
            raise self.bulk
        return {code: dict(quote) for code, quote in self.bulk.items() if code in stock_codes}


@pytest.fixture
def provider():
    def _install(**kwargs):
        prov = _Provider(**kwargs)
        patcher = patch.object(runtime_quotes, "_provider", prov)
        patcher.start()
        installed.append(patcher)
        return prov

    installed: list = []
    yield _install
    for patcher in installed:
        patcher.stop()


@pytest.mark.asyncio
async def test_quote_map_bulk_fetches_domestic_once_and_dedupes_codes(provider):
    prov = provider(
        bulk={"005930": {"price": 80000.0}, "000660": {"price": 200000.0}},
        quotes={"AAPL": {"price": 300.0}},
    )
    quote_map = await runtime_quotes.fetch_quote_map(["005930", "AAPL", "000660", "005930", "", "AAPL"])

    assert prov.bulk_calls == [["005930", "000660"]]
    assert prov.quote_calls == [("AAPL", {"force_refresh": False, "use_ws_cache": True})]
    assert quote_map == {"005930": {"price": 80000.0}, "000660": {"price": 200000.0}, "AAPL": {"price": 300.0}}


@pytest.mark.asyncio
async def test_quote_map_refetches_stale_or_missing_bulk_codes_with_forced_rest(provider):
    prov = provider(
        bulk={"005930": {"price": 80000.0, "_stale": True}},
        quotes={"005930": {"price": 81000.0}, "000660": {"price": 200000.0}},
    )
    quote_map = await runtime_quotes.fetch_quote_map(["005930", "000660"])

    assert sorted(prov.quote_calls) == [
        ("000660", {"force_refresh": True, "use_ws_cache": False}),
        ("005930", {"force_refresh": True, "use_ws_cache": False}),
    ]
    assert quote_map["005930"] == {"price": 81000.0}


@pytest.mark.asyncio
async def test_quote_map_on_holiday_uses_cached_path_for_domestic_misses(provider):
    prov = provider(bulk={}, quotes={"005930": {"price": 80000.0}, "AAPL": {"price": 300.0}})
    await runtime_quotes.fetch_quote_map(["005930", "AAPL"], force_kr=False)

    assert all(kwargs["force_refresh"] is False for _, kwargs in prov.quote_calls)
    assert sorted(code for code, _ in prov.quote_calls) == ["005930", "AAPL"]


@pytest.mark.asyncio
async def test_quote_map_survives_bulk_and_per_code_failures(provider):
    prov = provider(
        bulk=RuntimeError("naver down"),
        quotes={"005930": RuntimeError("kis down"), "000660": {"price": 200000.0}},
    )
    quote_map = await runtime_quotes.fetch_quote_map(["005930", "000660"])

    assert prov.bulk_calls == [["005930", "000660"]]
    assert quote_map == {"005930": {}, "000660": {"price": 200000.0}}


@pytest.mark.asyncio
async def test_quote_map_bounds_per_code_concurrency(provider):
    codes = [f"TICK{i}" for i in range(10)]
    prov = provider(quotes={code: {"price": 1.0} for code in codes}, delay=0.01)
    quote_map = await runtime_quotes.fetch_quote_map(codes, concurrency=4)

    assert len(quote_map) == 10
    assert prov.bulk_calls == []  # 국내 코드가 없으면 벌크를 치지 않는다
    assert 1 < prov.max_active <= 4


@pytest.mark.asyncio
async def test_bulk_seam_tolerates_provider_without_bulk_support():
    class _Legacy:
        async def fetch_quote(self, stock_code, **_):
            return {}

    with patch.object(runtime_quotes, "_provider", _Legacy()):
        assert await runtime_quotes.fetch_bulk_kr_quotes(["005930"]) == {}
    assert await runtime_quotes.fetch_bulk_kr_quotes([]) == {}


def test_kr_trading_day_follows_market_calendar(monkeypatch):
    monkeypatch.delenv("PORTFOLIO_MARKET_SESSIONS", raising=False)
    assert runtime_quotes.kr_trading_day(date(2026, 9, 30)) is True   # 수요일
    assert runtime_quotes.kr_trading_day(date(2026, 9, 24)) is False  # 추석
    assert runtime_quotes.kr_trading_day(date(2026, 10, 3)) is False  # 토요일
    # 달력이 모르는 날은 거래일로 본다(종전처럼 강제 조회 — 최신가를 놓치지 않는다).
    assert runtime_quotes.kr_trading_day(date(2030, 1, 2)) is True


def test_usable_quote_rejects_stale_and_priceless_quotes():
    assert runtime_quotes.usable_quote({"price": 1.0}) is True
    assert runtime_quotes.usable_quote({"price": 1.0, "_stale": True}) is False
    assert runtime_quotes.usable_quote({"price": ""}) is False
    assert runtime_quotes.usable_quote({}) is False
    assert runtime_quotes.usable_quote(None) is False


@pytest.mark.asyncio
async def test_quote_service_provider_bulk_delegates_to_stock_quotes():
    from services.portfolio import quote_service

    with patch.object(quote_service.stock_quotes, "get_bulk_quote_snapshots",
                      new=AsyncMock(return_value={"005930": {"price": 1.0}})) as bulk:
        result = await quote_service._PortfolioRuntimeQuoteProvider().fetch_bulk_kr_quotes(["005930"])
    bulk.assert_awaited_once_with(["005930"])
    assert result == {"005930": {"price": 1.0}}
