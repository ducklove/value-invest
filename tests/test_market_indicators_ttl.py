"""Per-source-group TTLs, single-flight and dead-code removal for market_indicators."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

import cache_layer
from services.market import indicator_health, naver_indicators
from services.market import indicators as mi

QUOTE = {"value": "100.00", "change": "1.00", "change_pct": "1.00%", "direction": "up"}
RATE = {"value": "4.25", "change": "", "change_pct": "", "direction": ""}


class FakeClock:
    def __init__(self, start: float = 10_000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture(autouse=True)
def _clear_caches():
    mi._indicators_cache.clear()
    mi._indicator_item_cache.clear()
    yield
    mi._indicators_cache.clear()
    mi._indicator_item_cache.clear()


@pytest.fixture
def clock():
    fake = FakeClock()
    with patch.object(cache_layer, "_monotonic", fake):
        yield fake


def test_every_catalog_code_has_a_source_group():
    for code in mi.CATALOG:
        assert mi.source_group(code) in mi.SOURCE_GROUP_TTL


def test_source_groups_match_publication_cadence():
    assert mi.source_group("SPX") == "quote"
    assert mi.source_group("USD_KRW") == "quote"
    assert mi.source_group("US5Y") == "quote"  # CNBC intraday yield
    assert mi.source_group("NIGHT_FUTURES") == "quote"
    assert mi.source_group("KOFR") == "daily"  # ECOS
    assert mi.source_group("US_SOFR") == "daily"
    assert mi.source_group("JP1Y") == "daily"  # MOF CSV
    assert mi.source_group("JP_TONA") == "daily"
    assert mi.source_group("US_BASE") == "daily"
    assert mi.source_group("GB_BASE") == "monthly"  # BIS
    assert mi.SOURCE_GROUP_TTL["quote"] == 60
    assert mi.SOURCE_GROUP_TTL["daily"] >= 1800
    assert mi.SOURCE_GROUP_TTL["monthly"] >= 6 * 3600


def test_indicator_health_codes_stay_on_intraday_cadence():
    # The health monitor judges freshness every minute — its codes must not be
    # stretched to a long TTL by the per-group map.
    assert {mi.source_group(code) for code in indicator_health.CODES} == {"quote"}


async def test_policy_rate_fetched_once_while_quote_refetched_each_minute(clock):
    calls = {"naver": 0, "policy": 0}

    async def fake_naver(client, codes):
        calls["naver"] += 1
        return {code: dict(QUOTE) for code in codes}

    async def fake_policy(client, codes):
        calls["policy"] += 1
        return {code: dict(RATE) for code in codes}

    with patch.object(mi.naver_indicators, "fetch_indicators", side_effect=fake_naver), \
         patch.object(mi, "_fetch_policy_rates", side_effect=fake_policy):
        for _ in range(10):
            result = await mi.fetch_indicators(["SPX", "GB_BASE", "US_BASE"])
            assert result["GB_BASE"]["value"] == "4.25"
            assert result["SPX"]["value"] == "100.00"
            clock.advance(61)

    assert calls["policy"] == 1
    assert calls["naver"] == 10


async def test_failed_slow_source_is_retried_at_intraday_cadence(clock):
    calls = 0

    async def failing_policy(client, codes):
        nonlocal calls
        calls += 1
        return {code: dict(mi._EMPTY) for code in codes}

    with patch.object(mi, "_fetch_policy_rates", side_effect=failing_policy):
        await mi.fetch_indicators(["GB_BASE"])
        clock.advance(30)
        await mi.fetch_indicators(["GB_BASE"])
        assert calls == 1
        clock.advance(31)
        await mi.fetch_indicators(["GB_BASE"])
    assert calls == 2


async def test_stale_value_kept_and_retried_at_intraday_cadence(clock):
    responses = [{"GB_BASE": dict(RATE)}, {"GB_BASE": dict(mi._EMPTY)}]

    async def policy(client, codes):
        return responses.pop(0) if responses else {"GB_BASE": {**RATE, "value": "4.00"}}

    with patch.object(mi, "_fetch_policy_rates", side_effect=policy):
        assert (await mi.fetch_indicators(["GB_BASE"]))["GB_BASE"]["value"] == "4.25"
        clock.advance(mi.SOURCE_GROUP_TTL["monthly"] + 1)
        stale = (await mi.fetch_indicators(["GB_BASE"]))["GB_BASE"]
        assert stale["value"] == "4.25" and stale["_stale"] is True
        clock.advance(61)
        recovered = (await mi.fetch_indicators(["GB_BASE"]))["GB_BASE"]
    assert recovered["value"] == "4.00"
    assert "_stale" not in recovered


async def test_batch_cache_expires_with_shortest_member(clock):
    naver_calls = 0

    async def fake_naver(client, codes):
        nonlocal naver_calls
        naver_calls += 1
        return {code: dict(QUOTE) for code in codes}

    async def fake_policy(client, codes):
        return {code: dict(RATE) for code in codes}

    with patch.object(mi.naver_indicators, "fetch_indicators", side_effect=fake_naver), \
         patch.object(mi, "_fetch_policy_rates", side_effect=fake_policy):
        await mi.fetch_indicators(["GB_BASE"])  # item cached for 6 h
        clock.advance(50)
        await mi.fetch_indicators(["SPX"])  # SPX fresh for 60 s from here
        clock.advance(5)
        await mi.fetch_indicators(["GB_BASE", "SPX"])
        assert naver_calls == 1
        clock.advance(56)  # SPX item now 61 s old → batch must not outlive it
        await mi.fetch_indicators(["GB_BASE", "SPX"])
    assert naver_calls == 2


async def test_concurrent_overlapping_requests_fetch_each_code_once():
    calls: list[tuple[str, ...]] = []

    async def fake_naver(client, codes):
        calls.append(tuple(codes))
        await asyncio.sleep(0.02)
        return {code: dict(QUOTE) for code in codes}

    with patch.object(mi.naver_indicators, "fetch_indicators", side_effect=fake_naver):
        a, b, c = await asyncio.gather(
            mi.fetch_indicators(["KOSPI", "SPX"]),
            mi.fetch_indicators(["SPX", "USD_KRW"]),
            mi.fetch_indicators(["KOSPI", "SPX"]),
        )

    fetched = [code for batch in calls for code in batch]
    assert sorted(fetched) == ["KOSPI", "SPX", "USD_KRW"]
    assert a == c == {"KOSPI": QUOTE, "SPX": QUOTE}
    assert b == {"SPX": QUOTE, "USD_KRW": QUOTE}
    assert not mi._code_inflight
    # Callers receive independent copies.
    a["SPX"]["value"] = "mutated"
    assert c["SPX"]["value"] == "100.00"


async def test_live_fetch_single_flight():
    calls = 0

    async def fake_hl(client, codes):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return {code: dict(QUOTE) for code in codes}

    mi._live_cache.clear()
    with patch.object(mi, "_fetch_hyperliquid_tickers", side_effect=fake_hl):
        results = await asyncio.gather(*(mi.fetch_indicators_live(["HL_GOLD"]) for _ in range(5)))
    mi._live_cache.clear()
    assert calls == 1
    assert all(r == {"HL_GOLD": QUOTE} for r in results)


class _Resp:
    def __init__(self, status_code: int, text: str = ""):
        self.status_code = status_code
        self.text = text
        self.content = text.encode("utf-8")


class _Client:
    def __init__(self, resp: _Resp):
        self.resp = resp
        self.urls: list[str] = []

    async def get(self, url, **kwargs):
        self.urls.append(url)
        return self.resp


async def test_us_policy_rate_falls_back_to_fred_via_shared_client():
    fed_page = _Client(_Resp(503))
    fred = _Client(_Resp(200, "observation_date,DFEDTARU\n2026-07-01,4.50\n2026-09-18,4.25\n"))
    requested: list[str] = []

    async def fake_get_http_client(name="default"):
        requested.append(name)
        return fred

    with patch.object(mi, "get_http_client", side_effect=fake_get_http_client):
        out = await mi._fetch_us_policy_rate(fed_page)

    assert requested == ["fred"]
    assert fred.urls == [mi._FRED_DFEDTARU_URL]
    assert out == {"value": "4.25", "change": "0.25", "change_pct": "5.56%", "direction": "down"}


async def test_us_policy_rate_fred_http_error_is_empty():
    fred = _Client(_Resp(500))

    async def fake_get_http_client(name="default"):
        return fred

    with patch.object(mi, "get_http_client", side_effect=fake_get_http_client):
        assert await mi._fetch_us_policy_rate(_Client(_Resp(503))) == mi._EMPTY


def test_legacy_naver_html_scrapers_are_gone_and_unreachable():
    # X9/D-02: every index/FX catalog code is served by naver_indicators (JSON),
    # which fetch_indicators checks first — the PC-HTML scrapers were dead code.
    for code, meta in mi.CATALOG.items():
        if meta["category"] in {"국내 지수", "해외 지수", "환율"}:
            assert code in naver_indicators.CODES, code
    for name in (
        "_fetch_kr_index",
        "_fetch_foreign_index",
        "_fetch_fx_daily",
        "_decode_naver_spans",
        "_parse_marketindex_block",
        "_MARKETINDEX_FX_MAP",
        "_FX_DAILY_MAP",
    ):
        assert not hasattr(mi, name), name
    assert "urllib" not in mi.__dict__
