from unittest.mock import AsyncMock, patch

import httpx
import pytest

from services.portfolio import fx, quote_service


@pytest.fixture(autouse=True)
def clear_rates():
    fx._fx_cache.clear()
    fx._fx_daily_cache.clear()
    yield
    fx._fx_cache.clear()
    fx._fx_daily_cache.clear()


def payload(code, price="206.88", change="-0.73", ratio="-0.35"):
    return {"exchangeInfo": {"reutersCode": code, "unit": "KRW", "closePrice": price,
        "fluctuations": change, "fluctuationsRatio": ratio, "localTradedAt": "2026-09-18T10:50:53+09:00"}}


@pytest.mark.asyncio
@pytest.mark.parametrize(("currency", "price", "rate"), [("CNY", "206.88", 206.88), ("JPY", "886.58", 8.8658), ("VND", "5.36", .0536)])
async def test_json_rate_preserves_units_for_cash_and_purchase_cost(currency, price, rate):
    calls = []

    def respond(request):
        calls.append(str(request.url))
        return httpx.Response(200, json=payload(f"FX_{currency}KRW", price))

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        with patch.object(fx, "get_http_client", AsyncMock(return_value=client)):
            cash = await quote_service.fetch_cash_quote(f"CASH_{currency}")
            assert cash["price"] == pytest.approx(rate)
            assert cash["change"] < 0
            assert await fx.price_to_krw(10, currency) == pytest.approx(rate * 10)
    assert calls == [f"https://api.stock.naver.com/marketindex/exchange/FX_{currency}KRW"]


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", [payload("FX_USDKRW"), payload("FX_CNYKRW", "NaN"), payload("FX_CNYKRW", "0"), {"exchangeInfo": {}}])
async def test_invalid_json_does_not_become_usable_rate(bad):
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200, json=bad))) as client:
        with patch.object(fx, "get_http_client", AsyncMock(return_value=client)):
            assert await fx.fetch_fx_daily_change("FX_CNYKRW") == {}
            with pytest.raises(fx.FXUnavailableError):
                await fx.fx_rate_for_currency("CNY")


@pytest.mark.asyncio
async def test_json_failure_keeps_last_rate_stale_and_refuses_cost_conversion():
    fx._fx_daily_cache.set("FX_CNYKRW", {"price": 200, "change": 1, "change_pct": .5}, ttl_seconds=-1)
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(503))) as client:
        with patch.object(fx, "get_http_client", AsyncMock(return_value=client)):
            stale = await fx.fetch_fx_daily_change("FX_CNYKRW")
            assert stale["price"] == 200 and stale["_stale"] is True
            with pytest.raises(fx.FXUnavailableError):
                await fx.fx_rate_for_currency("CNY")


# --- X7: 지표 바와 포트폴리오 환산이 같은 USD/KRW 원본을 공유한다 -------------

def _counting_client(calls, body):
    def respond(request):
        calls.append(request.url.path)
        return httpx.Response(200, json=body)
    return httpx.AsyncClient(transport=httpx.MockTransport(respond))


@pytest.mark.asyncio
async def test_exchange_payload_reuses_within_max_age_then_refetches(monkeypatch):
    calls = []
    clock = [1000.0]
    monkeypatch.setattr(fx.time, "monotonic", lambda: clock[0])
    async with _counting_client(calls, payload("FX_USDKRW", "1,391.50", "2.50", "0.18")) as client:
        first, fetched_at = await fx.fetch_exchange_payload("FX_USDKRW", max_age=60, client=client)
        clock[0] += 59
        again, again_at = await fx.fetch_exchange_payload("FX_USDKRW", max_age=60, client=client)
        assert calls == ["/marketindex/exchange/FX_USDKRW"]
        assert again == first and again_at == fetched_at
        clock[0] += 2
        await fx.fetch_exchange_payload("FX_USDKRW", max_age=60, client=client)
        assert len(calls) == 2
        # max_age=0 (포트폴리오 경로)은 항상 새로 받는다 — 자체 300초 캐시가 앞에 있다.
        await fx.fetch_exchange_payload("FX_USDKRW", client=client)
        assert len(calls) == 3


@pytest.mark.asyncio
async def test_dashboard_usd_krw_equals_portfolio_conversion_without_second_fetch():
    from services.market import naver_indicators

    calls = []
    body = payload("FX_USDKRW", "1,391.50", "2.50", "0.18")
    async with _counting_client(calls, body) as client:
        indicators = await naver_indicators.fetch_indicators(client, ["USD_KRW"])
    assert calls == ["/marketindex/exchange/FX_USDKRW"]
    assert indicators["USD_KRW"]["value"] == "1,391.50"

    # 포트폴리오 환산은 지표 바가 받은 같은 응답을 쓴다 — 추가 네트워크 없음.
    failing = AsyncMock(side_effect=AssertionError("no second Naver FX request expected"))
    with patch.object(fx, "get_http_client", failing):
        daily = await fx.fetch_fx_daily_change("FX_USDKRW")
        rate = await fx.fx_rate_for_currency("USD")
    failing.assert_not_awaited()
    assert daily["price"] == 1391.5
    assert f"{rate:,.2f}" == indicators["USD_KRW"]["value"]
    assert f"{daily['change']:,.2f}" == indicators["USD_KRW"]["change"]
    assert fx.cached_rate_for_currency("USD") == pytest.approx(1391.5)


@pytest.mark.asyncio
async def test_invalid_indicator_payload_does_not_seed_portfolio_rate():
    from services.market import naver_indicators

    calls = []
    # 단위가 KRW 가 아니면 지표 바는 표시하되 환산 캐시에는 넣지 않는다.
    body = {"exchangeInfo": {**payload("FX_USDKRW")["exchangeInfo"], "unit": "USD"}}
    async with _counting_client(calls, body) as client:
        await naver_indicators.fetch_indicators(client, ["USD_KRW"])
    assert fx._fx_daily_cache.get("FX_USDKRW") is None
    assert fx.cached_rate_for_currency("USD") is None


@pytest.mark.asyncio
async def test_exchange_payload_returns_copies_not_the_cached_object():
    calls = []
    async with _counting_client(calls, payload("FX_USDKRW", "1,391.50")) as client:
        first, _ = await fx.fetch_exchange_payload("FX_USDKRW", max_age=60, client=client)
        first["exchangeInfo"]["closePrice"] = "0"
        again, _ = await fx.fetch_exchange_payload("FX_USDKRW", max_age=60, client=client)
        again["exchangeInfo"].clear()
        third, _ = await fx.fetch_exchange_payload("FX_USDKRW", max_age=60, client=client)
    assert calls == ["/marketindex/exchange/FX_USDKRW"]
    assert third["exchangeInfo"]["closePrice"] == "1,391.50"
