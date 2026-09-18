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
