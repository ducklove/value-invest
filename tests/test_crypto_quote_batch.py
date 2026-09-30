"""special_assets.fetch_crypto_quote — Upbit 한 번 호출로 BTC/ETH/USDT 공유."""

import asyncio
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from services.portfolio import special_assets

_ROWS = [
    {"market": "KRW-BTC", "trade_price": 114769000.4, "signed_change_price": -231000.2,
     "signed_change_rate": -0.002008},
    {"market": "KRW-ETH", "trade_price": 3664000.0, "signed_change_price": 12000.0,
     "signed_change_rate": 0.003286},
    {"market": "KRW-USDT", "trade_price": 1361.2, "signed_change_price": 0.0,
     "signed_change_rate": 0.0},
]


@pytest.fixture(autouse=True)
def _clear_cache():
    special_assets._upbit_tickers_cache.clear()
    yield
    special_assets._upbit_tickers_cache.clear()


def _client(handler, calls):
    def wrapped(request):
        calls.append(str(request.url))
        return handler(request)

    return httpx.AsyncClient(transport=httpx.MockTransport(wrapped))


def _run_all(client):
    async def run():
        with patch.object(special_assets, "get_http_client", AsyncMock(return_value=client)):
            return await asyncio.gather(*(
                special_assets.fetch_crypto_quote(code)
                for code in ("CRYPTO_BTC", "CRYPTO_ETH", "CRYPTO_USDT", "NOT_CRYPTO")
            ))

    return asyncio.run(run())


def test_crypto_quotes_share_one_upbit_request():
    calls = []
    client = _client(lambda request: httpx.Response(200, json=_ROWS), calls)
    btc, eth, usdt, other = _run_all(client)
    assert calls == ["https://api.upbit.com/v1/ticker?markets=KRW-BTC,KRW-ETH,KRW-USDT"]
    assert btc == {"price": 114769000, "change": -231000, "change_pct": -0.2}
    assert eth == {"price": 3664000, "change": 12000, "change_pct": 0.33}
    assert usdt == {"price": 1361, "change": 0, "change_pct": 0.0}
    assert other == {}


def test_crypto_quote_error_payload_is_empty_and_not_cached():
    calls = []
    client = _client(lambda request: httpx.Response(404, json={"error": {"name": "x"}}), calls)
    assert _run_all(client)[:3] == [{}, {}, {}]
    ok_client = _client(lambda request: httpx.Response(200, json=_ROWS), calls)
    assert _run_all(ok_client)[0]["price"] == 114769000
    assert len(calls) == 2


def test_crypto_quote_transport_error_is_empty():
    def boom(request):
        raise httpx.ConnectError("down")

    assert _run_all(_client(boom, []))[:3] == [{}, {}, {}]


def test_crypto_quote_missing_market_is_empty():
    client = _client(lambda request: httpx.Response(200, json=_ROWS[:1]), [])
    btc, eth, _usdt, _ = _run_all(client)
    assert btc["price"] == 114769000 and eth == {}
