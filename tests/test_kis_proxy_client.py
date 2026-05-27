from unittest.mock import AsyncMock, patch

import httpx
import pytest

import kis_proxy_client


class _FakeResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {"ok": True}


class _FakeClient:
    def __init__(self):
        self.calls = []

    async def get(self, url, params=None, headers=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        return _FakeResponse()


class _HTTPErrorResponse:
    def __init__(self, status_code, text=""):
        self.status_code = status_code
        self.text = text
        self.request = httpx.Request("GET", "http://test.local")

    def raise_for_status(self):
        raise httpx.HTTPStatusError("transient", request=self.request, response=self)

    def json(self):
        return {}


class _SequenceClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    async def get(self, url, params=None, headers=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        return self.responses.pop(0)


@pytest.mark.asyncio
async def test_kis_proxy_token_header_is_forwarded_when_configured():
    fake = _FakeClient()
    with patch.object(kis_proxy_client, "PROXY_TOKEN", "secret"), \
         patch.object(kis_proxy_client, "_get_client", new=AsyncMock(return_value=fake)), \
         patch.object(kis_proxy_client, "_acquire_rate_slot", new=AsyncMock()):
        payload = await kis_proxy_client._get("/v1/stocks/005930/quote")

    assert payload == {"ok": True}
    assert fake.calls[0]["headers"] == {"X-KIS-Proxy-Token": "secret"}


@pytest.mark.asyncio
async def test_kis_proxy_retries_transient_http_status_without_body_match():
    fake = _SequenceClient([_HTTPErrorResponse(502, "proxy busy"), _FakeResponse()])
    with patch.object(kis_proxy_client, "_get_client", new=AsyncMock(return_value=fake)), \
         patch.object(kis_proxy_client, "_acquire_rate_slot", new=AsyncMock()), \
         patch.object(kis_proxy_client.asyncio, "sleep", new=AsyncMock()):
        payload = await kis_proxy_client._get("/v1/stocks/005930/quote")

    assert payload == {"ok": True}
    assert len(fake.calls) == 2


@pytest.mark.asyncio
async def test_daily_adjusted_history_uses_local_daily_api_before_kis():
    local_items = [{"stck_bsop_date": "20260430", "stck_clpr": 220500.0}]
    kis = AsyncMock(side_effect=AssertionError("KIS history should not run when local daily API succeeds"))

    with patch.object(kis_proxy_client.close_price_client, "get_daily_price_items", new=AsyncMock(return_value=local_items)), \
         patch.object(kis_proxy_client, "_get", new=kis):
        payload = await kis_proxy_client.get_history("005930", period="D", adjusted=True)

    assert payload == {"items": local_items, "source": "local_daily_price_api"}
    kis.assert_not_awaited()


@pytest.mark.asyncio
async def test_daily_history_falls_back_to_kis_when_local_daily_api_fails():
    kis_payload = {"items": [{"stck_bsop_date": "20260430", "stck_clpr": "220500"}]}

    with patch.object(
        kis_proxy_client.close_price_client,
        "get_daily_price_items",
        new=AsyncMock(side_effect=kis_proxy_client.close_price_client.ClosePriceClientError("local down")),
    ), patch.object(kis_proxy_client, "_get", new=AsyncMock(return_value=kis_payload)):
        payload = await kis_proxy_client.get_history("005930", period="D", adjusted=True)

    assert payload == kis_payload


@pytest.mark.asyncio
async def test_daily_history_falls_back_to_kis_when_local_daily_api_is_empty():
    kis_payload = {"items": [{"stck_bsop_date": "20260430", "stck_clpr": "220500"}]}

    with patch.object(kis_proxy_client.close_price_client, "get_daily_price_items", new=AsyncMock(return_value=[])), \
         patch.object(kis_proxy_client, "_get", new=AsyncMock(return_value=kis_payload)):
        payload = await kis_proxy_client.get_history("005930", period="D", adjusted=True)

    assert payload == kis_payload


@pytest.mark.asyncio
async def test_non_daily_history_stays_on_kis():
    kis_payload = {"items": [{"stck_bsop_date": "20260430", "stck_clpr": "220500"}]}
    local = AsyncMock(side_effect=AssertionError("local daily API is only for adjusted daily history"))

    with patch.object(kis_proxy_client.close_price_client, "get_daily_price_items", new=local), \
         patch.object(kis_proxy_client, "_get", new=AsyncMock(return_value=kis_payload)):
        payload = await kis_proxy_client.get_history("005930", period="W", adjusted=True)

    assert payload == kis_payload
    local.assert_not_awaited()


def test_close_price_rows_are_normalized_to_kis_history_items():
    rows = kis_proxy_client.close_price_client.close_rows_to_kis_items(
        {
            "prices": [
                {"date": "2026-04-30", "close": "220,500"},
                {"date": "20260428", "close": 222000.0},
                {"date": "bad", "close": 1},
            ]
        }
    )

    assert rows == [
        {
            "stck_bsop_date": "20260428",
            "stck_clpr": 222000.0,
            "date": "2026-04-28",
            "close": 222000.0,
            "close_price": 222000.0,
        },
        {
            "stck_bsop_date": "20260430",
            "stck_clpr": 220500.0,
            "date": "2026-04-30",
            "close": 220500.0,
            "close_price": 220500.0,
        },
    ]


def test_daily_price_rows_are_normalized_to_kis_history_items():
    rows = kis_proxy_client.close_price_client.daily_rows_to_kis_items(
        {
            "prices": {
                "005930": [
                    {
                        "date": "2026-04-30",
                        "open": 226000,
                        "high": 227000,
                        "low": 220000,
                        "close": "220,500",
                        "volume": 22161975,
                        "trading_value": 4984706020346,
                    }
                ]
            }
        }
    )

    assert rows == [
        {
            "stck_bsop_date": "20260430",
            "stck_clpr": 220500.0,
            "date": "2026-04-30",
            "close": 220500.0,
            "close_price": 220500.0,
            "stck_oprc": 226000.0,
            "open": 226000.0,
            "stck_hgpr": 227000.0,
            "high": 227000.0,
            "stck_lwpr": 220000.0,
            "low": 220000.0,
            "acml_vol": 22161975.0,
            "volume": 22161975.0,
            "acml_tr_pbmn": 4984706020346.0,
            "trade_value": 4984706020346.0,
            "trading_value": 4984706020346.0,
        },
    ]


@pytest.mark.asyncio
async def test_internal_close_price_requires_explicit_date_range():
    getter = AsyncMock(side_effect=AssertionError("internal close API should not be called without dates"))

    with patch.object(kis_proxy_client.close_price_client, "_get_client", new=getter):
        rows = await kis_proxy_client.close_price_client.get_daily_closes("005930")

    assert rows == []
    getter.assert_not_awaited()
