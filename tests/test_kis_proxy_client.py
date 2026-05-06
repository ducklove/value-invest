from unittest.mock import AsyncMock, patch

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
async def test_daily_adjusted_history_prefers_internal_close_price_api():
    internal_items = [{"stck_bsop_date": "20260430", "stck_clpr": 220500.0}]
    kis_fallback = AsyncMock(side_effect=AssertionError("KIS fallback should not run"))

    with patch.object(kis_proxy_client.close_price_client, "get_daily_close_items", new=AsyncMock(return_value=internal_items)), \
         patch.object(kis_proxy_client, "_get", new=kis_fallback):
        payload = await kis_proxy_client.get_history("005930", period="D", adjusted=True)

    assert payload == {"items": internal_items, "source": "internal_close_api"}
    kis_fallback.assert_not_awaited()


@pytest.mark.asyncio
async def test_daily_history_falls_back_to_kis_when_internal_close_is_empty():
    kis_fallback = AsyncMock(return_value={"items": [{"stck_bsop_date": "20260430", "stck_clpr": "220500"}]})

    with patch.object(kis_proxy_client.close_price_client, "get_daily_close_items", new=AsyncMock(return_value=[])), \
         patch.object(kis_proxy_client, "_get", new=kis_fallback):
        payload = await kis_proxy_client.get_history("005930", period="D", adjusted=True)

    assert payload["items"][0]["stck_clpr"] == "220500"
    kis_fallback.assert_awaited_once()


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


@pytest.mark.asyncio
async def test_internal_close_price_requires_explicit_date_range():
    getter = AsyncMock(side_effect=AssertionError("internal close API should not be called without dates"))

    with patch.object(kis_proxy_client.close_price_client, "_get_client", new=getter):
        rows = await kis_proxy_client.close_price_client.get_daily_closes("005930")

    assert rows == []
    getter.assert_not_awaited()
