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
