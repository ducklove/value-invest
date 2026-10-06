import asyncio
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from services.brokers import futures_underlyings as master


def row(code, underlying, market="1"):
    record = bytearray(b" " * 96 + b"\n")
    record[:9] = code.encode("ascii")
    record[9:39] = "주식선물 202611 (10)".encode("cp949").ljust(30)
    record[69:75] = underlying.encode("ascii")
    record[95:96] = market.encode("ascii")
    return bytes(record)


@pytest.fixture(autouse=True)
def empty_cache():
    master._cache.clear()
    with patch.object(master, "_lock", asyncio.Lock()):
        yield
    master._cache.clear()


def test_maps_each_maturity_to_official_underlying_code():
    data = row("KA486B000", "006800") + row("KA486C000", "006800") + row("K1236B000", "0001A0", "2") + row("K4566B000", "069500", "3")
    assert master.parse_master(data) == {
        "KA486B000": "006800", "KA486C000": "006800", "K1236B000": "0001A0", "K4566B000": "069500",
    }


@pytest.mark.parametrize("data", [b"", b"<html>error</html>", row("KA486B000", "006800")[:-1],
    row("KA486B000", "006800") * 2, row("KA486B000", "ABCDEF"), row("KA486B000", "006800", "?")])
def test_invalid_master_is_rejected(data):
    with pytest.raises(ValueError):
        master.parse_master(data)


@pytest.mark.asyncio
async def test_download_is_cached_and_shared_between_futures():
    data = b"".join(row(f"K{i:08d}", "006800") for i in range(1000))
    calls = []

    async def response(request):
        calls.append(str(request.url))
        await asyncio.sleep(0)
        return httpx.Response(200, content=data)

    async with httpx.AsyncClient(transport=httpx.MockTransport(response)) as client:
        with patch.object(master, "get_http_client", AsyncMock(return_value=client)):
            first, second = await asyncio.gather(master.underlying_codes(), master.underlying_codes())
            assert first == second == await master.underlying_codes()
    assert len(first) == 1000
    assert calls == [master.MASTER_URL]


@pytest.mark.asyncio
@pytest.mark.parametrize("status, data", [(503, b""), (200, b""), (200, row("KA486B000", "006800"))])
async def test_failed_or_partial_download_is_not_cached_as_mapping(status, data):
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(status, content=data))) as client:
        with patch.object(master, "get_http_client", AsyncMock(return_value=client)) as get_client:
            assert await master.underlying_codes() == {}
            assert await master.underlying_codes() == {}
            get_client.assert_awaited_once()
    assert master._cache.get("codes") is None


@pytest.mark.asyncio
async def test_failed_refresh_keeps_previous_verified_mapping():
    mapping = {"KA486B000": "006800"}
    master._cache.set("codes", mapping, ttl_seconds=-1)
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(503))) as client:
        with patch.object(master, "get_http_client", AsyncMock(return_value=client)):
            assert await master.underlying_codes() == mapping
