import asyncio

import pytest

from services.brokers import namuh_ws


@pytest.mark.asyncio
async def test_two_connections_per_key_cancellation_and_independent_keys():
    release = asyncio.Event()
    opened = asyncio.Queue()

    async def connect(cid):
        async with namuh_ws.slot(cid):
            await opened.put(cid)
            await release.wait()

    first = asyncio.create_task(connect("same-key"))
    second = asyncio.create_task(connect("same-key"))
    third = asyncio.create_task(connect("same-key"))
    other = asyncio.create_task(connect("other-key"))
    try:
        entered = [await asyncio.wait_for(opened.get(), 1) for _ in range(3)]
        assert entered.count("same-key") == 2
        assert entered.count("other-key") == 1
        assert opened.empty()
        third.cancel()
        await asyncio.gather(third, return_exceptions=True)
    finally:
        release.set()
        await asyncio.gather(first, second, third, other, return_exceptions=True)
    assert "same-key" not in namuh_ws._slots
    assert "other-key" not in namuh_ws._slots
