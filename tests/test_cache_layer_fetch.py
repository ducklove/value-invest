"""MemoryTTLCache copy semantics (X8/O5) and cached_fetch (X1)."""

from __future__ import annotations

import asyncio
import copy as copy_module
import time
from unittest.mock import patch

import httpx
import pytest

import cache_layer
from cache_layer import FetchResult, MemoryTTLCache, SingleFlight, cached_fetch, cached_fetch_result

# ---------------------------------------------------------------------------
# Copy semantics
# ---------------------------------------------------------------------------


def test_get_returns_isolated_copy():
    cache = MemoryTTLCache("t.copy", 60)
    cache.set("k", {"rows": [1, 2]})
    got = cache.get("k")
    got["rows"].append(3)
    assert cache.get("k") == {"rows": [1, 2]}


def test_mutating_argument_after_set_does_not_leak():
    cache = MemoryTTLCache("t.copy", 60)
    original = {"rows": [1, 2]}
    entry = cache.set("k", original)
    original["rows"].append(99)
    assert cache.get("k") == {"rows": [1, 2]}
    # set() hands back the caller's own object rather than a second copy.
    assert entry.value is original


def test_get_entry_and_getitem_are_isolated():
    cache = MemoryTTLCache("t.copy", 60)
    cache.set("k", {"a": {"b": 1}})
    cache.get_entry("k").value["a"]["b"] = 2
    cache["k"]["a"]["b"] = 3
    assert cache.get("k") == {"a": {"b": 1}}


def test_each_operation_deep_copies_at_most_once():
    cache = MemoryTTLCache("t.copy", 60)
    payload = {"rows": list(range(10))}
    with patch.object(cache_layer, "_deepcopy", wraps=copy_module.deepcopy) as spy:
        cache.set("k", payload)
        assert spy.call_count == 1
        spy.reset_mock()
        cache.get("k")
        assert spy.call_count == 1
        spy.reset_mock()
        cache.get_entry("k")
        assert spy.call_count == 1


def test_copy_false_read_returns_stored_object_without_copy():
    cache = MemoryTTLCache("t.copy", 60)
    cache.set("k", {"big": [1]})
    first = cache.get("k", copy=False)
    second = cache.get("k", copy=False)
    assert first is second
    # A copying read is still independent of the stored object.
    assert cache.get("k") is not first


def test_copy_false_set_stores_caller_object():
    cache = MemoryTTLCache("t.copy", 60)
    frozen = {"x": 1}
    cache.set("k", frozen, copy=False)
    assert cache.get("k", copy=False) is frozen


def test_expired_entry_is_not_copied_for_non_stale_read():
    cache = MemoryTTLCache("t.copy", 0)
    cache.set("k", {"x": 1})
    with patch.object(cache_layer, "_deepcopy") as spy:
        assert cache.get("k") is None
        spy.assert_not_called()


def test_age_seconds():
    cache = MemoryTTLCache("t.age", 60)
    assert cache.age_seconds("missing") is None
    cache["k"] = (time.monotonic() - 30, {"x": 1})
    assert 29 <= cache.age_seconds("k") <= 31


# ---------------------------------------------------------------------------
# SingleFlight / cached_fetch
# ---------------------------------------------------------------------------


async def test_single_flight_shares_one_call():
    flight = SingleFlight()
    calls = 0

    async def factory():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return calls

    results = await asyncio.gather(*(flight.run("k", factory) for _ in range(5)))
    assert calls == 1
    assert [value for value, _ in results] == [1] * 5
    assert sum(1 for _, shared in results if not shared) == 1
    assert not flight.in_flight("k")


async def test_single_flight_propagates_exception_to_all_callers():
    flight = SingleFlight()

    async def factory():
        await asyncio.sleep(0.01)
        raise ValueError("boom")

    results = await asyncio.gather(*(flight.run("k", factory) for _ in range(3)), return_exceptions=True)
    assert all(isinstance(r, ValueError) for r in results)
    assert not flight.in_flight("k")


async def test_single_flight_cancelled_caller_does_not_cancel_shared_load():
    flight = SingleFlight()
    started = asyncio.Event()

    async def factory():
        started.set()
        await asyncio.sleep(0.02)
        return "ok"

    leader = asyncio.create_task(flight.run("k", factory))
    await started.wait()
    follower = asyncio.create_task(flight.run("k", factory))
    await asyncio.sleep(0)
    leader.cancel()
    assert await follower == ("ok", True)


async def test_cached_fetch_ten_concurrent_cold_callers_one_upstream_call():
    cache = MemoryTTLCache("t.fetch", 60)
    calls = 0

    async def loader():
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        return {"rows": [1]}

    values = await asyncio.gather(*(cached_fetch(cache, "k", loader) for _ in range(10)))
    assert calls == 1
    assert all(v == {"rows": [1]} for v in values)
    # Followers get independent copies.
    values[1]["rows"].append(2)
    assert values[2] == {"rows": [1]}
    assert cache.get("k") == {"rows": [1]}

    result = await cached_fetch_result(cache, "k", loader)
    assert result.from_cache and calls == 1


async def test_cached_fetch_serves_stale_on_error_within_stale_ttl():
    cache = MemoryTTLCache("t.fetch", 60)
    cache["k"] = (time.monotonic() - 120, {"v": "old"})

    async def failing():
        raise httpx.ConnectError("down")

    result = await cached_fetch_result(cache, "k", failing, stale_ttl=600)
    assert result == FetchResult({"v": "old"}, stale=True, error=result.error)
    assert isinstance(result.error, httpx.ConnectError)

    # Outside stale_ttl the error propagates.
    with pytest.raises(httpx.ConnectError):
        await cached_fetch(cache, "k", failing, stale_ttl=60)
    # No stale fallback requested → error propagates.
    with pytest.raises(httpx.ConnectError):
        await cached_fetch(cache, "k", failing)


async def test_cached_fetch_unlisted_error_propagates_even_with_stale():
    cache = MemoryTTLCache("t.fetch", 60)
    cache["k"] = (time.monotonic() - 120, {"v": "old"})

    class Unexpected(Exception):
        pass

    async def failing():
        raise Unexpected()

    with pytest.raises(Unexpected):
        await cached_fetch(cache, "k", failing, stale_ttl=600)


async def test_cached_fetch_invalid_value_falls_back_to_stale_and_is_not_cached():
    cache = MemoryTTLCache("t.fetch", 60)
    cache["k"] = (time.monotonic() - 120, ["good"])

    async def empty():
        return []

    result = await cached_fetch_result(cache, "k", empty, stale_ttl=600, is_valid=bool)
    assert result.stale and result.value == ["good"]
    # Not cached: the old record is still the stale one.
    assert cache.get("k") is None

    fresh_cache = MemoryTTLCache("t.fetch2", 60)
    assert await cached_fetch(fresh_cache, "k", empty, stale_ttl=600, is_valid=bool) == []
    assert fresh_cache.get("k") is None


async def test_cached_fetch_ttl_callable_and_force():
    cache = MemoryTTLCache("t.fetch", 60)
    n = 0

    async def loader():
        nonlocal n
        n += 1
        return {"n": n}

    await cached_fetch(cache, "k", loader, ttl=lambda value: 3600 if value["n"] == 1 else 1)
    assert cache.get_entry("k").ttl_seconds == 3600
    assert (await cached_fetch(cache, "k", loader)) == {"n": 1}
    assert (await cached_fetch(cache, "k", loader, force=True)) == {"n": 2}
    assert n == 2


async def test_leader_mutating_its_result_does_not_leak_into_followers():
    """The leader resumes first; mutating what it got must not reach followers."""
    cache = MemoryTTLCache("t.fetch", 60)
    release = asyncio.Event()

    async def loader():
        await release.wait()
        return {"rows": [1]}

    async def mutating_caller():
        value = await cached_fetch(cache, "k", loader)
        value["rows"].append("leader-mutation")  # synchronous, before followers resume
        return value

    leader = asyncio.create_task(mutating_caller())
    await asyncio.sleep(0)
    followers = [asyncio.create_task(cached_fetch(cache, "k", loader)) for _ in range(3)]
    await asyncio.sleep(0)
    release.set()
    await leader
    assert [await f for f in followers] == [{"rows": [1]}] * 3
    assert cache.get("k") == {"rows": [1]}


async def test_uncached_invalid_value_is_isolated_between_concurrent_callers():
    cache = MemoryTTLCache("t.fetch", 60)
    release = asyncio.Event()

    async def loader():
        await release.wait()
        return []

    async def mutating_caller():
        value = await cached_fetch(cache, "k", loader, is_valid=bool)
        value.append("leader-mutation")
        return value

    leader = asyncio.create_task(mutating_caller())
    await asyncio.sleep(0)
    follower = asyncio.create_task(cached_fetch(cache, "k", loader, is_valid=bool))
    await asyncio.sleep(0)
    release.set()
    await leader
    assert await follower == []
