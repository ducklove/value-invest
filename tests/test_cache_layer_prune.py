"""MemoryTTLCache — opt-in amortised eviction of expired records."""

from __future__ import annotations

from unittest.mock import patch

import cache_layer
from cache_layer import MemoryTTLCache


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_default_cache_keeps_expired_records_for_stale_reads():
    clock = _Clock()
    cache = MemoryTTLCache("t.keep", 10)
    with patch.object(cache_layer, "_monotonic", clock):
        cache.set("old", {"v": 1})
        clock.now += 60
        for i in range(MemoryTTLCache.PRUNE_EVERY_SETS * 2):
            cache.set(f"k{i}", i)
        assert "old" in cache
        assert cache.get_entry("old", allow_stale=True).value == {"v": 1}


def test_opted_in_cache_prunes_expired_records_every_n_sets():
    clock = _Clock()
    cache = MemoryTTLCache("t.prune", 10, evict_expired_after=0)
    every = MemoryTTLCache.PRUNE_EVERY_SETS
    with patch.object(cache_layer, "_monotonic", clock):
        cache.set("expired", 1)
        cache.set("default_ttl", 2, ttl_seconds=None)  # 기본 TTL(10s) 적용
        cache._data["no_ttl"] = cache._data["default_ttl"].__class__(
            value=3, monotonic_at=clock.now, cached_at="x", expires_at=None, ttl_seconds=None,
        )
        clock.now += 10
        cache.set("fresh", 4, ttl_seconds=100)
        # 아직 N 번째 쓰기 전 — 만료 레코드가 남아 있다(get 은 None).
        assert "expired" in cache and cache.get("expired") is None
        for i in range(every - 4):
            cache.set(f"k{i}", i, ttl_seconds=100)
        assert "expired" in cache
        cache.set("trigger", 0, ttl_seconds=100)  # N 번째 쓰기 → 정리
        assert "expired" not in cache and "default_ttl" not in cache
        assert "no_ttl" in cache and cache.get("fresh") == 4
        assert len(cache.keys()) == (every - 4) + 3  # k* + fresh + trigger + no_ttl


def test_prune_grace_keeps_recently_expired_records():
    clock = _Clock()
    cache = MemoryTTLCache("t.grace", 10, evict_expired_after=30)
    with patch.object(cache_layer, "_monotonic", clock):
        cache.set("a", 1)
        clock.now += 20  # 만료 10초 경과 — 유예(30s) 이내
        assert cache.prune_expired() == 0
        assert cache.get_entry("a", allow_stale=True).value == 1
        clock.now += 20  # 만료 30초 경과
        assert cache.prune_expired() == 1
        assert "a" not in cache


def test_prune_expired_on_demand_for_default_cache():
    clock = _Clock()
    cache = MemoryTTLCache("t.manual", 5)
    with patch.object(cache_layer, "_monotonic", clock):
        cache.set("a", 1)
        cache.set("b", 2, ttl_seconds=50)
        clock.now += 5
        assert cache.prune_expired() == 1
        assert cache.keys() == ["b"]
