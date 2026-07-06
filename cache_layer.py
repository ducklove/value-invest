from __future__ import annotations

import copy
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable

# DB cache_values 테이블의 네임스페이스 키. leaf 모듈인 여기 두어
# repositories/cache_values.py 와 repositories/analysis.py 가 순환 없이
# 공유한다.
CACHE_NS_LATEST_REPORT = "reports.latest"
CACHE_NS_REPORT_LIST = "reports.list"


def now_iso() -> str:
    """Naive local ISO timestamp, matching the rest of the app's DB rows."""
    return datetime.now().isoformat(timespec="seconds")


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def expires_at_for(cached_at: datetime, ttl_seconds: float | None) -> str | None:
    if ttl_seconds is None:
        return None
    return (cached_at + timedelta(seconds=float(ttl_seconds))).isoformat(timespec="seconds")


@dataclass(frozen=True)
class CachePolicy:
    ttl_seconds: float | None
    allow_stale: bool = False


@dataclass(frozen=True)
class CacheEntry:
    key: str
    value: Any
    cached_at: str
    expires_at: str | None
    ttl_seconds: float | None
    stale: bool

    @property
    def fresh(self) -> bool:
        return not self.stale

    def copy_value(self) -> Any:
        return copy.deepcopy(self.value)

    def with_value_copy(self) -> "CacheEntry":
        return CacheEntry(
            key=self.key,
            value=self.copy_value(),
            cached_at=self.cached_at,
            expires_at=self.expires_at,
            ttl_seconds=self.ttl_seconds,
            stale=self.stale,
        )


@dataclass
class _MemoryRecord:
    value: Any
    monotonic_at: float
    cached_at: str
    expires_at: str | None
    ttl_seconds: float | None


class MemoryTTLCache:
    """Small in-process cache with explicit timestamps and TTL semantics."""

    def __init__(self, namespace: str, default_ttl_seconds: float | None = None):
        self.namespace = namespace
        self.default_ttl_seconds = default_ttl_seconds
        self._data: dict[str, _MemoryRecord] = {}

    def _ttl(self, ttl_seconds: float | None) -> float | None:
        return self.default_ttl_seconds if ttl_seconds is None else ttl_seconds

    def _entry_for(self, key: str, record: _MemoryRecord, *, now: float) -> CacheEntry:
        ttl_seconds = record.ttl_seconds
        stale = False
        if ttl_seconds is not None:
            stale = (now - record.monotonic_at) >= ttl_seconds
        return CacheEntry(
            key=key,
            value=copy.deepcopy(record.value),
            cached_at=record.cached_at,
            expires_at=record.expires_at,
            ttl_seconds=ttl_seconds,
            stale=stale,
        )

    def get_entry(self, key: str, *, allow_stale: bool = False) -> CacheEntry | None:
        record = self._data.get(key)
        if record is None:
            return None
        entry = self._entry_for(key, record, now=time.monotonic())
        if entry.stale and not allow_stale:
            return None
        return entry

    def get(self, key: str, *, allow_stale: bool = False) -> Any | None:
        entry = self.get_entry(key, allow_stale=allow_stale)
        return entry.copy_value() if entry else None

    def set(
        self,
        key: str,
        value: Any,
        *,
        ttl_seconds: float | None = None,
        cached_at: str | None = None,
    ) -> CacheEntry:
        effective_ttl = self._ttl(ttl_seconds)
        cached_at_dt = parse_iso(cached_at) or datetime.now()
        cached_at_iso = cached_at_dt.isoformat(timespec="seconds")
        expires_at = expires_at_for(cached_at_dt, effective_ttl)
        self._data[key] = _MemoryRecord(
            value=copy.deepcopy(value),
            monotonic_at=time.monotonic(),
            cached_at=cached_at_iso,
            expires_at=expires_at,
            ttl_seconds=effective_ttl,
        )
        return self.get_entry(key, allow_stale=True)  # type: ignore[return-value]

    def get_or_set(
        self,
        key: str,
        factory: Callable[[], Any],
        *,
        ttl_seconds: float | None = None,
        allow_stale: bool = False,
    ) -> CacheEntry:
        cached = self.get_entry(key, allow_stale=allow_stale)
        if cached is not None and (allow_stale or cached.fresh):
            return cached
        return self.set(key, factory(), ttl_seconds=ttl_seconds)

    def delete(self, key: str) -> None:
        self._data.pop(key, None)

    def clear(self) -> None:
        self._data.clear()

    def keys(self) -> list[str]:
        return list(self._data.keys())

    def __setitem__(self, key: str, value: Any) -> None:
        """Compatibility path for older tests that seeded raw TTL tuples."""
        if isinstance(value, tuple) and len(value) == 2:
            monotonic_at, payload = value
            try:
                monotonic_at = float(monotonic_at)
            except (TypeError, ValueError):
                self.set(key, payload)
                return
            age = max(0.0, time.monotonic() - monotonic_at)
            cached_at_dt = datetime.now() - timedelta(seconds=age)
            ttl_seconds = self.default_ttl_seconds
            self._data[key] = _MemoryRecord(
                value=copy.deepcopy(payload),
                monotonic_at=monotonic_at,
                cached_at=cached_at_dt.isoformat(timespec="seconds"),
                expires_at=expires_at_for(cached_at_dt, ttl_seconds),
                ttl_seconds=ttl_seconds,
            )
            return
        self.set(key, value)

    def __getitem__(self, key: str) -> Any:
        record = self._data[key]
        return copy.deepcopy(record.value)

    def __contains__(self, key: str) -> bool:
        return key in self._data
