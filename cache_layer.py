from __future__ import annotations

import asyncio
import copy
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable, Hashable

import httpx

from core.errors import AppError

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


_deepcopy = copy.deepcopy


def _monotonic() -> float:
    """Clock for TTL math — a module hook so tests can simulate elapsed time
    without patching the global ``time.monotonic`` the event loop relies on."""
    return time.monotonic()


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
        return _deepcopy(self.value)

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
    """Small in-process cache with explicit timestamps and TTL semantics.

    Copy semantics (X8/O5): every operation deep-copies at most once.

    * ``set()`` stores ``deepcopy(value)`` — later mutation of the caller's
      object never leaks into the cache. The returned :class:`CacheEntry`
      references the caller's *original* object (no second copy).
    * ``get()`` / ``get_entry()`` / ``cache[key]`` return one fresh deep copy,
      so callers may mutate what they receive.
    * ``copy=False`` on reads returns the stored object itself. Only for
      read-only consumers of large payloads — mutating it corrupts the cache.
      ``set(..., copy=False)`` likewise stores the caller's object as-is (the
      caller promises never to mutate it afterwards).

    Expired records are kept by default: ``get_entry(allow_stale=True)`` and
    :func:`cached_fetch`'s ``stale_ttl`` serve them as a fallback. Caches with
    an unbounded key space (per stock code, per query) whose readers never ask
    for stale values opt in with ``evict_expired_after`` — every
    ``PRUNE_EVERY_SETS`` writes, records expired for at least that many
    seconds are dropped (amortised O(1) per write). :meth:`prune_expired`
    runs the same sweep on demand.
    """

    PRUNE_EVERY_SETS = 256

    def __init__(
        self,
        namespace: str,
        default_ttl_seconds: float | None = None,
        *,
        evict_expired_after: float | None = None,
    ):
        self.namespace = namespace
        self.default_ttl_seconds = default_ttl_seconds
        self.evict_expired_after = evict_expired_after
        self._data: dict[str, _MemoryRecord] = {}
        self._flight: SingleFlight | None = None
        self._sets_since_prune = 0

    @property
    def flight(self) -> "SingleFlight":
        """Per-cache in-flight registry used by :func:`cached_fetch`."""
        if self._flight is None:
            self._flight = SingleFlight()
        return self._flight

    def _ttl(self, ttl_seconds: float | None) -> float | None:
        return self.default_ttl_seconds if ttl_seconds is None else ttl_seconds

    def _entry_for(
        self, key: str, record: _MemoryRecord, *, now: float, copy_value: bool = True
    ) -> CacheEntry:
        ttl_seconds = record.ttl_seconds
        stale = False
        if ttl_seconds is not None:
            stale = (now - record.monotonic_at) >= ttl_seconds
        return CacheEntry(
            key=key,
            value=_deepcopy(record.value) if copy_value else record.value,
            cached_at=record.cached_at,
            expires_at=record.expires_at,
            ttl_seconds=ttl_seconds,
            stale=stale,
        )

    def _is_stale(self, record: _MemoryRecord, now: float) -> bool:
        ttl_seconds = record.ttl_seconds
        return ttl_seconds is not None and (now - record.monotonic_at) >= ttl_seconds

    def get_entry(
        self, key: str, *, allow_stale: bool = False, copy: bool = True
    ) -> CacheEntry | None:
        record = self._data.get(key)
        if record is None:
            return None
        now = _monotonic()
        # 만료 판정을 먼저 해 버려질 값은 복사하지 않는다.
        if not allow_stale and self._is_stale(record, now):
            return None
        return self._entry_for(key, record, now=now, copy_value=copy)

    def get(self, key: str, *, allow_stale: bool = False, copy: bool = True) -> Any | None:
        # _entry_for 가 이미 한 번 복사했다 — 두 번째 deepcopy 는 하지 않는다.
        entry = self.get_entry(key, allow_stale=allow_stale, copy=copy)
        return entry.value if entry else None

    def stored_value(self, key: str, default: Any = None) -> Any:
        """The stored object itself (no copy) — ``default`` when absent.

        Internal helper for :func:`cached_fetch`; never hand this object to a
        caller that might mutate it.
        """
        record = self._data.get(key)
        return default if record is None else record.value

    def age_seconds(self, key: str) -> float | None:
        """Seconds since ``key`` was stored (monotonic), or None when absent."""
        record = self._data.get(key)
        if record is None:
            return None
        return max(0.0, _monotonic() - record.monotonic_at)

    def set(
        self,
        key: str,
        value: Any,
        *,
        ttl_seconds: float | None = None,
        cached_at: str | None = None,
        copy: bool = True,
    ) -> CacheEntry:
        effective_ttl = self._ttl(ttl_seconds)
        cached_at_dt = parse_iso(cached_at) or datetime.now()
        cached_at_iso = cached_at_dt.isoformat(timespec="seconds")
        expires_at = expires_at_for(cached_at_dt, effective_ttl)
        record = _MemoryRecord(
            value=_deepcopy(value) if copy else value,
            monotonic_at=_monotonic(),
            cached_at=cached_at_iso,
            expires_at=expires_at,
            ttl_seconds=effective_ttl,
        )
        self._data[key] = record
        self._maybe_prune()
        # 반환 entry 는 호출자 원본을 가리킨다(저장본과 분리돼 있으므로 두 번째
        # 복사가 필요 없다).
        return CacheEntry(
            key=key,
            value=value,
            cached_at=cached_at_iso,
            expires_at=expires_at,
            ttl_seconds=effective_ttl,
            stale=self._is_stale(record, record.monotonic_at),
        )

    def _maybe_prune(self) -> None:
        if self.evict_expired_after is None:
            return
        self._sets_since_prune += 1
        if self._sets_since_prune >= self.PRUNE_EVERY_SETS:
            self.prune_expired()

    def prune_expired(self, grace_seconds: float | None = None) -> int:
        """Drop records expired for at least ``grace_seconds`` (default:
        ``evict_expired_after``, else 0). Records without a TTL are kept.
        Returns the number of records removed."""
        self._sets_since_prune = 0
        if grace_seconds is None:
            grace_seconds = self.evict_expired_after or 0.0
        now = _monotonic()
        doomed = [
            key
            for key, record in self._data.items()
            if record.ttl_seconds is not None
            and (now - record.monotonic_at) >= record.ttl_seconds + grace_seconds
        ]
        for key in doomed:
            del self._data[key]
        return len(doomed)

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
            age = max(0.0, _monotonic() - monotonic_at)
            cached_at_dt = datetime.now() - timedelta(seconds=age)
            ttl_seconds = self.default_ttl_seconds
            self._data[key] = _MemoryRecord(
                value=_deepcopy(payload),
                monotonic_at=monotonic_at,
                cached_at=cached_at_dt.isoformat(timespec="seconds"),
                expires_at=expires_at_for(cached_at_dt, ttl_seconds),
                ttl_seconds=ttl_seconds,
            )
            return
        self.set(key, value)

    def __getitem__(self, key: str) -> Any:
        record = self._data[key]
        return _deepcopy(record.value)

    def __contains__(self, key: str) -> bool:
        return key in self._data


# ---------------------------------------------------------------------------
# Single-flight + stale-while-error fetch helper (X1 / O7)
# ---------------------------------------------------------------------------

# 로더 실패로 간주해 stale 값을 대신 돌려줄 예외들. 광역 Exception 대신 외부
# 호출에서 실제로 나는 유형만 나열한다(core/errors 계층 + httpx + 파싱 오류).
FETCH_ERRORS: tuple[type[BaseException], ...] = (
    AppError,
    httpx.HTTPError,
    OSError,
    TimeoutError,
    ValueError,
    KeyError,
    TypeError,
)


class SingleFlight:
    """One in-flight task per key; concurrent callers await the same result.

    Generalises the per-code lock in ``services/stock_quotes.py``: instead of
    serialising callers (each re-running the factory after the lock), the
    first caller starts one task and every concurrent caller awaits it. The
    task is shielded, so a cancelled caller does not cancel the shared load
    for the others; the result still lands in the cache for later callers.
    """

    def __init__(self) -> None:
        self._inflight: dict[Hashable, asyncio.Task] = {}

    def in_flight(self, key: Hashable) -> bool:
        return key in self._inflight

    def _done(self, key: Hashable, task: asyncio.Task) -> None:
        if self._inflight.get(key) is task:
            del self._inflight[key]
        if not task.cancelled():
            # 모든 호출자가 취소돼 아무도 결과를 읽지 않아도 "Task exception
            # was never retrieved" 경고가 나지 않게 한 번 읽어 둔다.
            task.exception()

    async def run(self, key: Hashable, factory: Callable[[], Awaitable[Any]]) -> tuple[Any, bool]:
        """Run ``factory`` once per key. Returns ``(value, shared)``.

        ``shared`` is True when this caller joined another caller's flight.
        Exceptions raised by the factory propagate to every waiting caller.
        """
        task = self._inflight.get(key)
        shared = task is not None
        if task is None:
            task = asyncio.ensure_future(factory())
            self._inflight[key] = task
            task.add_done_callback(lambda done, k=key: self._done(k, done))
        return await asyncio.shield(task), shared


@dataclass(frozen=True)
class FetchResult:
    """Outcome of :func:`cached_fetch_result`."""

    value: Any
    from_cache: bool = False  # fresh cache hit, loader not called
    stale: bool = False  # loader failed/invalid → last good value served
    shared: bool = False  # joined another caller's in-flight load
    error: BaseException | None = None


_MISSING = object()


def _stale_value(cache: MemoryTTLCache, key: str, stale_ttl: float | None, copy_value: bool) -> Any:
    if stale_ttl is None:
        return _MISSING
    age = cache.age_seconds(key)
    if age is None or age > stale_ttl:
        return _MISSING
    entry = cache.get_entry(key, allow_stale=True, copy=copy_value)
    return _MISSING if entry is None else entry.value


async def cached_fetch_result(
    cache: MemoryTTLCache,
    key: str,
    loader: Callable[[], Awaitable[Any]],
    ttl: float | Callable[[Any], float | None] | None = None,
    *,
    stale_ttl: float | None = None,
    force: bool = False,
    copy: bool = True,
    is_valid: Callable[[Any], bool] | None = None,
    errors: tuple[type[BaseException], ...] = FETCH_ERRORS,
) -> FetchResult:
    """Fresh cache hit → value; otherwise one shared ``loader()`` call per key.

    * **single-flight**: concurrent misses on the same ``key`` share one
      in-flight loader call (followers receive a deep copy when ``copy``).
    * **stale-while-error**: when the loader raises one of ``errors`` — or
      returns a value rejected by ``is_valid`` — the last good value is served
      if it was stored at most ``stale_ttl`` seconds ago. ``stale_ttl=None``
      disables the fallback (errors propagate; invalid values are returned
      as-is, uncached). ``float("inf")`` serves any last good value.
    * ``ttl`` may be a number, ``None`` (the cache default) or a callable that
      derives the TTL from the loaded value.
    * ``force`` skips the fresh-hit check (manual refresh) but still joins an
      in-flight load.
    """
    if not force:
        entry = cache.get_entry(key, copy=copy)
        if entry is not None:
            return FetchResult(entry.value, from_cache=True)

    async def load() -> tuple[Any, Any, bool]:
        """Returns ``(value, pristine, ok)``.

        ``pristine`` is an object no caller ever receives: the cache's own
        stored copy when the value was cached, else the loader's original.
        Followers deep-copy *that*, so a leader mutating the ``value`` it was
        handed (it resumes first) can never leak into a follower's result.
        """
        value = await loader()
        if is_valid is not None and not is_valid(value):
            return value, value, False
        ttl_seconds = ttl(value) if callable(ttl) else ttl
        cache.set(key, value, ttl_seconds=ttl_seconds, copy=copy)
        return value, cache.stored_value(key, value), True

    try:
        (value, pristine, ok), shared = await cache.flight.run(key, load)
    except errors as exc:
        stale = _stale_value(cache, key, stale_ttl, copy)
        if stale is _MISSING:
            raise
        return FetchResult(stale, stale=True, error=exc)
    if not ok:
        stale = _stale_value(cache, key, stale_ttl, copy)
        if stale is not _MISSING:
            return FetchResult(stale, stale=True, shared=shared)
        if copy:
            # 캐시되지 않은 값은 leader 도 복사본을 받아야 원본(pristine)이 보존된다.
            return FetchResult(_deepcopy(pristine), shared=shared)
    if shared and copy:
        value = _deepcopy(pristine)
    return FetchResult(value, shared=shared)


async def cached_fetch(
    cache: MemoryTTLCache,
    key: str,
    loader: Callable[[], Awaitable[Any]],
    ttl: float | Callable[[Any], float | None] | None = None,
    *,
    stale_ttl: float | None = None,
    force: bool = False,
    copy: bool = True,
    is_valid: Callable[[Any], bool] | None = None,
    errors: tuple[type[BaseException], ...] = FETCH_ERRORS,
) -> Any:
    """:func:`cached_fetch_result` returning only the value."""
    result = await cached_fetch_result(
        cache,
        key,
        loader,
        ttl,
        stale_ttl=stale_ttl,
        force=force,
        copy=copy,
        is_valid=is_valid,
        errors=errors,
    )
    return result.value
