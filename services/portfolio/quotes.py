from __future__ import annotations

import time
from datetime import datetime
from typing import Any

from cache_layer import MemoryTTLCache


QUOTE_CACHE_TTL = 60

# 같은 거래일 안에서 낮은 랭크 소스(naver/rest)가 높은 랭크(ws) 시세를 덮지
# 못하게 막는 보호 시간. 늦게 도착한 REST 응답이 더 새로운 WS 틱을 되돌리는
# 레이스를 막는 게 목적이므로, 이 시간이 지나면 시각 비교로 더 새로운 시세를
# 받아들인다 — 보호를 무기한으로 두면 WS 틱이 끊긴 종목(거래 한산, WS 연결
# 유실)이 아침 틱 한 번에 그날 내내 동결된다.
#
# 20초인 이유: 레이스의 최대 폭은 REST 응답의 비행 시간(KIS proxy 타임아웃
# 20초)이고, 프론트 일반 폴링이 60초 주기라 55초 미만이면 어떤 값이든 첫
# 폴링에서 동결이 풀린다 — 더 줄여도 회복은 빨라지지 않고 마진만 준다.
QUOTE_RANK_PROTECT_SECONDS = 20.0


def quote_from_ws(
    ws_quote: dict[str, Any] | None,
    *,
    max_age_seconds: float | None = None,
) -> dict[str, Any] | None:
    if not ws_quote or ws_quote.get("price") is None:
        return None
    if max_age_seconds is not None:
        try:
            ts = float(ws_quote["ts"])
        except (KeyError, TypeError, ValueError):
            return None
        if (time.time() - ts) > max_age_seconds:
            return None
    return {
        "date": ws_quote.get("date", ""),
        "price": ws_quote["price"],
        "previous_close": ws_quote.get("previous_close"),
        "change": ws_quote.get("change"),
        "change_pct": ws_quote.get("change_pct"),
        "trade_value": ws_quote.get("trade_value"),
        "source": "ws",
        "ts": ws_quote.get("ts"),
    }


def _quote_date_value(quote: dict[str, Any] | None) -> int | None:
    if not quote:
        return None
    raw = quote.get("date")
    if not raw:
        return None
    text = str(raw)
    try:
        if len(text) == 8 and text.isdigit():
            return datetime(int(text[:4]), int(text[4:6]), int(text[6:8])).date().toordinal()
        return datetime.fromisoformat(text[:10]).date().toordinal()
    except (TypeError, ValueError):
        return None


def _quote_time_value(quote: dict[str, Any] | None) -> float | None:
    if not quote:
        return None
    raw = None
    for key in ("ts", "fetched_at", "fetchedAt", "_receivedAt"):
        value = quote.get(key)
        if value is not None and value != "":
            raw = value
            break
    if raw is None:
        return None
    try:
        numeric = float(raw)
        return numeric * 1000 if numeric < 10_000_000_000 else numeric
    except (TypeError, ValueError):
        pass
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).timestamp() * 1000
    except (TypeError, ValueError):
        return None


def _quote_source_rank(quote: dict[str, Any] | None) -> int:
    if not quote or quote.get("_stale") is True:
        return 0
    source = str(quote.get("source") or quote.get("_source") or "").lower()
    if "ws" in source:
        return 4
    if "rest" in source or "quote" in source:
        return 3
    if "history" in source:
        return 1
    return 2


def _quote_is_recent(quote: dict[str, Any] | None) -> bool:
    timestamp_ms = _quote_time_value(quote)
    if timestamp_ms is None:
        # 시각을 알 수 없으면 보수적으로 "신선"으로 취급해 랭크 보호를 유지.
        return True
    return (time.time() * 1000 - timestamp_ms) < QUOTE_RANK_PROTECT_SECONDS * 1000


def should_accept_quote_snapshot(
    current: dict[str, Any] | None,
    incoming: dict[str, Any] | None,
) -> bool:
    if not incoming or incoming.get("price") is None:
        return False
    if incoming.get("_stale") is True and current and current.get("price") is not None:
        return False

    current_date = _quote_date_value(current)
    incoming_date = _quote_date_value(incoming)
    if current_date is not None and incoming_date is not None:
        if incoming_date < current_date:
            return False
        if incoming_date > current_date:
            return True

    current_rank = _quote_source_rank(current)
    incoming_rank = _quote_source_rank(incoming)
    if incoming_rank > current_rank:
        return True
    # 랭크 강등은 현재 시세가 보호 시간 안에 있을 때만 거부 — 보호가 끝나면
    # 아래 시각 비교로 넘어가 더 새로운 값이면 받아들인다.
    if incoming_rank < current_rank and _quote_is_recent(current):
        return False

    current_time = _quote_time_value(current)
    incoming_time = _quote_time_value(incoming)
    if current_time is not None and incoming_time is not None:
        return incoming_time >= current_time
    return True


class PortfolioQuoteCache:
    """Fresh quote cache plus non-expiring last-known fallback."""

    def __init__(self, ttl_seconds: float = QUOTE_CACHE_TTL):
        self.ttl_seconds = ttl_seconds
        self._fresh = MemoryTTLCache("portfolio.quote", ttl_seconds)
        self._last_known: dict[str, dict[str, Any]] = {}

    @property
    def last_known_quotes(self) -> dict[str, dict[str, Any]]:
        return self._last_known

    def get_fresh(self, code: str) -> dict[str, Any] | None:
        cached = self._fresh.get(code)
        return dict(cached) if cached else None

    def get_cached(self, code: str) -> dict[str, Any]:
        cached = self._fresh.get_entry(code, allow_stale=True)
        if cached:
            quote = dict(cached.value)
            if cached.stale:
                quote["_stale"] = True
            return quote
        return self.get_fallback(code, mark_stale=True)

    def get_fallback(self, code: str, *, mark_stale: bool = False) -> dict[str, Any]:
        quote = self._last_known.get(code)
        if not quote:
            return {}
        result = dict(quote)
        if mark_stale:
            result["_stale"] = True
        return result

    def remember(self, code: str, quote: dict[str, Any] | None) -> bool:
        if not quote or quote.get("price") is None:
            return False
        if quote.get("_stale") is True:
            return False
        if not should_accept_quote_snapshot(self._last_known.get(code), quote):
            return False
        snapshot = dict(quote)
        self._fresh.set(code, snapshot)
        self._last_known[code] = snapshot
        return True

    def clear(self) -> None:
        self._fresh.clear()
        self._last_known.clear()
