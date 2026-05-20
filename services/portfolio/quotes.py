from __future__ import annotations

import time
from typing import Any


QUOTE_CACHE_TTL = 60


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


class PortfolioQuoteCache:
    """Fresh quote cache plus non-expiring last-known fallback."""

    def __init__(self, ttl_seconds: float = QUOTE_CACHE_TTL):
        self.ttl_seconds = ttl_seconds
        self._fresh: dict[str, tuple[float, dict[str, Any]]] = {}
        self._last_known: dict[str, dict[str, Any]] = {}

    @property
    def last_known_quotes(self) -> dict[str, dict[str, Any]]:
        return self._last_known

    def get_fresh(self, code: str) -> dict[str, Any] | None:
        cached = self._fresh.get(code)
        if not cached:
            return None
        if (time.monotonic() - cached[0]) >= self.ttl_seconds:
            return None
        return dict(cached[1])

    def get_cached(self, code: str) -> dict[str, Any]:
        cached = self._fresh.get(code)
        if cached:
            quote = dict(cached[1])
            if (time.monotonic() - cached[0]) >= self.ttl_seconds:
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
        snapshot = dict(quote)
        self._fresh[code] = (time.monotonic(), snapshot)
        self._last_known[code] = snapshot
        return True

    def clear(self) -> None:
        self._fresh.clear()
        self._last_known.clear()
