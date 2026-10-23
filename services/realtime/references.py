"""Daily reference hydration without blocking trade delivery or using account keys."""

from __future__ import annotations

import asyncio
import logging
import math
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import httpx

from cache_layer import MemoryTTLCache
from domain.timeutil import KST
from services import stock_quotes
from services.market.quote_policy import trade_timestamp

logger = logging.getLogger(__name__)
NEW_YORK = ZoneInfo("America/New_York")


def context(tick):
    """Toss's US day market starts at 20:00 ET for the next trading day."""
    at = trade_timestamp(tick.get("as_of"))
    if at is None:
        return None
    currency = tick.get("original_currency", tick.get("currency"))
    if currency not in {"KRW", "USD"}:
        return None
    local = datetime.fromtimestamp(at, NEW_YORK if currency == "USD" else KST)
    session = local.date() + timedelta(days=currency == "USD" and local.hour >= 20)
    return tick["code"], currency, session.isoformat()


def positive(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
        return number if math.isfinite(number) and number > 0 else None
    except (TypeError, ValueError):
        return None


class DailyReferences:
    def __init__(self, ready):
        self.ready = ready
        self.cache = MemoryTTLCache("realtime.daily_references", 86400, evict_expired_after=0)
        self.retry = MemoryTTLCache("realtime.reference_retry", 30, evict_expired_after=0)
        self.tasks = {}
        self.semaphore = asyncio.Semaphore(2)
        self.failures = 0

    def enrich(self, tick):
        key = context(tick)
        if key is None:
            return tick
        code, currency, session = key
        reference = self.cache.get(key)
        result = {**tick, "reference_session": session, "reference_date": None, "reference_source": None,
                  "original_previous_close": None, "previous_close": None, "change": None, "change_pct": None}
        if reference is None:
            if key not in self.tasks and not self.retry.get(key):
                self.tasks[key] = asyncio.create_task(self._load(key), name="daily-reference")
            return result
        price = positive(tick.get("original_price") if currency == "USD" else tick.get("price"))
        rate = positive(tick.get("fx_rate")) if currency == "USD" else 1
        base = reference["price"]
        if price is None or rate is None:
            return result
        delta = price - base
        result.update(previous_close=base * rate, change=delta * rate, change_pct=delta / base * 100,
                      reference_date=reference["date"], reference_source=reference["source"])
        if currency == "USD":
            result["original_previous_close"] = base
        return result

    async def _load(self, key):
        code, currency, session = key
        try:
            async with self.semaphore:
                row = await asyncio.wait_for(stock_quotes.get_daily_reference(code, currency, session), timeout=12)
            if not row or positive(row.get("price")) is None or row.get("currency") != currency or row.get("session") != session:
                raise ValueError("Unavailable daily reference")
            self.cache.set(key, row)
            self.ready(code)
        except (httpx.HTTPError, OSError, ValueError, TypeError, TimeoutError) as exc:
            self.failures += 1
            self.retry.set(key, True)
            logger.info("Daily reference deferred: code=%s error=%s", code, type(exc).__name__)
        finally:
            self.tasks.pop(key, None)

    def snapshot(self):
        return {"pending": len(self.tasks), "failures": self.failures, "retry_seconds": 30}

    async def close(self):
        for task in list(self.tasks.values()):
            task.cancel()
        await asyncio.gather(*list(self.tasks.values()), return_exceptions=True)
        self.tasks.clear()
        self.cache.clear()
        self.retry.clear()
