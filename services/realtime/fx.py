"""Public FX refresh, isolated from index/stock latency and broker connections."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime

import httpx

from core.http import get_http_client
from domain.timeutil import KST
from services.market import naver_indicators
from services.realtime.indices import INTERVAL, number, stamp

logger = logging.getLogger(__name__)
CODES = tuple(naver_indicators.FX)


class FxStream:
    def __init__(self, publish):
        self.publish = publish
        self.cache = {}
        self.updated = 0.0
        self.state = "idle"

    def quotes(self, codes):
        if time.monotonic() - self.updated > INTERVAL * 3:
            return {}
        return {code: dict(self.cache[code]) for code in codes if code in self.cache and not self.cache[code].get("_stale")}

    def snapshot(self):
        return {"state": self.state, "mode": "polling", "interval_seconds": INTERVAL, "source": "naver_json",
                "codes": list(CODES), "as_of": {code: row.get("as_of") for code, row in self.cache.items()}}

    async def poll_once(self):
        try:
            client = await get_http_client("market_indicators")
            fresh = await asyncio.wait_for(naver_indicators.fetch_indicators(client, list(CODES), fx_max_age=1), timeout=10)
        except (httpx.HTTPError, OSError, ValueError, TypeError, TimeoutError):
            fresh = {}
        now = datetime.now(KST)
        result = {}
        for code in CODES:
            row = fresh.get(code)
            valid = False
            if isinstance(row, dict):
                try:
                    at = stamp(row.get("as_of"))
                    valid = number(row.get("value")) > 0 and at is not None and at <= now and not row.get("_stale")
                    before = stamp(self.cache.get(code, {}).get("as_of"))
                    valid = valid and (before is None or at >= before)
                except (ValueError, TypeError):
                    pass
            if valid:
                # Display units (100 JPY / 100 VND) and the provider's timestamp
                # remain intact. Quiet FX feeds must not masquerade as new trades.
                result[code] = {**row, "update_mode": "polling", "refresh_seconds": INTERVAL}
            elif code in self.cache:
                result[code] = {**self.cache[code], "_stale": True}
        self.cache, self.updated = result, time.monotonic()
        state = "polling" if len(result) == len(CODES) and all(not row.get("_stale") for row in result.values()) else "degraded"
        if state != self.state:
            logger.info("FX refresh: state=%s available=%s", state, sum(not row.get("_stale") for row in result.values()))
        self.state = state
        if result:
            self.publish(result)

    async def run(self, stop, active):
        while not stop.is_set():
            started = time.monotonic()
            if active():
                await self.poll_once()
            else:
                self.state = "idle"
            try:
                await asyncio.wait_for(stop.wait(), timeout=max(1, INTERVAL - (time.monotonic() - started)) if active() else 1)
            except TimeoutError:
                pass
