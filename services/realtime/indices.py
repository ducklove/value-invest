"""Public index snapshots, polled once per server and pushed over the quote socket.

Toss exposes indices through REST only. Reuse the stock sockets' token; issuing a
separate token would invalidate theirs. Index points never enter the stock cache.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from datetime import datetime

import httpx

from core.http import get_http_client
from domain.timeutil import KST
from services.market import naver_indicators
from services.realtime.toss import BASE, TossError

logger = logging.getLogger(__name__)
CODES = ("KOSPI", "KOSDAQ")
INTERVAL = 5


def stamp(value):
    try:
        at = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return at if at.tzinfo else None
    except (ValueError, TypeError):
        return None


def number(value):
    if value is None or isinstance(value, bool):
        raise ValueError("Missing index value")
    result = float(str(value).replace(",", "").rstrip("%"))
    if not math.isfinite(result):
        raise ValueError("Invalid index value")
    return result


def normalize(row, reference, now):
    if not isinstance(row, dict) or row.get("symbol") not in CODES:
        return None
    at = stamp(row.get("timestamp"))
    if at is None or at > now:
        return None
    try:
        price = number(row.get("lastPrice"))
        if price <= 0:
            return None
    except (ValueError, TypeError):
        return None
    reference = reference if isinstance(reference, dict) else {}
    ref_at = stamp(reference.get("as_of"))
    if ref_at and ref_at > at:
        return dict(reference)  # Never replace a newer reference with an older snapshot.
    result = {"value": f"{price:,.2f}", "change": "", "change_pct": "", "direction": "",
              "as_of": at.isoformat(), "fetched_at": now.isoformat(), "source": "toss_rest",
              "market_status": reference.get("market_status", ""), "delay_minutes": 0}
    # A previous session's daily change cannot serve as today's base.
    if ref_at and ref_at.astimezone(KST).date() == at.astimezone(KST).date() and not reference.get("_stale"):
        try:
            change = abs(number(reference.get("change")))
            sign = {"up": 1, "down": -1}.get(reference.get("direction"), 0)
            if not sign and change:
                raise ValueError("Unknown change direction")
            base = number(reference.get("value")) - sign * change
            if base <= 0:
                raise ValueError("Invalid previous close")
            delta = price - base
            result.update(change=f"{abs(delta):,.2f}", change_pct=f"{abs(delta / base * 100):.2f}%",
                          direction="up" if delta > 0 else "down" if delta < 0 else "")
        except (ValueError, TypeError):
            pass
    return result


class IndexStream:
    def __init__(self, token, publish):
        self.token, self.publish = token, publish
        self.cache, self.reference = {}, {}
        self.updated = self.reference_at = 0.0
        self.retry_at = 0.0
        self.state, self.reason = "idle", None

    def quotes(self, codes):
        if not self.cache or time.monotonic() - self.updated > INTERVAL * 3:
            return {}
        return {code: dict(self.cache[code]) for code in codes if code in self.cache and not self.cache[code].get("_stale")}

    def snapshot(self):
        return {"state": self.state, "mode": "polling", "interval_seconds": INTERVAL,
                "source": "toss_rest" if any(row.get("source") == "toss_rest" for row in self.cache.values()) else "naver_json",
                "reason": self.reason, "codes": list(CODES),
                "as_of": {code: row.get("as_of") for code, row in self.cache.items()}}

    async def _prices(self):
        if self.token is None or time.monotonic() < self.retry_at:
            return []
        access = await self.token.get()
        client = await get_http_client("toss")
        for attempt in range(2):
            response = await client.get(BASE + "/api/v1/market-indicators/prices", params={"symbols": ",".join(CODES)},
                                        headers={"Authorization": "Bearer " + access}, timeout=5)
            if response.status_code == 401 and attempt == 0:
                access = await self.token.get(expired=access)
                continue
            if response.status_code in {403, 429}:
                try:
                    pause = min(300, max(30, float(response.headers.get("Retry-After", 60))))
                except ValueError:
                    pause = 60
                self.retry_at = time.monotonic() + pause
            response.raise_for_status()
            rows = response.json().get("result")
            if not isinstance(rows, list):
                raise ValueError("Invalid index response")
            return rows
        return []

    async def poll_once(self):
        rows = []
        try:
            rows = await self._prices()
        except (httpx.HTTPError, OSError, ValueError, TypeError, AttributeError, TossError):
            pass
        now = datetime.now(KST)
        valid = {row["symbol"] for row in rows if isinstance(row, dict) and normalize(row, {}, now)}
        reference_due = False
        for row in rows:
            if not isinstance(row, dict) or row.get("symbol") not in CODES:
                continue
            at = stamp(row.get("timestamp"))
            ref = self.reference.get(row["symbol"], {})
            ref_at = stamp(ref.get("as_of"))
            reference_due |= bool(at and ((ref_at and (at < ref_at or at.astimezone(KST).date() != ref_at.astimezone(KST).date()))
                                         or (ref.get("market_status") == "OPEN" and (now - at).total_seconds() > 90)))
        if set(CODES) - valid or reference_due or time.monotonic() - self.reference_at >= 60 or not self.reference:
            try:
                client = await get_http_client("market_indicators")
                fresh = await asyncio.wait_for(naver_indicators.fetch_indicators(client, list(CODES)), timeout=10)
                if isinstance(fresh, dict) and fresh:
                    self.reference, self.reference_at = fresh, time.monotonic()
            except (httpx.HTTPError, OSError, ValueError, TypeError, TimeoutError):
                pass
        now = datetime.now(KST)
        parsed = {row["symbol"]: tick for row in rows if isinstance(row, dict) and row.get("symbol") in CODES
                  and (tick := normalize(row, self.reference.get(row["symbol"], {}), now))}
        result = {}
        for code in CODES:
            tick = parsed.get(code)
            if tick is None and time.monotonic() - self.reference_at < 15:
                tick = self.reference.get(code)
            if tick and tick.get("value"):
                tick = {**tick, "update_mode": "polling", "refresh_seconds": INTERVAL}
                at = stamp(tick.get("as_of"))
                if tick.get("market_status") == "OPEN" and (at is None or (now - at).total_seconds() > 90):
                    tick["_stale"] = True
                previous_at = stamp(self.cache.get(code, {}).get("as_of"))
                if at and previous_at and at < previous_at:
                    tick = {**self.cache[code], "_stale": True}
                result[code] = tick
            elif code in self.cache:
                result[code] = {**self.cache[code], "_stale": True}
        self.cache, self.updated = result, time.monotonic()
        primary = any(row.get("source") == "toss_rest" for row in result.values())
        state = "stale" if len(result) < len(CODES) or any(row.get("_stale") for row in result.values()) else "polling" if primary else "fallback"
        reason = "refresh_failed" if state == "stale" else "toss_unavailable" if self.token and state == "fallback" else None
        if (state, reason) != (self.state, self.reason):
            logger.info("Index refresh: state=%s reason=%s", state, reason)
        self.state, self.reason = state, reason
        if result:
            self.publish(self.quotes(CODES) if state != "stale" else result)

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
