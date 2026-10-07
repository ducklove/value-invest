"""Single-process market-data owner: demand, allocation, cache and browser delivery.

Account workers remain responsible for credentials and account synchronization.
NH prices/notices keep their existing shared socket; accepted prices are published here.
"""

from __future__ import annotations

import asyncio
import fcntl
import logging
import math
import os
import time
from dataclasses import dataclass, field
from pathlib import Path

import aiosqlite

import kis_key_manager
from repositories.db import get_db
from services.market.quote_policy import trade_timestamp
from services.portfolio.quotes import should_accept_quote_snapshot
from services.realtime.allocation import PRIORITIES, allocate, fair_codes, sanitize
from services.realtime.kis import KisSource
from services.realtime.toss import Token, TossSource

logger = logging.getLogger(__name__)
FRESH_SECONDS = 90


@dataclass(eq=False)
class Client:
    user: str
    requested: dict = field(default_factory=dict)
    pending: dict = field(default_factory=dict)
    wake: asyncio.Event = field(default_factory=asyncio.Event)
    plan: dict = field(default_factory=dict)

    def put(self, message):
        # Prices coalesce per symbol. A slow browser never blocks the upstream socket.
        key = ("quote", message["code"]) if message.get("type") == "quote" else message["type"]
        self.pending[key] = message
        if len(self.pending) > 1004:
            self.pending.pop(next(iter(self.pending)))
        self.wake.set()

    async def next(self):
        while not self.pending:
            self.wake.clear()
            await self.wake.wait()
        key = next(iter(self.pending))
        return self.pending.pop(key)


class QuoteHub:
    def __init__(self):
        self.sources = []
        self.clients: set[Client] = set()
        self.holdings = {}
        self.assignments = {}
        self.cache = {}
        self.running = False
        self.reason = "starting"
        self._lock_file = None
        self._wake = asyncio.Event()
        self._logged_states = {}

    def attach(self, user):
        client = Client(user)
        self.clients.add(client)
        return client

    def detach(self, client):
        self.clients.discard(client)
        self._wake.set()

    def subscribe(self, client, requested):
        client.requested = sanitize(requested)
        wanted = {code for codes in client.requested.values() for code in codes}
        for key in list(client.pending):
            if isinstance(key, tuple) and key[1] not in wanted:
                client.pending.pop(key)
        self._wake.set()
        for code in wanted:
            quote = self.quote(client.user, code)
            if quote:
                client.put(quote)

    def quote(self, user, code):
        candidates = [self.cache.get((None, code)), self.cache.get((user, code))]
        current = None
        for tick in candidates:
            at = trade_timestamp((tick or {}).get("as_of"))
            if tick and at is not None and 0 <= time.time() - at < FRESH_SECONDS:
                if should_accept_quote_snapshot(current, tick):
                    current = tick
        return dict(current) if current else None

    def publish(self, quote, user=None):
        code = quote.get("code")
        at = trade_timestamp(quote.get("as_of"))
        try:
            price = float(quote.get("price"))
        except (ValueError, TypeError):
            return
        if not code or not math.isfinite(price) or price <= 0 or at is None or not 0 <= time.time() - at < FRESH_SECONDS:
            return
        # Preserve the REST base-price metadata absent from Toss's trade protocol.
        from services import stock_quotes
        current = stock_quotes.get_stock_cached(code)
        tick = {"type": "quote", **quote}
        if tick.get("previous_close") is None and current and current.previous_close:
            tick["previous_close"] = current.previous_close
            tick["change"] = price - current.previous_close
            tick["change_pct"] = tick["change"] / current.previous_close * 100
        key = (user, code)
        if not should_accept_quote_snapshot(self.cache.get(key), tick):
            return
        self.cache[key] = tick
        if user is None:
            stock_quotes.remember_quote(code, tick)
        for client in self.clients:
            if user is not None and client.user != user:
                continue
            if any(code in codes for codes in client.requested.values()):
                accepted = self.quote(client.user, code)
                if accepted:
                    client.put(accepted)

    def _demands(self):
        demands = {user: {group: list(codes) for group, codes in requested.items()}
                   for user, requested in self.holdings.items()}
        for client in self.clients:
            target = demands.setdefault(client.user, {})
            for group, codes in client.requested.items():
                target[group] = list(dict.fromkeys(target.get(group, []) + codes))
        # One symbol may occur in multiple groups/tabs; its highest priority wins.
        return {user: sanitize(requested) for user, requested in demands.items()}

    @staticmethod
    def _nh_coverage(user):
        from services.brokers import realtime
        return realtime.approved_codes(user)

    async def reconcile(self):
        demands = self._demands()
        shared = {}
        for user, requested in demands.items():
            covered = self._nh_coverage(user)
            shared[user] = {group: [code for code in codes if code not in covered] for group, codes in requested.items()}
        self.assignments = allocate(fair_codes(shared), self.sources, self.assignments)
        for source in self.sources:
            codes = [code for code, owner in self.assignments.items() if owner == source.id]
            try:
                await source.set_codes(codes)
            except (OSError, TimeoutError, ValueError) as exc:
                logger.warning("Realtime subscription update deferred: source=%s error=%s", source.id, type(exc).__name__)
            snapshot = source.snapshot()
            signature = tuple(snapshot.get(key) for key in ("state", "requested", "subscribed", "rejected", "reason"))
            if self._logged_states.get(source.id) != signature:
                self._logged_states[source.id] = signature
                logger.info("Realtime source: id=%s state=%s requested=%s subscribed=%s rejected=%s reason=%s",
                            source.id, *signature)
        approved = {code for source in self.sources for code in source.approved}
        for client in self.clients:
            wanted = list(dict.fromkeys(code for group in PRIORITIES for code in client.requested.get(group, [])))
            live = approved | self._nh_coverage(client.user)
            plan = {"type": "subscriptions", "ws": [code for code in wanted if code in live],
                    "rest": [code for code in wanted if code not in live],
                    "pending": [code for code in wanted if code in self.assignments and code not in live], "shared": True}
            if plan != client.plan:
                client.plan = plan
                client.put(plan)
            client.put(self.status(client))
        wanted = set(self.assignments) | {code for request in demands.values() for codes in request.values() for code in codes}
        now = time.time()
        for key, tick in list(self.cache.items()):
            at = trade_timestamp(tick.get("as_of"))
            if key[1] not in wanted or at is None or now - at >= FRESH_SECONDS:
                self.cache.pop(key)

    def status(self, client=None):
        sources = [source.snapshot() for source in self.sources]
        from services.brokers import realtime
        demands = self._demands() if client is None else {client.user: client.requested}
        for index, user in enumerate(sorted(demands)):
            nh = realtime.status(user)
            for name in ("domestic", "foreign"):
                if name in nh:
                    sources.append({"id": f"namuh:{index}:{name}", "provider": "namuh", **nh[name]})
        active = [row for row in sources if row.get("requested") or row.get("reserved")]
        connected = sum(row["state"] in {"connected", "subscribed", "live"} for row in active)
        wanted = list(dict.fromkeys(code for request in demands.values() for codes in request.values() for code in codes))
        approved = set(client.plan.get("ws", [])) if client else {code for source in self.sources for code in source.approved}
        if client is None:
            approved |= set().union(*(self._nh_coverage(user) for user in demands))
        if client:
            fresh = sum(self.quote(client.user, code) is not None for code in wanted)
        else:
            recent = {tick["code"] for tick in self.cache.values() if (at := trade_timestamp(tick.get("as_of")))
                      and 0 <= time.time() - at < FRESH_SECONDS}
            fresh = len(set(wanted) & recent)
        return {"type": "stream_status", "shared": True, "active": bool(client), "can_takeover": False,
                "stream_state": "connected" if approved else "connecting" if active and self.running else "offline",
                "slots_active": len(active), "slots_connected": connected, "slots_total": len(sources),
                "requested": len(wanted), "subscribed": len(set(wanted) & approved), "receiving": fresh,
                "fallback": len(set(wanted) - approved), "sources": sources,
                "disconnected_codes": [code for code in wanted if code not in approved],
                "reason": self.reason}

    async def _load_holdings(self):
        db = await get_db()
        rows = await (await db.execute("SELECT google_sub,stock_code,benchmark_code,pair_long_code FROM user_portfolio ORDER BY google_sub,stock_code")).fetchall()
        demands = {}
        for row in rows:
            request = demands.setdefault(row["google_sub"], {"portfolio": [], "benchmark": []})
            request["portfolio"].append(row["stock_code"])
            if row["pair_long_code"]:
                request["portfolio"].append(row["pair_long_code"])
            if row["benchmark_code"]:
                request["benchmark"].append(row["benchmark_code"])
        self.holdings = {user: sanitize(requested) for user, requested in demands.items()}

    async def run(self, stop):
        from repositories.db import DB_PATH
        lock_path = Path(DB_PATH).parent / "data" / "realtime-owner.lock"
        lock_path.parent.mkdir(exist_ok=True)
        self._lock_file = lock_path.open("a")
        try:
            fcntl.flock(self._lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.reason = "owner_conflict"
            self._lock_file.close()
            self._lock_file = None
            logger.error("Realtime connections already owned by another process")
            return
        self.sources = [KisSource(slot, self.publish) for slot in kis_key_manager.all_slots()]
        if os.environ.get("TOSS_CLIENT_ID") and os.environ.get("TOSS_CLIENT_SECRET"):
            token = Token()
            self.sources += [TossSource(index, token, self.publish) for index in range(2)]
        self.running, self.reason = True, None
        last_holdings = 0.0
        try:
            while not stop.is_set():
                self._wake.clear()
                try:
                    if time.monotonic() - last_holdings >= 15:
                        await self._load_holdings()
                        last_holdings = time.monotonic()
                    await self.reconcile()
                except (aiosqlite.Error, OSError, ValueError) as exc:
                    logger.warning("Realtime reconciliation deferred: error=%s", type(exc).__name__)
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=1)
                except TimeoutError:
                    pass
        finally:
            self.running = False
            await asyncio.gather(*(source.close() for source in self.sources), return_exceptions=True)
            self.sources.clear()
            self.assignments.clear()
            self.cache.clear()
            self._lock_file.close()
            self._lock_file = None

    async def watch_kis_account(self, user, cid, credential, changed):
        if not self.running:
            return False
        for source in self.sources:
            if source.provider == "kis" and source.slot.app_key == credential["app_key"]:
                await source.watch_account(user, cid, credential["hts_id"], changed)
                return True
        return False


_hub = QuoteHub()


def get_hub():
    return _hub
