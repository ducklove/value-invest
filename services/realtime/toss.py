"""Official Toss price channels only. One shared token, at most two connections.

Protocol: https://openapi.tossinvest.com/openapi-docs/latest/asyncapi.json
No account or order endpoints are used by this adapter.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import re
import ssl
import time
from datetime import datetime

import httpx
import truststore
import websockets

from core.http import get_http_client
from domain.portfolio_codes import is_korean_stock, is_plain_us_ticker, yahoo_symbol
from domain.timeutil import KST
from services.brokers.overseas_realtime import to_won
from services.portfolio.identifiers import static_foreign_ticker

logger = logging.getLogger(__name__)
BASE = "https://openapi.tossinvest.com"
WS = "wss://openapi-ws.tossinvest.com/ws/v1"


def instrument(code: str) -> tuple[str, str] | None:
    if is_korean_stock(code):
        return "kr", code
    alias = static_foreign_ticker(code)
    resolved = alias["ticker"] if alias else code
    if is_plain_us_ticker(resolved):
        # The provider's master/ACK is authoritative for class-share spelling.
        return "us", yahoo_symbol(resolved).replace("-", ".")
    return None


def normalize(message: object, code: str, now: datetime | None = None) -> dict | None:
    pair = instrument(code)
    if not isinstance(message, dict) or not pair or message.get("type") != "message":
        return None
    if message.get("topic") != f"trade:{pair[0]}:{pair[1]}":
        return None
    data = message.get("data")
    if not isinstance(data, dict) or isinstance(data.get("price"), bool):
        return None
    try:
        at = datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00"))
        if at.tzinfo is None:
            return None
        now = now or datetime.now(KST)
        price = float(data["price"])
        if not 0 < price < 1e12 or not 0 <= (now - at).total_seconds() < 90:
            return None
        currency = "KRW" if pair[0] == "kr" else "USD"
        if data.get("currency") != currency:
            return None
        # volume is the individual trade size, never cumulative daily volume.
        return {"type": "quote", "code": code, "price": price, "currency": currency,
                "source": "toss_ws", "market": "UN" if currency == "KRW" else "US",
                "date": at.astimezone(KST).date().isoformat(), "as_of": at.isoformat(),
                "received_at": now.isoformat(), "ts": now.timestamp()}
    except (KeyError, ValueError, TypeError, OverflowError):
        return None


class TossError(Exception):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


class Token:
    def __init__(self):
        self._value = ""
        self._expires = 0.0
        self._lock = asyncio.Lock()
        self._retry_at = 0.0
        self._error = None

    async def get(self, *, expired: str | None = None) -> str:
        async with self._lock:
            if self._value and self._value != expired and time.monotonic() < self._expires:
                return self._value
            if time.monotonic() < self._retry_at:
                raise TossError(self._error)
            client = await get_http_client("toss")
            response = await client.post(BASE + "/oauth2/token", data={
                "grant_type": "client_credentials", "client_id": os.environ.get("TOSS_CLIENT_ID", ""),
                "client_secret": os.environ.get("TOSS_CLIENT_SECRET", "")}, timeout=15)
            if response.status_code != 200:
                self._error = {401: "authentication_error", 403: "ip_not_allowed", 429: "rate_limited"}.get(response.status_code, "token_error")
                self._retry_at = time.monotonic() + (60 if response.status_code in {401, 403, 429} else 5)
                raise TossError(self._error)
            try:
                data = response.json()
                value, duration = data["access_token"], float(data["expires_in"])
                if not isinstance(value, str) or not value or not math.isfinite(duration) or duration <= 60:
                    raise ValueError
            except (ValueError, KeyError, TypeError):
                raise TossError("token_response_invalid") from None
            self._value, self._expires = value, time.monotonic() + duration - 60
            self._retry_at, self._error = 0, None
            return value


class TossSource:
    provider = "toss"
    capacity = 100

    def __init__(self, index: int, token: Token, publish):
        self.id = f"toss:{index}"
        self.token, self.publish = token, publish
        self.codes: list[str] = []
        self.approved: set[str] = set()
        self.rejected: dict[str, str] = {}
        self.state = "idle"
        self.reason = None
        self.last_frame_at = None
        self.last_tick_at = None
        self.reconnects = 0
        self.blocked_until = 0.0
        self._task = None
        self._ws = None
        self._lock = asyncio.Lock()
        self._revision = 0
        self._ack_id = None
        self._ack_at = 0.0

    def supports(self, code):
        return instrument(code) is not None

    def available(self, code):
        return code not in self.rejected and time.monotonic() >= self.blocked_until

    async def set_codes(self, codes):
        if codes == self.codes and (not codes or (self._task and not self._task.done())):
            return
        self.codes = list(codes)
        self.approved.intersection_update(codes)
        if (self._task is None or self._task.done()) and codes:
            self._task = asyncio.create_task(self._run(), name=self.id)
        elif self._ws is not None:
            try:
                await self._declare()
            except websockets.exceptions.ConnectionClosed:
                pass  # The receiver owns teardown and reconnection.

    async def _declare(self):
        async with self._lock:
            if self._ws is None:
                return
            self._revision += 1
            self._ack_id = str(self._revision)
            self._ack_at = time.monotonic()
            declarations = [{"id": self._ack_id}]
            for market in ("kr", "us"):
                symbols = sorted({pair[1] for code in self.codes if (pair := instrument(code)) and pair[0] == market})
                if symbols:
                    declarations.append({"type": f"trade:{market}", "codes": symbols})
            # An empty array really removes all subscriptions (an id-only array is unnecessary).
            await self._ws.send(json.dumps(declarations if self.codes else []))

    async def _run(self):
        delay, expired = 2, None
        access = None
        context = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        while True:
            try:
                self.state = "connecting"
                access = await self.token.get(expired=expired)
                expired = None
                async with websockets.connect(WS, additional_headers={"Authorization": "Bearer " + access},
                                              ssl=context, open_timeout=15, close_timeout=3,
                                              ping_interval=30, ping_timeout=15, max_size=2**20) as ws:
                    self._ws = ws
                    self.reason = None
                    self.state = "connected"
                    await self._declare()
                    while True:
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        except TimeoutError:
                            if self.codes and self._ack_at and time.monotonic() - self._ack_at > 15:
                                raise TossError("subscription_timeout")
                            await ws.send("PING")
                            continue
                        self.last_frame_at = datetime.now(KST).isoformat()
                        try:
                            message = json.loads(raw)
                        except (ValueError, UnicodeError):
                            continue
                        if not isinstance(message, dict):
                            continue
                        kind = message.get("type")
                        if kind == "subscriptions":
                            if self.codes and message.get("id") != self._ack_id:
                                continue
                            self._ack_at = 0
                            topics = message.get("subscribed", [])
                            if not isinstance(topics, list) or not isinstance(message.get("rejected", []), list):
                                raise TossError("subscription_response_invalid")
                            self.approved = {code for code in self.codes if (pair := instrument(code))
                                             and f"trade:{pair[0]}:{pair[1]}" in topics}
                            for rejected in message.get("rejected", []):
                                if not isinstance(rejected, dict):
                                    continue
                                for code in self.codes:
                                    pair = instrument(code)
                                    if rejected.get("target") == f"trade:{pair[0]}:{pair[1]}":
                                        reason = rejected.get("code", "subscription_rejected")
                                        self.rejected[code] = reason if re.fullmatch(r"[a-z-]{1,48}", str(reason)) else "subscription_rejected"
                            self.state = "subscribed" if self.approved else "degraded" if self.codes else "connected"
                            delay = 2
                        elif kind == "message":
                            for code in self.approved:
                                tick = normalize(message, code)
                                if tick:
                                    if tick["currency"] == "USD":
                                        # Existing portfolio prices are expressed in KRW.
                                        tick.update(previous_close=None, change=None)
                                        tick = await self._to_won(tick)
                                    if tick:
                                        self.last_tick_at = tick["as_of"]
                                        self.state = "live"
                                        self.publish(tick)
                                    break
                        elif kind == "error":
                            error = message.get("error")
                            code = error.get("code") if isinstance(error, dict) else None
                            if code == "rate-limit-exceeded":
                                await asyncio.sleep(1)
                                await self._declare()
                            else:
                                raise TossError("server_shutdown" if code == "server-shutdown" else "subscription_error")
                        if self._ack_at and time.monotonic() - self._ack_at > 15:
                            raise TossError("subscription_timeout")
            except asyncio.CancelledError:
                raise
            except (httpx.HTTPError, OSError, ValueError, TypeError, TossError, websockets.exceptions.WebSocketException, TimeoutError) as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status == 401:
                    expired = access
                self.reason = exc.reason if isinstance(exc, TossError) else {401: "authentication_error", 403: "ip_not_allowed"}.get(status, "connection_error")
                logger.warning("Realtime source disconnected: source=%s reason=%s", self.id, self.reason)
            finally:
                self._ws = None
                self.approved.clear()
                self._ack_at = 0
                self.state = "reconnecting"
            self.reconnects += 1
            self.blocked_until = time.monotonic() + min(delay, 60)
            await asyncio.sleep(delay + random.random())
            delay = min(delay * 2, 60)

    @staticmethod
    async def _to_won(tick):
        # to_won also scales previous_close/change, which are absent in the trade protocol.
        converted = await to_won({**tick, "previous_close": 0, "change": 0})
        if converted:
            converted.pop("previous_close", None)
            converted.pop("change", None)
        return converted

    async def close(self):
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        self._task, self._ws = None, None
        self.approved.clear()
        self.state = "offline"

    def snapshot(self):
        state = self.state
        if state == "live":
            from services.market.quote_policy import trade_timestamp
            at = trade_timestamp(self.last_tick_at)
            if at is None or not 0 <= time.time() - at < 90:
                state = "subscribed"
        return {"id": self.id, "provider": self.provider, "state": state, "capacity": self.capacity,
                "requested": len(self.codes), "subscribed": len(self.approved), "rejected": len(self.rejected),
                "reason": self.reason, "last_frame_at": self.last_frame_at, "last_tick_at": self.last_tick_at,
                "reconnects": self.reconnects}
