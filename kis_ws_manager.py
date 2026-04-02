"""KIS WebSocket Manager — real-time quote streaming for Korean stocks.

Supports multiple concurrent WebSocket connections, one per KIS API key slot.
Each browser session that acquires a key gets its own KIS WebSocket with up to
40 real-time subscriptions.  Quote data is cached in a shared module-level dict
so REST fallback and other modules can read the latest prices.

Public API
----------
- WsConnection                — one KIS WS connection tied to a KeySlot
- get_cached_quote(code)      — latest quote dict for a single stock
- get_all_cached_quotes()     — full in-memory cache snapshot
- is_korean_stock(code)       — True for 6-char Korean stock codes
- stop_all()                  — stop all active connections (shutdown)
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from typing import Any

import websockets

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WS_URI = "ws://ops.koreainvestment.com:21000"
TR_ID = "H0STCNT0"
MAX_SUBSCRIPTIONS = 40  # 41 hard limit, keep 1 buffer

# Priority order — lower index = higher priority
PRIORITY_ORDER: list[str] = ["portfolio", "benchmark", "sidebar", "analysis"]

# Korean stock code pattern: first 5 chars are digits, 6th is digit or uppercase letter
_KR_CODE_RE = re.compile(r"^\d{5}[\dA-Z]$")

# Reconnect delay after disconnect
RECONNECT_DELAY_S = 5.0


# ---------------------------------------------------------------------------
# Korean stock code detection
# ---------------------------------------------------------------------------


def is_korean_stock(code: str) -> bool:
    """Return True if *code* looks like a Korean stock code."""
    return bool(_KR_CODE_RE.match(code)) if isinstance(code, str) else False


# ---------------------------------------------------------------------------
# Quote cache (shared across all connections)
# ---------------------------------------------------------------------------

_quote_cache: dict[str, dict[str, Any]] = {}


def get_cached_quote(code: str) -> dict[str, Any] | None:
    """Return the latest cached quote for *code*, or None."""
    return _quote_cache.get(code)


def get_all_cached_quotes() -> dict[str, dict[str, Any]]:
    """Return a shallow copy of the full quote cache."""
    return dict(_quote_cache)


# ---------------------------------------------------------------------------
# H0STCNT0 message parsing
# ---------------------------------------------------------------------------

_SIGN_NEGATIVE = {"4", "5"}


def _parse_h0stcnt0(raw: str) -> dict[str, Any] | None:
    """Parse a single H0STCNT0 wire-format message."""
    parts = raw.split("|", 3)
    if len(parts) < 4:
        return None

    _enc_flag, tr_id, _count, payload_str = parts
    if tr_id != TR_ID:
        return None

    fields = payload_str.split("^")
    if len(fields) < 34:
        logger.warning("H0STCNT0: expected >=34 fields, got %d", len(fields))
        return None

    try:
        code = fields[0]
        trade_time = fields[1]  # HHMMSS
        price = int(fields[2])
        sign = fields[3]  # 1~5
        change = int(fields[4])
        change_pct = float(fields[5])
        volume = int(fields[13])
        business_date = fields[33] if len(fields) > 33 else ""

        if sign in _SIGN_NEGATIVE:
            change = -change
            change_pct = -change_pct

        return {
            "code": code,
            "trade_time": trade_time,
            "price": price,
            "change_sign": sign,
            "change": change,
            "change_pct": change_pct,
            "volume": volume,
            "business_date": business_date,
            "ts": time.time(),
        }
    except (ValueError, IndexError) as exc:
        logger.warning("H0STCNT0 parse error: %s — %s", exc, fields[:6])
        return None


# ---------------------------------------------------------------------------
# WsConnection — one KIS WebSocket tied to a KeySlot
# ---------------------------------------------------------------------------


class WsConnection:
    """A single KIS WebSocket connection tied to a KeySlot.

    Each connection has its own subscription set, listener queue, and
    reconnect loop.  Quotes are written to the shared ``_quote_cache``.
    """

    def __init__(self, key_slot: Any):
        self.key_slot = key_slot
        self._ws: Any = None
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._current_subs: set[str] = set()
        self._requested: dict[str, list[str]] = {}
        self.listener: asyncio.Queue = asyncio.Queue(maxsize=256)

    # -- Subscription management ------------------------------------------

    def update_subscriptions(
        self, requested: dict[str, list[str]],
    ) -> dict[str, list[str]]:
        """Set requested codes and return the subscription plan."""
        self._requested = {k: list(v) for k, v in requested.items()}
        return self._compute_plan()

    def _compute_plan(self) -> dict[str, list[str]]:
        ws_codes: list[str] = []
        rest_codes: list[str] = []
        seen: set[str] = set()
        for category in PRIORITY_ORDER:
            for code in self._requested.get(category, []):
                if code in seen:
                    continue
                seen.add(code)
                if is_korean_stock(code):
                    ws_codes.append(code)
                else:
                    rest_codes.append(code)
        rest_codes = ws_codes[MAX_SUBSCRIPTIONS:] + rest_codes
        ws_codes = ws_codes[:MAX_SUBSCRIPTIONS]
        return {"ws": ws_codes, "rest": rest_codes}

    async def sync_subscriptions(self) -> None:
        """Add/remove WS subscriptions to match the current plan."""
        if self._ws is None:
            return
        plan = self._compute_plan()
        desired = set(plan["ws"])

        for code in (self._current_subs - desired):
            try:
                await self._ws.send(self._make_msg(code, subscribe=False))
                self._current_subs.discard(code)
                logger.debug("Unsubscribed %s (slot %d)", code, self.key_slot.slot_id)
            except Exception as exc:
                logger.warning("Unsub %s failed: %s", code, exc)

        for code in (desired - self._current_subs):
            try:
                await self._ws.send(self._make_msg(code, subscribe=True))
                self._current_subs.add(code)
                logger.debug("Subscribed %s (slot %d)", code, self.key_slot.slot_id)
            except Exception as exc:
                logger.warning("Sub %s failed: %s", code, exc)

    def _make_msg(self, code: str, *, subscribe: bool = True) -> str:
        return json.dumps({
            "header": {
                "approval_key": self.key_slot._approval_key,
                "custtype": "P",
                "tr_type": "1" if subscribe else "2",
                "content-type": "utf-8",
            },
            "body": {
                "input": {
                    "tr_id": TR_ID,
                    "tr_key": code,
                }
            },
        })

    # -- Lifecycle --------------------------------------------------------

    async def start(self) -> None:
        """Launch the WebSocket event loop as a background task."""
        self._stop_event.clear()
        self._task = asyncio.create_task(
            self._ws_loop(),
            name=f"kis-ws-slot-{self.key_slot.slot_id}",
        )
        _active_connections.add(self)

    async def stop(self) -> None:
        """Stop the WebSocket connection and background task."""
        self._stop_event.set()
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        _active_connections.discard(self)

    # -- Event loop -------------------------------------------------------

    async def _ws_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                approval_key = await self.key_slot.get_approval_key()
                logger.info(
                    "Connecting to KIS WebSocket (slot %d)", self.key_slot.slot_id
                )

                async with websockets.connect(
                    WS_URI,
                    additional_headers={"approval_key": approval_key},
                    ping_interval=None,
                ) as ws:
                    self._ws = ws
                    self._current_subs = set()
                    logger.info(
                        "KIS WebSocket connected (slot %d)", self.key_slot.slot_id
                    )

                    await self.sync_subscriptions()

                    async for raw_msg in ws:
                        if self._stop_event.is_set():
                            break

                        if isinstance(raw_msg, bytes):
                            raw_msg = raw_msg.decode("utf-8", errors="replace")

                        # Real-time data: starts with '0' or '1'
                        if raw_msg and raw_msg[0] in ("0", "1"):
                            quote = _parse_h0stcnt0(raw_msg)
                            if quote is not None:
                                _quote_cache[quote["code"]] = quote
                                try:
                                    self.listener.put_nowait(quote)
                                except asyncio.QueueFull:
                                    try:
                                        self.listener.get_nowait()
                                        self.listener.put_nowait(quote)
                                    except Exception:
                                        pass
                            continue

                        # JSON control messages (PINGPONG, confirmations)
                        try:
                            ctrl = json.loads(raw_msg)
                            tr_id = ctrl.get("header", {}).get("tr_id", "")
                            if tr_id == "PINGPONG":
                                await ws.send(raw_msg)
                                logger.debug("PINGPONG echoed (slot %d)", self.key_slot.slot_id)
                            elif tr_id:
                                rt_cd = ctrl.get("body", {}).get("rt_cd")
                                msg1 = ctrl.get("body", {}).get("msg1", "")
                                logger.debug(
                                    "KIS ctrl (slot %d): tr_id=%s rt_cd=%s msg=%s",
                                    self.key_slot.slot_id, tr_id, rt_cd, msg1,
                                )
                        except (json.JSONDecodeError, KeyError):
                            pass

            except asyncio.CancelledError:
                logger.info("KIS WebSocket task cancelled (slot %d)", self.key_slot.slot_id)
                break
            except Exception as exc:
                logger.error(
                    "KIS WebSocket error (slot %d): %s",
                    self.key_slot.slot_id, exc, exc_info=True,
                )
            finally:
                self._ws = None
                self._current_subs = set()

            if not self._stop_event.is_set():
                logger.info(
                    "KIS WebSocket reconnecting in %.0fs (slot %d)...",
                    RECONNECT_DELAY_S, self.key_slot.slot_id,
                )
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=RECONNECT_DELAY_S
                    )
                except asyncio.TimeoutError:
                    pass


# ---------------------------------------------------------------------------
# Active connection tracking & shutdown
# ---------------------------------------------------------------------------

_active_connections: set[WsConnection] = set()


async def stop_all() -> None:
    """Stop all active WebSocket connections (call on server shutdown)."""
    for conn in list(_active_connections):
        await conn.stop()
    _active_connections.clear()
    logger.info("All KIS WebSocket connections stopped")
