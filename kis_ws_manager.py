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
from datetime import datetime, time as dtime, timedelta, timezone
from typing import Any

import websockets

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WS_URI = "ws://ops.koreainvestment.com:21000"
# H0STCNT0 = KRX 정규시장 실시간 체결가 (09:00~15:30)
# H0NXCNT0 = NXT(넥스트레이드) 실시간 체결가 (08:00~09:00 프리, 15:30~21:00 애프터)
# 두 TR은 동일 와이어 포맷이며, 한 시점에는 둘 중 하나만 활성이므로 시간대별로
# 단일 TR을 구독하여 종목당 1슬롯만 사용한다(통합 H0UNCNT0는 권한 이슈로 미사용).
_ACCEPTED_TR_IDS = {"H0STCNT0", "H0NXCNT0", "H0UNCNT0"}
MAX_SUBSCRIPTIONS = 40  # KIS hard limit ~41

KST = timezone(timedelta(hours=9))
_KRX_OPEN = dtime(9, 0)
_KRX_CLOSE = dtime(15, 30)
_NXT_AFTER_CLOSE = dtime(21, 0)


def active_market_code(now: datetime | None = None) -> str:
    """KIS REST FID_COND_MRKT_DIV_CODE matching the active session window.

    Returns ``"J"`` for KRX 정규시간, ``"NX"`` for NXT 운영시간.
    Mirrors :func:`_active_tr_id` so REST fallback queries the same venue
    as the WebSocket subscription.
    """
    return "J" if _active_tr_id(now) == "H0STCNT0" else "NX"


def _active_tr_id(now: datetime | None = None) -> str:
    """Return the TR_ID that should be subscribed at *now* (KST)."""
    cur = (now or datetime.now(KST)).timetz().replace(tzinfo=None)
    if _KRX_OPEN <= cur < _KRX_CLOSE:
        return "H0STCNT0"
    if cur < _NXT_AFTER_CLOSE:  # 00:00~09:00 또는 15:30~21:00 → NXT
        return "H0NXCNT0"
    return "H0STCNT0"  # 21:00 이후는 어차피 데이터 없음 — 기본값


def _seconds_until_next_boundary(now: datetime | None = None) -> float:
    """Seconds until the next TR_ID switch boundary in KST."""
    cur = now or datetime.now(KST)
    today = cur.date()
    boundaries = [
        datetime.combine(today, _KRX_OPEN, tzinfo=KST),
        datetime.combine(today, _KRX_CLOSE, tzinfo=KST),
        datetime.combine(today, _NXT_AFTER_CLOSE, tzinfo=KST),
    ]
    for b in boundaries:
        if b > cur:
            return (b - cur).total_seconds()
    # 다음 날 09:00
    next_open = datetime.combine(today + timedelta(days=1), _KRX_OPEN, tzinfo=KST)
    return (next_open - cur).total_seconds()

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
_quote_cache_date: str = ""  # KST YYYYMMDD when the cache was last valid

# Set of KRX stock codes that have been observed to NOT trade on the NXT
# after-hours market (KIS proxy returned a 5xx for `?market=NX`). Subsequent
# REST quote calls during NXT hours skip the NX attempt and go straight to
# KRX. Persistent for the process lifetime — clears on restart.
_nxt_unsupported: set[str] = set()


def is_nxt_unsupported(code: str) -> bool:
    return code in _nxt_unsupported


def mark_nxt_unsupported(code: str) -> None:
    _nxt_unsupported.add(code)


def _flush_stale_cache() -> None:
    """Clear the entire WS quote cache when the KST date rolls over."""
    global _quote_cache_date
    from datetime import datetime, timezone, timedelta
    kst = timezone(timedelta(hours=9))
    today = datetime.now(kst).strftime("%Y%m%d")
    if _quote_cache_date and _quote_cache_date != today:
        _quote_cache.clear()
        logger.info("WS quote cache cleared (date rolled %s → %s)", _quote_cache_date, today)
    _quote_cache_date = today


def get_cached_quote(code: str) -> dict[str, Any] | None:
    """Return the latest cached quote for *code*, or None."""
    _flush_stale_cache()
    return _quote_cache.get(code)


def get_all_cached_quotes() -> dict[str, dict[str, Any]]:
    """Return a shallow copy of the full quote cache."""
    _flush_stale_cache()
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
    if tr_id not in _ACCEPTED_TR_IDS:
        return None

    fields = payload_str.split("^")
    if len(fields) < 34:
        logger.warning("%s: expected >=34 fields, got %d", tr_id, len(fields))
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
        # 구독 단위는 (code, tr_id) — 시간대 전환 시 (code, KRX) → (code, NXT)로 교체.
        self._current_subs: set[tuple[str, str]] = set()
        self._requested: dict[str, list[str]] = {}
        self._boundary_task: asyncio.Task | None = None
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
        active_tr = _active_tr_id()
        desired: set[tuple[str, str]] = {(code, active_tr) for code in plan["ws"]}

        for code, tr_id in (self._current_subs - desired):
            try:
                await self._ws.send(self._make_msg(code, tr_id, subscribe=False))
                self._current_subs.discard((code, tr_id))
                logger.debug(
                    "Unsubscribed %s/%s (slot %d)",
                    code, tr_id, self.key_slot.slot_id,
                )
            except Exception as exc:
                logger.warning("Unsub %s/%s failed: %s", code, tr_id, exc)

        for code, tr_id in (desired - self._current_subs):
            try:
                await self._ws.send(self._make_msg(code, tr_id, subscribe=True))
                self._current_subs.add((code, tr_id))
                logger.debug(
                    "Subscribed %s/%s (slot %d)",
                    code, tr_id, self.key_slot.slot_id,
                )
            except Exception as exc:
                logger.warning("Sub %s/%s failed: %s", code, tr_id, exc)

    def _make_msg(self, code: str, tr_id: str, *, subscribe: bool = True) -> str:
        return json.dumps({
            "header": {
                "approval_key": self.key_slot._approval_key,
                "custtype": "P",
                "tr_type": "1" if subscribe else "2",
                "content-type": "utf-8",
            },
            "body": {
                "input": {
                    "tr_id": tr_id,
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
        self._boundary_task = asyncio.create_task(
            self._boundary_loop(),
            name=f"kis-ws-boundary-{self.key_slot.slot_id}",
        )
        _active_connections.add(self)

    async def _boundary_loop(self) -> None:
        """Re-sync subscriptions whenever the active TR_ID window switches."""
        while not self._stop_event.is_set():
            delay = _seconds_until_next_boundary() + 1.0  # 1초 버퍼
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
                return  # stop requested
            except asyncio.TimeoutError:
                pass
            try:
                logger.info(
                    "TR boundary reached → switching to %s (slot %d)",
                    _active_tr_id(), self.key_slot.slot_id,
                )
                await self.sync_subscriptions()
            except Exception as exc:
                logger.warning("Boundary re-sync failed: %s", exc)

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
        if self._boundary_task is not None:
            self._boundary_task.cancel()
            try:
                await self._boundary_task
            except asyncio.CancelledError:
                pass
            self._boundary_task = None
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
                                if quote["code"] not in _quote_cache:
                                    logger.info(
                                        "First quote: %s @ %s",
                                        quote["code"], quote["price"],
                                    )
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
                                logger.info(
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
