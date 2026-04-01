"""KIS WebSocket Manager — real-time quote streaming for Korean stocks.

Connects directly to Korea Investment & Securities (KIS) WebSocket API
to receive H0STCNT0 (실시간 체결가) data.  Replaces polling the KIS proxy
for live price updates.

Public API
----------
- load_credentials()           — read KIS_APP_KEY / KIS_APP_SECRET / KIS_BASE_URL
- start() / stop()             — lifecycle (call from FastAPI lifespan)
- get_cached_quote(code)       — latest quote dict for a single stock
- get_all_cached_quotes()      — full in-memory cache snapshot
- update_subscriptions(req)    — priority-based subscription management
- is_korean_stock(code)        — True for 6-char Korean stock codes
- add_listener() / remove_listener(q) — asyncio.Queue fan-out for browser WS
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from typing import Any

import httpx
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

# Approval key refresh interval (23 hours — keys are valid ~24h)
APPROVAL_KEY_TTL_S = 23 * 3600


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

_credentials: dict[str, str] = {}


def load_credentials() -> dict[str, str]:
    """Read KIS API credentials from environment variables."""
    global _credentials
    app_key = os.environ.get("KIS_APP_KEY", "")
    app_secret = os.environ.get("KIS_APP_SECRET", "")
    base_url = os.environ.get("KIS_BASE_URL", "").rstrip("/")
    if not app_key or not app_secret or not base_url:
        logger.warning(
            "KIS WebSocket credentials incomplete — "
            "set KIS_APP_KEY, KIS_APP_SECRET, KIS_BASE_URL env vars"
        )
    _credentials = {
        "app_key": app_key,
        "app_secret": app_secret,
        "base_url": base_url,
    }
    return _credentials


# ---------------------------------------------------------------------------
# Approval key management
# ---------------------------------------------------------------------------

_approval_key: str = ""
_approval_key_ts: float = 0.0


async def _obtain_approval_key() -> str:
    """POST to KIS REST API to get a WebSocket approval key."""
    global _approval_key, _approval_key_ts

    # Reuse if still fresh
    if _approval_key and (time.monotonic() - _approval_key_ts) < APPROVAL_KEY_TTL_S:
        return _approval_key

    creds = _credentials or load_credentials()
    url = f"{creds['base_url']}/oauth2/Approval"
    payload = {
        "grant_type": "client_credentials",
        "appkey": creds["app_key"],
        "secretkey": creds["app_secret"],
    }

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(url, json=payload)
        resp.raise_for_status()
        data = resp.json()

    key = data.get("approval_key", "")
    if not key:
        raise RuntimeError(f"KIS approval key empty: {data}")

    _approval_key = key
    _approval_key_ts = time.monotonic()
    logger.info("KIS WebSocket approval key obtained")
    return _approval_key


# ---------------------------------------------------------------------------
# Korean stock code detection
# ---------------------------------------------------------------------------


def is_korean_stock(code: str) -> bool:
    """Return True if *code* looks like a Korean stock code.

    Korean codes are 6 characters where the first 5 are digits.
    The 6th may be a digit or an uppercase letter (e.g. "K" for preferred).
    Examples: "005930", "00088K", "03473K".
    Non-Korean: "CASH_KRW", "KRX_GOLD", "CRYPTO_BTC", "AAPL".
    """
    return bool(_KR_CODE_RE.match(code)) if isinstance(code, str) else False


# ---------------------------------------------------------------------------
# Quote cache (in-memory)
# ---------------------------------------------------------------------------

_quote_cache: dict[str, dict[str, Any]] = {}
_cache_lock = asyncio.Lock()


def get_cached_quote(code: str) -> dict[str, Any] | None:
    """Return the latest cached quote for *code*, or None."""
    return _quote_cache.get(code)


def get_all_cached_quotes() -> dict[str, dict[str, Any]]:
    """Return a shallow copy of the full quote cache."""
    return dict(_quote_cache)


# ---------------------------------------------------------------------------
# Listener (fan-out) management
# ---------------------------------------------------------------------------

_listeners: list[asyncio.Queue] = []
_listeners_lock = asyncio.Lock()


def add_listener() -> asyncio.Queue:
    """Create and register a new asyncio.Queue for quote updates."""
    q: asyncio.Queue = asyncio.Queue(maxsize=256)
    _listeners.append(q)
    logger.debug("Listener added (total=%d)", len(_listeners))
    return q


async def remove_listener(q: asyncio.Queue) -> None:
    """Unregister a listener queue."""
    async with _listeners_lock:
        try:
            _listeners.remove(q)
            logger.debug("Listener removed (total=%d)", len(_listeners))
        except ValueError:
            pass


async def _broadcast(msg: dict[str, Any]) -> None:
    """Send a message to all registered listener queues (non-blocking)."""
    async with _listeners_lock:
        dead: list[asyncio.Queue] = []
        for q in _listeners:
            try:
                q.put_nowait(msg)
            except asyncio.QueueFull:
                # Slow consumer — drop oldest and retry
                try:
                    q.get_nowait()
                    q.put_nowait(msg)
                except Exception:
                    dead.append(q)
        for q in dead:
            try:
                _listeners.remove(q)
            except ValueError:
                pass


# ---------------------------------------------------------------------------
# Subscription management
# ---------------------------------------------------------------------------

_current_subs: set[str] = set()   # codes currently subscribed on the WS
_requested: dict[str, list[str]] = {}  # category -> [codes]


def update_subscriptions(
    requested: dict[str, list[str]],
) -> dict[str, list[str]]:
    """Determine which codes to subscribe via WS vs fall back to REST.

    *requested* maps priority category to stock code lists, e.g.::

        {
            "portfolio": ["005930", "000660"],
            "benchmark": ["005930"],
            "sidebar":   ["035420", "003550"],
        }

    Returns ``{"ws": [...], "rest": [...]}`` — codes that fit within
    the 40-slot limit (by priority) vs those that overflow.
    """
    global _requested
    _requested = {k: list(v) for k, v in requested.items()}
    return _compute_subscription_plan()


def _compute_subscription_plan() -> dict[str, list[str]]:
    """Walk priority order, collect unique Korean codes up to MAX_SUBSCRIPTIONS.

    Non-Korean codes (CASH, GOLD, CRYPTO, foreign) are always placed in *rest*.
    """
    ws_codes: list[str] = []
    rest_codes: list[str] = []
    seen: set[str] = set()

    for category in PRIORITY_ORDER:
        for code in _requested.get(category, []):
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


# ---------------------------------------------------------------------------
# H0STCNT0 message parsing
# ---------------------------------------------------------------------------

# Change-sign mapping: 1=상한 2=상승 3=보합 4=하한 5=하한
_SIGN_NEGATIVE = {"4", "5"}


def _parse_h0stcnt0(raw: str) -> dict[str, Any] | None:
    """Parse a single H0STCNT0 wire-format message.

    Format: ``0|H0STCNT0|001|005930^153025^67500^2^500^0.75^...``
    """
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

        # Negate for 하락/하한
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
# WebSocket connection loop
# ---------------------------------------------------------------------------

_ws_task: asyncio.Task | None = None
_stop_event: asyncio.Event | None = None
_ws_connection: Any = None  # websockets connection object


def _make_subscribe_msg(code: str, *, subscribe: bool = True) -> str:
    """Build JSON subscribe/unsubscribe message for H0STCNT0."""
    return json.dumps({
        "header": {
            "approval_key": _approval_key,
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


async def _sync_subscriptions() -> None:
    """Add/remove WS subscriptions to match the current plan."""
    global _current_subs

    if _ws_connection is None:
        return

    plan = _compute_subscription_plan()
    desired = set(plan["ws"])

    to_add = desired - _current_subs
    to_remove = _current_subs - desired

    for code in to_remove:
        try:
            await _ws_connection.send(_make_subscribe_msg(code, subscribe=False))
            _current_subs.discard(code)
            logger.debug("Unsubscribed %s", code)
        except Exception as exc:
            logger.warning("Unsubscribe %s failed: %s", code, exc)

    for code in to_add:
        try:
            await _ws_connection.send(_make_subscribe_msg(code, subscribe=True))
            _current_subs.add(code)
            logger.debug("Subscribed %s", code)
        except Exception as exc:
            logger.warning("Subscribe %s failed: %s", code, exc)


async def _ws_loop() -> None:
    """Main WebSocket event loop with auto-reconnect."""
    global _ws_connection, _current_subs

    while not _stop_event.is_set():
        try:
            approval_key = await _obtain_approval_key()
            logger.info("Connecting to KIS WebSocket at %s", WS_URI)

            async with websockets.connect(
                WS_URI,
                additional_headers={"approval_key": approval_key},
                ping_interval=None,  # KIS uses its own PINGPONG
            ) as ws:
                _ws_connection = ws
                _current_subs = set()
                logger.info("KIS WebSocket connected")

                # Subscribe to whatever is currently requested
                await _sync_subscriptions()

                async for raw_msg in ws:
                    if _stop_event.is_set():
                        break

                    if isinstance(raw_msg, bytes):
                        raw_msg = raw_msg.decode("utf-8", errors="replace")

                    # Real-time data starts with '0' or '1'
                    if raw_msg and raw_msg[0] in ("0", "1"):
                        quote = _parse_h0stcnt0(raw_msg)
                        if quote is not None:
                            _quote_cache[quote["code"]] = quote
                            await _broadcast(quote)
                        continue

                    # JSON control messages (PINGPONG, subscription confirmations)
                    try:
                        ctrl = json.loads(raw_msg)
                        tr_id = ctrl.get("header", {}).get("tr_id", "")
                        if tr_id == "PINGPONG":
                            await ws.send(raw_msg)
                            logger.debug("PINGPONG echoed")
                        elif tr_id:
                            rt_cd = ctrl.get("body", {}).get("rt_cd")
                            msg1 = ctrl.get("body", {}).get("msg1", "")
                            logger.debug("KIS ctrl: tr_id=%s rt_cd=%s msg=%s", tr_id, rt_cd, msg1)
                    except (json.JSONDecodeError, KeyError):
                        pass

        except asyncio.CancelledError:
            logger.info("KIS WebSocket task cancelled")
            break
        except Exception as exc:
            logger.error("KIS WebSocket error: %s", exc, exc_info=True)
        finally:
            _ws_connection = None
            _current_subs = set()

        if not _stop_event.is_set():
            logger.info(
                "KIS WebSocket reconnecting in %.0fs...", RECONNECT_DELAY_S
            )
            try:
                await asyncio.wait_for(
                    _stop_event.wait(), timeout=RECONNECT_DELAY_S
                )
            except asyncio.TimeoutError:
                pass  # timeout expired — reconnect


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


async def start() -> None:
    """Start the KIS WebSocket manager background task."""
    global _ws_task, _stop_event

    creds = _credentials or load_credentials()
    if not creds.get("app_key"):
        logger.warning("KIS WebSocket not started — credentials missing")
        return

    if _ws_task is not None and not _ws_task.done():
        logger.warning("KIS WebSocket already running")
        return

    _stop_event = asyncio.Event()
    _ws_task = asyncio.create_task(_ws_loop(), name="kis-ws-manager")
    logger.info("KIS WebSocket manager started")


async def stop() -> None:
    """Stop the KIS WebSocket manager."""
    global _ws_task, _ws_connection

    if _stop_event is not None:
        _stop_event.set()

    if _ws_connection is not None:
        try:
            await _ws_connection.close()
        except Exception:
            pass

    if _ws_task is not None:
        _ws_task.cancel()
        try:
            await _ws_task
        except asyncio.CancelledError:
            pass
        _ws_task = None

    logger.info("KIS WebSocket manager stopped")
