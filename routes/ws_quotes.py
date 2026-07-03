"""Browser WebSocket endpoint for real-time stock quotes.

Supports up to N concurrent active sessions (N = number of KIS API keys).
Each active session starts with one KIS WebSocket connection, then expands to
additional KIS key slots when the requested live Korean stocks exceed one
connection's 40-code capacity. Sessions beyond N can still take over an
existing slot (kicking the oldest session).
"""

from __future__ import annotations

import asyncio
import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

import kis_key_manager
import kis_ws_manager
from core.config import get_settings
from deps import get_current_user
from services import stock_quotes

logger = logging.getLogger(__name__)

router = APIRouter()


def _origin_allowed(origin: str | None) -> bool:
    """Allow the handshake only from a same-site browser.

    Browsers cannot forge the Origin header, so requiring it to be in the
    CORS allowlist blocks drive-by cross-site pages from opening the quote
    socket and exhausting scarce KIS key slots via ``takeover``. The SPA is
    served same-origin from the app, so its Origin is the app's own URL,
    which must be present in CORS_ALLOWED_ORIGINS (it is by default).
    """
    if not origin:
        return False
    allowed = get_settings().cors_allowed_origins
    if "*" in allowed:
        return True
    target = origin.rstrip("/").lower()
    return any(target == candidate.rstrip("/").lower() for candidate in allowed)


# ---------------------------------------------------------------------------
# Session tracking
# ---------------------------------------------------------------------------


class _Conn:
    """One KIS WebSocket connection bundled with the key slot and relay task it owns.

    Keeping the three together (instead of three parallel lists) makes start,
    trim, and teardown atomic per connection — a partial failure can no longer
    desynchronize the lists or leak a key slot.
    """

    __slots__ = ("key_slot", "ws_conn", "relay_task")

    def __init__(
        self,
        key_slot: kis_key_manager.KeySlot,
        ws_conn: kis_ws_manager.WsConnection,
    ) -> None:
        self.key_slot = key_slot
        self.ws_conn = ws_conn
        self.relay_task: asyncio.Task | None = None


class _Session:
    """Mutable state for one browser WebSocket handler."""

    __slots__ = (
        "conns",
        "send_lock",
        "is_active",
        "kicked",
    )

    def __init__(self) -> None:
        self.conns: list[_Conn] = []
        self.send_lock = asyncio.Lock()
        self.is_active: bool = False
        self.kicked: bool = False


# Ordered dict (insertion order) — oldest session is first
_sessions: dict[WebSocket, _Session] = {}
_sessions_lock = asyncio.Lock()


def _can_takeover(user: dict | None) -> bool:
    return bool(user and user.get("is_admin"))


def _ws_status_payload(
    user: dict | None,
    *,
    active: bool = False,
    session: _Session | None = None,
    **extra,
) -> dict:
    payload = {
        "type": "ws_status",
        "occupied": kis_key_manager.available_count() == 0,
        "active": active,
        "can_takeover": _can_takeover(user),
        "slots_total": kis_key_manager.total_count(),
        "slots_available": kis_key_manager.available_count(),
    }
    if session is not None:
        payload["slots_active"] = len(session.conns)
    payload.update(extra)
    return payload


async def _send_json(websocket: WebSocket, session: _Session, payload: dict) -> None:
    async with session.send_lock:
        await websocket.send_json(payload)


def _quote_payload(raw_quote: dict) -> dict | None:
    stock = stock_quotes.remember_quote(raw_quote.get("code"), raw_quote)
    if stock is None:
        return None
    return {"type": "quote", **stock_quotes.stock_to_quote(stock)}


async def _start_connection(
    websocket: WebSocket,
    session: _Session,
    key_slot: kis_key_manager.KeySlot,
) -> None:
    ws_conn = kis_ws_manager.WsConnection(key_slot)
    entry = _Conn(key_slot, ws_conn)
    # Register the slot before start() so that if start() raises, teardown
    # still releases the key slot instead of leaking it.
    session.conns.append(entry)
    try:
        await ws_conn.start()
    except Exception:
        session.conns.remove(entry)
        await _teardown_conn(entry)
        raise

    async def _relay() -> None:
        try:
            while True:
                quote = await ws_conn.listener.get()
                try:
                    payload = _quote_payload(quote)
                except Exception:
                    logger.warning("ws relay: failed to build quote payload", exc_info=True)
                    continue
                if payload:
                    await _send_json(websocket, session, payload)
        except asyncio.CancelledError:
            pass
        except Exception:
            # Send failure (browser gone) or listener error: stop relaying and
            # let the main receive loop detect the disconnect and clean up.
            logger.warning("ws relay stopped unexpectedly", exc_info=True)

    entry.relay_task = asyncio.create_task(
        _relay(), name=f"ws-quote-relay-{key_slot.slot_id}"
    )


async def _teardown_conn(entry: _Conn) -> None:
    """Cancel the relay task, stop the connection, and always release the slot."""
    if entry.relay_task is not None:
        entry.relay_task.cancel()
        try:
            await entry.relay_task
        except asyncio.CancelledError:
            pass
        entry.relay_task = None
    try:
        await entry.ws_conn.stop()
    except Exception:
        logger.warning("ws teardown: connection stop failed", exc_info=True)
    await kis_key_manager.release(entry.key_slot)


async def _stop_session(session: _Session) -> None:
    for entry in list(session.conns):
        await _teardown_conn(entry)
    session.conns.clear()
    session.is_active = False


async def _evict_oldest_session(exclude: _Session | None = None) -> bool:
    old_ws: WebSocket | None = None
    old_session: _Session | None = None
    async with _sessions_lock:
        for candidate_ws, candidate_session in list(_sessions.items()):
            if candidate_session is exclude:
                continue
            old_ws = candidate_ws
            old_session = candidate_session
            old_session.kicked = True
            del _sessions[candidate_ws]
            break

    if old_ws is None or old_session is None:
        return False

    await _stop_session(old_session)
    try:
        await _send_json(old_ws, old_session, {"type": "ws_taken_over"})
        await old_ws.close(code=4001, reason="taken_over")
    except Exception:
        pass
    return True


async def _trim_extra_connections(session: _Session, desired_count: int) -> None:
    while len(session.conns) > desired_count:
        entry = session.conns.pop()
        await _teardown_conn(entry)


async def _ensure_session_capacity(
    websocket: WebSocket,
    session: _Session,
    requested: dict,
) -> None:
    if not session.is_active:
        return
    total_slots = max(1, kis_key_manager.total_count())
    total_ws_capacity = total_slots * kis_ws_manager.MAX_SUBSCRIPTIONS
    all_live_plan = kis_ws_manager.plan_requested_subscriptions(
        requested,
        max_subscriptions=total_ws_capacity,
    )
    desired = max(
        1,
        min(
            total_slots,
            (len(all_live_plan["ws"]) + kis_ws_manager.MAX_SUBSCRIPTIONS - 1)
            // kis_ws_manager.MAX_SUBSCRIPTIONS,
        ),
    )

    while len(session.conns) < desired:
        key_slot = await kis_key_manager.acquire()
        if key_slot is None:
            break
        await _start_connection(websocket, session, key_slot)

    await _trim_extra_connections(session, min(desired, len(session.conns)))


def _apply_multi_connection_plan(session: _Session, requested: dict) -> dict[str, list[str]]:
    capacity = len(session.conns) * kis_ws_manager.MAX_SUBSCRIPTIONS
    result = kis_ws_manager.plan_requested_subscriptions(
        requested,
        max_subscriptions=capacity,
    )
    for idx, entry in enumerate(session.conns):
        start = idx * kis_ws_manager.MAX_SUBSCRIPTIONS
        end = start + kis_ws_manager.MAX_SUBSCRIPTIONS
        entry.ws_conn.update_subscriptions({"portfolio": result["ws"][start:end]})
    return result


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@router.websocket("/ws/quotes")
async def ws_quotes(websocket: WebSocket):
    if not _origin_allowed(websocket.headers.get("origin")):
        logger.warning(
            "ws/quotes rejected: untrusted origin %r", websocket.headers.get("origin")
        )
        await websocket.close(code=1008)
        return
    await websocket.accept()
    session = _Session()
    current_user = await get_current_user(websocket)  # WebSocket exposes the same cookie mapping as Request.

    try:
        # Send cached WS quotes only when the active WS market matches the
        # REST market. After NXT closes, KRX WS cache can be older than the
        # final NXT REST quote and must not repaint the browser backwards.
        cached = (
            kis_ws_manager.get_all_cached_quotes()
            if kis_ws_manager.ws_cache_matches_rest_market()
            else {}
        )
        for quote in cached.values():
            payload = _quote_payload(quote)
            if payload:
                await _send_json(websocket, session, payload)

        # Report slot availability
        await _send_json(websocket, session, _ws_status_payload(current_user))

        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            action = msg.get("action")

            if action == "ping":
                await _send_json(websocket, session, {"type": "pong"})

            elif action == "takeover":
                if not _can_takeover(current_user):
                    await _send_json(
                        websocket,
                        session,
                        _ws_status_payload(current_user, active=False, forbidden=True),
                    )
                    continue

                if session.is_active:
                    await _send_json(
                        websocket,
                        session,
                        _ws_status_payload(current_user, active=True, session=session, occupied=False),
                    )
                    continue

                # Try to acquire a free key slot
                key_slot = await kis_key_manager.acquire()

                # If none available, an explicit admin request may kick the
                # oldest session to free one slot. Passive clients and
                # subscription expansion never call this path.
                if key_slot is None:
                    await _evict_oldest_session(exclude=session)
                    key_slot = await kis_key_manager.acquire()

                if key_slot is None:
                    await _send_json(
                        websocket,
                        session,
                        _ws_status_payload(current_user, active=False, occupied=True),
                    )
                    continue

                # Create a new KIS WebSocket connection for this session
                await _start_connection(websocket, session, key_slot)
                session.is_active = True

                async with _sessions_lock:
                    _sessions[websocket] = session

                await _send_json(
                    websocket,
                    session,
                    _ws_status_payload(current_user, active=True, session=session, occupied=False),
                )

            elif action == "release":
                if session.is_active:
                    async with _sessions_lock:
                        _sessions.pop(websocket, None)
                    await _stop_session(session)
                await _send_json(
                    websocket,
                    session,
                    _ws_status_payload(current_user, active=False, released=True),
                )

            elif action == "subscribe":
                if not session.is_active or not session.conns:
                    # Not active — all codes fall back to REST
                    await _send_json(websocket, session, {
                        "type": "subscriptions",
                        "ws": [],
                        "rest": kis_ws_manager.plan_requested_subscriptions(
                            msg.get("requested", {}),
                            max_subscriptions=0,
                        )["rest"],
                    })
                    continue

                requested = msg.get("requested", {})
                await _ensure_session_capacity(websocket, session, requested)
                result = _apply_multi_connection_plan(session, requested)
                await _send_json(websocket, session, {
                    "type": "subscriptions",
                    **result,
                    "slots_active": len(session.conns),
                    "slots_total": kis_key_manager.total_count(),
                    "slots_available": kis_key_manager.available_count(),
                })
                await asyncio.gather(
                    *(entry.ws_conn.sync_subscriptions() for entry in session.conns)
                )

                # Send cached quotes for newly subscribed codes
                all_cached = kis_ws_manager.get_all_cached_quotes()
                for code in result["ws"]:
                    if code in all_cached:
                        payload = _quote_payload(all_cached[code])
                        if payload:
                            await _send_json(websocket, session, payload)

    except WebSocketDisconnect:
        logger.debug("Browser WebSocket disconnected")
    except Exception as exc:
        logger.warning("Browser WebSocket error: %s", exc)
    finally:
        # Clean up only if this session was NOT already kicked
        if not session.kicked:
            await _stop_session(session)
            async with _sessions_lock:
                _sessions.pop(websocket, None)
