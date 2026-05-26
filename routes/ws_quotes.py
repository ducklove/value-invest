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

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Session tracking
# ---------------------------------------------------------------------------


class _Session:
    """Mutable state for one browser WebSocket handler."""

    __slots__ = (
        "key_slots",
        "ws_conns",
        "relay_tasks",
        "send_lock",
        "is_active",
        "kicked",
    )

    def __init__(self) -> None:
        self.key_slots: list[kis_key_manager.KeySlot] = []
        self.ws_conns: list[kis_ws_manager.WsConnection] = []
        self.relay_tasks: list[asyncio.Task] = []
        self.send_lock = asyncio.Lock()
        self.is_active: bool = False
        self.kicked: bool = False


# Ordered dict (insertion order) — oldest session is first
_sessions: dict[WebSocket, _Session] = {}
_sessions_lock = asyncio.Lock()


async def _send_json(websocket: WebSocket, session: _Session, payload: dict) -> None:
    async with session.send_lock:
        await websocket.send_json(payload)


async def _start_connection(
    websocket: WebSocket,
    session: _Session,
    key_slot: kis_key_manager.KeySlot,
) -> None:
    conn = kis_ws_manager.WsConnection(key_slot)
    await conn.start()
    session.key_slots.append(key_slot)
    session.ws_conns.append(conn)

    async def _relay() -> None:
        try:
            while True:
                quote = await conn.listener.get()
                await _send_json(websocket, session, {"type": "quote", **quote})
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    session.relay_tasks.append(
        asyncio.create_task(_relay(), name=f"ws-quote-relay-{key_slot.slot_id}")
    )


async def _stop_session(session: _Session) -> None:
    for task in list(session.relay_tasks):
        task.cancel()
    for task in list(session.relay_tasks):
        try:
            await task
        except asyncio.CancelledError:
            pass
    session.relay_tasks.clear()

    for conn in list(session.ws_conns):
        await conn.stop()
    session.ws_conns.clear()

    for slot in list(session.key_slots):
        await kis_key_manager.release(slot)
    session.key_slots.clear()
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
    while len(session.ws_conns) > desired_count:
        task = session.relay_tasks.pop()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        conn = session.ws_conns.pop()
        await conn.stop()
        slot = session.key_slots.pop()
        await kis_key_manager.release(slot)


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

    while len(session.ws_conns) < desired:
        key_slot = await kis_key_manager.acquire()
        if key_slot is None:
            if not await _evict_oldest_session(exclude=session):
                break
            key_slot = await kis_key_manager.acquire()
            if key_slot is None:
                break
        await _start_connection(websocket, session, key_slot)

    await _trim_extra_connections(session, min(desired, len(session.ws_conns)))


def _apply_multi_connection_plan(session: _Session, requested: dict) -> dict[str, list[str]]:
    capacity = len(session.ws_conns) * kis_ws_manager.MAX_SUBSCRIPTIONS
    result = kis_ws_manager.plan_requested_subscriptions(
        requested,
        max_subscriptions=capacity,
    )
    for idx, conn in enumerate(session.ws_conns):
        start = idx * kis_ws_manager.MAX_SUBSCRIPTIONS
        end = start + kis_ws_manager.MAX_SUBSCRIPTIONS
        conn.update_subscriptions({"portfolio": result["ws"][start:end]})
    return result


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@router.websocket("/ws/quotes")
async def ws_quotes(websocket: WebSocket):
    await websocket.accept()
    session = _Session()

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
            await _send_json(websocket, session, {"type": "quote", **quote})

        # Report slot availability
        await _send_json(websocket, session, {
            "type": "ws_status",
            "occupied": kis_key_manager.available_count() == 0,
            "slots_total": kis_key_manager.total_count(),
            "slots_available": kis_key_manager.available_count(),
        })

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
                if session.is_active:
                    await _send_json(
                        websocket,
                        session,
                        {
                            "type": "ws_status",
                            "occupied": False,
                            "active": True,
                            "slots_active": len(session.ws_conns),
                        },
                    )
                    continue

                # Try to acquire a free key slot
                key_slot = await kis_key_manager.acquire()

                # If none available, kick the oldest session to free a slot
                if key_slot is None:
                    async with _sessions_lock:
                        if _sessions:
                            oldest_ws, old_session = next(iter(_sessions.items()))
                            old_session.kicked = True
                            del _sessions[oldest_ws]
                        else:
                            oldest_ws, old_session = None, None

                    if old_session is not None:
                        await _stop_session(old_session)
                        try:
                            await _send_json(oldest_ws, old_session, {"type": "ws_taken_over"})
                            await oldest_ws.close(code=4001, reason="taken_over")
                        except Exception:
                            pass

                    key_slot = await kis_key_manager.acquire()

                if key_slot is None:
                    await _send_json(
                        websocket,
                        session,
                        {"type": "ws_status", "occupied": True, "active": False},
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
                    {
                        "type": "ws_status",
                        "occupied": False,
                        "active": True,
                        "slots_active": len(session.ws_conns),
                        "slots_total": kis_key_manager.total_count(),
                        "slots_available": kis_key_manager.available_count(),
                    },
                )

            elif action == "subscribe":
                if not session.is_active or not session.ws_conns:
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
                    "slots_active": len(session.ws_conns),
                    "slots_total": kis_key_manager.total_count(),
                    "slots_available": kis_key_manager.available_count(),
                })
                await asyncio.gather(
                    *(conn.sync_subscriptions() for conn in session.ws_conns)
                )

                # Send cached quotes for newly subscribed codes
                all_cached = kis_ws_manager.get_all_cached_quotes()
                for code in result["ws"]:
                    if code in all_cached:
                        await _send_json(
                            websocket,
                            session,
                            {"type": "quote", **all_cached[code]},
                        )

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
