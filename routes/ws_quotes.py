"""Browser WebSocket endpoint for real-time stock quotes.

Supports up to N concurrent active sessions (N = number of KIS API keys).
Each active session gets its own KIS WebSocket connection with up to 40
real-time subscriptions.  Sessions beyond N can still take over an existing
slot (kicking the oldest session).
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
        "key_slot",
        "ws_conn",
        "relay_task",
        "status_task",
        "send_lock",
        "is_active",
        "kicked",
    )

    def __init__(self) -> None:
        self.key_slot: kis_key_manager.KeySlot | None = None
        self.ws_conn: kis_ws_manager.WsConnection | None = None
        self.relay_task: asyncio.Task | None = None
        self.status_task: asyncio.Task | None = None
        self.send_lock = asyncio.Lock()
        self.is_active: bool = False
        self.kicked: bool = False


# Ordered dict (insertion order) — oldest session is first
_sessions: dict[WebSocket, _Session] = {}
_sessions_lock = asyncio.Lock()


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------


@router.websocket("/ws/quotes")
async def ws_quotes(websocket: WebSocket):
    await websocket.accept()
    session = _Session()

    async def send_json(payload: dict) -> None:
        # Starlette WebSocket sends are not meant to be interleaved from
        # multiple tasks.  Quotes, KIS status, and command responses share
        # this lock so the browser sees ordered JSON messages.
        async with session.send_lock:
            await websocket.send_json(payload)

    try:
        # Send all currently cached quotes on connect
        cached = kis_ws_manager.get_all_cached_quotes()
        for quote in cached.values():
            await send_json({"type": "quote", **quote})

        # Report slot availability
        await send_json({
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
                await send_json({"type": "pong"})

            elif action == "takeover":
                if session.is_active:
                    await send_json(
                        {
                            "type": "ws_status",
                            "occupied": False,
                            "active": True,
                            "kis_connected": bool(
                                session.ws_conn and session.ws_conn.connected
                            ),
                        }
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
                        if old_session.relay_task:
                            old_session.relay_task.cancel()
                        if old_session.status_task:
                            old_session.status_task.cancel()
                        if old_session.ws_conn:
                            await old_session.ws_conn.stop()
                        if old_session.key_slot:
                            await kis_key_manager.release(old_session.key_slot)
                        try:
                            async with old_session.send_lock:
                                await oldest_ws.send_json({"type": "ws_taken_over"})
                            await oldest_ws.close(code=4001, reason="taken_over")
                        except Exception:
                            pass

                    key_slot = await kis_key_manager.acquire()

                if key_slot is None:
                    await send_json(
                        {
                            "type": "ws_status",
                            "occupied": True,
                            "active": False,
                            "kis_connected": False,
                        }
                    )
                    continue

                # Create a new KIS WebSocket connection for this session
                session.key_slot = key_slot
                session.ws_conn = kis_ws_manager.WsConnection(key_slot)
                await session.ws_conn.start()
                session.is_active = True

                # Relay quotes from the KIS connection to the browser
                conn = session.ws_conn  # capture for closure

                async def _relay() -> None:
                    try:
                        while True:
                            quote = await conn.listener.get()
                            await send_json({"type": "quote", **quote})
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        pass

                session.relay_task = asyncio.create_task(
                    _relay(), name="ws-quote-relay"
                )

                async def _relay_status() -> None:
                    try:
                        while True:
                            status = await conn.status_listener.get()
                            await send_json(
                                {
                                    "type": "ws_status",
                                    "occupied": False,
                                    "active": True,
                                    **status,
                                }
                            )
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        pass

                session.status_task = asyncio.create_task(
                    _relay_status(), name="ws-kis-status-relay"
                )

                async with _sessions_lock:
                    _sessions[websocket] = session

                await send_json(
                    {
                        "type": "ws_status",
                        "occupied": False,
                        "active": True,
                        "kis_connected": conn.connected,
                        "slot_id": conn.key_slot.slot_id,
                    }
                )

            elif action == "subscribe":
                if not session.is_active or session.ws_conn is None:
                    # Not active — all codes fall back to REST
                    await send_json({
                        "type": "subscriptions",
                        "ws": [],
                        "rest": list({
                            c
                            for codes in msg.get("requested", {}).values()
                            for c in codes
                        }),
                    })
                    continue

                requested = msg.get("requested", {})
                result = session.ws_conn.update_subscriptions(requested)
                await send_json({"type": "subscriptions", **result})
                await session.ws_conn.sync_subscriptions()

                # Send cached quotes for newly subscribed codes
                all_cached = kis_ws_manager.get_all_cached_quotes()
                for code in result["ws"]:
                    if code in all_cached:
                        await send_json(
                            {"type": "quote", **all_cached[code]}
                        )

    except WebSocketDisconnect:
        logger.debug("Browser WebSocket disconnected")
    except Exception as exc:
        logger.warning("Browser WebSocket error: %s", exc)
    finally:
        # Clean up only if this session was NOT already kicked
        if not session.kicked:
            if session.relay_task is not None:
                session.relay_task.cancel()
                try:
                    await session.relay_task
                except asyncio.CancelledError:
                    pass
            if session.status_task is not None:
                session.status_task.cancel()
                try:
                    await session.status_task
                except asyncio.CancelledError:
                    pass
            if session.ws_conn is not None:
                await session.ws_conn.stop()
            if session.key_slot is not None:
                await kis_key_manager.release(session.key_slot)
            async with _sessions_lock:
                _sessions.pop(websocket, None)
