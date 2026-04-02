"""Browser WebSocket endpoint for real-time stock quotes.

Relays quotes from kis_ws_manager to connected browser clients.
Only one browser session can actively receive real-time WebSocket quotes
at a time. Other sessions fall back to REST polling. A new session can
"take over" the active slot from an existing session.
"""

from __future__ import annotations

import asyncio
import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

import kis_ws_manager

logger = logging.getLogger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# Active subscriber tracking — at most one browser WS drives subscriptions
# ---------------------------------------------------------------------------

_active_ws: WebSocket | None = None
_active_ws_lock = asyncio.Lock()


async def _set_active(ws: WebSocket | None) -> WebSocket | None:
    """Set *ws* as the active subscriber.  Returns the previously active WS."""
    global _active_ws
    async with _active_ws_lock:
        prev = _active_ws
        _active_ws = ws
        return prev


async def _clear_if_active(ws: WebSocket) -> None:
    """Clear the active subscriber only if it matches *ws*."""
    global _active_ws
    async with _active_ws_lock:
        if _active_ws is ws:
            _active_ws = None


@router.websocket("/ws/quotes")
async def ws_quotes(websocket: WebSocket):
    await websocket.accept()
    listener: asyncio.Queue | None = None
    relay_task: asyncio.Task | None = None
    is_active = False  # whether THIS connection owns the active slot

    try:
        # Send all currently cached quotes on connect
        cached = kis_ws_manager.get_all_cached_quotes()
        for quote in cached.values():
            await websocket.send_json({"type": "quote", **quote})

        # Check if another session is already active
        async with _active_ws_lock:
            occupied = _active_ws is not None
        await websocket.send_json({"type": "ws_status", "occupied": occupied})

        # Handle incoming messages from the browser
        while True:
            raw = await websocket.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            action = msg.get("action")

            if action == "ping":
                await websocket.send_json({"type": "pong"})

            elif action == "takeover":
                # Kick previous active session (if any)
                prev = await _set_active(websocket)
                is_active = True
                if prev is not None and prev is not websocket:
                    try:
                        await prev.send_json({"type": "ws_taken_over"})
                        await prev.close(code=4001, reason="taken_over")
                    except Exception:
                        pass  # already disconnected

                # Set up listener for real-time quote relay
                if listener is None:
                    listener = kis_ws_manager.add_listener()

                    async def _relay():
                        try:
                            while True:
                                quote = await listener.get()
                                await websocket.send_json(
                                    {"type": "quote", **quote}
                                )
                        except asyncio.CancelledError:
                            pass
                        except Exception:
                            pass

                    relay_task = asyncio.create_task(
                        _relay(), name="ws-quote-relay"
                    )

                await websocket.send_json(
                    {"type": "ws_status", "occupied": False, "active": True}
                )

            elif action == "subscribe":
                if not is_active:
                    # Not the active subscriber — ignore subscription requests
                    await websocket.send_json(
                        {
                            "type": "subscriptions",
                            "ws": [],
                            "rest": list(
                                {
                                    c
                                    for codes in msg.get("requested", {}).values()
                                    for c in codes
                                }
                            ),
                        }
                    )
                    continue

                requested = msg.get("requested", {})
                old_cached = set(kis_ws_manager.get_all_cached_quotes().keys())
                result = kis_ws_manager.update_subscriptions(requested)
                await websocket.send_json({"type": "subscriptions", **result})

                # Sync the actual KIS WS subscriptions
                await kis_ws_manager._sync_subscriptions()

                # Send cached quotes for newly relevant codes
                all_cached = kis_ws_manager.get_all_cached_quotes()
                new_ws_codes = set(result["ws"])
                for code in new_ws_codes:
                    if code in all_cached:
                        await websocket.send_json(
                            {"type": "quote", **all_cached[code]}
                        )

    except WebSocketDisconnect:
        logger.debug("Browser WebSocket disconnected")
    except Exception as exc:
        logger.warning("Browser WebSocket error: %s", exc)
    finally:
        if relay_task is not None:
            relay_task.cancel()
            try:
                await relay_task
            except asyncio.CancelledError:
                pass
        if listener is not None:
            await kis_ws_manager.remove_listener(listener)
        if is_active:
            await _clear_if_active(websocket)
