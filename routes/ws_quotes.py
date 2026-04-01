"""Browser WebSocket endpoint for real-time stock quotes.

Relays quotes from kis_ws_manager to connected browser clients.
"""

from __future__ import annotations

import asyncio
import json
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

import kis_ws_manager

logger = logging.getLogger(__name__)

router = APIRouter()


@router.websocket("/ws/quotes")
async def ws_quotes(websocket: WebSocket):
    await websocket.accept()
    listener: asyncio.Queue | None = None
    relay_task: asyncio.Task | None = None

    try:
        # Send all currently cached quotes on connect
        cached = kis_ws_manager.get_all_cached_quotes()
        for quote in cached.values():
            await websocket.send_json({"type": "quote", **quote})

        # Set up listener for real-time quote relay
        listener = kis_ws_manager.add_listener()

        async def _relay():
            """Forward quotes from kis_ws_manager to the browser."""
            try:
                while True:
                    quote = await listener.get()
                    await websocket.send_json({"type": "quote", **quote})
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

        relay_task = asyncio.create_task(_relay(), name="ws-quote-relay")

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

            elif action == "subscribe":
                requested = msg.get("requested", {})
                # Remember previously subscribed codes to detect new ones
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
