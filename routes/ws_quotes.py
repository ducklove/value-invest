"""Users receive their private broker prices; guests and users share Toss prices."""

from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect

from core.config import get_settings
from deps import get_current_user
from services.realtime.allocation import sanitize
from services.realtime.hub import get_hub

router = APIRouter()


def _origin_allowed(origin: str | None) -> bool:
    if not origin:
        return False
    allowed = get_settings().cors_allowed_origins
    target = origin.rstrip("/").lower()
    return "*" in allowed or any(target == candidate.rstrip("/").lower() for candidate in allowed)


@router.get("/api/realtime/status")
async def realtime_status(request: Request):
    user = await get_current_user(request)
    if not user or not user.get("is_admin"):
        raise HTTPException(403, "관리자만 연결 진단을 확인할 수 있습니다.")
    hub = get_hub()
    return {**hub.status(), "owner_running": hub.running}


@router.websocket("/ws/quotes")
async def ws_quotes(websocket: WebSocket):
    if not _origin_allowed(websocket.headers.get("origin")):
        await websocket.close(code=1008)
        return
    await websocket.accept()
    hub = get_hub()
    user = await get_current_user(websocket)
    client = hub.attach(user["google_sub"] if user else None)
    send_lock = asyncio.Lock()

    def status():
        return {**hub.status(client), "guest": client.user is None}

    async def send(payload):
        async with send_lock:
            await asyncio.wait_for(websocket.send_json(payload), timeout=10)

    async def relay():
        try:
            while True:
                await send(await client.next())
        except (WebSocketDisconnect, RuntimeError, TimeoutError, OSError):
            await websocket.close()

    task = None
    try:
        await send({**status(), "type": "ws_status", "active": True, "forbidden": False})
        task = asyncio.create_task(relay(), name="shared-quote-client")
        while True:
            try:
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=60)
            except TimeoutError:
                raw = '{"action":"ping"}'
            current = await get_current_user(websocket)
            if (current["google_sub"] if current else None) != client.user:
                await websocket.close(code=1008)
                break
            if len(raw) > 64000:
                await websocket.close(code=1009)
                break
            try:
                message = json.loads(raw)
            except ValueError:
                continue
            if not isinstance(message, dict):
                continue
            action = message.get("action")
            if action == "ping":
                await send({**status(), "type": "pong"})
            elif action == "subscribe":
                requested = sanitize(message.get("requested"))
                hub.subscribe(client, requested)
            elif action in {"acquire", "takeover"}:
                # Cached frontends may still send these; they cannot evict another browser.
                await send({**status(), "type": "ws_status", "active": True, "forbidden": False})
            elif action == "release":
                hub.subscribe(client, {})
                await send({**status(), "type": "ws_status", "active": True, "released": True})
    except (WebSocketDisconnect, RuntimeError, TimeoutError, OSError):
        pass
    finally:
        hub.detach(client)
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
