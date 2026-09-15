"""로그인 사용자 전용 NH 키 등록·잔고 조회·연동 및 실시간 시세 전달."""

import asyncio
import json
import time

from fastapi import APIRouter, Body, HTTPException, Request, WebSocket, WebSocketDisconnect

from deps import get_current_user
from repositories import account_holdings, brokers
from repositories.broker_secrets import BrokerError, decrypt, encrypt
from services.brokers import namuh, realtime
from services.brokers.sync import fetch_snapshot, sync_account

router = APIRouter()


async def user_id(request) -> str:
    user = await get_current_user(request)
    if not user:
        raise HTTPException(401, "로그인이 필요합니다.")
    return user["google_sub"]


def selection(user: str, payload: dict) -> dict:
    try:
        choice = json.loads(decrypt(str(payload.get("selection", ""))))
        if choice["user"] != user or choice["expires_at"] < time.time():
            raise ValueError
        return choice
    except (KeyError, ValueError, TypeError, BrokerError):
        raise BrokerError("계좌 선택이 만료되었거나 올바르지 않습니다. 키 확인을 다시 실행해 주세요.") from None


@router.post("/api/portfolio/namuh/credentials")
async def register(request: Request, payload: dict = Body(...)):
    user = await user_id(request)
    key, secret = payload.get("app_key"), payload.get("app_secret")
    if not all(isinstance(value, str) and 8 <= len(value.strip()) <= 2048 for value in (key, secret)):
        raise BrokerError("나무 앱키와 시크릿을 입력해 주세요.")
    cid = await brokers.store_credential(user, key.strip(), secret.strip())
    rows = await namuh.accounts(user, cid)
    return {"credential_id": cid, "accounts": [{"account_mask": "•••••••" + row["account_no"][-4:], "environment": row["environment"],
        "selection": encrypt(json.dumps({**row, "credential_id": cid, "user": user, "expires_at": time.time() + 900}))} for row in rows]}


@router.post("/api/portfolio/accounts/{account_id}/namuh/preview")
async def preview(account_id: str, request: Request, payload: dict = Body(...)):
    user = await user_id(request)
    await account_holdings.require_account(user, account_id, writable=True)
    choice = selection(user, payload)
    choice["include_overseas"] = payload.get("include_overseas") is not False
    rows, balances = await fetch_snapshot(user, choice)
    return {"items": rows, "balances": balances}


@router.post("/api/portfolio/accounts/{account_id}/namuh")
async def connect(account_id: str, request: Request, payload: dict = Body(...)):
    user = await user_id(request)
    await account_holdings.ensure(user)
    choice = selection(user, payload)
    own = await namuh.accounts(user, choice["credential_id"])
    if {"account_no": choice["account_no"], "environment": choice["environment"]} not in own:
        raise BrokerError("현재 키의 계좌 목록에서 선택한 계좌를 확인하지 못했습니다.")
    await brokers.link_account(user, account_id, choice["credential_id"], choice["account_no"], choice["environment"], payload.get("include_overseas") is not False)
    return await sync_account(user, account_id)


@router.post("/api/portfolio/accounts/{account_id}/namuh/sync")
async def sync(account_id: str, request: Request):
    return await sync_account(await user_id(request), account_id)


@router.delete("/api/portfolio/accounts/{account_id}/namuh")
async def disconnect(account_id: str, request: Request):
    await brokers.disconnect(await user_id(request), account_id)
    return {"ok": True}


@router.websocket("/ws/namuh")
async def quotes(websocket: WebSocket):
    from routes.ws_quotes import _origin_allowed
    if not _origin_allowed(websocket.headers.get("origin")):
        await websocket.close(code=1008)
        return
    try:
        user = await user_id(websocket)
    except HTTPException:
        await websocket.close(code=1008)
        return
    await websocket.accept()
    sent = {}
    try:
        while True:
            # 사용자 인증과 키 연결은 주기적으로 재확인한다.
            if await user_id(websocket) != user:
                break
            links = [link for link in await brokers.list_links() if link["google_sub"] == user]
            if not links:
                break
            rows = await account_holdings.list_positions(user)
            for code in {row["stock_code"] for row in rows}:
                tick = realtime.quote(user, code)
                if tick and sent.get(code) != tick["ts"]:
                    await websocket.send_json(tick)
                    sent[code] = tick["ts"]
            await websocket.send_json({"type": "namuh_status", **realtime.status(user)})
            await asyncio.sleep(1)
    except (WebSocketDisconnect, OSError, HTTPException, RuntimeError):
        pass
    finally:
        try:
            await websocket.close()
        except RuntimeError:
            pass
