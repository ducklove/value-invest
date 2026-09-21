"""로그인 사용자 전용 NH 키 등록·잔고 조회·연동 및 실시간 시세 전달."""

import asyncio
import json
import re
import time
from datetime import date
from typing import Annotated, Literal

from fastapi import APIRouter, Body, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from deps import get_current_user
from domain.broker_assets import ACCOUNT_PRODUCTS
from repositories import account_holdings, broker_activity, brokers
from repositories.broker_secrets import BrokerError, decrypt, encrypt
from services.brokers import kis_realtime, namuh, notifications, realtime
from services.brokers.sync import fetch_snapshot, sync_account

router = APIRouter()


async def user_id(request) -> str:
    user = await get_current_user(request)
    if not user:
        raise HTTPException(401, "로그인이 필요합니다.")
    return user["google_sub"]


def selection(user: str, payload: dict, provider: str = "namuh") -> dict:
    try:
        choice = json.loads(decrypt(str(payload.get("selection", ""))))
        if choice["user"] != user or choice["expires_at"] < time.time() or choice.get("provider", "namuh") != provider:
            raise ValueError
        product = payload.get("product", "stocks")
        if product not in ACCOUNT_PRODUCTS:
            raise ValueError
        choice["product"] = product
        return choice
    except (KeyError, ValueError, TypeError, BrokerError):
        raise BrokerError("계좌 선택이 만료되었거나 올바르지 않습니다. 키 확인을 다시 실행해 주세요.") from None


class KisCredential(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    app_key: Annotated[str, Field(min_length=8, max_length=2048)]
    app_secret: Annotated[str, Field(min_length=8, max_length=2048)]
    account_no: Annotated[str, Field(min_length=10, max_length=16)]
    environment: Literal["live", "mock"] = "live"
    hts_id: Annotated[str, Field(max_length=32, pattern=r"^[A-Za-z0-9_]*$")] = ""


@router.post("/api/portfolio/kis/credentials")
async def register_kis(request: Request, payload: object = Body(...)):
    from services.brokers import kis
    user = await user_id(request)
    try:
        payload = KisCredential.model_validate(payload)
    except ValidationError:
        # 기본 422의 input 필드에 키·시크릿이 재출력되지 않도록 직접 검증한다.
        raise BrokerError("한국투자증권 앱키·시크릿·계좌번호·HTS ID 입력 형식을 확인해 주세요.") from None
    account_no = re.sub(r"[-\s]", "", payload.account_no)
    if not re.fullmatch(r"[0-9]{8}01", account_no):
        raise BrokerError("한국투자증권 주식 계좌번호 8자리와 상품코드 01을 입력해 주세요.")
    cid = await brokers.store_credential(user, payload.app_key, payload.app_secret, provider="kis",
                                        environment=payload.environment, hts_id=payload.hts_id)
    await kis.token(user, cid, payload.environment)
    choice = {"account_no": account_no, "credential_id": cid, "provider": "kis", "environment": payload.environment,
              "user": user, "expires_at": time.time() + 900}
    # 토큰 인증만으로 계좌 소유를 확정하지 않는다. 미리보기/연결의 실제 잔고 조회가 검증한다.
    return {"accounts": [{"account_no": account_no, "environment": payload.environment, "selection": encrypt(json.dumps(choice))}]}


@router.post("/api/portfolio/accounts/{account_id}/kis/preview")
async def preview_kis(account_id: str, request: Request, payload: dict = Body(...)):
    user = await user_id(request)
    await account_holdings.require_account(user, account_id, writable=True)
    choice = selection(user, payload, "kis")
    choice["include_overseas"] = payload.get("include_overseas") is not False
    rows, balances = await fetch_snapshot(user, choice)
    return {"items": rows, "balances": balances}


@router.post("/api/portfolio/accounts/{account_id}/kis")
async def connect_kis(account_id: str, request: Request, payload: dict = Body(...)):
    user = await user_id(request)
    await account_holdings.require_account(user, account_id, writable=True)
    choice = selection(user, payload, "kis")
    choice["include_overseas"] = payload.get("include_overseas") is not False
    await fetch_snapshot(user, choice)
    await brokers.link_account(user, account_id, choice["credential_id"], choice["account_no"], choice["environment"],
                               choice["include_overseas"], choice["product"], provider="kis")
    return await sync_account(user, account_id)


async def require_provider(user: str, account_id: str, provider: str):
    if (await brokers.get_link(user, account_id)).get("provider", "namuh") != provider:
        raise BrokerError("계좌에 연결된 증권사가 일치하지 않습니다.")


@router.post("/api/portfolio/accounts/{account_id}/kis/sync")
async def sync_kis(account_id: str, request: Request):
    user = await user_id(request)
    await require_provider(user, account_id, "kis")
    return await sync_account(user, account_id)


@router.delete("/api/portfolio/accounts/{account_id}/kis")
async def disconnect_kis(account_id: str, request: Request):
    user = await user_id(request)
    await require_provider(user, account_id, "kis")
    await brokers.disconnect(user, account_id)
    return {"ok": True}


@router.post("/api/portfolio/namuh/credentials")
async def register(request: Request, payload: dict = Body(...)):
    user = await user_id(request)
    key, secret = payload.get("app_key"), payload.get("app_secret")
    if not all(isinstance(value, str) and 8 <= len(value.strip()) <= 2048 for value in (key, secret)):
        raise BrokerError("나무 앱키와 시크릿을 입력해 주세요.")
    cid = await brokers.store_credential(user, key.strip(), secret.strip())
    rows = await namuh.accounts(user, cid)
    return {"credential_id": cid, "accounts": [{"account_no": row["account_no"], "account_mask": "•••••••" + row["account_no"][-4:], "environment": row["environment"],
        "selection": encrypt(json.dumps({**row, "credential_id": cid, "user": user, "expires_at": time.time() + 900}))} for row in rows]}


@router.post("/api/portfolio/accounts/{account_id}/namuh/preview")
async def preview(account_id: str, request: Request, payload: dict = Body(...)):
    user = await user_id(request)
    await account_holdings.require_account(user, account_id, writable=True)
    choice = selection(user, payload)
    choice["include_overseas"] = payload.get("include_overseas") is not False
    rows, balances = await fetch_snapshot(user, choice)
    return {"items": rows, "balances": balances, "broker_snapshot": balances.get("_snapshot", {})}


@router.post("/api/portfolio/accounts/{account_id}/namuh")
async def connect(account_id: str, request: Request, payload: dict = Body(...)):
    user = await user_id(request)
    await account_holdings.ensure(user)
    choice = selection(user, payload)
    own = await namuh.accounts(user, choice["credential_id"])
    if {"account_no": choice["account_no"], "environment": choice["environment"]} not in own:
        raise BrokerError("현재 키의 계좌 목록에서 선택한 계좌를 확인하지 못했습니다.")
    await brokers.link_account(user, account_id, choice["credential_id"], choice["account_no"], choice["environment"], payload.get("include_overseas") is not False, choice["product"])
    return await sync_account(user, account_id, include_activity=True)


@router.post("/api/portfolio/accounts/{account_id}/namuh/sync")
async def sync(account_id: str, request: Request):
    user = await user_id(request)
    await require_provider(user, account_id, "namuh")
    return await sync_account(user, account_id, include_activity=True)


@router.delete("/api/portfolio/accounts/{account_id}/namuh")
async def disconnect(account_id: str, request: Request):
    user = await user_id(request)
    await require_provider(user, account_id, "namuh")
    await brokers.disconnect(user, account_id)
    return {"ok": True}


class ActivityNote(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    revision: Annotated[int, Field(ge=1, strict=True)]
    reason: Annotated[str, Field(max_length=500)] = ""
    kind: Literal["dividend", "interest", "other_income", "transfer", "internal", "trade", "fee", "review"]
    fx_rate: Annotated[float, Field(gt=0, le=1e7, allow_inf_nan=False, strict=True)] | None = None
    income_amount: Annotated[float, Field(ge=-1e15, le=1e15, allow_inf_nan=False, strict=True)] | None = None


@router.get("/api/portfolio/accounts/{account_id}/activity")
async def activity_history(account_id: str, request: Request, offset: Annotated[int, Query(ge=0)] = 0):
    return await broker_activity.history(await user_id(request), account_id, offset=offset)


@router.post("/api/portfolio/accounts/{account_id}/activity/sync")
async def activity_sync(account_id: str, request: Request, start: date, end: date):
    return await sync_account(await user_id(request), account_id, include_activity=True, start=start, end=end)


@router.patch("/api/portfolio/accounts/{account_id}/activity/{transaction_id}")
async def activity_note(account_id: str, transaction_id: int, request: Request, payload: ActivityNote):
    return await broker_activity.annotate(await user_id(request), account_id, transaction_id, **payload.model_dump())


@router.websocket("/ws/namuh")
@router.websocket("/ws/broker-accounts")
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
    last_accounts = None
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
            if any(row.get("provider", "namuh") == "namuh" for row in links):
                await websocket.send_json({"type": "namuh_status", **realtime.status(user), "notifications": notifications.status(user)})
            if any(row.get("provider") == "kis" for row in links):
                await websocket.send_json({"type": "kis_account_status", **kis_realtime.status(user)})
            revisions = [(link["account_id"], link["last_sync_at"], link.get("sync_error")) for link in links]
            if revisions != last_accounts:
                await websocket.send_json({"type": "accounts_changed", "accounts": [
                    {"account_id": aid, "synced_at": at, "error": error} for aid, at, error in revisions]})
                last_accounts = revisions
            await asyncio.sleep(1)
    except (WebSocketDisconnect, OSError, HTTPException, RuntimeError):
        pass
    finally:
        try:
            await websocket.close()
        except RuntimeError:
            pass
