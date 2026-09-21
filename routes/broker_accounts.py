"""로그인 사용자 전용 증권사 키 등록·잔고 조회·연동 및 실시간 시세 전달."""

import asyncio
import json
import time
from datetime import date
from typing import Annotated, Literal

from fastapi import APIRouter, Body, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from deps import get_current_user
from repositories import account_holdings, broker_activity, brokers
from repositories.broker_secrets import BrokerError, decrypt, encrypt
from services.brokers import realtime
from services.brokers.registry import catalog, get_adapter
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
        if product not in {item.id for item in get_adapter(provider).definition.products}:
            raise ValueError
        choice["product"] = product
        return choice
    except (KeyError, ValueError, TypeError, BrokerError):
        raise BrokerError("계좌 선택이 만료되었거나 올바르지 않습니다. 키 확인을 다시 실행해 주세요.") from None


class Credential(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    app_key: Annotated[str, Field(min_length=8, max_length=2048)]
    app_secret: Annotated[str, Field(min_length=8, max_length=2048)]
    account_no: Annotated[str, Field(max_length=16)] = ""
    environment: Literal["live", "mock"] = "live"
    hts_id: Annotated[str, Field(max_length=32, pattern=r"^[A-Za-z0-9_]*$")] = ""


@router.get("/api/portfolio/brokers")
async def broker_catalog(request: Request):
    await user_id(request)
    return catalog()


@router.post("/api/portfolio/{provider}/credentials")
async def register_broker(provider: str, request: Request, payload: object = Body(...)):
    user = await user_id(request)
    adapter = get_adapter(provider)
    try:
        inputs = Credential.model_validate(payload).model_dump()
    except ValidationError:
        # 422 응답의 input 필드에 키·시크릿을 다시 출력하지 않는다.
        raise BrokerError("증권사 앱키·시크릿·계좌번호·HTS ID 입력 형식을 확인해 주세요.") from None
    definition = adapter.definition
    if inputs["environment"] not in definition.environments or (inputs["account_no"] and not definition.account_input) or (inputs["hts_id"] and not definition.hts_id):
        raise BrokerError("이 증권사에서 지원하지 않는 연결 정보입니다.")
    inputs = adapter.validate_inputs(inputs)
    cid = await brokers.store_credential(user, inputs["app_key"], inputs["app_secret"], provider=provider,
                                        environment=inputs["environment"], hts_id=inputs["hts_id"])
    accounts = await adapter.discover(user, cid, inputs)
    return {"accounts": [{"account_no": row["account_no"], "environment": row["environment"],
        "selection": encrypt(json.dumps({**row, "credential_id": cid, "provider": provider, "user": user, "expires_at": time.time() + 900}))} for row in accounts]}


async def register_kis(request: Request, payload):
    """기존 내부 호출의 호환 진입점. HTTP 경로는 공통 핸들러가 처리한다."""
    return await register_broker("kis", request, payload)


async def require_provider(user: str, account_id: str, provider: str):
    get_adapter(provider)
    if (await brokers.get_link(user, account_id)).get("provider", "namuh") != provider:
        raise BrokerError("계좌에 연결된 증권사가 일치하지 않습니다.")


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


@router.post("/api/portfolio/accounts/{account_id}/{provider}/preview")
async def preview(account_id: str, provider: str, request: Request, payload: dict = Body(...)):
    user = await user_id(request)
    await account_holdings.require_account(user, account_id, writable=True)
    choice = selection(user, payload, provider)
    choice["include_overseas"] = payload.get("include_overseas") is not False
    rows, balances = await fetch_snapshot(user, choice)
    return {"items": rows, "balances": balances, "broker_snapshot": balances.get("_snapshot", {})}


@router.post("/api/portfolio/accounts/{account_id}/{provider}")
async def connect(account_id: str, provider: str, request: Request, payload: dict = Body(...)):
    user = await user_id(request)
    await account_holdings.require_account(user, account_id, writable=True)
    choice = selection(user, payload, provider)
    choice["include_overseas"] = payload.get("include_overseas") is not False
    # 인증·전체 잔고 조회를 먼저 검증한다. 닫기/미리보기는 계좌 원장을 변경하지 않는다.
    await fetch_snapshot(user, choice)
    await brokers.link_account(user, account_id, choice["credential_id"], choice["account_no"], choice["environment"],
                               choice["include_overseas"], choice["product"], provider=provider)
    return await sync_account(user, account_id, include_activity=True)


@router.post("/api/portfolio/accounts/{account_id}/{provider}/sync")
async def sync(account_id: str, provider: str, request: Request):
    user = await user_id(request)
    await require_provider(user, account_id, provider)
    return await sync_account(user, account_id, include_activity=True)


@router.delete("/api/portfolio/accounts/{account_id}/{provider}")
async def disconnect(account_id: str, provider: str, request: Request):
    user = await user_id(request)
    await require_provider(user, account_id, provider)
    await brokers.disconnect(user, account_id)
    return {"ok": True}


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
            for provider in {row.get("provider", "namuh") for row in links}:
                await websocket.send_json(get_adapter(provider).status(user))
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
