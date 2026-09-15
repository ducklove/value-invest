"""로그인 사용자 전용 퀀트 연구 API. 주문 활성화 API는 제공하지 않는다."""

from fastapi import APIRouter, Body, HTTPException, Request

from deps import get_current_user
from repositories import quant
from services.quant import service
from services.quant.models import RunRequest

router = APIRouter(prefix="/api/quant")


async def user_id(request):
    user = await get_current_user(request)
    if not user:
        raise HTTPException(401, "로그인이 필요합니다.")
    return user["google_sub"]


@router.get("/capabilities")
async def capabilities(request: Request):
    await user_id(request)
    return await service.capabilities()


@router.get("/runs")
async def runs(request: Request):
    return {"runs": await quant.list_runs(await user_id(request))}


@router.post("/runs", status_code=202)
async def create(request: Request, payload: RunRequest):
    return await quant.create_run(await user_id(request), payload.request_key, payload.config.model_dump(mode="json"))


@router.get("/runs/{rid}")
async def detail(rid: str, request: Request):
    return await quant.get_run(await user_id(request), rid)


@router.post("/runs/{rid}/cancel")
async def cancel(rid: str, request: Request):
    return await quant.cancel(await user_id(request), rid)


@router.post("/runs/{rid}/watch")
async def watch(rid: str, request: Request, enabled: bool = Body(..., embed=True)):
    await quant.set_watch(await user_id(request), rid, enabled)
    return {"ok": True, "enabled": enabled, "mode": "signal_observation"}


@router.get("/observations")
async def observations(request: Request):
    user = await user_id(request)
    watches = await quant.watches(user)
    return {
        "watches": [
            {k: v for k, v in w.items() if k not in {"google_sub", "config_json", "result_json"}} for w in watches
        ],
        "observations": [
            {k: v for k, v in row.items() if k != "payload_json"} for row in await quant.observations(user)
        ],
    }
