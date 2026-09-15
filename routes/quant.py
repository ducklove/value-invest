"""로그인 사용자 전용 퀀트 연구 API. 주문 활성화 API는 제공하지 않는다."""

from fastapi import APIRouter, Body, HTTPException, Request

from deps import get_current_user
from repositories import quant, quant_forward
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
    user = await user_id(request)
    return {**await quant.get_run(user, rid), "forward": await quant_forward.get(user, rid)}


@router.post("/runs/{rid}/forward")
async def forward(rid: str, request: Request, enabled: bool = Body(..., embed=True)):
    user = await user_id(request)
    if enabled:
        run = await quant.get_run(user, rid)
        service.verify(run.get("result") or {}, run["config"])
        await quant_forward.start(user, rid)
    else:
        await quant_forward.stop(user, rid)
    return {"forward": await quant_forward.get(user, rid)}


@router.post("/runs/{rid}/cancel")
async def cancel(rid: str, request: Request):
    return await quant.cancel(await user_id(request), rid)


@router.post("/runs/{rid}/watch")
async def watch(rid: str, request: Request, enabled: bool = Body(..., embed=True)):
    user = await user_id(request)
    if enabled:
        run = await quant.get_run(user, rid)
        expected = service.EXPECTED_ENGINES.get(run["config"]["strategy"])
        if (run.get("result") or {}).get("engine_version") != expected:
            raise quant.QuantError("현재 엔진으로 새 연구를 실행한 후 관찰해 주세요.")
    await quant.set_watch(user, rid, enabled)
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
