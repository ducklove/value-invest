"""가상 운용의 시작·중지·계산을 저장소의 원자적 상태 변환에 연결한다."""

from datetime import datetime

from repositories import quant_paper
from repositories.quant import QuantError
from services.quant import paper
from services.quant.scanner_model import KST


async def get(user, limit=60):
    data = await quant_paper.get(user, limit)
    return {**data, "summary": paper.summary(data["state"])} if data else None


async def start(user, config):
    def apply(sc, state):
        if not sc or not sc["enabled"]:
            raise QuantError("먼저 감시 계좌를 설정하고 시장 감시를 시작하세요.")
        if state:
            if state["account_id"] != sc["account_id"] or state["config"] != config.model_dump():
                raise QuantError("기존 가상 원장은 같은 계좌·설정으로 재개하세요. 초기 자금과 이력을 덮어쓰지 않습니다.")
            state["enabled"] = True
        else:
            state = paper.initial(config, sc["account_id"], datetime.now(KST).timestamp())
        return state, [], True
    await quant_paper.update(user, apply)


async def pause(user):
    def apply(sc, state):
        if not state:
            return None
        state["enabled"] = False
        now = datetime.now(KST)
        events = [paper.event("cancel", code, now, reason="사용자가 신규 가상 진입 중지", pending=p)
                  for code, p in state["pending"].items() if p["kind"] == "entry"]
        state["cancels"] += len(events)
        state["pending"] = {c: p for c, p in state["pending"].items() if p["kind"] == "exit"}
        return state, events, True
    await quant_paper.update(user, apply)


async def process(user, generation, row=None, spot=None, future=None, config=None, now=None):
    now = now or datetime.now(KST)

    def apply(sc, state):
        if not state or not sc or not sc["enabled"] or state["account_id"] != sc["account_id"]:
            return None
        events = paper.step(state, row, spot, future, config, now) if row else paper.expire_pending(state, now)
        write = bool(events) or now.timestamp() - state.get("saved_at", 0) >= 5
        if write:
            state["saved_at"] = now.timestamp()
        return state, events, write
    return await quant_paper.update(user, apply, generation=generation)


async def assert_account(user, account_id):
    data = await quant_paper.get(user, limit=0)
    if data and data["state"]["account_id"] != account_id:
        raise QuantError("가상 원장이 연결된 감시 계좌를 유지하세요.")
