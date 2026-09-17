"""가상 운용 상태와 체결 근거 원장. 사용자·감시 generation을 쓰기 시점에 확인."""

import json
from datetime import datetime

from repositories.db import get_db, read_snapshot, transaction
from repositories.quant import QuantError, encode
from services.quant import paper
from services.quant.scanner_model import KST


async def get(user, limit=60):
    async with read_snapshot() as db:
        row = await (await db.execute("SELECT state_json FROM quant_paper_accounts WHERE google_sub=?", (user,))).fetchone()
        if not row:
            return None
        state = json.loads(row[0])
        events = await (await db.execute(
            "SELECT id,payload_json FROM quant_paper_events WHERE google_sub=? ORDER BY id DESC LIMIT ?", (user, limit))).fetchall()
    return {"state": state, "summary": paper.summary(state),
            "events": [{"id": r[0], **json.loads(r[1])} for r in events]}


async def save(db, user, state, events):
    await db.execute("UPDATE quant_paper_accounts SET state_json=? WHERE google_sub=?", (encode(state), user))
    if events:
        await db.executemany("INSERT INTO quant_paper_events(google_sub,payload_json) VALUES(?,?)",
                             [(user, encode(e)) for e in events])


async def start(user, config):
    async with transaction() as db:
        scanner = await (await db.execute("SELECT config_json FROM quant_scanners WHERE google_sub=?", (user,))).fetchone()
        if not scanner:
            raise QuantError("먼저 감시 계좌를 설정하세요.")
        sc = json.loads(scanner[0])
        if not sc["enabled"]:
            raise QuantError("먼저 시장 감시를 시작하세요.")
        row = await (await db.execute("SELECT state_json FROM quant_paper_accounts WHERE google_sub=?", (user,))).fetchone()
        now = datetime.now(KST).timestamp()
        if row:
            state = json.loads(row[0])
            if state["account_id"] != sc["account_id"] or state["config"] != config.model_dump():
                raise QuantError("기존 가상 원장은 같은 계좌·설정으로 재개하세요. 초기 자금과 이력을 덮어쓰지 않습니다.")
            state["enabled"] = True
            await save(db, user, state, [])
        else:
            state = paper.initial(config, sc["account_id"], now)
            await db.execute("INSERT INTO quant_paper_accounts VALUES(?,?)", (user, encode(state)))


async def pause(user):
    async with transaction() as db:
        row = await (await db.execute("SELECT state_json FROM quant_paper_accounts WHERE google_sub=?", (user,))).fetchone()
        if row:
            state = json.loads(row[0]);state["enabled"] = False
            now = datetime.now(KST)
            events = [paper.event("cancel", code, now, reason="사용자가 신규 가상 진입 중지", pending=p)
                      for code, p in state["pending"].items() if p["kind"] == "entry"]
            state["cancels"] += len(events)
            pending = {c: p for c, p in state["pending"].items() if p["kind"] == "exit"}
            state["pending"] = pending
            await save(db, user, state, events)


async def process(user, generation, row=None, spot=None, future=None, config=None, now=None):
    now = now or datetime.now(KST)
    async with transaction() as db:
        guard = await (await db.execute("SELECT generation,config_json FROM quant_scanners WHERE google_sub=?", (user,))).fetchone()
        raw = await (await db.execute("SELECT state_json FROM quant_paper_accounts WHERE google_sub=?", (user,))).fetchone()
        if not guard or guard[0] != generation or not raw:
            return None
        sc = json.loads(guard[1]);state = json.loads(raw[0])
        if not sc["enabled"] or state["account_id"] != sc["account_id"]:
            return None
        events = paper.step(state, row, spot, future, config, now) if row else paper.expire_pending(state, now)
        if events or now.timestamp() - state.get("saved_at", 0) >= 5:
            state["saved_at"] = now.timestamp()
            await save(db, user, state, events)
    return state


async def assert_account(user, account_id):
    db = await get_db()
    row = await (await db.execute("SELECT state_json FROM quant_paper_accounts WHERE google_sub=?", (user,))).fetchone()
    if row and json.loads(row[0])["account_id"] != account_id:
        raise QuantError("가상 원장이 연결된 감시 계좌를 유지하세요.")
