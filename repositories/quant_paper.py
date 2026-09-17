"""가상 원장 저장소. 상태 변환과 이벤트를 같은 generation 검사·트랜잭션으로 저장한다."""

import json

from repositories.db import read_snapshot, transaction
from repositories.quant import encode


async def get(user, limit=60):
    async with read_snapshot() as db:
        row = await (await db.execute("SELECT state_json FROM quant_paper_accounts WHERE google_sub=?", (user,))).fetchone()
        if not row:
            return None
        events = await (await db.execute(
            "SELECT id,payload_json FROM quant_paper_events WHERE google_sub=? ORDER BY id DESC LIMIT ?", (user, limit))).fetchall()
    return {"state": json.loads(row[0]), "events": [{"id": r[0], **json.loads(r[1])} for r in events]}


async def update(user, transform, *, generation=None):
    """transform은 I/O 없는 동기 함수이며 (새 상태, 이벤트, 저장 여부)를 반환한다."""
    async with transaction() as db:
        scanner = await (await db.execute("SELECT generation,config_json FROM quant_scanners WHERE google_sub=?", (user,))).fetchone()
        if generation is not None and (not scanner or scanner[0] != generation):
            return None
        raw = await (await db.execute("SELECT state_json FROM quant_paper_accounts WHERE google_sub=?", (user,))).fetchone()
        result = transform(json.loads(scanner[1]) if scanner else None, json.loads(raw[0]) if raw else None)
        if result is None:
            return None
        state, events, write = result
        if write:
            await db.execute("INSERT INTO quant_paper_accounts VALUES(?,?) ON CONFLICT(google_sub) DO UPDATE SET state_json=excluded.state_json", (user, encode(state)))
            if events:
                await db.executemany("INSERT INTO quant_paper_events(google_sub,payload_json) VALUES(?,?)",
                                     [(user, encode(e)) for e in events])
        return state
