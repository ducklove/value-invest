"""순회 설정·관측·기회 기록. 계좌와 주문 테이블은 변경하지 않는다."""

import json
import time
import uuid

from repositories.db import get_db, transaction
from repositories.quant import encode


async def settings(user=None):
    db = await get_db()
    rows = await (await db.execute(
        "SELECT * FROM quant_scanners" + (" WHERE google_sub=?" if user else ""),
        (user,) if user else (),
    )).fetchall()
    return [{**dict(r), "config": json.loads(r["config_json"]), "progress": json.loads(r["progress_json"])} for r in rows]


async def configure(user, config):
    async with transaction() as db:
        await db.execute(
            "INSERT INTO quant_scanners(google_sub,config_json,generation) VALUES(?,?,?) "
            "ON CONFLICT(google_sub) DO UPDATE SET config_json=excluded.config_json,generation=excluded.generation,progress_json='{}'",
            (user, encode(config), str(uuid.uuid4())),
        )
        # 다른 기준의 관측을 신규 후보로 재사용하지 않는다. 이벤트 이력은 보존한다.
        await db.execute("DELETE FROM quant_scan_rows WHERE google_sub=?", (user,))


async def rows(user):
    db = await get_db()
    data = await (await db.execute("SELECT payload_json FROM quant_scan_rows WHERE google_sub=?", (user,))).fetchall()
    return [json.loads(r[0]) for r in data]


async def stop(user):
    async with transaction() as db:
        await db.execute("UPDATE quant_scanners SET config_json=json_set(config_json,'$.enabled',json('false')),generation=? WHERE google_sub=?",
                         (str(uuid.uuid4()), user))


async def record(user, generation, progress, row=None, event=None):
    async with transaction() as db:
        result = await db.execute("UPDATE quant_scanners SET progress_json=? WHERE google_sub=? AND generation=?",
                                  (encode(progress), user, generation))
        if not result.rowcount:
            return False
        if row:
            await db.execute("INSERT INTO quant_scan_rows VALUES(?,?,?) ON CONFLICT(google_sub,contract) DO UPDATE SET payload_json=excluded.payload_json",
                             (user, row["contract"], encode(row)))
        if event:
            await db.execute("INSERT INTO quant_scan_events(google_sub,contract,observed_at,payload_json) VALUES(?,?,?,?)",
                             (user, event["contract"], time.time(), encode(event)))
            await db.execute("DELETE FROM quant_scan_events WHERE google_sub=? AND id NOT IN "
                             "(SELECT id FROM quant_scan_events WHERE google_sub=? ORDER BY id DESC LIMIT 3000)", (user, user))
    return True


async def events(user, limit=100):
    db = await get_db()
    data = await (await db.execute("SELECT id,observed_at,payload_json FROM quant_scan_events WHERE google_sub=? ORDER BY id DESC LIMIT ?",
                                   (user, min(limit, 3000)))).fetchall()
    return [{"id": r[0], "observed_at": r[1], **json.loads(r[2])} for r in data]
