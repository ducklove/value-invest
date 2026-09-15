"""사용자별 현선물 가정·호가 재생 보고서. 실제 계좌 원장을 변경하지 않는다."""

import json
import time
import uuid

from repositories.db import get_db, transaction
from repositories.quant import QuantError, encode


async def existing(user, key, payload):
    db = await get_db()
    row = await (
        await db.execute("SELECT * FROM quant_basis_runs WHERE google_sub=? AND request_key=?", (user, key))
    ).fetchone()
    if not row:
        return None
    if row["input_json"] != encode(payload):
        raise QuantError("동일 요청 키의 연구 입력을 바꿀 수 없습니다.")
    return unpack(row)


def unpack(row):
    return {
        "id": row["id"],
        "created_at": row["created_at"],
        "input": json.loads(row["input_json"]),
        "result": json.loads(row["result_json"]),
    }


async def save(user, key, payload, result):
    async with transaction() as db:
        row = await (
            await db.execute("SELECT * FROM quant_basis_runs WHERE google_sub=? AND request_key=?", (user, key))
        ).fetchone()
        if row:
            if row["input_json"] != encode(payload):
                raise QuantError("동일 요청 키의 연구 입력을 바꿀 수 없습니다.")
            return unpack(row)
        count = await (await db.execute("SELECT COUNT(*) FROM quant_basis_runs WHERE google_sub=?", (user,))).fetchone()
        if count[0] >= 50:
            raise QuantError("현선물 연구 보관 한도(50개)에 도달했습니다.")
        rid = uuid.uuid4().hex
        await db.execute(
            "INSERT INTO quant_basis_runs VALUES (?,?,?,?,?,?)",
            (rid, user, key, encode(payload), encode(result), time.time()),
        )
    return await get(user, rid)


async def get(user, rid):
    db = await get_db()
    row = await (await db.execute("SELECT * FROM quant_basis_runs WHERE id=? AND google_sub=?", (rid, user))).fetchone()
    if not row:
        raise QuantError("현선물 연구 기록을 찾을 수 없습니다.")
    return unpack(row)


async def listing(user):
    db = await get_db()
    rows = await (
        await db.execute(
            "SELECT id,created_at,json_extract(result_json,'$.config.contract') AS contract,"
            "json_extract(result_json,'$.mode') AS mode FROM quant_basis_runs WHERE google_sub=? ORDER BY created_at DESC LIMIT 50",
            (user,),
        )
    ).fetchall()
    return [dict(r) for r in rows]
