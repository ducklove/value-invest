"""사전 고정 전진 평가. 중지한 평가의 기간·설정을 바꾸거나 다시 시작하지 않는다."""

import json
import time
from datetime import datetime, timedelta, timezone

from repositories import quant
from repositories.db import get_db, transaction


async def start(user, rid):
    run = await quant.get_run(user, rid)
    if run["status"] != "succeeded":
        raise quant.QuantError("완료된 연구만 전진 평가할 수 있습니다.")
    # 오늘 장중에 본 정보와 겹치지 않도록 다음 한국 날짜부터 고정한다.
    start_date = str(datetime.now(timezone(timedelta(hours=9))).date() + timedelta(days=1))
    async with transaction() as db:
        existing = await (await db.execute("SELECT status FROM quant_forward WHERE run_id=?", (rid,))).fetchone()
        if existing:
            if existing[0] == "stopped":
                raise quant.QuantError("중지한 평가는 보존됩니다. 새 연구에서 별도 평가를 시작해 주세요.")
            return
        count = await (
            await db.execute("SELECT COUNT(*) FROM quant_forward WHERE google_sub=? AND status='active'", (user,))
        ).fetchone()
        if count[0] >= 5:
            raise quant.QuantError("활성 전진 평가는 사용자당 5개까지 가능합니다.")
        await db.execute(
            "INSERT INTO quant_forward (run_id,google_sub,status,start_date,created_at) VALUES (?,?,'active',?,?)",
            (rid, user, start_date, time.time()),
        )


async def stop(user, rid):
    await quant.get_run(user, rid)
    async with transaction() as db:
        await db.execute(
            "UPDATE quant_forward SET status='stopped',stopped_at=? WHERE run_id=? AND google_sub=? AND status='active'",
            (time.time(), rid, user),
        )


async def get(user, rid):
    await quant.get_run(user, rid)
    db = await get_db()
    row = await (
        await db.execute("SELECT * FROM quant_forward WHERE run_id=? AND google_sub=?", (rid, user))
    ).fetchone()
    if not row:
        return None
    result = dict(row)
    result.pop("google_sub")
    raw = result.pop("payload_json")
    result["payload"] = json.loads(raw) if raw else None
    return result


async def due():
    db = await get_db()
    row = await (
        await db.execute(
            "SELECT f.*,r.config_json,r.result_json FROM quant_forward f JOIN quant_runs r ON r.id=f.run_id WHERE f.status='active' AND (f.checked_at IS NULL OR f.checked_at<?) ORDER BY f.checked_at,f.created_at LIMIT 1",
            (time.time() - 3600,),
        )
    ).fetchone()
    return dict(row) if row else None


async def save(session, payload=None, error=None):
    content = quant.encode(payload) if payload is not None else None
    if content and len(content.encode()) > 8_000_000:
        raise quant.QuantError("전진 평가 원장이 보관 용량을 초과했습니다.")
    async with transaction() as db:
        # 중지가 늦은 응답보다 우선하며, 실패 시 마지막 검증 원장을 보존한다.
        await db.execute(
            "UPDATE quant_forward SET checked_at=?,error=?,payload_json=COALESCE(?,payload_json) WHERE run_id=? AND status='active'",
            (time.time(), error, content, session["run_id"]),
        )
