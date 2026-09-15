"""사용자별 연구 작업·불변 결과·일별 관찰 기록."""

import hashlib
import json
import time
import uuid

from core.errors import AppError
from repositories.db import get_db, transaction


class QuantError(AppError):
    status_code = 400


def encode(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(encode(value).encode()).hexdigest()


def unpack(row, *, result=False):
    value = dict(row)
    value["config"] = json.loads(value.pop("config_json"))
    raw = value.pop("result_json", None)
    if result:
        value["result"] = json.loads(raw) if raw else None
    value.pop("google_sub", None)
    return value


async def create_run(user, request_key, config):
    content = encode(config)
    async with transaction() as db:
        row = await (
            await db.execute("SELECT * FROM quant_runs WHERE google_sub=? AND request_key=?", (user, request_key))
        ).fetchone()
        if row:
            if row["config_json"] != content:
                raise QuantError("같은 요청 키로 다른 실험을 실행할 수 없습니다.")
            return unpack(row)
        active = await (
            await db.execute("SELECT COUNT(*) FROM quant_runs WHERE status IN ('queued','running')")
        ).fetchone()
        count = await (await db.execute("SELECT COUNT(*) FROM quant_runs WHERE google_sub=?", (user,))).fetchone()
        if active[0] >= 10 or count[0] >= 100:
            raise QuantError("연구 대기열 또는 보관 한도(사용자당 100개)에 도달했습니다.")
        rid = uuid.uuid4().hex
        await db.execute(
            "INSERT INTO quant_runs (id,google_sub,request_key,config_json,status,created_at) VALUES (?,?,?,?,'queued',?)",
            (rid, user, request_key, content, time.time()),
        )
    return await get_run(user, rid)


async def get_run(user, rid, *, result=True):
    db = await get_db()
    row = await (await db.execute("SELECT * FROM quant_runs WHERE id=? AND google_sub=?", (rid, user))).fetchone()
    if not row:
        raise QuantError("연구 기록을 찾을 수 없습니다.")
    return unpack(row, result=result)


async def list_runs(user):
    db = await get_db()
    rows = await (
        await db.execute(
            "SELECT id,config_json,status,error,created_at,finished_at,snapshot_id FROM quant_runs WHERE google_sub=? ORDER BY created_at DESC LIMIT 100",
            (user,),
        )
    ).fetchall()
    return [unpack(row) for row in rows]


async def cancel(user, rid):
    async with transaction() as db:
        await db.execute(
            "UPDATE quant_runs SET status='cancelled',finished_at=? WHERE id=? AND google_sub=? AND status IN ('queued','running')",
            (time.time(), rid, user),
        )
    return await get_run(user, rid)


async def claim():
    async with transaction() as db:
        row = await (
            await db.execute("SELECT * FROM quant_runs WHERE status='queued' ORDER BY created_at LIMIT 1")
        ).fetchone()
        if not row:
            return None
        await db.execute(
            "UPDATE quant_runs SET status='running',started_at=? WHERE id=? AND status='queued'",
            (time.time(), row["id"]),
        )
        return dict(row)


async def finish(rid, result=None, error=None):
    content = encode(result) if result is not None else None
    if content and len(content.encode()) > 8_000_000:
        raise QuantError("연구 결과가 보관 용량을 초과했습니다.")
    async with transaction() as db:
        await db.execute(
            "UPDATE quant_runs SET status=?,result_json=?,snapshot_id=?,error=?,finished_at=? WHERE id=? AND status='running'",
            (
                "failed" if error else "succeeded",
                content,
                result["snapshot"]["snapshot_id"] if result else None,
                error,
                time.time(),
                rid,
            ),
        )


async def recover():
    async with transaction() as db:
        await db.execute("UPDATE quant_runs SET status='queued',started_at=NULL WHERE status='running'")


async def set_watch(user, rid, enabled):
    run = await get_run(user, rid)
    if run["status"] != "succeeded":
        raise QuantError("완료된 연구만 관찰할 수 있습니다.")
    async with transaction() as db:
        count = await (
            await db.execute("SELECT COUNT(*) FROM quant_watches WHERE enabled=1 AND google_sub=?", (user,))
        ).fetchone()
        existing = await (
            await db.execute("SELECT enabled FROM quant_watches WHERE run_id=? AND google_sub=?", (rid, user))
        ).fetchone()
        if enabled and count[0] >= 5 and not (existing and existing[0]):
            raise QuantError("동시 관찰은 사용자당 5개까지 가능합니다.")
        await db.execute(
            "INSERT INTO quant_watches (run_id,google_sub,enabled,created_at) VALUES (?,?,?,?) ON CONFLICT(run_id) DO UPDATE SET enabled=excluded.enabled,error=NULL",
            (rid, user, int(enabled), time.time()),
        )


async def watches(user=None):
    db = await get_db()
    where, args = (
        ("w.google_sub=?", (user,))
        if user is not None
        else ("w.enabled=1 AND (w.checked_at IS NULL OR w.checked_at<?)", (time.time() - 3600,))
    )
    fields = "w.*" if user is not None else "w.*,r.config_json,r.result_json"
    limit = 100 if user is not None else 1
    rows = await (
        await db.execute(
            f"SELECT {fields} FROM quant_watches w JOIN quant_runs r ON r.id=w.run_id WHERE {where} ORDER BY w.checked_at,w.created_at LIMIT ?",
            (*args, limit),
        )
    ).fetchall()
    return [dict(row) for row in rows]


async def observations(user):
    db = await get_db()
    rows = await (
        await db.execute(
            "SELECT run_id,market_date,observed_at,payload_json FROM quant_observations WHERE google_sub=? ORDER BY observed_at DESC LIMIT 100",
            (user,),
        )
    ).fetchall()
    return [{**dict(row), "payload": json.loads(row["payload_json"])} for row in rows]


async def latest_observation(watch):
    db = await get_db()
    row = await (
        await db.execute(
            "SELECT payload_json FROM quant_observations WHERE run_id=? AND google_sub=? ORDER BY market_date DESC LIMIT 1",
            (watch["run_id"], watch["google_sub"]),
        )
    ).fetchone()
    return json.loads(row[0]) if row else None


async def record_observation(watch, payload=None, error=None):
    async with transaction() as db:
        active = await (
            await db.execute("SELECT enabled FROM quant_watches WHERE run_id=?", (watch["run_id"],))
        ).fetchone()
        if not active or not active[0]:
            return
        if payload is not None:
            await db.execute(
                "INSERT OR IGNORE INTO quant_observations (run_id,google_sub,market_date,observed_at,payload_json) VALUES (?,?,?,?,?)",
                (watch["run_id"], watch["google_sub"], payload["signal"]["date"], time.time(), encode(payload)),
            )
        await db.execute(
            "UPDATE quant_watches SET checked_at=?,error=? WHERE run_id=?", (time.time(), error, watch["run_id"])
        )
