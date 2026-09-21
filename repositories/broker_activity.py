"""증권사 수입·입출금 원장. 잔고는 증권사 스냅샷만 변경한다."""

import json
import math

from domain.broker_activity import INCOME_KINDS, KINDS, stamp
from repositories import account_holdings, brokers
from repositories.broker_secrets import BrokerError
from repositories.db import get_db, transaction


async def state(user: str, aid: str) -> dict | None:
    db = await get_db()
    row = await (await db.execute("SELECT * FROM broker_activity_state WHERE google_sub=? AND account_id=?", (user, aid))).fetchone()
    return dict(row) if row else None


async def set_error(user: str, aid: str, message: str):
    async with transaction() as db:
        link = await brokers.get_link(user, aid)
        await db.execute("INSERT INTO broker_activity_state (google_sub,account_id,account_fingerprint,started_at,error) VALUES (?,?,?,?,?) "
                         "ON CONFLICT(account_id) DO UPDATE SET error=excluded.error",
                         (user, aid, link["account_fingerprint"], stamp(), message))


async def _project(db, row: dict):
    user, aid, data = row["google_sub"], row["account_id"], json.loads(row["data_json"])
    kind, fx = row["kind"], data["fx_rate"]
    net = data["net_amount"]
    target_flow = round(net * fx, 2) if kind == "transfer" and fx and net and not row["baseline"] else 0
    difference = round(target_flow - row["projected_flow"], 2)
    if difference:
        now = stamp()
        # 외부 자금만 NAV 좌수에 반영한다. 현금은 변경하지 않는다.
        # 이미 정산된 원거래를 덮어쓰지 않고 수정 차액을 현재 정산에 반영한다.
        cursor = await db.execute(
            "INSERT INTO portfolio_cashflows (google_sub,date,type,amount,memo,created_at,account_id) VALUES (?,?,?,?,?,?,?)",
            (user, now[:10], "deposit" if difference > 0 else "withdrawal", abs(difference),
             ("NH 입출금 · " + (row["reason"] or data["description"]))[:500], now, aid))
        await db.execute("INSERT INTO broker_cashflow_links VALUES (?,?)", (cursor.lastrowid, row["id"]))
    amount = None
    income = data["income_amount"] if kind in INCOME_KINDS else (-net if kind == "fee" and net is not None else None)
    if income is not None and income != 0 and fx:
        amount = round(income * fx, 2)
        # 예전에 수동 수취한 같은 계좌 배당은 자동으로 다시 집계하지 않는다.
        receipts = await (await db.execute("SELECT result_json FROM portfolio_dividend_receipts WHERE google_sub=?", (user,))).fetchall()
        duplicate = any(r.get("account_id") == aid and r.get("received_date") == data["date"]
                        and r.get("stock_code") == data["stock_code"] and r.get("currency") == data["currency"]
                        and r.get("net_amount") == income for item in receipts if (r := json.loads(item["result_json"])))
        if duplicate:
            amount = None
    await db.execute("UPDATE broker_transactions SET projected_flow=?,income_krw=? WHERE id=?", (target_flow, amount, row["id"]))


async def store(user: str, link: dict, entries: list[dict]):
    """호출자의 잔고 교체 트랜잭션에 참여. 전 페이지 조회 완료 후에만 적용한다."""
    async with transaction() as db:
        old_state = await state(user, link["account_id"])
        initial = not old_state or not old_state["last_import_at"] or old_state["account_fingerprint"] != link["account_fingerprint"]
        now = stamp()
        changed = 0
        for data in entries:
            previous = await (await db.execute("SELECT * FROM broker_transactions WHERE google_sub=? AND source_key=?", (user, data["source_key"]))).fetchone()
            if previous and previous["source_revision"] == data["source_revision"]:
                continue
            baseline = initial or data["date"] < old_state["started_at"][:10]
            if previous:
                # 증권사 정정 내용을 자동 덮어쓰되 사유는 보존하고 분류 재확인을 요청한다.
                await db.execute("UPDATE broker_transactions SET data_json=?,source_revision=?,kind='review',revision=revision+1,updated_at=? WHERE id=?",
                                 (json.dumps(data, ensure_ascii=False), data["source_revision"], now, previous["id"]))
                tid = previous["id"]
            else:
                cursor = await db.execute(
                    "INSERT INTO broker_transactions (google_sub,account_id,source_key,source_revision,data_json,kind,baseline,created_at,updated_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (user, link["account_id"], data["source_key"], data["source_revision"], json.dumps(data, ensure_ascii=False),
                     data["auto_kind"], int(baseline), now, now))
                tid = cursor.lastrowid
            row = dict(await (await db.execute("SELECT * FROM broker_transactions WHERE id=?", (tid,))).fetchone())
            await _project(db, row)
            changed += 1
        await db.execute("INSERT INTO broker_activity_state (google_sub,account_id,account_fingerprint,started_at,last_import_at) VALUES (?,?,?,?,?) "
                         "ON CONFLICT(account_id) DO UPDATE SET account_fingerprint=excluded.account_fingerprint,started_at=excluded.started_at,"
                         "last_import_at=excluded.last_import_at,error=NULL",
                         (user, link["account_id"], link["account_fingerprint"], now if initial else old_state["started_at"], now))
        return changed


async def history(user: str, aid: str, limit: int = 200, offset: int = 0) -> dict:
    await account_holdings.require_account(user, aid)
    db = await get_db()
    rows = await (await db.execute("SELECT * FROM broker_transactions WHERE google_sub=? AND account_id=? ORDER BY json_extract(data_json,'$.date') DESC,id DESC LIMIT ? OFFSET ?",
                                   (user, aid, limit + 1, offset))).fetchall()
    items = [{**json.loads(row["data_json"]), "id": row["id"], "kind": row["kind"], "reason": row["reason"],
              "revision": row["revision"], "baseline": bool(row["baseline"])} for row in rows[:limit]]
    totals = await (await db.execute(
        "SELECT kind,json_extract(data_json,'$.currency') AS currency,SUM(CAST(json_extract(data_json,'$.income_amount') AS REAL)) AS amount "
        "FROM broker_transactions WHERE google_sub=? AND account_id=? AND kind IN ('dividend','interest','other_income') GROUP BY kind,currency",
        (user, aid))).fetchall()
    progress = await state(user, aid)
    return {"items": items, "has_more": len(rows) > limit, "totals": [dict(row) for row in totals],
            "state": {key: progress[key] for key in ("started_at", "last_import_at", "error")} if progress else None}


async def annotate(user: str, aid: str, tid: int, *, revision: int, reason: str, kind: str,
                   fx_rate: float | None = None, income_amount: float | None = None):
    if kind not in KINDS or len(reason) > 500 or (fx_rate is not None and (isinstance(fx_rate, bool) or not math.isfinite(fx_rate) or not 0 < fx_rate <= 1e7)):
        raise BrokerError("분류·사유·환율을 확인해 주세요.")
    async with transaction() as db:
        await account_holdings.require_account(user, aid)
        found = await (await db.execute("SELECT * FROM broker_transactions WHERE google_sub=? AND account_id=? AND id=?", (user, aid, tid))).fetchone()
        if not found:
            raise BrokerError("거래내역을 찾을 수 없습니다.")
        row = dict(found)
        if row["revision"] != revision:
            raise BrokerError("거래내역이 변경됐습니다. 새로 조회한 뒤 저장해 주세요.")
        data = json.loads(row["data_json"])
        net = data["net_amount"]
        if kind in INCOME_KINDS and (net is None or net == 0):
            raise BrokerError("실제 입출금액이 확인된 거래만 수입 또는 수입 취소로 분류할 수 있습니다.")
        if kind in INCOME_KINDS:
            income = income_amount if income_amount is not None else data["income_amount"]
            if income is None or isinstance(income, bool) or not math.isfinite(income) or not 0 < income / net <= 1:
                raise BrokerError("원금을 제외한 세후 수입액을 확인해 입력해 주세요. 취소 수입은 음수로 입력합니다.")
            data["income_amount"] = income
        if kind in {"transfer", "fee"} and (net is None or not net or (kind == "fee" and net > 0)):
            raise BrokerError("예수금 증감액과 분류가 맞지 않습니다.")
        if kind != row["kind"] and not reason.strip():
            raise BrokerError("분류를 변경하는 사유를 입력해 주세요.")
        if data["currency"] != "KRW" and fx_rate is not None:
            data["fx_rate"] = fx_rate
        row.update(kind=kind, reason=reason.strip(), data_json=json.dumps(data, ensure_ascii=False))
        await _project(db, row)
        await db.execute("UPDATE broker_transactions SET kind=?,reason=?,data_json=?,revision=revision+1,updated_at=? WHERE id=?",
                         (kind, row["reason"], row["data_json"], stamp(), tid))
        await db.execute("INSERT INTO broker_transaction_notes (transaction_id,kind,reason,changed_at) VALUES (?,?,?,?)", (tid, kind, row["reason"], stamp()))
    return {"ok": True}
