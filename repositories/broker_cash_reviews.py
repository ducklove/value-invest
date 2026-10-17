"""연동 잔고의 설명되지 않은 현금 변동. 실제 거래 원장이나 NAV를 변경하지 않는다."""

import json
from decimal import Decimal

from domain.broker_activity import stamp
from repositories.broker_secrets import BrokerError
from repositories.db import read_snapshot, transaction

# direction: 1=입금, -1=출금, 0=양방향. 사유 선택 자체를 기록하고 기타만 설명을 요구한다.
CATEGORIES = {
    "external_deposit": ("비관리 계좌에서 입금", 1),
    "external_withdrawal": ("비관리 계좌로 출금", -1),
    "distribution": ("배당·분배금 출금", -1),
    "living_expense": ("생활비·소비", -1),
    "gift": ("증여·지원금", 0),
    "loan": ("대출·상환", 0),
    "tax_fee": ("세금·수수료", -1),
    "refund": ("정정·환급·취소", 0),
    "transfer": ("일반 입출금", 0),
    "other": ("기타", 0),
    "managed_transfer": ("관리 계좌 간 이체", 0),
    "dividend": ("배당·분배금 입금", 1),
    "lending_fee": ("대여수수료 입금", 1),
    "interest": ("이자·예탁금이용료 입금", 1),
    "capital_gains_tax": ("양도소득세 출금", -1),
    "trade": ("매매 결제", 0),
    "exchange": ("환전", 0),
    "cash_management": ("CMA·RP 등 계좌 내 이동", 0),
}


def _decimal(value) -> Decimal:
    return Decimal(str(value or 0)).quantize(Decimal("0.000001"))


async def _evidence(db, user: str, aid: str) -> dict:
    """확정 원장만 사용. 외부 이체와 미확인·정정 거래는 사유를 추측하지 않는다."""
    rows = await (await db.execute(
        "SELECT id,kind,reason,data_json FROM broker_transactions WHERE google_sub=? AND account_id=?",
        (user, aid))).fetchall()
    result = {}
    for row in rows:
        data = json.loads(row["data_json"])
        kind, label = row["kind"], data.get("description", "")
        known = kind in {"dividend", "interest", "internal", "trade"}
        known |= kind == "other_income" and (bool(row["reason"]) or any(s in label for s in ("대여수수료", "대여료")))
        known |= kind == "fee" and (bool(row["reason"]) or "양도소득세" in label)
        known |= kind == "transfer" and bool(data.get("managed_account_id"))
        effects = {data["currency"]: data["net_amount"]} if known and data.get("net_amount") is not None else {}
        if effects and data["currency"] != "KRW" and data.get("domestic_tax_krw"):
            effects["KRW"] = -data["domestic_tax_krw"]
        result[str(row["id"])] = {"date": data["date"], "effects": effects}
    return result


async def _reconcile(db, user: str, aid: str, state: dict, evidence: dict, now: str):
    # 최초 기준 잔고에 포함된 원장은 다시 사용하지 않는다.
    baseline = json.loads(state["baseline_json"])
    pools = {}
    for key, new in evidence.items():
        if key in baseline:
            continue
        day = new.get("date")
        if not day or day < state["started_at"][:10]:
            continue
        for currency, amount in new["effects"].items():
            pools[day, currency, key] = _decimal(amount)
    rows = await (await db.execute(
        "SELECT * FROM broker_cash_reviews WHERE google_sub=? AND account_id=? AND fingerprint=? ORDER BY id",
        (user, aid, state["fingerprint"]))).fetchall()
    for row in rows:
        remaining = _decimal(row["amount"])
        candidates = [key for key, amount in pools.items() if amount and key[1] == row["currency"]
                      and row["from_at"][:10] <= key[0] <= row["to_at"][:10]]
        total = sum((pools[key] for key in candidates), Decimal(0))
        if total * remaining > 0 and abs(total) <= abs(remaining):
            # 한 구간에 여러 수입·세금·결제가 함께 반영된 경우 순액으로 대조한다.
            remaining -= total
            for key in candidates:
                pools[key] = Decimal(0)
        else:
            # 같은 날 여러 조회에 나눠 반영된 거래를 날짜 합계 하나로 묶어 놓치지 않는다.
            candidates.sort(key=lambda key: (pools[key] != remaining, key))
            for key in candidates:
                available = pools[key]
                if available * remaining > 0 and abs(available) <= abs(remaining):
                    remaining -= available
                    pools[key] = Decimal(0)
        if remaining != _decimal(row["unexplained"]):
            # 지연 수집으로 설명 대상액이 바뀌면 이전 답변은 이력에 남기고 재확인한다.
            await db.execute(
                "UPDATE broker_cash_reviews SET unexplained=?,category='',reason='',revision=revision+1,updated_at=? WHERE id=?",
                (float(remaining), now, row["id"]))


async def observe(db, user: str, link: dict, positions: list[dict], previous: list[dict]):
    """완전한 잔고와 같은 트랜잭션에서 관측한다. 최초·재연결은 기준 잔고만 저장한다."""
    if link["environment"] != "live" or link.get("product", "stocks") in {"krfuture", "gbfuture"}:
        return  # 선물 CASH 행은 평가액을 나타내는 합성 값이다.
    aid, now = link["account_id"], stamp()
    fingerprint = ":".join(str(link.get(k, "")) for k in ("provider", "account_fingerprint", "environment", "product", "include_overseas"))
    cash = {r["currency"]: float(r["quantity"]) for r in positions if r["stock_code"].startswith("CASH_")}
    state = await (await db.execute("SELECT * FROM broker_cash_review_state WHERE google_sub=? AND account_id=?", (user, aid))).fetchone()
    evidence = await _evidence(db, user, aid)
    if not state or state["fingerprint"] != fingerprint or not link["last_sync_at"]:
        await db.execute(
            "INSERT INTO broker_cash_review_state VALUES (?,?,?,?,?,?,?) ON CONFLICT(account_id) DO UPDATE SET "
            "fingerprint=excluded.fingerprint,started_at=excluded.started_at,last_at=excluded.last_at,cash_json=excluded.cash_json,baseline_json=excluded.baseline_json",
            (aid, user, fingerprint, now, now, json.dumps(cash), json.dumps(evidence)))
        return
    state = dict(state)
    old_cash = json.loads(state["cash_json"])
    def inventory(rows):
        return {(r["stock_code"], r["currency"]): (r["quantity"], r["avg_price"]) for r in rows if not r["stock_code"].startswith("CASH_")}
    changed = inventory(positions) != inventory(previous)
    for currency in old_cash.keys() | cash.keys():
        amount = _decimal(cash.get(currency)) - _decimal(old_cash.get(currency))
        if amount:
            await db.execute(
                "INSERT INTO broker_cash_reviews (google_sub,account_id,fingerprint,from_at,to_at,currency,amount,unexplained,positions_changed,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (user, aid, fingerprint, state["last_at"], now, currency, float(amount), float(amount), int(changed), now))
    await _reconcile(db, user, aid, state, evidence, now)
    await db.execute("UPDATE broker_cash_review_state SET cash_json=?,last_at=? WHERE google_sub=? AND account_id=?", (json.dumps(cash), now, user, aid))


async def history(user: str, *, pending: bool = True, offset: int = 0) -> dict:
    async with read_snapshot() as db:
        # 연결 해제한 계좌의 과거 기록은 자동 표시에서 제외하고 이력에서 보존한다.
        scope = ("AND r.unexplained!=0 AND r.category='' AND l.account_id IS NOT NULL AND l.last_sync_at IS NOT NULL "
                 "AND l.environment='live' AND l.product NOT IN ('krfuture','gbfuture') "
                 "AND r.fingerprint=s.fingerprint") if pending else ""
        query = ("FROM broker_cash_reviews r JOIN portfolio_accounts a ON a.account_id=r.account_id "
                 "LEFT JOIN broker_account_links l ON l.account_id=r.account_id AND l.google_sub=r.google_sub "
                 "LEFT JOIN broker_cash_review_state s ON s.account_id=r.account_id AND s.google_sub=r.google_sub "
                 "WHERE r.google_sub=? " + scope)
        count = (await (await db.execute("SELECT COUNT(*) " + query, (user,))).fetchone())[0]
        rows = await (await db.execute("SELECT r.*,a.name AS account_name " + query + " ORDER BY r.id DESC LIMIT 100 OFFSET ?", (user, offset))).fetchall()
        items = [{k: row[k] for k in row.keys() if k not in {"google_sub", "fingerprint"}} for row in rows]
        return {"items": items, "total": count, "has_more": offset + len(items) < count,
                "categories": [{"id": key, "label": label, "direction": direction} for key, (label, direction) in CATEGORIES.items()]}


async def annotate(user: str, review_id: int, *, revision: int, category: str, reason: str = "") -> dict:
    if category not in CATEGORIES or not isinstance(reason, str) or len(reason) > 500:
        raise BrokerError("사유를 확인해 주세요.")
    reason = reason.strip()
    if category in {"other", "transfer"} and not reason:
        raise BrokerError("일반 입출금·기타는 구체적인 사유를 입력해 주세요.")
    async with transaction() as db:
        row = await (await db.execute("SELECT * FROM broker_cash_reviews WHERE google_sub=? AND id=?", (user, review_id))).fetchone()
        if not row:
            raise BrokerError("예수금 변동을 찾을 수 없습니다.")
        if row["revision"] != revision:
            raise BrokerError("예수금 변동이 변경됐습니다. 새로 조회한 뒤 저장해 주세요.")
        direction = CATEGORIES[category][1]
        if not row["unexplained"] or direction * row["unexplained"] < 0:
            raise BrokerError("입금·출금 방향에 맞는 사유를 선택해 주세요.")
        now = stamp()
        await db.execute("UPDATE broker_cash_reviews SET category=?,reason=?,revision=revision+1,updated_at=? WHERE id=?", (category, reason, now, review_id))
        await db.execute("INSERT INTO broker_cash_review_notes (review_id,amount,category,reason,changed_at) VALUES (?,?,?,?,?)", (review_id, row["unexplained"], category, reason, now))
    return {"ok": True}
