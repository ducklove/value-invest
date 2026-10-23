"""연동 잔고의 설명되지 않은 현금 변동. 실제 거래 원장이나 NAV를 변경하지 않는다."""

import json
from collections import defaultdict
from decimal import Decimal

from domain.broker_activity import stamp
from domain.cash_review import minor_limit
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


def _fingerprint(link: dict) -> str:
    return ":".join(str(link.get(k, "")) for k in ("provider", "account_fingerprint", "environment", "product", "include_overseas"))


async def store_evidence(db, user: str, link: dict, batch):
    """조회가 끝난 기간을 통째로 교체한다. 거래번호를 만들거나 수입·현금을 가산하지 않는다."""
    fp = _fingerprint(link)
    await db.execute("DELETE FROM broker_cash_review_evidence WHERE google_sub=? AND account_id=? AND fingerprint=? AND booked_date BETWEEN ? AND ? "
                     "AND kind NOT IN ('trade_buy','trade_sell','trade_charges')",
                     (user, link["account_id"], fp, batch.start, batch.end))
    for row in batch.cash_summaries:
        await db.execute("INSERT INTO broker_cash_review_evidence VALUES (?,?,?,?,?,?,?,?)",
                         (user, link["account_id"], fp, row["booked_date"], row["date"], row["currency"], row["kind"], row["amount"]))


async def store_trades(db, user: str, link: dict, rows: list[dict]):
    """당일 누적 합계를 교체한다. 배당 기간 재조회가 이전 날짜의 매매 합계를 지우지 않는다."""
    for row in rows:
        await db.execute("INSERT INTO broker_cash_review_evidence VALUES (?,?,?,?,?,?,?,?) "
                         "ON CONFLICT DO UPDATE SET amount=excluded.amount",
                         (user, link["account_id"], _fingerprint(link), row["booked_date"], row["date"], row["currency"], row["kind"], row["amount"]))


async def _evidence(db, user: str, aid: str, fp: str) -> dict:
    """확정 원장만 사용. 외부 이체와 미확인·정정 거래는 사유를 추측하지 않는다."""
    rows = await (await db.execute(
        "SELECT id,kind,reason,data_json FROM broker_transactions WHERE google_sub=? AND account_id=?",
        (user, aid))).fetchall()
    result = {}
    ledger_groups = set()
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
        if effects:
            ledger_groups.add((data["date"], data["currency"], kind))
    summaries = await (await db.execute("SELECT * FROM broker_cash_review_evidence WHERE google_sub=? AND account_id=? AND fingerprint=?",
                                       (user, aid, fp))).fetchall()
    for row in summaries:
        if (row["date"], row["currency"], row["kind"]) in ledger_groups:
            continue  # 같은 수입이 기존 확정 원장에도 있으면 합계로 다시 설명하지 않는다.
        key = ":".join(("summary", row["booked_date"], row["date"], row["currency"], row["kind"]))
        result[key] = {"date": row["date"], "effects": {row["currency"]: row["amount"]}, "summary": True}
    return result


async def _reconcile(db, user: str, aid: str, state: dict, evidence: dict, now: str):
    # 최초 기준 잔고에 포함된 원장은 다시 사용하지 않는다.
    baseline = json.loads(state["baseline_json"])
    pools = {}
    for key, new in evidence.items():
        if key in baseline and not new.get("summary"):
            continue
        day = new.get("date")
        if not day or day < state["started_at"][:10]:
            continue
        for currency, amount in new["effects"].items():
            if new.get("summary"):
                # 같은 날 기준 잔고 이후 합계가 늘어난 부분만 사용한다.
                difference = _decimal(amount) - _decimal(baseline.get(key, {}).get("effects", {}).get(currency))
                amount = max(0, difference) if amount >= 0 else min(0, difference)
            pools[day, currency, key] = _decimal(amount)
    rows = await (await db.execute(
        "SELECT * FROM broker_cash_reviews WHERE google_sub=? AND account_id=? AND fingerprint=? ORDER BY id",
        (user, aid, state["fingerprint"]))).fetchall()
    for index, row in enumerate(rows):
        remaining = _decimal(row["amount"])
        candidates = [key for key, amount in pools.items() if amount and key[1] == row["currency"]
                      and row["from_at"][:10] <= key[0] <= row["to_at"][:10]]
        # 금액이 정확히 맞는 다음 관측을 우선한다. 오후 대여료를 새벽 MMW 이자에서 먼저 빼지 않는다.
        if sum((pools[key] for key in candidates), Decimal(0)) != remaining:
            candidates = [key for key in candidates if pools[key] == remaining or not any(
                _decimal(later["amount"]) == pools[key] and later["currency"] == key[1]
                and later["from_at"][:10] <= key[0] <= later["to_at"][:10] for later in rows[index + 1:])]
        total = sum((pools[key] for key in candidates), Decimal(0))
        if total * remaining > 0 and abs(total) <= abs(remaining):
            # 한 구간에 여러 수입·세금·결제가 함께 반영된 경우 순액으로 대조한다.
            remaining -= total
            for key in candidates:
                pools[key] = Decimal(0)
        else:
            # 같은 날 여러 조회에 나눠 반영된 거래를 날짜 합계 하나로 묶어 놓치지 않는다.
            candidates.sort(key=lambda key: (pools[key] != remaining, key))
            matched_trade = False
            for key in candidates:
                available = pools[key]
                if available * remaining > 0 and abs(available) <= abs(remaining):
                    remaining -= available
                    pools[key] = Decimal(0)
                    matched_trade |= key[2].endswith((":trade_buy", ":trade_sell"))
                elif remaining * available > 0 and abs(available) > abs(remaining) and key[2].startswith("summary:"):
                    # 일별 합계가 여러 잔고 조회에 나눠 반영된 경우만 나눠 대조한다.
                    # 같은 방향 관측 총액보다 확인 금액이 크면 반대 변동이 섞였을 수 있어 보류한다.
                    future_capacity = abs(remaining) + sum((abs(_decimal(later["amount"])) for later in rows[index + 1:]
                        if later["currency"] == key[1] and _decimal(later["amount"]) * remaining > 0
                        and later["from_at"][:10] <= key[0] <= later["to_at"][:10]), Decimal(0))
                    known_total = sum((abs(value) for other, value in pools.items()
                                       if other[:2] == key[:2] and value * remaining > 0), Decimal(0))
                    # 매수 원금이 맞은 관측의 비용만 일별 비용에서 나눠 사용한다.
                    # 매도 비용은 이후 양수 매도대금과 함께 순액 대조할 수 있어 음수 관측 합계만으로 제한하지 않는다.
                    trade_cost = matched_trade and key[2].endswith(":trade_charges")
                    if trade_cost or future_capacity >= known_total:
                        pools[key] -= remaining
                        remaining = Decimal(0)
        if remaining != _decimal(row["unexplained"]):
            # 지연 수집으로 설명 대상액이 바뀌면 이전 답변은 이력에 남기고 재확인한다.
            await db.execute(
                "UPDATE broker_cash_reviews SET unexplained=?,category='',reason='',revision=revision+1,updated_at=? WHERE id=?",
                (float(remaining), now, row["id"]))


async def observe(db, user: str, link: dict, positions: list[dict], previous: list[dict], *, fx_rates: dict | None = None):
    """완전한 잔고와 같은 트랜잭션에서 관측한다. 최초·재연결은 기준 잔고만 저장한다."""
    if link["environment"] != "live" or link.get("product", "stocks") in {"krfuture", "gbfuture"}:
        return  # 선물 CASH 행은 평가액을 나타내는 합성 값이다.
    aid, now = link["account_id"], stamp()
    fingerprint = _fingerprint(link)
    cash = {r["currency"]: float(r["quantity"]) for r in positions if r["stock_code"].startswith("CASH_")}
    state = await (await db.execute("SELECT * FROM broker_cash_review_state WHERE google_sub=? AND account_id=?", (user, aid))).fetchone()
    evidence = await _evidence(db, user, aid, fingerprint)
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
            basis = max(abs(old_cash.get(currency, 0)), abs(cash.get(currency, 0)))
            limit = minor_limit(currency, basis, (fx_rates or {}).get(currency))
            await db.execute(
                "INSERT INTO broker_cash_reviews (google_sub,account_id,fingerprint,from_at,to_at,currency,amount,unexplained,positions_changed,updated_at,cash_basis,minor_limit_native) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (user, aid, fingerprint, state["last_at"], now, currency, float(amount), float(amount), int(changed), now,
                 basis, float(limit) if limit else None))
    await _reconcile(db, user, aid, state, evidence, now)
    await db.execute("UPDATE broker_cash_review_state SET cash_json=?,last_at=? WHERE google_sub=? AND account_id=?", (json.dumps(cash), now, user, aid))


def _recover_cash_bases(rows: list[dict], states: dict) -> dict[int, float]:
    """마지막 현금에서 원래 순변동을 역산한다. 현재 잔고를 과거 잔고로 대체하지 않는다."""
    recovered = {}
    running = {}
    invalid = set()
    for row in rows:  # id 내림차순. 잔고와 관측은 같은 트랜잭션에서 기록된다.
        state = states.get(row["account_id"])
        if not state or row["fingerprint"] != state["fingerprint"] or row["from_at"] < state["started_at"]:
            continue
        key = (row["account_id"], row["fingerprint"], row["currency"])
        if key in invalid:
            continue
        if not row["from_at"] <= row["to_at"] <= state["last_at"]:
            invalid.add(key)
            continue
        if key not in running:
            running[key] = _decimal(json.loads(state["cash_json"]).get(row["currency"], 0))
        after = running[key]
        before = after - _decimal(row["amount"])
        basis = max(abs(before), abs(after))
        if row["cash_basis"] is not None and abs(basis - _decimal(row["cash_basis"])) > Decimal("0.000001"):
            invalid.add(key)  # 저장된 기준과 다르면 누락·변경된 이력을 억지로 복원하지 않는다.
            continue
        recovered[row["id"]] = float(basis)
        running[key] = before
    return recovered


async def history(user: str, *, pending: bool = True, offset: int = 0, fx_rates: dict | None = None) -> dict:
    async with read_snapshot() as db:
        query = ("SELECT r.*,a.name AS account_name, (l.account_id IS NOT NULL AND l.last_sync_at IS NOT NULL "
                 "AND l.environment='live' AND l.product NOT IN ('krfuture','gbfuture') AND r.fingerprint=s.fingerprint) AS is_current "
                 "FROM broker_cash_reviews r JOIN portfolio_accounts a ON a.account_id=r.account_id "
                 "LEFT JOIN broker_account_links l ON l.account_id=r.account_id AND l.google_sub=r.google_sub "
                 "LEFT JOIN broker_cash_review_state s ON s.account_id=r.account_id AND s.google_sub=r.google_sub "
                 "WHERE r.google_sub=? ORDER BY r.id DESC")
        rows = [dict(row) for row in await (await db.execute(query, (user,))).fetchall()]
        states = {row["account_id"]: dict(row) for row in await (await db.execute(
            "SELECT * FROM broker_cash_review_state WHERE google_sub=?", (user,))).fetchall()}
        recovered = _recover_cash_bases(rows, states)
        for row in rows:
            if row["cash_basis"] is None:
                row["cash_basis"] = recovered.get(row["id"])
        totals = defaultdict(Decimal)
        limits = {}
        for row in rows:
            if row["category"] or not row["unexplained"]:
                continue
            key = (row["account_id"], row["fingerprint"], row["currency"], row["to_at"][:10])
            limit = (_decimal(row["minor_limit_native"]) if row["minor_limit_native"] is not None else
                     minor_limit(row["currency"], row["cash_basis"], (fx_rates or {}).get(row["currency"])))
            totals[key] += abs(_decimal(row["unexplained"]))
            limits[key] = min(limits.get(key, limit), limit)
        items = []
        for row in rows:
            key = (row["account_id"], row["fingerprint"], row["currency"], row["to_at"][:10])
            automatic = "confirmed" if not row["unexplained"] else None
            if not row["category"] and row["unexplained"] and totals[key] <= limits[key]:
                automatic = "minor"
            required = bool(row["unexplained"] and not row["category"] and not automatic)
            if pending and (not row["is_current"] or not required):
                continue
            item = {k: value for k, value in row.items() if k not in {"google_sub", "fingerprint", "is_current"}}
            items.append({**item, "auto_resolution": automatic, "review_required": required,
                          "minor_limit": float(limits.get(key, 0))})
        count = len(items)
        return {"items": items[offset:offset + 100], "total": count, "has_more": offset + 100 < count,
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
