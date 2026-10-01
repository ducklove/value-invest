"""수동 배당 수취·배당 일정과 NH 배당 입금 기록의 대응. 순수 함수만 둔다.

NH 배당 기록은 `repositories.broker_activity.dividend_records()`의 형태다:
date(실거래일), booked_date(처리일), account_id, stock_code, symbol, currency,
net_amount(원통화 실입금), gross_amount, fx_rate, domestic_tax_krw.
"""

import re
from datetime import date

NH_CONFIRMED = "nh_confirmed"
# NH 계좌 몫만 입금 확인. 같은 종목을 수동·다른 증권사 계좌에도 보유해 그 몫은 확인할 수 없다.
NH_PARTIAL = "nh_partial"
UNCONFIRMED = "unconfirmed"
NEEDS_REVIEW = "needs_review"

# 정산·소급 등록 차이. NH 처리일이 실제 입금일보다 늦게 잡히는 행이 있다.
RECEIPT_DAY_TOLERANCE = 5
# 배당 일정의 지급일은 현지 기준이다. 해외는 다음 영업일 이후 입금되는 경우가 있다.
CALENDAR_DAYS_BEFORE, CALENDAR_DAYS_AFTER = 3, 10


def _day(value) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _ticker(value: str) -> str:
    """'AAPL US' · 'BRK.B' · 'BRK-B' → 'AAPLUS'가 아니라 시장 접미사를 뺀 영숫자 티커."""
    text = str(value or "").strip().upper()
    text = re.sub(r"\s+[A-Z]{2}$", "", text)
    return re.sub(r"[^A-Z0-9]", "", text)


def same_stock(code: str, record: dict) -> bool:
    code = str(code or "").strip().upper()
    if not code:
        return False
    if record.get("stock_code") and record["stock_code"].upper() == code:
        return True
    symbol = record.get("symbol") or ""
    # 해외 종목코드를 해석하지 못한 NH 행은 미국식 티커만 보조로 대조한다.
    return bool(symbol) and "." not in code and not code[:1].isdigit() and _ticker(code) == _ticker(symbol)


def _tolerance(currency: str, amount: float) -> float:
    if currency == "KRW":
        # 국내 원천징수는 소득세·지방소득세를 각각 10원 미만 절사한다.
        return max(20.0, abs(amount) * 0.002)
    unit = 1.0 if currency in {"JPY", "VND"} else 0.01
    return max(unit * 2, abs(amount) * 0.002)


def _net_candidates(record: dict) -> list[tuple[float, float]]:
    """(금액, 허용오차) 후보. 수동 입력은 국가별 기본 세율을 쓰므로 NH의 세목 배분과 다를 수 있다."""
    currency, net = record["currency"], record.get("net_amount")
    if net is None:
        return []
    out = [(net, _tolerance(currency, net))]
    rate, domestic = record.get("fx_rate"), record.get("domestic_tax_krw") or 0
    if rate and domestic:
        # 국내 추가 원천징수(원화)를 외화로 환산해 뺀 경제적 세후액.
        economic = net - domestic / rate
        out.append((economic, max(_tolerance(currency, economic), abs(economic) * 0.005)))
    gross = record.get("gross_amount")
    if gross is not None and gross != net:
        out.append((gross, _tolerance(currency, gross)))
    return out


def receipt_matches(receipt: dict, record: dict) -> bool:
    account = receipt.get("account_id")
    if account and record.get("account_id") and account != record["account_id"]:
        return False
    if receipt.get("currency") != record.get("currency") or not same_stock(receipt.get("stock_code"), record):
        return False
    received = _day(receipt.get("received_date"))
    days = [d for d in (_day(record.get("date")), _day(record.get("booked_date"))) if d]
    if not received or not days or min(abs((received - d).days) for d in days) > RECEIPT_DAY_TOLERANCE:
        return False
    try:
        net = float(receipt["net_amount"])
    except (KeyError, TypeError, ValueError):
        return False
    return any(abs(net - value) <= tol for value, tol in _net_candidates(record))


def _receipt_distance(receipt: dict, record: dict) -> tuple:
    received = _day(receipt.get("received_date"))
    d = _day(record.get("date"))
    days = abs((received - d).days) if received and d else 99
    diff = abs(float(receipt.get("net_amount") or 0) - float(record.get("net_amount") or 0))
    return days, diff


def pair_receipts(receipts: list[dict], records: list[dict]) -> dict[int, dict]:
    """수동 수취 index → NH 기록. 한 NH 기록은 한 수취만 확인한다(가까운 날짜·금액 우선)."""
    candidates = sorted(
        ((_receipt_distance(r, n), i, j) for i, r in enumerate(receipts) for j, n in enumerate(records) if receipt_matches(r, n)),
        key=lambda item: item[0])
    used_r, used_n, out = set(), set(), {}
    for _, i, j in candidates:
        if i in used_r or j in used_n:
            continue
        used_r.add(i)
        used_n.add(j)
        out[i] = records[j]
    return out


def evidence(record: dict) -> dict:
    return {key: record.get(key) for key in ("date", "booked_date", "currency", "net_amount", "gross_amount", "account_id", "id")}


def annotate_receipts(receipts: list[dict], records: list[dict]) -> list[dict]:
    pairs = pair_receipts(receipts, records)
    return [{**r, "verification": NH_CONFIRMED if i in pairs else UNCONFIRMED,
             "nh_match": evidence(pairs[i]) if i in pairs else None} for i, r in enumerate(receipts)]


def annotate_calendar(events: list[dict], records: list[dict], receipts: list[dict], today: date,
                      nh_only: set[str] | None = None) -> list[dict]:
    """지난 지급일 일정에 NH 확인/미확인을 붙인다. 미래·배당락·기준일·예상 건은 판정하지 않는다.

    일정은 전 계좌 합산 보유 기준이다. nh_only(NH 연동 계좌에만 보유한 종목)를 주면 그 밖의
    종목은 NH 입금이 있어도 다른 계좌 몫이 확인되지 않았으므로 'NH 일부 확인'이다.
    """
    confirmed_receipts = annotate_receipts(receipts, records)
    receipt_keys = {r.get("source_key"): r for r in confirmed_receipts if r.get("source_key") and r["verification"] == NH_CONFIRMED}
    pending = [i for i, ev in enumerate(events)
               if ev.get("date_kind") == "payment" and ev.get("type") != "estimated" and (_day(ev.get("date")) or today) < today]
    candidates = []
    for i in pending:
        ev, day = events[i], _day(events[i].get("date"))
        for j, record in enumerate(records):
            got = _day(record.get("date"))
            if got and same_stock(ev.get("stock_code"), record) and -CALENDAR_DAYS_BEFORE <= (got - day).days <= CALENDAR_DAYS_AFTER:
                candidates.append((abs((got - day).days), i, j))
    matched, used = {}, set()
    for _, i, j in sorted(candidates):
        if i in matched or j in used:
            continue
        matched[i] = records[j]
        used.add(j)
    out = []
    for i, ev in enumerate(events):
        if i not in pending:
            out.append({**ev, "verification": None, "nh_match": None})
        elif i in matched:
            whole = nh_only is None or str(ev.get("stock_code") or "") in nh_only
            out.append({**ev, "verification": NH_CONFIRMED if whole else NH_PARTIAL, "nh_match": evidence(matched[i])})
        elif ev.get("source_key") in receipt_keys:
            out.append({**ev, "verification": NH_CONFIRMED, "nh_match": receipt_keys[ev["source_key"]]["nh_match"]})
        else:
            out.append({**ev, "verification": UNCONFIRMED, "nh_match": None})
    return out


def nh_duplicate(receipt: dict, records: list[dict], nh_only: set[str]) -> dict | None:
    """NH 연동 계좌에만 있는 종목의 수동 수취가 이미 가져온 NH 배당과 같은 지급으로 보이면 그 기록.

    수동 수취는 수동 계좌 현금을 늘리고 NH 계좌 현금은 증권사 잔고가 이미 반영했으므로
    같은 배당을 다시 기록하면 현금이 중복된다(수익은 짝짓기로 한 번만 집계된다).
    """
    code = str(receipt.get("stock_code") or "").strip().upper()
    received = _day(receipt.get("received_date"))
    if not code or code not in nh_only or not received:
        return None
    window = max(CALENDAR_DAYS_AFTER, RECEIPT_DAY_TOLERANCE)
    for record in records:
        days = [d for d in (_day(record.get("date")), _day(record.get("booked_date"))) if d]
        if (record.get("currency") == receipt.get("currency") and same_stock(code, record)
                and days and min(abs((received - d).days) for d in days) <= window):
            return record
    return None
