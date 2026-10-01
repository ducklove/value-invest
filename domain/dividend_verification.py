"""수동 배당 수취·배당 일정과 NH 배당 입금 기록의 대응. 순수 함수만 둔다.

NH 배당 기록은 `repositories.broker_activity.dividend_records()`의 형태다:
date(실거래일), booked_date(처리일), account_id, stock_code, symbol, currency,
net_amount(원통화 실입금), gross_amount, tax_amount(현지세), fx_rate, domestic_tax_krw,
gross_krw·net_krw(환율 검산된 원화). 세금 정산('외화제세금환급')은 `dividend_adjustments()`의
형태이며 배당이 아니라 같은 종목 배당의 조정으로만 붙인다.
"""

import re
from datetime import date, timedelta

NH_CONFIRMED = "nh_confirmed"
# NH 계좌 몫만 입금 확인. 같은 종목을 수동·다른 증권사 계좌에도 보유해 그 몫은 확인할 수 없다.
NH_PARTIAL = "nh_partial"
UNCONFIRMED = "unconfirmed"
NEEDS_REVIEW = "needs_review"

# 정산·소급 등록 차이. NH 처리일이 실제 입금일보다 늦게 잡히는 행이 있다.
RECEIPT_DAY_TOLERANCE = 5
# 배당 일정의 지급일은 현지 기준이다. 해외는 다음 영업일 이후 입금되는 경우가 있다.
CALENDAR_DAYS_BEFORE, CALENDAR_DAYS_AFTER = 3, 10
# 배당락·기준일 일정: 권리일 −1일 ≤ 입금일 < 같은 종목의 다음 권리일.
# 다음 권리일이 없으면 배당락 +60일(국내 기준일은 결산배당 4월 지급을 고려해 +130일)까지 본다.
RIGHTS_DAYS_BEFORE = 1
RIGHTS_FALLBACK_DAYS = {"ex_date": 60, "record_date": 130}
# 다음 권리일이 멀어도(연배당·이력 누락) 이보다 늦은 입금은 같은 회차로 보지 않는다.
RIGHTS_MAX_DAYS = 130
# 수취 입력이 배당락 일정(source_key)에서 왔을 때 그 회차로 보는 NH 입금 범위(월배당 다음 회차와 겹치지 않게).
SOURCE_RIGHTS_DAYS = 35

# NH 해외 기록 'TICKER 시장' → 잔고 종목코드 접미사와 그 시장에서 가능한 배당 통화.
_MARKET_SUFFIX = {"US": "", "HK": ".HK", "JP": ".T", "AU": ".AX", "DE": ".DE"}
_MARKET_CURRENCIES = {"US": {"USD"}, "HK": {"HKD", "CNY", "USD"}, "JP": {"JPY"}, "AU": {"AUD"}, "DE": {"EUR"}}
# 미국 외 거래소 접미사. 이 접미사가 붙은 코드는 미국 티커로 대조하지 않는다('BRK.B'는 미국).
_FOREIGN_SUFFIX = re.compile(r"\.(HK|T|AX|DE|F|L|SS|SZ|TO|V|PA|AS|SW|MI|KS|KQ|VN|HM|SI|BK|TW|TWO)$")
_KR_CODE = re.compile(r"[0-9][0-9A-Z]{5}")


def _day(value) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _alnum(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def _symbol(value) -> tuple[str, str]:
    """'AAPL US' → ('AAPL', 'US'). 시장 접미사가 없으면 ('', '')."""
    match = re.fullmatch(r"([A-Z0-9./-]{1,16})\s+([A-Z]{2})", str(value or "").strip().upper())
    return (match[1], match[2]) if match else ("", "")


def same_stock(code: str, record: dict, currency: str | None = None) -> bool:
    """잔고 종목코드와 NH 기록이 같은 종목인가.

    해석된 NH 종목코드가 있으면 그것만 비교한다(티커가 같아도 다른 시장이면 다른 종목).
    해석하지 못한 해외 기록만 'TICKER 시장'으로 대조하며, 이때 시장 접미사와 배당 통화가
    모두 맞아야 한다(currency를 주면 기록 통화와도 같아야 한다).
    """
    code = str(code or "").strip().upper()
    if not code:
        return False
    resolved = str(record.get("stock_code") or "").strip().upper()
    if resolved:
        return resolved == code
    ticker, market = _symbol(record.get("symbol"))
    if not ticker or market not in _MARKET_SUFFIX:
        return False
    paid = str(record.get("currency") or "").upper()
    if (currency and paid and currency.upper() != paid) or (paid and paid not in _MARKET_CURRENCIES[market]):
        return False
    suffix = _MARKET_SUFFIX[market]
    if suffix:
        if not code.endswith(suffix):
            return False
        base = code[:-len(suffix)]
    else:
        if _FOREIGN_SUFFIX.search(code) or _KR_CODE.fullmatch(code):
            return False
        base = code
    if market in {"HK", "JP"}:
        return (_alnum(base).lstrip("0") or "0") == (_alnum(ticker).lstrip("0") or "0")
    return _alnum(base) == _alnum(ticker)


def _same_record_stock(a: dict, b: dict) -> bool:
    """NH 기록 두 건(배당·세금 정산)이 같은 종목인가. 원문 'TICKER 시장'을 우선 비교한다."""
    sa, sb = _symbol(a.get("symbol")), _symbol(b.get("symbol"))
    if sa[0] and sb[0]:
        return sa[1] == sb[1] and _alnum(sa[0]) == _alnum(sb[0])
    code = str(a.get("stock_code") or "").strip().upper()
    return bool(code) and code == str(b.get("stock_code") or "").strip().upper()


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
    if receipt.get("currency") != record.get("currency") or not same_stock(receipt.get("stock_code"), record, receipt.get("currency")):
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


def _pair_indexes(receipts: list[dict], records: list[dict]) -> dict[int, int]:
    candidates = sorted(
        ((_receipt_distance(r, n), i, j) for i, r in enumerate(receipts) for j, n in enumerate(records) if receipt_matches(r, n)),
        key=lambda item: item[0])
    used_r, used_n, out = set(), set(), {}
    for _, i, j in candidates:
        if i in used_r or j in used_n:
            continue
        used_r.add(i)
        used_n.add(j)
        out[i] = j
    return out


def pair_receipts(receipts: list[dict], records: list[dict]) -> dict[int, dict]:
    """수동 수취 index → NH 기록. 한 NH 기록은 한 수취만 확인한다(가까운 날짜·금액 우선)."""
    return {i: records[j] for i, j in _pair_indexes(receipts, records).items()}


def _adjustment_evidence(adjustment: dict) -> dict:
    return {key: adjustment.get(key) for key in ("date", "currency", "net_amount", "domestic_tax_krw", "income_krw",
                                                  "refund_base_gross", "description", "id")}


def evidence(record: dict) -> dict:
    out = {key: record.get(key) for key in ("date", "booked_date", "currency", "net_amount", "gross_amount", "account_id", "id",
                                            "tax_amount", "domestic_tax_krw", "fx_rate", "gross_krw", "net_krw",
                                            "stock_name", "symbol")}
    out["adjustments"] = [_adjustment_evidence(a) for a in record.get("adjustments") or []]
    return out


def annotate_receipts(receipts: list[dict], records: list[dict]) -> list[dict]:
    pairs = pair_receipts(receipts, records)
    return [{**r, "verification": NH_CONFIRMED if i in pairs else UNCONFIRMED,
             "nh_match": evidence(pairs[i]) if i in pairs else None} for i, r in enumerate(receipts)]


def attach_adjustments(records: list[dict], adjustments: list[dict]) -> tuple[list[dict], list[dict]]:
    """세금 정산을 같은 계좌·종목·통화의 가장 가까운 이전 NH 배당에 붙인다.

    원배당 세전(refund_base_gross)이 정확히 같은 배당을 우선하고, 없으면 날짜가 가장 가까운 이전 배당이다.
    반환: (adjustments 키를 채운 배당 기록 사본, 붙일 배당이 없는 정산).
    """
    out = [{**r, "adjustments": []} for r in records]
    orphans = []
    for adj in sorted(adjustments, key=lambda a: str(a.get("date") or "")):
        day = _day(adj.get("date"))
        base = adj.get("refund_base_gross")
        best = None
        for j, record in enumerate(out):
            got = _day(record.get("date"))
            if (not day or not got or got > day or record.get("currency") != adj.get("currency")
                    or (adj.get("account_id") and record.get("account_id") and adj["account_id"] != record["account_id"])
                    or not _same_record_stock(record, adj)):
                continue
            exact = base is not None and record.get("gross_amount") is not None and abs(record["gross_amount"] - base) <= 0.005
            key = (not exact, (day - got).days)
            if best is None or key < best[0]:
                best = (key, j)
        if best is None:
            orphans.append(adj)
        else:
            out[best[1]]["adjustments"].append(adj)
    return out, orphans


def _rights_day(ev: dict) -> date | None:
    return _day(ev.get("ex_date") or ev.get("record_date") or (ev.get("date") if ev.get("date_kind") in RIGHTS_FALLBACK_DAYS else None))


def _currency_ok(ev: dict, record: dict) -> bool:
    return not ev.get("currency") or not record.get("currency") or ev["currency"] == record["currency"]


def _rights_windows(events: list[dict], pending: list[int]) -> dict[int, tuple[date, date]]:
    """배당락·기준일 일정 index → [시작, 끝) 입금일 범위."""
    timeline: dict[str, list[date]] = {}
    for ev in events:
        day = _rights_day(ev)
        if day and ev.get("type") != "estimated":
            timeline.setdefault(str(ev.get("stock_code") or "").upper(), []).append(day)
    out = {}
    for i in pending:
        ev = events[i]
        day = _day(ev.get("date"))
        later = [d for d in timeline.get(str(ev.get("stock_code") or "").upper(), []) if d > day]
        end = min(later) if later else day + timedelta(days=RIGHTS_FALLBACK_DAYS[ev["date_kind"]])
        out[i] = (day - timedelta(days=RIGHTS_DAYS_BEFORE), min(end, day + timedelta(days=RIGHTS_MAX_DAYS)))
    return out


def _greedy(candidates: list[tuple], matched: dict, used: set):
    for _, i, j in sorted(candidates):
        if i in matched or j in used:
            continue
        matched[i] = j
        used.add(j)


def link_calendar(events: list[dict], records: list[dict], receipts: list[dict], today: date,
                  nh_only: set[str] | None = None) -> tuple[list[dict], list[int]]:
    """배당 일정에 NH 입금을 일대일로 연결한다. 반환: (판정한 일정, 어느 일정에도 연결되지 않은 NH 기록 index).

    1) 지난 지급일 일정(공시·수집): 같은 종목·통화 NH 배당이 지급일 −3일~+10일이면 연결(가까운 날 우선).
    2) 지난 배당락·기준일 일정(예상 제외): 권리일 −1일 ≤ 입금일 < 같은 종목의 다음 권리일(없으면 +60일,
       국내 기준일 +130일, 최대 +130일)이면 연결(권리일에서 가까운 입금 우선). 일정 날짜는 바꾸지 않고
       실제 입금일을 paid_date로 준다.
    3) 연결되면 NH 연동 계좌에만 보유한 종목은 NH 확인, 그 밖은 NH 일부 확인(nh_only=None이면 전부 확인).
       NH로 확인된 수동 수취가 일정 source_key에 연결돼 있어도 NH 확인이다.
    4) 연결이 없을 때: 지난 지급일은 미확인. 배당락·기준일은 NH 연동 계좌에만 보유한 종목이고 대기 범위
       (배당락 +60일, 기준일 +130일)가 지났으며 그 전에 같은 종목 NH 배당이 있었을 때만 미확인, 그 밖은 None.
    """
    pairs = _pair_indexes(receipts, records)
    receipt_keys = {receipts[i].get("source_key"): j for i, j in pairs.items() if receipts[i].get("source_key")}
    payments = {i for i, ev in enumerate(events)
                if ev.get("date_kind") == "payment" and ev.get("type") != "estimated" and (_day(ev.get("date")) or today) < today}
    rights = {i for i, ev in enumerate(events)
              if ev.get("date_kind") in RIGHTS_FALLBACK_DAYS and ev.get("type") != "estimated" and _day(ev.get("date"))
              and _day(ev.get("date")) < today}
    matched: dict[int, int] = {}
    used: set[int] = set()
    # 수동 수취로 이미 연결된 NH 기록은 그 일정의 근거로 남긴다(다른 일정·NH 단독 행으로 다시 쓰지 않는다).
    via_receipt = set()
    for i, ev in enumerate(events):
        j = receipt_keys.get(ev.get("source_key")) if ev.get("source_key") else None
        if j is not None and j not in used and (i in payments or i in rights):
            matched[i] = j
            used.add(j)
            via_receipt.add(i)
    candidates = []
    for i in sorted(payments):
        if i in matched:
            continue
        ev, day = events[i], _day(events[i].get("date"))
        for j, record in enumerate(records):
            got = _day(record.get("date"))
            if (got and _currency_ok(ev, record) and same_stock(ev.get("stock_code"), record, ev.get("currency"))
                    and -CALENDAR_DAYS_BEFORE <= (got - day).days <= CALENDAR_DAYS_AFTER):
                candidates.append((abs((got - day).days), i, j))
    _greedy(candidates, matched, used)
    windows = _rights_windows(events, sorted(i for i in rights if i not in matched))
    candidates = []
    for i, (start, end) in windows.items():
        ev, day = events[i], _day(events[i].get("date"))
        for j, record in enumerate(records):
            got = _day(record.get("date"))
            if (j not in used and got and start <= got < end and _currency_ok(ev, record)
                    and same_stock(ev.get("stock_code"), record, ev.get("currency"))):
                candidates.append((abs((got - day).days), i, j))
    _greedy(candidates, matched, used)
    out = []
    for i, ev in enumerate(events):
        if i in matched:
            record = records[matched[i]]
            whole = nh_only is None or str(ev.get("stock_code") or "") in nh_only or i in via_receipt
            out.append({**ev, "verification": NH_CONFIRMED if whole else NH_PARTIAL, "nh_match": evidence(record),
                        "paid_date": record.get("date")})
        elif i in payments:
            out.append({**ev, "verification": UNCONFIRMED, "nh_match": None})
        elif i in rights:
            day = _day(ev["date"])
            waited = day + timedelta(days=RIGHTS_FALLBACK_DAYS[ev["date_kind"]]) < today
            # 일정은 현재 보유 기준이라 나중에 산 종목의 지난 배당락도 보인다. 그 전에 같은 종목 NH 배당을
            # 받은 적이 있을 때만(그때도 NH로 보유) 받을 배당을 못 본 것으로 판정한다.
            held = any(_day(r.get("date")) and _day(r.get("date")) < day and _currency_ok(ev, r)
                       and same_stock(ev.get("stock_code"), r, ev.get("currency")) for r in records)
            missing = waited and held and nh_only is not None and str(ev.get("stock_code") or "") in nh_only
            out.append({**ev, "verification": UNCONFIRMED if missing else None, "nh_match": None})
        else:
            out.append({**ev, "verification": None, "nh_match": None})
    return out, [j for j in range(len(records)) if j not in used]


def annotate_calendar(events: list[dict], records: list[dict], receipts: list[dict], today: date,
                      nh_only: set[str] | None = None) -> list[dict]:
    """link_calendar의 일정 판정만 돌려준다."""
    return link_calendar(events, records, receipts, today, nh_only)[0]


def _gross_krw(record: dict) -> float | None:
    if record.get("gross_krw") is not None:
        return float(record["gross_krw"])
    if record.get("currency") == "KRW" and record.get("gross_amount") is not None:
        return float(record["gross_amount"])
    return None


def nh_payment_event(record: dict) -> dict:
    """어느 일정에도 연결되지 않은 NH 배당 입금 → 캘린더 행(NH 입금).

    금액은 실제 입금 기준이다: 세전·현지세·세후는 원통화, 원화 세전은 NH 환율이 검산된 경우에만 준다.
    수취 입력 대상이 아니다(현금은 NH 잔고가 이미 반영).
    """
    ticker, _ = _symbol(record.get("symbol"))
    code = record.get("stock_code") or ticker or record.get("symbol") or ""
    day = record.get("date")
    return {"date": day, "pay_date": day, "ex_date": None, "record_date": None, "declaration_date": None,
            "paid_date": day, "stock_code": code, "stock_name": record.get("stock_name") or code,
            "symbol": record.get("symbol"), "label": "NH 배당 입금", "type": "payment", "date_kind": "payment",
            "date_precision": "day", "confirmed": False, "date_status": "nh", "amount_status": "actual",
            "currency": record.get("currency"), "amount_per_share": None, "shares": None,
            "gross_amount": record.get("gross_amount"), "tax_amount": record.get("tax_amount"),
            "net_amount": record.get("net_amount"), "domestic_tax_krw": record.get("domestic_tax_krw"),
            "expected_amount_krw": _gross_krw(record), "holding_basis": "nh_actual", "frequency": None,
            "cashflow": True, "receiptable": False, "source_key": None, "source_aliases": [],
            "source": "NH 거래내역", "source_url": None, "fetched_at": None, "data_status": "fresh",
            "fx_source": "nh" if _gross_krw(record) is not None else "unavailable",
            "verification": NH_CONFIRMED, "nh_match": evidence(record)}


def nh_duplicate(receipt: dict, records: list[dict], nh_only: set[str]) -> dict | None:
    """NH 연동 계좌에만 있는 종목의 수동 수취가 이미 가져온 NH 배당과 같은 지급으로 보이면 그 기록.

    수동 수취는 수동 계좌 현금을 늘리고 NH 계좌 현금은 증권사 잔고가 이미 반영했으므로
    같은 배당을 다시 기록하면 현금이 중복된다(수익은 짝짓기로 한 번만 집계된다).
    국내·해외 모두 수취일 ±10일 안의 같은 종목·통화 NH 입금(실거래일 또는 처리일)이면 중복이다.
    배당락·기준일 일정(source_key)에서 온 수취는 그 권리일 −1일~+35일의 NH 입금도 같은 회차로 본다.
    """
    code = str(receipt.get("stock_code") or "").strip().upper()
    received = _day(receipt.get("received_date"))
    if not code or code not in nh_only or not received:
        return None
    window = max(CALENDAR_DAYS_AFTER, RECEIPT_DAY_TOLERANCE)
    source = re.fullmatch(r"(.+):ex_date:(\d{4}-\d{2}-\d{2})", str(receipt.get("source_key") or ""))
    rights = _day(source[2]) if source and source[1].strip().upper() == code else None
    for record in records:
        if record.get("currency") != receipt.get("currency") or not same_stock(code, record, receipt.get("currency")):
            continue
        days = [d for d in (_day(record.get("date")), _day(record.get("booked_date"))) if d]
        if days and min(abs((received - d).days) for d in days) <= window:
            return record
        paid = _day(record.get("date"))
        if rights and paid and -RIGHTS_DAYS_BEFORE <= (paid - rights).days <= SOURCE_RIGHTS_DAYS:
            return record
    return None
