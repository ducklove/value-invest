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


_SUM_KEYS = ("net_amount", "gross_amount", "gross_krw", "net_krw")
_TAX_KEYS = ("tax_amount", "domestic_tax_krw")


def group_evidence(records: list[dict]) -> dict:
    """한 일정에 붙은 NH 입금(같은 날 여러 건일 수 있다)의 근거. 여러 건이면 금액은 합계, parts에 건별 근거.

    원화 환산액은 모든 건이 검산됐을 때만 더한다(하나라도 없으면 None). 세금은 없는 건을 0으로 본다.
    """
    out = evidence(records[0])
    if len(records) == 1:
        return out
    for key in _SUM_KEYS:
        values = [r.get(key) for r in records]
        out[key] = round(sum(values), 6) if all(v is not None for v in values) else None
    for key in _TAX_KEYS:
        values = [r.get(key) for r in records if r.get(key) is not None]
        out[key] = round(sum(values), 6) if values else None
    if len({r.get("account_id") for r in records}) > 1:
        out["account_id"] = None
    out["id"] = [r.get("id") for r in records]
    out["adjustments"] = [_adjustment_evidence(a) for r in records for a in r.get("adjustments") or []]
    out["parts"] = [{key: value for key, value in evidence(r).items() if key != "adjustments"} for r in records]
    return out


def annotate_receipts(receipts: list[dict], records: list[dict]) -> list[dict]:
    pairs = pair_receipts(receipts, records)
    return [{**r, "verification": NH_CONFIRMED if i in pairs else UNCONFIRMED,
             "nh_match": evidence(pairs[i]) if i in pairs else None} for i, r in enumerate(receipts)]


def attach_adjustments(records: list[dict], adjustments: list[dict]) -> tuple[list[dict], list[dict]]:
    """세금 정산을 같은 계좌·종목·통화의 이전 NH 배당에 붙인다.

    원배당 세전(refund_base_gross)이 있으면 세전이 정확히 같은 이전 배당에만 붙인다. 없으면 가져온 기간 밖
    배당의 정산으로 보고 붙이지 않는다(엉뚱한 회차에 '세금 정산'이 보이지 않게). 원배당 세전을 모르면
    날짜가 가장 가까운 이전 배당이다. 반환: (adjustments 키를 채운 배당 기록 사본, 붙일 배당이 없는 정산).
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
            if base is not None and (record.get("gross_amount") is None or abs(record["gross_amount"] - base) > 0.005):
                continue
            if best is None or (day - got).days < best[0]:
                best = ((day - got).days, j)
        if best is None:
            orphans.append(adj)
        else:
            out[best[1]]["adjustments"].append(adj)
    return out, orphans


def _rights_day(ev: dict) -> date | None:
    return _day(ev.get("ex_date") or ev.get("record_date") or (ev.get("date") if ev.get("date_kind") in RIGHTS_FALLBACK_DAYS else None))


def _currency_ok(ev: dict, record: dict) -> bool:
    return not ev.get("currency") or not record.get("currency") or ev["currency"] == record["currency"]


def _wait_days(ev: dict) -> int:
    """배당락·기준일 이후 입금을 기다리는 기간. 국내(원화)는 결산배당이 다음 분기 기준일 뒤(4월)에 지급된다."""
    return RIGHTS_FALLBACK_DAYS["record_date"] if ev.get("currency") == "KRW" else RIGHTS_FALLBACK_DAYS[ev["date_kind"]]


def _late_windows(events: list[dict], pending: list[int], boundaries: list[dict] = ()) -> dict[int, tuple[date, date]]:
    """권리일 순서 배정 대상 일정 index → [시작, 끝) 입금일 범위.

    - 배당락·기준일: 권리일 −1일부터. 해외는 같은 종목의 다음 권리일 전까지(없으면 +60일), 국내(원화)는 +130일까지
      (결산배당이 다음 분기 기준일 뒤에 지급되므로 다음 권리일에서 끊지 않는다). 최대 +130일.
    - 지급일 ±에서 연결되지 못한 지급일 일정: 지급일 −3일부터 다음 일정 전까지, 최대 +60일(늦게 입금된 해외 배당).
    - boundaries: 기준 시점에 보유하지 않아 캘린더에서 뺀 일정. 연결 대상은 아니지만 다음 권리일 경계로 남겨
      앞 회차가 그 회차 입금을 가져가지 않게 한다.
    """
    timeline: dict[str, list[date]] = {}
    for ev in [*events, *boundaries]:
        day = _rights_day(ev) or _day(ev.get("date"))
        if day and ev.get("type") != "estimated":
            timeline.setdefault(str(ev.get("stock_code") or "").upper(), []).append(day)
    out = {}
    for i in pending:
        ev = events[i]
        day = _day(ev.get("date"))
        later = [d for d in timeline.get(str(ev.get("stock_code") or "").upper(), []) if d > day]
        if ev.get("date_kind") == "payment":
            end = min([*later, day + timedelta(days=RIGHTS_FALLBACK_DAYS["ex_date"])])
            out[i] = (day - timedelta(days=CALENDAR_DAYS_BEFORE), end)
            continue
        if ev.get("currency") == "KRW":
            end = day + timedelta(days=RIGHTS_MAX_DAYS)
        else:
            end = min(later) if later else day + timedelta(days=_wait_days(ev))
        out[i] = (day - timedelta(days=RIGHTS_DAYS_BEFORE), min(end, day + timedelta(days=RIGHTS_MAX_DAYS)))
    return out


def _ordered_assignment(rows: list[tuple[date, int]], units: list[tuple[date, list[int]]],
                        allowed) -> dict[int, list[int]]:
    """한 종목의 일정(날짜순)과 입금 묶음(날짜순)을 순서를 지키며 짝짓는다.

    k번째 입금이 그보다 앞선 일정에 배정되면 그 뒤 입금은 더 앞선 일정에 가지 않는다(교차 금지). 연결 수가
    가장 많은 배정을 고르고, 같으면 권리일 전 입금이 적고, 그다음 권리일~입금일 합이 작은 배정이다.
    그래서 월배당 경계일(다음 배당락 −1일) 입금이 다음 회차로 밀리지 않고, 국내 결산배당(12월 기준일 →
    4월 지급)이 3월 기준일을 건너 자기 회차에 붙는다.
    """
    n, m = len(rows), len(units)
    # score = (연결 수, −권리일 전 입금 수, −일수 합) 최대화.
    best = [[(0, 0, 0)] * (m + 1) for _ in range(n + 1)]
    move = [[None] * (m + 1) for _ in range(n + 1)]
    for a in range(1, n + 1):
        for b in range(1, m + 1):
            options = [(best[a - 1][b], "row"), (best[a][b - 1], "unit")]
            if allowed(rows[a - 1][1], units[b - 1]):
                lag = (units[b - 1][0] - rows[a - 1][0]).days
                prev = best[a - 1][b - 1]
                options.append(((prev[0] + 1, prev[1] - (lag < 0), prev[2] - abs(lag)), "pair"))
            best[a][b], move[a][b] = max(options, key=lambda item: item[0])
    out: dict[int, list[int]] = {}
    a, b = n, m
    while a and b:
        step = move[a][b]
        if step == "pair":
            out[rows[a - 1][1]] = units[b - 1][1]
            a, b = a - 1, b - 1
        elif step == "row":
            a -= 1
        else:
            b -= 1
    return out


def _record_rank(record: dict) -> tuple:
    """같은 날 같은 종목 입금 중 대표(세전이 큰 정규 배당)를 고르는 정렬 키."""
    return (-(record.get("gross_amount") or record.get("net_amount") or 0), str(record.get("id") or ""))


# NH 입금 세전 합계가 기준 시점 수량 × 주당 배당보다 이만큼(비율) 적어도 전체 확인으로 본다(반올림·세목 배분 차이).
AMOUNT_TOLERANCE = 0.03


def amount_covers(ev: dict, group: list[dict]) -> bool | None:
    """연결된 NH 입금(세전, 원통화 합계)이 일정의 예상 세전(주당 배당 × 기준 시점 수량)을 덮는가.

    True: NH ≥ 예상 − 허용오차(max(3%, 통화 반올림)) — 그 배당 전체가 NH로 들어왔다. False: 뚜렷이 적다(다른 계좌 몫).
    None: 비교할 수 없다(주당 금액·수량·세전 없음, 통화 불일치).
    """
    try:
        amount, shares = float(ev.get("amount_per_share")), float(ev.get("shares"))
    except (TypeError, ValueError):
        return None
    currency = ev.get("currency")
    if amount <= 0 or shares <= 0 or not currency or any(r.get("currency") != currency for r in group):
        return None
    grosses = [r.get("gross_amount") for r in group]
    if not grosses or any(g is None for g in grosses):
        return None
    expected = amount * shares
    return sum(grosses) >= expected - max(expected * AMOUNT_TOLERANCE, _tolerance(currency, expected))


def exact_quantity(ev: dict) -> bool:
    """일정 수량이 그 배당의 기준 시점 수량 그대로인가(그 시점 정산 수량, 미래·예상은 현재 수량).

    첫 정산 근사·정산 기록 없음·수량을 다른 날 정산에서 가져온 행은 아니다. 그런 행에서 NH가 예상보다 적은 것은
    다른 계좌 몫이 아니라 그때 수량이 적었던 것일 수 있다.
    """
    return ev.get("holding_basis") in {"snapshot", "current"} and not ev.get("quantity_as_of")


def link_calendar(events: list[dict], records: list[dict], receipts: list[dict], today: date,
                  nh_only: set[str] | None = None, *, boundaries: list[dict] = ()) -> tuple[list[dict], list[int]]:
    """배당 일정에 NH 입금을 연결한다. 반환: (판정한 일정, 어느 일정에도 연결되지 않은 NH 기록 index).

    일정은 기준 시점(배당락 전 거래일·국내 기준일 2거래일 전)에 보유한 배당만 있다(domain.dividend_entitlement).
    boundaries는 그 시점에 보유하지 않아 뺀 일정으로, 연결하지 않고 입금 범위의 다음 권리일 경계로만 쓴다.

    1) 지급일 일정(공시·수집, 오늘 이후 포함 — NH 기록이 있으면 그것이 근거다): 같은 종목·통화 NH 배당이 지급일
       −3일~+10일이면 일대일 연결(가까운 날 우선). 같은 날 같은 종목 입금이 더 있으면(NH 계좌 여러 개) 그 일정에 함께 붙인다.
    2) 배당락·기준일 일정(예상 제외)과 1)에서 연결되지 못한 지급일 일정: 종목별로 순서를 지키는 배정
       (_ordered_assignment, 범위는 _late_windows). 같은 날 같은 종목 입금은 한 묶음으로 한 일정에 붙는다(Yahoo가
       같은 날 정규·추가 분배를 한 건으로 합친 경우, NH 계좌 여러 개). 일정 날짜는 바꾸지 않고 실제 입금일을 paid_date로 준다.
    3) 연결되면 금액으로 판정한다(amount_covers): NH 세전 합계가 주당 배당 × 기준 시점 수량을 덮으면 NH 확인,
       수량이 그 시점 그대로(exact_quantity)인데 뚜렷이 적으면 NH 일부 확인(다른 계좌 몫이 있다). 비교할 수 없거나
       (금액·수량 모름, 통화 다름) 근사 수량에서 적으면 현재 NH 연동 계좌에만 보유한 종목(nh_only)은 NH 확인,
       그 밖은 NH 일부 확인(nh_only=None이면 전부 확인).
       NH로 확인된 수동 수취가 일정 source_key에 연결돼 있으면 금액과 관계없이 NH 확인이다.
    4) 연결이 없을 때: 지난 지급일(오늘 전)은 미확인. 배당락·기준일은 NH 연동 계좌에만 보유한 종목이고 대기 범위
       (배당락 +60일, 기준일·국내 +130일)가 지났으며, 그 시점 보유가 정산 기록으로 확인됐거나(holding_basis 'snapshot')
       그 전에 같은 종목 NH 배당이 있었을 때만 미확인, 그 밖은 None.
    """
    pairs = _pair_indexes(receipts, records)
    receipt_keys = {receipts[i].get("source_key"): j for i, j in pairs.items() if receipts[i].get("source_key")}
    real = {i for i, ev in enumerate(events) if ev.get("type") != "estimated" and _day(ev.get("date"))}
    payments = {i for i in real if events[i].get("date_kind") == "payment"}
    rights = {i for i in real if events[i].get("date_kind") in RIGHTS_FALLBACK_DAYS}
    # 예상 지급일(오늘 이후)도 −3일 일찍 들어온 입금은 받는다. 그렇지 않으면 NH 입금 행과 함께 두 번 합산된다.
    linkable = payments | {i for i, ev in enumerate(events)
                           if ev.get("type") == "estimated" and ev.get("date_kind") == "payment" and _day(ev.get("date"))}
    matched: dict[int, list[int]] = {}
    used: set[int] = set()

    def link(i: int, js: list[int]):
        matched[i] = sorted(js, key=lambda j: _record_rank(records[j]))
        used.update(js)

    days = [_day(r.get("date")) for r in records]
    stock_records: dict[tuple, list[int]] = {}

    def of_stock(i: int) -> list[int]:
        """일정과 같은 종목·통화인 NH 기록 index(종목·통화별로 한 번만 계산)."""
        ev = events[i]
        key = (str(ev.get("stock_code") or "").upper(), ev.get("currency"))
        if key not in stock_records:
            stock_records[key] = [j for j, record in enumerate(records)
                                  if days[j] and _currency_ok(ev, record) and same_stock(ev.get("stock_code"), record, ev.get("currency"))]
        return stock_records[key]

    # 수동 수취로 이미 연결된 NH 기록은 그 일정의 근거로 남긴다(다른 일정·NH 단독 행으로 다시 쓰지 않는다).
    via_receipt = set()
    for i, ev in enumerate(events):
        j = receipt_keys.get(ev.get("source_key")) if ev.get("source_key") else None
        if j is not None and j not in used and (i in payments or i in rights):
            link(i, [j])
            via_receipt.add(i)
    candidates = []
    for i in sorted(linkable - set(matched)):
        day = _day(events[i]["date"])
        for j in of_stock(i):
            if j not in used and -CALENDAR_DAYS_BEFORE <= (days[j] - day).days <= CALENDAR_DAYS_AFTER:
                candidates.append((abs((days[j] - day).days), _record_rank(records[j]), i, j))
    for *_, i, j in sorted(candidates):
        if i not in matched and j not in used:
            link(i, [j])
    # 같은 날 같은 종목 입금(다른 NH 계좌·추가 분배)은 이미 그 날짜로 연결된 지급일 일정에 함께 붙인다.
    for i in sorted(matched):
        if i not in linkable:
            continue
        first = records[matched[i][0]]
        extra = [j for j in of_stock(i) if j not in used and records[j].get("date") == first.get("date")
                 and records[j].get("currency") == first.get("currency")]
        if extra:
            link(i, matched[i] + extra)
    # 배당락·기준일, 늦게 입금된 지급일: 종목·통화별 순서 배정.
    pending = sorted(i for i in rights | payments if i not in matched)
    windows = _late_windows(events, pending, boundaries)
    groups: dict[tuple, list[int]] = {}
    for i in pending:
        groups.setdefault((str(events[i].get("stock_code") or "").upper(), events[i].get("currency")), []).append(i)
    for rows in groups.values():
        # 한 그룹의 일정은 종목·통화가 같으므로 후보 NH 기록도 같다.
        by_day: dict[tuple, list[int]] = {}
        for j in of_stock(rows[0]):
            if j not in used:
                by_day.setdefault((days[j], records[j].get("currency")), []).append(j)
        units = sorted(((day, members) for (day, _), members in by_day.items()), key=lambda u: (u[0], u[1]))

        def allowed(i: int, unit: tuple[date, list[int]]) -> bool:
            start, end = windows[i]
            return start <= unit[0] < end

        assigned = _ordered_assignment(sorted((_day(events[i]["date"]), i) for i in rows), units, allowed)
        for i, members in assigned.items():
            link(i, members)
    out = []
    for i, ev in enumerate(events):
        if i in matched:
            group = [records[j] for j in matched[i]]
            covers = amount_covers(ev, group)
            if i in via_receipt or covers:
                whole = True
            elif covers is False and exact_quantity(ev):
                whole = False
            else:
                whole = nh_only is None or str(ev.get("stock_code") or "") in nh_only
            out.append({**ev, "verification": NH_CONFIRMED if whole else NH_PARTIAL, "nh_match": group_evidence(group),
                        "paid_date": group[0].get("date")})
        elif i in payments:
            past = _day(ev["date"]) < today
            out.append({**ev, "verification": UNCONFIRMED if past else None, "nh_match": None})
        elif i in rights:
            day = _day(ev["date"])
            waited = day + timedelta(days=_wait_days(ev)) < today
            # 그 시점 보유가 정산 기록으로 확인된 행이거나, 그 전에 같은 종목 NH 배당을 받은 적이 있을 때만
            # (그때도 NH로 보유) 받을 배당을 못 본 것으로 판정한다. 첫 정산 근사·정산 기록 없는 행은 단정하지 않는다.
            held = ev.get("holding_basis") == "snapshot" or any(days[j] < day for j in of_stock(i))
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
    배당락·기준일 일정(source_key)에서 온 수취는 그 권리일 −1일 이후 첫 NH 입금이 해외 +35일(월배당 다음 회차와
    겹치지 않게), 국내 +130일(결산배당 4월 지급 — 캘린더 연결 범위와 같다) 안이면 같은 회차로 본다.
    """
    code = str(receipt.get("stock_code") or "").strip().upper()
    received = _day(receipt.get("received_date"))
    if not code or code not in nh_only or not received:
        return None
    window = max(CALENDAR_DAYS_AFTER, RECEIPT_DAY_TOLERANCE)
    candidates = [r for r in records
                  if r.get("currency") == receipt.get("currency") and same_stock(code, r, receipt.get("currency"))]
    for record in candidates:
        days = [d for d in (_day(record.get("date")), _day(record.get("booked_date"))) if d]
        if days and min(abs((received - d).days) for d in days) <= window:
            return record
    source = re.fullmatch(r"(.+):ex_date:(\d{4}-\d{2}-\d{2})", str(receipt.get("source_key") or ""))
    rights = _day(source[2]) if source and source[1].strip().upper() == code else None
    if not rights:
        return None
    after = sorted((r for r in candidates if _day(r.get("date")) and (_day(r["date"]) - rights).days >= -RIGHTS_DAYS_BEFORE),
                   key=lambda r: str(r["date"]))
    limit = RIGHTS_MAX_DAYS if receipt.get("currency") == "KRW" else SOURCE_RIGHTS_DAYS
    if after and (_day(after[0]["date"]) - rights).days <= limit:
        return after[0]
    return None
