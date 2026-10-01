"""배당 권리의 기준 시점과 그 시점의 보유 수량. 순수 함수만 둔다.

보유 기록은 일별 정규장 정산(``portfolio_stock_snapshots``, KRX 거래일 15:30 KST 마감 직후, 전 계좌 합산 수량)이다.
배당 한 건의 기준 시점(bound)을 정하고 "bound 이하 마지막 정산"의 보유를 그 배당의 보유로 본다.

기준 시점:

- 배당락일 E를 알면 E 전 거래일 종가 보유자가 받는다.
  - 국내·아시아·태평양 시장(국내 6자리·.KS/.KQ, 일본 .T, 홍콩 .HK, 호주 .AX, 중국 .SS/.SZ, 싱가포르 .SI,
    대만 .TW/.TWO, 베트남 .VN/.HM, 태국 .BK, 인도네시아 .JK, 뉴질랜드 .NZ): E 거래가 그날 KST 15:30 정산 전에
    열린다(정산이 E 거래를 담을 수 있다) → bound = E − 1일(E 전 마지막 정산).
  - 서쪽 시장(접미사 없는 미국·Reuters .O/.N/.K, 유럽 .DE/.F/.L/.PA/.AS/.SW/.MI, 캐나다 .TO/.V 등): E 거래는
    KST 저녁 이후에 열려 E 날짜의 KST 오후 정산은 전 거래일까지만 담는다 → bound = E.
- 국내 기준일 R만 알면(KIS 예탁원 기준일, 브리프 기준일): T+2 결제라 R(휴장이면 R 이하 마지막 거래일)에서
  2거래일 전 종가 보유자가 받는다 → bound = 그 거래일. KRX 휴장일 달력이 없는 연도는 평일로 근사한다.
- 해외 기준일만 있으면 그 시장의 배당락일처럼 다루고, 지급일만 있으면 지급일 − 1일이다(둘 다 근사).
"""

from __future__ import annotations

from bisect import bisect_right
from datetime import date, timedelta

from domain.market_calendar import is_trading_day
from domain.portfolio_codes import is_korean_stock, yahoo_symbol

# 그날 장이 KST 15:30 정산 전에 열리는 아시아·태평양 시장(배당락일 거래가 그날 정산에 섞일 수 있다).
EAST_SUFFIXES = frozenset("KS KQ T HK AX SS SZ SI TW TWO VN HM BK JK NZ".split())
# 이 수만큼 이어진 정산에서만 빠졌다가 앞뒤 정산(이 일수 안)에 다시 보이면 기록 누락으로 보고 보유로 메운다.
GAP_MAX_SNAPSHOTS = 2
GAP_MAX_DAYS = 7

BASIS_CURRENT = "current"
BASIS_SNAPSHOT = "snapshot"
BASIS_EARLIEST = "earliest_snapshot"
BASIS_FALLBACK = "current_fallback"


def _day(value) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def holding_identity(code: str | None) -> str:
    """정산 기록·현재 보유·배당 일정의 종목코드를 같은 종목으로 맞추는 키.

    Reuters 미국 접미사(GOOGL.O)·클래스 표기(BRK.B)는 Yahoo 표기로, 국내 .KS/.KQ는 6자리 코드로 맞춘다.
    """
    symbol = yahoo_symbol(code)
    root, dot, suffix = symbol.rpartition(".")
    if dot and suffix in {"KS", "KQ"} and is_korean_stock(root):
        return root
    return symbol


def market_side(code: str | None) -> str:
    """'KR'(국내), 'east'(그날 장이 KST 15:30 정산 전에 열리는 아시아·태평양 시장), 'west'(그 밖)."""
    identity = holding_identity(code)
    if is_korean_stock(identity):
        return "KR"
    _, dot, suffix = identity.rpartition(".")
    if dot and suffix in EAST_SUFFIXES:
        return "east"
    return "west"


def _krx_day(day: date) -> tuple[bool, bool]:
    """(거래일 여부, 근사 여부). 달력이 없는 연도는 평일로 근사한다."""
    known = is_trading_day(day)
    if known is None:
        return day.weekday() < 5, True
    return known, False


def krx_record_entitlement_day(record: date) -> tuple[date, bool]:
    """국내 기준일 R의 배당을 받으려면 종가에 보유해야 하는 거래일과 근사 여부.

    T+2 결제가 R까지 끝나야 하므로 R 이하 마지막 거래일에서 2거래일 전이다.
    """
    approximate = False
    day = record
    while True:
        trading, approx = _krx_day(day)
        approximate |= approx
        if trading:
            break
        day -= timedelta(days=1)
    for _ in range(2):
        day -= timedelta(days=1)
        while True:
            trading, approx = _krx_day(day)
            approximate |= approx
            if trading:
                break
            day -= timedelta(days=1)
    return day, approximate


def reference_point(event: dict, code: str) -> dict | None:
    """배당 한 건의 기준 시점. {'date': bound(date), 'rule': 규칙, 'approximate': bool}. 날짜가 없으면 None.

    규칙: 'ex_date_prev_day'(국내·아시아·태평양 배당락), 'ex_date_same_day'(미국·유럽 등 배당락), 'krx_record_t2'(국내 기준일),
    'record_date'(해외 기준일만, 근사), 'pay_date'(지급일만, 근사).
    """
    side = market_side(code)
    ex = _day(event.get("ex_date"))
    if ex:
        if side == "west":
            return {"date": ex, "rule": "ex_date_same_day", "approximate": False}
        return {"date": ex - timedelta(days=1), "rule": "ex_date_prev_day", "approximate": False}
    record = _day(event.get("record_date"))
    if record:
        if side == "KR":
            day, approximate = krx_record_entitlement_day(record)
            return {"date": day, "rule": "krx_record_t2", "approximate": approximate}
        bound = record if side == "west" else record - timedelta(days=1)
        return {"date": bound, "rule": "record_date", "approximate": True}
    pay = _day(event.get("pay_date"))
    if pay:
        return {"date": pay - timedelta(days=1), "rule": "pay_date", "approximate": True}
    return None


_MISSING = object()


def _held(state) -> bool:
    """정산 행의 보유 여부. None은 수량 기록 전 정산의 보유(평가액 > 0), 0 이하는 미보유·공매도."""
    return state is None or state > 0


class HoldingHistory:
    """사용자의 일별 정산 보유: 정산일 → 종목 identity → 수량(None = 보유, 수량 미기록).

    rows: date, stock_code, quantity, market_value. 수량이 없는 옛 정산 행은 평가액이 양수이면 보유로 본다.
    같은 날 같은 종목(코드 표기만 다른 행)은 수량을 합친다.
    """

    def __init__(self, rows: list[dict]):
        by_date: dict[str, dict[str, float | None]] = {}
        for row in rows:
            day = str(row.get("date") or "")[:10]
            if not _day(day):
                continue
            identity = holding_identity(row.get("stock_code"))
            quantity = row.get("quantity")
            if quantity is None:
                state = None if float(row.get("market_value") or 0) > 0 else 0.0
            else:
                state = float(quantity)
            bucket = by_date.setdefault(day, {})
            if identity in bucket:
                previous = bucket[identity]
                state = None if previous is None or state is None else previous + state
            bucket[identity] = state
        self.dates = sorted(by_date)
        self.by_date = by_date

    def __bool__(self) -> bool:
        return bool(self.dates)

    def _state(self, index: int, identity: str):
        return self.by_date[self.dates[index]].get(identity, _MISSING)

    def _known_quantity(self, identity: str, index: int) -> tuple[float | None, str | None]:
        """수량 미기록 정산의 보유 수량: 이어서 보유한 가장 가까운 뒤(없으면 앞) 정산의 수량."""
        for step in (1, -1):
            k = index + step
            while 0 <= k < len(self.dates):
                state = self._state(k, identity)
                if state is _MISSING or not _held(state):
                    break
                if state is not None:
                    return state, self.dates[k]
                k += step
        return None, None

    def _gap_source(self, identity: str, index: int) -> int | None:
        """index 정산에서 빠진 종목이 기록 누락인가: 앞뒤 가장 가까운 정산에 보유로 있고, 빠진 정산이
        GAP_MAX_SNAPSHOTS 이하, 앞뒤 정산 간격이 GAP_MAX_DAYS 이하이면 앞 정산 index."""
        before = next((j for j in range(index - 1, -1, -1) if self._state(j, identity) is not _MISSING), None)
        after = next((k for k in range(index + 1, len(self.dates)) if self._state(k, identity) is not _MISSING), None)
        if before is None or after is None:
            return None
        if not (_held(self._state(before, identity)) and _held(self._state(after, identity))):
            return None
        if after - before - 1 > GAP_MAX_SNAPSHOTS:
            return None
        if (_day(self.dates[after]) - _day(self.dates[before])).days > GAP_MAX_DAYS:
            return None
        return before

    def at(self, code: str, bound: date) -> dict:
        """bound 이하 마지막 정산의 보유. bound가 첫 정산보다 이르면 첫 정산(가장 가까운 근거)을 쓴다.

        반환: held, basis('snapshot'|'earliest_snapshot'), as_of(그 정산일), quantity(모르면 None),
        quantity_as_of(수량을 다른 정산에서 가져왔으면 그 날짜), gap_filled(기록 누락을 앞 정산으로 메웠는지).
        """
        identity = holding_identity(code)
        index = bisect_right(self.dates, bound.isoformat()) - 1
        basis = BASIS_SNAPSHOT
        if index < 0:
            index, basis = 0, BASIS_EARLIEST
        out = {"basis": basis, "as_of": self.dates[index], "gap_filled": False}
        state = self._state(index, identity)
        source = index
        if state is _MISSING:
            gap = self._gap_source(identity, index)
            if gap is None:
                return {**out, "held": False, "quantity": None, "quantity_as_of": None}
            source, state = gap, self._state(gap, identity)
            out["gap_filled"] = True
        if not _held(state):
            return {**out, "held": False, "quantity": None, "quantity_as_of": None}
        if state is None:
            quantity, quantity_day = self._known_quantity(identity, source)
        else:
            quantity, quantity_day = state, self.dates[source]
        return {**out, "held": True, "quantity": quantity,
                "quantity_as_of": quantity_day if quantity_day != out["as_of"] else None}


def entitlement(code: str, raw: dict, today: date, history: HoldingHistory, current_quantity: float | None) -> dict:
    """배당 한 건의 보유 근거. held가 False면 기준 시점에 보유하지 않아 일정에서 뺀다.

    - 예상(estimated) 일정, 기준 시점이 오늘 이후 → 현재 보유('current').
    - 기준 시점이 지났으면 그 시점 이하 마지막 정산('snapshot'), 첫 정산보다 이르면 첫 정산('earliest_snapshot').
      정산 기록이 전혀 없으면 현재 보유('current_fallback').
    반환: held, quantity, holding_basis, holding_as_of, quantity_as_of, holding_gap_filled,
    reference_date, reference_rule, reference_approximate, excluded_reason(뺀 이유).
    """
    reference = None if raw.get("estimated") else reference_point(raw, code)
    out = {"reference_date": reference["date"].isoformat() if reference else None,
           "reference_rule": reference["rule"] if reference else None,
           "reference_approximate": bool(reference and reference["approximate"]),
           "holding_as_of": None, "quantity_as_of": None, "holding_gap_filled": False}
    held_now = current_quantity is not None and current_quantity > 0
    if reference is None or reference["date"] >= today or not history:
        basis = BASIS_CURRENT if reference is None or reference["date"] >= today else BASIS_FALLBACK
        return {**out, "held": held_now, "quantity": current_quantity if held_now else None, "holding_basis": basis,
                "excluded_reason": None if held_now else "not_held_now"}
    point = history.at(code, reference["date"])
    out.update(holding_basis=point["basis"], holding_as_of=point["as_of"], quantity_as_of=point["quantity_as_of"],
               holding_gap_filled=point["gap_filled"])
    if not point["held"]:
        reason = "absent_from_first_record" if point["basis"] == BASIS_EARLIEST else "not_held_at_reference"
        return {**out, "held": False, "quantity": None, "excluded_reason": reason}
    return {**out, "held": True, "quantity": point["quantity"], "excluded_reason": None}
