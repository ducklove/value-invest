"""배당 권리의 기준 시점과 그 시점의 보유 수량. 순수 함수만 둔다.

보유 기록은 일별 정산(``portfolio_stock_snapshots``, 전 계좌 합산)이다. 배당 한 건의 기준 시점(bound)을 정하고
"bound 이하 마지막 정산"의 보유를 그 배당의 보유로 본다. 정산 시각은 시기마다 다르다.

- 정규장 정산(``regular_close_v1``, 2026-09-30부터): KRX 거래일 15:30 KST 마감 시각의 잔고.
- 그 전 정산(``legacy_latest``): 매일 약 20:05 KST 실행 시점의 현재 잔고(KRX 휴장일에도 썼다). 2026-06-30 전 행은
  수량 없이 평가액만 있다.

기준 시점:

- 배당락일 E를 알면 E 전 거래일 종가 보유자가 받는다.
  - 국내·아시아·태평양 시장(국내 6자리·.KS/.KQ, 일본 .T, 홍콩 .HK, 호주 .AX, 중국 .SS/.SZ, 싱가포르 .SI,
    대만 .TW/.TWO, 베트남 .VN/.HM, 태국 .BK, 인도네시아 .JK, 뉴질랜드 .NZ): E 거래가 그날 정산 전에 열린다
    (정산이 E 거래를 담을 수 있다) → bound = E − 1일(E 전 마지막 정산).
  - 서쪽 시장(접미사 없는 미국·Reuters .O/.N/.K, 유럽 .DE/.F/.L/.PA/.AS/.SW/.MI, 캐나다 .TO/.V 등): E 정규장은
    KST 밤에 열려 E 날짜 정산은 전 거래일 장까지 담는다 → bound = E. 정산 시각에 따라 E 날짜 정산에 E 거래 일부가
    섞인다: 15:30 정산은 미국 주간거래, 20:05 정산은 유럽 E 장 초반(16:00 KST~)과 미국 프리마켓·주간거래.
- 국내 기준일 R만 알면(KIS 예탁원 기준일, 브리프 기준일): T+2 결제라 R(휴장이면 R 이하 마지막 거래일)에서
  2거래일 전 종가 보유자가 받는다 → bound = 그 거래일. KRX 휴장일 달력이 없는 연도는 주말·양력 고정 공휴일·
  연말 휴장일(12월 마지막 평일)만 빼서 근사한다(설·추석·대체공휴일·임시 휴장은 모른다).
- 해외 기준일만 있으면 그 시장의 배당락일처럼 다루고, 지급일만 있으면 지급일 − 1일이다(둘 다 근사).

수량: 수량이 없는 옛 정산은 이어서 보유한 가장 가까운 정산의 수량을 쓰되, 그 사이 평가액이 가격 변동만으로는
설명되지 않게 뛰면(매매가 있었다) 옮기지 않고 수량을 모른다고 한다(``VALUE_RATIO_BOUNDS``).
"""

from __future__ import annotations

from bisect import bisect_right
from datetime import date, timedelta

from domain.market_calendar import is_trading_day
from domain.portfolio_codes import is_korean_stock, yahoo_symbol

# 그날 장이 KST 15:30 정산 전에 열리는 아시아·태평양 시장(배당락일 거래가 그날 정산에 섞일 수 있다).
EAST_SUFFIXES = frozenset("KS KQ T HK AX SS SZ SI TW TWO VN HM BK JK NZ".split())
# 휴장일 달력이 없는 연도의 근사에 쓰는 KRX 양력 고정 휴장일(신정·삼일절·근로자의 날·어린이날·현충일·광복절·개천절·한글날·성탄절).
KRX_FIXED_HOLIDAYS = frozenset("01-01 03-01 05-01 05-05 06-06 08-15 10-03 10-09 12-25".split())
# 이웃한 두 정산(같은 종목) 사이 평가액(원화) 비율의 하루치 한계(정산 사이 평일 수만큼 거듭제곱). 벗어나면 가격만으로는
# 설명되지 않으므로 그 사이 수량이 바뀌었다고 본다. 국내는 가격제한폭 ±30%. 해외는 제한폭이 없어 운영 정산
# (2026-07~09, 수량이 같은 이웃 정산 2,385쌍)의 최대 일간 변동 1.19배를 덮는 1.22배(환율 포함)다.
VALUE_RATIO_BOUNDS = {"KR": (0.70, 1.30), "foreign": (1 / 1.22, 1.22)}

BASIS_CURRENT = "current"
BASIS_SNAPSHOT = "snapshot"
BASIS_EARLIEST = "earliest_snapshot"
BASIS_FALLBACK = "current_fallback"
# 기준 시점에 보유했지만 수량을 모르는 이유.
QUANTITY_NOT_RECORDED = "not_recorded"            # 이어서 보유한 정산 어디에도 수량 기록이 없다(기록 전에 판 종목)
QUANTITY_CHANGED = "changed_before_record"        # 수량 기록 전에 평가액이 매매로만 설명되게 바뀌었다


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


def _approximate_krx_trading_day(day: date) -> bool:
    """휴장일 달력이 없는 연도의 근사: 주말·양력 고정 휴장일·연말 휴장일(12월 마지막 평일)만 휴장으로 본다."""
    if day.weekday() >= 5 or day.strftime("%m-%d") in KRX_FIXED_HOLIDAYS:
        return False
    # 연말 휴장은 12월 31일, 그날이 주말이면 그 직전 평일이다(= 12월의 마지막 평일).
    return not (day.month == 12 and all((day + timedelta(days=k)).weekday() >= 5 for k in range(1, 32 - day.day)))


def _krx_day(day: date) -> tuple[bool, bool]:
    """(거래일 여부, 근사 여부). 달력이 없는 연도는 _approximate_krx_trading_day로 근사한다."""
    known = is_trading_day(day)
    if known is None:
        return _approximate_krx_trading_day(day), True
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


def _weekdays_between(start: date, end: date) -> int:
    """start 다음 날부터 end까지의 평일 수(최소 1)."""
    return max(1, sum((start + timedelta(days=k)).weekday() < 5 for k in range(1, (end - start).days + 1)))


class HoldingHistory:
    """사용자의 일별 정산 보유: 정산일 → 종목 identity → (수량, 원화 평가액). 수량 None = 보유, 수량 미기록.

    rows: date, stock_code, quantity, market_value. 수량이 없는 옛 정산 행은 평가액이 양수이면 보유로 본다.
    같은 날 같은 종목(코드 표기만 다른 행)은 수량·평가액을 합친다. 정산에 없는 종목은 그 정산 시점에 보유하지 않은 것이다
    (정산은 그 시점 잔고의 전 종목을 쓰며, 시세가 실패해도 행을 빼지 않는다 — 앞뒤 정산으로 메우지 않는다).
    """

    def __init__(self, rows: list[dict]):
        by_date: dict[str, dict[str, tuple[float | None, float]]] = {}
        for row in rows:
            day = str(row.get("date") or "")[:10]
            if not _day(day):
                continue
            identity = holding_identity(row.get("stock_code"))
            quantity = row.get("quantity")
            value = float(row.get("market_value") or 0)
            state = (None if value > 0 else 0.0) if quantity is None else float(quantity)
            bucket = by_date.setdefault(day, {})
            if identity in bucket:
                previous, previous_value = bucket[identity]
                state = None if previous is None or state is None else previous + state
                value += previous_value
            bucket[identity] = (state, value)
        self.dates = sorted(by_date)
        self.by_date = by_date

    def __bool__(self) -> bool:
        return bool(self.dates)

    def _state(self, index: int, identity: str):
        entry = self.by_date[self.dates[index]].get(identity)
        return _MISSING if entry is None else entry[0]

    def _value(self, index: int, identity: str) -> float:
        entry = self.by_date[self.dates[index]].get(identity)
        return entry[1] if entry else 0.0

    def _price_only(self, identity: str, a: int, b: int) -> bool:
        """이웃한 a·b 정산 사이 평가액 변화가 가격(·환율) 변동만으로 설명되는가(VALUE_RATIO_BOUNDS).

        평가액을 모르면(0 이하) 판단하지 않는다(True). 한계 안이라도 작은 매매는 가려내지 못한다.
        """
        first, second = min(a, b), max(a, b)
        before, after = self._value(first, identity), self._value(second, identity)
        if before <= 0 or after <= 0:
            return True
        days = _weekdays_between(_day(self.dates[first]), _day(self.dates[second]))
        low, high = VALUE_RATIO_BOUNDS["KR" if is_korean_stock(identity) else "foreign"]
        return low ** days <= after / before <= high ** days

    def _known_quantity(self, identity: str, index: int) -> tuple[float | None, str | None, str | None]:
        """수량 미기록 정산의 보유 수량: 이어서 보유한 가장 가까운 뒤(없으면 앞) 정산의 수량.

        반환 (수량, 그 정산일, 모르는 이유). 가는 길에 평가액이 매매로만 설명되게 바뀌면(_price_only가 False) 그 방향의
        수량은 기준 시점 수량이 아니므로 쓰지 않는다. 둘 다 못 쓰면 (None, None, QUANTITY_CHANGED|QUANTITY_NOT_RECORDED).
        """
        changed = False
        for step in (1, -1):
            previous, k = index, index + step
            while 0 <= k < len(self.dates):
                state = self._state(k, identity)
                if state is _MISSING or not _held(state):
                    break
                if not self._price_only(identity, previous, k):
                    changed = True
                    break
                if state is not None:
                    return state, self.dates[k], None
                previous, k = k, k + step
        return None, None, QUANTITY_CHANGED if changed else QUANTITY_NOT_RECORDED

    def at(self, code: str, bound: date) -> dict:
        """bound 이하 마지막 정산의 보유. bound가 첫 정산보다 이르면 첫 정산(가장 가까운 근거)을 쓴다.

        반환: held, basis('snapshot'|'earliest_snapshot'), as_of(그 정산일), quantity(모르면 None),
        quantity_as_of(수량을 다른 정산에서 가져왔으면 그 날짜), quantity_unknown(보유인데 수량을 모르는 이유).
        """
        identity = holding_identity(code)
        index = bisect_right(self.dates, bound.isoformat()) - 1
        basis = BASIS_SNAPSHOT
        if index < 0:
            index, basis = 0, BASIS_EARLIEST
        out = {"basis": basis, "as_of": self.dates[index], "quantity_as_of": None, "quantity_unknown": None}
        state = self._state(index, identity)
        if state is _MISSING or not _held(state):
            return {**out, "held": False, "quantity": None}
        if state is not None:
            return {**out, "held": True, "quantity": state}
        quantity, quantity_day, reason = self._known_quantity(identity, index)
        return {**out, "held": True, "quantity": quantity, "quantity_as_of": quantity_day, "quantity_unknown": reason}


def entitlement(code: str, raw: dict, today: date, history: HoldingHistory, current_quantity: float | None) -> dict:
    """배당 한 건의 보유 근거. held가 False면 기준 시점에 보유하지 않아 일정에서 뺀다.

    - 예상(estimated) 일정, 기준 시점이 오늘 이후 → 현재 보유('current').
    - 기준 시점이 지났으면 그 시점 이하 마지막 정산('snapshot'), 첫 정산보다 이르면 첫 정산('earliest_snapshot').
      정산 기록이 전혀 없으면 현재 보유('current_fallback').
    반환: held, quantity, holding_basis, holding_as_of, quantity_as_of, quantity_unknown_reason,
    reference_date, reference_rule, reference_approximate, excluded_reason(뺀 이유).
    """
    reference = None if raw.get("estimated") else reference_point(raw, code)
    out = {"reference_date": reference["date"].isoformat() if reference else None,
           "reference_rule": reference["rule"] if reference else None,
           "reference_approximate": bool(reference and reference["approximate"]),
           "holding_as_of": None, "quantity_as_of": None, "quantity_unknown_reason": None}
    held_now = current_quantity is not None and current_quantity > 0
    if reference is None or reference["date"] >= today or not history:
        basis = BASIS_CURRENT if reference is None or reference["date"] >= today else BASIS_FALLBACK
        return {**out, "held": held_now, "quantity": current_quantity if held_now else None, "holding_basis": basis,
                "excluded_reason": None if held_now else "not_held_now"}
    point = history.at(code, reference["date"])
    out.update(holding_basis=point["basis"], holding_as_of=point["as_of"], quantity_as_of=point["quantity_as_of"],
               quantity_unknown_reason=point["quantity_unknown"])
    if not point["held"]:
        reason = "absent_from_first_record" if point["basis"] == BASIS_EARLIEST else "not_held_at_reference"
        return {**out, "held": False, "quantity": None, "excluded_reason": reason}
    return {**out, "held": True, "quantity": point["quantity"], "excluded_reason": None}
