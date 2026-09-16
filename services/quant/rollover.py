"""종목별 한 월물만 선택한다. 최종거래일 2거래일 전부터 다음 상장 월물 관찰."""

import re
from collections import defaultdict
from datetime import date, timedelta

from repositories.quant import QuantError

POLICY = "front-month-roll-2-trading-days-v1"
CALENDAR_VERSION = "krx-2026-2027-reviewed-20260916"
# 공휴일·대체공휴일 + 근로자의 날·연말 휴장. 임시 휴장은 확인 후 추가한다.
# 2026: finance-pi 달력과 대조, 제헌절 포함.
# 2027: 우주항공청 2026-06-29 월력요항과 KRX 휴장 규칙.
# https://www.kasa.go.kr/prog/plcyBrf/brief/kor/sub01_01_04/view.do?plcyBrfNo=431
# https://www.krx.co.kr/contents/OPN/01/01040401/OPN01040401T1.jsp
HOLIDAYS = {
    2026: frozenset("01-01 02-16 02-17 02-18 03-02 05-01 05-05 05-25 06-03 07-17 "
                    "08-17 09-24 09-25 10-05 10-09 12-25 12-31".split()),
    2027: frozenset("01-01 02-06 02-07 02-08 02-09 03-01 05-01 05-03 05-05 05-13 "
                    "06-06 07-17 07-19 08-15 08-16 09-14 09-15 09-16 10-03 10-04 "
                    "10-09 10-11 12-25 12-27 12-31".split()),
}


def trading_day(day):
    if day.year not in HOLIDAYS:
        raise QuantError(f"{day.year}년 거래일 달력 미확인: 월물 선택을 보류합니다.")
    return day.weekday() < 5 and day.strftime("%m-%d") not in HOLIDAYS[day.year]


def previous_trading_day(day):
    day -= timedelta(days=1)
    while not trading_day(day):
        day -= timedelta(days=1)
    return day


def schedule(month):
    """주식선물: 두 번째 목요일, 휴장이면 이전 거래일. 반환값은 규칙상 예정일."""
    first = date(int(month[:4]), int(month[4:]), 1)
    expiry = first + timedelta(days=(3 - first.weekday()) % 7 + 7)
    if not trading_day(expiry):
        expiry = previous_trading_day(expiry)
    roll = previous_trading_day(previous_trading_day(expiry))
    return expiry, roll


def active(row, today):
    """재시작·전환 직전 수신·구독 교체 중에도 이전 월물 신호를 거절한다."""
    return (row.get("roll_policy") == POLICY and trading_day(today)
            and today.isoformat() < row.get("roll_on", ""))


def universe(contracts, today):
    trading_day(today)  # 알 수 없는 연도를 평일로 간주하지 않는다.
    groups = defaultdict(list)
    for row in contracts:
        groups[row["spot_code"]].append(row)
    selected, excluded = [], []
    for stock, rows in sorted(groups.items()):
        try:
            months = {}
            for row in rows:
                # 기초자산 연결은 마스터 필드, 결제월만 공식 표시명에서 읽는다.
                match = re.search(r"\bF\s+(20\d{2}(?:0[1-9]|1[0-2]))\b", row.get("contract_name", ""))
                if not match:
                    raise QuantError("결제월 식별 불가")
                month = match[1]
                if month in months:
                    raise QuantError("동일 결제월 계약 중복: 권리조정 확인 필요")
                months[month] = row
            front, chosen = None, None
            for month, row in sorted(months.items()):
                if month < today.strftime("%Y%m"):
                    continue
                expiry, roll = schedule(month)
                if expiry < today:
                    continue
                if front is None:
                    front = row
                if today >= roll:
                    continue
                chosen = {**row, "delivery_month": month, "expiry": expiry.strftime("%Y%m%d"),
                          "roll_on": roll.isoformat(), "roll_policy": POLICY,
                          "expiry_verified": False, "contract_role": "front" if row is front else "next",
                          "rolled_from": front["contract"] if row is not front else None}
                break
            if chosen is None:
                raise QuantError("전환 가능한 다음 월물 없음")
            selected.append(chosen)
        except (QuantError, ValueError) as exc:
            excluded.append({"spot_code": stock, "name": rows[0]["name"], "reason": str(exc)})
    return {"contracts": selected, "excluded": excluded, "source_contracts": len(contracts),
            "underlyings": len(groups), "roll_policy": POLICY, "roll_trading_days": 2,
            "calendar_version": CALENDAR_VERSION, "calendar_until": "2027-12-31"}
