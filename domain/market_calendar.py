"""KRX 거래일과 포트폴리오 평가 시각. 임시 변경은 명시적 설정으로 적용한다."""

import json
import os
from datetime import date, datetime
from zoneinfo import ZoneInfo

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



def closing_at(day: str) -> datetime | None:
    parsed = date.fromisoformat(day)
    overrides = json.loads(os.getenv("PORTFOLIO_MARKET_SESSIONS", "{}"))
    if day in overrides:
        clock = overrides[day]
        return datetime.fromisoformat(f"{day}T{clock}:00").replace(tzinfo=ZoneInfo("Asia/Seoul")) if clock else None
    if parsed.year not in HOLIDAYS:
        raise ValueError(f"{parsed.year}년 거래일 달력을 확인해야 합니다.")
    if parsed.weekday() >= 5 or parsed.strftime("%m-%d") in HOLIDAYS[parsed.year]:
        return None
    # 수능일의 변경 시간을 추정해 적용하지 않는다. KRX 공지 확인 후 override.
    if parsed.month == 11 and parsed.weekday() == 3 and 15 <= parsed.day <= 21:
        raise ValueError("수능일 거래시간 확인 필요: PORTFOLIO_MARKET_SESSIONS 설정")
    return datetime.fromisoformat(f"{day}T15:30:00").replace(tzinfo=ZoneInfo("Asia/Seoul"))
