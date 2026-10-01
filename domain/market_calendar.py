"""KRX 거래일과 포트폴리오 평가 시각. 임시 변경은 명시적 설정으로 적용한다.

특수 세션일(현재는 수능일 후보: 11월 셋째 주 목요일, 15~21일)은 개장·마감
시각이 KRX 공지로 바뀌므로 추정하지 않는다. ``closing_at`` 은 그런 날에
``PORTFOLIO_MARKET_SESSIONS`` override 가 없으면 ``MarketCalendarUnknown``
(ValueError 하위 클래스)을 던지고, 정기 작업은 이를 로그된 실패로 처리한다.
운영자는 KRX 공지를 확인해 .env 에 예) ``PORTFOLIO_MARKET_SESSIONS={"2026-11-19":"16:30"}``
(값은 정규장 마감 HH:MM, 휴장이면 빈 문자열/null)을 설정한다.
``services.data_quality.check_market_calendar_coverage`` 가 30일 전부터
미설정 특수 세션일을 경고한다.
"""

import json
import os
from datetime import date, datetime, timedelta

from domain.timeutil import KST

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

SESSIONS_ENV = "PORTFOLIO_MARKET_SESSIONS"


class MarketCalendarUnknown(ValueError):
    """거래일/세션 시각을 달력만으로 확정할 수 없다 — 운영자 확인 필요.

    ValueError 하위 클래스라 기존 ``except ValueError`` 처리 경로가 그대로
    동작한다.
    """


def session_overrides() -> dict:
    """PORTFOLIO_MARKET_SESSIONS JSON ({"YYYY-MM-DD": "HH:MM" | "" | null})."""
    return json.loads(os.getenv(SESSIONS_ENV, "{}"))


def is_csat_candidate(day: date) -> bool:
    """수능일 후보(11월 15~21일 목요일). 휴장·세션 시각은 KRX 공지로 확정한다."""
    return day.month == 11 and day.weekday() == 3 and 15 <= day.day <= 21


def closing_at(day: str) -> datetime | None:
    parsed = date.fromisoformat(day)
    overrides = session_overrides()
    if day in overrides:
        clock = overrides[day]
        return datetime.fromisoformat(f"{day}T{clock}:00").replace(tzinfo=KST) if clock else None
    if parsed.year not in HOLIDAYS:
        raise MarketCalendarUnknown(f"{parsed.year}년 거래일 달력을 확인해야 합니다.")
    if parsed.weekday() >= 5 or parsed.strftime("%m-%d") in HOLIDAYS[parsed.year]:
        return None
    # 수능일의 변경 시간을 추정해 적용하지 않는다. KRX 공지 확인 후 override.
    if is_csat_candidate(parsed):
        raise MarketCalendarUnknown("수능일 거래시간 확인 필요: PORTFOLIO_MARKET_SESSIONS 설정")
    return datetime.fromisoformat(f"{day}T15:30:00").replace(tzinfo=KST)


def is_trading_day(day: date) -> bool | None:
    """KRX 정규장 거래일인가. 휴장일 달력이 없는 연도는 None(호출자가 근사 처리를 정한다).

    수능일 후보는 개장·마감 시각만 바뀌는 거래일로 본다. ``PORTFOLIO_MARKET_SESSIONS`` 로
    휴장을 지정한 날은 휴장이다(``closing_at`` 과 같은 규칙).
    """
    try:
        return closing_at(day.isoformat()) is not None
    except MarketCalendarUnknown:
        return True if day.year in HOLIDAYS else None


def unconfigured_special_sessions(start: date, days: int, overrides: dict | None = None) -> list[date]:
    """[start, start+days] 중 override 없는 특수 세션 후보일(수능일 후보).

    주말·등록된 휴장일은 closing_at 이 먼저 None 을 돌려주므로 제외한다.
    """
    overrides = session_overrides() if overrides is None else overrides
    found = []
    for offset in range(days + 1):
        day = start + timedelta(days=offset)
        if day.isoformat() in overrides or not is_csat_candidate(day):
            continue
        holidays = HOLIDAYS.get(day.year)
        if holidays is not None and day.strftime("%m-%d") in holidays:
            continue
        found.append(day)
    return found


def missing_holiday_years(start: date, days: int) -> list[int]:
    """[start, start+days] 에 걸친 연도 중 HOLIDAYS 에 없는 연도."""
    end = start + timedelta(days=days)
    return [year for year in range(start.year, end.year + 1) if year not in HOLIDAYS]
