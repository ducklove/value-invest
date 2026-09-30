"""KST 단일 출처 — fin_commons.timeutil 과 같은 고정 오프셋 정의.

한국은 1988년 이후 DST가 없으므로 ZoneInfo("Asia/Seoul")와 현대 날짜에서
오프셋·tzname("KST")이 같다. 순수 stdlib만 쓰므로 repositories/services/루트
모듈 어디서든 import 할 수 있다.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

KST = timezone(timedelta(hours=9), name="KST")


def now_kst() -> datetime:
    """현재 시각(KST aware)."""
    return datetime.now(KST)


def today_kst() -> date:
    """KST 기준 오늘 날짜."""
    return now_kst().date()
