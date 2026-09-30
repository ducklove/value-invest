"""domain.timeutil — 허브 KST 단일 출처."""

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from domain import timeutil


def test_kst_is_fixed_plus_nine_named_kst():
    moment = datetime(2026, 9, 30, 12, tzinfo=timeutil.KST)
    assert moment.utcoffset() == timedelta(hours=9)
    assert moment.tzname() == "KST"
    # 현대 날짜에서는 IANA Asia/Seoul 과 같은 순간·같은 벽시계.
    seoul = datetime(2026, 9, 30, 12, tzinfo=ZoneInfo("Asia/Seoul"))
    assert moment == seoul
    assert moment.isoformat() == seoul.isoformat() == "2026-09-30T12:00:00+09:00"


def test_now_kst_is_aware_and_today_matches():
    now = timeutil.now_kst()
    assert now.utcoffset() == timedelta(hours=9)
    assert abs(now - datetime.now(timezone.utc)) < timedelta(minutes=1)
    today = timeutil.today_kst()
    assert isinstance(today, date) and not isinstance(today, datetime)
    assert today in {now.date(), timeutil.now_kst().date()}


def test_time_windows_reexports_single_source():
    from services.portfolio import time_windows

    assert time_windows.KST is timeutil.KST
    assert time_windows.now_kst is timeutil.now_kst
