from datetime import datetime, timezone

from services.portfolio import time_windows as tw


def test_today_keeps_previous_date_before_and_after_close():
    for hour, minute in ((15, 29), (15, 30), (15, 35), (20, 0), (23, 59)):
        assert tw.portfolio_today_baseline_date(datetime(2026, 9, 30, hour, minute)) == "2026-09-29"


def test_today_uses_korean_calendar_date():
    assert tw.portfolio_today_baseline_date(datetime(2026, 9, 30, 14, 59, tzinfo=timezone.utc)) == "2026-09-29"
    assert tw.portfolio_today_baseline_date(datetime(2026, 9, 30, 15, 0, tzinfo=timezone.utc)) == "2026-09-30"


def test_regular_close_marker_includes_minutes():
    assert tw.settlement_marker_seconds("2026-09-30") == "2026-09-30T15:30:00"
    assert not tw.is_after_settlement_marker("2026-09-30T15:29:59", "2026-09-30")
    assert tw.is_after_settlement_marker("2026-09-30T15:30:01", "2026-09-30")
    assert tw.intraday_axis_baseline_ts("2026-09-30") == "2026-09-30T15:30"


def test_chart_window_preserves_aftermarket_without_resetting_baseline():
    for hour in (15, 20):
        assert tw.intraday_axis_window(datetime(2026, 9, 30, hour, 45)) == (
            "2026-09-29T15:30", "2026-09-30T20:00",
        )
