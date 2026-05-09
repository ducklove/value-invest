from datetime import datetime, timezone

from services.portfolio import time_windows as tw


def test_today_baseline_uses_previous_settlement_before_22_kst():
    assert tw.portfolio_today_baseline_date(datetime(2026, 5, 9, 21, 59, 59)) == "2026-05-08"


def test_today_baseline_uses_current_settlement_at_22_kst():
    assert tw.portfolio_today_baseline_date(datetime(2026, 5, 9, 22, 0, 0)) == "2026-05-09"


def test_today_baseline_converts_aware_datetimes_to_kst():
    assert tw.portfolio_today_baseline_date(datetime(2026, 5, 9, 12, 59, tzinfo=timezone.utc)) == "2026-05-08"
    assert tw.portfolio_today_baseline_date(datetime(2026, 5, 9, 13, 0, tzinfo=timezone.utc)) == "2026-05-09"


def test_intraday_settlement_marker_filters_completed_window():
    assert not tw.is_after_settlement_marker("2026-05-09T21:59:59", "2026-05-09")
    assert tw.is_after_settlement_marker("2026-05-09T22:00:00", "2026-05-09")


def test_intraday_axis_baseline_timestamp_is_start_of_chart_day():
    assert tw.intraday_axis_baseline_ts("2026-05-09") == "2026-05-09T00:00"
