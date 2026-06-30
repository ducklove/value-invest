from datetime import datetime, timezone

from services.portfolio import time_windows as tw


def test_today_baseline_uses_previous_settlement_before_20_kst():
    assert tw.portfolio_today_baseline_date(datetime(2026, 5, 9, 19, 59, 59)) == "2026-05-08"


def test_today_baseline_uses_current_settlement_at_20_kst():
    assert tw.portfolio_today_baseline_date(datetime(2026, 5, 9, 20, 0, 0)) == "2026-05-09"


def test_today_baseline_converts_aware_datetimes_to_kst():
    assert tw.portfolio_today_baseline_date(datetime(2026, 5, 9, 10, 59, tzinfo=timezone.utc)) == "2026-05-08"
    assert tw.portfolio_today_baseline_date(datetime(2026, 5, 9, 11, 0, tzinfo=timezone.utc)) == "2026-05-09"


def test_intraday_settlement_marker_filters_completed_window():
    assert not tw.is_after_settlement_marker("2026-05-09T19:59:59", "2026-05-09")
    assert tw.is_after_settlement_marker("2026-05-09T20:00:00", "2026-05-09")


def test_intraday_axis_baseline_timestamp_is_start_of_chart_day():
    assert tw.intraday_axis_baseline_ts("2026-05-09") == "2026-05-09T20:00"


def test_intraday_axis_window_uses_settlement_to_next_settlement():
    assert tw.intraday_axis_window(datetime(2026, 5, 9, 19, 59, 59)) == (
        "2026-05-08T20:00",
        "2026-05-09T20:00",
    )
    assert tw.intraday_axis_window(datetime(2026, 5, 9, 20, 0, 0)) == (
        "2026-05-09T20:00",
        "2026-05-10T20:00",
    )


def test_settlement_marker_seconds_uses_20_kst_boundary():
    assert tw.settlement_marker_seconds("2026-05-09") == "2026-05-09T20:00:00"
