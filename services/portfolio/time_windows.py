from __future__ import annotations

from datetime import date, datetime, timedelta

from domain.timeutil import KST, now_kst

SETTLEMENT_HOUR = 15
SETTLEMENT_MINUTE = 30


def _as_kst(now: datetime | None) -> datetime:
    if now is None:
        return now_kst()
    if now.tzinfo is None:
        return now
    return now.astimezone(KST)


def today_kst_date(now: datetime | None = None) -> date:
    return _as_kst(now).date()


def portfolio_today_baseline_date(now: datetime | None = None, *, settlement_hour: int = SETTLEMENT_HOUR) -> str:
    """오늘 성과는 마감 후에도 직전 거래일을 기준으로 유지한다."""
    return (_as_kst(now).date() - timedelta(days=1)).isoformat()


def settlement_marker(
    baseline_date: str,
    *,
    settlement_hour: int = SETTLEMENT_HOUR,
    settlement_minute: int = SETTLEMENT_MINUTE,
) -> str:
    return f"{baseline_date}T{settlement_hour:02d}:{settlement_minute:02d}"


def settlement_marker_seconds(
    baseline_date: str,
    *,
    settlement_hour: int = SETTLEMENT_HOUR,
    settlement_minute: int = SETTLEMENT_MINUTE,
) -> str:
    return f"{settlement_marker(baseline_date, settlement_hour=settlement_hour, settlement_minute=settlement_minute)}:00"


def next_settlement_marker(baseline_date: str, *, settlement_hour: int = SETTLEMENT_HOUR) -> str:
    next_day = date.fromisoformat(baseline_date) + timedelta(days=1)
    return settlement_marker(next_day.isoformat(), settlement_hour=settlement_hour)


def intraday_axis_window(now: datetime | None = None, *, settlement_hour: int = SETTLEMENT_HOUR) -> tuple[str, str]:
    baseline = portfolio_today_baseline_date(now, settlement_hour=settlement_hour)
    return (
        settlement_marker(baseline, settlement_hour=settlement_hour),
        f"{today_kst_date(now).isoformat()}T20:00",
    )


def is_after_settlement_marker(ts: object, baseline_date: str, *, settlement_hour: int = SETTLEMENT_HOUR) -> bool:
    return str(ts or "") > settlement_marker_seconds(baseline_date, settlement_hour=settlement_hour)


def intraday_axis_baseline_ts(query_date: date | str) -> str:
    day = query_date.isoformat() if isinstance(query_date, date) else str(query_date)
    return settlement_marker(day)
