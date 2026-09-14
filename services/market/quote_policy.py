"""KRX 애프터마켓 시행일과 거래소 간 체결 시각 비교."""

from datetime import date, datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))
KRX_AFTERMARKET_START = date(2026, 9, 14)


def integrated_market_enabled(now: datetime | None = None) -> bool:
    current = now or datetime.now(KST)
    if current.tzinfo is not None:
        current = current.astimezone(KST)
    return current.date() >= KRX_AFTERMARKET_START


def trade_timestamp(value: object) -> float | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=KST)
        return parsed.timestamp()
    except (TypeError, ValueError, OverflowError):
        return None
