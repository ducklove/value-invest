"""Best-effort equity-market session clock — which markets are trading *now*.

The live 마켓테이프 features indices for markets that are currently open and hides
closed ones (whose quotes are stale last-close values). This answers "is market X
open at instant T" using pure KST arithmetic — no IANA tz database needed, so it
behaves identically on the Linux box and on Windows dev/CI.

Scope/caveats: regular continuous sessions only. Holidays are NOT modeled (a
holiday reads as "open"); lunch breaks (JP/HK/CN) are treated as open. Good enough
for *featuring* a market on the tape, not for trading decisions. Korea has no DST
(KST = UTC+9 fixed); US Eastern DST is derived from the standard
2nd-Sunday-March .. 1st-Sunday-November rule.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))

# Market codes this clock knows about.
MARKETS = ("KR", "US", "JP", "HK", "CN")


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """Date of the n-th ``weekday`` (0=Mon .. 6=Sun) of ``month`` (n >= 1)."""
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (n - 1))


def us_eastern_is_dst(d: date) -> bool:
    """US Eastern DST window: 2nd Sun of March (02:00) .. 1st Sun of November (02:00).

    Date-granular (ignores the 02:00 changeover instant) — at most an hour off on
    the two transition days a year, which only nudges the US session edge on the
    tape and never matters for featuring.
    """
    start = _nth_weekday(d.year, 3, 6, 2)   # 2nd Sunday of March
    end = _nth_weekday(d.year, 11, 6, 1)    # 1st Sunday of November
    return start <= d < end


def _in_window(minutes: int, start: tuple[int, int], end: tuple[int, int]) -> bool:
    return start[0] * 60 + start[1] <= minutes <= end[0] * 60 + end[1]


def open_markets(now: datetime | None = None) -> set[str]:
    """Set of market codes open at ``now`` (defaults to the current KST instant).

    KR/JP/HK/CN sessions fall on the same KST calendar day, so they are tested in
    KST directly. The US session straddles KST midnight, so it is evaluated on the
    US Eastern local clock (weekday + 09:30–16:00 ET).
    """
    now = (now or datetime.now(KST)).astimezone(KST)
    out: set[str] = set()

    if now.weekday() < 5:  # Mon–Fri in KST
        hm = now.hour * 60 + now.minute
        if _in_window(hm, (9, 0), (15, 30)):   # KRX 09:00–15:30
            out.add("KR")
        if _in_window(hm, (9, 0), (15, 0)):    # 도쿄 09:00–15:00 (JST = KST)
            out.add("JP")
        if _in_window(hm, (10, 30), (17, 0)):  # 홍콩 09:30–16:00 (HKT = KST-1h)
            out.add("HK")
        if _in_window(hm, (10, 30), (16, 0)):  # 상하이 09:30–15:00 (CST = KST-1h)
            out.add("CN")

    # US — convert to US Eastern and test that local clock + weekday.
    utc = now.astimezone(timezone.utc)
    et_offset = 4 if us_eastern_is_dst(utc.date()) else 5
    et = utc - timedelta(hours=et_offset)
    if et.weekday() < 5 and _in_window(et.hour * 60 + et.minute, (9, 30), (16, 0)):
        out.add("US")

    return out
