"""배당 캘린더 서비스 (로드맵 신규 기능 ④).

보유 종목의 배당 이벤트(최근 + 다가오는)를 월 단위 캘린더 + 월별 예상
현금흐름 합계로 만든다.

데이터 소스 — 전부 기존 테이블에서만 파생하며 새 외부 수집은 하지 않는다:
  * market_data.dividend_per_share         — 국내 주식 연간 DPS (DART 수집)
  * preferred_dividends.dividend_per_share — 우선주 연간 DPS (수기 Google Sheet)
  * foreign_dividends.dps_native/currency  — 해외 종목 trailing 연간 DPS (yfinance)
  * daily_market_briefs.payload.upcoming_events — 일일 브리프 생성 시 KIS 배당
    일정에서 받아 저장된 '배당기준일' 행 (있을 때만 — 확정 이벤트로 표시)
  * user_portfolio                          — 보유 수량 (주당 × 수량 = 예상 금액)

추정 휴리스틱 — 종목별 정확한 지급일 데이터가 없으므로, 알려진 연간 DPS 로
'예상' 이벤트를 생성한다 (confirmed:false, type:'estimated' 로 명확히 구분):
  * 국내 주식(우선주 포함): 12월 결산 정기배당 가정 — 매년 4월 15일 연 1회
    지급 이벤트 (3월 주총 → 4월 지급이 일반적).
  * 해외 USD 종목: 분기 배당 가정 — 3/6/9/12월 15일, 연간 DPS 의 1/4 씩.
  * 그 외 통화(관리자 수동 KRW override 포함): 반기 배당 가정 — 6/12월
    15일, 연간 DPS 의 1/2 씩.
확정 이벤트는 최근 daily brief 의 '배당기준일' 행뿐이며 type='ex_date'.
기준일은 현금 유입일이 아니므로 월별 합계(total_krw)에는 포함하지 않고
이벤트 목록에만 노출한다.

환산: 해외 per-share 는 services.portfolio.fx 의 실시간 환율로 KRW 환산.
fx_rate_for_currency 는 조회 실패/미지원 통화에서 1.0 을 돌려주므로, 1.0 은
'환율 모름'으로 보고 foreign_dividends.dps_krw(수집 시점 환산)로 폴백한다.
"""

from __future__ import annotations

import json
import logging
from datetime import date

from repositories import db as db_repo
from repositories import foreign_dividends as foreign_dividends_repo
from repositories import portfolio as portfolio_repo
from services.portfolio import fx
from services.portfolio.identifiers import (
    is_korean_stock,
    is_special_asset,
    normalize_portfolio_code,
)

logger = logging.getLogger(__name__)

# 추정 이벤트 일자/월 규약 — 위 모듈 docstring 의 휴리스틱과 한 몸.
EVENT_DAY = 15
KR_PAY_MONTHS = (4,)            # 국내 연 1회 결산배당 지급(추정)
QUARTERLY_MONTHS = (3, 6, 9, 12)  # USD 분기 배당(추정)
SEMIANNUAL_MONTHS = (6, 12)       # 기타 통화 반기 배당(추정)


def _shift_month(year: int, month: int, delta: int) -> tuple[int, int]:
    """(year, month) 를 delta 개월 이동. month 는 1~12."""
    base = year * 12 + (month - 1) + delta
    return base // 12, base % 12 + 1


def _month_key(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def window_months(today: date, months_back: int, months_forward: int) -> list[tuple[int, int]]:
    """오늘이 속한 달 기준 [back .. forward] 의 (year, month) 목록 (포함)."""
    out: list[tuple[int, int]] = []
    for delta in range(-months_back, months_forward + 1):
        out.append(_shift_month(today.year, today.month, delta))
    return out


def estimated_events_for_holding(
    item: dict,
    annual_dps_krw: float | None,
    foreign_row: dict | None,
    months: list[tuple[int, int]],
) -> list[dict]:
    """한 보유 종목의 '예상' 배당 이벤트 목록 (순수 함수 — I/O 없음).

    annual_dps_krw: 실시간 환율 반영이 끝난 연간 주당 배당(KRW).
    foreign_row:    foreign_dividends 행 (해외 종목만, 표시용 native 금액/통화).
    """
    code = normalize_portfolio_code(item.get("stock_code"))
    if not code or is_special_asset(code):
        return []  # 현금/금/암호화폐 — 배당 개념 없음
    try:
        shares = float(item.get("quantity") or 0)
    except (TypeError, ValueError):
        return []
    if shares <= 0:
        return []
    if annual_dps_krw is None or annual_dps_krw <= 0:
        return []  # '무배당 확정(0)' 과 '미수집(None)' 모두 이벤트 없음

    if is_korean_stock(code):
        cadence, label = KR_PAY_MONTHS, "연간 배당 (예상)"
        currency, annual_native = "KRW", float(annual_dps_krw)
    else:
        currency = str((foreign_row or {}).get("currency") or "KRW").upper()
        native = (foreign_row or {}).get("dps_native")
        # 수동 override(KRW 직접 입력) 행은 native 가 없다 — KRW 로 표시.
        if native is None or float(native) <= 0 or currency == "KRW":
            currency, annual_native = "KRW", float(annual_dps_krw)
        else:
            annual_native = float(native)
        if currency == "USD":
            cadence, label = QUARTERLY_MONTHS, "분기 배당 (예상)"
        else:
            cadence, label = SEMIANNUAL_MONTHS, "반기 배당 (예상)"

    fraction = 1.0 / len(cadence)
    events: list[dict] = []
    for year, month in months:
        if month not in cadence:
            continue
        events.append({
            "date": date(year, month, EVENT_DAY).isoformat(),
            "stock_code": code,
            "stock_name": item.get("stock_name") or code,
            "label": label,
            "type": "estimated",
            "amount_per_share": round(annual_native * fraction, 4),
            "currency": currency,
            "shares": shares,
            "expected_amount_krw": round(float(annual_dps_krw) * fraction * shares),
            "confirmed": False,
        })
    return events


async def _latest_brief_upcoming_events(google_sub: str) -> list[dict]:
    """가장 최근 daily brief 의 upcoming_events('배당기준일') 행.

    repositories.market_brief 는 (sub, date) 단건 조회만 제공 — 최신 행
    선택은 캘린더 전용 요구라 여기서 직접 질의한다 (읽기 전용).
    """
    try:
        db = await db_repo.get_db()
        cursor = await db.execute(
            """SELECT payload_json FROM daily_market_briefs
               WHERE google_sub = ? ORDER BY brief_date DESC LIMIT 1""",
            (google_sub,),
        )
        row = await cursor.fetchone()
    except Exception as exc:  # 캘린더는 brief 없이도 동작해야 한다
        logger.warning("dividend_calendar: brief lookup failed: %s", exc)
        return []
    if not row:
        return []
    try:
        payload = json.loads(row["payload_json"] or "{}")
    except json.JSONDecodeError:
        return []
    events = payload.get("upcoming_events")
    return events if isinstance(events, list) else []


def _confirmed_events(
    brief_events: list[dict],
    holdings_by_code: dict[str, dict],
    start_iso: str,
    end_iso: str,
) -> list[dict]:
    """brief 의 '배당기준일' 행 → 확정(ex_date) 이벤트. 보유 중 + 기간 내만."""
    out: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for ev in brief_events:
        if not isinstance(ev, dict):
            continue
        code = normalize_portfolio_code(ev.get("stock_code"))
        day = str(ev.get("date") or "")
        holding = holdings_by_code.get(code)
        if not holding or not day or not (start_iso <= day <= end_iso):
            continue
        if (code, day) in seen:
            continue
        seen.add((code, day))
        try:
            shares = float(holding.get("quantity") or 0)
        except (TypeError, ValueError):
            continue
        if shares <= 0:
            continue
        # KIS 의 주당 배당금(원). 없으면 금액 추정 대신 None — 정직하게.
        try:
            per_share = float(ev["amount"]) if ev.get("amount") is not None else None
        except (TypeError, ValueError):
            per_share = None
        out.append({
            "date": day,
            "stock_code": code,
            "stock_name": holding.get("stock_name") or ev.get("stock_name") or code,
            "label": "배당기준일 (확정)",
            "type": "ex_date",
            "amount_per_share": per_share,
            "currency": "KRW",
            "shares": shares,
            "expected_amount_krw": round(per_share * shares) if per_share else None,
            "confirmed": True,
        })
    return out


def _monthly_aggregation(events: list[dict], months: list[tuple[int, int]]) -> list[dict]:
    """월별 합계 — total_krw 는 현금 유입 이벤트만(기준일 ex_date 제외),
    count 는 그 달의 전체 이벤트 수(펼친 목록 행수와 일치)."""
    totals: dict[str, float] = {}
    counts: dict[str, int] = {}
    for ev in events:
        month = ev["date"][:7]
        counts[month] = counts.get(month, 0) + 1
        if ev["type"] != "ex_date" and ev.get("expected_amount_krw"):
            totals[month] = totals.get(month, 0) + float(ev["expected_amount_krw"])
    return [
        {
            "month": _month_key(y, m),
            "total_krw": round(totals.get(_month_key(y, m), 0)),
            "count": counts.get(_month_key(y, m), 0),
        }
        for y, m in months
    ]


async def build_calendar(
    google_sub: str,
    months_back: int = 2,
    months_forward: int = 10,
    *,
    today: date | None = None,
) -> dict:
    """배당 캘린더 페이로드 — 이벤트 목록(날짜순) + 월별 합계 + 요약."""
    today = today or date.today()
    months = window_months(today, months_back, months_forward)
    start_iso = date(months[0][0], months[0][1], 1).isoformat()
    end_y, end_m = _shift_month(months[-1][0], months[-1][1], 1)
    end_iso = date(end_y, end_m, 1).isoformat()  # 배타 상한 (다음 달 1일)

    holdings = [
        h for h in await portfolio_repo.get_portfolio(google_sub)
        if not is_special_asset(h.get("stock_code")) and float(h.get("quantity") or 0) > 0
    ]
    events: list[dict] = []
    if holdings:
        codes = [h["stock_code"] for h in holdings]
        dps_map = await portfolio_repo.get_trailing_dividends(codes)
        code_set = set(codes)
        foreign_rows = {
            r["stock_code"]: r
            for r in await foreign_dividends_repo.list_foreign_dividends()
            if r["stock_code"] in code_set
        }

        # 통화별 실시간 환율 (중복 조회 방지). 1.0 = '모름' 센티널 → 폴백.
        fx_rates: dict[str, float | None] = {}
        for row in foreign_rows.values():
            cur = str(row.get("currency") or "KRW").upper()
            if cur != "KRW" and cur not in fx_rates:
                try:
                    rate = await fx.fx_rate_for_currency(cur)
                except Exception as exc:
                    logger.warning("dividend_calendar: fx lookup failed (%s): %s", cur, exc)
                    rate = None
                fx_rates[cur] = rate if rate and rate > 0 and rate != 1.0 else None

        for holding in holdings:
            code = holding["stock_code"]
            frow = foreign_rows.get(code)
            annual_dps_krw = dps_map.get(code)
            if frow is not None:
                cur = str(frow.get("currency") or "KRW").upper()
                native = frow.get("dps_native")
                rate = fx_rates.get(cur)
                if native and float(native) > 0 and cur != "KRW" and rate:
                    annual_dps_krw = float(native) * rate  # 실시간 환산 우선
            events.extend(
                estimated_events_for_holding(holding, annual_dps_krw, frow, months)
            )

        holdings_by_code = {h["stock_code"]: h for h in holdings}
        brief_events = await _latest_brief_upcoming_events(google_sub)
        events.extend(
            _confirmed_events(brief_events, holdings_by_code, start_iso, end_iso)
        )

    events.sort(key=lambda ev: (ev["date"], ev["stock_code"]))
    monthly = _monthly_aggregation(events, months)
    confirmed_count = sum(1 for ev in events if ev["confirmed"])
    return {
        "as_of": today.isoformat(),
        "months_back": months_back,
        "months_forward": months_forward,
        "start_month": _month_key(*months[0]),
        "end_month": _month_key(*months[-1]),
        "events": events,
        "monthly": monthly,
        "summary": {
            "event_count": len(events),
            "confirmed_count": confirmed_count,
            "estimated_count": len(events) - confirmed_count,
            "total_expected_krw": round(sum(m["total_krw"] for m in monthly)),
        },
    }
