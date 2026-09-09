"""공시와 종목별 배당 이력으로 구성하는 지급·권리일 캘린더."""

from __future__ import annotations

import asyncio
import json
from datetime import date

from domain.dividend_schedule import FREQUENCY_LABELS, calendar_event, event_day, frequency_of, project_events
from repositories import db as db_repo
from repositories import foreign_dividends as foreign_dividends_repo
from repositories import portfolio as portfolio_repo
from services import dividend_sources
from services.portfolio import fx
from services.portfolio.identifiers import is_special_asset, normalize_portfolio_code
from services.portfolio.time_windows import today_kst_date


def _shift_month(year: int, month: int, offset: int) -> tuple[int, int]:
    year, month = divmod(year * 12 + month - 1 + offset, 12)
    return year, month + 1


def _month_key(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def window_months(today: date, months_back: int, months_forward: int) -> list[tuple[int, int]]:
    return [_shift_month(today.year, today.month, offset) for offset in range(-months_back, months_forward + 1)]


async def _latest_brief_upcoming_events(google_sub: str) -> list[dict]:
    db = await db_repo.get_db()
    row = await (await db.execute(
        "SELECT payload_json FROM daily_market_briefs WHERE google_sub=? ORDER BY brief_date DESC LIMIT 1", (google_sub,),
    )).fetchone()
    if not row:
        return []
    try:
        payload = json.loads(row["payload_json"] or "{}")
    except (TypeError, json.JSONDecodeError):
        return []
    events = payload.get("upcoming_events") if isinstance(payload, dict) else None
    return events if isinstance(events, list) else []


def _monthly_aggregation(events: list[dict], months: list[tuple[int, int]]) -> list[dict]:
    rows = []
    for year, month in months:
        key = _month_key(year, month)
        selected = [e for e in events if e["date"].startswith(key)]
        payments = [e for e in selected if e["cashflow"]]
        rows.append({"month": key, "count": len(selected),
                     "total_krw": round(sum(e["expected_amount_krw"] or 0 for e in payments)),
                     "announced_krw": round(sum(e["expected_amount_krw"] or 0 for e in payments if e["confirmed"])),
                     "estimated_krw": round(sum(e["expected_amount_krw"] or 0 for e in payments if e["date_status"] == "estimated")),
                     "unconverted_count": sum(e["expected_amount_krw"] is None for e in payments)})
    return rows


async def _rate(currency: str) -> float | None:
    try:
        return await fx.fx_rate_for_currency(currency)
    except fx.FXUnavailableError:
        return None


async def build_calendar(google_sub: str, months_back: int = 2, months_forward: int = 10, *, today: date | None = None) -> dict:
    today = today or today_kst_date()
    months = window_months(today, months_back, months_forward)
    start = date(*months[0], 1)
    end = date(*_shift_month(*months[-1], 1), 1)
    holdings = [{**h, "stock_code": normalize_portfolio_code(h.get("stock_code"))}
                for h in await portfolio_repo.get_portfolio(google_sub)
                if not is_special_asset(h.get("stock_code")) and float(h.get("quantity") or 0) > 0]
    codes = [h["stock_code"] for h in holdings]
    histories = await dividend_sources.get_histories(codes) if codes else {}
    # 기존 브리프의 배당기준일을 배당락일로 해석하지 않는다.
    for row in await _latest_brief_upcoming_events(google_sub) if codes else []:
        code = normalize_portfolio_code(row.get("stock_code")) if isinstance(row, dict) else ""
        day = dividend_sources.parse_day(row.get("date")) if code else None
        if code not in codes or not day or row.get("type") != "배당기준일":
            continue
        feed = histories.setdefault(code, {"events": [], "status": "unavailable"})
        if any(day in (e.get("ex_date"), e.get("record_date")) for e in feed["events"]):
            continue
        feed["events"].append({"record_date": day, "pay_date": None, "ex_date": None,
                               "amount_per_share": dividend_sources.number(row.get("amount")), "currency": "KRW",
                               "source": "기존 브리프 기준일", "source_url": None, "official": False})
    currencies = sorted({e["currency"] for feed in histories.values() for e in feed["events"]})
    rates = dict(zip(currencies, await asyncio.gather(*(_rate(c) for c in currencies))))
    foreign_rows = {r["stock_code"]: r for r in await foreign_dividends_repo.list_foreign_dividends()} if codes else {}
    events, coverage = [], []
    for holding in holdings:
        code = holding["stock_code"]
        feed = histories.get(code, {"events": [], "status": "unavailable"})
        raw = feed["events"]
        frequency = frequency_of(raw, today, feed.get("frequency_hint"))
        projected = project_events(raw, today, end, frequency) if feed.get("status") == "fresh" else []
        coverage.append({"stock_code": code, "stock_name": holding.get("stock_name") or code,
                         "frequency": frequency, "frequency_label": FREQUENCY_LABELS[frequency],
                         "status": feed.get("status"), "fetched_at": feed.get("fetched_at"),
                         "has_payment_dates": any(e.get("pay_date") for e in raw), "event_count": len(raw)})
        for item in [*raw, *projected]:
            if not start.isoformat() <= event_day(item) < end.isoformat():
                continue
            rate = rates.get(item["currency"])
            stored = foreign_rows.get(code, {})
            rate_source = "current"
            if rate is None and stored.get("currency") == item["currency"] and (stored.get("dps_native") or 0) > 0 and (stored.get("dps_krw") or 0) > 0:
                rate = stored["dps_krw"] / stored["dps_native"]
                rate_source = "stored"
            events.append({**calendar_event(holding, item, rate, frequency, feed), "fx_source": rate_source if rate else "unavailable"})
    events.sort(key=lambda event: (event["date"], event["stock_code"]))
    monthly = _monthly_aggregation(events, months)
    return {"as_of": today.isoformat(), "months_back": months_back, "months_forward": months_forward,
            "start_month": _month_key(*months[0]), "end_month": _month_key(*months[-1]),
            "events": events, "monthly": monthly, "coverage": coverage,
            "summary": {"event_count": len(events), "confirmed_count": sum(e["confirmed"] for e in events),
                        "estimated_count": sum(e["date_status"] == "estimated" for e in events),
                        "observed_count": sum(e["date_status"] == "observed" for e in events),
                        "total_expected_krw": sum(m["total_krw"] for m in monthly),
                        "unknown_payment_count": sum(not c["has_payment_dates"] for c in coverage),
                        "stale_count": sum(c["status"] != "fresh" for c in coverage)}}
