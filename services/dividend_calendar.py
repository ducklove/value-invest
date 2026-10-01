"""공시와 종목별 배당 이력으로 구성하는 지급·권리일 캘린더.

지난 배당은 기준 시점(배당락 전 거래일 종가, 국내 기준일은 T+2로 2거래일 전 종가)에 보유한 종목만, 그때 수량으로
넣는다(domain.dividend_entitlement). 보유 근거는 일별 정규장 정산(portfolio_stock_snapshots)이다. 미래·예상 일정은
현재 보유 수량이다. 지금은 없지만 기준 시점에 보유했던 종목(매도)도 지난 권리일·매도 뒤 지급 행으로 넣는다.
"""

from __future__ import annotations

import asyncio
import json
from datetime import date, timedelta

from domain.dividend_entitlement import GAP_MAX_DAYS, HoldingHistory, entitlement, holding_identity, reference_point
from domain.dividend_schedule import FREQUENCY_LABELS, calendar_event, event_day, frequency_of, project_events
from domain.dividend_verification import (
    NH_CONFIRMED,
    NH_PARTIAL,
    UNCONFIRMED,
    attach_adjustments,
    link_calendar,
    nh_payment_event,
)
from repositories import broker_activity
from repositories import db as db_repo
from repositories import dividend_receipts as dividend_receipts_repo
from repositories import foreign_dividends as foreign_dividends_repo
from repositories import portfolio as portfolio_repo
from repositories import snapshots as snapshots_repo
from services import dividend_sources
from services.portfolio import fx
from services.portfolio.identifiers import is_special_asset, normalize_portfolio_code
from services.portfolio.time_windows import today_kst_date

# 조회 시작보다 이 일수 앞부터 정산에 보유로 남은 종목은 지금 없어도 넣는다(기준 시점 보유 뒤 매도한 종목의 지급 행).
# 국내 결산배당(12월 기준일 → 4월 지급)이 조회 시작 직후 지급되는 경우를 덮는다.
SOLD_LOOKBACK_DAYS = 130
_POINT_FIELDS = ("holding_basis", "holding_as_of", "quantity_as_of", "holding_gap_filled", "reference_date",
                 "reference_rule", "reference_approximate")


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
    """월별 세전 합계. 지급일이 있는 행(cashflow)만 더한다.

    - 일정 행: 주당 금액 × 기준 시점 보유 수량(미래·예상은 현재 수량). NH 입금이 연결돼도 이 예상 금액을 그대로 쓰고
      실제 NH 금액은 더하지 않는다(연결된 배당락·기준일 행은 원래대로 합계 밖).
    - NH 입금 행(date_status 'nh', 어느 일정에도 연결되지 않은 NH 배당): 실제 세전 원화(NH 환율 검산분)를 더한다.
      nh_only_krw·nh_only_count로 따로 보여 준다. 한 NH 입금은 한 행에만 쓰이므로 이중 집계가 없다.
    """
    rows = []
    for year, month in months:
        key = _month_key(year, month)
        selected = [e for e in events if e["date"].startswith(key)]
        payments = [e for e in selected if e["cashflow"]]
        nh_rows = [e for e in payments if e["date_status"] == "nh"]
        rows.append({"month": key, "count": len(selected),
                     "total_krw": round(sum(e["expected_amount_krw"] or 0 for e in payments)),
                     "announced_krw": round(sum(e["expected_amount_krw"] or 0 for e in payments if e["confirmed"])),
                     "estimated_krw": round(sum(e["expected_amount_krw"] or 0 for e in payments if e["date_status"] == "estimated")),
                     "nh_only_krw": round(sum(e["expected_amount_krw"] or 0 for e in nh_rows)),
                     "nh_only_count": len(nh_rows),
                     "nh_count": sum(bool(e.get("nh_match")) for e in selected),
                     "unconverted_count": sum(e["expected_amount_krw"] is None for e in payments)})
    return rows


async def _rate(currency: str) -> float | None:
    try:
        return await fx.fx_rate_for_currency(currency)
    except fx.FXUnavailableError:
        return None


async def _sold_holdings(google_sub: str, since: date, holdings: list[dict], records: list[dict]) -> list[dict]:
    """since 이후 정산에 보유로 남았지만 지금은 없는 종목. 같은 종목의 표기 차이(GOOGL.O·GOOGL)는 하나로 묶는다.

    코드는 같은 종목의 NH 배당 기록 코드(입금 연결용)를, 없으면 마지막 정산 코드를 쓴다. 이름은 정산 당시 종목명,
    NH 기록 종목명, 코드 순이다.
    """
    held = {holding_identity(h["stock_code"]) for h in holdings}
    groups: dict[str, list[dict]] = {}
    for row in await snapshots_repo.get_held_codes_since(google_sub, since.isoformat()):
        code = normalize_portfolio_code(row.get("stock_code"))
        if not code or is_special_asset(code) or holding_identity(code) in held:
            continue
        groups.setdefault(holding_identity(code), []).append({**row, "code": code})
    if not groups:
        return []
    names = await snapshots_repo.get_snapshot_stock_names(google_sub, [r["stock_code"] for rows in groups.values() for r in rows])
    nh_codes: dict[str, tuple[str, str | None]] = {}
    for record in records:
        code = normalize_portfolio_code(record.get("stock_code"))
        if code:
            nh_codes.setdefault(holding_identity(code), (code, record.get("stock_name")))
    out = []
    for identity, rows in groups.items():
        rows.sort(key=lambda r: (r["last_date"], r["code"]), reverse=True)
        nh = nh_codes.get(identity)
        code = nh[0] if nh else rows[0]["code"]
        name = next((names[r["stock_code"]] for r in rows if names.get(r["stock_code"])), None) or (nh[1] if nh else None)
        out.append({"stock_code": code, "stock_name": name or code, "quantity": None, "held_now": False,
                    "last_held_date": rows[0]["last_date"]})
    return sorted(out, key=lambda h: h["stock_code"])


async def build_calendar(google_sub: str, months_back: int = 2, months_forward: int = 10, *, today: date | None = None) -> dict:
    today = today or today_kst_date()
    months = window_months(today, months_back, months_forward)
    start = date(*months[0], 1)
    end = date(*_shift_month(*months[-1], 1), 1)
    holdings = [{**h, "stock_code": normalize_portfolio_code(h.get("stock_code")), "held_now": True}
                for h in await portfolio_repo.get_portfolio(google_sub)
                if not is_special_asset(h.get("stock_code")) and float(h.get("quantity") or 0) > 0]
    held_codes = [h["stock_code"] for h in holdings]
    records, orphans = attach_adjustments(await broker_activity.dividend_records(google_sub),
                                          await broker_activity.dividend_adjustments(google_sub))
    sold = await _sold_holdings(google_sub, start - timedelta(days=SOLD_LOOKBACK_DAYS), holdings, records)
    entries = holdings + sold
    codes = [h["stock_code"] for h in entries]
    histories = await dividend_sources.get_histories(codes) if codes else {}
    # 기존 브리프의 배당기준일을 배당락일로 해석하지 않는다(보유 중인 종목만).
    for row in await _latest_brief_upcoming_events(google_sub) if held_codes else []:
        code = normalize_portfolio_code(row.get("stock_code")) if isinstance(row, dict) else ""
        day = dividend_sources.parse_day(row.get("date")) if code else None
        if code not in held_codes or not day or row.get("type") != "배당기준일":
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
    candidates, coverage = [], {}
    for holding in entries:
        code = holding["stock_code"]
        feed = histories.get(code, {"events": [], "status": "unavailable"})
        raw = feed["events"]
        frequency = frequency_of(raw, today, feed.get("frequency_hint"))
        # 예상 일정은 지금 보유한 종목만 만든다(매도한 종목의 미래는 예측하지 않는다).
        projected = project_events(raw, today, end, frequency) if feed.get("status") == "fresh" and holding["held_now"] else []
        coverage[code] = {"stock_code": code, "stock_name": holding.get("stock_name") or code, "held": holding["held_now"],
                          "frequency": frequency, "frequency_label": FREQUENCY_LABELS[frequency],
                          "status": feed.get("status"), "fetched_at": feed.get("fetched_at"),
                          "has_payment_dates": any(e.get("pay_date") for e in raw), "event_count": len(raw)}
        for item in [*raw, *projected]:
            if start.isoformat() <= event_day(item) < end.isoformat():
                candidates.append((holding, item, frequency, feed))
    # 기준 시점이 지난 일정의 가장 이른 시점보다 며칠 앞 정산부터 읽는다(그 시점의 기록 누락을 앞뒤 정산으로 판단).
    past = [ref["date"] for ref in (reference_point(item, h["stock_code"]) for h, item, *_ in candidates if not item.get("estimated"))
            if ref and ref["date"] < today]
    since = (min(past) - timedelta(days=GAP_MAX_DAYS)).isoformat() if past else None
    history = HoldingHistory(await snapshots_repo.get_stock_holdings_from(google_sub, since) if since else [])
    events, excluded = [], []
    for holding, item, frequency, feed in candidates:
        code = holding["stock_code"]
        point = entitlement(code, item, today, history, float(holding["quantity"]) if holding["held_now"] else None)
        rate = rates.get(item["currency"])
        stored = foreign_rows.get(code, {})
        rate_source = "current"
        if rate is None and stored.get("currency") == item["currency"] and (stored.get("dps_native") or 0) > 0 and (stored.get("dps_krw") or 0) > 0:
            rate = stored["dps_krw"] / stored["dps_native"]
            rate_source = "stored"
        basis = {**holding, "quantity": point["quantity"], **{key: point[key] for key in _POINT_FIELDS}}
        event = {**calendar_event(basis, item, rate, frequency, feed), "fx_source": rate_source if rate else "unavailable"}
        if point["held"]:
            events.append(event)
        else:
            excluded.append({**event, "excluded_reason": point["excluded_reason"]})
    with_events = {e["stock_code"] for e in events}
    coverage_rows = [c for c in coverage.values() if c["held"] or c["stock_code"] in with_events]
    # NH 배당 입금을 일정에 연결한다(지급일 ±, 배당락·기준일 이후 다음 권리일 전). 세금 정산은 해당 배당에 붙인다.
    # 기준 시점에 보유하지 않아 뺀 일정은 연결하지 않고 다음 권리일 경계로만 쓴다.
    if events or records:
        receipts = await dividend_receipts_repo.list_receipts(google_sub, limit=10000, verify=False) if events else []
        # 금액으로 판정할 수 없을 때만 쓴다: 현재 NH 연동 계좌에만 있는 종목은 NH 입금으로 전체 확인된다.
        nh_only = await broker_activity.nh_only_codes(google_sub) if events else set()
        events, unlinked = link_calendar(events, records, receipts, today, nh_only, boundaries=excluded)
        # 연결할 일정이 없는 NH 입금(미보유·이력 없음·창 밖 권리일)은 실제 입금 행으로 보여 준다.
        events += [nh_payment_event(records[j]) for j in unlinked
                   if start.isoformat() <= str(records[j].get("date") or "") < end.isoformat()]
    events.sort(key=lambda event: (event["date"], event["stock_code"]))
    monthly = _monthly_aggregation(events, months)
    return {"as_of": today.isoformat(), "months_back": months_back, "months_forward": months_forward,
            "start_month": _month_key(*months[0]), "end_month": _month_key(*months[-1]),
            "events": events, "monthly": monthly, "coverage": coverage_rows,
            "summary": {"event_count": len(events), "confirmed_count": sum(e["confirmed"] for e in events),
                        "estimated_count": sum(e["date_status"] == "estimated" for e in events),
                        "observed_count": sum(e["date_status"] == "observed" for e in events),
                        "total_expected_krw": sum(m["total_krw"] for m in monthly),
                        "unknown_payment_count": sum(not c["has_payment_dates"] for c in coverage_rows),
                        "stale_count": sum(c["status"] != "fresh" for c in coverage_rows),
                        "unconfirmed_count": sum(e.get("verification") == UNCONFIRMED for e in events),
                        "nh_confirmed_count": sum(e.get("verification") == NH_CONFIRMED for e in events),
                        "nh_partial_count": sum(e.get("verification") == NH_PARTIAL for e in events),
                        # NH 입금이 연결된 행(일정 + NH 입금 행)과 그중 NH 입금 행.
                        "nh_count": sum(bool(e.get("nh_match")) for e in events),
                        "nh_only_count": sum(e["date_status"] == "nh" for e in events),
                        "nh_only_krw": sum(m["nh_only_krw"] for m in monthly),
                        "nh_unattached_adjustment_count": len(orphans),
                        # 기준 시점에 보유하지 않아 뺀 일정, 지금은 없지만 기준 시점에 보유한 종목(매도) 수.
                        "not_held_count": len(excluded),
                        "sold_stock_count": sum(not c["held"] for c in coverage_rows)}}
