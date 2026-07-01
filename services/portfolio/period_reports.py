"""Portfolio monthly/annual period report builder.

The first-class artifact is a stable JSON data snapshot. The UI can render it
directly, and richer AI/commentary layers can reuse the same schema later
without changing historical records.
"""

from __future__ import annotations

import calendar
import hashlib
import json
import math
import re
from datetime import date, datetime
from typing import Any

from repositories import portfolio_reports as reports_repo
from repositories import snapshots as snapshots_repo
from services.portfolio import foreign, identifiers, risk
from services.portfolio.time_windows import today_kst_date

SCHEMA_VERSION = 2
VALID_PERIOD_TYPES = {"monthly", "annual"}
MONTHLY_KEY_RE = re.compile(r"^\d{4}-\d{2}$")
ANNUAL_KEY_RE = re.compile(r"^\d{4}$")


class PeriodReportError(ValueError):
    """User-facing period report validation/generation failure."""


def _round(value: float | None, digits: int = 4) -> float | None:
    if value is None:
        return None
    try:
        if not math.isfinite(float(value)):
            return None
    except (TypeError, ValueError):
        return None
    return round(float(value), digits)


def _pct_change(start: float | None, end: float | None) -> float | None:
    try:
        s = float(start)
        e = float(end)
    except (TypeError, ValueError):
        return None
    if s <= 0:
        return None
    return _round((e / s - 1.0) * 100.0)


def _float_or_none(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out):
        return None
    return out


def _iso_now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _fmt_krw(value: float | None) -> str:
    if value is None:
        return "-"
    av = abs(value)
    sign = "-" if value < 0 else ""
    if av >= 1e8:
        return f"{sign}{av / 1e8:,.2f}억"
    if av >= 1e4:
        return f"{sign}{av / 1e4:,.0f}만"
    return f"{value:,.0f}원"


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "-"
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.2f}%"


def _activity_label(activity: str | None) -> str:
    labels = {
        "new_position": "신규 매수",
        "closed_position": "전량 매도",
        "increased_position": "추가 매수",
        "reduced_position": "부분 매도",
        "futures_short": "선물 매도",
        "unchanged_position": "수량 유지",
        "value_only_increase": "평가액 증가",
        "value_only_decrease": "평가액 감소",
    }
    return labels.get(str(activity or ""), str(activity or "-"))


def normalize_period_type(period_type: str) -> str:
    pt = str(period_type or "").strip().lower()
    aliases = {
        "month": "monthly",
        "monthly": "monthly",
        "m": "monthly",
        "year": "annual",
        "yearly": "annual",
        "annual": "annual",
        "y": "annual",
    }
    pt = aliases.get(pt, pt)
    if pt not in VALID_PERIOD_TYPES:
        raise PeriodReportError("period_type은 monthly 또는 annual이어야 합니다.")
    return pt


def period_bounds(
    period_type: str,
    period_key: str,
    *,
    today: date | None = None,
) -> dict[str, Any]:
    pt = normalize_period_type(period_type)
    key = str(period_key or "").strip()
    anchor = today or today_kst_date()
    if pt == "monthly":
        if not MONTHLY_KEY_RE.match(key):
            raise PeriodReportError("monthly period_key 형식은 YYYY-MM이어야 합니다.")
        year, month = [int(part) for part in key.split("-")]
        start = date(year, month, 1)
        last_day = calendar.monthrange(year, month)[1]
        nominal_end = date(year, month, last_day)
    else:
        if not ANNUAL_KEY_RE.match(key):
            raise PeriodReportError("annual period_key 형식은 YYYY이어야 합니다.")
        year = int(key)
        start = date(year, 1, 1)
        nominal_end = date(year, 12, 31)

    if start > anchor:
        raise PeriodReportError("미래 기간 보고서는 생성할 수 없습니다.")
    end = min(nominal_end, anchor)
    return {
        "type": pt,
        "key": key,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "nominal_end_date": nominal_end.isoformat(),
        "is_complete": nominal_end < anchor,
    }


def _default_month_key(today: date) -> str:
    first = today.replace(day=1)
    prev_last = first.toordinal() - 1
    prev = date.fromordinal(prev_last)
    return f"{prev.year:04d}-{prev.month:02d}"


def _period_keys_from_nav(nav_history: list[dict], *, today: date | None = None) -> dict[str, Any]:
    anchor = today or today_kst_date()
    months: dict[str, dict] = {}
    years: dict[str, dict] = {}
    for row in nav_history:
        raw = str(row.get("date") or "")[:10]
        if len(raw) < 10:
            continue
        try:
            d = date.fromisoformat(raw)
        except ValueError:
            continue
        if d > anchor:
            continue
        mkey = f"{d.year:04d}-{d.month:02d}"
        ykey = f"{d.year:04d}"
        months.setdefault(mkey, period_bounds("monthly", mkey, today=anchor))
        years.setdefault(ykey, period_bounds("annual", ykey, today=anchor))
    default_month = _default_month_key(anchor)
    if default_month not in months and nav_history:
        try:
            months[default_month] = period_bounds("monthly", default_month, today=anchor)
        except PeriodReportError:
            pass
    current_year = f"{anchor.year:04d}"
    if current_year not in years and nav_history:
        years[current_year] = period_bounds("annual", current_year, today=anchor)
    return {
        "monthly": sorted(months.values(), key=lambda p: p["key"], reverse=True),
        "annual": sorted(years.values(), key=lambda p: p["key"], reverse=True),
        "defaults": {"monthly": default_month, "annual": current_year},
    }


async def available_periods(google_sub: str) -> dict[str, Any]:
    nav_history = await snapshots_repo.get_nav_history(google_sub)
    saved = await reports_repo.list_period_reports(google_sub, limit=36)
    periods = _period_keys_from_nav(nav_history)
    periods["saved"] = [_saved_meta(row) for row in saved]
    return periods


def _select_snapshots(
    nav_history: list[dict],
    start_date: str,
    end_date: str,
) -> tuple[dict | None, dict | None, list[dict], list[str], str]:
    warnings: list[str] = []
    rows = sorted(
        [r for r in nav_history if r.get("date")],
        key=lambda r: str(r.get("date")),
    )
    before_start = [r for r in rows if str(r["date"]) < start_date]
    in_or_before_end = [r for r in rows if str(r["date"]) <= end_date]
    in_period = [r for r in rows if start_date <= str(r["date"]) <= end_date]

    baseline_mode = "previous_close"
    baseline = before_start[-1] if before_start else None
    if baseline is None and in_period:
        baseline = in_period[0]
        baseline_mode = "first_snapshot_in_period"
        warnings.append("기간 시작 전 기준 스냅샷이 없어 기간 내 첫 스냅샷을 기준으로 사용했습니다.")
    end_snapshot = in_or_before_end[-1] if in_or_before_end else None
    if end_snapshot is None:
        warnings.append("기간 종료일까지의 스냅샷이 없습니다.")

    if baseline and end_snapshot:
        points = [
            r for r in rows
            if str(baseline["date"]) <= str(r["date"]) <= str(end_snapshot["date"])
        ]
    else:
        points = []
    if len(points) < 2:
        warnings.append("기간 수익률/리스크 계산에 필요한 스냅샷이 부족합니다.")
    return baseline, end_snapshot, points, warnings, baseline_mode


def _cashflow_summary(cashflows: list[dict], start_date: str, end_date: str) -> dict[str, Any]:
    rows = [
        row for row in cashflows
        if start_date <= str(row.get("date") or "") <= end_date
    ]
    deposits = [float(r.get("amount") or 0) for r in rows if r.get("type") == "deposit"]
    withdrawals = [float(r.get("amount") or 0) for r in rows if r.get("type") == "withdrawal"]
    total_deposit = sum(deposits)
    total_withdrawal = sum(withdrawals)
    return {
        "count": len(rows),
        "deposit_count": len(deposits),
        "withdrawal_count": len(withdrawals),
        "total_deposit": _round(total_deposit, 2),
        "total_withdrawal": _round(total_withdrawal, 2),
        "net_cashflow": _round(total_deposit - total_withdrawal, 2),
        "rows": [
            {
                "id": row.get("id"),
                "date": row.get("date"),
                "type": row.get("type"),
                "amount": _round(row.get("amount"), 2),
                "memo": row.get("memo") or "",
            }
            for row in sorted(rows, key=lambda r: (str(r.get("date") or ""), int(r.get("id") or 0)))
        ],
    }


def _daily_metrics(points: list[dict]) -> dict[str, Any]:
    clean = risk.clean_series(points, "nav")
    rets = risk.simple_returns(clean)
    ret_values = [r for _, r in rets]
    best = max(rets, key=lambda r: r[1]) if rets else None
    worst = min(rets, key=lambda r: r[1]) if rets else None
    drawdown = risk.max_drawdown(clean)
    return {
        "points": len(clean),
        "return_observations": len(rets),
        "best_day": {"date": best[0], "return_pct": _round(best[1] * 100.0)} if best else None,
        "worst_day": {"date": worst[0], "return_pct": _round(worst[1] * 100.0)} if worst else None,
        "annualized_volatility_pct": _round(risk.annualized_volatility_pct(ret_values)),
        "max_drawdown_pct": _round(drawdown["max_drawdown_pct"]) if drawdown else None,
        "max_drawdown_peak_date": drawdown["peak_date"] if drawdown else None,
        "max_drawdown_trough_date": drawdown["trough_date"] if drawdown else None,
    }


def _rows_by_code(rows: list[dict]) -> dict[str, dict]:
    return {str(r.get("stock_code") or ""): r for r in rows if r.get("stock_code")}


def _row_quantity(row: dict | None) -> float | None:
    if not row:
        return None
    return _float_or_none(row.get("quantity"))


def _row_unit_price(row: dict | None) -> float | None:
    if not row:
        return None
    direct = _float_or_none(row.get("unit_price"))
    if direct is not None and direct > 0:
        return direct
    qty = _row_quantity(row)
    value = _float_or_none(row.get("market_value"))
    if qty is None or abs(qty) <= 1e-12 or value is None:
        return None
    return value / qty


def _trade_unit_price(start_price: float | None, end_price: float | None) -> float | None:
    prices = [p for p in (start_price, end_price) if p is not None and p > 0]
    if len(prices) == 2:
        return sum(prices) / 2.0
    if prices:
        return prices[0]
    return None


def _position_value_for_weight(row: dict | None) -> float:
    if not row:
        return 0.0
    value = _float_or_none(row.get("market_value")) or 0.0
    qty = _row_quantity(row)
    if qty is not None and qty < 0:
        if value < 0:
            return value
        unit_price = _row_unit_price(row)
        if unit_price is None or unit_price <= 0:
            unit_price = _float_or_none(row.get("avg_price_krw"))
        if unit_price is not None and unit_price > 0:
            return qty * unit_price
        if value > 0:
            return -abs(value)
    return value


def _portfolio_total_for_weight(rows: list[dict]) -> float:
    total = sum(float(r.get("market_value") or 0) for r in rows)
    if total > 0:
        return total
    return sum(abs(_position_value_for_weight(r)) for r in rows)


def _weight_pct(position_value: float, portfolio_total: float) -> float:
    if portfolio_total <= 0 or abs(position_value) <= 1e-12:
        return 0.0
    return _round(position_value / portfolio_total * 100.0) or 0.0


def _needs_snapshot_name_resolution(row: dict) -> bool:
    code = str(row.get("stock_code") or "").strip()
    name = str(row.get("stock_name") or "").strip()
    if not code or (name and name != code):
        return False
    return (
        identifiers.is_korean_stock(code)
        or identifiers.is_cash_asset(code)
        or identifiers.is_special_asset(code)
        or identifiers.static_foreign_ticker(code) is not None
    )


async def _enrich_snapshot_names(rows: list[dict]) -> list[dict]:
    codes = sorted({str(row.get("stock_code") or "").strip() for row in rows if _needs_snapshot_name_resolution(row)})
    if not codes:
        return rows
    resolved: dict[str, str] = {}
    for code in codes:
        try:
            name = await foreign.resolve_name(code)
        except Exception:
            name = None
        clean = str(name or "").strip()
        if clean and clean != code:
            resolved[code] = clean
    if not resolved:
        return rows
    for row in rows:
        code = str(row.get("stock_code") or "").strip()
        if code in resolved and _needs_snapshot_name_resolution(row):
            row["stock_name"] = resolved[code]
    return rows


def _concentration(rows: list[dict]) -> dict[str, Any]:
    values = sorted([float(r.get("market_value") or 0) for r in rows if float(r.get("market_value") or 0) > 0], reverse=True)
    total = sum(values)
    if total <= 0:
        return {"top1_weight_pct": None, "top3_weight_pct": None, "top5_weight_pct": None, "hhi": None}
    weights = [v / total * 100.0 for v in values]
    hhi = sum((w / 100.0) ** 2 for w in weights)
    return {
        "top1_weight_pct": _round(sum(weights[:1])),
        "top3_weight_pct": _round(sum(weights[:3])),
        "top5_weight_pct": _round(sum(weights[:5])),
        "hhi": _round(hhi, 6),
    }


def _holding_changes(start_rows: list[dict], end_rows: list[dict]) -> dict[str, Any]:
    start_by = _rows_by_code(start_rows)
    end_by = _rows_by_code(end_rows)
    total_end = _portfolio_total_for_weight(end_rows)
    changes = []
    counts = {"added": 0, "removed": 0, "increased": 0, "decreased": 0, "unchanged": 0}
    for code in sorted(set(start_by) | set(end_by)):
        s = start_by.get(code, {})
        e = end_by.get(code, {})
        sv = float(s.get("market_value") or 0)
        ev = float(e.get("market_value") or 0)
        delta = ev - sv
        if sv <= 0 and ev > 0:
            status = "added"
        elif sv > 0 and ev <= 0:
            status = "removed"
        elif abs(delta) < 1:
            status = "unchanged"
        elif delta > 0:
            status = "increased"
        else:
            status = "decreased"
        counts[status] += 1
        changes.append({
            "stock_code": code,
            "stock_name": e.get("stock_name") or s.get("stock_name") or code,
            "group_name": e.get("group_name") or s.get("group_name") or "기타",
            "status": status,
            "start_value": _round(sv, 2),
            "end_value": _round(ev, 2),
            "change_value": _round(delta, 2),
            "change_pct": _pct_change(sv, ev),
            "end_weight_pct": _weight_pct(_position_value_for_weight(e), total_end),
        })
    return {
        "counts": counts,
        "top_increases": sorted(changes, key=lambda r: r["change_value"], reverse=True)[:8],
        "top_decreases": sorted(changes, key=lambda r: r["change_value"])[:8],
        "all": sorted(changes, key=lambda r: abs(r["change_value"]), reverse=True),
    }


def _composition_changes(start_rows: list[dict], end_rows: list[dict]) -> dict[str, Any]:
    start_by = _rows_by_code(start_rows)
    end_by = _rows_by_code(end_rows)
    total_start = _portfolio_total_for_weight(start_rows)
    total_end = _portfolio_total_for_weight(end_rows)
    avg_portfolio_value = max(1.0, (total_start + total_end) / 2.0)
    counts = {
        "new_positions": 0,
        "closed_positions": 0,
        "increased_positions": 0,
        "reduced_positions": 0,
        "futures_short_positions": 0,
        "unchanged_positions": 0,
        "value_only_changes": 0,
    }
    rows: list[dict[str, Any]] = []

    for code in sorted(set(start_by) | set(end_by)):
        s = start_by.get(code)
        e = end_by.get(code)
        sv = float((s or {}).get("market_value") or 0)
        ev = float((e or {}).get("market_value") or 0)
        signed_sv = _position_value_for_weight(s)
        signed_ev = _position_value_for_weight(e)
        value_delta = ev - sv
        sq_raw = _row_quantity(s)
        eq_raw = _row_quantity(e)
        has_quantity_basis = ((s is None) or sq_raw is not None) and ((e is None) or eq_raw is not None)
        sq = sq_raw if sq_raw is not None else (0.0 if has_quantity_basis else None)
        eq = eq_raw if eq_raw is not None else (0.0 if has_quantity_basis else None)
        qty_delta = (eq - sq) if sq is not None and eq is not None else None
        start_price = _row_unit_price(s)
        end_price = _row_unit_price(e)
        unit_for_trade = _trade_unit_price(start_price, end_price)
        trade_value_estimate: float | None = None
        price_effect_value: float | None = None
        confidence = "quantity_delta" if has_quantity_basis else "value_only"

        if has_quantity_basis and qty_delta is not None:
            if eq is not None and eq < 0 and qty_delta < -1e-9:
                activity = "futures_short"
                counts["futures_short_positions"] += 1
            elif sv <= 0 and ev > 0 and eq and eq > 0:
                activity = "new_position"
                counts["new_positions"] += 1
            elif sv > 0 and ev <= 0 and sq and sq > 0:
                activity = "closed_position"
                counts["closed_positions"] += 1
            elif abs(qty_delta) <= 1e-9:
                activity = "unchanged_position"
                counts["unchanged_positions"] += 1
            elif qty_delta > 0:
                activity = "increased_position"
                counts["increased_positions"] += 1
            else:
                activity = "reduced_position"
                counts["reduced_positions"] += 1
            if abs(qty_delta) > 1e-9 and unit_for_trade is not None:
                trade_value_estimate = qty_delta * unit_for_trade
                price_effect_value = value_delta - trade_value_estimate
        elif sv <= 0 and ev > 0:
            activity = "new_position"
            counts["new_positions"] += 1
            trade_value_estimate = ev
            price_effect_value = 0.0
            confidence = "position_boundary"
        elif sv > 0 and ev <= 0:
            activity = "closed_position"
            counts["closed_positions"] += 1
            trade_value_estimate = -sv
            price_effect_value = 0.0
            confidence = "position_boundary"
        elif abs(value_delta) < 1:
            activity = "unchanged_position"
            counts["unchanged_positions"] += 1
        else:
            activity = "value_only_increase" if value_delta > 0 else "value_only_decrease"
            counts["value_only_changes"] += 1

        group_name = (e or {}).get("group_name") or (s or {}).get("group_name") or "기타"
        row = {
            "stock_code": code,
            "stock_name": (e or {}).get("stock_name") or (s or {}).get("stock_name") or code,
            "group_name": group_name,
            "activity": activity,
            "confidence": confidence,
            "start_quantity": _round(sq, 6) if sq is not None else None,
            "end_quantity": _round(eq, 6) if eq is not None else None,
            "quantity_change": _round(qty_delta, 6) if qty_delta is not None else None,
            "start_unit_price": _round(start_price, 4),
            "end_unit_price": _round(end_price, 4),
            "trade_unit_price": _round(unit_for_trade, 4),
            "trade_value_estimate": _round(trade_value_estimate, 2),
            "price_effect_value": _round(price_effect_value, 2),
            "start_value": _round(sv, 2),
            "end_value": _round(ev, 2),
            "value_change": _round(value_delta, 2),
            "start_weight_pct": _weight_pct(signed_sv, total_start),
            "end_weight_pct": _weight_pct(signed_ev, total_end),
        }
        row["weight_change_ppt"] = _round((row["end_weight_pct"] or 0) - (row["start_weight_pct"] or 0))
        rows.append(row)

    gross_buy = sum(float(r.get("trade_value_estimate") or 0) for r in rows if float(r.get("trade_value_estimate") or 0) > 0)
    gross_sell = -sum(float(r.get("trade_value_estimate") or 0) for r in rows if float(r.get("trade_value_estimate") or 0) < 0)
    by_group: dict[str, dict[str, Any]] = {}
    for row in rows:
        group = row["group_name"] or "기타"
        item = by_group.setdefault(group, {
            "group_name": group,
            "gross_buy_value_estimate": 0.0,
            "gross_sell_value_estimate": 0.0,
            "net_trade_value_estimate": 0.0,
            "start_value": 0.0,
            "end_value": 0.0,
            "start_weight_value": 0.0,
            "end_weight_value": 0.0,
            "buy_count": 0,
            "sell_count": 0,
        })
        tv = float(row.get("trade_value_estimate") or 0)
        if tv > 0:
            item["gross_buy_value_estimate"] += tv
            item["buy_count"] += 1
        elif tv < 0:
            item["gross_sell_value_estimate"] += abs(tv)
            item["sell_count"] += 1
        item["net_trade_value_estimate"] += tv
        item["start_value"] += float(row.get("start_value") or 0)
        item["end_value"] += float(row.get("end_value") or 0)
        item["start_weight_value"] += _position_value_for_weight(start_by.get(row["stock_code"]))
        item["end_weight_value"] += _position_value_for_weight(end_by.get(row["stock_code"]))
    group_rows = []
    for item in by_group.values():
        item["value_change"] = item["end_value"] - item["start_value"]
        item["start_weight_pct"] = _weight_pct(item["start_weight_value"], total_start)
        item["end_weight_pct"] = _weight_pct(item["end_weight_value"], total_end)
        item["weight_change_ppt"] = item["end_weight_pct"] - item["start_weight_pct"]
        group_rows.append({
            "group_name": item["group_name"],
            "gross_buy_value_estimate": _round(item["gross_buy_value_estimate"], 2),
            "gross_sell_value_estimate": _round(item["gross_sell_value_estimate"], 2),
            "net_trade_value_estimate": _round(item["net_trade_value_estimate"], 2),
            "start_value": _round(item["start_value"], 2),
            "end_value": _round(item["end_value"], 2),
            "value_change": _round(item["value_change"], 2),
            "start_weight_pct": _round(item["start_weight_pct"]),
            "end_weight_pct": _round(item["end_weight_pct"]),
            "weight_change_ppt": _round(item["weight_change_ppt"]),
            "buy_count": item["buy_count"],
            "sell_count": item["sell_count"],
        })

    buy_rows = [r for r in rows if float(r.get("trade_value_estimate") or 0) > 0]
    sell_rows = [r for r in rows if float(r.get("trade_value_estimate") or 0) < 0]
    exact_rows = [r for r in rows if r.get("confidence") == "quantity_delta"]
    return {
        "basis": "quantity_delta_when_available_else_position_boundary",
        "summary": {
            **counts,
            "buy_like_count": counts["new_positions"] + counts["increased_positions"],
            "sell_like_count": counts["closed_positions"] + counts["reduced_positions"] + counts["futures_short_positions"],
            "gross_buy_value_estimate": _round(gross_buy, 2),
            "gross_sell_value_estimate": _round(gross_sell, 2),
            "net_trade_value_estimate": _round(gross_buy - gross_sell, 2),
            "estimated_turnover_pct": _round((gross_buy + gross_sell) / avg_portfolio_value * 100.0),
            "quantity_basis_count": len(exact_rows),
            "quantity_basis_ratio_pct": _round(len(exact_rows) / len(rows) * 100.0) if rows else None,
        },
        "top_buys": sorted(buy_rows, key=lambda r: float(r.get("trade_value_estimate") or 0), reverse=True)[:10],
        "top_sells": sorted(sell_rows, key=lambda r: float(r.get("trade_value_estimate") or 0))[:10],
        "by_group": sorted(group_rows, key=lambda r: abs(float(r.get("net_trade_value_estimate") or 0)), reverse=True),
        "all": sorted(rows, key=lambda r: abs(float(r.get("trade_value_estimate") or r.get("value_change") or 0)), reverse=True),
    }


def _group_changes(start_rows: list[dict], end_rows: list[dict]) -> dict[str, Any]:
    def aggregate(rows: list[dict]) -> dict[str, dict]:
        total = sum(float(r.get("market_value") or 0) for r in rows)
        out: dict[str, dict] = {}
        for row in rows:
            name = str(row.get("group_name") or "기타")
            item = out.setdefault(name, {"group_name": name, "market_value": 0.0, "stock_count": 0})
            item["market_value"] += float(row.get("market_value") or 0)
            item["stock_count"] += 1
        for item in out.values():
            item["weight_pct"] = item["market_value"] / total * 100.0 if total > 0 else None
        return out

    start_by = aggregate(start_rows)
    end_by = aggregate(end_rows)
    rows = []
    for name in sorted(set(start_by) | set(end_by)):
        s = start_by.get(name, {"market_value": 0.0, "weight_pct": None, "stock_count": 0})
        e = end_by.get(name, {"market_value": 0.0, "weight_pct": None, "stock_count": 0})
        rows.append({
            "group_name": name,
            "start_value": _round(s["market_value"], 2),
            "end_value": _round(e["market_value"], 2),
            "change_value": _round(e["market_value"] - s["market_value"], 2),
            "start_weight_pct": _round(s["weight_pct"]),
            "end_weight_pct": _round(e["weight_pct"]),
            "weight_change_ppt": _round(
                (e["weight_pct"] if e["weight_pct"] is not None else 0)
                - (s["weight_pct"] if s["weight_pct"] is not None else 0)
            ),
            "start_stock_count": s["stock_count"],
            "end_stock_count": e["stock_count"],
        })
    return {
        "groups": sorted(rows, key=lambda r: abs(r["weight_change_ppt"] or 0), reverse=True),
    }


def _top_holdings(rows: list[dict], limit: int = 10) -> list[dict]:
    total = sum(float(r.get("market_value") or 0) for r in rows)
    ranked = sorted(rows, key=lambda r: float(r.get("market_value") or 0), reverse=True)[:limit]
    return [
        {
            "stock_code": row.get("stock_code"),
            "stock_name": row.get("stock_name") or row.get("stock_code"),
            "group_name": row.get("group_name") or "기타",
            "market_value": _round(row.get("market_value"), 2),
            "weight_pct": _round(float(row.get("market_value") or 0) / total * 100.0) if total > 0 else None,
        }
        for row in ranked
    ]


def _data_quality(
    *,
    warnings: list[str],
    baseline: dict | None,
    end_snapshot: dict | None,
    start_rows: list[dict],
    end_rows: list[dict],
    period: dict[str, Any],
    baseline_mode: str,
) -> dict[str, Any]:
    stock_start_date = start_rows[0]["date"] if start_rows else None
    stock_end_date = end_rows[0]["date"] if end_rows else None
    notes = list(dict.fromkeys(warnings))
    if baseline and stock_start_date and stock_start_date != baseline.get("date"):
        notes.append("포트폴리오 기준 스냅샷과 종목별 시작 스냅샷 날짜가 다릅니다.")
    if end_snapshot and stock_end_date and stock_end_date != end_snapshot.get("date"):
        notes.append("포트폴리오 종료 스냅샷과 종목별 종료 스냅샷 날짜가 다릅니다.")
    if not start_rows:
        notes.append("시작 종목별 스냅샷이 없어 종목 변화가 제한적으로 표시됩니다.")
    if not end_rows:
        notes.append("종료 종목별 스냅샷이 없어 종목 변화가 제한적으로 표시됩니다.")
    stock_rows = [*start_rows, *end_rows]
    quantity_rows = [row for row in stock_rows if row.get("quantity") is not None]
    if stock_rows and not quantity_rows:
        notes.append("종목별 스냅샷에 수량 정보가 없어 매수/매도 구성 변화는 신규·제거와 평가액 변화 중심으로 제한 표시됩니다.")
    elif stock_rows and len(quantity_rows) < len(stock_rows):
        notes.append("일부 종목별 스냅샷에 수량 정보가 없어 매수/매도 구성 변화의 일부는 추정치입니다.")
    return {
        "status": "warning" if notes else "ok",
        "warnings": notes,
        "baseline_mode": baseline_mode,
        "period_complete": bool(period.get("is_complete")),
        "portfolio_baseline_date": baseline.get("date") if baseline else None,
        "portfolio_end_date": end_snapshot.get("date") if end_snapshot else None,
        "stock_baseline_date": stock_start_date,
        "stock_end_date": stock_end_date,
    }


def _source_hash(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _saved_meta(row: dict[str, Any]) -> dict[str, Any]:
    report = row.get("report") or {}
    summary = report.get("summary") or {}
    quality = report.get("data_quality") or {}
    return {
        "period_type": row.get("period_type"),
        "period_key": row.get("period_key"),
        "start_date": row.get("start_date"),
        "end_date": row.get("end_date"),
        "baseline_date": row.get("baseline_date"),
        "generated_at": row.get("generated_at"),
        "updated_at": row.get("updated_at"),
        "source_hash": row.get("source_hash"),
        "nav_return_pct": summary.get("nav_return_pct"),
        "ending_value": summary.get("ending_value"),
        "data_quality_status": quality.get("status"),
    }


def _build_notes(report: dict[str, Any]) -> list[dict[str, str]]:
    summary = report.get("summary") or {}
    holdings = report.get("holdings") or {}
    composition = report.get("composition_changes") or {}
    groups = report.get("allocation") or {}
    quality = report.get("data_quality") or {}
    notes: list[dict[str, str]] = []
    nav_ret = summary.get("nav_return_pct")
    if nav_ret is not None:
        direction = "상승" if nav_ret >= 0 else "하락"
        notes.append({
            "category": "performance",
            "level": "info",
            "message": f"기간 NAV는 {_fmt_pct(nav_ret)} {direction}했습니다.",
        })
    comp_summary = composition.get("summary") or {}
    if comp_summary:
        notes.append({
            "category": "composition",
            "level": "info",
            "message": (
                f"매수/증가 {comp_summary.get('buy_like_count', 0)}개, "
                f"매도/축소 {comp_summary.get('sell_like_count', 0)}개, "
                f"순 구성 변화 추정 {_fmt_krw(comp_summary.get('net_trade_value_estimate'))}입니다."
            ),
        })
    counts = (holdings.get("changes") or {}).get("counts") or {}
    changed = sum(int(counts.get(k) or 0) for k in ("added", "removed", "increased", "decreased"))
    if changed:
        notes.append({
            "category": "activity",
            "level": "info",
            "message": f"평가액 기준 변동 종목은 {changed}개입니다. 추가 {counts.get('added', 0)}, 제거 {counts.get('removed', 0)}.",
        })
    group_rows = groups.get("groups") or []
    if group_rows:
        top_group = group_rows[0]
        notes.append({
            "category": "allocation",
            "level": "info",
            "message": f"가장 큰 비중 변화는 {top_group['group_name']} ({_fmt_pct(top_group.get('weight_change_ppt'))}p)입니다.",
        })
    if quality.get("warnings"):
        notes.append({
            "category": "data_quality",
            "level": "warning",
            "message": "데이터 품질 경고가 있어 보고서 해석 시 기준일을 확인해야 합니다.",
        })
    return notes


def render_report_markdown(report: dict[str, Any]) -> str:
    period = report.get("period") or {}
    summary = report.get("summary") or {}
    cash = report.get("cashflows") or {}
    composition = report.get("composition_changes") or {}
    holdings = (report.get("holdings") or {}).get("changes") or {}
    allocation = report.get("allocation") or {}
    lines = [
        f"# 포트폴리오 기간 보고서: {period.get('label') or period.get('key')}",
        "",
        "## 요약",
        f"- 기간: {period.get('start_date')} ~ {period.get('end_date')} (기준: {period.get('baseline_date') or '-'})",
        f"- NAV 수익률: {_fmt_pct(summary.get('nav_return_pct'))}",
        f"- 평가금액: {_fmt_krw(summary.get('starting_value'))} -> {_fmt_krw(summary.get('ending_value'))}",
        f"- 순입출금: {_fmt_krw(cash.get('net_cashflow'))} (입금 {_fmt_krw(cash.get('total_deposit'))}, 출금 {_fmt_krw(cash.get('total_withdrawal'))})",
        "",
        "## 매수/매도 구성 변화",
    ]
    comp_summary = composition.get("summary") or {}
    lines.append(
        f"- 매수/증가 {comp_summary.get('buy_like_count', 0)} · 매도/축소 {comp_summary.get('sell_like_count', 0)} · 순 구성 변화 추정 {_fmt_krw(comp_summary.get('net_trade_value_estimate'))}"
    )
    for row in (composition.get("top_buys") or [])[:5]:
        lines.append(
            f"- {_activity_label(row.get('activity'))}: {row['stock_name']} {row['stock_code']} "
            f"{_fmt_krw(row.get('trade_value_estimate'))} / 비중 {_fmt_pct(row.get('start_weight_pct'))} -> {_fmt_pct(row.get('end_weight_pct'))}"
        )
    for row in (composition.get("top_sells") or [])[:5]:
        lines.append(
            f"- {_activity_label(row.get('activity'))}: {row['stock_name']} {row['stock_code']} "
            f"{_fmt_krw(row.get('trade_value_estimate'))} / 비중 {_fmt_pct(row.get('start_weight_pct'))} -> {_fmt_pct(row.get('end_weight_pct'))}"
        )
    lines.extend([
        "",
        "## 종목 변동",
    ])
    counts = holdings.get("counts") or {}
    lines.append(
        f"- 추가 {counts.get('added', 0)} · 제거 {counts.get('removed', 0)} · 증가 {counts.get('increased', 0)} · 감소 {counts.get('decreased', 0)}"
    )
    for row in (holdings.get("top_increases") or [])[:5]:
        lines.append(f"- 증가: {row['stock_name']} {row['stock_code']} {_fmt_krw(row.get('change_value'))}")
    for row in (holdings.get("top_decreases") or [])[:5]:
        lines.append(f"- 감소: {row['stock_name']} {row['stock_code']} {_fmt_krw(row.get('change_value'))}")
    lines.extend(["", "## 그룹 비중 변화"])
    for row in (allocation.get("groups") or [])[:8]:
        lines.append(
            f"- {row['group_name']}: {_fmt_pct(row.get('start_weight_pct'))} -> {_fmt_pct(row.get('end_weight_pct'))} ({_fmt_pct(row.get('weight_change_ppt'))}p)"
        )
    notes = report.get("review_notes") or []
    if notes:
        lines.extend(["", "## 검토 메모"])
        for note in notes:
            lines.append(f"- [{note.get('category')}] {note.get('message')}")
    return "\n".join(lines).strip() + "\n"


async def build_period_report(
    google_sub: str,
    period_type: str,
    period_key: str,
) -> dict[str, Any]:
    period = period_bounds(period_type, period_key)
    nav_history = await snapshots_repo.get_nav_history(google_sub)
    if not nav_history:
        raise PeriodReportError("포트폴리오 NAV 스냅샷이 없어 보고서를 생성할 수 없습니다.")

    baseline, end_snapshot, points, warnings, baseline_mode = _select_snapshots(
        nav_history,
        period["start_date"],
        period["end_date"],
    )
    if not baseline or not end_snapshot:
        raise PeriodReportError("보고서 기준 스냅샷이 부족합니다.")

    cashflows_all = await snapshots_repo.get_cashflows(google_sub)
    cashflows = _cashflow_summary(cashflows_all, period["start_date"], period["end_date"])
    start_rows = await snapshots_repo.get_stock_snapshot_rows_on_or_before(google_sub, str(baseline["date"]))
    end_rows = await snapshots_repo.get_stock_snapshot_rows_on_or_before(google_sub, str(end_snapshot["date"]))
    await _enrich_snapshot_names(start_rows)
    await _enrich_snapshot_names(end_rows)
    holding_changes = _holding_changes(start_rows, end_rows)
    composition_changes = _composition_changes(start_rows, end_rows)
    allocation = _group_changes(start_rows, end_rows)
    daily = _daily_metrics(points)

    summary = {
        "baseline_date": baseline.get("date"),
        "ending_date": end_snapshot.get("date"),
        "starting_nav": _round(baseline.get("nav")),
        "ending_nav": _round(end_snapshot.get("nav")),
        "nav_return_pct": _pct_change(baseline.get("nav"), end_snapshot.get("nav")),
        "starting_value": _round(baseline.get("total_value"), 2),
        "ending_value": _round(end_snapshot.get("total_value"), 2),
        "value_change": _round(float(end_snapshot.get("total_value") or 0) - float(baseline.get("total_value") or 0), 2),
        "value_change_pct": _pct_change(baseline.get("total_value"), end_snapshot.get("total_value")),
        "starting_invested": _round(baseline.get("total_invested"), 2),
        "ending_invested": _round(end_snapshot.get("total_invested"), 2),
        "invested_change": _round(float(end_snapshot.get("total_invested") or 0) - float(baseline.get("total_invested") or 0), 2),
        "fx_usdkrw_start": _round(baseline.get("fx_usdkrw"), 4),
        "fx_usdkrw_end": _round(end_snapshot.get("fx_usdkrw"), 4),
    }

    concentration = {
        "start": _concentration(start_rows),
        "end": _concentration(end_rows),
    }
    turnover_base = max(
        1.0,
        ((summary["starting_value"] or 0) + (summary["ending_value"] or 0)) / 2.0,
    )
    gross_change = sum(abs(float(row.get("change_value") or 0)) for row in holding_changes["all"])

    report = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _iso_now(),
        "period": {
            **period,
            "label": "월간 투자 보고서" if period["type"] == "monthly" else "연간 투자 보고서",
            "baseline_date": baseline.get("date"),
            "ending_snapshot_date": end_snapshot.get("date"),
        },
        "summary": summary,
        "cashflows": cashflows,
        "composition_changes": composition_changes,
        "risk": daily,
        "allocation": {
            **allocation,
            "concentration": concentration,
            "top_holdings_start": _top_holdings(start_rows),
            "top_holdings_end": _top_holdings(end_rows),
        },
        "holdings": {
            "snapshot_start_count": len(start_rows),
            "snapshot_end_count": len(end_rows),
            "gross_value_change": _round(gross_change, 2),
            "turnover_proxy_pct": _round(gross_change / turnover_base * 100.0),
            "changes": holding_changes,
        },
        "data_quality": _data_quality(
            warnings=warnings,
            baseline=baseline,
            end_snapshot=end_snapshot,
            start_rows=start_rows,
            end_rows=end_rows,
            period=period,
            baseline_mode=baseline_mode,
        ),
    }
    report["review_notes"] = _build_notes(report)
    source_payload = {
        "period": report["period"],
        "summary_inputs": {"baseline": baseline, "end": end_snapshot, "points": points},
        "cashflows": cashflows["rows"],
        "start_rows": start_rows,
        "end_rows": end_rows,
        "composition_changes": composition_changes,
        "schema_version": SCHEMA_VERSION,
    }
    report["source_hash"] = _source_hash(source_payload)
    return report


async def generate_and_save_period_report(
    google_sub: str,
    period_type: str,
    period_key: str,
) -> dict[str, Any]:
    report = await build_period_report(google_sub, period_type, period_key)
    report_md = render_report_markdown(report)
    return await reports_repo.save_period_report(google_sub, report, report_md=report_md)


async def get_saved_period_report(
    google_sub: str,
    period_type: str,
    period_key: str,
) -> dict[str, Any] | None:
    pt = normalize_period_type(period_type)
    period_bounds(pt, period_key)
    return await reports_repo.get_period_report(google_sub, pt, str(period_key).strip())


async def list_saved_period_reports(
    google_sub: str,
    *,
    period_type: str | None = None,
    limit: int = 36,
) -> list[dict[str, Any]]:
    pt = normalize_period_type(period_type) if period_type else None
    rows = await reports_repo.list_period_reports(google_sub, period_type=pt, limit=limit)
    return [_saved_meta(row) for row in rows]
