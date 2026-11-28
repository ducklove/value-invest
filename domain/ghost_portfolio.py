"""유령 포트폴리오("가만히 있었다면") 및 매도 후 추적 순수 계산 규칙.

외부 I/O나 DB 의존성 없이 정산 스냅샷·현금흐름·일봉·권리일 입력만으로
방치 평가액 시리즈, 매매 효과(원, %p), 매도 후 5·20거래일·현재가 비교를 계산한다.
계산할 수 없는 종목·구간은 숫자를 추정하지 않고 미계산 사유와 비중을 남긴다.
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Any

from domain.broker_assets import is_futures_contract, is_futures_value
from domain.dividend_entitlement import (
    holding_identity,
    krx_record_entitlement_day,
)
from domain.portfolio_codes import (
    is_cash_asset,
    is_korean_stock,
    is_special_asset,
    normalize_portfolio_code,
)

MIN_CALCULABLE_WEIGHT_PCT = 50.0

GHOST_ASSUMPTIONS: tuple[str, ...] = (
    "기준일 정산 스냅샷의 종목별 보유 수량과 원화 현금(예수금·CMA/RP)을 고정하고, 이후 각 정산일 종가로 재평가했습니다.",
    "기간 중 순입출금(입금 − 출금 − 분배금 출금)은 실제 계좌와 같은 정산일에 같은 금액의 원화 현금으로 반영했습니다(현금 비례 가정, 추가 매수·매도 없음).",
    "미계산 종목(및 기준일 정산 잔차)은 실제 평가액과 방치 평가액 양쪽에서 제외해 '계산 가능 종목 + 계산 가능 현금 + 순입출금' 범위끼리만 비교하며, 기준일 계산 가능 비중이 50% 미만이면 헤드라인 수치를 내지 않습니다.",
    "수익률(%·%p) 비교 분모는 계산 가능 범위의 기준일 평가액에 기간 중 순입출금을 잔여 일수 비율로 가중한 평균 투입 자본(Modified Dietz)입니다.",
    "방치 계좌의 현금배당 재투자와 매매 수수료·세금 차이는 별도 가산하지 않은 세전 가격 기준입니다.",
)

POST_SALE_NOTES: tuple[str, ...] = (
    "매도 사건 출처 우선순위: NH 실현손익 원장(체결 단가) → 일별 정산 스냅샷 보유 소멸(소멸 정산일 종가 기준) / 수동 매매 기록(체결일이 없으면 기록일 기준이며 인접일 NH·스냅샷 소멸이 있으면 그쪽 우선).",
    "NH 실현손익은 NH 일반 계좌 원장 기준(ISA·연금 등 비일반 계좌 제외)입니다.",
    "매도일 이후 배당락일·액면분할·권리락이 겹치는 구간이나 배당·분할 여부를 확인하지 못한 종목은 가격 기준이 달라지므로 미계산으로 표시합니다.",
    "변동률과 금액 차이는 매도 수량 기준 세전 가격 변화이며, 매도대금의 재투자 수익은 추정하지 않습니다.",
)

SOURCE_LABELS: dict[str, str] = {
    "nh_realized_pnl": "NH 실현손익",
    "manual_trade": "수동 매매",
    "snapshot_disappearance": "스냅샷 소멸",
}

SALE_TYPE_LABELS: dict[str, str] = {
    "closed": "전량",
    "partial": "일부",
    "unknown": "매도",
}

KRX_DAILY_RATIO_MIN = 0.69
KRX_DAILY_RATIO_MAX = 1.31
KRX_TRADE_BAR_TOLERANCE = 0.05
OVERSEAS_BAR_EPSILON = 0.005
MAX_CARRY_FORWARD_DAYS = 10


def _finite(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    return num if math.isfinite(num) else None


def _iso_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    text = str(value or "").strip()[:10]
    if len(text) != 10:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _round(value: float | None, digits: int = 2) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    return round(value, digits)


def signed_cashflow_amount(row: dict[str, Any]) -> float:
    amount = _finite(row.get("amount")) or 0.0
    kind = row.get("type")
    if kind == "deposit":
        return amount
    if kind in {"withdrawal", "distribution"}:
        return -amount
    return 0.0


def settled_cashflows_between(
    cashflows: list[dict[str, Any]],
    baseline_date: str,
    ending_date: str,
) -> list[dict[str, Any]]:
    """기준 정산일 이후 ~ 종료 정산일 사이에 실제 정산 반영된 입출금·분배금."""
    settled: list[dict[str, Any]] = []
    for row in cashflows or []:
        has_applied_col = "applied_snapshot_date" in row
        applied = row.get("applied_snapshot_date")
        if row.get("type") == "distribution" and has_applied_col and not applied:
            continue
        if has_applied_col and not applied and row.get("units_change") is None:
            continue
        eff = str(applied or row.get("date") or "")[:10]
        if not _iso_date(eff) or not (baseline_date < eff <= ending_date):
            continue
        signed = signed_cashflow_amount(row)
        if abs(signed) <= 1e-9:
            continue
        settled.append({
            "effective_date": eff,
            "date": str(row.get("date") or eff)[:10],
            "type": row.get("type"),
            "amount": abs(signed),
            "signed_amount": signed,
        })
    settled.sort(key=lambda r: r["effective_date"])
    return settled


def modified_dietz_capital(
    baseline_value: float,
    flows: list[dict[str, Any]],
    baseline_date: str,
    target_date: str,
) -> float | None:
    """기준일 평가액과 기간 중 순입출금의 잔여 기간 가중 평균 투입 자본."""
    d0 = _iso_date(baseline_date)
    dt = _iso_date(target_date)
    if d0 is None or dt is None:
        return baseline_value if baseline_value > 0 else None
    total_days = (dt - d0).days
    if total_days <= 0:
        return baseline_value if baseline_value > 0 else None
    weighted = 0.0
    for flow in flows:
        eff = flow["effective_date"]
        if eff > target_date:
            continue
        cf_day = _iso_date(eff)
        if cf_day is None:
            continue
        clamped = min(dt, max(d0, cf_day))
        remaining_days = max(0, (dt - clamped).days)
        weighted += flow["signed_amount"] * (remaining_days / total_days)
    capital = baseline_value + weighted
    return capital if capital > 0 else None


def lookup_close_on_or_before(
    closes: list[tuple[str, float]],
    target_date: str,
    *,
    max_lag_days: int = MAX_CARRY_FORWARD_DAYS,
) -> float | None:
    """정산일 이하 가장 최근 거래일 종가. 장기 공백이면 None."""
    if not closes:
        return None
    target_d = _iso_date(target_date)
    if target_d is None:
        return None
    lo, hi = 0, len(closes) - 1
    best_idx = -1
    while lo <= hi:
        mid = (lo + hi) // 2
        if closes[mid][0] <= target_date:
            best_idx = mid
            lo = mid + 1
        else:
            hi = mid - 1
    if best_idx < 0:
        return None
    found_date, price = closes[best_idx]
    found_d = _iso_date(found_date)
    if found_d is None or (target_d - found_d).days > max_lag_days:
        return None
    return price if price > 0 else None


def detect_krx_series_split_or_mismatch(
    baseline_unit_price: float | None,
    baseline_date: str,
    ending_date: str,
    daily_bars: list[dict[str, Any]],
) -> str | None:
    """국내 종목 일봉에서 미확보·기준일 수정주가 괴리·상장주식수 급변·가격제한폭 초과 변동을 감지한다."""
    if not daily_bars:
        return "분할 여부 확인 전 (일봉 미확보)"
    window = [b for b in daily_bars if baseline_date <= str(b.get("date") or "") <= ending_date]
    if not window:
        return "분할 여부 확인 전 (기간 일봉 미확보)"
    closes = [(str(b["date"]), float(b["close"])) for b in daily_bars if b.get("date") and (_finite(b.get("close")) or 0) > 0]
    base_close = lookup_close_on_or_before(closes, baseline_date)
    end_close = lookup_close_on_or_before(closes, ending_date)
    if base_close is None or end_close is None:
        return "분할 여부 확인 전 (기준·종료일 일봉 미확보)"
    if baseline_unit_price and baseline_unit_price > 0:
        ratio = base_close / baseline_unit_price
        if ratio < 0.90 or ratio > 1.10:
            return "분할·권리락으로 기준일 가격 기준 불일치"
    prev_close: float | None = None
    prev_shares: float | None = None
    for bar in window:
        close = _finite(bar.get("close"))
        if close is not None and close > 0:
            if prev_close is not None and prev_close > 0:
                step = close / prev_close
                if step < KRX_DAILY_RATIO_MIN or step > KRX_DAILY_RATIO_MAX:
                    return "분할·권리락 의심 가격 변동"
            prev_close = close
        shares = _finite(bar.get("listed_shares"))
        if shares is not None and shares > 0:
            if prev_shares is not None and prev_shares > 0:
                s_ratio = shares / prev_shares
                if s_ratio < 0.92 or s_ratio > 1.08:
                    return "상장주식수 변동(분할·증자·감자) 구간"
            prev_shares = shares
    return None


def detect_snapshot_quantity_split_flags(
    *,
    stock_rows: list[dict[str, Any]],
    manual_trades: list[dict[str, Any]],
    nh_rows: list[dict[str, Any]],
    cashflows: list[dict[str, Any]],
    baseline_date: str,
    ending_date: str,
) -> dict[str, str]:
    """스냅샷에서 거래 기록(또는 자금 이동) 없이 같은 종목 수량이 급변하면 분할 의심으로 표시한다."""
    by_date: dict[str, dict[str, dict[str, Any]]] = {}
    day_total_mv: dict[str, float] = {}
    for raw in stock_rows or []:
        d = str(raw.get("date") or "")[:10]
        code = normalize_portfolio_code(raw.get("stock_code"))
        if not _iso_date(d) or not (baseline_date <= d <= ending_date) or not code:
            continue
        mv = _finite(raw.get("market_value")) or 0.0
        day_total_mv[d] = day_total_mv.get(d, 0.0) + mv
        if is_cash_asset(code) or code == "CMA_RP_KRW" or is_futures_contract(code) or is_futures_value(code):
            continue
        qty = _finite(raw.get("quantity"))
        unit = _finite(raw.get("unit_price"))
        cur = by_date.setdefault(d, {}).get(code)
        if cur is None:
            by_date[d][code] = {"quantity": qty, "market_value": mv, "unit_price": unit}
        else:
            cur["market_value"] += mv
            cur["quantity"] = None if (cur["quantity"] is None or qty is None) else (cur["quantity"] + qty)
            if cur["quantity"] and cur["quantity"] > 0:
                cur["unit_price"] = cur["market_value"] / cur["quantity"]

    dates = sorted(by_date)
    if len(dates) < 2 or baseline_date not in by_date:
        return {}

    trade_days_by_ident: dict[str, list[date]] = {}
    pending_nh_idents: set[str] = set()
    for t in manual_trades or []:
        if t.get("side") not in {"buy", "sell"}:
            continue
        td = _iso_date(t.get("trade_date"))
        ident = holding_identity(t.get("stock_code"))
        if td and ident:
            trade_days_by_ident.setdefault(ident, []).append(td)
    for r in nh_rows or []:
        ident = holding_identity(r.get("stock_code") or r.get("symbol"))
        if not ident:
            continue
        issues = set(r.get("issues") or ())
        if "detail_pending" in issues or "detail_missing" in issues:
            pending_nh_idents.add(ident)
            continue
        td = _iso_date(r.get("trade_date"))
        if td:
            trade_days_by_ident.setdefault(ident, []).append(td)

    flows = settled_cashflows_between(cashflows, baseline_date, ending_date)

    def has_trade_between(ident: str, d0_iso: str, d1_iso: str) -> bool:
        if ident in pending_nh_idents:
            return True
        low_d = (_iso_date(d0_iso) or date.min) - timedelta(days=3)
        high_d = (_iso_date(d1_iso) or date.max) + timedelta(days=2)
        return any(low_d <= td <= high_d for td in trade_days_by_ident.get(ident, ()))

    def net_cf_between(d0_iso: str, d1_iso: str) -> float:
        return sum(f["signed_amount"] for f in flows if d0_iso < f["effective_date"] <= d1_iso)

    flags: dict[str, str] = {}
    baseline_codes = sorted(by_date[baseline_date])
    for code in baseline_codes:
        ident = holding_identity(code)
        prev_d: str | None = None
        prev_info: dict[str, Any] | None = None
        for d in dates:
            info = by_date[d].get(code)
            if info is None or info["quantity"] is None or info["quantity"] <= 0:
                prev_d, prev_info = None, None
                continue
            if prev_d is not None and prev_info is not None:
                q0, q1 = float(prev_info["quantity"]), float(info["quantity"])
                q_ratio = q1 / q0
                if abs(q_ratio - 1.0) >= 0.05 and not has_trade_between(ident, prev_d, d):
                    u0 = prev_info["unit_price"] or (prev_info["market_value"] / q0 if prev_info["market_value"] > 0 else None)
                    u1 = info["unit_price"] or (info["market_value"] / q1 if info["market_value"] > 0 else None)
                    # 1) 단가가 수량과 역방향으로 움직이면 전형적인 액면분할·무상증자·병합
                    inverse_price = bool(
                        u0 and u1 and u0 > 0 and u1 > 0
                        and ((q_ratio > 1.05 and u1 / u0 < 0.88) or (q_ratio < 0.95 and u1 / u0 > 1.14))
                    )
                    # 2) 수량이 크게 변했는데 계좌 내 현금·타 종목 감소(또는 입출금)로 설명되지 않는 경우
                    unfunded_jump = False
                    if abs(q_ratio - 1.0) >= 0.25:
                        if not u1 or u1 <= 0:
                            unfunded_jump = True
                        else:
                            implied_trade_krw = (q1 - q0) * u1
                            other_mv_0 = day_total_mv.get(prev_d, 0.0) - float(prev_info["market_value"])
                            other_mv_1 = day_total_mv.get(d, 0.0) - float(info["market_value"])
                            available_funding_change = (other_mv_1 - other_mv_0) - net_cf_between(prev_d, d)
                            # 실제 현금 매수라면 타 자산·현금이 implied_trade_krw만큼 줄었어야 한다(합이 ~0).
                            if abs(available_funding_change + implied_trade_krw) > max(abs(implied_trade_krw) * 0.45, 50_000.0):
                                unfunded_jump = True
                    if inverse_price or unfunded_jump:
                        flags[code] = "스냅샷 수량 급변(분할·무상증자 의심)"
                        break
            prev_d, prev_info = d, info
    return flags


def _is_krw_cash_code(code: str) -> bool:
    return code in {"CASH_KRW", "CMA_RP_KRW"}


def _same_quantity_snapshot_value(snap_row: dict[str, Any] | None, qty: float, *, allow_fallback: bool) -> float | None:
    """같은 날 스냅샷에서 기준일과 같은 수량을 보유했다면 그 평가액을 그대로 쓴다(무매매 시 실제 == 방치)."""
    if not snap_row or (snap_row.get("priced_from_fallback") and not allow_fallback):
        return None
    s_qty = _finite(snap_row.get("quantity"))
    s_mv = _finite(snap_row.get("market_value"))
    if s_qty is None or s_mv is None or s_mv <= 0:
        return None
    if abs(s_qty - qty) > max(1e-9, abs(qty) * 1e-9):
        return None
    return s_mv


def _merge_baseline_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for raw in rows or []:
        code = normalize_portfolio_code(raw.get("stock_code"))
        if not code:
            continue
        mv = _finite(raw.get("market_value")) or 0.0
        qty = _finite(raw.get("quantity"))
        unit = _finite(raw.get("unit_price"))
        if code not in merged:
            merged[code] = {
                "stock_code": code,
                "stock_name": str(raw.get("stock_name") or code),
                "market_value": mv,
                "quantity": qty,
                "unit_price": unit,
                "currency": raw.get("currency"),
            }
        else:
            cur = merged[code]
            cur["market_value"] += mv
            if cur["quantity"] is None or qty is None:
                cur["quantity"] = None
            else:
                cur["quantity"] += qty
            if cur["quantity"] and cur["quantity"] != 0:
                cur["unit_price"] = cur["market_value"] / cur["quantity"]
    return [
        item for item in merged.values()
        if abs(item["market_value"]) > 1e-6 or (item["quantity"] is not None and abs(item["quantity"]) > 1e-9)
    ]


def compute_ghost_portfolio(
    *,
    nav_points: list[dict[str, Any]],
    baseline_stocks: list[dict[str, Any]],
    snapshot_prices_by_date: dict[str, dict[str, dict[str, Any]]],
    external_krw_closes: dict[str, list[tuple[str, float]]],
    split_flags: dict[str, str],
    cashflows: list[dict[str, Any]],
    stock_names: dict[str, str] | None = None,
    uncalc_krw_trade_flows: dict[str, list[tuple[str, float]]] | None = None,
) -> dict[str, Any]:
    """기준일 보유 수량을 고정한 방치 평가액 시리즈와 계산 가능 범위 한정 매매 효과를 계산한다."""
    names = stock_names or {}
    if len(nav_points) < 2:
        return {
            "available": False,
            "scope": "unavailable",
            "reason": "비교할 일별 정산 스냅샷이 2개 이상 필요합니다.",
            "assumptions": list(GHOST_ASSUMPTIONS),
        }
    baseline_pt = nav_points[0]
    ending_pt = nav_points[-1]
    baseline_date = str(baseline_pt["date"])[:10]
    ending_date = str(ending_pt["date"])[:10]
    baseline_total = _finite(baseline_pt.get("total_value"))
    if baseline_total is None or baseline_total <= 0:
        return {
            "available": False,
            "scope": "unavailable",
            "reason": "기준일 총평가액을 확인할 수 없습니다.",
            "assumptions": list(GHOST_ASSUMPTIONS),
        }

    items = _merge_baseline_rows(baseline_stocks)
    if not items:
        return {
            "available": False,
            "scope": "unavailable",
            "reason": "기준일 종목별 정산 내역이 없습니다.",
            "assumptions": list(GHOST_ASSUMPTIONS),
        }

    dates = [str(pt["date"])[:10] for pt in nav_points]
    fx_by_date = {str(pt["date"])[:10]: _finite(pt.get("fx_usdkrw")) for pt in nav_points}
    flows = settled_cashflows_between(cashflows, baseline_date, ending_date)

    item_series: dict[str, list[float]] = {}
    holdings_out: list[dict[str, Any]] = []
    baseline_cash_krw = 0.0

    for item in items:
        code = item["stock_code"]
        name = names.get(code) or item.get("stock_name") or code
        base_mv = float(item["market_value"])
        qty = item["quantity"]
        weight_pct = _round(base_mv / baseline_total * 100.0, 2) or 0.0

        if _is_krw_cash_code(code):
            baseline_cash_krw += base_mv
            series_vals = [base_mv for _ in dates]
            item_series[code] = series_vals
            holdings_out.append({
                "stock_code": code,
                "stock_name": name,
                "asset_kind": "cash",
                "status": "calculable",
                "reason": None,
                "quantity": qty,
                "baseline_value": _round(base_mv, 2),
                "baseline_weight_pct": weight_pct,
                "ending_ghost_value": _round(base_mv, 2),
                "ghost_change_krw": 0.0,
                "ghost_return_pct": 0.0,
            })
            continue

        if is_cash_asset(code):
            if qty is None or qty <= 0:
                holdings_out.append({
                    "stock_code": code,
                    "stock_name": name,
                    "asset_kind": "cash",
                    "status": "uncalculated",
                    "reason": "환율 미확보 (외화 현금 수량 미기록)",
                    "quantity": qty,
                    "baseline_value": _round(base_mv, 2),
                    "baseline_weight_pct": weight_pct,
                    "ending_ghost_value": None,
                    "ghost_change_krw": None,
                    "ghost_return_pct": None,
                })
                continue

            series_vals = [base_mv]
            fx_missing = False
            ext_fx = external_krw_closes.get(code) or []
            for d in dates[1:]:
                snap_row = (snapshot_prices_by_date.get(d) or {}).get(code)
                same_qty_mv = _same_quantity_snapshot_value(snap_row, float(qty), allow_fallback=False)
                if same_qty_mv is not None:
                    series_vals.append(same_qty_mv)
                    continue
                snap_price = _finite(snap_row.get("unit_price")) if snap_row and not snap_row.get("priced_from_fallback") else None
                if snap_price is None and snap_row and not snap_row.get("priced_from_fallback"):
                    s_qty = _finite(snap_row.get("quantity"))
                    s_mv = _finite(snap_row.get("market_value"))
                    if s_qty and s_qty > 0 and s_mv and s_mv > 0:
                        snap_price = s_mv / s_qty
                if snap_price and snap_price > 0:
                    series_vals.append(qty * snap_price)
                elif code == "CASH_USD" and (fx_by_date.get(d) or 0) > 0:
                    series_vals.append(qty * float(fx_by_date[d]))
                else:
                    ext = lookup_close_on_or_before(ext_fx, d)
                    if ext and ext > 0:
                        series_vals.append(qty * ext)
                    else:
                        fx_missing = True
                        break

            if fx_missing:
                holdings_out.append({
                    "stock_code": code,
                    "stock_name": name,
                    "asset_kind": "cash",
                    "status": "uncalculated",
                    "reason": "환율 미확보",
                    "quantity": qty,
                    "baseline_value": _round(base_mv, 2),
                    "baseline_weight_pct": weight_pct,
                    "ending_ghost_value": None,
                    "ghost_change_krw": None,
                    "ghost_return_pct": None,
                })
            else:
                baseline_cash_krw += base_mv
                item_series[code] = series_vals
                end_val = series_vals[-1]
                holdings_out.append({
                    "stock_code": code,
                    "stock_name": name,
                    "asset_kind": "cash",
                    "status": "calculable",
                    "reason": None,
                    "quantity": qty,
                    "baseline_value": _round(base_mv, 2),
                    "baseline_weight_pct": weight_pct,
                    "ending_ghost_value": _round(end_val, 2),
                    "ghost_change_krw": _round(end_val - base_mv, 2),
                    "ghost_return_pct": _round((end_val / base_mv - 1.0) * 100.0, 2) if base_mv > 0 else 0.0,
                })
            continue

        uncalc_reason: str | None = None
        if is_futures_contract(code) or is_futures_value(code):
            uncalc_reason = "선물·파생 평가자산"
        elif qty is not None and qty < 0:
            uncalc_reason = "공매도·숏 포지션"
        elif qty is None or qty <= 0:
            uncalc_reason = "기준일 보유 수량 미기록"
        elif code in split_flags:
            uncalc_reason = split_flags[code]

        series_vals = [base_mv]
        if uncalc_reason is None:
            ext_closes = external_krw_closes.get(code) or []
            for d in dates[1:]:
                snap_row = (snapshot_prices_by_date.get(d) or {}).get(code)
                # 수량이 기준일과 같으면(직전 값 대체 행 포함) 실제 평가액과 같은 숫자를 써서 무매매 구간의 가짜 차이를 막는다.
                same_qty_mv = _same_quantity_snapshot_value(snap_row, float(qty), allow_fallback=True)
                if same_qty_mv is not None:
                    series_vals.append(same_qty_mv)
                    continue
                snap_price = None
                if snap_row and not snap_row.get("priced_from_fallback"):
                    candidate = _finite(snap_row.get("unit_price"))
                    if candidate is None:
                        s_qty = _finite(snap_row.get("quantity"))
                        s_mv = _finite(snap_row.get("market_value"))
                        if s_qty and s_qty > 0 and s_mv is not None and s_mv > 0:
                            candidate = s_mv / s_qty
                    if candidate and candidate > 0:
                        snap_price = candidate
                if snap_price is not None:
                    series_vals.append(float(qty) * snap_price)
                    continue
                ext_price = lookup_close_on_or_before(ext_closes, d)
                if ext_price is not None:
                    series_vals.append(float(qty) * ext_price)
                else:
                    if is_special_asset(code):
                        uncalc_reason = "특수자산 과거 종가 미제공"
                    elif not is_korean_stock(code):
                        uncalc_reason = "해외 종목 과거 종가·환율 미확보"
                    else:
                        uncalc_reason = "종가 데이터 미확보"
                    break

        if uncalc_reason is not None:
            holdings_out.append({
                "stock_code": code,
                "stock_name": name,
                "asset_kind": "stock",
                "status": "uncalculated",
                "reason": uncalc_reason,
                "quantity": qty,
                "baseline_value": _round(base_mv, 2),
                "baseline_weight_pct": weight_pct,
                "ending_ghost_value": None,
                "ghost_change_krw": None,
                "ghost_return_pct": None,
            })
        else:
            item_series[code] = series_vals
            end_val = series_vals[-1]
            holdings_out.append({
                "stock_code": code,
                "stock_name": name,
                "asset_kind": "stock",
                "status": "calculable",
                "reason": None,
                "quantity": qty,
                "baseline_value": _round(base_mv, 2),
                "baseline_weight_pct": weight_pct,
                "ending_ghost_value": _round(end_val, 2),
                "ghost_change_krw": _round(end_val - base_mv, 2),
                "ghost_return_pct": _round((end_val / base_mv - 1.0) * 100.0, 2) if base_mv > 0 else None,
            })

    sum_items_d0 = sum(float(h["baseline_value"] or 0.0) for h in holdings_out)
    residual_d0 = baseline_total - sum_items_d0
    if abs(residual_d0) > max(1000.0, baseline_total * 0.005):
        holdings_out.append({
            "stock_code": "_UNTRACKED",
            "stock_name": "종목별 정산 미분류 잔차",
            "asset_kind": "stock",
            "status": "uncalculated",
            "reason": "기준일 총평가액과 종목별 합계 차이",
            "quantity": None,
            "baseline_value": _round(residual_d0, 2),
            "baseline_weight_pct": _round(residual_d0 / baseline_total * 100.0, 2) or 0.0,
            "ending_ghost_value": None,
            "ghost_change_krw": None,
            "ghost_return_pct": None,
        })

    holdings_out.sort(key=lambda h: abs(float(h["baseline_value"] or 0.0)), reverse=True)

    non_cash = [h for h in holdings_out if h["asset_kind"] != "cash"]
    calc_non_cash = [h for h in non_cash if h["status"] == "calculable"]
    uncalc_items = [h for h in holdings_out if h["status"] == "uncalculated"]
    uncalculated = [
        {
            "stock_code": h["stock_code"],
            "stock_name": h["stock_name"],
            "quantity": h["quantity"],
            "baseline_value": h["baseline_value"],
            "baseline_weight_pct": h["baseline_weight_pct"],
            "reason": h["reason"],
        }
        for h in uncalc_items
    ]

    cash_weight_pct = _round(
        sum(float(h["baseline_value"] or 0.0) for h in holdings_out if h["asset_kind"] == "cash" and h["status"] == "calculable")
        / baseline_total * 100.0,
        2,
    ) or 0.0
    uncalc_principal_d0 = sum(float(h["baseline_value"] or 0.0) for h in uncalc_items)
    uncalc_weight_pct = _round(max(0.0, min(100.0, uncalc_principal_d0 / baseline_total * 100.0)), 2) or 0.0
    calc_weight_pct = _round(max(0.0, 100.0 - uncalc_weight_pct), 2) or 0.0

    coverage = {
        "total_holdings": len(non_cash),
        "calculable_holdings": len(calc_non_cash),
        "uncalculated_holdings": len(uncalculated),
        "cash_weight_pct": cash_weight_pct,
        "calculable_weight_pct": calc_weight_pct,
        "uncalculated_weight_pct": uncalc_weight_pct,
    }

    assumptions = list(GHOST_ASSUMPTIONS)
    if uncalculated:
        assumptions.append(
            f"미계산 {len(uncalculated)}건(기준일 비중 {uncalc_weight_pct:.2f}%)의 일자별 평가액은 실제·방치 양쪽에서 제외하고 "
            f"계산 가능 범위(기준일 비중 {calc_weight_pct:.2f}%)만 비교했습니다."
        )

    # 계산 범위 기준일 평가액 = 계산 가능 종목·현금의 기준일 평가액 합(정산 잔차·미계산 제외)
    scoped_baseline = sum(float(h["baseline_value"] or 0.0) for h in holdings_out if h["status"] == "calculable")
    low_coverage = calc_weight_pct < MIN_CALCULABLE_WEIGHT_PCT or scoped_baseline <= 0
    no_calc_holdings = bool(non_cash and not calc_non_cash)

    if low_coverage or no_calc_holdings:
        reason_msg = (
            f"계산 가능 비중 부족 (기준일 계산 가능 비중 {calc_weight_pct:.1f}% < 50%)"
            if low_coverage and uncalculated
            else "기준일 보유 종목의 가격·수량을 확인할 수 없어 방치 평가액을 미계산했습니다."
        )
        series_uncalc = [
            {
                "date": str(pt["date"])[:10],
                "actual_value": _round(_finite(pt.get("total_value")), 2),
                "ghost_value": None,
                "net_cashflow_cum": 0.0,
                "diff_krw": None,
                "fx_usdkrw": _finite(pt.get("fx_usdkrw")),
            }
            for pt in nav_points
        ]
        return {
            "available": True,
            "scope": "uncalculated",
            "reason": reason_msg,
            "total_baseline_value": _round(baseline_total, 2),
            "baseline_value": _round(max(0.0, scoped_baseline), 2),
            "baseline_cash_krw": _round(baseline_cash_krw, 2),
            "net_cashflow_krw": _round(sum(f["signed_amount"] for f in flows), 2) or 0.0,
            "actual_end_value": None,
            "ghost_end_value": None,
            "actual_return_pct": None,
            "ghost_return_pct": None,
            "effect_krw": None,
            "effect_pct_p": None,
            "coverage": coverage,
            "holdings": holdings_out,
            "uncalculated": uncalculated,
            "series": series_uncalc,
            "assumptions": assumptions,
        }

    # 미계산 종목의 일자별 실제 평가액(및 매매로 현금 전환된 몫)과 종목별 정산 밖 잔차를 실제 쪽에서 빼서
    # 양쪽 모두 '계산 가능 종목 + 계산 가능 현금 + 순입출금' 범위끼리만 비교한다.
    uncalc_codes = [h for h in uncalc_items if h["stock_code"] != "_UNTRACKED"]
    trade_flows_map = uncalc_krw_trade_flows or {}
    # code -> (마지막으로 스냅샷에서 확인된 평가액, 그 시점까지의 누적 원화 매매 현금)
    last_seen: dict[str, tuple[float, float]] = {
        h["stock_code"]: (float(h["baseline_value"] or 0.0), 0.0) for h in uncalc_codes
    }

    def residual_on(idx: int, d: str) -> float:
        if idx == 0:
            return residual_d0
        snap_day = snapshot_prices_by_date.get(d)
        if not snap_day:
            return residual_d0
        rows_sum = sum(_finite(r.get("market_value")) or 0.0 for r in snap_day.values())
        return float(_finite(nav_points[idx].get("total_value")) or 0.0) - rows_sum

    def uncalc_deduction_on(idx: int, d: str) -> float:
        if idx == 0:
            return sum(float(h["baseline_value"] or 0.0) for h in uncalc_codes)
        snap_day = snapshot_prices_by_date.get(d) or {}
        deduction = 0.0
        for h in uncalc_codes:
            u_code = h["stock_code"]
            # 이 날짜까지 기록된 미계산 종목의 순현금 유입(매도 + / 매수 -)
            cum_cash = sum(amt for td, amt in (trade_flows_map.get(u_code) or ()) if baseline_date < td <= d)
            snap_u = snap_day.get(u_code)
            cur_mv = _finite(snap_u.get("market_value")) if snap_u is not None else None
            if cur_mv is not None and cur_mv != 0:
                last_seen[u_code] = (cur_mv, cum_cash)
                deduction += cur_mv + cum_cash
                continue
            seen_mv, seen_cash = last_seen[u_code]
            post_cash = cum_cash - seen_cash
            # 스냅샷에서 사라진 뒤: 체결 기록이 있으면 그 현금을, 없으면 마지막 평가액을 현금 전환분으로 본다.
            deduction += seen_cash + (post_cash if abs(post_cash) > 1e-9 else seen_mv)
        return deduction

    cf_idx = 0
    running_cf = 0.0
    series_out: list[dict[str, Any]] = []
    for idx, pt in enumerate(nav_points):
        d = dates[idx]
        while cf_idx < len(flows) and flows[cf_idx]["effective_date"] <= d:
            running_cf += flows[cf_idx]["signed_amount"]
            cf_idx += 1
        raw_actual = float(_finite(pt.get("total_value")) or 0.0)
        actual_scoped = raw_actual - residual_on(idx, d) - uncalc_deduction_on(idx, d)
        if idx == 0:
            ghost_scoped = scoped_baseline
        else:
            calc_sum = sum(vals[idx] for vals in item_series.values())
            ghost_scoped = calc_sum + running_cf
        series_out.append({
            "date": d,
            "actual_value": _round(actual_scoped, 2),
            "ghost_value": _round(ghost_scoped, 2),
            "raw_actual_value": _round(raw_actual, 2),
            "net_cashflow_cum": _round(running_cf, 2),
            "diff_krw": _round(actual_scoped - ghost_scoped, 2),
            "fx_usdkrw": _finite(pt.get("fx_usdkrw")),
        })

    actual_end = float(series_out[-1]["actual_value"])
    ghost_end = float(series_out[-1]["ghost_value"])
    total_cf = running_cf
    effect_krw = _round(actual_end - ghost_end, 2)
    capital = modified_dietz_capital(scoped_baseline, flows, baseline_date, ending_date)
    if capital and capital > 0:
        actual_pnl = actual_end - scoped_baseline - total_cf
        ghost_pnl = ghost_end - scoped_baseline - total_cf
        actual_ret = _round(actual_pnl / capital * 100.0, 2)
        ghost_ret = _round(ghost_pnl / capital * 100.0, 2)
        effect_pct_p = _round((actual_pnl - ghost_pnl) / capital * 100.0, 2)
    else:
        actual_ret = None
        ghost_ret = None
        effect_pct_p = None

    return {
        "available": True,
        "scope": "partial" if uncalculated else "complete",
        "reason": None,
        "total_baseline_value": _round(baseline_total, 2),
        "baseline_value": _round(scoped_baseline, 2),
        "baseline_cash_krw": _round(baseline_cash_krw, 2),
        "net_cashflow_krw": _round(total_cf, 2),
        "actual_end_value": _round(actual_end, 2),
        "ghost_end_value": _round(ghost_end, 2),
        "actual_return_pct": actual_ret,
        "ghost_return_pct": ghost_ret,
        "effect_krw": effect_krw,
        "effect_pct_p": effect_pct_p,
        "coverage": coverage,
        "holdings": holdings_out,
        "uncalculated": uncalculated,
        "series": series_out,
        "assumptions": assumptions,
    }


def krx_ex_dividend_date_from_record(record_date: str) -> str | None:
    """국내 배당기준일(R)로부터 배당락일(권리 보유 마지막 거래일 다음 거래일)을 구한다."""
    rec = _iso_date(record_date)
    if rec is None:
        return None
    entitlement_day, _ = krx_record_entitlement_day(rec)
    probe = entitlement_day + timedelta(days=1)
    while probe <= rec:
        if probe.weekday() < 5:
            from domain.market_calendar import is_trading_day
            known = is_trading_day(probe)
            if known is True or known is None:
                return probe.isoformat()
        probe += timedelta(days=1)
    return rec.isoformat()


def _weekdays_between(start_iso: str, end_iso: str) -> int:
    start_d = _iso_date(start_iso)
    end_d = _iso_date(end_iso)
    if not start_d or not end_d or end_d <= start_d:
        return 0
    return sum((start_d + timedelta(days=k)).weekday() < 5 for k in range(1, (end_d - start_d).days + 1))


def collect_sale_events(
    *,
    nh_rows: list[dict[str, Any]],
    manual_trades: list[dict[str, Any]],
    stock_snapshots: list[dict[str, Any]],
    effective_since: str,
    stock_names: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """세 출처에서 매도 사건을 모은다. 체결일 없는 수동 매매는 인접일 NH·스냅샷 소멸이 있으면 그쪽을 우선한다."""
    names = stock_names or {}

    snap_by_date: dict[str, dict[str, dict[str, Any]]] = {}
    for row in stock_snapshots or []:
        d = str(row.get("date") or "")[:10]
        code = normalize_portfolio_code(row.get("stock_code"))
        if not _iso_date(d) or not code or is_cash_asset(code) or code == "CMA_RP_KRW":
            continue
        if is_futures_contract(code) or is_futures_value(code):
            continue
        ident = holding_identity(code)
        qty = _finite(row.get("quantity"))
        mv = _finite(row.get("market_value")) or 0.0
        cur = snap_by_date.setdefault(d, {}).get(ident)
        if cur is None:
            snap_by_date[d][ident] = {"code": code, "quantity": qty, "market_value": mv}
        else:
            cur["market_value"] += mv
            cur["quantity"] = None if (cur["quantity"] is None or qty is None) else (cur["quantity"] + qty)
    snap_dates = sorted(snap_by_date)

    def infer_sale_type(ident: str, trade_date: str) -> str:
        candidates = [d for d in snap_dates if d >= trade_date]
        if not candidates:
            return "unknown"
        nearest = candidates[0]
        if (_iso_date(nearest) - _iso_date(trade_date)).days > 7:  # type: ignore[operator]
            return "unknown"
        entry = snap_by_date[nearest].get(ident)
        if entry is None:
            return "closed"
        qty = entry["quantity"]
        mv = entry["market_value"]
        if (qty is not None and qty <= 0) or (qty is None and mv <= 0):
            return "closed"
        return "partial"

    events: list[dict[str, Any]] = []
    nh_ident_dates: list[tuple[str, date]] = []
    explicit_manual_ident_dates: list[tuple[str, date]] = []
    valid_snap_ident_dates: list[tuple[str, date]] = []
    pending_nh_idents: set[str] = set()

    # 1) NH 실현손익 원장 (일반 계좌, 같은 날·같은 종목 분할 체결은 가중평균 합산)
    nh_grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in nh_rows or []:
        t_date = str(row.get("trade_date") or "")[:10]
        if not _iso_date(t_date) or t_date < effective_since:
            continue
        raw_code = str(row.get("stock_code") or row.get("symbol") or "").strip()
        code = normalize_portfolio_code(raw_code) if raw_code else ""
        ident = holding_identity(code) if code else ""
        issues = set(row.get("issues") or ())
        currency = str(row.get("currency") or "KRW").strip().upper() or "KRW"
        row_name = str(row.get("stock_name") or names.get(code) or code or "종목 상세 미확인").strip()

        if "detail_pending" in issues or "detail_missing" in issues or not code:
            if ident:
                pending_nh_idents.add(ident)
            events.append({
                "trade_date": t_date,
                "stock_code": code,
                "stock_name": row_name,
                "currency": currency,
                "source": "nh_realized_pnl",
                "sale_type": "unknown",
                "price_basis": "execution",
                "date_is_recorded_at": False,
                "date_note": None,
                "quantity": _finite(row.get("sell_quantity")),
                "sell_price": None,
                "sell_amount": None,
                "status": "uncalculated",
                "reason": "NH 일자·종목 상세 미확보",
            })
            continue

        t_day = _iso_date(t_date)
        assert t_day is not None
        if "fx_implausible" in issues or (row.get("market") == "overseas" and "native_missing" in issues):
            nh_ident_dates.append((ident, t_day))
            events.append({
                "trade_date": t_date,
                "stock_code": code,
                "stock_name": row_name,
                "currency": currency,
                "source": "nh_realized_pnl",
                "sale_type": infer_sale_type(ident, t_date),
                "price_basis": "execution",
                "date_is_recorded_at": False,
                "date_note": None,
                "quantity": _finite(row.get("sell_quantity")),
                "sell_price": None,
                "sell_amount": None,
                "status": "uncalculated",
                "reason": "NH 체결 통화·금액 검산 보류",
            })
            continue

        qty = _finite(row.get("sell_quantity"))
        amt = _finite(row.get("sell_krw") if currency == "KRW" else row.get("sell_native"))
        if qty is None or qty <= 0 or amt is None or amt <= 0:
            nh_ident_dates.append((ident, t_day))
            events.append({
                "trade_date": t_date,
                "stock_code": code,
                "stock_name": row_name,
                "currency": currency,
                "source": "nh_realized_pnl",
                "sale_type": infer_sale_type(ident, t_date),
                "price_basis": "execution",
                "date_is_recorded_at": False,
                "date_note": None,
                "quantity": qty,
                "sell_price": None,
                "sell_amount": amt,
                "status": "uncalculated",
                "reason": "NH 매도 수량·대금 미확인",
            })
            continue

        key = (t_date, ident, currency)
        bucket = nh_grouped.get(key)
        if bucket is None:
            nh_grouped[key] = {
                "trade_date": t_date,
                "stock_code": code,
                "ident": ident,
                "stock_name": row_name,
                "currency": currency,
                "quantity": qty,
                "sell_amount": amt,
            }
        else:
            bucket["quantity"] += qty
            bucket["sell_amount"] += amt

    for bucket in nh_grouped.values():
        t_date = bucket["trade_date"]
        ident = bucket["ident"]
        nh_ident_dates.append((ident, _iso_date(t_date)))  # type: ignore[arg-type]
        qty = bucket["quantity"]
        amt = bucket["sell_amount"]
        events.append({
            "trade_date": t_date,
            "stock_code": bucket["stock_code"],
            "stock_name": names.get(bucket["stock_code"]) or bucket["stock_name"],
            "currency": bucket["currency"],
            "source": "nh_realized_pnl",
            "sale_type": infer_sale_type(ident, t_date),
            "price_basis": "execution",
            "date_is_recorded_at": False,
            "date_note": None,
            "quantity": _round(qty, 6),
            "sell_price": amt / qty,
            "sell_amount": _round(amt, 2),
            "status": "ok",
            "reason": None,
        })

    # 수동 매매 후보를 먼저 정규화해 체결일 명시 건과 기록일(created_at) 기준 건을 구분한다.
    nh_exact_keys = {(ident, d.isoformat()) for ident, d in nh_ident_dates}
    manual_candidates: list[dict[str, Any]] = []
    manual_grouped: dict[tuple[str, str, str, bool], dict[str, Any]] = {}

    for trade in manual_trades or []:
        if trade.get("side") != "sell":
            continue
        t_date = str(trade.get("trade_date") or "")[:10]
        t_day = _iso_date(t_date)
        if not t_day or t_date < effective_since:
            continue
        code = normalize_portfolio_code(trade.get("stock_code"))
        if not code or is_cash_asset(code):
            continue
        ident = holding_identity(code)
        recorded_only = bool(trade.get("date_is_recorded_at", False))
        # NH 원장과 같은 종목·인접일(기록일 기준은 ±5일, 체결일 명시는 당일)로 겹치면 NH 우선
        if recorded_only:
            if ident in pending_nh_idents or any(cid == ident and abs((d - t_day).days) <= 5 for cid, d in nh_ident_dates):
                continue
        else:
            if (ident, t_date) in nh_exact_keys:
                continue

        currency = str(trade.get("currency") or "KRW").strip().upper() or "KRW"
        qty = _finite(trade.get("quantity"))
        price = _finite(trade.get("price"))
        gross = _finite(trade.get("gross_amount"))
        if gross is None and qty and price:
            gross = qty * price
        qty_after = _finite(trade.get("quantity_after"))
        sale_type = "closed" if (qty_after is not None and qty_after == 0) else ("partial" if (qty_after is not None and qty_after > 0) else infer_sale_type(ident, t_date))
        date_note = "기록일 기준(체결일과 다를 수 있음)" if recorded_only else None

        if qty is None or qty <= 0 or gross is None or gross <= 0:
            if not recorded_only:
                explicit_manual_ident_dates.append((ident, t_day))
            manual_candidates.append({
                "trade_date": t_date,
                "ident": ident,
                "stock_code": code,
                "stock_name": names.get(code) or str(trade.get("stock_name") or code),
                "currency": currency,
                "source": "manual_trade",
                "sale_type": sale_type,
                "price_basis": "execution",
                "date_is_recorded_at": recorded_only,
                "date_note": date_note,
                "quantity": qty,
                "sell_price": None,
                "sell_amount": None,
                "status": "uncalculated",
                "reason": "매도 수량·체결가 미확인",
            })
            continue

        key = (t_date, ident, currency, recorded_only)
        bucket = manual_grouped.get(key)
        if bucket is None:
            manual_grouped[key] = {
                "trade_date": t_date,
                "stock_code": code,
                "ident": ident,
                "stock_name": str(trade.get("stock_name") or code),
                "currency": currency,
                "quantity": qty,
                "sell_amount": gross,
                "sale_type": sale_type,
                "date_is_recorded_at": recorded_only,
                "date_note": date_note,
            }
        else:
            bucket["quantity"] += qty
            bucket["sell_amount"] += gross
            if sale_type == "closed":
                bucket["sale_type"] = "closed"

    for bucket in manual_grouped.values():
        t_date = bucket["trade_date"]
        ident = bucket["ident"]
        if not bucket["date_is_recorded_at"]:
            explicit_manual_ident_dates.append((ident, _iso_date(t_date)))  # type: ignore[arg-type]
        qty = bucket["quantity"]
        amt = bucket["sell_amount"]
        manual_candidates.append({
            "trade_date": t_date,
            "ident": ident,
            "stock_code": bucket["stock_code"],
            "stock_name": names.get(bucket["stock_code"]) or bucket["stock_name"],
            "currency": bucket["currency"],
            "source": "manual_trade",
            "sale_type": bucket["sale_type"],
            "price_basis": "execution",
            "date_is_recorded_at": bucket["date_is_recorded_at"],
            "date_note": bucket["date_note"],
            "quantity": _round(qty, 6),
            "sell_price": amt / qty,
            "sell_amount": _round(amt, 2),
            "status": "ok",
            "reason": None,
        })

    # 2) 스냅샷 보유 소멸 (NH 원장 또는 체결일이 명시된 수동 매매가 없을 때 생성;
    #    체결일 없는 기록일 기준 수동 매매와 인접일로 겹치면 스냅샷 소멸을 우선한다.)
    def is_covered_by_confirmed_trade(ident: str, prev_date: str, curr_date: str) -> bool:
        if ident in pending_nh_idents:
            return True
        p_day = _iso_date(prev_date)
        c_day = _iso_date(curr_date)
        if not p_day or not c_day:
            return False
        low_bound = p_day - timedelta(days=3)
        high_bound = c_day + timedelta(days=2)
        return any(
            cid == ident and low_bound <= d <= high_bound
            for cid, d in (*nh_ident_dates, *explicit_manual_ident_dates)
        )

    snap_events: list[dict[str, Any]] = []
    for prev_d, curr_d in zip(snap_dates, snap_dates[1:]):
        if curr_d < effective_since:
            continue
        prev_map = snap_by_date[prev_d]
        curr_map = snap_by_date[curr_d]
        for ident, prev_info in prev_map.items():
            prev_qty = prev_info["quantity"]
            prev_mv = prev_info["market_value"]
            was_held = (prev_qty is not None and prev_qty > 0) or (prev_qty is None and prev_mv > 0)
            if not was_held:
                continue
            curr_info = curr_map.get(ident)
            now_held = curr_info is not None and (
                (curr_info["quantity"] is not None and curr_info["quantity"] > 0)
                or (curr_info["quantity"] is None and curr_info["market_value"] > 0)
            )
            if now_held or is_covered_by_confirmed_trade(ident, prev_d, curr_d):
                continue
            code = prev_info["code"]
            currency = "KRW" if is_korean_stock(code) or is_special_asset(code) else "USD"
            c_day = _iso_date(curr_d)
            if _weekdays_between(prev_d, curr_d) > 2:
                # 정산 공백이 길어 매도일을 모르면 기록일 기준 수동 매매가 인접해 있을 때 수동 매매를 유지한다.
                has_adjacent_recorded_manual = bool(
                    c_day and any(
                        mc["ident"] == ident and mc["date_is_recorded_at"]
                        and abs(((_iso_date(mc["trade_date"]) or c_day) - c_day).days) <= 5
                        for mc in manual_candidates
                    )
                )
                if not has_adjacent_recorded_manual:
                    snap_events.append({
                        "trade_date": curr_d,
                        "stock_code": code,
                        "stock_name": names.get(code) or code,
                        "currency": currency,
                        "source": "snapshot_disappearance",
                        "sale_type": "closed",
                        "price_basis": "close_estimate",
                        "date_is_recorded_at": False,
                        "date_note": None,
                        "quantity": prev_qty,
                        "sell_price": None,
                        "sell_amount": None,
                        "status": "uncalculated",
                        "reason": "정산 공백으로 정확한 매도일 미상",
                    })
            else:
                if c_day:
                    valid_snap_ident_dates.append((ident, c_day))
                snap_events.append({
                    "trade_date": curr_d,
                    "stock_code": code,
                    "stock_name": names.get(code) or code,
                    "currency": currency,
                    "source": "snapshot_disappearance",
                    "sale_type": "closed",
                    "price_basis": "close_estimate",
                    "date_is_recorded_at": False,
                    "date_note": None,
                    "quantity": prev_qty,
                    "sell_price": None,
                    "sell_amount": None,
                    "status": "ok",
                    "reason": None,
                })

    # 3) 수동 매매 확정: 체결일 미기재(기록일 기준) 건이 스냅샷 소멸과 인접일(±5일)로 겹치면 스냅샷 소멸을 우선한다.
    for mc in manual_candidates:
        ident = mc["ident"]
        t_day = _iso_date(mc["trade_date"])
        if mc["date_is_recorded_at"] and t_day:
            if any(cid == ident and abs((sd - t_day).days) <= 5 for cid, sd in valid_snap_ident_dates):
                continue
        clean_mc = {k: v for k, v in mc.items() if k != "ident"}
        events.append(clean_mc)

    events.extend(snap_events)
    events.sort(key=lambda e: (e["trade_date"], e["stock_code"]), reverse=True)
    return events


def _uncalc_horizon(reason: str) -> dict[str, Any]:
    return {
        "status": "uncalculated",
        "date": None,
        "price": None,
        "change_pct": None,
        "diff_amount": None,
        "reason": reason,
        "outcome": None,
        "outcome_label": None,
    }


def _bar_contains_price(bar: dict[str, Any], price: float, *, tolerance: float) -> bool:
    close = _finite(bar.get("close"))
    lo = _finite(bar.get("low"))
    hi = _finite(bar.get("high"))
    if lo is not None and hi is not None and lo > 0 and hi > 0:
        low_b = min(lo, hi) * (1.0 - tolerance)
        high_b = max(lo, hi) * (1.0 + tolerance)
        return low_b <= price <= high_b
    if close is not None and close > 0:
        return close * (1.0 - max(tolerance, 0.05)) <= price <= close * (1.0 + max(tolerance, 0.05))
    return False


def _match_sale_bar_index(
    *,
    code: str,
    bars: list[dict[str, Any]],
    trade_date: str,
    sell_price: float | None,
    price_basis: str,
) -> tuple[int | None, str | None]:
    """매도 기준 거래일 봉을 찾는다.

    - 스냅샷 소멸(종가 기준): 소멸 정산일 당일 봉만 허용.
    - 국내 체결: trade_date 당일 봉이 있어야 하며 체결가가 당일 고저 범위(시간외 감안 여유 포함)에 들어야 한다.
    - 해외 체결: 시차로 인해 KST 주문일(trade_date) 봉과 직전 거래일 봉을 순서대로 검사해 체결가가 고저 범위에 드는 봉을 택하고, 둘 다 아니면 미계산 처리한다.
    """
    t_day = _iso_date(trade_date)
    if t_day is None or not bars:
        return None, "매도일 일봉 데이터 미확보"

    exact_idx: int | None = None
    prev_idx: int | None = None
    for idx, bar in enumerate(bars):
        b_date = str(bar.get("date") or "")[:10]
        b_day = _iso_date(b_date)
        if b_day is None:
            continue
        if b_date == trade_date:
            exact_idx = idx
            break
        if b_day < t_day and (t_day - b_day).days <= 6:
            prev_idx = idx
        elif b_day > t_day:
            break

    if price_basis == "close_estimate":
        if exact_idx is None:
            return None, "소멸 정산일 일봉 데이터 미확보"
        return exact_idx, None

    if sell_price is None or sell_price <= 0:
        return None, "매도 단가 미확인"

    if is_korean_stock(code):
        if exact_idx is None:
            return None, "매도일 일봉 데이터 미확보"
        if not _bar_contains_price(bars[exact_idx], sell_price, tolerance=KRX_TRADE_BAR_TOLERANCE):
            return None, "분할·수정주가 기준 불일치"
        return exact_idx, None

    # 해외 종목: trade_date 봉과 직전 거래일 봉 중 체결가가 고저 범위에 드는 봉을 기준으로 선택
    candidates = [i for i in (exact_idx, prev_idx) if i is not None]
    if not candidates:
        return None, "매도일·직전 거래일 일봉 미확보"
    for cand_idx in candidates:
        if _bar_contains_price(bars[cand_idx], sell_price, tolerance=OVERSEAS_BAR_EPSILON):
            return cand_idx, None
    return None, "매도일·직전 거래일 고저 범위 불일치"


def _interval_corporate_issue(
    code: str,
    bars: list[dict[str, Any]],
    start_idx: int,
    end_idx: int,
    corp: dict[str, Any] | None,
) -> str | None:
    """(bars[start_idx].date, bars[end_idx].date] 구간의 배당락·분할·주식수 급변을 확인한다."""
    start_date = str(bars[start_idx]["date"])[:10]
    end_date = str(bars[end_idx]["date"])[:10]

    prev_close = _finite(bars[start_idx].get("close"))
    prev_shares = _finite(bars[start_idx].get("listed_shares"))
    is_kr = is_korean_stock(code)
    for idx in range(start_idx + 1, end_idx + 1):
        bar = bars[idx]
        close = _finite(bar.get("close"))
        if is_kr and close and close > 0 and prev_close and prev_close > 0:
            ratio = close / prev_close
            if ratio < KRX_DAILY_RATIO_MIN or ratio > KRX_DAILY_RATIO_MAX:
                return "분할·권리락 의심 구간"
        if close and close > 0:
            prev_close = close
        shares = _finite(bar.get("listed_shares"))
        if shares and shares > 0:
            if prev_shares and prev_shares > 0:
                s_ratio = shares / prev_shares
                if s_ratio < 0.92 or s_ratio > 1.08:
                    return "상장주식수 변동(증자·분할) 구간"
            prev_shares = shares

    if end_date <= start_date:
        return None

    if not corp or not corp.get("verified"):
        return "배당락·분할 여부 확인 전"

    for split_d in corp.get("split_dates") or ():
        if start_date < split_d <= end_date:
            return "액면분할·병합 구간"

    for ex_d in corp.get("ex_dates") or ():
        if start_date < ex_d <= end_date:
            return "배당락 구간"

    return None


def _evaluate_horizon(
    *,
    code: str,
    bars: list[dict[str, Any]],
    start_idx: int,
    target_idx: int,
    sell_price: float,
    quantity: float | None,
    corp: dict[str, Any] | None,
) -> dict[str, Any]:
    issue = _interval_corporate_issue(code, bars, start_idx, target_idx, corp)
    if issue:
        return _uncalc_horizon(issue)
    target_bar = bars[target_idx]
    target_price = _finite(target_bar.get("close"))
    if target_price is None or target_price <= 0:
        return _uncalc_horizon("종가 데이터 미확보")
    change_pct = _round((target_price / sell_price - 1.0) * 100.0, 2)
    diff_amt = _round((target_price - sell_price) * quantity, 2) if (quantity is not None and quantity > 0) else None
    if change_pct is None:
        return _uncalc_horizon("수익률 계산 불가")
    if change_pct > 0:
        outcome, outcome_label = "rose_after", "매도 후 상승"
    elif change_pct < 0:
        outcome, outcome_label = "avoided_drop", "하락 회피"
    else:
        outcome, outcome_label = "flat", "보합"
    return {
        "status": "ok",
        "date": str(target_bar.get("date") or "")[:10],
        "price": _round(target_price, 4),
        "change_pct": change_pct,
        "diff_amount": diff_amt,
        "reason": None,
        "outcome": outcome,
        "outcome_label": outcome_label,
    }


def evaluate_post_sales(
    *,
    sale_events: list[dict[str, Any]],
    daily_bars: dict[str, list[dict[str, Any]]],
    corporate_events: dict[str, dict[str, Any]],
    since: str,
) -> dict[str, Any]:
    """매도 사건 목록에 5거래일·20거래일·현재가를 결합하고 건수 요약을 만든다."""
    items_out: list[dict[str, Any]] = []
    total_count = len(sale_events)
    calculable_any = 0
    uncalc_sale = 0
    matured_5d = calculable_5d = 0
    matured_20d = calculable_20d = uncalc_20d = pending_20d = 0
    avoided_20d = rose_20d = flat_20d = 0

    for raw in sale_events:
        code = raw.get("stock_code") or ""
        source = raw.get("source") or "nh_realized_pnl"
        sale_type = raw.get("sale_type") or "unknown"
        price_basis = raw.get("price_basis") or "execution"
        base_item = {
            "trade_date": raw["trade_date"],
            "matched_bar_date": None,
            "stock_code": code,
            "stock_name": raw.get("stock_name") or code or "종목 미확인",
            "currency": raw.get("currency") or "KRW",
            "source": source,
            "source_label": SOURCE_LABELS.get(source, source),
            "sale_type": sale_type,
            "sale_type_label": SALE_TYPE_LABELS.get(sale_type, "매도"),
            "price_basis": price_basis,
            "date_is_recorded_at": bool(raw.get("date_is_recorded_at")),
            "date_note": raw.get("date_note"),
            "quantity": _finite(raw.get("quantity")),
            "sell_price": _round(_finite(raw.get("sell_price")), 4),
            "sell_amount": _round(_finite(raw.get("sell_amount")), 2),
        }

        if raw.get("status") == "uncalculated":
            reason = raw.get("reason") or "매도 내역 미확인"
            uncalc_sale += 1
            items_out.append({
                **base_item,
                "status": "uncalculated",
                "reason": reason,
                "day5": _uncalc_horizon(reason),
                "day20": _uncalc_horizon(reason),
                "current": _uncalc_horizon(reason),
            })
            continue

        if is_special_asset(code):
            reason = "특수자산 일봉 추적 미제공"
            uncalc_sale += 1
            items_out.append({
                **base_item,
                "status": "uncalculated",
                "reason": reason,
                "day5": _uncalc_horizon(reason),
                "day20": _uncalc_horizon(reason),
                "current": _uncalc_horizon(reason),
            })
            continue

        bars = daily_bars.get(code) or []
        sell_price = _finite(raw.get("sell_price"))
        qty = _finite(raw.get("quantity"))

        idx0, bar_err = _match_sale_bar_index(
            code=code,
            bars=bars,
            trade_date=raw["trade_date"],
            sell_price=sell_price,
            price_basis=price_basis,
        )
        if idx0 is None:
            reason = bar_err or "매도일 일봉 데이터 미확보"
            uncalc_sale += 1
            items_out.append({
                **base_item,
                "status": "uncalculated",
                "reason": reason,
                "day5": _uncalc_horizon(reason),
                "day20": _uncalc_horizon(reason),
                "current": _uncalc_horizon(reason),
            })
            continue

        bar0 = bars[idx0]
        base_item["matched_bar_date"] = str(bar0.get("date") or "")[:10]
        bar0_close = _finite(bar0.get("close"))

        if sell_price is None and price_basis == "close_estimate":
            if bar0_close and bar0_close > 0:
                sell_price = bar0_close
                base_item["sell_price"] = _round(sell_price, 4)
                if qty and qty > 0:
                    base_item["sell_amount"] = _round(sell_price * qty, 2)
                if raw.get("currency") and bar0.get("currency"):
                    base_item["currency"] = str(bar0["currency"])

        if sell_price is None or sell_price <= 0:
            reason = "매도 단가 미확인"
            uncalc_sale += 1
            items_out.append({
                **base_item,
                "status": "uncalculated",
                "reason": reason,
                "day5": _uncalc_horizon(reason),
                "day20": _uncalc_horizon(reason),
                "current": _uncalc_horizon(reason),
            })
            continue

        corp = corporate_events.get(code)
        elapsed = len(bars) - 1 - idx0

        if idx0 + 5 < len(bars):
            matured_5d += 1
            day5 = _evaluate_horizon(
                code=code, bars=bars, start_idx=idx0, target_idx=idx0 + 5,
                sell_price=sell_price, quantity=qty, corp=corp,
            )
            if day5["status"] == "ok":
                calculable_5d += 1
        else:
            day5 = {
                "status": "pending",
                "date": None,
                "price": None,
                "change_pct": None,
                "diff_amount": None,
                "elapsed_days": max(0, elapsed),
                "target_days": 5,
                "reason": f"5거래일 미도래 ({max(0, elapsed)}/5일)",
                "outcome": None,
                "outcome_label": None,
            }

        if idx0 + 20 < len(bars):
            matured_20d += 1
            day20 = _evaluate_horizon(
                code=code, bars=bars, start_idx=idx0, target_idx=idx0 + 20,
                sell_price=sell_price, quantity=qty, corp=corp,
            )
            if day20["status"] == "ok":
                calculable_20d += 1
                if day20["outcome"] == "avoided_drop":
                    avoided_20d += 1
                elif day20["outcome"] == "rose_after":
                    rose_20d += 1
                else:
                    flat_20d += 1
            else:
                uncalc_20d += 1
        else:
            pending_20d += 1
            day20 = {
                "status": "pending",
                "date": None,
                "price": None,
                "change_pct": None,
                "diff_amount": None,
                "elapsed_days": max(0, elapsed),
                "target_days": 20,
                "reason": f"20거래일 미도래 ({max(0, elapsed)}/20일)",
                "outcome": None,
                "outcome_label": None,
            }

        current = _evaluate_horizon(
            code=code, bars=bars, start_idx=idx0, target_idx=len(bars) - 1,
            sell_price=sell_price, quantity=qty, corp=corp,
        )

        any_ok = any(h["status"] == "ok" for h in (day5, day20, current))
        if any_ok:
            calculable_any += 1
        row_status = "ok" if any_ok else ("pending" if day20["status"] == "pending" and current["status"] != "uncalculated" else "uncalculated")
        row_reason = None if any_ok else (day20.get("reason") or current.get("reason"))

        items_out.append({
            **base_item,
            "status": row_status,
            "reason": row_reason,
            "day5": day5,
            "day20": day20,
            "current": current,
        })

    return {
        "since": since,
        "summary": {
            "total_count": total_count,
            "calculable_any_count": calculable_any,
            "uncalculated_sale_count": uncalc_sale,
            "matured_5d_count": matured_5d,
            "calculable_5d_count": calculable_5d,
            "matured_20d_count": matured_20d,
            "calculable_20d_count": calculable_20d,
            "uncalculated_20d_count": uncalc_20d,
            "pending_20d_count": pending_20d,
            "avoided_drop_20d_count": avoided_20d,
            "rose_after_20d_count": rose_20d,
            "flat_20d_count": flat_20d,
        },
        "items": items_out,
        "notes": list(POST_SALE_NOTES),
    }
