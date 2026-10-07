"""화면별 정산 기준선과 입출금 조회. 한 응답은 같은 DB 스냅샷을 사용한다."""

from datetime import date, timedelta

from repositories import snapshots
from repositories.db import read_snapshot
from services.portfolio import nav_link, summary_contributors
from services.portfolio.time_windows import settlement_marker_seconds


@read_snapshot()
async def regular_performance(user: str, day: str) -> dict | None:
    current = await snapshots.get_snapshot_by_date(user, day)
    if not current or current.get("price_basis") != "regular_close_v1":
        return None
    previous = await snapshots.get_latest_snapshot_before_date(user, day)
    link_factor = None
    if previous and previous.get("price_basis") != current["price_basis"]:
        # 새 기준 첫 날: 이전 구간 마지막 정산과 NAV 를 연결해 비교한다(nav_link 와 같은
        # 규칙 — 연결 이력의 전환일 수익률과 같은 값). 연결할 수 없을 때만 비교 보류.
        net = await nav_link.boundary_net_cashflow(user, previous, current)
        link_factor = nav_link.link_factor(previous, current, net)
        comparable = link_factor is not None
        if comparable:
            previous = {**previous, "return_nav": previous["return_nav"] * link_factor}
    else:
        comparable = previous is not None
        flows = await snapshots.get_cashflows(user)
        flows = [r for r in flows if r.get("applied_snapshot_date") == day]
        net = sum(r["amount"] * (1 if r["type"] == "deposit" else -1) for r in flows)
    pnl = current["total_value"] - previous["total_value"] - net if comparable else None
    pct = (current["return_nav"] / previous["return_nav"] - 1) * 100 if comparable and previous["return_nav"] > 0 else None
    after = await snapshots.get_cashflows_created_after(user, current["cashflow_cutoff_at"])
    after_net = sum(r["amount"] * (1 if r["type"] == "deposit" else -1) for r in after)
    usd_change = usd_pct = usd_value_change = None
    current_fx, previous_fx = current.get("fx_usdkrw"), (previous or {}).get("fx_usdkrw")
    if comparable and current_fx and previous_fx:
        usd_value_change = current["total_value"] / current_fx - previous["total_value"] / previous_fx
        usd_change = usd_value_change - net / current_fx
        if previous["return_nav"] > 0:
            usd_pct = (current["return_nav"] / current_fx / (previous["return_nav"] / previous_fx) - 1) * 100
    return {**current, "prev_date": previous["date"] if comparable else None,
            "prev_value": previous["total_value"] if comparable else None,
            "change_krw": pnl, "change_pct": pct, "net_cashflow": net,
            "change_usd": usd_change, "change_usd_pct": usd_pct, "value_change_usd": usd_value_change,
            "after_close_net_cashflow": after_net, "source": "regular_close",
            "comparison_unavailable": not comparable,
            "prev_nav_link_factor": link_factor}


async def net_cashflow_since_snapshot(user: str, snap_date: str) -> tuple[float, dict[str, float]]:
    rows = await snapshots.get_cashflows_created_after(user, settlement_marker_seconds(snap_date))
    by_stock = {}
    for row in rows:
        signed = row["amount"] if row["type"] == "deposit" else -row["amount"]
        code = row.get("cash_code", "CASH_KRW")
        by_stock[code] = by_stock.get(code, 0) + signed
    return sum(by_stock.values()), by_stock


@read_snapshot()
async def previous_day(user: str, baseline_date: str) -> dict:
    snapshot = await snapshots.get_snapshot_on_or_before(user, baseline_date) or {}
    snap_date = snapshot.get("date")
    expected = date.fromisoformat(baseline_date)
    while expected.weekday() >= 5:
        expected -= timedelta(days=1)
    from domain.market_calendar import closing_at
    try:
        while closing_at(expected.isoformat()) is None:
            expected -= timedelta(days=1)
    except ValueError:
        pass  # 미확인 과거 달력이 정산 원자료의 조회까지 막지는 않는다.
    stocks = await snapshots.get_stock_snapshots_exact_date(user, snap_date) if snap_date else []
    marker = settlement_marker_seconds(snap_date) if snap_date else baseline_date
    rows = await snapshots.get_cashflows_created_after(user, marker)
    cashflows = []
    net = 0.0
    by_stock = {}
    for row in rows:
        signed = nav_link.signed_cashflow(row)
        net += signed
        if signed:
            cashflows.append({**row, "signed_amount": signed})
            code = row.get("cash_code", "CASH_KRW")
            by_stock[code] = by_stock.get(code, 0) + signed
    return {
        "regular_close": await regular_performance(user, (date.fromisoformat(baseline_date) + timedelta(days=1)).isoformat()),
        "price_basis": snapshot.get("price_basis"), "cashflow_cutoff_at": snapshot.get("cashflow_cutoff_at"),
        "expected_date": expected.isoformat(),
        "settlement_pending": not snap_date or snap_date < expected.isoformat(),
        "date": snap_date, "total_value": snapshot.get("total_value"),
        "fx_usdkrw": snapshot.get("fx_usdkrw"), "nav": snapshot.get("nav"),
        "return_nav": snapshot.get("return_nav"), "return_factor": snapshot.get("return_factor", 1),
        "stock_values": {s["stock_code"]: s["market_value"] for s in stocks},
        **(await summary_contributors.baseline_details(user, snapshot, stocks) if snap_date else {}),
        "today_net_cashflow": net,
        "today_cashflows_by_stock": by_stock,
        "today_cashflows": cashflows,
    }


@read_snapshot()
async def period_start(user: str, *, yearly: bool = False) -> dict:
    snapshot = await (snapshots.get_year_start_snapshot(user) if yearly else snapshots.get_month_end_snapshot(user))
    # 기준점 뒤에 정산 기준 변경이 있으면 NAV 계열 값만 최신 구간 척도로 연결한다.
    # 금액(total_value)·종목별 금액·입출금은 원래 값 그대로다.
    linked = await nav_link.link_snapshot(user, snapshot)
    if snapshot and linked is None:
        return {"stock_values": {}, "comparison_unavailable": True, "reason": "정산 기준 변경"}
    snapshot = linked
    result = dict(snapshot) if snapshot else {}
    result["stock_values"] = {}
    if snapshot and snapshot.get("date"):
        # 휴일·정산 누락으로 이전 날짜를 택해도 합계와 종목별 금액의 기준일은 같다.
        stocks = await snapshots.get_stock_snapshots_exact_date(user, snapshot["date"])
        result["stock_values"] = {s["stock_code"]: s["market_value"] for s in stocks}
        result.update(await summary_contributors.baseline_details(user, snapshot, stocks))
        result["net_cashflow"], result["cashflows_by_stock"] = await net_cashflow_since_snapshot(user, snapshot["date"])
    return result


@read_snapshot()
async def intraday(user: str, axis_start: str, axis_end: str) -> list[dict]:
    points = await snapshots.get_intraday_snapshots_between(user, axis_start, axis_end)
    snapshot = await snapshots.get_snapshot_on_or_before(user, axis_start[:10])
    if snapshot and snapshot["total_value"]:
        points = [{"ts": axis_start, "total_value": snapshot["total_value"]}] + points
    return points
