"""화면별 정산 기준선과 입출금 조회. 한 응답은 같은 DB 스냅샷을 사용한다."""

from repositories import snapshots
from repositories.db import read_snapshot
from services.portfolio.time_windows import settlement_marker_seconds


async def net_cashflow_since_snapshot(user: str, snap_date: str) -> tuple[float, dict[str, float]]:
    rows = await snapshots.get_cashflows_created_after(user, settlement_marker_seconds(snap_date))
    net = sum(row["amount"] if row["type"] == "deposit" else -row["amount"]
              for row in rows if row["type"] in {"deposit", "withdrawal"})
    return net, {"CASH_KRW": net} if net else {}


@read_snapshot()
async def previous_day(user: str, baseline_date: str) -> dict:
    snapshot = await snapshots.get_snapshot_on_or_before(user, baseline_date) or {}
    snap_date = snapshot.get("date")
    stocks = await snapshots.get_stock_snapshots_exact_date(user, snap_date) if snap_date else []
    marker = settlement_marker_seconds(snap_date) if snap_date else baseline_date
    rows = await snapshots.get_cashflows_created_after(user, marker)
    cashflows = []
    net = 0.0
    for row in rows:
        signed = row["amount"] if row["type"] == "deposit" else -row["amount"] if row["type"] == "withdrawal" else 0
        net += signed
        if signed:
            cashflows.append({**row, "signed_amount": signed})
    return {
        "date": snap_date, "total_value": snapshot.get("total_value"),
        "fx_usdkrw": snapshot.get("fx_usdkrw"), "nav": snapshot.get("nav"),
        "stock_values": {s["stock_code"]: s["market_value"] for s in stocks},
        "today_net_cashflow": net,
        "today_cashflows_by_stock": {"CASH_KRW": net} if cashflows else {},
        "today_cashflows": cashflows,
    }


@read_snapshot()
async def period_start(user: str, *, yearly: bool = False) -> dict:
    snapshot = await (snapshots.get_year_start_snapshot(user) if yearly else snapshots.get_month_end_snapshot(user))
    result = dict(snapshot) if snapshot else {}
    result["stock_values"] = {}
    if snapshot and snapshot.get("date"):
        # 휴일·정산 누락으로 이전 날짜를 택해도 합계와 종목별 금액의 기준일은 같다.
        stocks = await snapshots.get_stock_snapshots_exact_date(user, snapshot["date"])
        result["stock_values"] = {s["stock_code"]: s["market_value"] for s in stocks}
        result["net_cashflow"], result["cashflows_by_stock"] = await net_cashflow_since_snapshot(user, snapshot["date"])
    return result


@read_snapshot()
async def intraday(user: str, axis_start: str, axis_end: str) -> list[dict]:
    points = await snapshots.get_intraday_snapshots_between(user, axis_start, axis_end)
    snapshot = await snapshots.get_snapshot_on_or_before(user, axis_start[:10])
    if snapshot and snapshot["total_value"]:
        points = [{"ts": axis_start, "total_value": snapshot["total_value"]}] + points
    return points
