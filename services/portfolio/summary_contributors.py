"""요약 카드가 같은 정산 기준에서 실시간 종목 손익을 계산하는 데 필요한 자료."""

from datetime import datetime, timezone

from domain.timeutil import KST
from repositories import portfolio_trades
from services.portfolio import fx
from services.portfolio.time_windows import settlement_marker_seconds


async def baseline_details(user: str, snapshot: dict, stocks: list[dict]) -> dict:
    cutoff = snapshot.get("cashflow_cutoff_at") or settlement_marker_seconds(snapshot["date"])
    dt = datetime.fromisoformat(cutoff)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=KST)
    trades = await portfolio_trades.trades_since(user, dt.astimezone(timezone.utc).isoformat())
    flows = {}
    for trade in trades:
        code = trade["stock_code"]
        row = flows.setdefault(code, {
            "stock_name": trade["stock_name"], "currency": trade["currency"],
            "quantity_change": 0.0, "cash_change": 0.0, "buy_amount": 0.0,
            "fx_rate": fx.cached_rate_for_currency(trade["currency"]), "comparable": True,
        })
        if trade["currency"] != row["currency"]:
            row["comparable"] = False
        row["quantity_change"] += trade["quantity"] * (1 if trade["side"] == "buy" else -1)
        row["cash_change"] += trade["cash_change"]
        if trade["side"] == "buy":
            row["buy_amount"] -= trade["cash_change"]
    return {
        "stock_positions": {s["stock_code"]: {
            "quantity": s.get("quantity"), "group_name": s.get("group_name"),
            "currency": s.get("currency"),
        } for s in stocks},
        "stock_trade_flows": flows,
    }
