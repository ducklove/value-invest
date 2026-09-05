"""일별 동일 수량 구간의 원화 평가손익을 분해하고 잔차를 공개한다.

가격 효과=q*(p1-p0)*r0, 환율 효과=q*p1*(r1-r0).
상호작용은 환율 항목에 포함한다. 체결 원장이 없어 수량 변경 구간은
추정하지 않는다. 실제 수입 분류는 잔고를 다시 늘리지 않는다.
"""

from collections import defaultdict
from datetime import date, timedelta
from math import isclose

from repositories import investment_insights as repo
from repositories import snapshots
from services.portfolio.identifiers import is_korean_stock, is_special_asset
from services.portfolio.theses import number
from services.portfolio.time_windows import today_kst_date


def decompose(nav: list[dict], stocks: list[dict], cashflows: list[dict], income: list[dict]) -> dict:
    first, last = nav[0], nav[-1]
    beginning, ending = number(first["total_value"]), number(last["total_value"])
    start, end = first["date"], last["date"]
    by_day = defaultdict(dict)
    for row in stocks:
        by_day[row["date"]][row["stock_code"]] = row
    totals = {"price": 0.0, "fx": 0.0, "combined": 0.0}
    details = {}
    eligible, examined = 0, 0
    reasons = defaultdict(int)
    for left, right in zip(nav, nav[1:]):
        a, b = by_day[left["date"]], by_day[right["date"]]
        for code in sorted(a.keys() | b.keys()):
            examined += 1
            x, y = a.get(code), b.get(code)
            if x is None or y is None:
                reasons["신규·매도·스냅샷 누락"] += 1
                continue
            q0, q1, v0, v1 = (number(x.get("quantity")), number(y.get("quantity")),
                               number(x.get("unit_price")), number(y.get("unit_price")))
            if None in (q0, q1, v0, v1) or q0 == 0 or not isclose(q0, q1, rel_tol=0, abs_tol=1e-9):
                reasons["수량 변경·가격 정보 누락"] += 1
                continue
            if x.get("priced_from_fallback") or y.get("priced_from_fallback"):
                reasons["대체 시세 사용"] += 1
                continue
            mv0, mv1 = number(x.get("market_value")), number(y.get("market_value"))
            if mv0 is None or mv1 is None or not isclose(mv0, q0*v0, abs_tol=1, rel_tol=1e-6) or not isclose(mv1, q1*v1, abs_tol=1, rel_tol=1e-6):
                reasons["평가액·수량 불일치"] += 1
                continue
            eligible += 1
            row = details.setdefault(code, {"stock_code": code, "stock_name": y.get("stock_name") or code,
                                             "price": 0.0, "fx": 0.0, "combined": 0.0, "intervals": 0})
            currency0, currency1 = x.get("currency"), y.get("currency")
            krw = (currency0 == currency1 == "KRW") or is_korean_stock(code) or is_special_asset(code)
            r0, r1 = number(x.get("fx_rate")), number(y.get("fx_rate"))
            if code == "CASH_KRW":
                price, exchange, combined = 0, 0, 0
            elif code.startswith("CASH_"):
                price, exchange, combined = 0, q0 * (v1 - v0), 0
            elif krw:
                price, exchange, combined = q0 * (v1 - v0), 0, 0
            elif currency0 and currency0 == currency1 and r0 and r1 and r0 > 0 and r1 > 0:
                price = q0 * (v1/r1 - v0/r0) * r0
                exchange, combined = q0 * (v1/r1) * (r1-r0), 0
            else:
                price, exchange, combined = 0, 0, q0 * (v1-v0)
                reasons["환율 이력 미확보"] += 1
            for key, amount in (("price", price), ("fx", exchange), ("combined", combined)):
                row[key] += amount
                totals[key] += amount
            row["intervals"] += 1
    # 신규 입출금은 정산 반영 전까지 과거 손익에서 차감하지 않는다.
    # applied_snapshot_date 도입 이전 자료는 units_change가 있을 때만 기록일을 사용한다.
    flows = [row for row in cashflows
             if (row.get("applied_snapshot_date") or row.get("units_change") is not None or "applied_snapshot_date" not in row)
             and start < str(row.get("applied_snapshot_date") or row["date"]) <= end]
    inflow = sum(float(row["amount"]) for row in flows if row["type"] == "deposit")
    outflow = sum(float(row["amount"]) for row in flows if row["type"] == "withdrawal")
    events = [row for row in income if start < row["date"] <= end]
    dividends = sum(float(row["amount_krw"]) for row in events if row["kind"] == "dividend")
    fees = sum(float(row["amount_krw"]) for row in events if row["kind"] == "fee")
    delta = round(ending - beginning, 2)
    amounts = {"external_flow": round(inflow-outflow, 2), **{key: round(value, 2) for key, value in totals.items()},
               "dividend": round(dividends, 2), "fee": -round(fees, 2)}
    amounts["unclassified"] = round(delta - sum(amounts.values()), 2)
    labels = {"external_flow": "순입출금", "price": "가격 변화", "fx": "환율 변화", "combined": "가격·환율 미분리",
              "dividend": "기록한 배당", "fee": "기록한 수수료·세금", "unclassified": "매매·기타 미분류"}
    return {
        "baseline_date": start, "ending_date": end, "starting_value": beginning, "ending_value": ending,
        "value_change": delta, "investment_pnl": round(delta-amounts["external_flow"], 2),
        "components": [{"key": key, "label": labels[key], "amount": value} for key, value in amounts.items()],
        "reconciliation_error": round(delta-sum(amounts.values()), 2),
        "stocks": sorted([{**row, **{key: round(row[key], 2) for key in totals},
                           "total": round(sum(row[key] for key in totals), 2)} for row in details.values()],
                         key=lambda row: abs(row["total"]), reverse=True),
        "coverage": {"eligible_intervals": eligible, "examined_intervals": examined, "issues": dict(reasons)},
        "income_events": income,
        "notes": ["일별 수량이 같은 구간의 추정 기여분입니다. 장중 매매·분할 등은 체결 원장 없이 구분할 수 없습니다.",
                  "가격은 전일 환율로 계산하고 가격·환율의 상호작용은 환율 효과에 포함합니다.",
                  "배당·비용 기록은 기존 잔고에 반영된 금액을 분류하며 잔고를 변경하지 않습니다. 미기록·시점 차이는 미분류로 남습니다.",
                  "스냅샷 기준 원화 금액 분해이며, 입출금을 중립화한 NAV 수익률과는 별개입니다."],
    }


async def build_attribution(user: str, start: str, end: str) -> dict:
    today = today_kst_date()
    start_date, end_date = date.fromisoformat(start), date.fromisoformat(end)
    if start_date > end_date or end_date > today or (end_date-start_date).days > 3660:
        raise ValueError("기간은 과거부터 오늘까지, 최대 10년으로 지정해 주세요.")
    nav = [row for row in await snapshots.get_nav_history(user)
           if row["date"] <= end and number(row.get("total_value")) is not None]
    before = [row for row in nav if row["date"] < start]
    inside = [row for row in nav if start <= row["date"] <= end]
    if not inside:
        return {"available": False, "message": "선택 기간의 일별 자산 기록이 없습니다.", "income_events": await repo.income_events(user, start, end)}
    baseline = before[-1] if before else inside[0]
    selected = [row for row in nav if baseline["date"] <= row["date"] <= end]
    if len(selected) < 2:
        return {"available": False, "message": "수익 분해에는 서로 다른 날짜의 기록이 2개 이상 필요합니다.", "income_events": await repo.income_events(user, start, end)}
    events = await repo.income_events(user, (date.fromisoformat(baseline["date"])+timedelta(days=1)).isoformat(), end)
    result = decompose(selected, await repo.stock_history(user, baseline["date"], end), await snapshots.get_cashflows(user), events)
    return {"available": True, "requested_start": start, "requested_end": end,
            "baseline_mode": "before_period" if before else "first_in_period", **result}
