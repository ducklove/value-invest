"""해외주식 양도세와 금융소득 누계의 순수 추정 계산. 신고용이 아니다.

세율·공제액은 시행일별 표로 둔다. 표의 첫 시행일은 이 앱이 계산하는 구간의 시작이며 법 제정일이 아니다.
세법이 바뀌면 새 시행일 행을 추가한다. RULES_REVIEWED_THROUGH를 넘은 연도는 마지막 행을 쓰되 화면에 미확인으로 알린다.
"""

from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal

from domain.dividend_entitlement import holding_identity

# NH 기간 손익 응답의 필드 의미(원화 기준 금액의 단위·환율, 실현손익 산식)를 운영 응답으로 확인하면 True로 바꾼다.
# False인 동안 화면과 액션 보드는 세액을 미검증 어림값으로만 표시한다. True여도 신고용 확정값은 아니다.
NH_PNL_FIELDS_VERIFIED = False
RULES_REVIEWED_THROUGH = 2026
# 2,000만원 근접 판단 비율. 넘으면 액션 보드에 올린다.
NEAR_THRESHOLD_RATIO = Decimal("0.8")


@dataclass(frozen=True)
class CapitalGainsRule:
    effective_from: date
    deduction_krw: int
    rate: Decimal  # 지방소득세 포함


@dataclass(frozen=True)
class FinancialIncomeRule:
    effective_from: date
    threshold_krw: int
    interest_withholding_rate: Decimal  # 지방소득세 포함


OVERSEAS_CAPITAL_GAINS_RULES = (CapitalGainsRule(date(2020, 1, 1), 2_500_000, Decimal("0.22")),)
FINANCIAL_INCOME_RULES = (FinancialIncomeRule(date(2013, 1, 1), 20_000_000, Decimal("0.154")),)


def rule_for(rules: tuple, year: int):
    """그 해 말일에 시행 중인 가장 최근 행. 연 단위 합계에 적용하므로 연말 기준으로 고른다."""
    applicable = [rule for rule in rules if rule.effective_from <= date(year, 12, 31)]
    return max(applicable, key=lambda rule: rule.effective_from) if applicable else None


def _decimal(value) -> Decimal:
    return Decimal(str(value or 0))


def _won(value: Decimal) -> int:
    return int(value.quantize(Decimal(1), rounding=ROUND_HALF_UP))


def capital_gains(realized_krw, year: int, *, rules: tuple = OVERSEAS_CAPITAL_GAINS_RULES) -> dict | None:
    """연간 해외 실현손익(손익 통산 후)에 공제와 세율을 적용한다. 표에 없는 연도는 None."""
    rule = rule_for(rules, year)
    if rule is None:
        return None
    gain = max(_decimal(realized_krw), Decimal(0))
    used = min(gain, Decimal(rule.deduction_krw))
    taxable = gain - used
    return {"deduction_krw": rule.deduction_krw, "deduction_used_krw": _won(used), "deduction_remaining_krw": _won(rule.deduction_krw - used),
            "taxable_krw": _won(taxable), "rate_pct": float(rule.rate * 100), "estimated_tax_krw": _won(taxable * rule.rate),
            "effective_from": rule.effective_from.isoformat()}


def loss_harvest(realized_krw, positions: list[dict], year: int, *, rules: tuple = OVERSEAS_CAPITAL_GAINS_RULES) -> dict | None:
    """평가손실을 올해 실현한다고 가정할 때 줄어드는 세액. 종목별은 그 종목만 팔 때, 합계는 전부 팔 때다."""
    base = capital_gains(realized_krw, year, rules=rules)
    if base is None:
        return None
    current_tax = base["estimated_tax_krw"]
    realized = _decimal(realized_krw)
    losers = sorted((row for row in positions if _decimal(row.get("unrealized_krw")) < 0), key=lambda row: _decimal(row["unrealized_krw"]))
    candidates = []
    for row in losers:
        after = capital_gains(realized + _decimal(row["unrealized_krw"]), year, rules=rules)["estimated_tax_krw"]
        candidates.append({**row, "unrealized_krw": _won(_decimal(row["unrealized_krw"])), "tax_saving_krw": current_tax - after})
    total_loss = sum((_decimal(row["unrealized_krw"]) for row in losers), Decimal(0))
    after_all = capital_gains(realized + total_loss, year, rules=rules)["estimated_tax_krw"]
    return {"current_tax_krw": current_tax, "total_loss_krw": _won(total_loss), "max_saving_krw": current_tax - after_all,
            "tax_after_all_krw": after_all, "loss_to_zero_tax_krw": base["taxable_krw"], "candidates": candidates}


def gross_interest(net_krw, year: int, *, rules: tuple = FINANCIAL_INCOME_RULES) -> int:
    """세후로만 남은 이자를 세전으로 되돌린다(금융소득 기준은 세전). 표에 없는 연도는 세후 그대로."""
    rule = rule_for(rules, year)
    net = _decimal(net_krw)
    return _won(net / (1 - rule.interest_withholding_rate) if rule else net)


def financial_income(year: int, *, dividends_krw, interest_krw, upcoming_krw=0, unscheduled_krw=0,
                     rules: tuple = FINANCIAL_INCOME_RULES) -> dict | None:
    """세전 금융소득 누계. 지급일 미확인 일정은 합계에 넣지 않고 근접 판단에만 쓴다."""
    rule = rule_for(rules, year)
    if rule is None:
        return None
    threshold = Decimal(rule.threshold_krw)
    received = _decimal(dividends_krw) + _decimal(interest_krw)
    projected = received + _decimal(upcoming_krw)
    upper = projected + _decimal(unscheduled_krw)
    status = "over" if projected > threshold else "near" if upper >= threshold * NEAR_THRESHOLD_RATIO else "ok"
    return {"threshold_krw": rule.threshold_krw, "dividends_krw": _won(_decimal(dividends_krw)), "interest_krw": _won(_decimal(interest_krw)),
            "received_krw": _won(received), "upcoming_krw": _won(_decimal(upcoming_krw)), "unscheduled_krw": _won(_decimal(unscheduled_krw)),
            "projected_krw": _won(projected), "remaining_krw": _won(max(threshold - projected, Decimal(0))),
            "ratio_pct": float((projected / threshold * 100).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)), "status": status,
            "effective_from": rule.effective_from.isoformat()}


def upcoming_income(events: list[dict], nh_quantities: dict[str, float], today: date) -> dict:
    """배당 캘린더의 올해 남은 일정을 NH 보유 몫과 나머지로 나눈다.

    일정 금액은 전 계좌 합산 수량 기준이다. 받은 배당이 NH 입금만이므로 합계에도 NH 몫만 넣고 나머지는 따로 보여 준다.
    이미 입금과 연결된 행은 받은 배당에 있으므로 뺀다. 지급일이 없는 행(배당락·기준일)은 올해 지급인지 알 수 없어 분리한다.
    """
    scheduled = unscheduled = other = Decimal(0)
    counts = {"scheduled": 0, "unscheduled": 0, "estimated": 0, "unknown_amount": 0}
    for event in events:
        day = str(event.get("date") or "")
        if event.get("nh_match") or event.get("date_status") == "nh" or not today.isoformat() < day or day[:4] != str(today.year):
            continue
        amount = event.get("expected_amount_krw")
        if amount is None:
            counts["unknown_amount"] += 1
            continue
        shares = float(event.get("shares") or 0)
        ratio = min(1.0, float(nh_quantities.get(holding_identity(event.get("stock_code")), 0)) / shares) if shares > 0 else 0.0
        mine = _decimal(amount) * Decimal(str(ratio))
        other += _decimal(amount) - mine
        if mine <= 0:
            continue
        if event.get("cashflow"):
            scheduled += mine
            counts["scheduled"] += 1
            counts["estimated"] += event.get("date_status") == "estimated"
        else:
            unscheduled += mine
            counts["unscheduled"] += 1
    return {"upcoming_krw": _won(scheduled), "unscheduled_krw": _won(unscheduled), "other_accounts_krw": _won(other), **counts}


def summarize_year(rows: list[dict]) -> dict:
    """한 해의 원장 행을 시장별·종목별로 합친다. 원화 손익을 확인하지 못한 행은 합계에서 빼고 센다."""
    result = {market: {"realized_krw": 0.0, "rows": 0, "excluded_rows": 0, "review_rows": 0, "pending_rows": 0, "by_currency": {}}
              for market in ("domestic", "overseas")}
    stocks: dict[tuple, dict] = {}
    for row in rows:
        target = result[row["market"]]
        issues = set(row.get("issues") or ())
        target["rows"] += 1
        target["pending_rows"] += "detail_pending" in issues
        target["review_rows"] += bool(issues & {"arith_mismatch", "native_missing"})
        krw, native = row.get("realized_krw"), row.get("realized_native")
        if krw is None:
            target["excluded_rows"] += 1
        else:
            target["realized_krw"] += krw
        if native is not None and row.get("currency") != "KRW":
            target["by_currency"][row["currency"]] = target["by_currency"].get(row["currency"], 0.0) + native
        key = (row["market"], row.get("stock_code") or row.get("symbol") or "", row.get("currency"))
        stock = stocks.setdefault(key, {"market": row["market"], "stock_code": key[1], "stock_name": row.get("stock_name") or "",
                                        "currency": row.get("currency"), "realized_krw": None, "realized_native": None, "rows": 0})
        stock["rows"] += 1
        if krw is not None:
            stock["realized_krw"] = (stock["realized_krw"] or 0.0) + krw
        if native is not None:
            stock["realized_native"] = (stock["realized_native"] or 0.0) + native
    for market in result.values():
        market["realized_krw"] = round(market["realized_krw"], 2)
        market["by_currency"] = [{"currency": currency, "realized_native": round(amount, 3)} for currency, amount in sorted(market["by_currency"].items())]
    ordered = sorted(stocks.values(), key=lambda stock: abs(stock["realized_krw"] or 0), reverse=True)
    return {**result, "stocks": [{**stock, "realized_krw": round(stock["realized_krw"], 2) if stock["realized_krw"] is not None else None} for stock in ordered]}


def approx_krw(value) -> str:
    """미검증 금액의 어림 표기. 정확한 숫자로 읽히지 않게 만원 단위로 줄인다."""
    amount = _decimal(value)
    if not amount:
        return "0원"
    sign = "-" if amount < 0 else ""
    amount = abs(amount)
    if amount >= 10000:
        return f"약 {sign}{_won(amount / 10000):,}만원"
    return f"약 {sign}{_won(amount / 100) * 100:,}원"
