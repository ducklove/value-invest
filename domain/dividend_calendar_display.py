"""배당 캘린더의 표시 기준일과 계산 금액. 계좌 입금 금액과 분리한다."""

import re
from datetime import date
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

from domain.dividend_entitlement import holding_identity, krx_record_entitlement_day, market_side
from domain.dividend_receipts import DIVIDEND_TAX_RATES
from domain.portfolio_trades import money_unit, withholding


def dividend_country(code: str, currency: str) -> str:
    """기존 국가별 기본 세율 선택 규칙을 사용한다."""
    code = holding_identity(code)
    if market_side(code) == "KR":
        return "KR"
    if code.endswith((".SS", ".SZ")) or currency == "CNY":
        return "CN"
    if code.endswith(".HK"):
        return "HK"
    if re.fullmatch(r"[A-Z]+([.-][A-Z]+)?", code) and currency in {"USD", "KRW"}:
        return "US"
    return "OTHER"


def _number(value) -> Decimal | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        return None
    return number if number.is_finite() and number >= 0 else None


def display_values(event: dict) -> dict:
    """원본 record_date·ex_date와 입금 근거는 유지하고 화면용 값만 만든다."""
    code = event.get("stock_code") or ""
    kind = event.get("date_kind") or event.get("type")
    ex_day = event.get("ex_date") or (event.get("date") if kind == "ex_date" else None)
    record_day = event.get("record_date") or (event.get("date") if kind == "record_date" else None)
    basis_day, basis_rule, approximate = ex_day or record_day, "ex_date" if ex_day else "record_date", False
    if not ex_day and record_day and market_side(code) == "KR":
        day, approximate = krx_record_entitlement_day(date.fromisoformat(record_day))
        basis_day, basis_rule = day.isoformat(), "krx_record_t2"
    currency = event.get("currency") or "KRW"
    tax_rate = DIVIDEND_TAX_RATES[dividend_country(code, currency)]
    shares = _number(event.get("shares"))
    per_share = None if event.get("amount_status") == "unknown" else _number(event.get("amount_per_share"))
    gross = tax = net = None
    if shares is not None and per_share is not None:
        gross = (shares * per_share).quantize(money_unit(currency), rounding=ROUND_HALF_UP)
        tax = withholding(gross, tax_rate, currency)
        net = gross - tax
    return {"dividend_basis_date": basis_day, "dividend_basis_rule": basis_rule if basis_day else None,
            "dividend_basis_approximate": approximate,
            "calculated_gross_amount": float(gross) if gross is not None else None,
            "calculated_tax_rate": float(tax_rate),
            "calculated_tax_amount": float(tax) if tax is not None else None,
            "calculated_net_amount": float(net) if net is not None else None}
