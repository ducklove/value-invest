"""새 어댑터의 정규화 경계: 누락을 0으로 처리하거나 부분 응답을 잔고로 저장하지 않는다."""

import math
import re

from repositories.broker_secrets import BrokerError
from services.portfolio.identifiers import CASH_FX_CODE


def number(row: dict, key: str) -> float:
    try:
        raw = row[key]
        if isinstance(raw, bool) or raw is None or not str(raw).strip():
            raise ValueError
        value = float(str(raw).replace(",", ""))
        if not math.isfinite(value):
            raise ValueError
        return value
    except (KeyError, ValueError, TypeError):
        raise BrokerError("증권사 잔고의 수량·금액이 누락되거나 올바르지 않아 기존 잔고를 유지합니다.") from None


def records(page: dict, key: str) -> list[dict]:
    rows = page.get(key)
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise BrokerError("증권사 잔고 목록이 불완전하여 기존 잔고를 유지합니다.")
    return rows


def summary(page: dict, key: str) -> dict:
    value = page.get(key)
    if not isinstance(value, dict) or not value:
        raise BrokerError("증권사 잔고 합계가 불완전하여 기존 잔고를 유지합니다.")
    return value


def no_debt(row: dict, *keys: str):
    if any(number(row, key) != 0 for key in keys):
        raise BrokerError("신용·대출·미수가 있는 계좌는 자동 합산을 지원하지 않습니다. 기존 잔고를 유지합니다.")


def position(code: str, name: str, quantity: float, price: float, currency: str = "KRW") -> dict:
    if currency != "KRW" and "CASH_" + currency not in CASH_FX_CODE:
        raise BrokerError("지원하지 않는 잔고 통화입니다.")
    if not code.startswith("CASH_") and (quantity < 0 or price < 0):
        raise BrokerError("음수 보유 수량·매입가는 자동 합산을 지원하지 않습니다.")
    return {"stock_code": code, "stock_name": name, "quantity": quantity, "avg_price": price,
            "currency": currency, "avg_price_currency": currency}


def stock_code(raw: str) -> str:
    code = str(raw).strip()
    if re.fullmatch(r"[AQ][0-9A-Z]{6}", code):
        code = code[1:]
    if not re.fullmatch(r"[0-9][0-9A-Z]{5}", code):
        raise BrokerError("지원하지 않는 종목이 있어 잔고 갱신을 보류했습니다.")
    return code


def merge(rows: list[dict]) -> list[dict]:
    result = {}
    for row in rows:
        code = row["stock_code"]
        if code in result:
            previous = result[code]
            if code.startswith("CASH_") or previous["currency"] != row["currency"]:
                raise BrokerError("중복된 통화·종목 잔고가 반환되어 갱신하지 않았습니다.")
            qty = previous["quantity"] + row["quantity"]
            if qty <= 0:
                raise BrokerError("합산할 수 없는 보유 수량입니다.")
            previous["avg_price"] = (previous["quantity"] * previous["avg_price"] + row["quantity"] * row["avg_price"]) / qty
            previous["quantity"] = qty
        else:
            result[code] = dict(row)
    return list(result.values())
