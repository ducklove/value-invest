"""실제 배당 이력과 예상 일정을 분리하는 순수 계산."""

from __future__ import annotations

from calendar import monthrange
from datetime import date, timedelta
from statistics import median

FREQUENCY_LABELS = {"monthly": "월배당", "quarterly": "분기배당", "semiannual": "반기배당", "annual": "연배당", "irregular": "비정기·자료 부족"}


def event_day(event: dict) -> str:
    return event.get("pay_date") or event.get("ex_date") or event.get("record_date") or ""


def frequency_of(events: list[dict], today: date, hint: str | None = None) -> str:
    if hint in FREQUENCY_LABELS:
        return hint
    cutoff = (today - timedelta(days=740)).isoformat()
    # 국내 결산배당의 지급 지연(4월→5월)을 월배당으로 오인하지 않는다.
    rights_days = [(e.get("ex_date") or e.get("record_date") or event_day(e), e) for e in events]
    days = sorted({day for day, e in rights_days if cutoff <= day <= today.isoformat() and (e.get("amount_per_share") or 0) > 0})
    if len(days) == 2 and 300 <= (date.fromisoformat(days[1]) - date.fromisoformat(days[0])).days <= 400:
        return "annual"
    if len(days) < 3:
        return "irregular"
    # 최근 여섯 간격. 주기 변경과 일회성 특별배당의 영향을 줄인다.
    gaps = [(date.fromisoformat(b) - date.fromisoformat(a)).days for a, b in zip(days, days[1:])][-6:]
    regular = [gap for gap in gaps if gap >= 20]
    if len(regular) < 2:
        return "irregular"
    spacing = median(regular)
    for name, lower, upper in (("monthly", 20, 45), ("quarterly", 65, 115), ("semiannual", 140, 220), ("annual", 300, 400)):
        if lower <= spacing <= upper and sum(lower <= gap <= upper for gap in regular) / len(regular) >= 0.6:
            return name
    return "irregular"


def project_events(events: list[dict], today: date, end: date, frequency: str) -> list[dict]:
    """최근 12개월 달·일을 재사용. 과거 빈칸을 실적으로 만들지 않는다.

    불규칙한 종목·오래된 자료는 예측하지 않는다. 월배당도 실제 패턴을
    복제하여 무지급월·연말 복수 지급을 보존한다.
    """
    history = sorted([e for e in events if event_day(e) <= today.isoformat() and (e.get("amount_per_share") or 0) > 0], key=event_day)
    if not history or frequency == "irregular":
        return []
    latest = date.fromisoformat(event_day(history[-1]))
    max_age = {"monthly": 100, "quarterly": 180, "semiannual": 300, "annual": 460}[frequency]
    if (today - latest).days > max_age:
        return []
    if (latest - date.fromisoformat(event_day(history[0]))).days < 300:
        return []
    templates = [e for e in history if event_day(e) > (latest - timedelta(days=365)).isoformat()]
    results = []
    for template in reversed(templates):
        original = date.fromisoformat(event_day(template))
        for year in range(today.year, end.year + 1):
            if year <= original.year:
                continue
            projected = date(year, original.month, min(original.day, monthrange(year, original.month)[1]))
            if not today < projected < end:
                continue
            if any(abs((date.fromisoformat(event_day(e)) - projected).days) <= 14 for e in events):
                continue
            if any(abs((date.fromisoformat(event_day(e)) - projected).days) <= 14 for e in results):
                continue
            kind = "pay_date" if template.get("pay_date") else "ex_date" if template.get("ex_date") else "record_date"
            results.append({**template, "pay_date": None, "ex_date": None, "record_date": None,
                            "declaration_date": None, kind: projected.isoformat(), "estimated": True,
                            "date_precision": "approximate", "basis_date": event_day(template)})
    return results


def calendar_event(holding: dict, raw: dict, rate: float | None, frequency: str, feed: dict) -> dict:
    code = holding["stock_code"]
    day = event_day(raw)
    kind = "payment" if raw.get("pay_date") else "ex_date" if raw.get("ex_date") else "record_date"
    estimated = bool(raw.get("estimated"))
    amount = raw.get("amount_per_share")
    shares = float(holding["quantity"])
    confirmed = bool(raw.get("official", feed.get("official"))) and not estimated
    label = {"payment": "지급일", "ex_date": "배당락일 · 지급일 미확인", "record_date": "배당기준일 · 지급일 미확인"}[kind]
    source_day = raw.get("ex_date") or raw.get("record_date") or day
    # 배당락일을 지급일로 보강해도 수취 건의 식별자는 유지한다.
    source_key = f"{code}:ex_date:{source_day}"
    return {**raw, "date": day, "stock_code": code, "stock_name": holding.get("stock_name") or code,
            "label": f"{FREQUENCY_LABELS[frequency]} · {label}" + (" (예상)" if estimated else ""),
            "type": "estimated" if estimated else kind, "date_kind": kind,
            "date_precision": "approximate" if estimated else "day", "confirmed": confirmed,
            "date_status": "estimated" if estimated else "announced" if confirmed else "observed",
            "amount_status": "unknown" if amount is None else "estimated" if estimated else "reported", "shares": shares,
            "holding_basis": "current", "frequency": frequency,
            "expected_amount_krw": round(amount * rate * shares) if amount is not None and rate else None,
            "cashflow": kind == "payment", "receiptable": not estimated and (amount is None or amount > 0),
            "source_key": source_key if not estimated else None,
            "source_aliases": [f"{code}:estimated:{day}", f"{code}:estimated:{day[:7]}-15"] if not estimated else [],
            "fetched_at": feed.get("fetched_at"), "data_status": feed.get("status", "unavailable")}
