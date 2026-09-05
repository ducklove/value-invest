"""저장된 연간 재무자료로 투자 논거의 반증 조건을 점검한다."""

import math
import operator
import re
from datetime import date

from repositories import investment_insights as repo
from services.portfolio.time_windows import today_kst_date

METRICS = {"manual": "직접 확인", "revenue_growth_pct": "매출 전년 대비 증감률(%)",
           "operating_margin_pct": "영업이익률(%)", "debt_ratio_pct": "부채비율(%)"}
OPERATORS = {"lt": operator.lt, "lte": operator.le, "gt": operator.gt, "gte": operator.ge}


def number(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def evaluate(thesis: dict, source: dict, today: date) -> dict:
    financials = source.get("financials") or []
    current = financials[0] if financials else {}
    metric = thesis["metric"]
    value = None
    if metric == "revenue_growth_pct" and len(financials) > 1:
        previous = financials[1]
        old, new = number(previous.get("revenue")), number(current.get("revenue"))
        if old is not None and old > 0 and new is not None and current["year"] == previous["year"] + 1:
            value = (new / old - 1) * 100
    elif metric in {"operating_margin_pct", "debt_ratio_pct"}:
        numerator, denominator = ("operating_profit", "revenue") if metric == "operating_margin_pct" else ("total_liabilities", "total_equity")
        top, bottom = number(current.get(numerator)), number(current.get(denominator))
        if top is not None and bottom is not None and bottom > 0:
            value = top / bottom * 100
    value = number(value)
    # 최신 연간 자료가 2년보다 오래되면 정상 통과 판정을 하지 않는다.
    stale = bool(current) and int(current["year"]) < today.year - 2
    breached = value is not None and OPERATORS[thesis["operator"]](value, thesis["threshold"])
    due = bool(thesis.get("deadline")) and thesis["deadline"] <= today.isoformat()
    status = "breached" if breached and not stale else "due" if due else "unknown" if value is None or stale else "monitoring"
    if metric == "manual" and not due:
        status = "manual"
    filing = source.get("filing") or {}
    receipt = str(filing.get("rcept_no") or "")
    return {
        "stock_code": thesis["stock_code"],
        "status": status, "value": round(value, 6) if value is not None else None,
        "metric_label": METRICS[metric], "threshold": thesis["threshold"], "operator": thesis["operator"],
        "year": current.get("year"), "report_date": current.get("report_date"), "stale": stale,
        "deadline": thesis.get("deadline"), "due": due,
        "thesis": thesis["thesis"], "invalidation": thesis["invalidation"], "evidence_url": thesis.get("evidence_url"),
        "filing_url": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={receipt}" if re.fullmatch(r"\d{14}", receipt) else None,
        "filing_name": filing.get("report_name"), "filing_date": filing.get("report_date"),
    }


async def review_theses(user: str) -> dict:
    items = await repo.list_theses(user)
    sources = {}
    for item in items:
        if item["archived"]:
            payload = {"status": "archived", "stock_code": item["stock_code"], "thesis": item["thesis"],
                       "invalidation": item["invalidation"], "evidence_url": item["evidence_url"]}
            item["check"] = await repo.record_check(user, item["id"], payload, item["updated_at"]) or {"status": "changed"}
            continue
        code = item["stock_code"]
        if code not in sources:
            sources[code] = await repo.thesis_source(code)
        check = evaluate(item, sources[code], today_kst_date())
        item["check"] = await repo.record_check(user, item["id"], check, item["updated_at"]) or {"status": "changed"}
    return {"items": items, "checked_at": repo.now(), "metrics": METRICS}


async def actions(user: str) -> list[dict]:
    report = await review_theses(user)
    result = []
    for item in report["items"]:
        check = item["check"]
        if check["status"] not in {"breached", "due", "unknown"}:
            continue
        label = {"breached": "반증 조건 충족", "due": "검토 기한 도래", "unknown": "근거 자료 확인 필요"}[check["status"]]
        result.append({
            "key": f"thesis:{item['id']}:{check['event_id']}", "category": "thesis",
            "severity": "high" if check["status"] == "breached" else "watch",
            "title": f"{item['stock_name']} · {label}", "detail": item["invalidation"],
            "source": "투자 논거", "stock_code": item["stock_code"], "stock_name": item["stock_name"],
            "url": "#pfThesisWrap", "metric": check["value"], "meta": check, "status": "open", "review": None,
        })
    return result
