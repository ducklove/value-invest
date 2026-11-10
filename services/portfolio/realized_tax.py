"""실현손익 원장·NH 수입·배당 일정을 합쳐 연도별 세금 추정을 만든다. 신고용이 아니다.

조회만 한다. 외부 호출은 배당 일정(자체 캐시·시간 제한)뿐이며 NH 호출은 services/brokers/realized_pnl.py의
백그라운드 수집이 맡는다. 순수 계산과 세율표는 domain/realized_tax.py에 있다.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import date

import httpx

from cache_layer import MemoryTTLCache, cached_fetch
from core.errors import AppError
from domain import realized_tax as rules
from domain.dividend_entitlement import holding_identity
from domain.portfolio_codes import is_korean_stock, is_special_asset
from repositories import broker_activity
from repositories import realized_pnl as ledger
from services import dividend_calendar
from services.portfolio.time_windows import today_kst_date

UPCOMING_TIMEOUT_SECONDS = 20.0
# NH 해외 손익 명세가 국가 목록에 적은 시장의 통화. 그 밖의 시장은 응답에 포함되는지 확인되지 않았다.
SPEC_LISTED_CURRENCIES = frozenset({"USD", "JPY", "HKD", "CNY"})
_upcoming_cache = MemoryTTLCache("portfolio.realized_tax.upcoming", 600, evict_expired_after=3600)
_UPCOMING_ERRORS = (AppError, httpx.HTTPError, sqlite3.Error, OSError, TimeoutError, ValueError, KeyError, TypeError)

NOTES = (
    "신고용이 아닌 추정입니다. 실제 신고·납부 금액은 증권사 양도세 자료나 세무 전문가로 확인하세요.",
    "NH 손익은 NH가 자체 산식으로 계산한 값입니다. 원화 손익에 취득 시 환율이 반영되지만, 원가 단가 산정과 환율 기준이 세법의 선입선출법·취득일·양도일 기준환율과 다를 수 있습니다.",
    "연도는 NH 주문일 기준입니다. 세법상 귀속은 결제일 기준이라 연말 매도분은 다음 해로 넘어갈 수 있습니다. 매도 전 시장별 결제일을 확인하세요.",
    "NH 해외 손익 명세의 국가 목록은 미국·일본·홍콩·상해·심천입니다. 호주·독일·베트남 매매가 응답에 포함되는지는 확인되지 않았습니다.",
    "국내 실현손익은 참고 금액입니다. 상장주식 소액주주의 장내 매매를 전제로 세액을 계산하지 않으며, 대주주·장외 거래와 해외 손익의 통산은 반영하지 않습니다.",
    "일반 유형 계좌만 계산합니다. ISA·연금·배우자 계좌와 국내시장복귀계좌(RIA) 공제는 반영하지 않습니다.",
    "금융소득은 NH 배당(세전)과 예탁금이용료·MMW 이자입니다. RP·발행어음·외화 이자, 국내 상장 해외·채권 ETF 매매차익(배당소득), 고배당기업 분리과세 특례는 반영하지 않습니다.",
    "손실 매도 감소액은 마지막 정산의 평가손실을 현재 환율로 실현한다고 가정한 값입니다. 수수료·환전 비용을 빼지 않았고, 매도 권유가 아닙니다.",
)


def _market_status(states: list[dict], account_ids: set[str]) -> str:
    """이 시장의 연도 합계를 믿을 수 있는 정도. 일부 계좌만 성공했거나 최근 조회가 실패했으면 ok가 아니다."""
    relevant = [row for row in states if row["account_id"] in account_ids]
    done = {row["account_id"] for row in relevant if row["last_success_at"]}
    failed = any(row["error"] for row in relevant)
    if account_ids and done >= account_ids and not failed:
        return "ok"
    if done:
        return "partial"
    return "failed" if failed else "pending"


def _gap(rows: list[dict], broker_totals: list[float | None]) -> float | None:
    """같은 계좌들의 원장 합계와 NH 기간 합계의 차이. 합계를 하나라도 받지 못했으면 비교하지 않는다."""
    if not broker_totals or any(value is None for value in broker_totals):
        return None
    return round(sum(row["realized_krw"] or 0 for row in rows) - sum(broker_totals), 2)


def _year_summary(year: int, rows: list[dict], states: list[dict], account_ids: set[str]) -> dict:
    year_rows = [row for row in rows if row["trade_date"][:4] == f"{year:04d}"]
    summary = rules.summarize_year(year_rows)
    year_states = [row for row in states if row["year"] == year]
    for market in ("domestic", "overseas"):
        market_states = [row for row in year_states if row["market"] == market and row["account_id"] in account_ids]
        if account_ids:
            summary[market]["status"] = _market_status(market_states, account_ids)
        else:
            # 연결을 해제한 계좌의 원장만 남았으면 보여 주되 갱신되지 않음을 표시한다.
            summary[market]["status"] = "partial" if summary[market]["rows"] else "no_account"
        linked_rows = [row for row in year_rows if row["market"] == market and row["account_id"] in account_ids]
        summary[market]["total_gap_krw"] = _gap(linked_rows, [row["broker_total"] for row in market_states])
    overseas = summary["overseas"]
    collected = overseas["status"] in {"ok", "partial"}
    tax = rules.capital_gains(overseas["realized_krw"], year) if collected else None
    if tax is None:
        tax_status = "unavailable"
    elif overseas["status"] != "ok" or overseas["excluded_rows"]:
        tax_status = "incomplete"
    else:
        tax_status = "estimated"
    return {"year": year, **summary, "tax": tax, "tax_status": tax_status, "rules_stale": year > rules.RULES_REVIEWED_THROUGH}


def _loss_positions(positions: list[dict], prices: list[dict], account_ids: set[str]) -> dict:
    """NH 해외 보유분의 평가손익. 원화 금액 = 수량 × (마지막 정산 원화 단가 − 현지 매입단가 × 정산 환율)."""
    by_identity = {holding_identity(row["stock_code"]): row for row in prices}
    rows, unpriced = [], 0
    overseas = [row for row in positions if row["account_id"] in account_ids and not is_special_asset(row["stock_code"])
                and not is_korean_stock(row["stock_code"]) and (row.get("currency") or "KRW") != "KRW"]
    for row in overseas:
        price = by_identity.get(holding_identity(row["stock_code"]))
        usable = (price and not price.get("priced_from_fallback") and price.get("unit_price") and price.get("fx_rate")
                  and price.get("currency") == row["currency"] and row.get("avg_price_currency") == row["currency"])
        if not usable:
            unpriced += 1
            continue
        quantity = float(row["quantity"])
        rows.append({"stock_code": row["stock_code"], "stock_name": row["stock_name"], "account_name": row["account_name"],
                     "currency": row["currency"], "quantity": quantity,
                     "unrealized_krw": round(quantity * (float(price["unit_price"]) - float(row["avg_price"]) * float(price["fx_rate"])), 2)})
    as_of = max((row["date"] for row in prices), default=None)
    return {"rows": rows, "position_count": len(overseas), "unpriced_count": unpriced, "as_of": as_of}


async def _load_upcoming_events(user: str, today: date) -> list[dict]:
    calendar = await asyncio.wait_for(
        dividend_calendar.build_calendar(user, months_back=0, months_forward=12 - today.month, today=today),
        timeout=UPCOMING_TIMEOUT_SECONDS)
    fields = ("date", "stock_code", "expected_amount_krw", "cashflow", "shares", "date_status", "nh_match")
    return [{key: event.get(key) for key in fields} for event in calendar["events"]]


async def _upcoming(user: str, today: date, positions: list[dict]) -> dict:
    """남은 지급 예정. 일정을 불러오지 못해도 받은 금액은 그대로 보여 준다."""
    quantities: dict[str, float] = {}
    for row in positions:
        identity = holding_identity(row["stock_code"])
        quantities[identity] = quantities.get(identity, 0.0) + float(row["quantity"])
    try:
        events = await cached_fetch(_upcoming_cache, f"{user}:{today.isoformat()}", lambda: _load_upcoming_events(user, today),
                                    errors=_UPCOMING_ERRORS)
    except _UPCOMING_ERRORS:
        return {"available": False, "upcoming_krw": 0, "unscheduled_krw": 0, "other_accounts_krw": 0, "scheduled": 0,
                "unscheduled": 0, "estimated": 0, "unknown_amount": 0}
    return {"available": True, **rules.upcoming_income(events, quantities, today)}


async def _financial_income(user: str, year: int, today: date, income_ids: set[str], states: list[dict], positions: list[dict]) -> dict | None:
    """올해 세전 금융소득 누계. 운영 broker_transactions에는 아직 이자 행이 없어 일별 수입 합계도 함께 읽는다."""
    dividends = 0.0
    dividend_rows = review_rows = unverified_rows = 0
    for record in await broker_activity.dividend_records(user):
        if record["account_id"] not in income_ids or str(record.get("date") or "")[:4] != f"{year:04d}":
            continue
        if record["kind"] != "dividend":
            review_rows += 1
            continue
        gross = record.get("gross_krw")
        if gross is None and record.get("currency") == "KRW":
            gross = record.get("gross_amount")
        if gross is None:
            unverified_rows += 1
            continue
        dividends += float(gross)
        dividend_rows += 1
    interest = 0.0
    interest_from = None
    # 수입 원장에 이자 행이 있으면 그날의 일별 합계로 다시 세지 않는다(사유 대조와 같은 규칙).
    ledger_days = set()
    for row in await ledger.ledger_interest_rows(user, year):
        if row["account_id"] not in income_ids:
            continue
        ledger_days.add(row["date"])
        gross = row.get("gross_amount") if row.get("currency") == "KRW" else None
        if gross is None and row.get("income_krw") is not None:
            gross = rules.gross_interest(row["income_krw"], year)
        if gross is None:
            unverified_rows += 1
            continue
        interest += float(gross)
        interest_from = min(interest_from or row["date"], row["date"])
    for row in await ledger.interest_rows(user, year):
        if row["account_id"] not in income_ids or (row["kind"] == "interest" and row["date"] in ledger_days):
            continue
        # MMW 평가이자는 세전, 예탁금이용료 등은 세후 합계만 남아 있어 원천징수율로 되돌린다.
        interest += float(row["amount"]) if row["kind"] == "mmw_interest" else rules.gross_interest(row["amount"], year)
        interest_from = min(interest_from or row["date"], row["date"])
    backfilled = {row["account_id"] for row in states if row["year"] == year and row["market"] == "income" and row["last_success_at"]}
    upcoming = await _upcoming(user, today, positions)
    result = rules.financial_income(year, dividends_krw=dividends, interest_krw=interest,
                                    upcoming_krw=upcoming["upcoming_krw"], unscheduled_krw=upcoming["unscheduled_krw"])
    if result is None:
        return None
    return {**result, "year": year, "dividend_rows": dividend_rows, "review_rows": review_rows, "unverified_rows": unverified_rows,
            "interest_complete": bool(income_ids) and income_ids <= backfilled, "interest_from": interest_from,
            "upcoming": upcoming, "rules_stale": year > rules.RULES_REVIEWED_THROUGH}


def _is_live_nh(row: dict) -> bool:
    return row["provider"] == "namuh" and row["environment"] == "live"


def _coverage(accounts: list[dict], states: list[dict]) -> tuple[dict, set[str], set[str]]:
    """손익 조회 대상(일반 유형 NH 주식 계좌), 수입 대상(일반 유형 NH 계좌)과 화면에 밝힐 미포함 계좌."""
    general = [row for row in accounts if row["type"] == "general"]
    pnl = [row for row in general if _is_live_nh(row) and (row["product"] or "stocks") == "stocks"]
    income = [row for row in general if _is_live_nh(row)]
    pnl_ids, income_ids = {row["account_id"] for row in pnl}, {row["account_id"] for row in income}
    names = {row["account_id"]: row["name"] for row in accounts}
    excluded = [{"name": row["name"], "reason": "account_type" if row["type"] != "general" else "product" if _is_live_nh(row) else "not_nh"}
                for row in accounts if row["account_id"] not in pnl_ids and row["holdings"]]
    errors = [{"account_name": names.get(row["account_id"], ""), "year": row["year"], "market": row["market"], "message": row["error"],
               "last_success_at": row["last_success_at"]} for row in states if row["error"] and row["account_id"] in income_ids]
    last_success = max((row["last_success_at"] for row in states if row["last_success_at"] and row["account_id"] in pnl_ids), default=None)
    return {"nh_accounts": [row["name"] for row in pnl], "excluded_accounts": excluded,
            "not_nh_count": sum(row["reason"] == "not_nh" for row in excluded), "errors": errors, "last_success_at": last_success}, pnl_ids, income_ids


async def build_summary(user: str, *, today: date | None = None) -> dict:
    today = today or today_kst_date()
    year = today.year
    accounts = await ledger.account_coverage(user)
    states = await ledger.states(user)
    coverage, pnl_ids, income_ids = _coverage(accounts, states)
    rows = await ledger.rows(user)
    base = {"as_of": today.isoformat(), "year": year, "fields_verified": rules.NH_PNL_FIELDS_VERIFIED, "coverage": coverage, "notes": list(NOTES)}
    if not pnl_ids and not income_ids and not rows:
        return {**base, "available": False, "message": "NH 실계좌를 연결하면 입력 없이 자동으로 계산합니다."}
    years = {year} | {int(row["trade_date"][:4]) for row in rows} | {row["year"] for row in states if row["market"] != "income" and row["account_id"] in pnl_ids}
    year_rows = [_year_summary(item, rows, states, pnl_ids) for item in sorted(years, reverse=True)]
    current = next(row for row in year_rows if row["year"] == year)
    positions = await ledger.nh_positions(user)
    general_positions = [row for row in positions if row["account_type"] == "general"]
    holdings = _loss_positions(general_positions, await ledger.latest_stock_prices(user), pnl_ids)
    harvest = rules.loss_harvest(current["overseas"]["realized_krw"], holdings["rows"], year) if current["tax"] else None
    reported = {row["currency"] for row in rows}
    unlisted = sorted({row["currency"] for row in general_positions if row["account_id"] in pnl_ids and not is_korean_stock(row["stock_code"])
                       and not is_special_asset(row["stock_code"])} - SPEC_LISTED_CURRENCIES - reported - {"KRW"})
    return {**base, "available": True, "current": current, "years": year_rows, "unlisted_currencies": unlisted,
            "loss_harvest": {**(harvest or {}), "available": harvest is not None, "position_count": holdings["position_count"],
                             "unpriced_count": holdings["unpriced_count"], "as_of": holdings["as_of"]},
            "financial_income": await _financial_income(user, year, today, income_ids, states, general_positions)}


def _approx(value, verified: bool) -> str:
    return f"{value:,.0f}원" if verified else rules.approx_krw(value)


def _action(key: str, severity: str, title: str, detail: str, metric, meta: dict) -> dict:
    return {"key": key, "category": "tax", "severity": severity, "title": title, "detail": detail, "source": "세금 추정",
            "stock_code": None, "stock_name": None, "url": "#pfTaxWrap", "metric": metric, "meta": meta, "status": "open", "review": None}


def action_items(summary: dict, today: date) -> list[dict]:
    """11~12월의 해외 공제 잔여와 금융소득 2,000만원 근접. 상태가 바뀌면 새 키로 다시 올린다."""
    if not summary.get("available"):
        return []
    items = []
    verified = bool(summary.get("fields_verified"))
    label = "신고용이 아닌 추정입니다." if verified else "NH 필드 확인 전의 미검증 추정이며 신고용이 아닙니다."
    current = summary["current"]
    if current["tax_status"] == "incomplete":
        label = f"일부 계좌·내역이 빠져 있습니다. {label}"
    tax, overseas, harvest = current["tax"], current["overseas"], summary["loss_harvest"]
    has_overseas = overseas["rows"] or harvest.get("position_count")
    if today.month in {11, 12} and tax and has_overseas:
        year = current["year"]
        settlement = "연말 매도분은 결제일이 다음 해면 귀속도 넘어갑니다."
        if tax["deduction_remaining_krw"] > 0:
            items.append(_action(
                f"tax:overseas:{year}:remaining", "watch", f"해외 양도차익 공제 잔여 {_approx(tax['deduction_remaining_krw'], verified)}",
                f"{year}년 NH 해외 실현손익 {_approx(overseas['realized_krw'], verified)} 기준입니다. {settlement} {label}",
                tax["deduction_remaining_krw"], {"tax": tax, "tax_status": current["tax_status"]}))
        else:
            saving = harvest.get("max_saving_krw") or 0
            reduce = f" 평가손실 종목을 실현하면 최대 {_approx(saving, verified)} 줄어듭니다." if saving > 0 else ""
            items.append(_action(
                f"tax:overseas:{year}:exhausted", "watch", f"해외 양도세 추정 {_approx(tax['estimated_tax_krw'], verified)}",
                f"{year}년 기본공제를 모두 사용한 것으로 계산됩니다.{reduce} {settlement} {label}",
                tax["estimated_tax_krw"], {"tax": tax, "tax_status": current["tax_status"], "max_saving_krw": saving}))
    income = summary.get("financial_income")
    if income and income["status"] in {"near", "over"}:
        over = income["status"] == "over"
        threshold = f"{income['threshold_krw'] // 10000:,}만원"
        items.append(_action(
            f"tax:financial-income:{income['year']}:{income['status']}", "high" if over else "watch",
            f"금융소득 {threshold} {'초과 예상' if over else '근접'} ({income['ratio_pct']:g}%)",
            f"{income['year']}년 세전 배당·이자 {rules.approx_krw(income['received_krw'])}에 남은 지급 예정 {rules.approx_krw(income['upcoming_krw'])}을 더한 추정입니다. "
            "초과분은 종합과세 대상이 될 수 있습니다. NH 외 계좌는 포함하지 않았고 신고용이 아닙니다.",
            income["projected_krw"], {"financial_income": {key: income[key] for key in ("threshold_krw", "received_krw", "upcoming_krw", "projected_krw", "ratio_pct")}}))
    return items


async def actions(user: str, *, today: date | None = None) -> list[dict]:
    today = today or today_kst_date()
    return action_items(await build_summary(user, today=today), today)
