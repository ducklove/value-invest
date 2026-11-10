"""NH 기간 손익 조회를 입력 없는 실현손익 원장으로 옮긴다. 주문 API는 쓰지 않는다.

연도 합계 응답을 먼저 반영하고 바뀐 일자·종목만 상세 조회한다. 그래서 상세가 늦거나 합계와 맞지 않아도 연도 합계는
NH 기간 합계와 같다. 호출은 namuh.pages의 앱키 잠금·호출 간격을 공유한다. 필드 의미는 2026-11-10 운영 응답으로
확인했다(docs/realized-tax.md, domain.realized_tax.NH_PNL_FIELDS_VERIFIED).
"""

import asyncio
import logging
import re
from datetime import date, datetime
from decimal import Decimal

import aiosqlite
import httpx

from cache_layer import FETCH_ERRORS
from core.errors import AppError
from domain.timeutil import KST
from repositories import brokers
from repositories import realized_pnl as ledger
from repositories.broker_secrets import BrokerError
from services.brokers import namuh, namuh_balance
from services.brokers.activity import (
    EMPTY_RESPONSE_CODES,
    _check_message,
    _lenient,
    _ymd,
    digest,
    numeric,
    overseas_symbol,
)
from services.brokers.symbols import domestic_code
from services.brokers.sync import sync_account
from services.portfolio import fx

logger = logging.getLogger(__name__)

GB_PERIOD = "/gbstock/inquiry/v1/periodPnl"
GB_DETAIL = "/gbstock/inquiry/v1/periodPnlDetail"
KR_TRADING = "/krstock/inquiry/v1/tradingPnl"
KR_DAILY = "/krstock/inquiry/v1/dailyTradingPnl"
BASIS_NATIVE, BASIS_KRW = "1", "2"
ALL_NATIONS = "000"
DOMESTIC, OVERSEAS, INCOME = "domestic", "overseas", "income"

# 올해 분은 매도 결과를 연말 전에 확인하도록 자주, 지난해는 정정만 살핀다.
DEFAULT_INTERVAL_SECONDS = 1800.0
PAST_YEAR_INTERVAL_SECONDS = 7 * 86400.0
TICK_SECONDS = 300.0
INITIAL_DELAY_SECONDS = 120.0
FAILURE_BACKOFF_SECONDS = 300.0
FAILURE_BACKOFF_MAX_SECONDS = 3600.0
# 원화 기준 금액이 원화가 아니면(거래통화 그대로 등) 세액에 넣지 않는다. 넉넉한 범위로 단위 오류만 거른다.
FX_RATIO_RANGE = (Decimal("0.6"), Decimal("1.5"))
NATIVE_TOLERANCE, KRW_TOLERANCE = Decimal("0.02"), Decimal("1.5")

# 거래통화·원화 기준 응답이 같은 필드명을 쓴다. 수량·금액·손익만 보관한다(계좌번호 등 식별 원문 제외).
_OVERSEAS_FIELDS = {"buy_qty": "byn_qty", "buy_amount": "fc_byn_amt1", "sell_qty": "sll_qty", "sell_amount": "fc_sll_amt",
                    "trading": "fc_sby_pls", "charges": "fc_sdr_xps", "realized": "fc_rzt_pls"}
_DOMESTIC_FIELDS = {"buy_qty": "byn_qty", "buy_amount": "byn_amt", "buy_fee": "byn_fee", "sell_qty": "sll_qty",
                    "sell_amount": "sll_amt", "book_amount": "sll_abk_amt", "sell_tax": "sll_tax_sum", "realized": "pls_amt"}
_DAILY_FIELDS = {"sell_qty": "cns_qty", "sell_amount": "cns_amt", "sell_fee": "sll_fee", "sell_tax": "sll_tax_amt",
                 "loan_interest": "lon_int_amt", "book_amount": "phs_amt", "buy_fee": "byn_fee", "realized": "pls_amt"}
_REALIZED_KEYS = ("sell_qty", "sell_amount", "trading", "realized")
# 한 계좌의 실패가 다른 계좌와 다음 주기의 수집을 멈추지 않는다.
_COLLECT_ERRORS = (*FETCH_ERRORS, aiosqlite.Error)


def _rows(pages: list[dict], block: str, summary_block: str | None = None) -> list[dict]:
    """모든 연속조회 페이지의 목록. 블록이 없으면 빈 응답으로 확인될 때만 0건이다(누락을 0건으로 숨기지 않는다)."""
    rows = []
    for page in pages:
        _check_message(page)
        value = page.get(block)
        if value is None:
            if _is_empty_page(page, summary_block):
                continue
            raise BrokerError("NH 손익 응답을 확인할 수 없어 이전 내역을 유지했습니다.")
        value = [value] if isinstance(value, dict) else value
        if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
            raise BrokerError("NH 손익 응답 형식이 올바르지 않아 이전 내역을 유지했습니다.")
        rows.extend(value)
    return rows


def _is_empty_page(page: dict, summary_block: str | None) -> bool:
    if str(page.get("rsp_cd") or "").strip() in EMPTY_RESPONSE_CODES:
        return True
    total = page.get(summary_block) if summary_block else None
    return isinstance(total, dict) and all(not _lenient(total, key) for key in total)


def _summary_total(pages: list[dict], field: str) -> float | None:
    """연속조회 마지막 합계 블록. 페이지마다 반복되는 합계를 더하지 않는다."""
    for page in reversed(pages):
        total = page.get("Output_0")
        if isinstance(total, dict) and total:
            value = _lenient(total, field)
            return float(value) if value is not None else None
    return None


def _amounts(rows: list[dict], fields: dict[str, str]) -> dict[str, Decimal | None]:
    totals = {}
    for name, field in fields.items():
        values = [value for value in (numeric(row, field) for row in rows) if value is not None]
        totals[name] = sum(values, Decimal(0)) if values else None
    return totals


def _has_realized(amounts: dict | None) -> bool:
    return bool(amounts) and any(amounts.get(key) for key in _REALIZED_KEYS)


def _float(value) -> float | None:
    return float(value) if value is not None else None


def _plain(amounts: dict | None) -> dict | None:
    return {key: _float(value) for key, value in amounts.items()} if amounts else None


def _arithmetic_ok(amounts: dict, tolerance: Decimal) -> bool:
    """실현손익 = 매매손익 − 제비용(명세 필드명 기준 가정). 값이 없으면 판단하지 않는다."""
    trading, charges, realized = amounts.get("trading"), amounts.get("charges"), amounts.get("realized")
    if trading is None or charges is None or realized is None:
        return True
    return abs(realized - (trading - charges)) <= tolerance


def fx_plausible(sell_native: Decimal | None, sell_krw: Decimal | None, reference: float | None) -> bool:
    """원화 기준 매도금액이 거래통화 금액의 환산으로 볼 수 있는지 확인한다."""
    if not sell_native or sell_krw is None:
        return True
    implied = abs(sell_krw / sell_native)
    if not reference or reference <= 0:
        return implied != 1
    return FX_RATIO_RANGE[0] <= implied / Decimal(str(reference)) <= FX_RATIO_RANGE[1]


def overseas_issues(native: dict | None, krw: dict | None, reference: float | None) -> tuple[list[str], bool]:
    """검산에서 나온 표시와, 원화 손익을 세액 합계에 넣을 수 있는지."""
    issues = []
    usable = True
    if native is None:
        issues.append("native_missing")
    if krw is None or krw.get("realized") is None:
        issues.append("krw_missing")
        usable = False
    if native and not _arithmetic_ok(native, NATIVE_TOLERANCE) or krw and not _arithmetic_ok(krw, KRW_TOLERANCE):
        issues.append("arith_mismatch")
    if native and krw and not fx_plausible(native.get("sell_amount"), krw.get("sell_amount"), reference):
        issues.append("fx_implausible")
        usable = False
    return issues, usable


def _entry(link: dict, *, market: str, group_key: str, day: str, currency: str, nation: str = "", symbol: str = "", code: str = "",
           name: str = "", ordinal: int = 0, native: dict | None = None, krw: dict | None = None, usable: bool = True,
           issues: list[str] | tuple = ()) -> dict:
    identity = [link["account_fingerprint"], link["environment"], "realized", group_key, day, symbol, ordinal]
    amounts = native or krw or {}
    return {"source_key": digest(identity), "group_key": group_key, "market": market, "trade_date": day, "stock_code": code or symbol,
            "symbol": symbol, "stock_name": name[:80], "nation_code": nation, "currency": currency,
            "sell_quantity": _float(amounts.get("sell_qty")), "sell_native": _float((native or {}).get("sell_amount")),
            "realized_native": _float((native or {}).get("realized")), "sell_krw": _float((krw or {}).get("sell_amount")),
            "realized_krw": _float((krw or {}).get("realized")) if usable else None,
            "issues": sorted(set(issues)), "data": {"native": _plain(native), "krw": _plain(krw)}}


def merge_overseas(native_rows: list[dict], krw_rows: list[dict], start: date, end: date) -> dict[str, dict]:
    """거래통화·원화 기준 기간 행을 일자·국가·통화 묶음으로 합친다. 원화 기준의 통화가 KRW로 와도 일자·국가로 맞춘다."""
    def identify(row: dict) -> tuple[str, str, str]:
        day = _ymd(row, "orr_dt")
        if not day or not start.isoformat() <= day <= end.isoformat():
            raise BrokerError("NH 조회 기간 밖의 손익이 반환되어 이전 내역을 유지했습니다.")
        nation = str(row.get("fc_sec_trd_nat_cd") or "").strip()
        if not re.fullmatch(r"[0-9]{3}", nation):
            raise BrokerError("NH 해외 손익의 국가코드를 확인할 수 없어 이전 내역을 유지했습니다.")
        return day, nation, str(row.get("trd_cur_cd") or "").strip().upper()

    groups: dict[str, dict] = {}
    for row in native_rows:
        day, nation, currency = identify(row)
        if not re.fullmatch(r"[A-Z]{3}", currency) or currency == "KRW":
            raise BrokerError("NH 해외 손익의 거래통화를 확인할 수 없어 이전 내역을 유지했습니다.")
        key = f"gb:{day}:{nation}:{currency}"
        groups.setdefault(key, {"key": key, "date": day, "nation": nation, "currency": currency, "native": [], "krw": []})["native"].append(row)
    for row in krw_rows:
        day, nation, currency = identify(row)
        if currency in {"", "KRW"}:
            matches = [group for group in groups.values() if group["date"] == day and group["nation"] == nation and group["currency"] != "KRW"]
            if len(matches) == 1:
                matches[0]["krw"].append(row)
                continue
            # 어느 거래통화의 원화인지 알 수 없는 행은 버리지 않고 별도 묶음으로 둔다(합계 누락 방지, 확인 필요 표시).
            currency = "KRW"
        key = f"gb:{day}:{nation}:{currency}"
        groups.setdefault(key, {"key": key, "date": day, "nation": nation, "currency": currency, "native": [], "krw": []})["krw"].append(row)
    return groups


def overseas_group_entry(link: dict, group: dict, reference: float | None) -> dict | None:
    """일자·국가·통화 합계 1행. 실현이 없는 묶음(매수만)은 None."""
    native = _amounts(group["native"], _OVERSEAS_FIELDS) if group["native"] else None
    krw = _amounts(group["krw"], _OVERSEAS_FIELDS) if group["krw"] else None
    if not _has_realized(native) and not _has_realized(krw):
        return None
    issues, usable = overseas_issues(native, krw, reference)
    return _entry(link, market=OVERSEAS, group_key=group["key"], day=group["date"], currency=group["currency"], nation=group["nation"],
                  native=native, krw=krw, usable=usable, issues=[*issues, ledger.DETAIL_PENDING])


def _detail_rows(rows: list[dict]) -> dict[tuple[str, int], tuple[dict, dict]] | None:
    """종목·응답 순번별 금액. 종목코드 없는 실현 행이 있으면 상세를 쓰지 않는다."""
    counts: dict[str, int] = {}
    result = {}
    for row in rows:
        amounts = _amounts([row], _OVERSEAS_FIELDS)
        if not _has_realized(amounts):
            continue
        symbol = str(row.get("iem_cd") or "").strip().upper()
        if not symbol:
            return None
        ordinal = counts.get(symbol, 0)
        counts[symbol] = ordinal + 1
        result[(symbol, ordinal)] = (row, amounts)
    return result


def _sum_matches(amounts: list[dict], total: Decimal | None, tolerance: Decimal) -> bool:
    if total is None or any(item.get("realized") is None for item in amounts):
        return False
    return abs(sum((item["realized"] for item in amounts), Decimal(0)) - total) <= tolerance * max(1, len(amounts))


async def _overseas_stock_code(symbol: str, nation: str, currency: str) -> str:
    """잔고 동기화와 같은 종목코드로 해석한다. 실패해도 원래 티커로 기록한다."""
    ticker = overseas_symbol(symbol)[0] or symbol
    try:
        return await namuh_balance.overseas_code({"fc_sec_trd_nat_cd": nation, "iem_cd": ticker}, currency)
    except BrokerError:
        return ""


async def overseas_detail_entries(link: dict, group: dict, native_rows: list[dict], krw_rows: list[dict], reference: float | None) -> list[dict] | None:
    """양쪽 기준 상세가 같은 종목으로 짝지어지고 기간 합계와 맞을 때만 종목 행을 만든다."""
    natives, krws = _detail_rows(native_rows), _detail_rows(krw_rows)
    if not natives or krws is None or natives.keys() != krws.keys():
        return None
    native_total = _amounts(group["native"], _OVERSEAS_FIELDS)["realized"] if group["native"] else None
    krw_total = _amounts(group["krw"], _OVERSEAS_FIELDS)["realized"] if group["krw"] else None
    if (not _sum_matches([item[1] for item in natives.values()], native_total, NATIVE_TOLERANCE)
            or not _sum_matches([item[1] for item in krws.values()], krw_total, KRW_TOLERANCE)):
        return None
    entries, codes = [], {}
    for (symbol, ordinal), (row, native) in natives.items():
        krw = krws[(symbol, ordinal)][1]
        if symbol not in codes:
            codes[symbol] = await _overseas_stock_code(symbol, group["nation"], group["currency"])
        issues, usable = overseas_issues(native, krw, reference)
        entries.append(_entry(link, market=OVERSEAS, group_key=group["key"], day=group["date"], currency=group["currency"],
                              nation=group["nation"], symbol=symbol, code=codes[symbol], name=str(row.get("iem_nm") or ""),
                              ordinal=ordinal, native=native, krw=krw, usable=usable, issues=issues))
    return entries


async def _fx_reference(currency: str) -> float | None:
    """단위 검산용 현재 환율. 없으면 검산 범위만 줄어들고 수집은 계속한다."""
    try:
        return await fx.fx_rate_for_currency(currency)
    except (AppError, httpx.HTTPError, TimeoutError, ValueError):
        return None


async def collect_overseas(user: str, link: dict, year: int, start: date, end: date) -> None:
    cid, account = link["credential_id"], link["account_no"]
    body = {"act_no": account, "sta_orr_dt": start.strftime("%Y%m%d"), "end_orr_dt": end.strftime("%Y%m%d"), "fc_sec_trd_nat_cd": ALL_NATIONS}
    native_pages = await namuh.pages(user, cid, GB_PERIOD, {**body, "iqr_dit": BASIS_NATIVE}, "live")
    krw_pages = await namuh.pages(user, cid, GB_PERIOD, {**body, "iqr_dit": BASIS_KRW}, "live")
    merged = merge_overseas(_rows(native_pages, "Output_1", "Output_0"), _rows(krw_pages, "Output_1", "Output_0"), start, end)
    references = {currency: await _fx_reference(currency) for currency in {group["currency"] for group in merged.values()}}
    groups = {}
    for key, group in merged.items():
        entry = overseas_group_entry(link, group, references[group["currency"]])
        if entry:
            groups[key] = {"digest": digest([group["native"], group["krw"]]), "entries": [entry]}
    await ledger.apply_period(user, link, year, OVERSEAS, groups, _summary_total(krw_pages, "fc_rzt_pls_sum"))
    for key in await ledger.pending_groups(user, link, year, OVERSEAS):
        group = merged.get(key)
        if not group:
            continue
        detail = {"act_no": account, "orr_dt": group["date"].replace("-", ""), "fc_sec_trd_nat_cd": group["nation"], "trd_cur_cd": group["currency"]}
        native_rows = _rows(await namuh.pages(user, cid, GB_DETAIL, {**detail, "iqr_dit": BASIS_NATIVE}, "live"), "Output_0")
        krw_rows = _rows(await namuh.pages(user, cid, GB_DETAIL, {**detail, "iqr_dit": BASIS_KRW}, "live"), "Output_0")
        await ledger.apply_detail(user, link, key, await overseas_detail_entries(link, group, native_rows, krw_rows, references[group["currency"]]))
    await ledger.finish(user, link, year, OVERSEAS)


def _domestic_symbol(raw) -> str:
    try:
        return domestic_code(raw)
    except BrokerError:
        symbol = re.sub(r"[^0-9A-Z]", "", str(raw or "").upper())[:12]
        if not symbol:
            raise BrokerError("NH 국내 손익의 종목코드를 확인할 수 없어 이전 내역을 유지했습니다.") from None
        return symbol


def domestic_groups(rows: list[dict], link: dict, year: int, end: date) -> dict[str, dict]:
    """종목별 기간 합계 1행. 일별 상세가 오기 전에도 연도 합계는 맞다. 매도가 없는 종목은 제외한다."""
    by_code: dict[str, list[dict]] = {}
    for row in rows:
        by_code.setdefault(_domestic_symbol(row.get("iem_cd")), []).append(row)
    groups = {}
    for code, stock_rows in by_code.items():
        amounts = _amounts(stock_rows, _DOMESTIC_FIELDS)
        if not _has_realized(amounts):
            continue
        key = f"kr:{year}:{code}"
        entry = _entry(link, market=DOMESTIC, group_key=key, day=end.isoformat(), currency="KRW", symbol=code, code=code,
                       name=str(stock_rows[0].get("iem_nm") or ""), native=amounts, krw=amounts, issues=[ledger.DETAIL_PENDING])
        groups[key] = {"digest": digest(stock_rows), "entries": [entry], "code": code, "name": entry["stock_name"], "total": amounts["realized"]}
    return groups


def domestic_detail_entries(link: dict, group: dict, group_key: str, rows: list[dict], start: date, end: date) -> list[dict] | None:
    """일별 매매내역이 종목 합계와 맞을 때만 일자 행으로 교체한다."""
    ordinals: dict[str, int] = {}
    entries, totals = [], []
    for row in rows:
        day = _ymd(row, "orr_dt")
        if not day or not start.isoformat() <= day <= end.isoformat():
            raise BrokerError("NH 조회 기간 밖의 손익이 반환되어 이전 내역을 유지했습니다.")
        amounts = _amounts([row], _DAILY_FIELDS)
        if "매도" not in str(row.get("sby_dit_cd_nm") or "") and not amounts["realized"]:
            continue  # 매수 행은 실현손익이 아니다.
        ordinal = ordinals.get(day, 0)
        ordinals[day] = ordinal + 1
        totals.append(amounts)
        entries.append(_entry(link, market=DOMESTIC, group_key=group_key, day=day, currency="KRW", symbol=group["code"], code=group["code"],
                              name=group["name"], ordinal=ordinal, native=amounts, krw=amounts))
    return entries if entries and _sum_matches(totals, group["total"], Decimal(1)) else None


async def collect_domestic(user: str, link: dict, year: int, start: date, end: date) -> None:
    cid, account = link["credential_id"], link["account_no"]
    window = {"act_no": account, "iqr_sta_dt": start.strftime("%Y%m%d"), "iqr_end_dt": end.strftime("%Y%m%d")}
    pages = await namuh.pages(user, cid, KR_TRADING, window, "live")
    groups = domestic_groups(_rows(pages, "Output_1", "Output_0"), link, year, end)
    await ledger.apply_period(user, link, year, DOMESTIC, groups, _summary_total(pages, "pls_amt"))
    for key in await ledger.pending_groups(user, link, year, DOMESTIC):
        group = groups.get(key)
        if not group:
            continue
        rows = _rows(await namuh.pages(user, cid, KR_DAILY, {**window, "iem_cd": group["code"]}, "live"), "Output_0")
        await ledger.apply_detail(user, link, key, domestic_detail_entries(link, group, key, rows, start, end))
    await ledger.finish(user, link, year, DOMESTIC)


async def backfill_income(user: str, link: dict, year: int, today: date) -> None:
    """올해 이자·배당을 연초부터 한 번 다시 읽는다. 일별 수입 합계는 2026-10 이후 자동 조회 구간만 있어 연초분이 비어 있다.

    기간 지정 가져오기와 같은 경로라 같은 키는 다시 넣지 않고, 일별 합계는 조회 기간만 교체한다.
    """
    result = await sync_account(user, link["account_id"], include_activity=True, start=date(year, 1, 1), end=today)
    if result.get("activity_error"):
        raise BrokerError(result["activity_error"])
    await ledger.finish(user, link, year, INCOME)


def is_due(state: dict | None, link: dict, *, now: datetime, interval_seconds: float) -> bool:
    """지금 조회할 차례인가. 연속 실패는 5분·10분·20분…(최대 1시간) 간격으로만 재시도한다."""
    if not state or state.get("account_fingerprint") != link["account_fingerprint"]:
        return True

    def elapsed(text: str | None) -> float:
        return (now.replace(tzinfo=None) - datetime.fromisoformat(text)).total_seconds() if text else float("inf")

    failures = state.get("failed_attempts") or 0
    if failures:
        return elapsed(state.get("attempted_at")) >= min(FAILURE_BACKOFF_SECONDS * 2 ** (failures - 1), FAILURE_BACKOFF_MAX_SECONDS)
    return elapsed(state.get("last_success_at")) >= interval_seconds


async def _attempt(user: str, link: dict, year: int, market: str, collection) -> None:
    """한 구분의 실패가 다른 구분과 이전 원장을 막지 않는다."""
    try:
        await collection
    except BrokerError as exc:
        await ledger.record_failure(user, link, year, market, str(exc))


async def collect_account(user: str, aid: str, *, now: datetime | None = None, interval_seconds: float = DEFAULT_INTERVAL_SECONDS) -> None:
    """차례가 된 연도·구분만 조회한다. 소유자가 아니면 get_link가 거절한다."""
    link = await brokers.get_link(user, aid)
    if link.get("provider", "namuh") != "namuh" or link["environment"] != "live":
        return
    now = now or datetime.now(KST)
    today = now.date()
    income_state = await ledger.state(user, aid, today.year, INCOME)
    needs_income = not (income_state and income_state.get("last_success_at") and income_state["account_fingerprint"] == link["account_fingerprint"])
    if needs_income and is_due(income_state, link, now=now, interval_seconds=0):
        await _attempt(user, link, today.year, INCOME, backfill_income(user, link, today.year, today))
    if link.get("product", "stocks") != "stocks":
        return  # 선물·금현물 계좌는 주식 손익 조회 대상이 아니다.
    for year, interval in ((today.year, interval_seconds), (today.year - 1, PAST_YEAR_INTERVAL_SECONDS)):
        start, end = date(year, 1, 1), min(date(year, 12, 31), today)
        for market, collector in ((DOMESTIC, collect_domestic), (OVERSEAS, collect_overseas)):
            if is_due(await ledger.state(user, aid, year, market), link, now=now, interval_seconds=interval):
                await _attempt(user, link, year, market, collector(user, link, year, start, end))


async def collect_due(*, stop: asyncio.Event | None = None, interval_seconds: float = DEFAULT_INTERVAL_SECONDS) -> None:
    for row in await brokers.list_links():
        if stop is not None and stop.is_set():
            return
        if row.get("provider") != "namuh" or row.get("environment") != "live":
            continue
        try:
            await collect_account(row["google_sub"], row["account_id"], interval_seconds=interval_seconds)
        except _COLLECT_ERRORS as exc:
            logger.warning("NH 실현손익 수집 보류: %s", type(exc).__name__)


async def run_loop(stop: asyncio.Event, *, interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
                   initial_delay_seconds: float = INITIAL_DELAY_SECONDS) -> None:
    """브라우저 접속과 무관하게 원장을 채운다. 시작 직후의 잔고·시세 복구와 겹치지 않게 늦게 시작한다."""
    delay = initial_delay_seconds
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
            return
        except TimeoutError:
            pass
        delay = TICK_SECONDS
        await collect_due(stop=stop, interval_seconds=interval_seconds)
