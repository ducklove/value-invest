"""Current valuation inputs from explicitly reported quarterly financials.

The annual market-data table is a historical fiscal-year series. It must not
be overwritten with TTM EPS or the latest quarter's BPS. This independent
snapshot uses NAVER's quarterly finance table, excludes consensus columns,
and requires four consecutive actual quarters before exposing TTM EPS.

NAVER does not supply filing/publication dates or an explicit CFS/OFS flag in
this response. Those fields remain unknown; this is a current provider
snapshot and is deliberately unavailable for historical point-in-time use.
"""

from __future__ import annotations

import calendar
import logging
import math
import re
from datetime import date, datetime
from zoneinfo import ZoneInfo

from cache_layer import MemoryTTLCache, cached_fetch
from core.http import get_http_client
from domain.numbers import parse_number

logger = logging.getLogger(__name__)

_API = "https://m.stock.naver.com/api/stock/{stock_code}/finance/quarter"
_KST = ZoneInfo("Asia/Seoul")
_CACHE_TTL_SECONDS = 3600
_NO_DATA_TTL_SECONDS = 300
_MAX_PERIOD_AGE_DAYS = 220
_cache = MemoryTTLCache("valuation.quarterly.v1", _CACHE_TTL_SECONDS, evict_expired_after=_CACHE_TTL_SECONDS)


def _today() -> date:
    return datetime.now(_KST).date()


def _empty(stock_code: str, reason: str, *, as_of: date) -> dict:
    return {
        "status": "unavailable",
        "stock_code": stock_code,
        "eps_ttm": None,
        "bps": None,
        "roe_ttm": None,
        "earnings_basis": "ttm",
        "equity_basis": "latest",
        "roe_basis": "ttm",
        "period_end": None,
        "available_date": None,
        "statement_basis": None,
        "statement_basis_label": "제공사 분기 재무표 기준 · 연결/별도 구분 미제공",
        "source": "NAVER Finance",
        "source_url": f"https://m.stock.naver.com/domestic/stock/{stock_code}/finance",
        "as_of": as_of.isoformat(),
        "as_of_basis": "current_provider_snapshot",
        "fetched_at": None,
        "earnings": None,
        "equity": None,
        "roe": None,
        "roe_reason": "four_reported_income_quarters_or_roe_unavailable",
        "reason": reason,
    }


def _period(key) -> date | None:
    text = str(key or "")
    if not re.fullmatch(r"\d{6}", text):
        return None
    year, month = int(text[:4]), int(text[4:])
    try:
        return date(year, month, calendar.monthrange(year, month)[1])
    except (ValueError, OverflowError):
        return None


def _number(cell) -> float | None:
    if not isinstance(cell, dict):
        return None
    value = parse_number(cell.get("value"), zero_as_none=False)
    return value if value is not None and math.isfinite(value) else None


def parse_quarterly_valuation(payload: dict, stock_code: str, *, today: date | None = None) -> dict:
    """Build a current TTM/latest-quarter snapshot without annual fallback.

    Missing latest-quarter EPS/BPS never causes an older complete window to
    masquerade as the latest one. Zero and loss quarters count toward TTM.
    Provider EPS already represents a quarter, so it is summed directly;
    provider PER is not reused. The latest quarterly ROE is already a rolling
    four-quarter ratio per the provider's Financial Summary definition; it is
    never summed or approximated using EPS/BPS. Four actual income quarters
    must be present before accepting it.
    """
    today = today or _today()
    result = _empty(stock_code, "quarterly_financials_unavailable", as_of=today)
    if not isinstance(payload, dict) or payload.get("financePeriodType") != "quarter":
        return result
    info = payload.get("financeInfo")
    if not isinstance(info, dict) or str(payload.get("itemCode") or "") != stock_code or str(info.get("itemCode") or "") != stock_code:
        result["reason"] = "quarterly_financials_symbol_mismatch"
        return result

    periods: dict[date, str] = {}
    seen: set[str] = set()
    for column in info.get("trTitleList") or []:
        if not isinstance(column, dict):
            continue
        key = str(column.get("key") or "")
        if key in seen:
            result["reason"] = "ambiguous_quarter_columns"
            return result
        seen.add(key)
        period_end = _period(key)
        # Unknown flags are not evidence of actual reported results.
        if column.get("isConsensus") == "N" and period_end is not None and period_end <= today:
            periods[period_end] = key
    if not periods:
        result["reason"] = "reported_quarters_unavailable"
        return result
    ordered = sorted(periods)
    latest = ordered[-1]
    result["period_end"] = latest.isoformat()
    if (today - latest).days > _MAX_PERIOD_AGE_DAYS:
        result["reason"] = "reported_quarters_stale"
        return result

    rows: dict[str, dict] = {}
    for row in info.get("rowList") or []:
        if not isinstance(row, dict):
            continue
        title = str(row.get("title") or "").strip().upper()
        if title in {"EPS", "BPS", "ROE", "지배주주순이익", "당기순이익"}:
            if title in rows:
                result["reason"] = "ambiguous_per_share_rows"
                return result
            rows[title] = row.get("columns") if isinstance(row.get("columns"), dict) else {}

    latest_key = periods[latest]
    bps = _number(rows.get("BPS", {}).get(latest_key))
    if bps is not None:
        result["bps"] = bps
        result["equity"] = {
            "period_end": latest.isoformat(),
            "available_date": None,
            "method": "latest_reported_bps",
            "statement_basis": None,
            "source_url": result["source_url"],
        }

    quarters = ordered[-4:]
    consecutive = len(quarters) == 4 and all(
        (right.year * 12 + right.month) - (left.year * 12 + left.month) == 3
        for left, right in zip(quarters, quarters[1:])
    )
    values = [_number(rows.get("EPS", {}).get(periods[period])) for period in quarters]
    period_start = None
    if consecutive:
        first = quarters[0]
        first_month = first.month - 2
        start_year = first.year
        if first_month <= 0:
            first_month += 12
            start_year -= 1
        period_start = date(start_year, first_month, 1).isoformat()
    if consecutive and all(value is not None for value in values):
        result["eps_ttm"] = round(sum(values), 6)
        result["earnings"] = {
            "period_start": period_start,
            "period_end": latest.isoformat(),
            "available_date": None,
            "method": "sum_reported_quarter_eps",
            "quarter_count": 4,
            "quarters": [{"period_end": period.isoformat(), "eps": value} for period, value in zip(quarters, values)],
            "statement_basis": None,
            "source_url": result["source_url"],
        }

    owner_income = rows.get("지배주주순이익", {})
    income_title = "지배주주순이익" if any(
        _number(owner_income.get(periods[period])) is not None for period in quarters
    ) else "당기순이익"
    income_values = [_number(rows.get(income_title, {}).get(periods[period])) for period in quarters]
    reported_roe = _number(rows.get("ROE", {}).get(latest_key))
    if consecutive and all(value is not None for value in income_values) and reported_roe is not None:
        result["roe_ttm"] = reported_roe
        result["roe_reason"] = None
        result["roe"] = {
            "period_start": period_start,
            "period_end": latest.isoformat(),
            "quarter_count": 4,
            "method": "provider_reported_ttm_roe",
            "net_income_account": income_title,
            "source_url": result["source_url"],
            "definition_url": f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={stock_code}",
        }

    complete = result["eps_ttm"] is not None and result["bps"] is not None
    result["status"] = "ready" if complete else "partial" if result["eps_ttm"] is not None or result["bps"] is not None else "unavailable"
    result["reason"] = None if complete else "four_consecutive_reported_eps_unavailable" if result["eps_ttm"] is None else "latest_reported_bps_unavailable"
    return result


async def fetch_current_valuation_basis(stock_code: str, *, as_of: date | str | None = None, corp_code: str | None = None) -> dict:
    """Fetch cached financial inputs only; divide by a fresh quote at the caller.

    ``corp_code`` is accepted for callers that also fetch DART data. This
    provider uses the listed security code and requires no DART API key.
    """
    today = _today()
    stock_code = str(stock_code or "").strip().upper()
    if not re.fullmatch(r"[0-9A-Z]{6}", stock_code):
        return _empty(stock_code, "invalid_stock_code", as_of=today)
    try:
        requested = date.fromisoformat(as_of) if isinstance(as_of, str) else as_of or today
    except ValueError:
        return _empty(stock_code, "invalid_as_of", as_of=today)
    if requested != today:
        return _empty(stock_code, "historical_point_in_time_unavailable", as_of=requested)

    async def load() -> dict:
        try:
            client = await get_http_client("naver_bulk")
            response = await client.get(_API.format(stock_code=stock_code), headers={"User-Agent": "Mozilla/5.0"}, timeout=6.0)
            response.raise_for_status()
            result = parse_quarterly_valuation(response.json(), stock_code, today=today)
            result["fetched_at"] = datetime.now(_KST).isoformat(timespec="seconds")
            return result
        except Exception as exc:
            logger.debug("Quarterly valuation unavailable for %s: %s", stock_code, type(exc).__name__)
            return _empty(stock_code, "quarterly_provider_unavailable", as_of=today)

    return await cached_fetch(
        _cache, f"{stock_code}:{today.isoformat()}", load,
        ttl=lambda result: _NO_DATA_TTL_SECONDS if result.get("status") == "unavailable" else _CACHE_TTL_SECONDS,
    )
