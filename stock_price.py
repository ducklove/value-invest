from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta
from statistics import median

try:
    import pandas as pd
except ImportError:  # pragma: no cover - optional dependency
    pd = None

try:
    import yfinance as yf
except ImportError:  # pragma: no cover - optional dependency
    yf = None

import kis_proxy_client
import kis_ws_manager


logger = logging.getLogger(__name__)

KRW_PER_EOK = 100_000_000


def _safe_float(value, *, zero_as_none: bool = True):
    if value in (None, ""):
        return None
    try:
        numeric = float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    if zero_as_none and numeric == 0:
        return None
    return numeric


def _get_first(mapping: dict | None, *keys: str):
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return value
    return None


def _quote_summary(payload: dict | None) -> dict:
    if not isinstance(payload, dict):
        return {}
    summary = payload.get("summary")
    return summary if isinstance(summary, dict) else payload


def _safe_div(numerator, denominator, multiply=1.0):
    if numerator is None or denominator is None or denominator == 0:
        return None
    return round(numerator / denominator * multiply, 2)


def _empty_series():
    if pd is None:
        return None
    return pd.Series(dtype="float64")


def _get_history_close_series(history):
    if pd is None or history is None or history.empty:
        return _empty_series()
    for column in ("Adj Close", "Close"):
        if column in history:
            return history[column]
    return _empty_series()


def _get_yfinance_aux(stock_code: str, start_year: int, end_year: int):
    if yf is None:
        raise RuntimeError("yfinance is not available")
    ticker = yf.Ticker(f"{stock_code}.KS")
    history = ticker.history(
        start=f"{start_year}-01-01",
        end=f"{end_year}-12-31",
        interval="1mo",
        auto_adjust=False,
    )
    shares = ticker.get_shares_full(start=f"{max(start_year - 1, 2000)}-01-01")
    dividends = ticker.dividends
    splits = ticker.splits
    return _get_history_close_series(history), shares, dividends, splits


def _get_weekly_yfinance_aux(stock_code: str, start_date: datetime, end_date: datetime):
    if yf is None:
        raise RuntimeError("yfinance is not available")
    ticker = yf.Ticker(f"{stock_code}.KS")
    history = ticker.history(
        start=start_date.strftime("%Y-%m-%d"),
        end=(end_date + timedelta(days=7)).strftime("%Y-%m-%d"),
        interval="1wk",
        auto_adjust=False,
    )
    shares = ticker.get_shares_full(start=(start_date - timedelta(days=370)).strftime("%Y-%m-%d"))
    dividends = ticker.dividends
    splits = ticker.splits
    return _get_history_close_series(history), shares, dividends, splits


def _normalize_datetime_index(series):
    if pd is None or series is None or series.empty:
        return _empty_series()
    normalized = series.dropna().copy()
    if normalized.empty:
        return _empty_series()
    index = pd.DatetimeIndex(normalized.index)
    if index.tz is not None:
        index = index.tz_localize(None)
    normalized.index = index.normalize()
    normalized = normalized.groupby(normalized.index).last()
    return normalized.sort_index()


def _group_last_by_year_series(series) -> dict[int, float]:
    if pd is None or series is None or series.empty:
        return {}
    cleaned = series.dropna()
    if cleaned.empty:
        return {}
    grouped = cleaned.groupby(cleaned.index.year).last()
    return {int(year): _safe_float(value, zero_as_none=False) for year, value in grouped.items()}


def _group_sum_by_year_series(series) -> dict[int, float]:
    if pd is None or series is None or series.empty:
        return {}
    cleaned = series.dropna()
    if cleaned.empty:
        return {}
    grouped = cleaned.groupby(cleaned.index.year).sum()
    return {int(year): _safe_float(value, zero_as_none=False) for year, value in grouped.items()}


def _group_close_by_year_series(series) -> dict[int, float]:
    if pd is None or series is None or series.empty:
        return {}
    cleaned = series.dropna()
    if cleaned.empty:
        return {}
    grouped = cleaned.groupby(cleaned.index.year).last()
    return {int(year): _safe_float(value, zero_as_none=False) for year, value in grouped.items()}


def _normalized_split_events(series) -> list[tuple[datetime, float]]:
    if pd is None or series is None or series.empty:
        return []
    cleaned = series[series > 0].sort_index()
    events: list[tuple[datetime, float]] = []
    last_dt = None
    last_ratio = None
    for idx, ratio in cleaned.items():
        dt = idx.to_pydatetime() if hasattr(idx, "to_pydatetime") else idx
        normalized_ratio = _safe_float(ratio, zero_as_none=False)
        if normalized_ratio is None:
            continue
        if last_dt and last_ratio == normalized_ratio and abs((dt - last_dt).days) <= 30:
            continue
        events.append((dt, normalized_ratio))
        last_dt = dt
        last_ratio = normalized_ratio
    return events


def _adjust_shares_for_splits(shares_by_year: dict[int, float], split_events: list[tuple[datetime, float]]) -> dict[int, float]:
    adjusted = {}
    for year, shares in shares_by_year.items():
        current_shares = shares
        for dt, ratio in split_events:
            if dt.year <= year or current_shares is None:
                continue
            future_scales = [value for future_year, value in shares_by_year.items() if future_year >= dt.year and value]
            if not future_scales:
                continue
            post_split_scale = median(future_scales)
            raw_scale_ceiling = (post_split_scale / ratio) * 1.5
            if current_shares <= raw_scale_ceiling:
                current_shares *= ratio
        adjusted[year] = round(current_shares, 2) if current_shares is not None else None
    return adjusted


def _split_adjustment_factor(ratio: float | None) -> float:
    ratio = _safe_float(ratio, zero_as_none=False)
    if ratio is None or ratio == 1:
        return 1.0
    return ratio if ratio < 1 else 1 / ratio


def _adjust_dividends_for_splits(dividends, split_events: list[tuple[datetime, float]]):
    if pd is None or dividends is None or dividends.empty or not split_events:
        return dividends if dividends is not None else _empty_series()

    adjusted = dividends.copy()
    for split_dt, ratio in split_events:
        split_ts = pd.Timestamp(split_dt)
        if getattr(adjusted.index, "tz", None) is None and split_ts.tz is not None:
            split_ts = split_ts.tz_localize(None)
        elif getattr(adjusted.index, "tz", None) is not None and split_ts.tz is None:
            split_ts = split_ts.tz_localize(adjusted.index.tz)
        factor = _split_adjustment_factor(ratio)
        if factor == 1:
            continue
        mask = adjusted.index < split_ts
        if mask.any():
            adjusted.loc[mask] = adjusted.loc[mask] * factor
    return adjusted


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    stripped = value.strip()
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(stripped, fmt).date()
        except ValueError:
            continue
    return None


def _parse_report_date(value: str | None, year: int | None = None) -> datetime | None:
    parsed = _parse_date(value)
    if parsed:
        return datetime.combine(parsed, datetime.min.time())
    if year is not None:
        return datetime(year + 1, 3, 31)
    return None


def _parse_year(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(str(value)[:4])
    except (TypeError, ValueError):
        return None


def _sorted_history_items(items: list[dict] | None) -> list[dict]:
    return sorted(
        items or [],
        key=lambda item: _parse_date(
            _get_first(item, "stck_bsop_date", "date", "trade_date", "business_date")
        )
        or date.min,
    )


def _group_close_by_year(items: list[dict] | None) -> dict[int, float]:
    grouped: dict[int, float] = {}
    for item in _sorted_history_items(items):
        trade_date = _parse_date(_get_first(item, "stck_bsop_date", "date", "trade_date", "business_date"))
        close_price = _safe_float(_get_first(item, "stck_clpr", "close_price", "close"))
        if trade_date and close_price is not None:
            grouped[trade_date.year] = close_price
    return grouped


def _group_dividends_by_year(items: list[dict] | None) -> dict[int, float]:
    grouped: dict[int, float] = {}
    for item in items or []:
        record_date = _parse_date(_get_first(item, "record_date", "dividend_date", "date"))
        dividend = _safe_float(_get_first(item, "per_sto_divi_amt", "dividend_per_share", "cash_dividend_per_share"))
        if record_date and dividend is not None:
            grouped[record_date.year] = round(grouped.get(record_date.year, 0.0) + dividend, 2)
    return grouped


def _build_dividend_events(items: list[dict] | None) -> list[tuple[date, float]]:
    events = []
    for item in items or []:
        record_date = _parse_date(_get_first(item, "record_date", "dividend_date", "date"))
        dividend = _safe_float(_get_first(item, "per_sto_divi_amt", "dividend_per_share", "cash_dividend_per_share"))
        if record_date and dividend is not None:
            events.append((record_date, dividend))
    return sorted(events, key=lambda item: item[0])


def _estimate_price_adjustment_factors(
    adjusted_close_by_year: dict[int, float],
    raw_close_by_year: dict[int, float],
) -> dict[int, float]:
    factors: dict[int, float] = {}
    for year, adjusted_close in adjusted_close_by_year.items():
        raw_close = raw_close_by_year.get(year)
        if raw_close is None or adjusted_close in (None, 0):
            continue
        factor = raw_close / adjusted_close
        if factor > 0:
            factors[year] = round(factor, 6)
    return factors


def _adjust_dividends_by_price_factors(
    dividends_by_year: dict[int, float],
    price_adjustment_factors: dict[int, float],
) -> dict[int, float]:
    adjusted: dict[int, float] = {}
    for year, dividend in dividends_by_year.items():
        factor = price_adjustment_factors.get(year)
        if factor and factor > 0:
            adjusted[year] = round(dividend / factor, 2)
        else:
            adjusted[year] = dividend
    return adjusted


def _forward_fill_year_values(values_by_year: dict[int, float], target_years: list[int]) -> dict[int, float]:
    if not values_by_year:
        return {}
    filled = dict(values_by_year)
    last_value = None
    for year in sorted(target_years):
        current = filled.get(year)
        if current is not None:
            last_value = current
            continue
        if last_value is not None:
            filled[year] = last_value
    return filled


def _normalize_financial_rows(
    financials_payload: dict,
    listed_shares: float | None,
) -> dict[int, dict]:
    balance_by_year: dict[int, dict] = {}
    ratio_by_year: dict[int, dict] = {}

    for row in financials_payload.get("balance_sheet", []):
        year = _parse_year(_get_first(row, "stac_yymm", "year"))
        if year is not None:
            balance_by_year[year] = row

    for row in financials_payload.get("financial_ratio", []):
        year = _parse_year(_get_first(row, "stac_yymm", "year"))
        if year is not None:
            ratio_by_year[year] = row

    implied_shares_by_year: dict[int, float] = {}
    for year in sorted(set(balance_by_year) | set(ratio_by_year)):
        total_equity_eok = _safe_float(_get_first(balance_by_year.get(year, {}), "total_cptl", "total_equity"))
        bps = _safe_float(_get_first(ratio_by_year.get(year, {}), "bps"))
        if total_equity_eok and bps:
            implied_shares_by_year[year] = round(total_equity_eok * KRW_PER_EOK / bps, 2)

    shares_by_year: dict[int, float] = {}
    if listed_shares and implied_shares_by_year:
        latest_year = max(implied_shares_by_year)
        latest_implied = implied_shares_by_year.get(latest_year)
        if latest_implied:
            scale = listed_shares / latest_implied
            shares_by_year = {
                year: round(value * scale, 2)
                for year, value in implied_shares_by_year.items()
            }
    elif listed_shares:
        base_years = sorted(set(balance_by_year) | set(ratio_by_year))
        shares_by_year = {year: listed_shares for year in base_years}
    else:
        shares_by_year = implied_shares_by_year

    normalized = {}
    for year in sorted(set(balance_by_year) | set(ratio_by_year)):
        normalized[year] = {
            "eps": _safe_float(_get_first(ratio_by_year.get(year, {}), "eps")),
            "bps": _safe_float(_get_first(ratio_by_year.get(year, {}), "bps")),
            "shares_outstanding": shares_by_year.get(year),
        }
    return normalized


def _derive_normalized_financial_rows(
    financial_data: list[dict] | None,
    listed_shares: float | None,
) -> dict[int, dict]:
    normalized: dict[int, dict] = {}
    for item in financial_data or []:
        year = item.get("year")
        if year is None:
            continue
        shares = listed_shares
        eps = None
        bps = None
        net_income = _safe_float(item.get("net_income"), zero_as_none=False)
        total_equity = _safe_float(item.get("total_equity"), zero_as_none=False)
        if shares and shares > 0:
            if net_income is not None:
                eps = round(net_income / shares, 2)
            if total_equity is not None:
                bps = round(total_equity / shares, 2)
        normalized[year] = {
            "eps": eps,
            "bps": bps,
            "shares_outstanding": shares,
        }
    return normalized


def _merge_normalized_financial_rows(
    primary: dict[int, dict],
    fallback: dict[int, dict],
) -> dict[int, dict]:
    merged: dict[int, dict] = {}
    for year in sorted(set(primary) | set(fallback)):
        row = dict(fallback.get(year, {}))
        row.update({key: value for key, value in primary.get(year, {}).items() if value is not None})
        merged[year] = row
    return merged


def _extract_kis_financial_data(financials_payload: dict) -> list[dict]:
    balance_by_year: dict[int, dict] = {}
    income_by_year: dict[int, dict] = {}

    for row in financials_payload.get("balance_sheet", []):
        year = _parse_year(_get_first(row, "stac_yymm", "year"))
        if year is not None:
            balance_by_year[year] = row

    for row in financials_payload.get("income_statement", []):
        year = _parse_year(_get_first(row, "stac_yymm", "year"))
        if year is not None:
            income_by_year[year] = row

    extracted = []
    for year in sorted(set(balance_by_year) | set(income_by_year)):
        balance = balance_by_year.get(year, {})
        income = income_by_year.get(year, {})
        row = {
            "year": year,
            "revenue": _safe_float(_get_first(income, "sale_account", "revenue")),
            "operating_profit": _safe_float(_get_first(income, "op_prfi", "operating_profit")),
            "net_income": _safe_float(_get_first(income, "thtr_ntin", "net_income")),
            "total_assets": _safe_float(_get_first(balance, "total_aset", "total_assets")),
            "total_liabilities": _safe_float(_get_first(balance, "total_lblt", "total_liabilities")),
            "total_equity": _safe_float(_get_first(balance, "total_cptl", "total_equity")),
        }
        if any(row.get(key) is not None for key in row if key != "year"):
            extracted.append(row)
    return extracted


def _merge_financial_data(
    existing_rows: list[dict] | None,
    supplemental_rows: list[dict] | None,
) -> list[dict]:
    merged: dict[int, dict] = {}

    for row in supplemental_rows or []:
        year = row.get("year")
        if year is None:
            continue
        merged[year] = dict(row)

    for row in existing_rows or []:
        year = row.get("year")
        if year is None:
            continue
        target = merged.setdefault(year, {"year": year})
        for key, value in row.items():
            if key == "year" or value in (None, ""):
                continue
            target[key] = value

    return [merged[year] for year in sorted(merged)]


async def ensure_financial_data_coverage(
    stock_code: str,
    financial_data: list[dict] | None = None,
) -> list[dict]:
    existing_rows = [dict(item) for item in (financial_data or [])]
    try:
        financials_payload = await kis_proxy_client.get_financials(stock_code)
    except Exception as exc:
        logger.warning("장기 재무 데이터 보강 실패(%s): %s", stock_code, exc)
        return existing_rows
    return _merge_financial_data(existing_rows, _extract_kis_financial_data(financials_payload))


def _financial_timeline(
    financial_data: list[dict] | None,
    normalized_financials: dict[int, dict],
) -> list[tuple[datetime, dict]]:
    report_dates = {}
    for item in financial_data or []:
        year = item.get("year")
        if year is None:
            continue
        report_date = _parse_report_date(item.get("report_date"), year)
        if report_date:
            report_dates[year] = report_date

    timeline = []
    for year, row in normalized_financials.items():
        report_date = report_dates.get(year) or _parse_report_date(None, year)
        if report_date:
            timeline.append((report_date, {"year": year, **row}))
    timeline.sort(key=lambda item: item[0])
    return timeline


def _sum_trailing_dividends(
    dividend_events: list[tuple[date, float]],
    current_date: date,
    days: int = 365,
) -> float | None:
    trailing_start = current_date - timedelta(days=days)
    total = sum(
        amount
        for event_date, amount in dividend_events
        if trailing_start < event_date <= current_date
    )
    return round(total, 2) if total else None


def market_data_needs_refresh(data: list[dict]) -> bool:
    if not data:
        return True
    keys = ("per", "pbr", "eps", "bps", "dividend_per_share", "dividend_yield", "market_cap")
    return not any(row.get(key) is not None for row in data for key in keys)


async def fetch_market_data(
    stock_code: str,
    financial_data: list[dict] | None = None,
    start_year: int = 2000,
    end_year: int | None = None,
) -> list[dict]:
    if end_year is None:
        end_year = datetime.now().year

    start_date = date(start_year, 1, 1)
    end_date = date(end_year, 12, 31)

    loop = asyncio.get_event_loop()
    yf_future = loop.run_in_executor(None, _get_yfinance_aux, stock_code, start_year, end_year)
    adjusted_history_payload, raw_history_payload, dividends_payload, financials_payload, quote_payload = await asyncio.gather(
        kis_proxy_client.get_history(
            stock_code,
            start_date=start_date,
            end_date=end_date,
            period="Y",
            adjusted=True,
        ),
        kis_proxy_client.get_history(
            stock_code,
            start_date=start_date,
            end_date=end_date,
            period="Y",
            adjusted=False,
        ),
        kis_proxy_client.get_dividends(
            stock_code,
            start_date=start_date,
            end_date=end_date,
        ),
        kis_proxy_client.get_financials(stock_code),
        kis_proxy_client.get_quote(stock_code),
        return_exceptions=True,
    )

    if isinstance(adjusted_history_payload, Exception):
        logger.warning("KIS adjusted history fetch failed (%s): %s", stock_code, adjusted_history_payload)
        adjusted_history_payload = {}
    if isinstance(raw_history_payload, Exception):
        logger.warning("KIS raw history fetch failed (%s): %s", stock_code, raw_history_payload)
        raw_history_payload = {}
    if isinstance(dividends_payload, Exception):
        logger.warning("KIS dividend fetch failed (%s): %s", stock_code, dividends_payload)
        dividends_payload = {}
    if isinstance(financials_payload, Exception):
        logger.warning("KIS financial fetch failed (%s): %s", stock_code, financials_payload)
        financials_payload = {}
    if isinstance(quote_payload, Exception):
        logger.warning("현재 상장주식수 조회 실패(%s): %s", stock_code, quote_payload)
        quote_payload = {}

    try:
        close_series, shares_series, dividends_series, splits_series = await yf_future
    except Exception as exc:
        logger.warning("yfinance 연간 시계열 조회 실패(%s): %s", stock_code, exc)
        close_series = _empty_series()
        shares_series = _empty_series()
        dividends_series = _empty_series()
        splits_series = _empty_series()

    close_by_year = _group_close_by_year_series(close_series)
    split_events = _normalized_split_events(splits_series)
    shares_by_year = _adjust_shares_for_splits(_group_last_by_year_series(shares_series), split_events)
    yfinance_dividends_by_year = _group_sum_by_year_series(dividends_series)

    kis_close_by_year = _group_close_by_year(adjusted_history_payload.get("items"))
    kis_raw_close_by_year = _group_close_by_year(raw_history_payload.get("items"))
    kis_raw_dividends_by_year = _group_dividends_by_year(dividends_payload.get("items"))
    kis_dividends_by_year = _adjust_dividends_by_price_factors(
        kis_raw_dividends_by_year,
        _estimate_price_adjustment_factors(kis_close_by_year, kis_raw_close_by_year),
    )

    if not close_by_year:
        close_by_year = kis_close_by_year

    dividends_by_year = {
        year: yfinance_dividends_by_year.get(year, kis_dividends_by_year.get(year))
        for year in sorted(set(yfinance_dividends_by_year) | set(kis_dividends_by_year))
        if yfinance_dividends_by_year.get(year) is not None or kis_dividends_by_year.get(year) is not None
    }

    listed_shares = _safe_float(_get_first(_quote_summary(quote_payload), "listed_shares", "lstn_stcn"))
    normalized_financials = _merge_normalized_financial_rows(
        _normalize_financial_rows(financials_payload, listed_shares),
        _derive_normalized_financial_rows(financial_data, listed_shares),
    )

    target_years = sorted(
        year
        for year in set(close_by_year) | set(dividends_by_year) | set(normalized_financials)
        if start_year <= year <= end_year
    )
    kis_shares_by_year = {
        year: row.get("shares_outstanding")
        for year, row in normalized_financials.items()
        if row.get("shares_outstanding") is not None
    }
    shares_by_year = _forward_fill_year_values(
        {
            year: shares_by_year.get(year) or kis_shares_by_year.get(year)
            for year in target_years
            if shares_by_year.get(year) is not None or kis_shares_by_year.get(year) is not None
        },
        target_years,
    )

    results = []
    for year in target_years:
        close_price = close_by_year.get(year)
        eps = normalized_financials.get(year, {}).get("eps")
        bps = normalized_financials.get(year, {}).get("bps")
        shares = shares_by_year.get(year)
        dps = dividends_by_year.get(year)
        if dps is None and close_price is not None:
            dps = 0.0
        results.append(
            {
                "year": year,
                "close_price": close_price,
                "per": _safe_div(close_price, eps) if close_price else None,
                "pbr": _safe_div(close_price, bps) if close_price else None,
                "eps": eps,
                "bps": bps,
                "dividend_per_share": dps,
                "dividend_yield": _safe_div(dps, close_price, 100)
                if dps is not None and close_price
                else None,
                "market_cap": round(close_price * shares, 2) if close_price and shares else None,
            }
        )

    return results


async def fetch_weekly_market_data(
    stock_code: str,
    financial_data: list[dict] | None = None,
    years: int = 10,
) -> list[dict]:
    end_date = date.today()
    start_date = end_date - timedelta(days=years * 370)
    loop = asyncio.get_event_loop()
    yf_future = loop.run_in_executor(
        None,
        _get_weekly_yfinance_aux,
        stock_code,
        datetime.combine(start_date, datetime.min.time()),
        datetime.combine(end_date, datetime.min.time()),
    )
    financials_payload, dividends_payload = await asyncio.gather(
        kis_proxy_client.get_financials(stock_code),
        kis_proxy_client.get_dividends(
            stock_code,
            start_date=start_date,
            end_date=end_date,
        ),
        return_exceptions=True,
    )
    if isinstance(financials_payload, Exception):
        logger.warning("KIS weekly financial fetch failed (%s): %s", stock_code, financials_payload)
        financials_payload = {}
    if isinstance(dividends_payload, Exception):
        logger.warning("KIS weekly dividend fetch failed (%s): %s", stock_code, dividends_payload)
        dividends_payload = {}
    normalized_financials = _merge_normalized_financial_rows(
        _normalize_financial_rows(financials_payload, None),
        _derive_normalized_financial_rows(financial_data, None),
    )
    financial_timeline = _financial_timeline(financial_data, normalized_financials)
    kis_dividend_events = _build_dividend_events(dividends_payload.get("items"))

    try:
        close_series, shares_series, dividends_series, splits_series = await yf_future
    except Exception as exc:
        logger.warning("yfinance 주간 시계열 조회 실패(%s): %s", stock_code, exc)
        close_series = _empty_series()
        shares_series = _empty_series()
        dividends_series = _empty_series()
        splits_series = _empty_series()

    close_series = _normalize_datetime_index(close_series)
    shares_series = _normalize_datetime_index(shares_series)
    dividends_series = _normalize_datetime_index(dividends_series)

    if close_series is None or close_series.empty:
        history_payload = await kis_proxy_client.get_history(
            stock_code,
            start_date=start_date,
            end_date=end_date,
            period="W",
            adjusted=True,
        )
        results = []
        active_financial = None
        timeline_index = 0

        for item in _sorted_history_items(history_payload.get("items")):
            trade_date = _parse_date(_get_first(item, "stck_bsop_date", "date", "trade_date", "business_date"))
            if trade_date is None:
                continue

            trade_datetime = datetime.combine(trade_date, datetime.min.time())
            while timeline_index < len(financial_timeline) and financial_timeline[timeline_index][0] <= trade_datetime:
                active_financial = financial_timeline[timeline_index][1]
                timeline_index += 1

            close_price = _safe_float(_get_first(item, "stck_clpr", "close_price", "close"))
            eps = active_financial.get("eps") if active_financial else None
            bps = active_financial.get("bps") if active_financial else None
            shares = active_financial.get("shares_outstanding") if active_financial else None
            trailing_dividends = _sum_trailing_dividends(kis_dividend_events, trade_date)
            if trailing_dividends is None and kis_dividend_events and close_price is not None:
                trailing_dividends = 0.0

            results.append(
                {
                    "date": trade_date.isoformat(),
                    "close_price": close_price,
                    "per": _safe_div(close_price, eps) if close_price else None,
                    "pbr": _safe_div(close_price, bps) if close_price else None,
                    "eps": eps,
                    "bps": bps,
                    "dividend_yield": _safe_div(trailing_dividends, close_price, 100)
                    if trailing_dividends is not None and close_price
                    else None,
                    "market_cap": round(close_price * shares, 2) if close_price and shares else None,
                }
            )

        return results

    week_dates = pd.DatetimeIndex(close_series.index) if pd is not None else []
    if pd is not None and shares_series is not None and not shares_series.empty:
        shares_by_week = shares_series.reindex(week_dates, method="ffill")
    elif pd is not None:
        shares_by_week = pd.Series(index=week_dates, dtype="float64")
    else:
        shares_by_week = None

    results = []
    active_financial = None
    timeline_index = 0
    has_yfinance_dividend_source = pd is not None and dividends_series is not None and not dividends_series.empty
    has_kis_dividend_source = bool(kis_dividend_events)

    for week_date, close_price in close_series.items():
        trade_datetime = week_date.to_pydatetime() if hasattr(week_date, "to_pydatetime") else week_date
        while timeline_index < len(financial_timeline) and financial_timeline[timeline_index][0] <= trade_datetime:
            active_financial = financial_timeline[timeline_index][1]
            timeline_index += 1

        close_value = _safe_float(close_price, zero_as_none=False)
        eps = active_financial.get("eps") if active_financial else None
        bps = active_financial.get("bps") if active_financial else None
        active_shares = active_financial.get("shares_outstanding") if active_financial else None
        shares = _safe_float(shares_by_week.get(week_date), zero_as_none=False) if shares_by_week is not None else active_shares
        if shares is None:
            shares = active_shares

        trailing_dividends = None
        if has_yfinance_dividend_source:
            trailing_start = week_date - pd.Timedelta(days=365)
            trailing_value = dividends_series[(dividends_series.index > trailing_start) & (dividends_series.index <= week_date)].sum()
            trailing_dividends = _safe_float(trailing_value, zero_as_none=False)
        elif has_kis_dividend_source:
            trailing_dividends = _sum_trailing_dividends(kis_dividend_events, trade_datetime.date())

        if trailing_dividends is None and close_value is not None and (has_yfinance_dividend_source or has_kis_dividend_source):
            trailing_dividends = 0.0

        results.append(
            {
                "date": week_date.strftime("%Y-%m-%d"),
                "close_price": close_value,
                "per": _safe_div(close_value, eps) if close_value else None,
                "pbr": _safe_div(close_value, bps) if close_value else None,
                "eps": eps,
                "bps": bps,
                "dividend_yield": _safe_div(trailing_dividends, close_value, 100)
                if trailing_dividends is not None and close_value
                else None,
                "market_cap": round(close_value * shares, 2) if close_value and shares else None,
            }
        )

    return results


async def fetch_quote_snapshot(stock_code: str) -> dict:
    ws_quote = kis_ws_manager.get_cached_quote(stock_code)
    if ws_quote and ws_quote.get("price") is not None:
        return {
            "date": ws_quote.get("date", date.today().isoformat()),
            "price": ws_quote["price"],
            "previous_close": ws_quote.get("previous_close"),
            "change": ws_quote.get("change"),
            "change_pct": ws_quote.get("change_pct"),
        }

    end_date = date.today()
    start_date = end_date - timedelta(days=14)

    quote_payload, history_payload = await asyncio.gather(
        kis_proxy_client.get_quote(stock_code),
        kis_proxy_client.get_history(
            stock_code,
            start_date=start_date,
            end_date=end_date,
            period="D",
            adjusted=True,
        ),
    )

    summary = _quote_summary(quote_payload)
    history_items = _sorted_history_items(history_payload.get("items"))
    latest_history_date = (
        _parse_date(_get_first(history_items[-1], "stck_bsop_date", "date", "trade_date", "business_date"))
        if history_items
        else None
    )

    latest_price = _safe_float(
        _get_first(summary, "current_price", "price", "stck_prpr"),
        zero_as_none=False,
    )
    change = _safe_float(
        _get_first(summary, "change", "price_change", "prdy_vrss"),
        zero_as_none=False,
    )
    change_pct = _safe_float(
        _get_first(summary, "change_rate", "change_pct", "prdy_ctrt"),
        zero_as_none=False,
    )
    previous_close = _safe_float(
        _get_first(summary, "previous_close", "base_price", "stck_sdpr"),
        zero_as_none=False,
    )

    if previous_close is None:
        if latest_price is not None and change is not None:
            previous_close = round(latest_price - change, 2)
        elif len(history_items) >= 2:
            previous_close = _safe_float(
                _get_first(history_items[-2], "stck_clpr", "close_price", "close"),
                zero_as_none=False,
            )

    if latest_price is None and history_items:
        latest_price = _safe_float(
            _get_first(history_items[-1], "stck_clpr", "close_price", "close"),
            zero_as_none=False,
        )

    if change is None and latest_price is not None and previous_close is not None:
        change = round(latest_price - previous_close, 2)
        change_pct = _safe_div(latest_price - previous_close, previous_close, 100)

    return {
        "date": (latest_history_date or end_date).isoformat(),
        "price": latest_price,
        "previous_close": previous_close,
        "change": change,
        "change_pct": change_pct,
    }
