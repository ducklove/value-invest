import asyncio
from datetime import datetime, timedelta
from statistics import median

import pandas as pd
import yfinance as yf


def _get_yfinance_aux(stock_code: str, start_year: int, end_year: int) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    """yfinance에서 종가/주식수/배당/분할 이력 조회."""
    ticker = yf.Ticker(f"{stock_code}.KS")
    history = ticker.history(
        start=f"{start_year}-01-01",
        end=f"{end_year}-12-31",
        interval="1mo",
        auto_adjust=False,
    )
    shares = ticker.get_shares_full(start="2015-01-01")
    dividends = ticker.dividends
    splits = ticker.splits
    close_series = history["Close"] if "Close" in history else pd.Series(dtype="float64")
    return close_series, shares, dividends, splits


def _get_weekly_yfinance_aux(stock_code: str, start_date: datetime, end_date: datetime) -> tuple[pd.Series, pd.Series, pd.Series]:
    """최근 주간 종가/주식수/배당 이력 조회."""
    ticker = yf.Ticker(f"{stock_code}.KS")
    history = ticker.history(
        start=start_date.strftime("%Y-%m-%d"),
        end=(end_date + timedelta(days=7)).strftime("%Y-%m-%d"),
        interval="1wk",
        auto_adjust=False,
    )
    shares = ticker.get_shares_full(start=(start_date - timedelta(days=370)).strftime("%Y-%m-%d"))
    dividends = ticker.dividends
    close_series = history["Close"] if "Close" in history else pd.Series(dtype="float64")
    return close_series, shares, dividends


def _safe_float(value):
    if value is None or pd.isna(value):
        return None
    try:
        numeric = float(value)
        return numeric if numeric != 0 else None
    except (TypeError, ValueError):
        return None


def _safe_div(numerator, denominator, multiply=1.0):
    if numerator is None or denominator is None or denominator == 0:
        return None
    return round(numerator / denominator * multiply, 2)


def _group_last_by_year(series: pd.Series | None) -> dict[int, float]:
    if series is None or series.empty:
        return {}
    cleaned = series.dropna()
    if cleaned.empty:
        return {}
    grouped = cleaned.groupby(cleaned.index.year).last()
    return {int(year): _safe_float(value) for year, value in grouped.items()}


def _group_sum_by_year(series: pd.Series | None) -> dict[int, float]:
    if series is None or series.empty:
        return {}
    cleaned = series.dropna()
    if cleaned.empty:
        return {}
    grouped = cleaned.groupby(cleaned.index.year).sum()
    return {int(year): _safe_float(value) for year, value in grouped.items()}


def _group_close_by_year(series: pd.Series | None) -> dict[int, float]:
    if series is None or series.empty:
        return {}
    cleaned = series.dropna()
    if cleaned.empty:
        return {}
    grouped = cleaned.groupby(cleaned.index.year).last()
    return {int(year): _safe_float(value) for year, value in grouped.items()}


def _normalize_datetime_index(series: pd.Series | None) -> pd.Series:
    if series is None or series.empty:
        return pd.Series(dtype="float64")
    normalized = series.dropna().copy()
    if normalized.empty:
        return pd.Series(dtype="float64")
    index = pd.DatetimeIndex(normalized.index)
    if index.tz is not None:
        index = index.tz_localize(None)
    normalized.index = index.normalize()
    normalized = normalized.groupby(normalized.index).last()
    return normalized.sort_index()


def _parse_report_date(value: str | None, year: int | None = None) -> datetime | None:
    if value:
        stripped = value.strip()
        for fmt in ("%Y%m%d", "%Y-%m-%d"):
            try:
                return datetime.strptime(stripped, fmt)
            except ValueError:
                continue
    if year is not None:
        return datetime(year + 1, 3, 31)
    return None


def _normalized_split_events(series: pd.Series | None) -> list[tuple[datetime, float]]:
    if series is None or series.empty:
        return []
    cleaned = series[series > 0].sort_index()
    events: list[tuple[datetime, float]] = []
    last_dt = None
    last_ratio = None
    for idx, ratio in cleaned.items():
        dt = idx.to_pydatetime()
        ratio = _safe_float(ratio)
        if ratio is None:
            continue
        if last_dt and last_ratio == ratio and abs((dt - last_dt).days) <= 30:
            continue
        events.append((dt, ratio))
        last_dt = dt
        last_ratio = ratio
    return events


def _adjust_shares_for_splits(shares_by_year: dict[int, float], split_events: list[tuple[datetime, float]]) -> dict[int, float]:
    adjusted = {}
    for year, shares in shares_by_year.items():
        current_shares = shares
        for dt, ratio in split_events:
            if dt.year <= year:
                continue
            future_scales = [value for future_year, value in shares_by_year.items() if future_year >= dt.year and value]
            if not future_scales:
                continue
            # yfinance 주식수 이력에는 이미 분할 보정된 연도가 섞여 있다.
            # 이후 연도 대표값 대비 충분히 작은 경우에만 분할 배수를 적용한다.
            post_split_scale = median(future_scales)
            raw_scale_ceiling = (post_split_scale / ratio) * 1.5
            if current_shares <= raw_scale_ceiling:
                current_shares *= ratio
        adjusted[year] = round(current_shares, 2) if current_shares is not None else None
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


def market_data_needs_refresh(data: list[dict]) -> bool:
    """기존 캐시에 파생 지표가 비어 있으면 재계산 대상."""
    if not data:
        return True
    keys = ("per", "pbr", "eps", "bps", "dividend_yield", "market_cap")
    return not any(row.get(key) is not None for row in data for key in keys)


async def fetch_market_data(
    stock_code: str,
    financial_data: list[dict] | None = None,
    start_year: int = 2000,
    end_year: int | None = None,
) -> list[dict]:
    """시장 데이터와 파생 지표를 조회한다.

    - 종가/주식수/배당: yfinance
    - EPS/BPS/PER/PBR/시총/배당수익률: 재무제표와 결합해 파생 계산
    """
    if end_year is None:
        end_year = datetime.now().year

    loop = asyncio.get_event_loop()
    aux_future = loop.run_in_executor(None, _get_yfinance_aux, stock_code, start_year, end_year)
    try:
        close_series, shares_series, dividends_series, splits_series = await aux_future
    except Exception:
        close_series = pd.Series(dtype="float64")
        shares_series = pd.Series(dtype="float64")
        dividends_series = pd.Series(dtype="float64")
        splits_series = pd.Series(dtype="float64")

    year_data: dict[int, dict] = {}
    for year, close_price in _group_close_by_year(close_series).items():
        year_data[year] = {"year": year, "close_price": close_price}

    shares_by_year = _group_last_by_year(shares_series)
    split_events = _normalized_split_events(splits_series)
    shares_by_year = _adjust_shares_for_splits(shares_by_year, split_events)
    dividends_by_year = _group_sum_by_year(dividends_series)
    fin_by_year = {item["year"]: item for item in (financial_data or [])}
    shares_by_year = _forward_fill_year_values(shares_by_year, sorted(year_data.keys()))

    for year, shares in shares_by_year.items():
        year_data.setdefault(year, {"year": year})["shares_outstanding"] = shares

    for year, dps in dividends_by_year.items():
        year_data.setdefault(year, {"year": year})["dividend_per_share"] = dps

    for year, row in year_data.items():
        close_price = row.get("close_price")
        shares = row.get("shares_outstanding")
        fin = fin_by_year.get(year, {})

        if close_price and shares:
            row["market_cap"] = round(close_price * shares, 2)

        if fin and shares:
            eps = _safe_div(fin.get("net_income"), shares)
            bps = _safe_div(fin.get("total_equity"), shares)
            row["eps"] = eps
            row["bps"] = bps
            row["per"] = _safe_div(close_price, eps) if close_price else None
            row["pbr"] = _safe_div(close_price, bps) if close_price else None

        dps = row.get("dividend_per_share")
        row["dividend_yield"] = _safe_div(dps, close_price, 100) if dps and close_price else None

        row.pop("shares_outstanding", None)
        row.pop("dividend_per_share", None)

    return [year_data[year] for year in sorted(year_data.keys())]


async def fetch_weekly_market_data(
    stock_code: str,
    financial_data: list[dict] | None = None,
    years: int = 3,
) -> list[dict]:
    """최근 N년 주간 밸류에이션 시계열을 계산한다."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=years * 370)

    loop = asyncio.get_event_loop()
    aux_future = loop.run_in_executor(None, _get_weekly_yfinance_aux, stock_code, start_date, end_date)
    try:
        close_series, shares_series, dividends_series = await aux_future
    except Exception:
        close_series = pd.Series(dtype="float64")
        shares_series = pd.Series(dtype="float64")
        dividends_series = pd.Series(dtype="float64")

    close_series = _normalize_datetime_index(close_series)
    if close_series.empty:
        return []

    week_dates = pd.DatetimeIndex(close_series.index)
    shares_series = _normalize_datetime_index(shares_series)
    dividends_series = _normalize_datetime_index(dividends_series)

    if not shares_series.empty:
        shares_by_week = shares_series.reindex(week_dates, method="ffill")
    else:
        shares_by_week = pd.Series(index=week_dates, dtype="float64")

    financial_timeline = []
    for item in financial_data or []:
        report_date = _parse_report_date(item.get("report_date"), item.get("year"))
        if report_date is None:
            continue
        financial_timeline.append((report_date, item))
    financial_timeline.sort(key=lambda x: x[0])

    results = []
    active_financial = None
    timeline_index = 0

    for week_date, close_price in close_series.items():
        while timeline_index < len(financial_timeline) and financial_timeline[timeline_index][0] <= week_date.to_pydatetime():
            active_financial = financial_timeline[timeline_index][1]
            timeline_index += 1

        shares = _safe_float(shares_by_week.get(week_date))
        close_value = _safe_float(close_price)
        row = {
            "date": week_date.strftime("%Y-%m-%d"),
            "close_price": close_value,
            "per": None,
            "pbr": None,
            "eps": None,
            "bps": None,
            "dividend_yield": None,
            "market_cap": round(close_value * shares, 2) if close_value and shares else None,
        }

        if active_financial and shares:
            eps = _safe_div(active_financial.get("net_income"), shares)
            bps = _safe_div(active_financial.get("total_equity"), shares)
            row["eps"] = eps
            row["bps"] = bps
            row["per"] = _safe_div(close_value, eps) if close_value else None
            row["pbr"] = _safe_div(close_value, bps) if close_value else None

        if not dividends_series.empty and close_value:
            trailing_start = week_date - pd.Timedelta(days=365)
            trailing_dividends = dividends_series[(dividends_series.index > trailing_start) & (dividends_series.index <= week_date)].sum()
            row["dividend_yield"] = _safe_div(_safe_float(trailing_dividends), close_value, 100)

        results.append(row)

    return results
