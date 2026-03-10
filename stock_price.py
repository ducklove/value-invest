import asyncio
from datetime import datetime
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

    - 종가: pykrx
    - 주식수/배당: yfinance
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

    for year, shares in shares_by_year.items():
        year_data.setdefault(year, {"year": year})["shares_outstanding"] = shares

    for year, dps in dividends_by_year.items():
        year_data.setdefault(year, {"year": year})["dividend_per_share"] = dps

    for year, row in year_data.items():
        close_price = row.get("close_price")
        shares = row.get("shares_outstanding")
        fin = fin_by_year.get(year, {})

        if fin and shares:
            eps = _safe_div(fin.get("net_income"), shares)
            bps = _safe_div(fin.get("total_equity"), shares)
            row["eps"] = eps
            row["bps"] = bps
            row["per"] = _safe_div(close_price, eps) if close_price else None
            row["pbr"] = _safe_div(close_price, bps) if close_price else None
            row["market_cap"] = round(close_price * shares, 2) if close_price else None

        dps = row.get("dividend_per_share")
        row["dividend_yield"] = _safe_div(dps, close_price, 100) if dps and close_price else None

        row.pop("shares_outstanding", None)
        row.pop("dividend_per_share", None)

    return [year_data[year] for year in sorted(year_data.keys())]
