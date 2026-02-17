import asyncio
from functools import partial
from datetime import datetime

import pandas as pd
from pykrx import stock as pykrx_stock


def _get_market_fundamental(stock_code: str, start: str, end: str) -> pd.DataFrame:
    """pykrx에서 PER/PBR/EPS/BPS/배당수익률 조회 (동기)."""
    df = pykrx_stock.get_market_fundamental_by_date(
        start, end, stock_code, freq="y"
    )
    return df


def _get_market_ohlcv(stock_code: str, start: str, end: str) -> pd.DataFrame:
    """pykrx에서 OHLCV 조회 (동기)."""
    df = pykrx_stock.get_market_ohlcv_by_date(
        start, end, stock_code, freq="y"
    )
    return df


def _get_market_cap(stock_code: str, start: str, end: str) -> pd.DataFrame:
    """pykrx에서 시가총액 조회 (동기)."""
    df = pykrx_stock.get_market_cap_by_date(
        start, end, stock_code, freq="y"
    )
    return df


async def fetch_market_data(
    stock_code: str, start_year: int = 2000, end_year: int | None = None
) -> list[dict]:
    """pykrx에서 시장 데이터를 비동기로 가져온다."""
    if end_year is None:
        end_year = datetime.now().year

    start = f"{start_year}0101"
    end = f"{end_year}1231"

    loop = asyncio.get_event_loop()

    fund_df, ohlcv_df, cap_df = await asyncio.gather(
        loop.run_in_executor(None, partial(_get_market_fundamental, stock_code, start, end)),
        loop.run_in_executor(None, partial(_get_market_ohlcv, stock_code, start, end)),
        loop.run_in_executor(None, partial(_get_market_cap, stock_code, start, end)),
    )

    results = []
    years_seen = set()

    for df, is_fund, is_ohlcv, is_cap in [
        (fund_df, True, False, False),
        (ohlcv_df, False, True, False),
        (cap_df, False, False, True),
    ]:
        if df is None or df.empty:
            continue
        for idx, row in df.iterrows():
            year = idx.year if hasattr(idx, "year") else int(str(idx)[:4])
            if year not in years_seen:
                years_seen.add(year)

    all_years = sorted(years_seen) if years_seen else list(range(start_year, end_year + 1))

    year_data = {}
    for y in all_years:
        year_data[y] = {"year": y}

    def _safe_float(val):
        if pd.isna(val):
            return None
        try:
            v = float(val)
            return v if v != 0 else None
        except (ValueError, TypeError):
            return None

    if fund_df is not None and not fund_df.empty:
        for idx, row in fund_df.iterrows():
            year = idx.year if hasattr(idx, "year") else int(str(idx)[:4])
            if year not in year_data:
                year_data[year] = {"year": year}
            d = year_data[year]
            if "BPS" in row.index:
                d["bps"] = _safe_float(row["BPS"])
            if "PER" in row.index:
                d["per"] = _safe_float(row["PER"])
            if "PBR" in row.index:
                d["pbr"] = _safe_float(row["PBR"])
            if "EPS" in row.index:
                d["eps"] = _safe_float(row["EPS"])
            if "DIV" in row.index:
                d["dividend_yield"] = _safe_float(row["DIV"])

    if ohlcv_df is not None and not ohlcv_df.empty:
        for idx, row in ohlcv_df.iterrows():
            year = idx.year if hasattr(idx, "year") else int(str(idx)[:4])
            if year not in year_data:
                year_data[year] = {"year": year}
            if "종가" in row.index:
                year_data[year]["close_price"] = _safe_float(row["종가"])

    if cap_df is not None and not cap_df.empty:
        for idx, row in cap_df.iterrows():
            year = idx.year if hasattr(idx, "year") else int(str(idx)[:4])
            if year not in year_data:
                year_data[year] = {"year": year}
            if "시가총액" in row.index:
                year_data[year]["market_cap"] = _safe_float(row["시가총액"])

    results = [year_data[y] for y in sorted(year_data.keys())]
    return results
