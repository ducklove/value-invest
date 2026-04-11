def _safe_div(numerator, denominator, multiply=1.0):
    """안전한 나눗셈. None이거나 분모가 0이면 None 반환."""
    if numerator is None or denominator is None or denominator == 0:
        return None
    return round(numerator / denominator * multiply, 2)


def _to_eok_won(value):
    if value is None:
        return None
    return round(value / 100_000_000, 2)


def analyze(
    financial_data: list[dict],
    market_data: list[dict],
    weekly_market_data: list[dict] | None = None,
) -> dict:
    """재무 + 시장 데이터를 병합하여 주간 지표 시계열 계산."""

    # 연도별 인덱싱 (legacy, kept for cached data compatibility)
    fin_by_year = {d["year"]: d for d in financial_data}
    mkt_by_year = {d["year"]: d for d in market_data}

    financial_years = sorted(fin_by_year.keys())
    market_years = sorted(mkt_by_year.keys())
    all_years = sorted(set(financial_years) | set(market_years))

    # Build annual indicators (kept for backward compat but secondary)
    price_series = []
    market_cap_series = []
    per_series = []
    pbr_series = []
    roe_series = []
    dividend_series = []
    dividend_per_share_series = []
    debt_ratio_series = []
    operating_margin_series = []
    eps_series = []

    for year in market_years:
        mkt = mkt_by_year.get(year, {})
        price_series.append({"year": year, "value": mkt.get("close_price")})
        market_cap_series.append({"year": year, "value": mkt.get("market_cap")})
        per_series.append({"year": year, "value": mkt.get("per")})
        pbr_series.append({"year": year, "value": mkt.get("pbr")})
        dividend_series.append({"year": year, "value": mkt.get("dividend_yield")})
        dividend_per_share_series.append({"year": year, "value": mkt.get("dividend_per_share")})
        eps_series.append({"year": year, "value": mkt.get("eps")})

    for year in financial_years:
        fin = fin_by_year.get(year, {})
        roe_series.append({"year": year, "value": _safe_div(fin.get("net_income"), fin.get("total_equity"), 100)})
        debt_ratio_series.append({"year": year, "value": _safe_div(fin.get("total_liabilities"), fin.get("total_equity"), 100)})
        operating_margin_series.append({"year": year, "value": _safe_div(fin.get("operating_profit"), fin.get("revenue"), 100)})

    # Weekly indicators — all from weekly_market_data
    wmd = weekly_market_data or []
    weekly_indicators = {
        "주가": [{"date": item["date"], "value": item.get("close_price")} for item in wmd],
        "PER": [{"date": item["date"], "value": item.get("per")} for item in wmd],
        "PBR": [{"date": item["date"], "value": item.get("pbr")} for item in wmd],
        "배당수익률 (%)": [{"date": item["date"], "value": item.get("dividend_yield")} for item in wmd],
        "시가총액": [{"date": item["date"], "value": item.get("market_cap")} for item in wmd],
        "EPS (원)": [{"date": item["date"], "value": item.get("eps")} for item in wmd],
        "ROE (%)": [{"date": item["date"], "value": item.get("roe")} for item in wmd],
        "부채비율 (%)": [{"date": item["date"], "value": item.get("debt_ratio")} for item in wmd],
        "영업이익률 (%)": [{"date": item["date"], "value": item.get("operating_margin")} for item in wmd],
    }

    return {
        "years": all_years,
        "indicators": {
            "주가 (원)": price_series,
            "시가총액": market_cap_series,
            "PER": per_series,
            "PBR": pbr_series,
            "ROE (%)": roe_series,
            "EPS (원)": eps_series,
            "배당수익률 (%)": dividend_series,
            "부채비율 (%)": debt_ratio_series,
            "영업이익률 (%)": operating_margin_series,
            "주당배당금 (원)": dividend_per_share_series,
        },
        "weekly_indicators": weekly_indicators,
    }
