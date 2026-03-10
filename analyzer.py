def _safe_div(numerator, denominator, multiply=1.0):
    """안전한 나눗셈. None이거나 분모가 0이면 None 반환."""
    if numerator is None or denominator is None or denominator == 0:
        return None
    return round(numerator / denominator * multiply, 2)


def analyze(
    financial_data: list[dict],
    market_data: list[dict],
    weekly_market_data: list[dict] | None = None,
) -> dict:
    """재무 + 시장 데이터를 병합하여 연간/주간 지표 시계열 계산."""

    # 연도별 인덱싱
    fin_by_year = {d["year"]: d for d in financial_data}
    mkt_by_year = {d["year"]: d for d in market_data}

    financial_years = sorted(fin_by_year.keys())
    market_years = sorted(mkt_by_year.keys())
    all_years = sorted(set(financial_years) | set(market_years))

    price_series = []
    per_series = []
    pbr_series = []
    roe_series = []
    dividend_series = []
    debt_ratio_series = []
    operating_margin_series = []
    eps_series = []
    eps_growth_series = []

    prev_eps = None

    for year in market_years:
        mkt = mkt_by_year.get(year, {})
        close_price = mkt.get("close_price")
        price_series.append({"year": year, "value": close_price})

        per = mkt.get("per")
        per_series.append({"year": year, "value": per})

        pbr = mkt.get("pbr")
        pbr_series.append({"year": year, "value": pbr})

        div_yield = mkt.get("dividend_yield")
        dividend_series.append({"year": year, "value": div_yield})
        eps = mkt.get("eps")
        eps_series.append({"year": year, "value": eps})

        eps_growth = None
        if eps is not None and prev_eps is not None and prev_eps != 0:
            eps_growth = round((eps - prev_eps) / abs(prev_eps) * 100, 2)
        eps_growth_series.append({"year": year, "value": eps_growth})
        prev_eps = eps

    for year in financial_years:
        fin = fin_by_year.get(year, {})

        roe = _safe_div(fin.get("net_income"), fin.get("total_equity"), 100)
        roe_series.append({"year": year, "value": roe})

        debt_ratio = _safe_div(
            fin.get("total_liabilities"), fin.get("total_equity"), 100
        )
        debt_ratio_series.append({"year": year, "value": debt_ratio})

        op_margin = _safe_div(
            fin.get("operating_profit"), fin.get("revenue"), 100
        )
        operating_margin_series.append({"year": year, "value": op_margin})

    weekly_indicators = {
        "주간 주가 (최근 3년)": [
            {"date": item["date"], "value": item.get("close_price")}
            for item in (weekly_market_data or [])
        ],
        "주간 PER (최근 3년)": [
            {"date": item["date"], "value": item.get("per")}
            for item in (weekly_market_data or [])
        ],
        "주간 PBR (최근 3년)": [
            {"date": item["date"], "value": item.get("pbr")}
            for item in (weekly_market_data or [])
        ],
        "주간 배당수익률 (최근 3년)": [
            {"date": item["date"], "value": item.get("dividend_yield")}
            for item in (weekly_market_data or [])
        ],
    }

    return {
        "years": all_years,
        "indicators": {
            "주가 (원)": price_series,
            "PER": per_series,
            "PBR": pbr_series,
            "ROE (%)": roe_series,
            "배당수익률 (%)": dividend_series,
            "부채비율 (%)": debt_ratio_series,
            "영업이익률 (%)": operating_margin_series,
            "EPS (원)": eps_series,
            "EPS 성장률 (%)": eps_growth_series,
        },
        "weekly_indicators": weekly_indicators,
    }
