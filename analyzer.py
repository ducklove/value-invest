def _safe_div(numerator, denominator, multiply=1.0):
    """안전한 나눗셈. None이거나 분모가 0이면 None 반환."""
    if numerator is None or denominator is None or denominator == 0:
        return None
    return round(numerator / denominator * multiply, 2)


def analyze(financial_data: list[dict], market_data: list[dict]) -> dict:
    """재무 + 시장 데이터를 병합하여 9개 지표 시계열 계산."""

    # 연도별 인덱싱
    fin_by_year = {d["year"]: d for d in financial_data}
    mkt_by_year = {d["year"]: d for d in market_data}

    all_years = sorted(set(fin_by_year.keys()) | set(mkt_by_year.keys()))

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

    for year in all_years:
        fin = fin_by_year.get(year, {})
        mkt = mkt_by_year.get(year, {})

        # 주가 (from pykrx)
        close_price = mkt.get("close_price")
        price_series.append({"year": year, "value": close_price})

        # PER (from pykrx)
        per = mkt.get("per")
        per_series.append({"year": year, "value": per})

        # PBR (from pykrx)
        pbr = mkt.get("pbr")
        pbr_series.append({"year": year, "value": pbr})

        # ROE (from DART: 당기순이익 / 자본총계 × 100)
        roe = _safe_div(fin.get("net_income"), fin.get("total_equity"), 100)
        roe_series.append({"year": year, "value": roe})

        # 배당수익률 (from pykrx)
        div_yield = mkt.get("dividend_yield")
        dividend_series.append({"year": year, "value": div_yield})

        # 부채비율 (from DART: 부채총계 / 자본총계 × 100)
        debt_ratio = _safe_div(
            fin.get("total_liabilities"), fin.get("total_equity"), 100
        )
        debt_ratio_series.append({"year": year, "value": debt_ratio})

        # 영업이익률 (from DART: 영업이익 / 매출액 × 100)
        op_margin = _safe_div(
            fin.get("operating_profit"), fin.get("revenue"), 100
        )
        operating_margin_series.append({"year": year, "value": op_margin})

        # EPS (from pykrx)
        eps = mkt.get("eps")
        eps_series.append({"year": year, "value": eps})

        # EPS 성장률
        eps_growth = None
        if eps is not None and prev_eps is not None and prev_eps != 0:
            eps_growth = round((eps - prev_eps) / abs(prev_eps) * 100, 2)
        eps_growth_series.append({"year": year, "value": eps_growth})
        prev_eps = eps

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
    }
