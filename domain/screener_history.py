"""스크리너 히스토리 지표 — 연도별 재무·배당 시계열에서 파생 지표를 만드는 순수 규칙.

입력은 종목 하나의 ``{사업연도: {revenue, operating_profit, net_income, equity,
liabilities, dps}}`` 이고, 출력은 스크리너 행에 붙일 평평한 dict 다. I/O·캐시는
``services/screener_history`` 가 맡는다.

정의 원칙:

* **연속된 사업연도만 비교한다.** 중간 연도가 비면 그 지점에서 추세 계산을
  멈춘다(없는 해를 건너뛰어 2년 전과 비교하지 않는다).
* **성장률은 기준 연도 값이 양수일 때만** 낸다. 적자 → 흑자 구간의 %는 의미가
  없으므로 ``None`` 으로 두고, 그 구간은 흑자 전환 플래그가 따로 표시한다.
* 자본이 0 이하(자본잠식)면 ROE·부채비율은 ``None``.
"""

from __future__ import annotations

from typing import Any

# 종목별로 보관·표시하는 지표. services/screener_history 의 저장 순서이기도 하다.
SERIES_FIELDS = ("revenue", "operating_profit", "net_income", "equity", "liabilities", "dps")

_FINANCIAL_FIELDS = ("revenue", "operating_profit", "net_income", "equity", "liabilities")

# 상세 패널에 보여 줄 최근 사업연도 수.
HISTORY_DISPLAY_YEARS = 5


def _num(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _ratio_pct(numerator: float | None, denominator: float | None) -> float | None:
    """``numerator / denominator * 100`` — 분모가 양수일 때만."""
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return numerator / denominator * 100


def _growth_pct(current: float | None, previous: float | None) -> float | None:
    """전년 대비 증가율(%). 기준(전년) 값이 양수일 때만 계산한다."""
    if current is None or previous is None or previous <= 0:
        return None
    return (current - previous) / previous * 100


def _year_metrics(values: dict[str, Any]) -> dict[str, float | None]:
    """한 사업연도의 원계열 + 그 해 기준 비율(ROE·영업이익률·부채비율)."""
    revenue = _num(values.get("revenue"))
    operating_profit = _num(values.get("operating_profit"))
    net_income = _num(values.get("net_income"))
    equity = _num(values.get("equity"))
    liabilities = _num(values.get("liabilities"))
    return {
        "revenue": revenue,
        "operating_profit": operating_profit,
        "net_income": net_income,
        "equity": equity,
        "liabilities": liabilities,
        "dps": _num(values.get("dps")),
        "roe": _ratio_pct(net_income, equity),
        "operating_margin": _ratio_pct(operating_profit, revenue),
        "debt_ratio": _ratio_pct(liabilities, equity),
    }


def _streak(years: dict[int, dict[str, float | None]], latest: int, holds) -> int:
    """``latest`` 에서 과거로 가며 ``holds(이번 해, 전년)`` 이 연속으로 참인 횟수.

    전년 자료가 없으면 거기서 멈춘다 — "3년 연속 증가"는 연속 4개 연도가 필요하다.
    """
    count = 0
    year = latest
    while year in years and (year - 1) in years and holds(years[year], years[year - 1]):
        count += 1
        year -= 1
    return count


def _run_length(years: dict[int, dict[str, float | None]], latest: int, holds) -> int:
    """``latest`` 에서 과거로 가며 ``holds(그 해)`` 가 연속으로 참인 연도 수."""
    count = 0
    year = latest
    while year in years and holds(years[year]):
        count += 1
        year -= 1
    return count


def _increased(key: str):
    def holds(current: dict, previous: dict) -> bool:
        a, b = current.get(key), previous.get(key)
        return a is not None and b is not None and a > b

    return holds


def _decreased(key: str):
    def holds(current: dict, previous: dict) -> bool:
        a, b = current.get(key), previous.get(key)
        return a is not None and b is not None and a < b

    return holds


def _positive(key: str):
    def holds(current: dict) -> bool:
        value = current.get(key)
        return value is not None and value > 0

    return holds


def _dps_raised(current: dict, previous: dict) -> bool:
    a, b = current.get("dps"), previous.get("dps")
    return a is not None and b is not None and b > 0 and a > b


def _dps_not_cut(current: dict, previous: dict) -> bool:
    a, b = current.get("dps"), previous.get("dps")
    return a is not None and b is not None and b > 0 and a >= b


def _turnaround(key: str, years: dict[int, dict[str, float | None]], latest: int) -> bool | None:
    """직전 사업연도 적자(0 이하) → 최근 사업연도 흑자. 비교할 자료가 없으면 ``None``."""
    current = years.get(latest, {}).get(key)
    previous = years.get(latest - 1, {}).get(key)
    if current is None or previous is None:
        return None
    return previous <= 0 < current


def _change(key: str, years: dict[int, dict[str, float | None]], latest: int) -> float | None:
    current = years.get(latest, {}).get(key)
    previous = years.get(latest - 1, {}).get(key)
    if current is None or previous is None:
        return None
    return current - previous


def _latest_year(years: dict[int, dict[str, float | None]], fields: tuple[str, ...]) -> int | None:
    candidates = [fy for fy, row in years.items() if any(row.get(field) is not None for field in fields)]
    return max(candidates) if candidates else None


def derive_history_metrics(
    series: dict[int, dict[str, Any]],
    *,
    reference_year: int | None = None,
) -> dict[str, Any]:
    """종목 하나의 연도별 시계열 → 스크리너 히스토리 지표.

    ``reference_year`` 는 유니버스에서 가장 흔한 최근 사업연도다. 종목의 최근
    재무 연도가 그보다 2년 이상 뒤처지면(수집 누락·거래정지 등) 낡은 추세를
    "최근"으로 보여 주지 않도록 재무 지표를 비운다. 배당도 같은 기준으로,
    최근 배당 연도가 ``reference_year - 1`` 보다 오래됐으면 현재 배당으로 치지 않는다.
    """
    years = {int(fy): _year_metrics(values) for fy, values in series.items() if isinstance(values, dict)}
    out: dict[str, Any] = {
        "history_fy": None,
        "fy_roe": None,
        "fy_operating_margin": None,
        "fy_debt_ratio": None,
        "fy_equity": None,
        "revenue_growth": None,
        "op_profit_growth": None,
        "net_income_growth": None,
        "revenue_cagr_3y": None,
        "revenue_growth_years": None,
        "op_profit_growth_years": None,
        "profit_years": None,
        "roe_change": None,
        "roe_up_years": None,
        "op_margin_change": None,
        "op_margin_up_years": None,
        "debt_ratio_change": None,
        "debt_ratio_down_years": None,
        "turnaround_op": None,
        "turnaround_net": None,
        "dps": None,
        "dps_fy": None,
        "dps_growth": None,
        "dps_growth_years": None,
        "dividend_no_cut_years": None,
        "history": [],
    }
    if not years:
        return out

    latest = _latest_year(years, _FINANCIAL_FIELDS)
    if latest is not None and reference_year is not None and latest < reference_year - 1:
        latest = None

    if latest is not None:
        current = years[latest]
        previous = years.get(latest - 1, {})
        out["history_fy"] = latest
        out["fy_roe"] = current["roe"]
        out["fy_operating_margin"] = current["operating_margin"]
        out["fy_debt_ratio"] = current["debt_ratio"]
        out["fy_equity"] = current["equity"]
        out["revenue_growth"] = _growth_pct(current["revenue"], previous.get("revenue"))
        out["op_profit_growth"] = _growth_pct(current["operating_profit"], previous.get("operating_profit"))
        out["net_income_growth"] = _growth_pct(current["net_income"], previous.get("net_income"))

        base = years.get(latest - 3, {}).get("revenue")
        if current["revenue"] is not None and current["revenue"] > 0 and base is not None and base > 0:
            out["revenue_cagr_3y"] = ((current["revenue"] / base) ** (1 / 3) - 1) * 100

        out["revenue_growth_years"] = _streak(years, latest, _increased("revenue"))
        out["op_profit_growth_years"] = _streak(years, latest, _increased("operating_profit"))
        out["profit_years"] = _run_length(years, latest, _positive("net_income"))
        out["roe_change"] = _change("roe", years, latest)
        out["roe_up_years"] = _streak(years, latest, _increased("roe"))
        out["op_margin_change"] = _change("operating_margin", years, latest)
        out["op_margin_up_years"] = _streak(years, latest, _increased("operating_margin"))
        out["debt_ratio_change"] = _change("debt_ratio", years, latest)
        out["debt_ratio_down_years"] = _streak(years, latest, _decreased("debt_ratio"))
        out["turnaround_op"] = _turnaround("operating_profit", years, latest)
        out["turnaround_net"] = _turnaround("net_income", years, latest)

    # 배당 기준 연도: 재무가 있으면 그 최근 사업연도, 없으면(우선주 등) 최근 배당 연도.
    dps_year = latest
    if dps_year is None:
        dps_year = _latest_year(years, ("dps",))
        if dps_year is not None and reference_year is not None and dps_year < reference_year - 1:
            dps_year = None
    if dps_year is not None:
        dps = years.get(dps_year, {}).get("dps")
        if dps is not None and dps > 0:
            out["dps"] = dps
            out["dps_fy"] = dps_year
            out["dps_growth"] = _growth_pct(dps, years.get(dps_year - 1, {}).get("dps"))
            out["dps_growth_years"] = _streak(years, dps_year, _dps_raised)
            out["dividend_no_cut_years"] = _streak(years, dps_year, _dps_not_cut)
        elif latest is not None:
            # 재무는 있는데 그 해 배당 기록이 없다 = 무배당으로 본다.
            out["dps_growth_years"] = 0
            out["dividend_no_cut_years"] = 0

    anchor = latest if latest is not None else dps_year
    if anchor is not None:
        shown = [fy for fy in sorted(years) if anchor - HISTORY_DISPLAY_YEARS < fy <= anchor]
        out["history"] = [
            {
                "fy": fy,
                "revenue": years[fy]["revenue"],
                "operating_profit": years[fy]["operating_profit"],
                "net_income": years[fy]["net_income"],
                "roe": years[fy]["roe"],
                "operating_margin": years[fy]["operating_margin"],
                "debt_ratio": years[fy]["debt_ratio"],
                "dps": years[fy]["dps"],
            }
            for fy in shown
        ]
    return out


def most_common_latest_year(all_series: dict[str, dict[int, dict[str, Any]]]) -> int | None:
    """유니버스에서 가장 많은 종목이 가진 '최근 재무 사업연도'."""
    counts: dict[int, int] = {}
    for series in all_series.values():
        candidates = [
            int(fy)
            for fy, values in series.items()
            if isinstance(values, dict) and any(_num(values.get(field)) is not None for field in _FINANCIAL_FIELDS)
        ]
        if candidates:
            latest = max(candidates)
            counts[latest] = counts.get(latest, 0) + 1
    if not counts:
        return None
    return max(counts, key=lambda fy: (counts[fy], fy))
