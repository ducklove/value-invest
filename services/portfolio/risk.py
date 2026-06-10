"""포트폴리오 NAV 히스토리 기반 리스크 지표 계산 (로드맵 신규 기능 ②).

순수 계산 모듈 — DB/네트워크 의존이 없어 단독으로 단위 테스트할 수 있다.
routes/portfolio_risk.py 가 repositories.snapshots.get_nav_history() 결과
([{date, nav, total_value, ...}] 날짜 오름차순)와
repositories.benchmark_daily.get_benchmark_rows() 결과([{date, close}])를
그대로 넘겨 호출한다.

수식 규약 (변경 시 tests/test_portfolio_risk.py 의 손계산 기대값도 같이 수정):

* 일간 수익률: 단순 수익률 r_t = nav_t / nav_{t-1} - 1.
  routes/analysis._compute_beta_from_pairs 와 같은 규약. 휴일/스냅샷 누락일은
  보간하지 않고 "연속으로 존재하는 포인트" 사이의 수익률로 처리한다.
* 누적 수익률: (last / base - 1) × 100. base 는 윈도 경계일 이전(이하)의
  마지막 포인트 — YTD 가 전년도 마지막 스냅샷을 기준으로 삼는
  repositories.snapshots.get_year_start_snapshot 과 같은 의미다.
* 연환산 수익률: ((last / base) ^ (365.25 / 경과 달력일수) - 1) × 100.
* 연환산 변동성: stdev(일간 단순수익률, 표본 표준편차) × sqrt(252) × 100.
  (로그수익률이 아닌 단순수익률 사용 — 기존 베타 계산과 규약 통일)
* Sharpe: (mean_daily × 252 - rf) / (stdev_daily × sqrt(252)).
  rf 는 연 무위험수익률(소수). 호출부는 퍼센트(RISK_FREE_RATE_PCT)로 받아
  /100 해서 넘긴다.
* MDD: 누적 최고점(running peak) 대비 최대 낙폭(음수 %). peak/trough 날짜 포함.
  current drawdown 은 마지막 포인트의 running peak 대비 낙폭.
* beta / correlation: 날짜 교집합 위에서 연속 페어 수익률의 Cov/Var(표본),
  Pearson 상관. 겹치는 수익률 표본이 MIN_BENCHMARK_RETURNS 미만이면 null.

윈도 의미(KST): 앵커는 services.portfolio.time_windows.today_kst_date() —
1M/3M/6M/1Y 는 앵커에서 달력 기준으로 거슬러 올라간 날짜(말일 클램프),
YTD 는 전년도 12-31, ALL 은 전체 시리즈.
"""

from __future__ import annotations

import calendar
import math
from datetime import date

from services.portfolio.time_windows import today_kst_date

WINDOWS = ("1M", "3M", "6M", "1Y", "YTD", "ALL")
_WINDOW_MONTHS = {"1M": 1, "3M": 3, "6M": 6, "1Y": 12}

TRADING_DAYS_PER_YEAR = 252
CALENDAR_DAYS_PER_YEAR = 365.25
# 기존 routes/analysis 베타 계산(표본 20개 미만이면 None)과 같은 하한.
MIN_BENCHMARK_RETURNS = 20

_EMPTY_METRICS: dict = {
    "cumulative_return_pct": None,
    "annualized_return_pct": None,
    "annualized_volatility_pct": None,
    "max_drawdown_pct": None,
    "max_drawdown_peak_date": None,
    "max_drawdown_trough_date": None,
    "current_drawdown_pct": None,
    "sharpe_ratio": None,
    "best_day": None,
    "worst_day": None,
}


def _round(value: float | None, digits: int = 4) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def clean_series(rows: list[dict] | None, value_key: str = "nav") -> list[tuple[str, float]]:
    """[{date, <value_key>, ...}] → 날짜 오름차순 (date, value) 페어.

    값이 None / 0 / 음수 / 숫자 변환 불가인 행은 버린다(0 NAV 가 수익률
    계산에 들어가면 0 나눗셈·-100% 왜곡이 생기므로 결측 취급).
    """
    out: list[tuple[str, float]] = []
    for row in rows or []:
        d = row.get("date")
        v = row.get(value_key)
        if not d or v is None:
            continue
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(f) or f <= 0:
            continue
        out.append((str(d), f))
    out.sort(key=lambda p: p[0])
    return out


def _shift_months(d: date, months: int) -> date:
    """달력 기준으로 months 개월 전 날짜(말일 클램프: 3/31 - 1M → 2/28)."""
    total = d.year * 12 + (d.month - 1) - months
    year, month0 = divmod(total, 12)
    month = month0 + 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def window_boundary(window: str, anchor: date) -> date | None:
    """윈도 시작 경계일. base 포인트는 '경계일 이하의 마지막 포인트'."""
    if window == "ALL":
        return None
    if window == "YTD":
        return date(anchor.year - 1, 12, 31)
    months = _WINDOW_MONTHS.get(window)
    if months is None:
        raise ValueError(f"unknown window: {window}")
    return _shift_months(anchor, months)


def slice_window(
    points: list[tuple[str, float]],
    window: str,
    anchor: date | None = None,
) -> tuple[list[tuple[str, float]], bool]:
    """(윈도 포인트(base 포함), 시리즈가 윈도를 모두 덮는지 여부).

    base = 경계일 이하의 마지막 포인트. 시리즈 첫 포인트가 경계일보다 늦으면
    가능한 구간(전체)을 그대로 쓰되 covers=False 로 표시한다
    ("짧으면 계산 가능한 만큼 계산하고 insufficient 플래그" 요구사항).
    """
    if not points:
        return [], False
    anchor = anchor or today_kst_date()
    boundary = window_boundary(window, anchor)
    if boundary is None:
        return list(points), True
    boundary_iso = boundary.isoformat()
    base_idx: int | None = None
    for i, (d, _) in enumerate(points):
        if d <= boundary_iso:
            base_idx = i
        else:
            break
    if base_idx is None:
        return list(points), False
    return points[base_idx:], True


def simple_returns(points: list[tuple[str, float]]) -> list[tuple[str, float]]:
    """연속 포인트 간 단순 수익률 [(날짜_t, r_t), ...]."""
    out: list[tuple[str, float]] = []
    for i in range(1, len(points)):
        prev = points[i - 1][1]
        cur = points[i][1]
        if prev <= 0:  # clean_series 가 거르지만 방어적으로 한 번 더
            continue
        out.append((points[i][0], cur / prev - 1.0))
    return out


def max_drawdown(points: list[tuple[str, float]]) -> dict | None:
    """running peak 대비 최대 낙폭과 현재 낙폭. 포인트 < 2 면 None."""
    if len(points) < 2:
        return None
    peak_value = points[0][1]
    peak_date = points[0][0]
    worst = 0.0
    worst_peak_date = peak_date
    worst_trough_date = points[0][0]
    for d, v in points:
        if v > peak_value:
            peak_value = v
            peak_date = d
        dd = v / peak_value - 1.0
        if dd < worst:
            worst = dd
            worst_peak_date = peak_date
            worst_trough_date = d
    current_dd = points[-1][1] / peak_value - 1.0
    return {
        "max_drawdown_pct": worst * 100.0,
        "peak_date": worst_peak_date,
        "trough_date": worst_trough_date,
        "current_drawdown_pct": current_dd * 100.0,
    }


def annualized_return_pct(base: tuple[str, float], last: tuple[str, float]) -> float | None:
    """달력일 기준 연환산 수익률(%). 경과일 < 1 이면 None."""
    try:
        days = (date.fromisoformat(last[0][:10]) - date.fromisoformat(base[0][:10])).days
    except ValueError:
        return None
    if days < 1 or base[1] <= 0:
        return None
    growth = last[1] / base[1]
    if growth <= 0:
        return None
    return (growth ** (CALENDAR_DAYS_PER_YEAR / days) - 1.0) * 100.0


def _mean_std(values: list[float]) -> tuple[float, float | None]:
    n = len(values)
    mean = sum(values) / n
    if n < 2:
        return mean, None
    var = sum((v - mean) ** 2 for v in values) / (n - 1)
    return mean, math.sqrt(var)


def annualized_volatility_pct(returns: list[float]) -> float | None:
    """표본 stdev × sqrt(252) × 100. 수익률 표본 < 2 면 None."""
    if len(returns) < 2:
        return None
    _, std = _mean_std(returns)
    if std is None:
        return None
    return std * math.sqrt(TRADING_DAYS_PER_YEAR) * 100.0


def sharpe_ratio(returns: list[float], risk_free_rate: float = 0.0) -> float | None:
    """(mean×252 - rf) / (stdev×sqrt(252)). rf 는 연율 소수. 변동성 0 이면 None."""
    if len(returns) < 2:
        return None
    mean, std = _mean_std(returns)
    if not std:
        return None
    annual_excess = mean * TRADING_DAYS_PER_YEAR - risk_free_rate
    return annual_excess / (std * math.sqrt(TRADING_DAYS_PER_YEAR))


def beta_correlation(
    points: list[tuple[str, float]],
    benchmark_points: list[tuple[str, float]],
    min_returns: int = MIN_BENCHMARK_RETURNS,
) -> dict:
    """날짜 교집합 위 연속 페어 수익률로 beta(Cov/Var)·Pearson 상관 계산.

    겹치는 수익률 표본이 min_returns 미만이면 beta/correlation 모두 null.
    """
    bench_by_date = dict(benchmark_points)
    paired = [(v, bench_by_date[d]) for d, v in points if d in bench_by_date]
    port_rets: list[float] = []
    bench_rets: list[float] = []
    for i in range(1, len(paired)):
        p0, b0 = paired[i - 1]
        p1, b1 = paired[i]
        if p0 <= 0 or b0 <= 0:
            continue
        port_rets.append(p1 / p0 - 1.0)
        bench_rets.append(b1 / b0 - 1.0)
    n = len(port_rets)
    result = {"beta": None, "correlation": None, "overlap_returns": n}
    if n < max(2, min_returns):
        return result
    mean_p = sum(port_rets) / n
    mean_b = sum(bench_rets) / n
    cov = sum((port_rets[i] - mean_p) * (bench_rets[i] - mean_b) for i in range(n)) / (n - 1)
    var_b = sum((b - mean_b) ** 2 for b in bench_rets) / (n - 1)
    var_p = sum((p - mean_p) ** 2 for p in port_rets) / (n - 1)
    if var_b > 0:
        result["beta"] = _round(cov / var_b)
    if var_b > 0 and var_p > 0:
        result["correlation"] = _round(cov / math.sqrt(var_p * var_b))
    return result


def compute_risk_metrics(
    series: list[dict] | None,
    window: str = "1Y",
    *,
    benchmark_rows: list[dict] | None = None,
    risk_free_rate_pct: float = 0.0,
    today: date | None = None,
    min_benchmark_returns: int = MIN_BENCHMARK_RETURNS,
) -> dict:
    """NAV 시리즈(+선택적 벤치마크)에서 리스크 지표 묶음을 계산.

    반환: {window, start_date, end_date, points, metrics{...},
           benchmark{beta, correlation, overlap_returns}|None, insufficient}.
    benchmark 의 code/name 표시는 라우트 쪽에서 덧붙인다.
    """
    win = (window or "").strip().upper()
    if win not in WINDOWS:
        raise ValueError(f"unknown window: {window}")

    all_points = clean_series(series, "nav")
    points, covers = slice_window(all_points, win, today)
    insufficient = (not covers) or len(points) < 2

    metrics = dict(_EMPTY_METRICS)
    if len(points) >= 2:
        base, last = points[0], points[-1]
        rets = simple_returns(points)
        ret_values = [r for _, r in rets]

        metrics["cumulative_return_pct"] = _round((last[1] / base[1] - 1.0) * 100.0)
        metrics["annualized_return_pct"] = _round(annualized_return_pct(base, last))
        metrics["annualized_volatility_pct"] = _round(annualized_volatility_pct(ret_values))
        metrics["sharpe_ratio"] = _round(sharpe_ratio(ret_values, risk_free_rate_pct / 100.0))

        dd = max_drawdown(points)
        if dd:
            metrics["max_drawdown_pct"] = _round(dd["max_drawdown_pct"])
            metrics["max_drawdown_peak_date"] = dd["peak_date"]
            metrics["max_drawdown_trough_date"] = dd["trough_date"]
            metrics["current_drawdown_pct"] = _round(dd["current_drawdown_pct"])

        if ret_values:
            best = max(rets, key=lambda r: r[1])
            worst = min(rets, key=lambda r: r[1])
            metrics["best_day"] = {"date": best[0], "return_pct": _round(best[1] * 100.0)}
            metrics["worst_day"] = {"date": worst[0], "return_pct": _round(worst[1] * 100.0)}

    benchmark_out: dict | None = None
    bench_points = clean_series(benchmark_rows, "close")
    if bench_points:
        benchmark_out = beta_correlation(points, bench_points, min_benchmark_returns)

    return {
        "window": win,
        "start_date": points[0][0] if points else None,
        "end_date": points[-1][0] if points else None,
        "points": len(points),
        "metrics": metrics,
        "benchmark": benchmark_out,
        "insufficient": insufficient,
    }
