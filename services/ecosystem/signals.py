"""형제 도구 신호를 같은 척도로 맞추는 순수 함수 (기회 보드·종목 360°·액션 보드 공용).

점수(``score``)는 0~100이고 높을수록 그 도구 기준에서 눈에 띄는 쪽이다. 근거는 둘 중 하나다.

- ``history3y``: 자기 3년 분포에서 현재 값 이하인 표본 비율(우선주 괴리율, 지주사 보유가치/시총).
- ``crossSection``: 같은 도구의 전체 종목 중 현재 값 이하인 비율(스팩 연환산, 자사주 비율, 국민연금 지분율 변화).

척도는 같아도 뜻은 다르다. 그래서 순위는 10점 구간으로 먼저 나누고, 같은 구간에서는 자기 역사 기준을
횡단면 기준보다 앞에 둔다. 자사주처럼 종목이 많고 거의 변하지 않는 횡단면 신호가 상단을 차지하지 않게
하려는 것이다. 어떤 점수도 수익 전망이 아니다. 거래비용을 추정할 수 없으므로 비용을 뺀 수치는 만들지 않고,
모르는 값은 0이 아니라 ``None`` 으로 둔다(docs/ecosystem/data-contract.md §2).

입출력은 모두 JSON 호환 dict 이고 키는 camelCase 다. 네트워크·DB·시계에 의존하지 않는다(``today`` 는 인자).
"""

from __future__ import annotations

import bisect
import math
import re
from collections import Counter
from collections.abc import Iterable, Mapping
from datetime import date, timedelta
from typing import Any

from domain.market_calendar import is_trading_day

TOOL_PREFERRED = "common_preferred_spread"
TOOL_HOLDING = "holding_value"
TOOL_SPAC = "spac-hunter"
TOOL_BUYBACKS = "buybacks"
TOOL_NPS = "nps-tracker"
TOOL_ETF = "eiayn"

TOOL_LABELS = {
    TOOL_PREFERRED: "우선주 괴리",
    TOOL_HOLDING: "지주사 할인",
    TOOL_SPAC: "스팩 청산가치",
    TOOL_BUYBACKS: "자사주",
    TOOL_NPS: "국민연금",
    TOOL_ETF: "ETF 평가",
}

BASIS_HISTORY = "history3y"
BASIS_CROSS_SECTION = "crossSection"
_BASIS_ORDER = {BASIS_HISTORY: 0, BASIS_CROSS_SECTION: 1}

# 두 형제 카드와 같은 최소 표본(holding_value computePairPercentiles, cps renderSpreadPercentiles).
MIN_HISTORY_POINTS = 30
WINDOW_1Y_DAYS = 365
WINDOW_3Y_DAYS = 1095
# 횡단면 순위는 한 칸이 10점보다 작아야 의미가 있다. 10개 미만이면 점수를 만들지 않는다.
MIN_CROSS_SECTION = 10

# 액션 보드 severity: 자기 3년 분포 상위 10% 이면 high. 고정 임계값은 백분위가 없을 때만 쓴다.
HIGH_PERCENTILE = 90

BOARD_MIN_SCORE = 80
BOARD_PER_TOOL = 12
# cps attractiveness.py 의 유동성 점수 하한(일평균 거래액 1억 이하 = 0점)과 같은 기준.
LOW_LIQUIDITY_KRW = 100_000_000

# data-contract §7.5 도구별 허용치. 자사주·국민연금은 연말 공시 기준이라 다음 해 사업보고서·공시
# 시점(약 15개월 뒤)까지는 정상이다. 날짜로만 판정하고 generatedAt 은 쓰지 않는다.
FRESHNESS = {
    TOOL_PREFERRED: {"tradingDays": 2},
    TOOL_HOLDING: {"tradingDays": 2},
    TOOL_SPAC: {"tradingDays": 2},
    TOOL_ETF: {"tradingDays": 5},
    TOOL_BUYBACKS: {"calendarDays": 460},
    TOOL_NPS: {"calendarDays": 460},
}
# 일봉 분포: 5거래일 넘게 밀리면 경고하고, 90일이 지나면 백분위 자체를 쓰지 않는다.
HISTORY_WARN_TRADING_DAYS = 5
HISTORY_MAX_CALENDAR_DAYS = 90

_DAY_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})")
_SOURCE_DATE_RE = re.compile(r"\((\d{4}-\d{2}-\d{2})\)")


# ---------------------------------------------------------------------------
# 기본 변환
# ---------------------------------------------------------------------------

def finite(value: Any) -> float | None:
    """유한한 숫자만 float 로. bool·문자열·NaN·Infinity 는 None."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def parse_day(value: Any) -> date | None:
    """'YYYY-MM-DD…' 로 시작하는 값의 날짜. asOf·lastUpdated 형식을 모두 받는다."""
    match = _DAY_RE.match(str(value or "").strip())
    if not match:
        return None
    try:
        return date.fromisoformat(match.group(1))
    except ValueError:
        return None


def day_text(value: Any) -> str | None:
    day = parse_day(value)
    return day.isoformat() if day else None


def metric(label: str, value: Any, unit: str) -> dict | None:
    """패널에 표시할 수치 하나. 값이 없으면 None(호출부가 뺀다)."""
    if unit == "text":
        return {"label": label, "value": value, "unit": unit} if value else None
    number = finite(value)
    return {"label": label, "value": number, "unit": unit} if number is not None else None


def _metrics(*items: dict | None) -> list[dict]:
    return [item for item in items if item is not None]


# ---------------------------------------------------------------------------
# 백분위·횡단면 순위
# ---------------------------------------------------------------------------

def percentile_rank(values: Iterable[Any] | None, current: Any, *, min_points: int = MIN_HISTORY_POINTS) -> int | None:
    """현재 값 이하인 표본 비율(정수 %). 형제 카드와 같은 식이고 표본이 부족하면 None."""
    number = finite(current)
    valid = [v for v in (finite(x) for x in values or []) if v is not None]
    if number is None or len(valid) < min_points:
        return None
    return math.floor(sum(1 for v in valid if v <= number) / len(valid) * 100 + 0.5)  # JS Math.round


def holding_percentiles(history: Any, ratio: Any, snapshot_day: Any) -> dict:
    """holding_value ``percentileHistory`` + 현재 비율 → 발행 필드와 같은 모양.

    holding_value ``pipeline/hub_summary.percentile_fields`` 와 같은 식이다. 형제가 새 필드를 발행하기 전
    폴백 경로에서 쓴다. 저장된 ``pctile3y`` 는 일봉 작업이 멈추면 같이 멈추므로 쓰지 않는다.
    """
    out: dict[str, Any] = {"ratioPct1y": None, "ratioPct3y": None, "ratioPctEnd": None}
    if not isinstance(history, Mapping):
        return out
    dates, ratios = history.get("dates"), history.get("ratio")
    if not isinstance(dates, list) or not isinstance(ratios, list):
        return out
    rows = sorted(
        (day.isoformat(), number)
        for day, number in ((parse_day(d), finite(r)) for d, r in zip(dates, ratios))
        if day is not None and number is not None
    )
    if not rows:
        return out
    out["ratioPctEnd"] = rows[-1][0]
    current = finite(ratio)
    if current is None:
        return out
    snapshot = day_text(snapshot_day)
    if snapshot and snapshot >= rows[-1][0]:  # 카드처럼 스냅샷 표본을 넣는다(같은 날이면 교체)
        rows = [row for row in rows if row[0] != snapshot] + [(snapshot, current)]
    latest = date.fromisoformat(rows[-1][0])
    for key, days in (("ratioPct1y", WINDOW_1Y_DAYS), ("ratioPct3y", WINDOW_3Y_DAYS)):
        cutoff = (latest - timedelta(days=days)).isoformat()
        out[key] = percentile_rank((value for day, value in rows if day >= cutoff), current)
    return out


def spread_percentiles(stats: Any, spread: Any) -> dict:
    """cps ``spreadPercentiles`` + 현재 괴리율 → 발행 필드와 같은 모양(카드와 같은 식)."""
    out: dict[str, Any] = {"spreadPct1y": None, "spreadPct3y": None, "spreadPctEnd": None}
    if not isinstance(stats, Mapping):
        return out
    out["spreadPctEnd"] = day_text(stats.get("date"))
    for key, source in (("spreadPct1y", "pctile1y"), ("spreadPct3y", "pctile3y")):
        values = stats.get(source + "Values")
        if isinstance(values, list):
            out[key] = percentile_rank(values, spread)
        else:
            stored = finite(stats.get(source))
            out[key] = math.floor(stored + 0.5) if stored is not None else None
    return out


def sorted_population(values: Iterable[Any]) -> list[float]:
    return sorted(v for v in (finite(x) for x in values) if v is not None)


def cross_section_score(population: list[float], value: Any) -> float | None:
    """정렬된 모집단에서 값 이하인 비율(소수 1자리). 모집단이 작거나 값이 없으면 None."""
    number = finite(value)
    if number is None or len(population) < MIN_CROSS_SECTION:
        return None
    return round(bisect.bisect_right(population, number) / len(population) * 100, 1)


# ---------------------------------------------------------------------------
# 신선도
# ---------------------------------------------------------------------------

def trading_days_behind(as_of: date, today: date) -> int:
    """as_of 다음 날부터 today 까지의 거래일 수. 긴 간격은 달력을 훑지 않고 어림한다."""
    if as_of >= today:
        return 0
    gap = (today - as_of).days
    if gap > 120:
        return gap * 5 // 7
    count = 0
    for offset in range(1, gap + 1):
        day = as_of + timedelta(days=offset)
        known = is_trading_day(day)
        if known or (known is None and day.weekday() < 5):
            count += 1
    return count


def freshness_warning(tool_id: str, as_of: Any, today: date) -> str | None:
    """도구별 허용치를 넘긴 기준일이면 경고 문구. asOf 를 모르면 그것도 경고다."""
    day = parse_day(as_of)
    if day is None:
        return "기준일을 확인할 수 없습니다"
    rule = FRESHNESS.get(tool_id) or {}
    if "tradingDays" in rule:
        behind = trading_days_behind(day, today)
        if behind > rule["tradingDays"]:
            return f"{day} 기준, {behind}거래일 지난 데이터입니다"
    elif "calendarDays" in rule:
        age = (today - day).days
        if age > rule["calendarDays"]:
            return f"{day} 기준, {age}일 지난 데이터입니다"
    return None


def history_score(percentile: Any, end: Any, today: date) -> tuple[int | None, str | None]:
    """쓸 수 있는 3년 백분위와 분포 경고. 분포가 너무 오래됐거나 값이 없으면 점수 없음."""
    number = finite(percentile)
    end_day = parse_day(end)
    if number is None:
        return None, "3년 백분위 없음: 표본 30개 미만이거나 분포를 받지 못했습니다"
    if end_day is None:
        return None, "3년 백분위 없음: 분포 기준일을 확인할 수 없습니다"
    if (today - end_day).days > HISTORY_MAX_CALENDAR_DAYS:
        return None, f"일봉 분포가 {end_day}에서 멈춰 백분위를 쓰지 않습니다"
    warning = None
    behind = trading_days_behind(end_day, today)
    if behind > HISTORY_WARN_TRADING_DAYS:
        warning = f"일봉 분포가 {end_day}까지입니다({behind}거래일 지연). 현재 값만 최신입니다"
    return math.floor(number + 0.5), warning


def history_severity(percentile: int | None, value: float | None, fixed_threshold: float) -> str:
    """액션 보드 severity. 쓸 수 있는 자기 역사 백분위가 있으면 그것으로, 없으면 고정 임계값으로."""
    if percentile is not None:
        return "high" if percentile >= HIGH_PERCENTILE else "watch"
    return "high" if value is not None and value >= fixed_threshold else "watch"


def _warnings(*items: str | None) -> list[str]:
    return [item for item in items if item]


# ---------------------------------------------------------------------------
# 도구별 원본 → 공통 입력 모양
# ---------------------------------------------------------------------------

def _ticker_code(ticker: Any) -> str:
    return str(ticker or "").split(".", 1)[0].strip().upper()


def spread_pairs(current: Mapping, config: list, dashboard: Any) -> list[dict]:
    """(current.json, config.json) + data/summary.json → summary §6.2 pairs 모양(폴백 경로)."""
    details: dict = {}
    if isinstance(dashboard, Mapping) and isinstance(dashboard.get("pairs"), list):
        details = {d.get("id"): d for d in dashboard["pairs"] if isinstance(d, Mapping)}
    prices = current.get("prices") if isinstance(current.get("prices"), Mapping) else {}
    pairs = []
    for item in config or []:
        if not isinstance(item, Mapping) or not item.get("id"):
            continue
        quote = prices.get(item["id"]) if isinstance(prices.get(item["id"]), Mapping) else {}
        detail = details.get(item["id"]) or {}
        live = detail.get("current") if isinstance(detail.get("current"), Mapping) else {}
        score = detail.get("attractiveness") if isinstance(detail.get("attractiveness"), Mapping) else {}
        spread = quote.get("spread")
        pairs.append({
            "id": item["id"],
            "name": item.get("name") or item.get("commonName") or item["id"],
            "preferredName": item.get("preferredName"),
            "commonCode": _ticker_code(item.get("commonTicker")),
            "preferredCode": _ticker_code(item.get("preferredTicker")),
            "spread": spread,
            "spreadChange": quote.get("spreadChange"),
            "commonPrice": quote.get("commonPrice"),
            "preferredPrice": quote.get("preferredPrice"),
            "date": quote.get("date"),
            **spread_percentiles(detail.get("spreadPercentiles"), spread),
            "attractiveness": score.get("total"),
            "commonAvgTradedValue20": live.get("commonAvgTradedValue20"),
            "preferredAvgTradedValue20": live.get("preferredAvgTradedValue20"),
        })
    return pairs


def holding_pairs(current: Mapping, config: list, dashboard: Any) -> list[dict]:
    """(current.json, config.json) + data/summary.json → summary §6.1 pairs 모양(폴백 경로)."""
    histories: dict = {}
    if isinstance(dashboard, Mapping) and isinstance(dashboard.get("pairs"), list):
        histories = {d.get("id"): d.get("percentileHistory") for d in dashboard["pairs"] if isinstance(d, Mapping)}
    live = {p.get("id"): p for p in current.get("pairs") or [] if isinstance(p, Mapping)}
    snapshot = current.get("generatedAt") or current.get("lastUpdated")
    pairs = []
    for item in config or []:
        if not isinstance(item, Mapping) or not item.get("id"):
            continue
        row = live.get(item["id"]) or {}
        pairs.append({
            "id": item["id"],
            "name": item.get("name") or item["id"],
            "holdingName": item.get("holdingName"),
            "code": _ticker_code(item.get("holdingTicker")),
            "ratio": row.get("ratio"),
            "ratioChange": row.get("ratioChange"),
            "holdingValue": row.get("holdingValue"),
            "marketCap": row.get("marketCap"),
            **holding_percentiles(histories.get(item["id"]), row.get("ratio"), snapshot),
        })
    return pairs


def nps_source_from_current(current: Mapping) -> dict:
    """nps-tracker current.json → summary §6.7 맵 모양(폴백 경로). 지분율 변화는 여기에 없다."""
    weights: dict[str, float] = {}
    ownership: dict[str, float] = {}
    names: dict[str, Any] = {}
    for holding in current.get("holdings") or []:
        if not isinstance(holding, Mapping):
            continue
        code = str(holding.get("stock_code") or "").upper()
        if not code:
            continue
        names[code] = holding.get("stock_name")
        weight = finite(holding.get("weight"))
        if weight is not None:
            weights[code] = weight
        pct = finite(holding.get("ownership_pct"))
        if pct is not None and pct > 0:
            ownership[code] = pct
    match = _SOURCE_DATE_RE.search(str(current.get("source") or ""))
    summary = current.get("summary") if isinstance(current.get("summary"), Mapping) else {}
    return {
        "weights": weights,
        "ownership": ownership,
        "change": None,
        "positionsAsOf": match.group(1) if match else None,
        "priceAsOf": summary.get("asOf") or current.get("asOf"),
        "names": names,
    }


# ---------------------------------------------------------------------------
# 신호 정규화
# ---------------------------------------------------------------------------

def _signal(tool_id: str, kind: str, code: str, name: Any, **fields: Any) -> dict:
    warnings = fields.pop("warnings", [])
    return {
        "tool": tool_id,
        "kind": kind,
        "label": TOOL_LABELS[tool_id],
        "code": code,
        "relatedCode": fields.pop("relatedCode", None),
        "name": name,
        "score": fields.pop("score", None),
        "scoreBasis": fields.pop("scoreBasis", None),
        "scoreNote": fields.pop("scoreNote", None),
        "rankable": fields.pop("rankable", False),
        "metrics": fields.pop("metrics", []),
        "asOf": fields.pop("asOf", None),
        "distributionEnd": fields.pop("distributionEnd", None),
        "warnings": warnings,
        "stale": bool(warnings),
        "averageTradedValue20": fields.pop("averageTradedValue20", None),
        "url": None,
        **fields,
    }


def preferred_signal(pair: Mapping, as_of: Any, today: date) -> dict | None:
    """우선주 쌍 → 신호. 거래 대상은 우선주이므로 code 는 우선주 코드다."""
    spread = finite(pair.get("spread"))
    code = str(pair.get("preferredCode") or "").upper()
    if spread is None or not code:
        return None
    score, history_warning = history_score(pair.get("spreadPct3y"), pair.get("spreadPctEnd"), today)
    day = day_text(pair.get("date")) or day_text(as_of)
    pct1y = finite(pair.get("spreadPct1y"))
    return _signal(
        TOOL_PREFERRED, "preferred", code, pair.get("preferredName") or pair.get("name"),
        relatedCode=str(pair.get("commonCode") or "").upper() or None,
        pairName=pair.get("name"),
        score=score,
        scoreBasis=BASIS_HISTORY if score is not None else None,
        scoreNote="자기 3년 분포에서 현재 괴리율 이하인 비율" if score is not None else None,
        rankable=score is not None,
        metrics=_metrics(
            metric("괴리율", spread, "%"),
            metric("전일 대비", pair.get("spreadChange"), "%p"),
            metric("3년 백분위", score, "pctile"),
            metric("1년 백분위", pct1y if score is not None else None, "pctile"),
            metric("cps 매력도", pair.get("attractiveness"), "점"),
            metric("보통주", pair.get("commonPrice"), "원"),
            metric("우선주", pair.get("preferredPrice"), "원"),
            metric("보통주 20일 평균 거래대금", pair.get("commonAvgTradedValue20"), "원"),
        ),
        asOf=day,
        distributionEnd=day_text(pair.get("spreadPctEnd")),
        warnings=_warnings(freshness_warning(TOOL_PREFERRED, day, today), history_warning),
        averageTradedValue20=finite(pair.get("preferredAvgTradedValue20")),
    )


def holding_signal(pair: Mapping, as_of: Any, today: date) -> dict | None:
    ratio = finite(pair.get("ratio"))
    code = str(pair.get("code") or "").upper()
    if ratio is None or not code:
        return None
    score, history_warning = history_score(pair.get("ratioPct3y"), pair.get("ratioPctEnd"), today)
    day = day_text(as_of)
    pct1y = finite(pair.get("ratioPct1y"))
    return _signal(
        TOOL_HOLDING, "holding", code, pair.get("holdingName") or pair.get("name"),
        pairName=pair.get("name"),
        score=score,
        scoreBasis=BASIS_HISTORY if score is not None else None,
        scoreNote="자기 3년 분포에서 현재 비율 이하인 비율(오래된 구간은 주간 표본)" if score is not None else None,
        rankable=score is not None,
        metrics=_metrics(
            metric("보유가치/시총", ratio, "%"),
            metric("전일 대비", pair.get("ratioChange"), "%p"),
            metric("3년 백분위", score, "pctile"),
            metric("1년 백분위", pct1y if score is not None else None, "pctile"),
            metric("보유가치", pair.get("holdingValue"), "억원"),
            metric("시가총액", pair.get("marketCap"), "억원"),
        ),
        asOf=day,
        distributionEnd=day_text(pair.get("ratioPctEnd")),
        warnings=_warnings(freshness_warning(TOOL_HOLDING, day, today), history_warning),
    )


def spac_signals(spacs: Iterable[Any], as_of: Any, today: date) -> list[dict]:
    """스팩 전체 → 신호. 합병이 진행 중이면 청산 가정이 맞지 않으므로 순위와 모집단에서 뺀다."""
    items = [s for s in spacs if isinstance(s, Mapping) and s.get("code")]
    population = sorted_population(s.get("annualizedReturn") for s in items if not s.get("mergerStatus"))
    day = day_text(as_of)
    stale = freshness_warning(TOOL_SPAC, day, today)
    out = []
    for item in items:
        annualized = finite(item.get("annualizedReturn"))
        merger = item.get("mergerStatus")
        score = None if merger else cross_section_score(population, annualized)
        out.append(_signal(
            TOOL_SPAC, "spac", str(item["code"]).upper(), item.get("name"),
            score=score,
            scoreBasis=BASIS_CROSS_SECTION if score is not None else None,
            scoreNote=f"합병 진행이 없는 스팩 {len(population):,}개 중 연환산 기대수익 이하 비율" if score is not None else None,
            rankable=score is not None,
            metrics=_metrics(
                metric("연환산 기대수익(거래비용 차감 전)", annualized, "%"),
                metric("청산가 괴리", item.get("liquidationDiscountPct"), "%"),
                metric("현재가", item.get("currentPrice"), "원"),
                metric("현재 청산가", item.get("currentLiquidationValue"), "원"),
                metric("수령 예정일", day_text(item.get("payoutDate")), "text"),
                metric("상태", merger or item.get("status"), "text"),
            ),
            asOf=day,
            warnings=_warnings(stale, f"{merger}: 청산 가정의 연환산 값과 다를 수 있습니다" if merger else None),
        ))
    return out


def buyback_signals(by_code: Mapping, today: date, *, codes: Iterable[str] | None = None) -> list[dict]:
    """자사주 비율(보통주 최신 스냅샷) → 신호. ``codes`` 를 주면 그 종목만 만든다(모집단은 전체)."""
    rows = {str(code).upper(): row for code, row in by_code.items() if isinstance(row, Mapping)}
    population = sorted_population(row.get("treasuryRatioPct") for row in rows.values())
    wanted = None if codes is None else {str(code).upper() for code in codes}
    out = []
    for code, row in rows.items():
        if wanted is not None and code not in wanted:
            continue
        ratio = finite(row.get("treasuryRatioPct"))
        if ratio is None:
            continue
        score = cross_section_score(population, ratio)
        day = day_text(row.get("asOf"))
        out.append(_signal(
            TOOL_BUYBACKS, "buybacks", code, row.get("name"),
            score=score,
            scoreBasis=BASIS_CROSS_SECTION if score is not None else None,
            scoreNote=f"상장사 {len(population):,}개 중 자사주 비율 이하 비율" if score is not None else None,
            rankable=score is not None and ratio > 0,
            metrics=_metrics(metric("자사주 비율", ratio, "%")),
            asOf=day,
            warnings=_warnings(freshness_warning(TOOL_BUYBACKS, day, today)),
        ))
    return out


def nps_signals(source: Mapping, today: date, *, codes: Iterable[str] | None = None) -> list[dict]:
    """국민연금 비중·지분율·변화 → 신호. 지분율이 실제로 늘어난 종목만 순위에 들어간다.

    비중이나 지분율 수준은 기회 신호가 아니라 문맥이다. 변화를 낼 수 없으면(``comparable: false``) 점수 없이
    수치만 내고, 왜 변화가 없는지 경고로 알린다.
    """
    weights = source.get("weights") if isinstance(source.get("weights"), Mapping) else {}
    ownership = source.get("ownership") if isinstance(source.get("ownership"), Mapping) else {}
    names = source.get("names") if isinstance(source.get("names"), Mapping) else {}
    change = source.get("change") if isinstance(source.get("change"), Mapping) else None
    changes = change.get("changes") if change and isinstance(change.get("changes"), Mapping) else {}
    comparable = bool(change and change.get("comparable"))
    population = sorted_population(changes.values()) if comparable else []
    day = day_text(source.get("positionsAsOf"))
    stale = freshness_warning(TOOL_NPS, day, today)
    change_note = None
    if change and not comparable:
        change_note = f"{change.get('from')}·{change.get('to')} 공시의 지분율이 같아 변화를 계산할 수 없습니다"
    elif change is None:
        change_note = "지분율 변화 데이터가 없습니다"
    wanted = None if codes is None else {str(code).upper() for code in codes}
    out = []
    for raw_code in sorted({*weights, *ownership}):
        code = str(raw_code).upper()
        if wanted is not None and code not in wanted:
            continue
        # changes 에 없는 종목은 0이 아니라 모르는 값이다(신규 편입이거나 한쪽 공시에 지분율이 없을 수 있다).
        delta = finite(changes.get(raw_code)) if comparable else None
        score = cross_section_score(population, delta) if delta is not None and delta > 0 else None
        out.append(_signal(
            TOOL_NPS, "nps", code, names.get(raw_code),
            score=score,
            scoreBasis=BASIS_CROSS_SECTION if score is not None else None,
            scoreNote=f"지분율이 바뀐 {len(population):,}개 중 변화폭 이하 비율" if score is not None else None,
            rankable=score is not None,
            metrics=_metrics(
                metric("공시 지분율", ownership.get(raw_code), "%"),
                metric("지분율 변화", delta, "%p"),
                metric("포트폴리오 비중(추정 수량 기반)", weights.get(raw_code), "%"),
                metric("가격 기준일", day_text(source.get("priceAsOf")), "text"),
            ),
            asOf=day,
            warnings=_warnings(stale, change_note),
        ))
    return out


def etf_signal(code: str, ranking: Mapping | None, as_of: Any, today: date) -> dict:
    """eiayn 커버 ETF. 점수는 형제 자체 모델 값이라 다른 신호와 같은 척도로 섞지 않는다."""
    ranking = ranking or {}
    day = day_text(as_of)
    return _signal(
        TOOL_ETF, "etf", code, ranking.get("shortName") or ranking.get("name"),
        metrics=_metrics(
            metric("AIYN 순위(상위 100)", ranking.get("rank"), "위"),
            metric("AIYN 점수", ranking.get("aiynScore"), "점"),
        ),
        asOf=day,
        warnings=_warnings(freshness_warning(TOOL_ETF, day, today)) if ranking else [],
    )


def percentile_by_code(signals: Iterable[dict], *code_fields: str) -> dict[str, int]:
    """신호 → {코드: 3년 백분위}. 한 코드에 쌍이 여럿이면 첫 쌍을 쓴다(``_match_preferred`` 와 같은 규칙)."""
    out: dict[str, int] = {}
    seen: set[str] = set()
    for signal in signals:
        for field in code_fields:
            code = signal.get(field)
            if not code or code in seen:
                continue
            seen.add(code)
            if signal.get("score") is not None:
                out[code] = signal["score"]
    return out


# ---------------------------------------------------------------------------
# 유동성
# ---------------------------------------------------------------------------

def liquidity_info(
    average20: Any = None,
    average_as_of: Any = None,
    quote: Mapping | None = None,
    *,
    session_complete: bool = True,
) -> dict:
    """후보의 거래대금. 형제의 20일 평균을 먼저 쓰고, 없으면 공유 시세의 당일 누적을 쓴다.

    둘 다 없으면 ``value`` 는 None 이다. 장중 누적값은 아침에 작게 보이므로 장이 끝난 뒤에만 ``low`` 로 판정한다.
    """
    value = finite(average20)
    if value is not None:
        return {"value": value, "basis": "avg20d", "asOf": day_text(average_as_of), "low": value < LOW_LIQUIDITY_KRW}
    if isinstance(quote, Mapping):
        value = finite(quote.get("trade_value"))
        if value is not None:
            low = value < LOW_LIQUIDITY_KRW if session_complete else None
            return {"value": value, "basis": "session", "asOf": day_text(quote.get("date")), "low": low}
    return {"value": None, "basis": None, "asOf": None, "low": None}


def signal_liquidity(signal: Mapping | None, quote: Mapping | None, *, session_complete: bool = True) -> dict:
    """신호의 20일 평균이 있으면 그 값을, 없으면 시세를 쓴다. 평균의 기준일은 일봉 분포의 마지막 날이다."""
    signal = signal or {}
    return liquidity_info(
        signal.get("averageTradedValue20"), signal.get("distributionEnd") or signal.get("asOf"),
        quote, session_complete=session_complete,
    )


# ---------------------------------------------------------------------------
# 순위
# ---------------------------------------------------------------------------

def _order_key(signal: Mapping) -> tuple:
    score = signal["score"]
    return (-min(int(score // 10), 9), _BASIS_ORDER.get(signal.get("scoreBasis"), 9), -score)


def select_candidates(
    signals: Iterable[dict],
    *,
    min_score: float = BOARD_MIN_SCORE,
    per_tool: int = BOARD_PER_TOOL,
) -> tuple[list[dict], dict[str, int]]:
    """신호 → (행 목록, 도구별 후보 수). 시세 조회 전 단계이고 순위 번호는 :func:`rank_rows` 가 붙인다.

    행은 종목코드 하나다. 대표 신호는 순서가 가장 앞선 신호이고, 같은 코드와 관련 코드(우선주의 보통주)의
    나머지 신호는 문맥으로 붙는다. 도구별 상한은 대표 신호 기준이며 잘린 수는 두 번째 반환값으로 드러난다.
    """
    by_code: dict[str, list[dict]] = {}
    for signal in signals:
        by_code.setdefault(signal["code"], []).append(signal)
    primaries = []
    for group in by_code.values():
        eligible = [s for s in group if s.get("rankable") and s["score"] >= min_score]
        if eligible:
            primaries.append(min(eligible, key=_order_key))
    primaries.sort(key=_order_key)
    eligible_counts: Counter[str] = Counter(p["tool"] for p in primaries)
    kept: Counter[str] = Counter()
    rows = []
    for primary in primaries:
        if kept[primary["tool"]] >= per_tool:
            continue
        kept[primary["tool"]] += 1
        related = by_code.get(primary.get("relatedCode") or "", [])
        others = [s for s in [*by_code[primary["code"]], *related] if s is not primary]
        others.sort(key=lambda s: (s.get("score") is None, -(s.get("score") or 0)))
        rows.append({"code": primary["code"], "name": primary.get("name"), "primary": primary, "others": others})
    return rows, dict(eligible_counts)


def rank_rows(rows: list[dict], quotes: Mapping, *, session_complete: bool = True) -> list[dict]:
    """시세를 붙이고 최종 순서를 정한다. 같은 구간·기준·점수이면 거래대금이 큰 후보가 앞이다."""
    for row in rows:
        primary = row["primary"]
        row["liquidity"] = signal_liquidity(primary, quotes.get(row["code"]), session_complete=session_complete)
    rows.sort(key=lambda r: (*_order_key(r["primary"]), -(r["liquidity"]["value"] or -1), str(r.get("name") or "")))
    for index, row in enumerate(rows, start=1):
        row["rank"] = index
    return rows
