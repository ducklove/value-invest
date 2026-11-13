"""Value screener service — validation, universe assembly, in-memory filtering.

Sits between ``routes/screener.py`` (HTTP) and ``repositories/screener.py``
(in-memory filter). Responsibilities:

* Validate and normalize the filter spec coming from the API so the repo only
  ever sees trusted (key, op, value) tuples.
* Fetch the full-universe snapshot from finance-pi and cache *the snapshot*
  (not the filtered result) so repeated screens with different filters reuse
  one fetch.
* Attach multi-year history metrics (``services/screener_history`` +
  ``domain/screener_history``) to every row and evaluate the history
  conditions (턴어라운드, 배당 증가 …) the UI offers as presets.
* Round numbers sensibly for display.

Caches:
* ``screener.snapshot`` — the finance-pi snapshot itself (10 min TTL, DB).
* ``screener.history`` — yearly financials/dividends (24 h, DB; owned by
  ``services/screener_history``).
* The enriched universe (snapshot rows + history metrics) is memoized in
  memory and rebuilt only when either source changes. Filtering ~3,000 rows
  in memory takes milliseconds, so filtered results are not cached.
"""

from __future__ import annotations

from typing import Any, Callable, Iterable

from core.errors import AppError, ExternalServiceError
from domain.screener_history import derive_history_metrics, most_common_latest_year
from repositories import cache_values
from repositories import screener as screener_repo
from services import screener_history
from services.market.sources.close_price import ClosePriceClientError, get_screener_snapshot

# Which metrics may be filtered, with a human label and sane bounds. Bounds
# exist to reject nonsense (e.g. P/E of 1e9) that would still run but produce
# a useless result. Kept intentionally loose — value screens vary.
# ``group`` is the heading the UI files the input card under.
FILTER_SPECS: dict[str, dict[str, Any]] = {
    "per": {"label": "P/E (배)", "group": "밸류에이션", "min": -1000, "max": 1000},
    "pbr": {"label": "P/B (배)", "group": "밸류에이션", "min": -100, "max": 100},
    "dividend_yield": {"label": "배당수익률 (%)", "group": "밸류에이션", "min": -50, "max": 200},
    "market_cap": {"label": "시가총액 (억원)", "group": "밸류에이션", "min": 0, "max": 1e8},
    "roe": {"label": "ROE (%)", "group": "수익성·재무", "min": -500, "max": 1000},
    "operating_margin": {"label": "영업이익률 (%)", "group": "수익성·재무", "min": -500, "max": 500},
    "debt_ratio": {"label": "부채비율 (%)", "group": "수익성·재무", "min": -100, "max": 1e6},
    "revenue_growth": {"label": "매출 증가율 (%)", "group": "성장 (전년 대비)", "min": -100, "max": 1e5},
    "op_profit_growth": {"label": "영업이익 증가율 (%)", "group": "성장 (전년 대비)", "min": -100, "max": 1e5},
    "dps_growth": {"label": "주당배당금 증가율 (%)", "group": "성장 (전년 대비)", "min": -100, "max": 1e5},
}

# 히스토리 지표가 필요한 수치 필터 — 히스토리 자료가 준비되기 전에는 쓸 수 없다.
HISTORY_FILTERS = frozenset({"revenue_growth", "op_profit_growth", "dps_growth"})


def _at_least(key: str, minimum: int) -> Callable[[dict], bool]:
    def test(row: dict) -> bool:
        value = row.get(key)
        return isinstance(value, (int, float)) and not isinstance(value, bool) and value >= minimum

    return test


def _is_true(key: str) -> Callable[[dict], bool]:
    return lambda row: row.get(key) is True


# 히스토리 조건 프리셋. 입력 없이 칩 하나로 켜고 끈다. ``description`` 은 조건의
# 정의이며 화면에 그대로 노출된다 — 계산은 domain/screener_history.py.
HISTORY_CONDITIONS: dict[str, dict[str, Any]] = {
    "turnaround": {
        "label": "영업이익 흑자 전환",
        "group": "실적 추세",
        "description": "직전 사업연도 영업손실 → 최근 사업연도 영업흑자 (턴어라운드)",
        "test": _is_true("turnaround_op"),
    },
    "net_turnaround": {
        "label": "순이익 흑자 전환",
        "group": "실적 추세",
        "description": "직전 사업연도 순손실 → 최근 사업연도 순이익 흑자",
        "test": _is_true("turnaround_net"),
    },
    "revenue_growth_3y": {
        "label": "매출 3년 연속 성장",
        "group": "실적 추세",
        "description": "최근 사업연도까지 3년 연속으로 매출이 전년보다 증가",
        "test": _at_least("revenue_growth_years", 3),
    },
    "op_profit_growth_3y": {
        "label": "영업이익 3년 연속 증가",
        "group": "실적 추세",
        "description": "최근 사업연도까지 3년 연속으로 영업이익이 전년보다 증가",
        "test": _at_least("op_profit_growth_years", 3),
    },
    "profit_5y": {
        "label": "5년 연속 흑자",
        "group": "실적 추세",
        "description": "최근 5개 사업연도 모두 순이익 흑자",
        "test": _at_least("profit_years", 5),
    },
    "roe_up_2y": {
        "label": "ROE 2년 연속 개선",
        "group": "수익성·재무",
        "description": "ROE(순이익 ÷ 자본)가 2년 연속 전년보다 상승",
        "test": _at_least("roe_up_years", 2),
    },
    "margin_up_2y": {
        "label": "영업이익률 2년 연속 개선",
        "group": "수익성·재무",
        "description": "영업이익률(영업이익 ÷ 매출)이 2년 연속 전년보다 상승",
        "test": _at_least("op_margin_up_years", 2),
    },
    "debt_down_2y": {
        "label": "부채비율 2년 연속 하락",
        "group": "수익성·재무",
        "description": "부채비율(부채 ÷ 자본)이 2년 연속 전년보다 하락",
        "test": _at_least("debt_ratio_down_years", 2),
    },
    "dividend_growth": {
        "label": "배당 증가",
        "group": "배당",
        "description": "최근 사업연도 주당배당금이 전년보다 증가 (전년에도 배당 지급)",
        "test": _at_least("dps_growth_years", 1),
    },
    "dividend_growth_3y": {
        "label": "배당 3년 연속 증가",
        "group": "배당",
        "description": "최근 사업연도까지 3년 연속으로 주당배당금이 전년보다 증가",
        "test": _at_least("dps_growth_years", 3),
    },
    "dividend_no_cut_3y": {
        "label": "배당 3년 연속 무삭감",
        "group": "배당",
        "description": "최근 4개 사업연도 연속 배당, 주당배당금을 한 번도 줄이지 않음",
        "test": _at_least("dividend_no_cut_years", 3),
    },
}

ALLOWED_SORTS = tuple(screener_repo.ALLOWED_SORTS.keys())
DEFAULT_SORT = "market_cap"
DEFAULT_SORT_DIR = "desc"

# Snapshot cache — the expensive finance-pi call. 10 min keeps it fresh enough
# for intraday use without hammering finance-pi on every screen.
_SNAPSHOT_CACHE_TTL = 10 * 60
_SNAPSHOT_CACHE_NS = "screener.snapshot"

MAX_LIMIT = 200

# (스냅샷 cached_at, 히스토리 built_at) → 히스토리 지표를 붙인 유니버스 행.
_universe_memo: dict[str, Any] = {"key": None, "rows": []}


def reset_state() -> None:
    """모듈 전역 메모 초기화 — 테스트와 DB 교체용."""
    _universe_memo.update(key=None, rows=[])


class ScreenerError(AppError):
    """User-facing screener input error (maps to HTTP 400).

    AppError defaults to 500; we override status_code to 400 so bad filter
    input comes back as a client error.
    """

    status_code = 400
    default_detail = "스크리너 조건이 올바르지 않습니다."


def _coerce_number(key: str, op: str, raw: Any) -> float:
    """Validate one filter value, returning a clean float or raising."""
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise ScreenerError(f"{key}의 {op} 값이 숫자가 아닙니다.") from exc
    spec = FILTER_SPECS[key]
    if value < spec["min"] or value > spec["max"]:
        raise ScreenerError(
            f"{key}의 {op} 값이 허용 범위({spec['min']}~{spec['max']})를 벗어났습니다."
        )
    return value


def normalize_filters(raw: dict[str, Any]) -> dict[str, list[tuple[str, float]]]:
    """Turn the API payload into a trusted ``{metric: [(op, value), ...]}`` map.

    Input shape (each optional, both directions independent)::

        {"per": {"min": 3, "max": 15}, "roe": {"min": 10}, ...}

    A metric may carry both ``min`` and ``max`` simultaneously (range filter).
    A metric with neither is dropped silently. Unknown metric names are
    rejected so the API surface stays explicit.
    """
    if not isinstance(raw, dict):
        raise ScreenerError("필터는 object 형태여야 합니다.")
    filters: dict[str, list[tuple[str, float]]] = {}
    for key, bounds in raw.items():
        if key not in FILTER_SPECS:
            raise ScreenerError(f"알 수 없는 필터 항목입니다: {key}")
        if not isinstance(bounds, dict):
            raise ScreenerError(f"{key} 필터는 object(min/max) 형태여야 합니다.")
        for op in ("min", "max"):
            if op in bounds and bounds[op] is not None:
                filters.setdefault(key, []).append((op, _coerce_number(key, op, bounds[op])))
    return filters


def _round_value(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, float):
        return round(value, 2)
    if isinstance(value, list):
        return [_round_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _round_value(item) for key, item in value.items()}
    return value


def _format_row(row: dict) -> dict:
    """Round float metrics (including the nested history table) for display."""
    return {key: _round_value(value) for key, value in row.items()}


async def _get_snapshot() -> tuple[list[dict], str | None]:
    """Fetch (or reuse cached) finance-pi screener snapshot rows.

    Returns ``(rows, cached_at)``; ``cached_at`` identifies this copy of the
    snapshot for the enriched-universe memo.

    finance-pi returns ``market_cap`` in KRW (원). The UI filter label and the
    display formatter both assume 억원 (100M KRW), so we normalize once here
    — the cached snapshot and every downstream filter/display see 억원.
    """
    cached = await cache_values.get_cache_value_entry(_SNAPSHOT_CACHE_NS, "latest")
    if cached is not None and isinstance(cached.value, list):
        return cached.value, cached.cached_at
    try:
        payload = await get_screener_snapshot()
    except ClosePriceClientError as exc:
        raise ExternalServiceError("스크리너 데이터를 불러오지 못했습니다.") from exc
    rows = payload.get("rows") or []
    for row in rows:
        mc = row.get("market_cap")
        if isinstance(mc, (int, float)) and mc != 0:
            # 원 → 억원. 0/None 은 그대로 둔다(데이터 없음).
            row["market_cap"] = round(mc / 1e8, 2)
    entry = await cache_values.set_cache_value(_SNAPSHOT_CACHE_NS, "latest", rows, ttl_seconds=_SNAPSHOT_CACHE_TTL)
    return rows, entry.cached_at


def _positive_number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        return None
    return float(value)


def enrich_rows(rows: list[dict], history: screener_history.HistorySeries | None) -> list[dict]:
    """스냅샷 행에 히스토리 지표를 붙이고, 재무 비율을 정확한 최근 사업연도 값으로 맞춘다.

    finance-pi 스냅샷은 계정 ID 별칭마다 최신값을 따로 골라 ``MAX`` 로 합치기
    때문에, 더는 쓰이지 않는 계정의 옛 값이 순이익·자본·매출에 섞여 든다
    (2026-11 표본에서 순이익이 절반가량 어긋났다). 배당수익률도 과거 공시
    수익률의 최댓값이다. 히스토리 자료가 있는 종목은 같은 사업보고서에서
    계정 우선순위대로 뽑은 값으로 다시 계산한다:

    * ROE·영업이익률·부채비율 = 최근 사업연도 재무제표 기준
    * P/B = 시가총액 ÷ 최근 사업연도 자본
    * 배당수익률 = 최근 사업연도 주당배당금 ÷ 현재 종가 (그 해 배당이 없으면 비움)

    히스토리 값이 없는 지표는 스냅샷 값을 그대로 둔다. P/E 는 스냅샷 값(네이버
    우선)을 유지한다. ``history`` 가 ``None`` 이면 지표만 비운 행을 돌려준다.
    """
    data = history or {}
    reference_year = most_common_latest_year(data) if data else None
    enriched: list[dict] = []
    for row in rows:
        out = dict(row)
        metrics = derive_history_metrics(data.get(str(row.get("ticker") or ""), {}), reference_year=reference_year)
        fy_roe = metrics.pop("fy_roe")
        fy_operating_margin = metrics.pop("fy_operating_margin")
        fy_debt_ratio = metrics.pop("fy_debt_ratio")
        fy_equity = metrics.pop("fy_equity")
        out.update(metrics)
        if metrics["history_fy"] is not None:
            if fy_roe is not None:
                out["roe"] = fy_roe
            if fy_operating_margin is not None:
                out["operating_margin"] = fy_operating_margin
            if fy_debt_ratio is not None:
                out["debt_ratio"] = fy_debt_ratio
            market_cap = _positive_number(out.get("market_cap"))
            equity = _positive_number(fy_equity)
            if market_cap is not None and equity is not None:
                out["pbr"] = market_cap * 1e8 / equity
        if metrics["history_fy"] is not None or metrics["dps_fy"] is not None:
            close = _positive_number(out.get("close"))
            dps = _positive_number(metrics["dps"])
            out["dividend_yield"] = dps / close * 100 if dps is not None and close is not None else None
        enriched.append(out)
    return enriched


async def _get_universe() -> tuple[list[dict], screener_history.HistoryState]:
    """스냅샷 + 히스토리 지표를 붙인 전 유니버스 행. 두 원본이 그대로면 메모를 쓴다."""
    snapshot, snapshot_stamp = await _get_snapshot()
    tickers = [str(row.get("ticker")) for row in snapshot if row.get("ticker")]
    history = await screener_history.get_history(tickers)
    key = (snapshot_stamp, len(snapshot), history.built_at)
    if _universe_memo["key"] != key:
        _universe_memo.update(key=key, rows=enrich_rows(snapshot, history.data))
    return _universe_memo["rows"], history


def normalize_conditions(raw: Any) -> list[str]:
    """히스토리 조건 키 목록을 검증한다 — 모르는 키는 거부, 중복은 한 번만."""
    if raw is None:
        return []
    if not isinstance(raw, (list, tuple)):
        raise ScreenerError("히스토리 조건은 배열 형태여야 합니다.")
    conditions: list[str] = []
    for key in raw:
        if not isinstance(key, str) or key not in HISTORY_CONDITIONS:
            raise ScreenerError(f"알 수 없는 히스토리 조건입니다: {key}")
        if key not in conditions:
            conditions.append(key)
    return conditions


def _history_payload(history: screener_history.HistoryState, rows: list[dict] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": history.status,
        "built_at": history.built_at,
        "refreshing": history.refreshing,
        "progress": history.progress,
    }
    if rows is not None and history.status == "ready":
        years = [row["history_fy"] for row in rows if row.get("history_fy") is not None]
        payload["covered"] = len(years)
        payload["dividend_covered"] = sum(1 for row in rows if row.get("dps") is not None)
        # 가장 많은 종목의 최근 사업연도 — 일찍 공시한 몇 종목의 다음 연도에 끌려가지 않게 최빈값.
        payload["latest_fiscal_year"] = max(set(years), key=lambda fy: (years.count(fy), fy)) if years else None
    return payload


async def run_screen(
    filters_raw: dict[str, Any],
    *,
    conditions_raw: Any = None,
    watchlist: Iterable[str] | None = None,
    sort_by: str = DEFAULT_SORT,
    sort_dir: str = DEFAULT_SORT_DIR,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """Validate inputs, assemble the universe, filter in memory, shape the response.

    ``watchlist`` — 주어지면(빈 집합 포함) 그 종목 코드만 대상으로 한다
    ("관심종목만 보기"). ``None`` 이면 전 유니버스.
    """
    if sort_by not in ALLOWED_SORTS:
        raise ScreenerError(f"정렬 기준은 {', '.join(ALLOWED_SORTS)} 중 하나여야 합니다.")
    if sort_dir not in ("asc", "desc"):
        raise ScreenerError("정렬 방향은 asc 또는 desc 여야 합니다.")
    if limit <= 0 or limit > MAX_LIMIT:
        raise ScreenerError(f"limit 은 1~{MAX_LIMIT} 사이여야 합니다.")
    if offset < 0:
        raise ScreenerError("offset 은 0 이상이어야 합니다.")

    filters = normalize_filters(filters_raw)
    conditions = normalize_conditions(conditions_raw)
    if not filters and not conditions and watchlist is None:
        # 빈 조건 전수 스캔은 의도치 않은 호출(수천 건 행 반환)이므로 거부한다.
        raise ScreenerError("필터·히스토리 조건을 하나 이상 고르거나 관심종목만 보기를 켜세요.")

    universe, history = await _get_universe()
    needs_history = bool(conditions) or any(key in HISTORY_FILTERS for key in filters)
    if needs_history and history.status != "ready":
        raise ScreenerError(
            "히스토리 데이터를 준비하는 중입니다. 2~3분 뒤 다시 시도하세요."
            if history.status == "building"
            else "히스토리 데이터를 불러올 수 없어 히스토리 조건을 쓸 수 없습니다."
        )

    candidates = universe
    watchlist_missing = 0
    if watchlist is not None:
        codes = {str(code).strip().upper() for code in watchlist if str(code).strip()}
        candidates = [row for row in candidates if str(row.get("ticker") or "").upper() in codes]
        watchlist_missing = len(codes) - len(candidates)
    for key in conditions:
        test = HISTORY_CONDITIONS[key]["test"]
        candidates = [row for row in candidates if test(row)]

    rows, total = screener_repo.screen_snapshot(
        candidates,
        filters,
        sort_by=sort_by,
        sort_dir=sort_dir,
        limit=limit,
        offset=offset,
    )

    return {
        "filters": {
            k: [{"op": op, "value": val} for op, val in pairs]
            for k, pairs in filters.items()
        },
        "conditions": conditions,
        "watchlist_only": watchlist is not None,
        "watchlist_missing": watchlist_missing,
        "sort_by": sort_by,
        "sort_dir": sort_dir,
        "limit": limit,
        "offset": offset,
        "total": total,
        "history": _history_payload(history),
        "rows": [_format_row(r) for r in rows],
    }


async def get_filter_specs() -> dict[str, Any]:
    """Available filters/conditions/sorts + current coverage, for the UI to render."""
    universe, history = await _get_universe()
    coverage = screener_repo.snapshot_coverage(universe)
    as_of = next((row.get("as_of") for row in universe if row.get("as_of")), None)
    return {
        "filters": {
            key: {**spec, "needs_history": key in HISTORY_FILTERS}
            for key, spec in FILTER_SPECS.items()
        },
        "conditions": [
            {"key": key, "label": spec["label"], "group": spec["group"], "description": spec["description"]}
            for key, spec in HISTORY_CONDITIONS.items()
        ],
        "sorts": ALLOWED_SORTS,
        "default_sort": DEFAULT_SORT,
        "default_sort_dir": DEFAULT_SORT_DIR,
        "max_limit": MAX_LIMIT,
        "coverage": coverage,
        "as_of": as_of,
        "history": _history_payload(history, universe),
    }
