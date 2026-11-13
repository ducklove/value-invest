"""스크리너 히스토리 데이터 — 전 유니버스 연도별 재무·배당 수집과 캐시.

finance-pi 의 스크리너 스냅샷(``/api/fundamentals/screener``)은 "최근 연간 보고서"
한 시점만 주고, 과거 시점(``as_of``)으로 다시 불러도 계정별 최신값을 섞어 쓰기
때문에 연도 간 비교에 쓸 수 없다. 대신 사업연도를 지정할 수 있는
``/api/fundamentals/basic?fiscal_year=`` 와 ``/api/fundamentals/dividends`` 를
종목 묶음으로 불러 ``{종목: {사업연도: 지표}}`` 시계열을 만든다.

비용: 유니버스 약 2,900종목 × 7개 연도를 400종목씩 묶어 약 60회 호출, 2~3분.
요청 경로에서 기다릴 수 없으므로 백그라운드 태스크로 한 번 모아 DB 캐시
(``cache_values``)에 24시간 둔다. 연간 재무·배당은 공시 시즌에만 바뀌므로 하루
한 번이면 충분하다. 만료된 뒤에는 옛 자료를 계속 보여 주면서 뒤에서 다시 모은다.

파생 지표 계산은 ``domain.screener_history`` (순수 규칙)에 있다.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from cache_layer import parse_iso
from domain.screener_history import SERIES_FIELDS
from domain.timeutil import now_kst
from repositories import cache_values
from services.market.sources import close_price, finance_pi
from services.market.sources.close_price import ClosePriceClientError

logger = logging.getLogger(__name__)

_CACHE_NS = "screener.history"
_CACHE_KEY = "v1"
HISTORY_TTL_SECONDS = 24 * 60 * 60

# 올해 포함 7개 연도를 묻는다. 연초(전년 사업보고서 공시 전)에도 완결된 사업연도
# 5개가 남아 "5년 연속" 조건을 계산할 수 있다.
FISCAL_YEAR_SPAN = 7
# finance-pi 는 한 호출에 500종목까지 받는다. 여유를 두고 400.
_BATCH_SIZE = 400
_BATCH_TIMEOUT_SECONDS = 60.0
# 묶음 사이에 숨을 돌려 finance-pi 의 조회 슬롯을 다른 요청(시세)에 양보한다.
_BATCH_PAUSE_SECONDS = 0.2
# 수집 실패 뒤 다시 시도하기까지의 간격.
_FAILURE_RETRY_SECONDS = 10 * 60

_FINANCIAL_METRICS = ("revenue", "operating_profit", "net_income", "equity", "liabilities")

HistorySeries = dict[str, dict[int, dict[str, float | None]]]


@dataclass(frozen=True)
class HistoryState:
    """스크리너가 히스토리 자료를 쓸 수 있는지와 그 자료.

    ``status``: ``ready``(자료 있음) · ``building``(첫 수집 중) ·
    ``unavailable``(finance-pi 꺼짐·수집 실패). ``refreshing`` 은 자료가 있는
    상태에서 뒤에서 다시 모으는 중인지.
    """

    status: str
    data: HistorySeries | None = None
    built_at: str | None = None
    fiscal_years: tuple[int, ...] = ()
    refreshing: bool = False
    progress: float | None = None


_memory: dict[str, Any] | None = None
_task: asyncio.Task | None = None
_progress: dict[str, int] = {"done": 0, "total": 0}
_last_failure: float | None = None


def reset_state() -> None:
    """모듈 전역 상태 초기화 — 테스트와 DB 교체용."""
    global _memory, _task, _last_failure
    if _task is not None and not _task.done():
        try:
            _task.cancel()
        except RuntimeError:
            # 태스크의 이벤트 루프가 이미 닫혔다(테스트 간 루프 교체) — 버리면 된다.
            pass
    _memory = None
    _task = None
    _last_failure = None
    _progress.update(done=0, total=0)


async def shutdown() -> None:
    """lifespan 종료 훅 — 진행 중인 수집을 멈춘다(닫힌 DB 에 쓰지 않게)."""
    task = _task
    if task is not None and not task.done():
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def wait_for_build() -> None:
    """진행 중인 수집이 끝날 때까지 기다린다(테스트·수동 실행용)."""
    task = _task
    if task is not None:
        await asyncio.gather(task, return_exceptions=True)


def _encode(data: HistorySeries) -> dict[str, dict[str, list[float | None]]]:
    return {
        code: {str(fy): [values.get(field) for field in SERIES_FIELDS] for fy, values in sorted(series.items())}
        for code, series in data.items()
    }


def _decode(raw: Any) -> HistorySeries:
    data: HistorySeries = {}
    if not isinstance(raw, dict):
        return data
    for code, series in raw.items():
        if not isinstance(series, dict):
            continue
        decoded: dict[int, dict[str, float | None]] = {}
        for fy, values in series.items():
            if not isinstance(values, list) or not str(fy).isdigit():
                continue
            decoded[int(fy)] = dict(zip(SERIES_FIELDS, values, strict=False))
        if decoded:
            data[str(code)] = decoded
    return data


def _remember(payload: dict[str, Any], *, cached_at: str | None, expires_at: str | None) -> dict[str, Any]:
    global _memory
    _memory = {
        "data": _decode(payload.get("tickers")),
        "built_at": payload.get("built_at") or cached_at,
        "fiscal_years": tuple(int(fy) for fy in payload.get("fiscal_years") or ()),
        "expires_at": parse_iso(expires_at),
    }
    return _memory


def _is_stale(entry: dict[str, Any]) -> bool:
    expires_at = entry.get("expires_at")
    # cache_values 는 만료 시각을 naive 로컬 시각으로 적는다 — 같은 시계로 비교한다.
    return expires_at is None or datetime.now() >= expires_at


async def _load_cached() -> dict[str, Any] | None:
    if _memory is not None:
        return _memory
    entry = await cache_values.get_cache_value_entry(_CACHE_NS, _CACHE_KEY, allow_stale=True)
    if entry is None or not isinstance(entry.value, dict):
        return None
    return _remember(entry.value, cached_at=entry.cached_at, expires_at=entry.expires_at)


def _building() -> bool:
    return _task is not None and not _task.done()


def _progress_ratio() -> float | None:
    total = _progress["total"]
    return round(_progress["done"] / total, 3) if total else None


def _can_start_build() -> bool:
    if not close_price.ENABLED or _building():
        return False
    if _last_failure is not None and time.monotonic() - _last_failure < _FAILURE_RETRY_SECONDS:
        return False
    return True


def _on_build_done(task: asyncio.Task) -> None:
    global _last_failure
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        _last_failure = time.monotonic()
        logger.error("스크리너 히스토리 수집이 예기치 않게 실패했습니다.", exc_info=exc)


def _start_build(tickers: list[str]) -> None:
    global _task
    _task = asyncio.create_task(_build(list(tickers)), name="screener-history")
    _task.add_done_callback(_on_build_done)


async def get_history(tickers: list[str]) -> HistoryState:
    """캐시된 히스토리 자료를 돌려주고, 없거나 낡았으면 뒤에서 수집을 시작한다.

    기다리지 않는다 — 첫 수집 중에는 ``status="building"`` 을 돌려주고 호출부가
    히스토리 없이 동작한다.
    """
    entry = await _load_cached()
    if (entry is None or _is_stale(entry)) and tickers and _can_start_build():
        _start_build(tickers)
    building = _building()
    if entry is not None and entry["data"]:
        return HistoryState(
            status="ready",
            data=entry["data"],
            built_at=entry["built_at"],
            fiscal_years=entry["fiscal_years"],
            refreshing=building,
            progress=_progress_ratio() if building else None,
        )
    if building:
        return HistoryState(status="building", progress=_progress_ratio())
    return HistoryState(status="unavailable")


async def _build(tickers: list[str]) -> None:
    global _last_failure
    this_year = now_kst().year
    fiscal_years = list(range(this_year - FISCAL_YEAR_SPAN + 1, this_year + 1))
    started = time.monotonic()
    try:
        data = await collect_history(tickers, fiscal_years)
    except ClosePriceClientError as exc:
        _last_failure = time.monotonic()
        logger.warning("스크리너 히스토리 수집 실패: %s", exc)
        return
    if not data:
        # 빈 결과를 24시간 캐시하면 하루 내내 조건이 전부 비게 된다.
        _last_failure = time.monotonic()
        logger.warning("스크리너 히스토리 수집 결과가 비어 있어 저장하지 않습니다.")
        return
    payload = {
        "built_at": now_kst().isoformat(timespec="seconds"),
        "fiscal_years": fiscal_years,
        "tickers": _encode(data),
    }
    entry = await cache_values.set_cache_value(_CACHE_NS, _CACHE_KEY, payload, ttl_seconds=HISTORY_TTL_SECONDS)
    _remember(payload, cached_at=entry.cached_at, expires_at=entry.expires_at)
    _last_failure = None
    logger.info(
        "스크리너 히스토리 수집 완료: %d종목, 사업연도 %d~%d, %.0f초",
        len(data), fiscal_years[0], fiscal_years[-1], time.monotonic() - started,
    )


def _amount(metric: Any) -> float | None:
    amount = metric.get("amount") if isinstance(metric, dict) else None
    if isinstance(amount, bool) or not isinstance(amount, (int, float)):
        return None
    return float(amount)


def pick_dps_by_year(rows: list[dict[str, Any]]) -> dict[int, float | None]:
    """배당 공시 행 → 사업연도별 주당 현금배당.

    같은 사업연도가 여러 사업보고서(당기·전기·전전기)에 실리므로 가장 늦게
    공시된 행을 쓴다. 종목 코드별로 조회하므로 보통주·우선주는 이미 갈려 있다.
    """
    latest: dict[int, tuple[str, float | None]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        fy = row.get("fiscal_year")
        if isinstance(fy, bool) or not isinstance(fy, int):
            continue
        available = str(row.get("available_date") or "")
        dps = row.get("cash_dividend_per_share")
        if isinstance(dps, bool) or not isinstance(dps, (int, float)):
            dps = None
        if fy not in latest or available > latest[fy][0]:
            latest[fy] = (available, None if dps is None else float(dps))
    return {fy: value for fy, (_, value) in latest.items()}


def _require_reachable() -> None:
    # 쿨다운 중이면 조회 함수가 빈 dict 를 돌려준다. 그걸 "자료 없음"으로 저장하지 않게 끊는다.
    if finance_pi.cooldown_active():
        raise ClosePriceClientError("finance-pi 쿨다운 중이라 히스토리 수집을 중단합니다.")


async def collect_history(tickers: list[str], fiscal_years: list[int]) -> HistorySeries:
    """finance-pi 에서 연도별 재무·배당을 모아 ``{종목: {사업연도: 지표}}`` 로 만든다."""
    codes = list(dict.fromkeys(str(t).strip().upper() for t in tickers if str(t).strip()))
    batches = [codes[i:i + _BATCH_SIZE] for i in range(0, len(codes), _BATCH_SIZE)]
    _progress.update(done=0, total=len(batches) * (len(fiscal_years) + 1))
    data: HistorySeries = {}

    for fiscal_year in fiscal_years:
        for batch in batches:
            _require_reachable()
            payload = await close_price.get_basic_fundamentals(
                batch, fiscal_year=fiscal_year, timeout=_BATCH_TIMEOUT_SECONDS
            )
            for code, item in payload.items():
                metrics = item.get("metrics") if isinstance(item, dict) else None
                if not isinstance(metrics, dict):
                    continue
                values = {name: _amount(metrics.get(name)) for name in _FINANCIAL_METRICS}
                if any(value is not None for value in values.values()):
                    data.setdefault(code, {}).setdefault(fiscal_year, {}).update(values)
            _progress["done"] += 1
            await asyncio.sleep(_BATCH_PAUSE_SECONDS)

    for batch in batches:
        _require_reachable()
        dividends = await close_price.get_dividend_history(
            batch, start_year=fiscal_years[0], end_year=fiscal_years[-1], timeout=_BATCH_TIMEOUT_SECONDS
        )
        for code, rows in dividends.items():
            for fiscal_year, dps in pick_dps_by_year(rows).items():
                if dps is not None:
                    data.setdefault(code, {}).setdefault(fiscal_year, {})["dps"] = dps
        _progress["done"] += 1
        await asyncio.sleep(_BATCH_PAUSE_SECONDS)

    return data
