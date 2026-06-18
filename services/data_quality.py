"""정기 데이터 품질 점검 서비스 (로드맵 신규 기능 ⑧).

scripts/check_data_quality.py 의 일회성 점검을 서비스로 승격한 모듈.
두 종류의 점검이 산다:

* **정기 점검 (run_all_checks)** — data-quality.timer 가 매일 22:30(KST)
  POST /api/internal/data-quality/check 로 구동. 기존 테이블에 대한
  소수의 집계 쿼리만 수행하고(외부 API 호출 없음) 결과를
  system_events(source='data_quality') 에 기록한다. 임계 초과 시
  level=error 이벤트 + systemd OnFailure 훅이 알림 경로가 된다.

* **종목별 시계열 이상치 점검 (inspect_stock)** — 배당/주가 시계열의
  outlier 탐지. yfinance 를 두드리는 비싼 경로라 정기 점검에는 포함하지
  않고 CLI(scripts/check_data_quality.py) 전용으로 남긴다.

설계 원칙:

* **절대 깨지지 않는다.** 개별 점검의 예외는 status='error' 결과로
  변환된다. 점검 실패가 웹 프로세스를 흔들면 안 된다.
* **결과 형식은 고정.** 각 점검은 {check, status: 'ok'|'warn'|'error',
  detail, value} 를 반환한다. 비정상(warn/error)만 개별 이벤트로 남기고,
  전체 결과는 kind='check_summary' 요약 이벤트 하나에 통째로 담는다 —
  관리자 패널은 이 요약 이벤트 하나만 읽으면 된다.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

import observability
from repositories import benchmark_daily as benchmark_repo
from repositories import system_events as system_events_repo
from repositories.db import get_db

logger = logging.getLogger(__name__)

SOURCE = "data_quality"
SUMMARY_KIND = "check_summary"

# NAV 스냅샷(22:00)·벤치마크 증분이 끝난 뒤 22:30 점검이 도는 전제 —
# 평일 22:10 이후에만 '당일' 데이터를 기대한다. 그 전(수동 실행 등)에는
# 직전 거래일까지만 기대해 거짓 경보를 막는다.
SETTLED_MINUTES = 22 * 60 + 10
# 장중 스냅샷은 09:00 장 시작 이후에만 의미가 있다 — 그 전엔 검사 생략.
_INTRADAY_CHECK_FROM_MINUTES = 10 * 60

# NAV 스냅샷: 1~2 거래일 지연 → warn, 3거래일 이상 → error.
_NAV_ERROR_TRADING_DAYS = 3
# 현재 보유 종목별 스냅샷도 NAV와 같은 기준으로 본다. 전체 NAV가 최신이어도
# 특정 해외/특수 종목만 조용히 빠지는 경우를 잡기 위함이다.
_STOCK_SNAPSHOT_ERROR_TRADING_DAYS = 3
# 벤치마크(yfinance): 미국장 시차·휴장·소스 지연을 감안해 느슨하게.
_BENCH_OK_TRADING_DAYS = 2
_BENCH_ERROR_TRADING_DAYS = 5
# system_events error 율: 최근 24시간 error 가 이 건수 이상이면 격상.
_EVENT_ERROR_ESCALATE = 10
_REUTERS_LIKE_SUFFIXES = (".OQ", ".PK", ".O", ".K")

# 종목별 시계열 점검 기본 임계치 (CLI --max-dividend-yield/--max-dividend-jump).
DEFAULT_MAX_DIVIDEND_YIELD = 50.0
DEFAULT_MAX_DIVIDEND_JUMP = 5.0


# ---------------------------------------------------------------------------
# 거래일 헬퍼 — 한국 공휴일은 무시한다. 휴장일의 거짓 경보(warn 1건)가
# 휴일 캘린더를 통째로 들이는 비용보다 싸다 (admin._compute_staleness 와
# 동일한 트레이드오프).
# ---------------------------------------------------------------------------

def _prev_weekday(d: date) -> date:
    d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def last_expected_trading_day(now: datetime, *, settled_minutes: int | None = None) -> date:
    """주말을 건너뛴 '데이터가 있어야 할 마지막 거래일'.

    settled_minutes 가 주어지면 평일 그 시각(분 단위) 이후에만 당일을
    기대하고, 그 전에는 직전 거래일로 물러난다.
    """
    d = now.date()
    if d.weekday() < 5:
        if settled_minutes is None or now.hour * 60 + now.minute >= settled_minutes:
            return d
    return _prev_weekday(d)


def trading_day_gap(expected: date, latest: date) -> int:
    """expected 까지 채워졌어야 할 데이터가 latest 에서 멈췄을 때, 빠진
    거래일(평일) 수. latest >= expected 면 0."""
    gap = 0
    cursor = expected
    while cursor > latest:
        if cursor.weekday() < 5:
            gap += 1
        cursor -= timedelta(days=1)
    return gap


# ---------------------------------------------------------------------------
# 정기 점검 함수들 — 각자 {check, status, detail, value} 를 반환한다.
# ---------------------------------------------------------------------------

async def check_nav_snapshot_freshness(now: datetime | None = None) -> dict:
    """NAV 스냅샷 신선도 — portfolio_snapshots 의 MAX(date) vs 기대 거래일."""
    check = "nav_snapshot_freshness"
    now = now or datetime.now()
    expected = last_expected_trading_day(now, settled_minutes=SETTLED_MINUTES)
    db = await get_db()
    cursor = await db.execute("SELECT MAX(date) AS d FROM portfolio_snapshots")
    row = await cursor.fetchone()
    latest = row["d"] if row else None
    if not latest:
        # 포트폴리오 사용자가 아예 없으면 스냅샷이 없는 게 정상이다.
        cursor = await db.execute("SELECT COUNT(*) AS n FROM user_portfolio")
        has_holdings = int((await cursor.fetchone())["n"]) > 0
        if not has_holdings:
            return {"check": check, "status": "ok", "detail": "포트폴리오 사용자 없음 — 검사 생략", "value": None}
        return {"check": check, "status": "error", "detail": "NAV 스냅샷 데이터 없음 (보유 종목은 존재)", "value": None}
    try:
        latest_d = date.fromisoformat(str(latest))
    except ValueError:
        return {"check": check, "status": "error", "detail": f"스냅샷 날짜 파싱 실패: {latest}", "value": None}
    gap = trading_day_gap(expected, latest_d)
    if gap <= 0:
        return {"check": check, "status": "ok", "detail": f"최신 {latest} (기대 {expected})", "value": 0}
    status = "error" if gap >= _NAV_ERROR_TRADING_DAYS else "warn"
    return {
        "check": check,
        "status": status,
        "detail": f"최신 {latest} — 기대 {expected} 대비 거래일 {gap}일 지연",
        "value": gap,
    }


async def check_portfolio_stock_snapshot_freshness(now: datetime | None = None) -> dict:
    """현재 보유 종목별 스냅샷 신선도.

    전체 NAV 최신성만 보면 한 종목의 가격 조회가 실패해 이전값으로 묻히는
    문제를 놓칠 수 있다. 현재 보유 중인 비현금 종목마다
    portfolio_stock_snapshots 의 최신 날짜를 확인한다.
    """
    check = "portfolio_stock_snapshot_freshness"
    now = now or datetime.now()
    expected = last_expected_trading_day(now, settled_minutes=SETTLED_MINUTES)
    db = await get_db()
    cursor = await db.execute(
        """
        WITH holdings AS (
            SELECT google_sub, stock_code
            FROM user_portfolio
            WHERE stock_code IS NOT NULL
              AND stock_code NOT LIKE 'CASH_%'
              AND COALESCE(quantity, 0) > 0
        ),
        latest AS (
            SELECT google_sub, stock_code, MAX(date) AS latest_date
            FROM portfolio_stock_snapshots
            GROUP BY google_sub, stock_code
        )
        SELECT h.google_sub, h.stock_code, l.latest_date
        FROM holdings h
        LEFT JOIN latest l
          ON l.google_sub = h.google_sub
         AND l.stock_code = h.stock_code
        ORDER BY h.google_sub, h.stock_code
        """
    )
    rows = [dict(row) for row in await cursor.fetchall()]
    if not rows:
        return {"check": check, "status": "ok", "detail": "보유 비현금 종목 없음 — 검사 생략", "value": None}

    missing = [row for row in rows if not row.get("latest_date")]
    stale: list[dict] = []
    parse_errors: list[dict] = []
    for row in rows:
        latest = row.get("latest_date")
        if not latest:
            continue
        try:
            latest_d = date.fromisoformat(str(latest))
        except ValueError:
            parse_errors.append(row)
            continue
        gap = trading_day_gap(expected, latest_d)
        if gap > 0:
            stale.append({**row, "gap": gap})

    if not (missing or parse_errors or stale):
        return {"check": check, "status": "ok", "detail": f"보유 종목별 스냅샷 최신 (기대 {expected})", "value": 0}

    stale.sort(key=lambda row: row["gap"], reverse=True)
    max_gap = stale[0]["gap"] if stale else 0
    status = "error" if (missing or parse_errors or max_gap >= _STOCK_SNAPSHOT_ERROR_TRADING_DAYS) else "warn"
    details: list[str] = []
    if missing or parse_errors:
        examples = missing[:4] + parse_errors[:4]
        missing_detail = ", ".join(f"{row['stock_code']}({row['google_sub'][:8]})" for row in examples)
        missing_suffix = " 등" if len(missing) + len(parse_errors) > len(examples) else ""
        details.append(f"누락/날짜 오류: {missing_detail}{missing_suffix}")
    if stale:
        stale_examples = stale[:5]
        for row in stale:
            code = row["stock_code"].upper()
            if any(code.endswith(suffix) for suffix in _REUTERS_LIKE_SUFFIXES) and row not in stale_examples:
                stale_examples.append(row)
            if len(stale_examples) >= 8:
                break
        for row in stale:
            if "." in row["stock_code"] and row not in stale_examples:
                stale_examples.append(row)
            if len(stale_examples) >= 8:
                break
        stale_detail = ", ".join(
            f"{row['stock_code']} 최신 {row['latest_date']}({row['gap']}일)"
            for row in stale_examples
        )
        stale_suffix = " 등" if len(stale) > len(stale_examples) else ""
        details.append(f"지연: {stale_detail}{stale_suffix}")
    return {
        "check": check,
        "status": status,
        "detail": f"보유 종목별 스냅샷 {'; '.join(details)} — 기대 {expected}",
        "value": max_gap or len(missing) + len(parse_errors),
    }


async def check_intraday_points(now: datetime | None = None) -> dict:
    """장중 스냅샷 — 거래일이라면 금일 portfolio_intraday 포인트가 있어야 한다."""
    check = "intraday_points"
    now = now or datetime.now()
    today = now.date()
    if today.weekday() >= 5:
        return {"check": check, "status": "ok", "detail": "주말 — 검사 생략", "value": None}
    if now.hour * 60 + now.minute < _INTRADAY_CHECK_FROM_MINUTES:
        return {"check": check, "status": "ok", "detail": "장 시작 전 — 검사 생략", "value": None}
    db = await get_db()
    cursor = await db.execute("SELECT COUNT(*) AS n FROM user_portfolio")
    if int((await cursor.fetchone())["n"]) == 0:
        return {"check": check, "status": "ok", "detail": "포트폴리오 사용자 없음 — 검사 생략", "value": None}
    # ts 는 로컬 KST ISO 분 단위 문자열 — 사전순 범위 스캔이 시간순과 일치.
    cursor = await db.execute(
        "SELECT COUNT(*) AS n FROM portfolio_intraday WHERE ts >= ? AND ts < ?",
        (today.isoformat() + "T00:00", today.isoformat() + "T99:99"),
    )
    count = int((await cursor.fetchone())["n"])
    if count == 0:
        return {"check": check, "status": "warn", "detail": f"금일({today}) 장중 스냅샷 0건", "value": 0}
    return {"check": check, "status": "ok", "detail": f"금일 장중 스냅샷 {count}건", "value": count}


async def check_benchmark_freshness(now: datetime | None = None) -> list[dict]:
    """벤치마크 일별 종가 신선도 — 추적 코드(YF_TICKER)별로 하나씩."""
    import benchmark_history

    now = now or datetime.now()
    expected = last_expected_trading_day(now, settled_minutes=SETTLED_MINUTES)
    results: list[dict] = []
    for code in benchmark_history.YF_TICKER:
        check = f"benchmark_freshness_{code}"
        latest = await benchmark_repo.get_benchmark_last_date(code)
        if not latest:
            # 한 번도 백필되지 않은 코드일 수 있다 — error 까지는 아님.
            results.append({"check": check, "status": "warn", "detail": f"{code} benchmark_daily 데이터 없음", "value": None})
            continue
        try:
            latest_d = date.fromisoformat(str(latest))
        except ValueError:
            results.append({"check": check, "status": "error", "detail": f"{code} 날짜 파싱 실패: {latest}", "value": None})
            continue
        gap = trading_day_gap(expected, latest_d)
        if gap <= _BENCH_OK_TRADING_DAYS:
            results.append({"check": check, "status": "ok", "detail": f"{code} 최신 {latest}", "value": gap})
        elif gap <= _BENCH_ERROR_TRADING_DAYS:
            results.append({"check": check, "status": "warn", "detail": f"{code} 최신 {latest} — 거래일 {gap}일 지연", "value": gap})
        else:
            results.append({"check": check, "status": "error", "detail": f"{code} 최신 {latest} — 거래일 {gap}일 지연", "value": gap})
    return results


async def check_system_events_error_rate(now: datetime | None = None, *, hours: float = 24.0) -> dict:
    """최근 24시간 system_events error 건수 — 서브시스템 전반의 조용한
    실패 누적을 잡는다. data_quality 자신의 이벤트는 자기증폭을 막기 위해
    제외한다."""
    check = "system_events_error_rate"
    now = now or datetime.now()
    since = (now - timedelta(hours=hours)).isoformat(timespec="seconds")
    summary = await system_events_repo.summarize_system_events(since)
    err_by_source = {
        src: levels.get("error", 0)
        for src, levels in summary.items()
        if src != SOURCE and levels.get("error")
    }
    total = sum(err_by_source.values())
    if total == 0:
        return {"check": check, "status": "ok", "detail": f"최근 {hours:g}시간 error 이벤트 없음", "value": 0}
    top = ", ".join(f"{src} {n}건" for src, n in sorted(err_by_source.items(), key=lambda kv: -kv[1])[:3])
    status = "error" if total >= _EVENT_ERROR_ESCALATE else "warn"
    return {"check": check, "status": status, "detail": f"최근 {hours:g}시간 error {total}건 ({top})", "value": total}


# ---------------------------------------------------------------------------
# 실행 + 기록
# ---------------------------------------------------------------------------

async def run_all_checks(*, now: datetime | None = None, record: bool = True) -> dict:
    """모든 정기 점검을 실행하고 결과를 system_events 에 기록한다.

    반환: {"counts": {"ok": n, "warn": n, "error": n}, "results": [...]}.
    record=False 는 테스트/드라이런용 — 이벤트를 남기지 않는다.
    """
    now = now or datetime.now()

    async def _safe(name: str, coro) -> list[dict]:
        # 개별 점검의 예외는 error 결과로 변환 — 점검 하나가 죽어도
        # 나머지 점검과 요약 기록은 계속된다.
        try:
            out = await coro
        except Exception as exc:
            logger.exception("data_quality: %s 점검 실행 실패", name)
            return [{"check": name, "status": "error", "detail": f"점검 실행 실패: {exc}", "value": None}]
        return out if isinstance(out, list) else [out]

    results: list[dict] = []
    results += await _safe("nav_snapshot_freshness", check_nav_snapshot_freshness(now=now))
    results += await _safe("portfolio_stock_snapshot_freshness", check_portfolio_stock_snapshot_freshness(now=now))
    results += await _safe("intraday_points", check_intraday_points(now=now))
    results += await _safe("benchmark_freshness", check_benchmark_freshness(now=now))
    results += await _safe("system_events_error_rate", check_system_events_error_rate(now=now))

    counts = {"ok": 0, "warn": 0, "error": 0}
    for r in results:
        counts[r["status"] if r.get("status") in counts else "error"] += 1

    if record:
        for r in results:
            if r["status"] == "ok":
                continue
            await observability.record_event(
                SOURCE,
                r["check"],
                level="error" if r["status"] == "error" else "warning",
                details={"detail": r.get("detail"), "value": r.get("value")},
                wait=True,
            )
        # 요약 이벤트 하나에 전체 결과를 담는다 — 관리자 패널의 유일한 읽기 지점.
        summary_level = "error" if counts["error"] else ("warning" if counts["warn"] else "info")
        await observability.record_event(
            SOURCE,
            SUMMARY_KIND,
            level=summary_level,
            details={"counts": counts, "results": results, "checked_at": now.isoformat(timespec="seconds")},
            wait=True,
        )

    return {"counts": counts, "results": results}


# ---------------------------------------------------------------------------
# 종목별 시계열 이상치 점검 — scripts/check_data_quality.py 에서 이전.
# yfinance 의존이라 정기 점검에는 포함하지 않는다.
# ---------------------------------------------------------------------------

def detect_series_anomalies(
    stock_code: str,
    corp_name: str,
    market_data: list[dict],
    raw_dividends_by_year: dict[int, float],
    adjusted_dividends_by_year: dict[int, float],
    split_events: list[tuple],
    max_dividend_yield: float = DEFAULT_MAX_DIVIDEND_YIELD,
    max_dividend_jump: float = DEFAULT_MAX_DIVIDEND_JUMP,
) -> list[dict]:
    """배당수익률 outlier / 전년 대비 배당 점프 / split 보정 / 누락 종가 탐지."""
    findings: list[dict] = []
    split_year_pairs = set()
    for split_ts, _ in split_events:
        split_year_pairs.add((split_ts.year - 1, split_ts.year))
        split_year_pairs.add((split_ts.year, split_ts.year + 1))

    for row in market_data:
        year = row["year"]
        dividend_yield = row.get("dividend_yield")
        if dividend_yield is not None and dividend_yield > max_dividend_yield:
            findings.append({
                "severity": "high",
                "type": "dividend_yield_outlier",
                "year": year,
                "message": f"{year} 배당수익률 {dividend_yield}%가 임계치 {max_dividend_yield}%를 초과합니다.",
            })

    prev_row = None
    for row in market_data:
        dps = row.get("dividend_per_share")
        if prev_row and dps is not None and prev_row.get("dividend_per_share") not in (None, 0):
            prev_dps = prev_row["dividend_per_share"]
            ratio = dps / prev_dps
            year_pair = (prev_row["year"], row["year"])
            if year_pair in split_year_pairs:
                prev_row = row
                continue
            if ratio >= max_dividend_jump or ratio <= (1 / max_dividend_jump):
                findings.append({
                    "severity": "medium",
                    "type": "dividend_jump",
                    "year": row["year"],
                    "message": (
                        f"{prev_row['year']} -> {row['year']} 주당배당금이 "
                        f"{prev_dps:.2f}원 -> {dps:.2f}원으로 크게 변했습니다."
                    ),
                })
        prev_row = row

    if split_events:
        split_summaries = [f"{ts.date()} x{ratio:g}" for ts, ratio in split_events]
        for year, raw_value in sorted(raw_dividends_by_year.items()):
            adjusted_value = adjusted_dividends_by_year.get(year)
            if raw_value is None or adjusted_value is None or raw_value == 0:
                continue
            diff_ratio = abs(adjusted_value - raw_value) / abs(raw_value)
            if diff_ratio >= 0.5:
                findings.append({
                    "severity": "info",
                    "type": "dividend_adjusted_for_split",
                    "year": year,
                    "message": (
                        f"{year} raw 주당배당금 {raw_value:.2f}원을 "
                        f"split/감자 이력({', '.join(split_summaries)}) 기준으로 {adjusted_value:.2f}원으로 보정했습니다."
                    ),
                })

    for row in market_data:
        if row.get("close_price") is None:
            findings.append({
                "severity": "low",
                "type": "missing_close_price",
                "year": row["year"],
                "message": f"{row['year']} 종가 데이터가 비어 있습니다.",
            })

    return findings


async def inspect_stock(
    stock_code: str,
    *,
    start_year: int = 2000,
    end_year: int | None = None,
    max_dividend_yield: float = DEFAULT_MAX_DIVIDEND_YIELD,
    max_dividend_jump: float = DEFAULT_MAX_DIVIDEND_JUMP,
) -> dict:
    """한 종목의 주가/배당 시계열을 점검해 findings 목록을 반환한다.

    cache/stock_price(yfinance·pandas) 는 함수 안에서 lazy import —
    웹 프로세스가 이 모듈을 임포트할 때 무거운 의존성을 끌고 오지 않게.
    """
    import asyncio

    import cache
    from repositories import financial as financial_repo
    import stock_price

    corp_name = await cache.get_corp_name(stock_code) or stock_code
    corp_code = await cache.get_corp_code(stock_code)
    if end_year is None:
        end_year = datetime.now().year
    financial_data = await financial_repo.get_financial_data(stock_code)
    market_data = await stock_price.fetch_market_data(
        stock_code,
        financial_data,
        start_year=start_year,
        end_year=end_year,
        corp_code=corp_code,
    )

    loop = asyncio.get_event_loop()
    try:
        _, _, raw_dividends, raw_splits = await loop.run_in_executor(
            None,
            stock_price._get_yfinance_aux,
            stock_code,
            start_year,
            end_year,
        )
    except Exception:
        raw_dividends = None
        raw_splits = None

    split_events = stock_price._normalized_split_events(raw_splits)
    adjusted_dividends = stock_price._adjust_dividends_for_splits(raw_dividends, split_events)
    raw_dividends_by_year = stock_price._group_sum_by_year_series(raw_dividends)
    adjusted_dividends_by_year = stock_price._group_sum_by_year_series(adjusted_dividends)

    findings = detect_series_anomalies(
        stock_code,
        corp_name,
        market_data,
        raw_dividends_by_year,
        adjusted_dividends_by_year,
        split_events,
        max_dividend_yield,
        max_dividend_jump,
    )

    return {
        "stock_code": stock_code,
        "corp_name": corp_name,
        "years": [row["year"] for row in market_data],
        "findings": findings,
        "split_events": [{"date": ts.strftime("%Y-%m-%d"), "ratio": ratio} for ts, ratio in split_events],
    }


__all__: list[str] = [
    "DEFAULT_MAX_DIVIDEND_JUMP",
    "DEFAULT_MAX_DIVIDEND_YIELD",
    "SOURCE",
    "SUMMARY_KIND",
    "check_benchmark_freshness",
    "check_intraday_points",
    "check_nav_snapshot_freshness",
    "check_portfolio_stock_snapshot_freshness",
    "check_system_events_error_rate",
    "detect_series_anomalies",
    "inspect_stock",
    "last_expected_trading_day",
    "run_all_checks",
    "trading_day_gap",
]
