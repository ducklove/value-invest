"""유령 포트폴리오("가만히 있었다면") + 매도 후 추적 서비스.

정산 스냅샷·입출금·NH 실현손익·수동 매매 기록을 읽고 일봉 종가와 배당·분할 일정을
결합해 `GET /api/portfolio/ghost` 응답을 만든다. LLM을 호출하지 않는다.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Any

import aiosqlite
import httpx

from core.cache import MemoryTTLCache, cached_fetch
from core.errors import AppError
from domain import ghost_portfolio as rules
from domain.portfolio_codes import (
    is_cash_asset,
    is_korean_stock,
    is_special_asset,
    normalize_portfolio_code,
    yahoo_symbol,
)
from domain.timeutil import KST
from repositories import corp_codes, portfolio, snapshots
from repositories import db as db_repo
from repositories import realized_pnl as realized_pnl_repo
from repositories.ticker_map import load_ticker_map
from services import dividend_sources, stock_price
from services.market.sources import close_price as close_price_client
from services.market.sources import kis_proxy as kis_proxy_client
from services.market.sources import yahoo
from services.portfolio import identifiers, nav_link
from services.portfolio.currencies import infer_yf_currency
from services.portfolio.time_windows import today_kst_date

logger = logging.getLogger(__name__)

GHOST_CACHE_TTL = 300.0
PRICE_SERIES_CACHE_TTL = 900.0
CORP_EVENT_CACHE_TTL = 900.0
UPSTREAM_TIMEOUT_SECONDS = 8.0
DEFAULT_LOOKBACK_CACHE_DAYS = 420

_ghost_cache = MemoryTTLCache("portfolio.ghost", GHOST_CACHE_TTL, evict_expired_after=1800)
_price_cache = MemoryTTLCache("portfolio.ghost.prices", PRICE_SERIES_CACHE_TTL, evict_expired_after=3600)
_corp_cache = MemoryTTLCache("portfolio.ghost.corp_events", CORP_EVENT_CACHE_TTL, evict_expired_after=3600)

_FETCH_ERRORS = (
    AppError,
    httpx.HTTPError,
    aiosqlite.Error,
    OSError,
    TimeoutError,
    ValueError,
    KeyError,
    TypeError,
)


def reset_cache() -> None:
    """테스트 및 수동 초기화용 인메모리 캐시 비우기."""
    _ghost_cache.clear()
    _price_cache.clear()
    _corp_cache.clear()


def _parse_since_param(since: str | None, today: date) -> date | None:
    if since is None:
        return None
    text = str(since).strip()
    if not text:
        return None
    if len(text) != 10:
        raise ValueError("기준일(since)은 YYYY-MM-DD 형식이어야 합니다.")
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError("기준일(since)은 유효한 YYYY-MM-DD 날짜여야 합니다.") from exc
    if parsed > today:
        raise ValueError("미래 날짜는 기준일로 지정할 수 없습니다.")
    if (today - parsed).days > 3660:
        raise ValueError("기준일은 최근 10년 이내로 지정해 주세요.")
    return parsed


async def _state_signature(user: str, since_key: str, today: date) -> str:
    """새 정산·매매·입출금이 들어오면 캐시 키가 즉시 바뀌도록 경량 서명을 만든다."""
    db = await db_repo.get_db()
    snap = await (
        await db.execute(
            "SELECT COUNT(*) AS n, MAX(date) AS last_d, COALESCE(SUM(total_value), 0) AS tv "
            "FROM portfolio_snapshots WHERE google_sub = ?",
            (user,),
        )
    ).fetchone()
    cf = await (
        await db.execute(
            "SELECT COUNT(*) AS n, MAX(id) AS max_id, COUNT(applied_snapshot_date) AS settled "
            "FROM portfolio_cashflows WHERE google_sub = ?",
            (user,),
        )
    ).fetchone()
    dist = await (
        await db.execute(
            "SELECT COUNT(*) AS n, MAX(id) AS max_id FROM portfolio_distributions WHERE google_sub = ?",
            (user,),
        )
    ).fetchone()
    tr = await (
        await db.execute(
            "SELECT COUNT(*) AS n, MAX(id) AS max_id FROM portfolio_trades WHERE google_sub = ?",
            (user,),
        )
    ).fetchone()
    pnl = await (
        await db.execute(
            "SELECT COUNT(*) AS n, MAX(created_at) AS last_c FROM broker_realized_pnl WHERE google_sub = ?",
            (user,),
        )
    ).fetchone()
    return (
        f"{db_repo.DB_PATH}:{user}:{since_key}:{today.isoformat()}:"
        f"s={snap['n']}:{snap['last_d']}:{round(float(snap['tv'] or 0), 0)}:"
        f"cf={cf['n']}:{cf['max_id']}:{cf['settled']}:"
        f"d={dist['n']}:{dist['max_id']}:"
        f"t={tr['n']}:{tr['max_id']}:"
        f"p={pnl['n']}:{pnl['last_c']}"
    )


async def _load_all_manual_trades(user: str) -> list[dict[str, Any]]:
    """수동 매매 기록을 읽고 체결일 명시 여부(`date_is_recorded_at`)를 구분한다."""
    db = await db_repo.get_db()
    rows = await (
        await db.execute(
            "SELECT result_json, created_at FROM portfolio_trades WHERE google_sub = ? ORDER BY id ASC",
            (user,),
        )
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        try:
            payload = json.loads(row["result_json"] or "{}")
        except (TypeError, ValueError):
            continue
        if not isinstance(payload, dict):
            continue
        explicit_date = str(payload.get("date") or payload.get("trade_date") or "").strip()[:10]
        date_is_recorded_at = False
        raw_date = explicit_date
        if not raw_date:
            date_is_recorded_at = True
            created = str(row["created_at"] or "").strip()
            if created:
                try:
                    dt = datetime.fromisoformat(created)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    raw_date = dt.astimezone(KST).date().isoformat()
                except ValueError:
                    raw_date = created[:10]
        out.append({**payload, "trade_date": raw_date, "date_is_recorded_at": date_is_recorded_at})
    return out


async def _load_stock_snapshots_range(user: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
    db = await db_repo.get_db()
    rows = await (
        await db.execute(
            """
            SELECT
                ps.date,
                ps.stock_code,
                COALESCE(up.stock_name, ps.stock_code) AS stock_name,
                ps.market_value,
                ps.quantity,
                ps.unit_price,
                ps.priced_from_fallback,
                ps.currency,
                ps.fx_rate
            FROM portfolio_stock_snapshots ps
            LEFT JOIN user_portfolio up
              ON up.google_sub = ps.google_sub
             AND up.stock_code = ps.stock_code
            WHERE ps.google_sub = ?
              AND ps.date >= ?
              AND ps.date <= ?
            ORDER BY ps.date ASC
            """,
            (user, start_date, end_date),
        )
    ).fetchall()
    return [dict(r) for r in rows]


async def _first_snapshot_with_quantities(user: str, from_date: str, to_date: str) -> str | None:
    db = await db_repo.get_db()
    row = await (
        await db.execute(
            """
            SELECT MIN(date) AS snap_date
            FROM portfolio_stock_snapshots
            WHERE google_sub = ?
              AND date >= ?
              AND date <= ?
              AND quantity IS NOT NULL
              AND quantity > 0
              AND stock_code NOT LIKE 'CASH_%'
              AND stock_code != 'CMA_RP_KRW'
            """,
            (user, from_date, to_date),
        )
    ).fetchone()
    return row["snap_date"] if row and row["snap_date"] else None


async def _resolve_stock_names(user: str, codes: set[str]) -> dict[str, str]:
    clean = sorted({normalize_portfolio_code(c) for c in codes if normalize_portfolio_code(c)})
    if not clean:
        return {}
    names: dict[str, str] = {}
    for code in clean:
        if code in identifiers.CASH_NAMES:
            names[code] = identifiers.CASH_NAMES[code]
        else:
            static = identifiers.static_foreign_ticker(code)
            if static and static.get("name"):
                names[code] = str(static["name"])
    special_names = {
        "KRX_GOLD": "KRX 금현물",
        "CMA_RP_KRW": "CMA 원화RP",
        "CRYPTO_BTC": "비트코인",
        "CRYPTO_ETH": "이더리움",
        "CRYPTO_USDT": "테더",
    }
    for code in clean:
        if code in special_names:
            names[code] = special_names[code]

    for item in await portfolio.get_portfolio(user):
        c = normalize_portfolio_code(item.get("stock_code"))
        if c and item.get("stock_name") and item["stock_name"] != c:
            names.setdefault(c, str(item["stock_name"]))

    snap_names = await snapshots.get_snapshot_stock_names(user, clean)
    for c, n in snap_names.items():
        nc = normalize_portfolio_code(c)
        if nc and n and n != nc:
            names.setdefault(nc, n)

    kr_missing = [c for c in clean if c not in names and is_korean_stock(c)]
    for code in kr_missing:
        resolved = await corp_codes.resolve_stock_name(code)
        if resolved:
            names[code] = resolved
    return names


def _yahoo_range_for(since: date, today: date) -> str:
    days = max(1, (today - since).days + 45)
    if days <= 700:
        return "2y"
    if days <= 1800:
        return "5y"
    return "10y"


def _slice_bars(rows: list[dict[str, Any]], since_iso: str, until_iso: str) -> list[dict[str, Any]]:
    return [r for r in rows if since_iso <= str(r.get("date") or "")[:10] <= until_iso]


async def _fetch_domestic_bars(codes: list[str], since: date, until: date) -> dict[str, list[dict[str, Any]]]:
    """국내 종목 일봉을 종목별 넉넉한 범위(최소 약 14개월)로 캐시하고 요청 구간으로 잘라 반환한다."""
    if not codes:
        return {}
    since_iso = since.isoformat()
    until_iso = until.isoformat()
    fetch_since = min(since, until - timedelta(days=DEFAULT_LOOKBACK_CACHE_DAYS))
    fetch_since_iso = fetch_since.isoformat()

    results: dict[str, list[dict[str, Any]]] = {}
    uncached: list[str] = []
    for code in codes:
        cached = _price_cache.get(f"KR:{until_iso}:{code}")
        if cached is not None and cached["covered_since"] <= since_iso:
            results[code] = _slice_bars(cached["rows"], since_iso, until_iso)
        else:
            uncached.append(code)
    if not uncached:
        return results

    fields = ("open", "high", "low", "close", "listed_shares")
    batch_rows: dict[str, list[dict[str, Any]]] = {}
    try:
        batch_rows = await asyncio.wait_for(
            close_price_client.get_daily_prices_batch(uncached, since=fetch_since, until=until, fields=fields),
            timeout=UPSTREAM_TIMEOUT_SECONDS,
        )
    except _FETCH_ERRORS as exc:
        logger.info("close_price 배치 조회 실패, 단건 조회로 폴백: %s", exc)

    still_missing: list[str] = []
    for code in uncached:
        rows = batch_rows.get(code) or []
        if rows:
            normalized = [{**r, "currency": "KRW"} for r in rows]
            _price_cache.set(f"KR:{until_iso}:{code}", {"covered_since": fetch_since_iso, "rows": normalized})
            results[code] = _slice_bars(normalized, since_iso, until_iso)
        else:
            still_missing.append(code)

    if not still_missing:
        return results

    sem = asyncio.Semaphore(3)

    async def fetch_one(code: str) -> tuple[str, list[dict[str, Any]]]:
        async with sem:
            try:
                rows = await asyncio.wait_for(
                    close_price_client.get_daily_prices(code, since=fetch_since, until=until, fields=fields),
                    timeout=4.0,
                )
                if rows:
                    return code, [{**r, "currency": "KRW"} for r in rows]
            except _FETCH_ERRORS:
                pass
            try:
                closes = await asyncio.wait_for(
                    close_price_client.get_daily_closes(code, since=fetch_since, until=until),
                    timeout=3.0,
                )
                if closes:
                    return code, [{**r, "currency": "KRW"} for r in closes]
            except _FETCH_ERRORS:
                pass
            try:
                payload = await asyncio.wait_for(
                    kis_proxy_client.get_history(code, start_date=fetch_since, end_date=until, period="D", adjusted=True),
                    timeout=6.0,
                )
                items = payload.get("items") if isinstance(payload, dict) else []
                parsed: list[dict[str, Any]] = []
                for item in stock_price._sorted_history_items(items or []):
                    d = stock_price._parse_date(
                        stock_price._get_first(item, "stck_bsop_date", "date", "trade_date", "business_date")
                    )
                    close = stock_price._safe_float(
                        stock_price._get_first(item, "stck_clpr", "close_price", "close"),
                        zero_as_none=False,
                    )
                    if not d or close is None or close <= 0:
                        continue
                    row_out: dict[str, Any] = {"date": d.isoformat(), "close": float(close), "currency": "KRW"}
                    for target_key, raw_keys in (
                        ("open", ("stck_oprc", "open", "open_price")),
                        ("high", ("stck_hgpr", "high", "high_price")),
                        ("low", ("stck_lwpr", "low", "low_price")),
                        ("listed_shares", ("lstn_stcn", "listed_shares")),
                    ):
                        val = stock_price._safe_float(stock_price._get_first(item, *raw_keys), zero_as_none=False)
                        if val is not None and val > 0:
                            row_out[target_key] = float(val)
                    parsed.append(row_out)
                return code, parsed
            except _FETCH_ERRORS:
                return code, []

    for code, rows in await asyncio.gather(*(fetch_one(c) for c in still_missing)):
        if rows:
            _price_cache.set(f"KR:{until_iso}:{code}", {"covered_since": fetch_since_iso, "rows": rows})
            results[code] = _slice_bars(rows, since_iso, until_iso)
        else:
            results[code] = []
    return results


async def _fetch_foreign_chart_and_events(
    codes: list[str],
    since: date,
    until: date,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """해외 종목 일봉(현지 통화)과 Yahoo 배당락·액면분할 일정을 함께 가져온다."""
    if not codes:
        return {}, {}
    ticker_map = await load_ticker_map()
    fetch_since = min(since, until - timedelta(days=DEFAULT_LOOKBACK_CACHE_DAYS))
    fetch_since_iso = (fetch_since - timedelta(days=15)).isoformat()
    slice_since_iso = (since - timedelta(days=15)).isoformat()
    until_iso = until.isoformat()
    range_ = _yahoo_range_for(fetch_since, until)

    bars_out: dict[str, dict[str, Any]] = {}
    events_out: dict[str, dict[str, Any]] = {}
    sem = asyncio.Semaphore(4)

    async def one(code: str) -> None:
        cache_key = f"YF:{until_iso}:{code}"
        cached = _price_cache.get(cache_key)
        if cached is not None and cached["covered_since"] <= slice_since_iso:
            bars_out[code] = {
                "rows": _slice_bars(cached["bars"]["rows"], slice_since_iso, until_iso),
                "currency": cached["bars"]["currency"],
            }
            events_out[code] = cached["events"]
            return
        static = identifiers.static_foreign_ticker(code) or {}
        mapped = ticker_map.get(code) or static.get("ticker") or code
        symbol = yahoo_symbol(mapped)
        async with sem:
            try:
                payload = await asyncio.wait_for(
                    yahoo.fetch_chart_json(
                        symbol, range_=range_, interval="1d", events="div,split", include_pre_post=False,
                    ),
                    timeout=UPSTREAM_TIMEOUT_SECONDS,
                )
            except _FETCH_ERRORS as exc:
                logger.info("해외 종목 Yahoo 차트 조회 실패 (%s/%s): %s", code, symbol, exc)
                bars_out[code] = {"rows": [], "currency": None}
                events_out[code] = {"verified": False, "ex_dates": set(), "split_dates": set()}
                return
        chart = yahoo.parse_chart(payload)
        if chart is None:
            bars_out[code] = {"rows": [], "currency": None}
            events_out[code] = {"verified": False, "ex_dates": set(), "split_dates": set()}
            return
        currency = chart.currency or infer_yf_currency(symbol)
        offset = chart.gmtoffset
        zone = timezone(timedelta(seconds=offset))
        rows: list[dict[str, Any]] = []
        for idx, ts in enumerate(chart.timestamps):
            close = chart.closes[idx] if idx < len(chart.closes) else None
            if close is None or close <= 0:
                continue
            try:
                d_iso = datetime.fromtimestamp(int(ts), tz=zone).date().isoformat()
            except (TypeError, ValueError, OverflowError, OSError):
                continue
            if d_iso < fetch_since_iso or d_iso > until_iso:
                continue
            bar: dict[str, Any] = {"date": d_iso, "close": round(float(close), 6), "currency": currency}
            op = chart.opens[idx] if idx < len(chart.opens) else None
            hi = chart.highs[idx] if idx < len(chart.highs) else None
            lo = chart.lows[idx] if idx < len(chart.lows) else None
            if op is not None and op > 0:
                bar["open"] = round(float(op), 6)
            if hi is not None and hi > 0:
                bar["high"] = round(float(hi), 6)
            if lo is not None and lo > 0:
                bar["low"] = round(float(lo), 6)
            rows.append(bar)
        dedup = {r["date"]: r for r in rows}
        sorted_rows = [dedup[k] for k in sorted(dedup)]

        ex_dates: set[str] = set()
        for div_row in chart.dividends:
            stamp = div_row.get("date")
            if stamp is not None:
                try:
                    ex_dates.add(datetime.fromtimestamp(int(stamp), tz=zone).date().isoformat())
                except (TypeError, ValueError, OverflowError, OSError):
                    pass

        split_dates: set[str] = set()
        raw_results = ((payload.get("chart") or {}).get("result") or [{}])
        raw_events = (raw_results[0].get("events") if raw_results and isinstance(raw_results[0], dict) else None) or {}
        raw_splits = raw_events.get("splits") if isinstance(raw_events, dict) else {}
        if isinstance(raw_splits, dict):
            for sp in raw_splits.values():
                if isinstance(sp, dict) and sp.get("date") is not None:
                    try:
                        split_dates.add(datetime.fromtimestamp(int(sp["date"]), tz=zone).date().isoformat())
                    except (TypeError, ValueError, OverflowError, OSError):
                        pass

        bar_pack = {"rows": sorted_rows, "currency": currency}
        ev_pack = {"verified": True, "ex_dates": ex_dates, "split_dates": split_dates}
        if sorted_rows:
            _price_cache.set(cache_key, {"covered_since": fetch_since_iso, "bars": bar_pack, "events": ev_pack})
        bars_out[code] = {"rows": _slice_bars(sorted_rows, slice_since_iso, until_iso), "currency": currency}
        events_out[code] = ev_pack

    await asyncio.gather(*(one(c) for c in codes))
    return bars_out, events_out


async def _fetch_fx_closes(currencies_needed: set[str], since: date, until: date) -> dict[str, list[tuple[str, float]]]:
    """외화 통화의 원화 환율 일봉(Yahoo XXXKRW=X)을 넓게 캐시하고 요청 구간으로 잘라 반환한다."""
    targets = sorted({c for c in currencies_needed if c and c != "KRW"})
    if not targets:
        return {}
    fetch_since = min(since, until - timedelta(days=DEFAULT_LOOKBACK_CACHE_DAYS))
    fetch_since_iso = (fetch_since - timedelta(days=15)).isoformat()
    slice_since_iso = (since - timedelta(days=15)).isoformat()
    until_iso = until.isoformat()
    range_ = _yahoo_range_for(fetch_since, until)

    out: dict[str, list[tuple[str, float]]] = {}
    for cur in targets:
        cache_key = f"FX:{until_iso}:{cur}"
        cached = _price_cache.get(cache_key)
        if cached is not None and cached["covered_since"] <= slice_since_iso:
            out[cur] = [(d, v) for d, v in cached["pairs"] if slice_since_iso <= d <= until_iso]
            continue
        series = await yahoo.fetch_close_series(f"{cur}KRW=X", range_=range_, interval="1d", session_date=False)
        pairs = [
            (str(r["date"])[:10], float(r["close"]))
            for r in (series.get("rows") or [])
            if r.get("date") and (r.get("close") or 0) > 0 and fetch_since_iso <= str(r["date"])[:10] <= until_iso
        ]
        pairs.sort(key=lambda p: p[0])
        if pairs:
            _price_cache.set(cache_key, {"covered_since": fetch_since_iso, "pairs": pairs})
        out[cur] = [(d, v) for d, v in pairs if slice_since_iso <= d <= until_iso]
    return out


async def _fetch_domestic_corporate_events(
    codes: list[str],
    since: date,
    today: date,
) -> dict[str, dict[str, Any]]:
    """국내 매도 종목의 배당락일·분할 일정을 확인한다."""
    if not codes:
        return {}
    results: dict[str, dict[str, Any]] = {}
    uncached: list[str] = []
    for code in codes:
        cached = _corp_cache.get(f"{today.isoformat()}:{code}")
        if cached is not None:
            results[code] = cached
        else:
            uncached.append(code)
    if not uncached:
        return results

    histories = await dividend_sources.get_histories(uncached, timeout=UPSTREAM_TIMEOUT_SECONDS)

    for code in uncached:
        feed = histories.get(code) or {}
        verified = feed.get("status") == "fresh"
        raw_events = list(feed.get("events") or [])

        if not verified:
            try:
                payload = await asyncio.wait_for(
                    kis_proxy_client.get_dividends(
                        code, start_date=since - timedelta(days=30), end_date=today + timedelta(days=30),
                    ),
                    timeout=4.0,
                )
                raw_events = dividend_sources.parse_kis_dividends(payload, code)
                verified = True
            except _FETCH_ERRORS:
                verified = False

        ex_dates: set[str] = set()
        for ev in raw_events:
            if ev.get("estimated"):
                continue
            if ev.get("ex_date"):
                ex_dates.add(str(ev["ex_date"])[:10])
            elif ev.get("record_date"):
                derived = rules.krx_ex_dividend_date_from_record(str(ev["record_date"])[:10])
                if derived:
                    ex_dates.add(derived)

        entry = {"verified": verified, "ex_dates": ex_dates, "split_dates": set()}
        results[code] = entry
        if verified:
            _corp_cache.set(f"{today.isoformat()}:{code}", entry)
    return results


def _select_anchor_snapshot(
    nav_rows: list[dict[str, Any]],
    requested_since: date | None,
    today: date,
) -> tuple[dict[str, Any] | None, str, str]:
    """(기준 스냅샷, baseline_mode, effective_since_iso)."""
    if requested_since is not None:
        since_iso = requested_since.isoformat()
        on_or_before = [r for r in nav_rows if str(r["date"])[:10] <= since_iso]
        if on_or_before:
            chosen = on_or_before[-1]
            mode = "exact_since" if str(chosen["date"])[:10] == since_iso else "on_or_before_since"
            return chosen, mode, since_iso
        after = [r for r in nav_rows if str(r["date"])[:10] >= since_iso]
        if after:
            return after[0], "first_after_since", since_iso
        return None, "unavailable", since_iso

    year_start_iso = f"{today.year}-01-01"
    prev_year_end = f"{today.year - 1}-12-31"
    prev_rows = [r for r in nav_rows if str(r["date"])[:10] <= prev_year_end]
    if prev_rows:
        return prev_rows[-1], "year_start", year_start_iso
    curr_year_rows = [r for r in nav_rows if str(r["date"])[:10] >= year_start_iso]
    if curr_year_rows:
        return curr_year_rows[0], "first_in_year", year_start_iso
    return (nav_rows[0], "earliest_snapshot", str(nav_rows[0]["date"])[:10]) if nav_rows else (None, "unavailable", year_start_iso)


def _collect_krw_trade_flows_by_code(
    nh_rows: list[dict[str, Any]],
    manual_trades: list[dict[str, Any]],
) -> dict[str, list[tuple[str, float]]]:
    """미계산 종목이 기간 중 매매되어 현금으로 전환된 경우 원화 유입(+)/유출(-)을 추적한다."""
    by_code: dict[str, list[tuple[str, float]]] = {}
    nh_days: set[tuple[str, str]] = set()
    for row in nh_rows or []:
        issues = set(row.get("issues") or ())
        if "detail_pending" in issues or "detail_missing" in issues or "fx_implausible" in issues:
            continue
        code = normalize_portfolio_code(row.get("stock_code") or row.get("symbol"))
        t_date = str(row.get("trade_date") or "")[:10]
        sell_krw = row.get("sell_krw")
        if code and len(t_date) == 10 and sell_krw and float(sell_krw) > 0:
            by_code.setdefault(code, []).append((t_date, float(sell_krw)))
            nh_days.add((code, t_date))
    for tr in manual_trades or []:
        code = normalize_portfolio_code(tr.get("stock_code"))
        t_date = str(tr.get("trade_date") or "")[:10]
        if not code or len(t_date) != 10 or tr.get("currency") != "KRW":
            continue
        if tr.get("side") == "sell" and (code, t_date) in nh_days:
            continue
        cash_change = tr.get("cash_change")
        if cash_change is None and tr.get("gross_amount") and tr.get("side") in {"buy", "sell"}:
            cash_change = float(tr["gross_amount"]) * (1.0 if tr["side"] == "sell" else -1.0)
        if cash_change is not None and float(cash_change) != 0:
            by_code.setdefault(code, []).append((t_date, float(cash_change)))
    return by_code


async def _compute_ghost_payload(user: str, requested_since: date | None, today: date) -> dict[str, Any]:
    raw_nav = await nav_link.get_nav_history(user)
    today_iso = today.isoformat()
    nav_rows = [
        r for r in raw_nav
        if r.get("date")
        and str(r["date"])[:10] <= today_iso
        and r.get("total_value") is not None
        and float(r["total_value"]) > 0
    ]
    nav_rows.sort(key=lambda r: str(r["date"])[:10])

    anchor, baseline_mode, effective_since = _select_anchor_snapshot(nav_rows, requested_since, today)
    baseline_note: str | None = None

    if requested_since is None and anchor is not None and nav_rows:
        anchor_date = str(anchor["date"])[:10]
        anchor_stocks = await snapshots.get_stock_snapshots_exact_date(user, anchor_date)
        non_cash_anchor = [
            s for s in anchor_stocks
            if not is_cash_asset(s.get("stock_code")) and s.get("stock_code") != "CMA_RP_KRW"
        ]
        if non_cash_anchor and all(s.get("quantity") is None for s in non_cash_anchor):
            first_qty_date = await _first_snapshot_with_quantities(user, f"{today.year}-01-01", today_iso)
            if first_qty_date and first_qty_date > anchor_date:
                later = next((r for r in nav_rows if str(r["date"])[:10] == first_qty_date), None)
                if later is not None:
                    baseline_note = (
                        f"연초 정산({anchor_date})에 종목별 보유 수량 기록이 없어 "
                        f"수량이 처음 기록된 {first_qty_date} 정산을 기준일로 사용했습니다."
                    )
                    anchor = later
                    baseline_mode = "first_quantity_recorded"

    baseline_date = str(anchor["date"])[:10] if anchor else None
    selected_nav = [r for r in nav_rows if baseline_date and str(r["date"])[:10] >= baseline_date]
    ending_date = str(selected_nav[-1]["date"])[:10] if selected_nav else None

    stock_range_start = min(baseline_date, effective_since) if baseline_date else effective_since
    stock_rows_task = asyncio.create_task(
        _load_stock_snapshots_range(user, stock_range_start, ending_date or today_iso)
    )
    cashflows_task = asyncio.create_task(snapshots.get_cashflows(user))
    nh_rows_task = asyncio.create_task(realized_pnl_repo.rows(user))
    manual_trades_task = asyncio.create_task(_load_all_manual_trades(user))

    stock_rows, cashflows, nh_rows, manual_trades = await asyncio.gather(
        stock_rows_task, cashflows_task, nh_rows_task, manual_trades_task
    )

    all_codes: set[str] = set()
    for r in stock_rows:
        if r.get("stock_code"):
            all_codes.add(normalize_portfolio_code(r["stock_code"]))
    for r in nh_rows:
        if r.get("stock_code") or r.get("symbol"):
            all_codes.add(normalize_portfolio_code(r.get("stock_code") or r.get("symbol")))
    for r in manual_trades:
        if r.get("stock_code"):
            all_codes.add(normalize_portfolio_code(r["stock_code"]))

    stock_names = await _resolve_stock_names(user, all_codes)

    sale_snapshots = [r for r in stock_rows if not baseline_date or str(r["date"])[:10] >= baseline_date]
    sale_events = rules.collect_sale_events(
        nh_rows=nh_rows,
        manual_trades=manual_trades,
        stock_snapshots=sale_snapshots,
        effective_since=effective_since,
        stock_names=stock_names,
    )

    baseline_stocks = [r for r in stock_rows if baseline_date and str(r["date"])[:10] == baseline_date]
    snapshot_prices_by_date: dict[str, dict[str, dict[str, Any]]] = {}
    for r in stock_rows:
        d = str(r["date"])[:10]
        c = normalize_portfolio_code(r.get("stock_code"))
        if not d or not c:
            continue
        day_bucket = snapshot_prices_by_date.setdefault(d, {})
        existing = day_bucket.get(c)
        if existing is None:
            day_bucket[c] = dict(r)
        else:
            existing["market_value"] = float(existing.get("market_value") or 0.0) + float(r.get("market_value") or 0.0)
            q_a, q_b = existing.get("quantity"), r.get("quantity")
            existing["quantity"] = None if (q_a is None or q_b is None) else (float(q_a) + float(q_b))
            existing["priced_from_fallback"] = bool(existing.get("priced_from_fallback") or r.get("priced_from_fallback"))
            if existing["quantity"] and existing["quantity"] != 0:
                existing["unit_price"] = existing["market_value"] / existing["quantity"]

    needed_kr_codes: set[str] = set()
    needed_foreign_codes: set[str] = set()
    baseline_foreign_cash_currencies: set[str] = set()
    for row in baseline_stocks:
        c = normalize_portfolio_code(row.get("stock_code"))
        if not c:
            continue
        if is_cash_asset(c):
            cur = c.removeprefix("CASH_")
            if cur and cur != "KRW":
                baseline_foreign_cash_currencies.add(cur)
            continue
        if is_special_asset(c):
            continue
        if is_korean_stock(c):
            needed_kr_codes.add(c)
        else:
            needed_foreign_codes.add(c)

    sold_kr_codes: set[str] = set()
    sold_foreign_codes: set[str] = set()
    for ev in sale_events:
        c = normalize_portfolio_code(ev.get("stock_code"))
        if not c or ev.get("status") == "uncalculated" or is_cash_asset(c) or is_special_asset(c):
            continue
        if is_korean_stock(c):
            needed_kr_codes.add(c)
            sold_kr_codes.add(c)
        else:
            needed_foreign_codes.add(c)
            sold_foreign_codes.add(c)

    earliest_needed_iso = min(
        [d for d in (baseline_date, effective_since, *(e["trade_date"] for e in sale_events if e.get("trade_date"))) if d],
        default=today_iso,
    )
    price_since = max(today - timedelta(days=3700), date.fromisoformat(earliest_needed_iso) - timedelta(days=14))

    kr_bars_task = asyncio.create_task(_fetch_domestic_bars(sorted(needed_kr_codes), price_since, today))
    foreign_task = asyncio.create_task(_fetch_foreign_chart_and_events(sorted(needed_foreign_codes), price_since, today))
    kr_corp_task = asyncio.create_task(_fetch_domestic_corporate_events(sorted(sold_kr_codes), price_since, today))

    kr_bars, (foreign_packs, foreign_corp_events), kr_corp_events = await asyncio.gather(
        kr_bars_task, foreign_task, kr_corp_task
    )

    external_krw_closes: dict[str, list[tuple[str, float]]] = {}
    split_flags: dict[str, str] = {}

    if baseline_date and ending_date:
        # 1) 스냅샷 수량 급변(거래 없이 수량 점프) 감지
        split_flags.update(
            rules.detect_snapshot_quantity_split_flags(
                stock_rows=stock_rows,
                manual_trades=manual_trades,
                nh_rows=nh_rows,
                cashflows=cashflows,
                baseline_date=baseline_date,
                ending_date=ending_date,
            )
        )

        # 2) 국내 기준일 보유 종목: 일봉 누락으로 분할 여부를 확인 못 한 종목도 미계산으로 표시
        baseline_kr_codes = {
            normalize_portfolio_code(r.get("stock_code"))
            for r in baseline_stocks
            if is_korean_stock(r.get("stock_code"))
        }
        for code in sorted(needed_kr_codes | baseline_kr_codes):
            bars = kr_bars.get(code) or []
            pairs = [(str(b["date"])[:10], float(b["close"])) for b in bars if b.get("date") and (b.get("close") or 0) > 0]
            pairs.sort(key=lambda p: p[0])
            if pairs:
                external_krw_closes[code] = pairs
            if code in baseline_kr_codes:
                base_row = (snapshot_prices_by_date.get(baseline_date) or {}).get(code)
                base_unit = float(base_row["unit_price"]) if base_row and base_row.get("unit_price") else None
                split_reason = rules.detect_krx_series_split_or_mismatch(base_unit, baseline_date, ending_date, bars)
                if split_reason:
                    split_flags[code] = split_reason

        # 3) 외화 환율 시계열 (해외 주식 통화 + 외화 예수금 통화)
        foreign_stock_currencies = {
            str(pack.get("currency") or "").upper()
            for pack in foreign_packs.values()
            if pack.get("rows") and pack.get("currency")
        }
        fx_needed = (foreign_stock_currencies | baseline_foreign_cash_currencies) - {"KRW"}
        if needed_foreign_codes:
            fx_needed.add("USD")
        fx_closes = await _fetch_fx_closes(fx_needed, price_since, today)
        snap_usd_pairs = [
            (str(pt["date"])[:10], float(pt["fx_usdkrw"]))
            for pt in selected_nav
            if pt.get("fx_usdkrw") and float(pt["fx_usdkrw"]) > 0
        ]
        usd_series = sorted({d: r for d, r in [*(fx_closes.get("USD") or []), *snap_usd_pairs]}.items())

        for cur in baseline_foreign_cash_currencies:
            cash_fx = usd_series if cur == "USD" else (fx_closes.get(cur) or [])
            if cash_fx:
                external_krw_closes[f"CASH_{cur}"] = cash_fx

        # 4) 해외 기준일 보유 종목: Yahoo 차트 실패로 분할 여부를 확인 못 한 종목은 미계산("분할 여부 확인 전")
        baseline_foreign_codes = {
            normalize_portfolio_code(r.get("stock_code"))
            for r in baseline_stocks
            if r.get("stock_code")
            and not is_korean_stock(r.get("stock_code"))
            and not is_cash_asset(r.get("stock_code"))
            and not is_special_asset(r.get("stock_code"))
        }
        for code in sorted(baseline_foreign_codes | set(foreign_packs)):
            pack = foreign_packs.get(code) or {}
            f_ev = foreign_corp_events.get(code) or {}
            if code in baseline_foreign_codes:
                if not f_ev.get("verified") or not pack.get("rows"):
                    split_flags.setdefault(code, "분할 여부 확인 전 (해외 차트 미확보)")
                    continue
                if any(baseline_date < s_d <= ending_date for s_d in (f_ev.get("split_dates") or ())):
                    split_flags[code] = "액면분할·병합 구간"
                    continue
            cur = str(pack.get("currency") or "").upper()
            fx_list = usd_series if cur == "USD" else (fx_closes.get(cur) or [])
            if not fx_list and cur != "KRW":
                continue
            krw_pairs: list[tuple[str, float]] = []
            for r in pack.get("rows") or []:
                d = str(r["date"])[:10]
                native_close = float(r["close"])
                if cur == "KRW":
                    krw_pairs.append((d, native_close))
                else:
                    rate = rules.lookup_close_on_or_before(fx_list, d)
                    if rate and rate > 0:
                        krw_pairs.append((d, native_close * rate))
            if krw_pairs:
                external_krw_closes[code] = krw_pairs

    if selected_nav and len(selected_nav) >= 2:
        ghost_result = rules.compute_ghost_portfolio(
            nav_points=selected_nav,
            baseline_stocks=baseline_stocks,
            snapshot_prices_by_date=snapshot_prices_by_date,
            external_krw_closes=external_krw_closes,
            split_flags=split_flags,
            cashflows=cashflows,
            stock_names=stock_names,
            uncalc_krw_trade_flows=_collect_krw_trade_flows_by_code(nh_rows, manual_trades),
        )
    else:
        ghost_result = {
            "available": False,
            "scope": "unavailable",
            "reason": "선택한 기준일 이후 정산 스냅샷이 2개 이상 쌓이면 방치 평가액을 비교합니다.",
            "assumptions": list(rules.GHOST_ASSUMPTIONS),
        }

    combined_bars: dict[str, list[dict[str, Any]]] = dict(kr_bars)
    for code, pack in foreign_packs.items():
        if code in sold_foreign_codes and pack.get("rows"):
            combined_bars[code] = pack["rows"]

    combined_corp: dict[str, dict[str, Any]] = {**foreign_corp_events, **kr_corp_events}
    post_sale_result = rules.evaluate_post_sales(
        sale_events=sale_events,
        daily_bars=combined_bars,
        corporate_events=combined_corp,
        since=effective_since,
    )

    has_any = bool(ghost_result.get("available") or post_sale_result["summary"]["total_count"] > 0)
    return {
        "available": has_any,
        "message": None if has_any else "일별 정산 스냅샷이나 매도 내역이 쌓이면 입력 없이 자동으로 계산합니다.",
        "as_of": today_iso,
        "requested_since": requested_since.isoformat() if requested_since else None,
        "effective_since": effective_since,
        "baseline_date": baseline_date,
        "baseline_mode": baseline_mode,
        "baseline_note": baseline_note,
        "ending_date": ending_date,
        "ghost": ghost_result,
        "post_sale": post_sale_result,
    }


async def build_ghost_report(
    user: str,
    *,
    since: str | None = None,
    today: date | None = None,
) -> dict[str, Any]:
    """`GET /api/portfolio/ghost` 진입점 — 상태 서명 기반 인메모리 캐시 적용."""
    anchor_today = today or today_kst_date()
    parsed_since = _parse_since_param(since, anchor_today)
    since_key = parsed_since.isoformat() if parsed_since else "default"
    sig = await _state_signature(user, since_key, anchor_today)
    return await cached_fetch(
        _ghost_cache,
        sig,
        lambda: _compute_ghost_payload(user, parsed_since, anchor_today),
        errors=_FETCH_ERRORS,
    )
