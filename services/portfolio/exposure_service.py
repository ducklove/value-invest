"""포트폴리오 X-Ray 조립 — 잔고·시세·형제 데이터를 모아 순수 계산에 넘긴다.

계산은 services.portfolio.exposure, 자료 수집은 services.portfolio.exposure_data 몫이다.
보유 수량은 선택한 계좌 기준(``account_id``)이고, 없으면 전체 계좌 합산이다.
결과는 사용자·계좌별로 10분 캐시한다(시세는 그 안에서 고정된다).
일봉은 국내가 finance-pi, 해외가 Yahoo(원화 환산)다. 한쪽이 실패해도 다른 쪽으로 계산한다.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from cache_layer import MemoryTTLCache, cached_fetch, now_iso
from domain.portfolio_codes import normalize_portfolio_code
from domain.timeutil import now_kst
from repositories import portfolio as portfolio_repo
from repositories import snapshots as snapshots_repo
from services import stock_quotes
from services.market.sources import close_price
from services.portfolio import exposure, exposure_data, foreign, futures_quotes, quote_service, risk
from services.portfolio.identifiers import static_foreign_ticker
from services.portfolio.time_windows import today_kst_date

logger = logging.getLogger(__name__)

REPORT_TTL_SECONDS = 600
# 시세 조회가 멈춰도 카드가 끝없이 기다리지 않게 하는 전체 상한.
QUOTE_BUDGET_SECONDS = 45.0

# 자료별로 낡았다고 볼 기준일수. eiayn 은 일배치, 지주사 지분은 분기 공시 주기다.
_MAX_AGE_DAYS = {
    "eiayn": 3, "holding_value": 120, "common_preferred_spread": 7, "kind": 45, "finance_pi": 5, "yahoo": 5,
}

_report_cache = MemoryTTLCache("portfolio.exposure.report", REPORT_TTL_SECONDS, evict_expired_after=0)


def _safe_float(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


async def _quotes_for(codes: list[str]) -> dict[str, dict]:
    """전체 상한 안에서 시세를 채운다. 넘기면 캐시(낡은 값 포함)로 대신한다."""
    if not codes:
        return {}
    try:
        return await asyncio.wait_for(quote_service.complete_quotes(codes), timeout=QUOTE_BUDGET_SECONDS)
    except TimeoutError:
        logger.warning("X-Ray quotes exceeded %.0fs budget; using cached quotes", QUOTE_BUDGET_SECONDS)
    fallback: dict[str, dict] = {}
    for code in codes:
        quote = stock_quotes.stock_to_quote(stock_quotes.get_stock_cached(code, allow_stale=True))
        if quote:
            quote["_stale"] = True
            fallback[code] = quote
    return fallback


async def _snapshot_unit_prices(google_sub: str) -> dict[str, tuple[float, str]]:
    """가장 최근 정산 스냅샷의 {코드: (주당 평가액, 날짜)} — 시세를 못 받은 종목의 대체값.

    스냅샷은 사용자 합산 행이라 계좌 수량과 곱할 수 있도록 주당 값으로 돌려준다.
    """
    latest = await snapshots_repo.get_latest_snapshot(google_sub)
    snap_date = latest.get("date") if latest else None
    if not snap_date:
        return {}
    prices: dict[str, tuple[float, str]] = {}
    for row in await snapshots_repo.get_stock_snapshots_exact_date(google_sub, snap_date):
        price = _safe_float(row.get("unit_price"))
        if price is None:
            value = _safe_float(row.get("market_value"))
            quantity = _safe_float(row.get("quantity"))
            price = value / quantity if value is not None and quantity else None
        if price is not None:
            prices[row["stock_code"]] = (price, snap_date)
    return prices


def _build_positions(
    items: list[dict],
    quotes: dict[str, dict],
    snapshot_prices: dict[str, tuple[float, str]],
) -> list[dict]:
    positions: list[dict] = []
    for item in items:
        code = item["stock_code"]
        quote = quotes.get(code) or {}
        price = _safe_float(quote.get("price"))
        stale = bool(quote.get("_stale"))
        if (price is None or stale) and code in snapshot_prices:
            price = snapshot_prices[code][0]
            stale = True
        quantity = _safe_float(item.get("quantity")) or 0.0
        positions.append({
            "code": code,
            "name": item.get("stock_name") or code,
            "quantity": quantity,
            "value": quantity * price if price is not None else None,
            "currency": item.get("currency") or "KRW",
            "underlying_code": quote.get("underlying_code"),
            "pair_long_code": item.get("pair_long_code"),
            "stale": stale,
        })
    return positions


async def _load_closes(codes: list[str]) -> tuple[dict[str, list[dict]] | None, str | None]:
    """finance-pi 최근 1년 일봉. 실패하면 (None, None) — 값은 0 이 아니라 null 로 남는다."""
    if not codes:
        return {}, None
    today = today_kst_date()
    since = risk.window_boundary("1Y", today)
    try:
        rows = await close_price.get_daily_prices_batch(codes, since=since, until=today, fields=("close",))
    except close_price.ClosePriceClientError as exc:
        logger.warning("X-Ray daily closes failed: %s", exc)
        return None, None
    if not rows:  # 비활성·쿨다운이면 빈 dict 가 온다.
        return None, None
    latest = max((series[-1]["date"] for series in rows.values() if series), default=None)
    return rows, latest


async def _yahoo_symbols(codes: list[str]) -> dict[str, str]:
    """보유 코드 → Yahoo 심볼. 금·가상자산은 대용 시리즈, 해외 종목은 해외 시세와 같은 순서(고정 표 → 티커 맵 → 코드 그대로)."""
    if not codes:
        return {}
    await foreign.ensure_ticker_map()
    symbols: dict[str, str] = {}
    for code in codes:
        if code in exposure.SPECIAL_SERIES_PROXIES:  # 금·가상자산은 Yahoo 대용 시리즈
            symbols[code] = exposure.SPECIAL_SERIES_PROXIES[code]
            continue
        static = static_foreign_ticker(code)
        ticker = static["ticker"] if static else (foreign._ticker_map.get(code) or code)
        symbols[code] = foreign.yfinance_direct_ticker(ticker)
    return symbols


async def _load_foreign_closes(positions: list[dict]) -> tuple[dict[str, list[dict]], dict | None]:
    """해외 보유 종목의 Yahoo 일봉 → 원화·국내 거래일 기준 종가. 해외 종목이 없으면 (빈 dict, None).

    환율은 보유 통화로 먼저 함께 받고, 시세 제공자가 다른 통화를 알려 주면 그 환율을 더 받는다.
    통화를 모르거나 환율을 못 받은 종목은 빠진다(원화가 아닌 수익률을 섞지 않는다).
    """
    codes = exposure.foreign_series_codes(positions)
    if not codes:
        return {}, None
    symbols = await _yahoo_symbols(codes)
    currency_of = {normalize_portfolio_code(p.get("code")): p.get("currency") for p in positions}
    # 대용 시리즈는 달러 표시다(보유 통화는 원화로 적혀 있다). 환율을 첫 요청에 함께 받는다.
    currency_of.update({code: "USD" for code in symbols if code in exposure.SPECIAL_SERIES_PROXIES})
    held_fx = {fx for code in symbols for fx in exposure.fx_symbols(currency_of.get(code))}
    wanted = [*symbols.values(), *sorted(held_fx)]
    loaded = await exposure_data.load_daily_series(wanted)
    extra = {fx for series in loaded.values() for fx in exposure.fx_symbols(series.get("currency"))} - set(wanted)
    if extra:
        loaded.update(await exposure_data.load_daily_series(sorted(extra)))
    fx_rows_by_symbol = {symbol: series["rows"] for symbol, series in loaded.items()}

    now = now_kst()
    out: dict[str, list[dict]] = {}
    latest: str | None = None
    for code, symbol in symbols.items():
        series = loaded.get(symbol)
        currency = exposure.normalize_currency((series or {}).get("currency") or currency_of.get(code))
        if not series or not currency:
            continue
        fx_rows = None if currency == "KRW" else exposure.krw_fx_rows(currency, fx_rows_by_symbol)
        if currency != "KRW" and not fx_rows:
            continue
        rows = exposure.krw_asof_closes(
            series["rows"], timezone=series["timezone"], close_time=series["close_time"],
            fx_rows=fx_rows, now=now,
        )
        if rows:
            out[code] = rows
            latest = max(latest or "", rows[-1]["session_date"])
    return out, {"requested": len(codes), "loaded": len(out), "as_of": latest}


def _sources(
    etf: dict,
    holdco: dict,
    pairs: dict,
    company: dict,
    closes_as_of: str | None,
    closes_ok: bool,
    foreign_meta: dict | None = None,
) -> list[dict]:
    today = today_kst_date()

    def row(source_id: str, label: str, payload: dict, detail: str | None = None) -> dict:
        return exposure.source_freshness(
            source_id, label, payload.get("as_of"), today=today, max_age_days=_MAX_AGE_DAYS[source_id],
            available=bool(payload.get("available")), detail=detail,
        )

    rows = [
        row("eiayn", "ETF 구성", etf, "EIAYN"),
        row("holding_value", "지주사 지분(설정값)", holdco, "지주사 지분가치"),
        row("common_preferred_spread", "우선주 쌍", pairs, "우선주 괴리율"),
        row("kind", "업종", company, "KIND 상장법인목록"),
        exposure.source_freshness(
            "finance_pi", "일봉", closes_as_of, today=today, max_age_days=_MAX_AGE_DAYS["finance_pi"],
            available=closes_ok, detail="finance-pi",
        ),
    ]
    if foreign_meta is not None:  # 해외·금·가상자산이 없는 계좌에는 보이지 않는다.
        rows.append(exposure.source_freshness(
            "yahoo", "해외·금·코인 일봉", foreign_meta.get("as_of"), today=today, max_age_days=_MAX_AGE_DAYS["yahoo"],
            available=bool(foreign_meta.get("loaded")), detail="Yahoo Finance",
        ))
    return rows


async def _build_report(google_sub: str, account_id: str | None) -> dict:
    items = await portfolio_repo.get_portfolio(google_sub, account_id)
    codes = [item["stock_code"] for item in items]
    quotes, etf, holdco, pairs, company = await asyncio.gather(
        _quotes_for(codes),
        exposure_data.load_etf_holdings(),
        exposure_data.load_holdcos(),
        exposure_data.load_preferred_pairs(),
        exposure_data.load_listed_companies(),
    )
    quotes = await futures_quotes.enrich_quotes(quotes, google_sub)
    snapshot_prices = await _snapshot_unit_prices(google_sub) if items else {}
    positions = _build_positions(items, quotes, snapshot_prices)

    pair_map = pairs.get("pairs") if pairs.get("available") else None
    company_map = company.get("companies") if company.get("available") else None
    holdco_map = holdco.get("holdcos") if holdco.get("available") else None
    etf_map = etf.get("etfs") if etf.get("available") else None

    series_codes = exposure.required_series_codes(positions, pair_map)
    sub_codes = exposure.holdco_subsidiary_codes(positions, holdco_map, pair_map, company_map)
    missing_subs = [code for code in sub_codes if code not in quotes]
    (closes, closes_as_of), (foreign_closes, foreign_meta), sub_quotes, instrument_types = await asyncio.gather(
        _load_closes(series_codes),
        _load_foreign_closes(positions),
        _quotes_for(missing_subs),
        exposure_data.load_instrument_types(exposure.foreign_symbols_needing_type(positions, etf_map)),
    )
    for position in positions:
        position["instrument_type"] = instrument_types.get(exposure.etf_lookup_key(position["code"]))
    sub_prices = {
        code: _safe_float((quotes.get(code) or sub_quotes.get(code) or {}).get("price")) for code in sub_codes
    }

    report = exposure.build_exposure_report(
        positions,
        etfs=etf_map,
        holdcos=holdco_map,
        preferred_pairs=pair_map,
        companies=company_map,
        closes=closes,
        foreign_closes=foreign_closes,
        sub_prices=sub_prices,
        sources=_sources(etf, holdco, pairs, company, closes_as_of, closes is not None, foreign_meta),
        holdco_as_of=holdco.get("as_of"),
        account_id=account_id,
    )
    report["computed_at"] = now_iso()
    return report


async def get_exposure_report(google_sub: str, account_id: str | None = None, *, force: bool = False) -> dict:
    """사용자·계좌별 X-Ray. 없는 계좌면 ``AccountError``(HTTP 400)가 그대로 올라간다."""
    key = f"{google_sub}|{account_id or ''}"
    return await cached_fetch(_report_cache, key, lambda: _build_report(google_sub, account_id), force=force)


def reset() -> None:
    """테스트용 — 결과 캐시를 비운다."""
    _report_cache.clear()

