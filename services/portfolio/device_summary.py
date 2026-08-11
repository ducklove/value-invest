"""ePaper 기기용 포트폴리오 요약.

기기(ESP32-S3 PhotoPainter)는 세로 480x800 e-paper 에 총액·손익·보유 종목만
그린다. 웹 SPA 는 ``/api/portfolio`` + ``/api/asset-quotes`` 를 받아 브라우저에서
합계를 계산하지만 기기에는 그럴 여력이 없으므로 서버에서 미리 접어서 내려준다.

**웹 화면과 같은 숫자가 나와야 한다.** 그래서 두 가지를 그대로 따른다:

* 시세는 캐시만 보고 없으면 포기하는 게 아니라, ``/api/asset-quotes`` 와 같은
  순서로 채운다 — 국내 벌크 조회 → 개별 조회 → stale 캐시 폴백. 캐시만 쓰던
  때는 워밍업이 안 된 종목이 통째로 빠져 총 평가액이 실제보다 작게 나왔다.
* 오늘/YTD 수익률은 매입가가 아니라 **NAV 기준**이다. 입출금이 있으면 평가액
  단순 비교는 성과가 아니라 현금 이동을 반영해버린다. 웹의
  ``_periodReturn(snap, 'nav')`` 과 같은 식을 쓴다.

평가액은 ``qty * quote.price`` 다 — 환율을 곱하지 않는다. 시세 계층이 이미
원화로 환산해서 내려주기 때문이다 (``foreign.fetch_foreign_quote`` 가
``fx.fx_to_krw`` 를 거친다).

종목 순서는 사용자가 서비스에서 정한 순서(``sort_order``)를 그대로 쓴다.
평가액 순으로 다시 정렬하지 않는다.
"""
from __future__ import annotations

import asyncio
import logging

from repositories import portfolio as portfolio_repo
from repositories import snapshots as snapshots_repo
from services import stock_quotes
from services.portfolio import identifiers, quote_service, time_windows

logger = logging.getLogger(__name__)

#: 세로 480x800 화면에 행 높이를 줄이지 않고 들어가는 최대 행 수.
DEFAULT_TOP_N = 12

#: 시세가 존재하지 않는 참조용 행 (지수/환율). 보유가 아니므로 미확보로 세지 않는다.
NON_QUOTABLE_PREFIXES = ("IDX_", "FX_")

_QUOTE_CONCURRENCY = 2
_QUOTE_ITEM_TIMEOUT = 20.0


def _safe_float(value) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _pct(numerator: float, denominator: float) -> float | None:
    if not denominator:
        return None
    return round(numerator / denominator * 100, 2)


async def _complete_quotes(codes: list[str]) -> dict[str, dict]:
    """웹의 /api/asset-quotes 와 같은 순서로 시세를 채운다."""
    results: dict[str, dict] = {}
    domestic = [c for c in codes if identifiers.is_korean_stock(c)]
    if domestic:
        try:
            results.update(
                await asyncio.wait_for(
                    stock_quotes.get_bulk_quote_snapshots(domestic), timeout=_QUOTE_ITEM_TIMEOUT
                )
            )
        except Exception as exc:
            logger.warning("device summary: 벌크 시세 실패, 개별 조회로 폴백: %s", exc)

    remaining = [c for c in codes if not results.get(c)]
    if not remaining:
        return results

    sem = asyncio.Semaphore(_QUOTE_CONCURRENCY)

    async def one(code: str) -> tuple[str, dict]:
        if code.startswith(NON_QUOTABLE_PREFIXES):
            return code, {}
        try:
            async with sem:
                quote = await asyncio.wait_for(
                    quote_service.fetch_quote(code), timeout=_QUOTE_ITEM_TIMEOUT
                )
            if quote:
                return code, quote
        except Exception as exc:
            logger.warning("device summary: %s 시세 실패: %s", code, exc)
        # 낡은 값이라도 있으면 쓴다 — 빈 값으로 두면 그 종목이 총액에서 통째로
        # 빠져 화면의 총 평가액이 조용히 작아진다.
        stale = stock_quotes.stock_to_quote(stock_quotes.get_stock_cached(code, allow_stale=True))
        if stale:
            stale["_stale"] = True
        return code, stale or {}

    for code, quote in await asyncio.gather(*(one(c) for c in remaining)):
        results[code] = quote
    return results


async def _current_nav(google_sub: str, total_value: float, baseline: dict) -> float | None:
    """현재 NAV = (평가액 - 미반영 입금) / (좌수 + 미반영 좌수변동).

    웹의 ``_curNavKrw`` 과 같은 식이다. 좌수를 모르면 마지막 스냅샷의 NAV 로
    떨어진다 — 수익률이 조금 낡을 뿐 엉뚱한 값이 나오진 않는다.
    """
    latest = await snapshots_repo.get_latest_snapshot(google_sub)
    if not latest:
        return None
    pending_units = 0.0
    pending_cash = 0.0
    for cashflow in baseline["cashflows"]:
        units = _safe_float(cashflow.get("units_change"))
        if units is not None:
            pending_units += units
        else:
            pending_cash += cashflow["signed_amount"]

    units = _safe_float(latest.get("total_units"))
    if units:
        units += pending_units
    value = total_value - pending_cash
    if units and units > 0 and value > 0:
        return value / units
    return _safe_float(latest.get("nav"))


async def _baseline(google_sub: str) -> dict:
    """Today 카드의 기준선: 직전 20:00 정산 스냅샷 + 그 뒤의 입출금."""
    baseline_date = time_windows.portfolio_today_baseline_date()
    snapshot = await snapshots_repo.get_snapshot_on_or_before(google_sub, baseline_date)
    created_after = (
        time_windows.settlement_marker_seconds(snapshot["date"]) if snapshot else baseline_date
    )
    rows = await snapshots_repo.get_cashflows_created_after(google_sub, created_after)
    cashflows = []
    net = 0.0
    for row in rows:
        signed = row["amount"] if row["type"] == "deposit" else -row["amount"] if row["type"] == "withdrawal" else 0.0
        if not signed:
            continue
        net += signed
        cashflows.append({**row, "signed_amount": signed})
    return {"snapshot": snapshot, "cashflows": cashflows, "net_cashflow": net}


def _empty_summary() -> dict:
    return {
        "total_value": 0.0,
        "day_pnl": 0.0,
        "day_pnl_pct": None,
        "ytd_pnl": 0.0,
        "ytd_pnl_pct": None,
        "holdings": [],
        "holdings_count": 0,
        "unpriced": 0,
        "stale": False,
    }


async def build_summary(google_sub: str, *, top_n: int = DEFAULT_TOP_N) -> dict:
    items = await portfolio_repo.get_portfolio(google_sub)
    if not items:
        return _empty_summary()

    quotes = await _complete_quotes([item["stock_code"] for item in items])

    total_value = 0.0
    unpriced = 0
    stale = False
    rows: list[dict] = []

    for item in items:                       # 서비스에서 정한 순서 그대로
        code = item["stock_code"]
        qty = _safe_float(item.get("quantity")) or 0.0
        quote = quotes.get(code) or {}
        price = _safe_float(quote.get("price"))
        if quote.get("_stale"):
            stale = True
        if price is None:
            if not code.startswith(NON_QUOTABLE_PREFIXES):
                unpriced += 1
            continue
        value = qty * price
        total_value += value
        rows.append({
            "code": code,
            "name": item.get("stock_name") or code,
            "value": round(value),
            "day_pct": _safe_float(quote.get("change_pct")),
        })

    for row in rows:
        row["weight_pct"] = _pct(row["value"], total_value)

    baseline = await _baseline(google_sub)
    cur_nav = await _current_nav(google_sub, total_value, baseline)
    year_start = await snapshots_repo.get_year_start_snapshot(google_sub)

    def period(snapshot: dict | None, *, subtract_cashflow: bool) -> tuple[float | None, float | None]:
        """(손익, 수익률%).

        수익률은 NAV 비교 — 입출금이 성과로 잡히지 않는다.
        금액은 평가액 차이 그대로다 (웹과 같은 정의). 그래서 기간 중 출금이
        크면 수익률은 플러스인데 금액은 마이너스로 나올 수 있다. 두 값의
        의미가 다를 뿐 오류가 아니다.
        """
        if not snapshot:
            return None, None
        base_value = _safe_float(snapshot.get("total_value"))
        base_nav = _safe_float(snapshot.get("nav"))
        pnl = None
        if base_value is not None:
            pnl = total_value - base_value
            if subtract_cashflow:
                pnl -= baseline["net_cashflow"]
        pct = None
        if base_nav and cur_nav:
            pct = round((cur_nav / base_nav - 1) * 100, 2)
        return pnl, pct

    day_pnl, day_pct = period(baseline["snapshot"], subtract_cashflow=True)
    ytd_pnl, ytd_pct = period(year_start, subtract_cashflow=False)

    return {
        "total_value": round(total_value),
        "day_pnl": round(day_pnl) if day_pnl is not None else None,
        "day_pnl_pct": day_pct,
        "ytd_pnl": round(ytd_pnl) if ytd_pnl is not None else None,
        "ytd_pnl_pct": ytd_pct,
        "holdings": rows[:top_n] if top_n and top_n > 0 else rows,
        "holdings_count": len(rows),
        "unpriced": unpriced,
        "stale": stale,
    }
