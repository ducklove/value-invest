"""ePaper 기기용 포트폴리오 요약.

기기(ESP32-S3 PhotoPainter)는 세로 480x800 e-paper 에 총액·손익·상위 종목만
그린다. 웹 SPA 는 ``/api/portfolio`` 응답을 받아 브라우저에서 합계를 계산하지만
기기에는 그럴 여력이 없으므로 서버에서 미리 접어서 내려준다.

시세는 ``snapshot_nav._fetch_total_value`` 처럼 ``force_refresh=True`` 로 새로
긁지 않고 웹 화면과 같은 캐시 경로(``quote_service.enrich_with_cached_quotes``)를
쓴다 — 기기는 수 분 간격으로 폴링하므로 강제 갱신을 걸면 종목 수만큼
업스트림 rate limit 을 태우게 된다.

평가액은 ``qty * quote.price`` 다 — **환율을 곱하지 않는다.** 시세 계층이
이미 원화로 환산해서 내려주기 때문이다 (``foreign.fetch_foreign_quote`` 가
``fx.fx_to_krw`` 를 거친다). 원금만 ``avg_price_currency`` 기준이라 별도로
환산한다.
"""
from __future__ import annotations

from repositories import portfolio as portfolio_repo
from services.portfolio import fx, quote_service

#: 세로 480x800 화면에 행 높이를 줄이지 않고 들어가는 최대 행 수.
DEFAULT_TOP_N = 12


def _safe_float(value) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _pct(numerator: float, denominator: float) -> float | None:
    """분모가 0/음수면 퍼센트는 의미가 없다 (원금 0 인 신규 편입 등)."""
    if not denominator:
        return None
    return round(numerator / denominator * 100, 2)


async def build_summary(google_sub: str, *, top_n: int = DEFAULT_TOP_N) -> dict:
    """기기 화면 한 장에 필요한 값만 담은 요약을 만든다.

    시세가 없는 종목은 합계에서 빠지고 ``unpriced`` 로 세어 내보낸다 —
    화면에 조용히 작은 총액을 띄우는 것보다 "몇 종목 값을 못 구했다"를
    보여주는 편이 낫다.
    """
    items = await portfolio_repo.get_portfolio(google_sub)
    if not items:
        return {
            "total_value": 0.0,
            "total_invested": 0.0,
            "total_pnl": 0.0,
            "total_pnl_pct": None,
            "day_pnl": 0.0,
            "day_pnl_pct": None,
            "holdings": [],
            "holdings_count": 0,
            "unpriced": 0,
            "stale": False,
        }

    enriched = await quote_service.enrich_with_cached_quotes(items)
    await fx.annotate_avg_price_krw(enriched)

    total_value = 0.0
    total_invested = 0.0
    day_pnl = 0.0
    unpriced = 0
    stale = False
    rows: list[dict] = []

    for item in enriched:
        qty = _safe_float(item.get("quantity")) or 0.0
        cost_krw = qty * (_safe_float(item.get("avg_price_krw")) or 0.0)

        quote = item.get("quote") or {}
        price = _safe_float(quote.get("price"))
        if quote.get("_stale"):
            stale = True
        if price is None:
            # 원금은 시세와 무관하게 알 수 있으므로 누적 수익률 분모에는
            # 넣지 않는다 — 평가액에서 빠진 종목의 원금만 남으면 수익률이
            # 실제보다 나쁘게 보인다.
            unpriced += 1
            continue

        # 시세는 이미 원화다 — foreign.fetch_foreign_quote 가 fx.fx_to_krw 로
        # 환산해서 price/change 를 내려준다. 여기서 환율을 한 번 더 곱하면
        # 해외 종목 평가액이 환율 배수만큼 부풀어 오른다.
        # snapshot_nav._fetch_total_value 도 같은 이유로 qty * price 만 쓴다.
        value_krw = qty * price
        change = _safe_float(quote.get("change")) or 0.0

        total_value += value_krw
        total_invested += cost_krw
        day_pnl += qty * change

        rows.append({
            "code": item.get("stock_code"),
            "name": item.get("stock_name") or item.get("stock_code"),
            "value": round(value_krw),
            "pnl_pct": _pct(value_krw - cost_krw, cost_krw),
            "day_pct": _safe_float(quote.get("change_pct")),
        })

    rows.sort(key=lambda row: row["value"], reverse=True)
    top = rows[:top_n] if top_n and top_n > 0 else rows
    for row in top:
        row["weight_pct"] = _pct(row["value"], total_value)

    prev_value = total_value - day_pnl
    return {
        "total_value": round(total_value),
        "total_invested": round(total_invested),
        "total_pnl": round(total_value - total_invested),
        "total_pnl_pct": _pct(total_value - total_invested, total_invested),
        "day_pnl": round(day_pnl),
        "day_pnl_pct": _pct(day_pnl, prev_value),
        "holdings": top,
        "holdings_count": len(rows),
        "unpriced": unpriced,
        "stale": stale,
    }
