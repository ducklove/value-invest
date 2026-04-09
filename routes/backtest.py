"""관심종목 universe 기반 가치투자 랭킹 백테스트.

사용자가 별표한 관심종목들을 universe로 삼아, 매 리밸런싱 시점에 value score
상위 K개를 동일가중으로 보유했다면 어떤 성과가 나왔을지 시뮬레이션한다.
buy & hold(관심종목 동일가중 매수 후 보유)와 비교한다.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date

from fastapi import APIRouter, Body, HTTPException, Request

import cache
import stock_price
from deps import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter()

MAX_UNIVERSE = 30
FETCH_CONCURRENCY = 4

SCORE_KEYS = {
    "per_low": ("per", False),       # lower is better, must be > 0
    "pbr_low": ("pbr", False),
    "dy_high": ("dividend_yield", True),
    "roe_high": ("roe", True),
}


@router.post("/api/backtest/watchlist")
async def backtest_watchlist(request: Request, payload: dict = Body(default={})):
    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    score_key = str(payload.get("score") or "pbr_low")
    if score_key != "blend" and score_key not in SCORE_KEYS:
        raise HTTPException(status_code=400, detail="알 수 없는 score 입니다.")
    try:
        top_k = max(1, min(int(payload.get("top_k") or 5), MAX_UNIVERSE))
    except (TypeError, ValueError):
        top_k = 5
    rebalance = str(payload.get("rebalance") or "monthly")
    if rebalance not in ("monthly", "quarterly"):
        rebalance = "monthly"
    try:
        years = int(payload.get("years") or 5)
    except (TypeError, ValueError):
        years = 5
    years = max(1, min(years, 15))

    starred = await cache.get_cached_analyses(google_sub=user["google_sub"], tab="starred")
    if not starred:
        raise HTTPException(status_code=400, detail="관심종목이 비어 있습니다.")
    universe = starred[:MAX_UNIVERSE]
    name_by_code = {s["stock_code"]: s.get("corp_name") or s["stock_code"] for s in universe}

    # 병렬 fetch (동시성 제한)
    sem = asyncio.Semaphore(FETCH_CONCURRENCY)

    async def _fetch(code: str):
        async with sem:
            try:
                bars = await stock_price.fetch_weekly_market_data(code, years=years)
            except Exception as e:
                logger.warning("backtest fetch failed (%s): %s", code, e)
                return code, []
            return code, [b for b in bars if b.get("close_price")]

    fetched = await asyncio.gather(*(_fetch(s["stock_code"]) for s in universe))
    series = {code: bars for code, bars in fetched if bars}
    if len(series) < 2:
        raise HTTPException(status_code=400, detail="시세 데이터를 불러올 수 있는 종목이 부족합니다.")

    # 마스터 주간 날짜 그리드
    all_dates = sorted({b["date"] for bars in series.values() for b in bars})

    # 종목별 forward-fill 정렬: index[d] -> bar (또는 None)
    aligned: dict[str, list[dict | None]] = {}
    for code, bars in series.items():
        bar_map = {b["date"]: b for b in bars}
        out: list[dict | None] = []
        last: dict | None = None
        for d in all_dates:
            if d in bar_map:
                last = bar_map[d]
            out.append(last)
        aligned[code] = out

    # 리밸런싱 인덱스: 각 월(또는 분기)의 첫 봉
    rebalance_idx: list[int] = []
    last_period: tuple | None = None
    for i, d in enumerate(all_dates):
        y = int(d[0:4])
        m = int(d[5:7])
        period = (y, m) if rebalance == "monthly" else (y, (m - 1) // 3)
        if period != last_period:
            rebalance_idx.append(i)
            last_period = period
    if not rebalance_idx:
        rebalance_idx = [0]

    def _score(bar: dict | None) -> float | None:
        if not bar:
            return None
        if score_key == "blend":
            pbr = bar.get("pbr")
            roe = bar.get("roe")
            if pbr is None or pbr <= 0 or roe is None:
                return None
            # 낮은 PBR + 높은 ROE → blend = roe / pbr (높을수록 좋음)
            return roe / pbr
        field, higher_better = SCORE_KEYS[score_key]
        v = bar.get(field)
        if v is None:
            return None
        if not higher_better and v <= 0:
            return None
        return v if higher_better else -v  # 큰 값이 좋은 쪽으로 통일

    # 전략 equity 시뮬레이션
    n = len(all_dates)
    strat_curve: list[float] = [1.0] * n
    holdings: dict[str, float] = {}     # stock_code -> dollar 할당
    ref_prices: dict[str, float] = {}   # 리밸런싱 시점 기준가
    rebalance_set = set(rebalance_idx)
    current_picks: list[str] = []
    current_equity = 1.0

    def _equity_at(i: int) -> float:
        if not holdings:
            return current_equity
        total = 0.0
        for code, dollars in holdings.items():
            bar = aligned[code][i]
            ref = ref_prices.get(code)
            if not bar or not ref:
                total += dollars  # 데이터 없음 → 변동 없음
                continue
            total += dollars * (bar["close_price"] / ref)
        return total

    last_picks_for_display: list[dict] = []

    for i in range(n):
        if i in rebalance_set:
            current_equity = _equity_at(i)
            scored: list[tuple[float, str]] = []
            for code in series.keys():
                bar = aligned[code][i]
                if not bar or not bar.get("close_price"):
                    continue
                s = _score(bar)
                if s is None:
                    continue
                scored.append((s, code))
            scored.sort(reverse=True)
            picks = [c for _, c in scored[:top_k]]
            if picks:
                per_stock = current_equity / len(picks)
                holdings = {c: per_stock for c in picks}
                ref_prices = {c: aligned[c][i]["close_price"] for c in picks}
                current_picks = picks
                last_picks_for_display = [
                    {
                        "stock_code": c,
                        "corp_name": name_by_code.get(c, c),
                        "score": round(_score(aligned[c][i]) or 0.0, 4),
                        "metrics": {
                            "per": aligned[c][i].get("per"),
                            "pbr": aligned[c][i].get("pbr"),
                            "roe": aligned[c][i].get("roe"),
                            "dividend_yield": aligned[c][i].get("dividend_yield"),
                        },
                    }
                    for c in picks
                ]
        strat_curve[i] = round(_equity_at(i), 6)

    # 벤치마크: 매 리밸런싱마다 관심종목 전체 동일가중 재배분
    bh_curve: list[float] = [1.0] * n
    bh_holdings: dict[str, float] = {}
    bh_refs: dict[str, float] = {}
    bh_equity = 1.0

    def _bh_equity_at(i: int) -> float:
        if not bh_holdings:
            return bh_equity
        total = 0.0
        for code, dollars in bh_holdings.items():
            bar = aligned[code][i]
            ref = bh_refs.get(code)
            if not bar or not ref:
                total += dollars
                continue
            total += dollars * (bar["close_price"] / ref)
        return total

    for i in range(n):
        if i in rebalance_set:
            bh_equity = _bh_equity_at(i)
            available = [
                code for code in series.keys()
                if (b := aligned[code][i]) and b.get("close_price")
            ]
            if available:
                per_stock = bh_equity / len(available)
                bh_holdings = {c: per_stock for c in available}
                bh_refs = {c: aligned[c][i]["close_price"] for c in available}
        bh_curve[i] = round(_bh_equity_at(i), 6)

    total_years = max((n - 1) / 52.0, 1e-9)
    strat_total = strat_curve[-1]
    bh_total = bh_curve[-1]

    def _cagr(eq: float) -> float | None:
        return eq ** (1.0 / total_years) - 1.0 if eq > 0 else None

    return {
        "score": score_key,
        "rebalance": rebalance,
        "top_k": top_k,
        "years": years,
        "universe_size": len(series),
        "universe": [
            {"stock_code": c, "corp_name": name_by_code.get(c, c)} for c in series.keys()
        ],
        "missing": [
            {"stock_code": c, "corp_name": name_by_code.get(c, c)}
            for c in (s["stock_code"] for s in universe)
            if c not in series
        ],
        "dates": all_dates,
        "strategy_equity": strat_curve,
        "benchmark_equity": bh_curve,
        "current_picks": last_picks_for_display,
        "stats": {
            "bars": n,
            "rebalances": len(rebalance_idx),
            "strategy_total_return_pct": round((strat_total - 1.0) * 100.0, 2),
            "benchmark_total_return_pct": round((bh_total - 1.0) * 100.0, 2),
            "strategy_cagr_pct": round((_cagr(strat_total) or 0.0) * 100.0, 2),
            "benchmark_cagr_pct": round((_cagr(bh_total) or 0.0) * 100.0, 2),
        },
    }
