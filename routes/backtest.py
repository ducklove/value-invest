"""가치투자 랭킹 백테스트.

사용자가 선택한 universe(관심종목·포트폴리오·시총 Top 30)에서 매 리밸런싱
시점에 value score 상위 K개를 동일가중으로 보유했다면 어떤 성과가 나왔을지
시뮬레이션한다. 벤치마크는 universe 전체 동일가중 리밸런싱.
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
# NPS pool: fetch top-N by market value to cover dynamic top-30 over time
MCAP_POOL_SIZE = 100
FETCH_CONCURRENCY = 8

# Cache for market-cap pool weekly series: years -> (series, name_by_code)
# Persists for the process lifetime — historical weekly data is immutable.
_mcap_cache: dict[int, tuple[dict, dict]] = {}

SCORE_KEYS = {
    "per_low": ("per", False),       # lower is better, must be > 0
    "pbr_low": ("pbr", False),
    "dy_high": ("dividend_yield", True),
    "roe_high": ("roe", True),
}


# ---------------------------------------------------------------------------
# Universe loaders
# ---------------------------------------------------------------------------

async def _load_static_universe(user: dict, source: str) -> list[dict]:
    """Load a fixed universe (watchlist or portfolio)."""
    if source == "watchlist":
        starred = await cache.get_cached_analyses(google_sub=user["google_sub"], tab="starred")
        if not starred:
            raise HTTPException(status_code=400, detail="관심종목이 비어 있습니다.")
        return [{"stock_code": s["stock_code"], "name": s.get("corp_name") or s["stock_code"]}
                for s in starred[:MAX_UNIVERSE]]
    elif source == "portfolio":
        items = await cache.get_portfolio(google_sub=user["google_sub"])
        if not items:
            raise HTTPException(status_code=400, detail="포트폴리오가 비어 있습니다.")
        kr = [i for i in items if i["stock_code"].isdigit() and len(i["stock_code"]) == 6]
        if not kr:
            raise HTTPException(status_code=400, detail="포트폴리오에 국내 주식이 없습니다.")
        return [{"stock_code": i["stock_code"], "name": i.get("stock_name") or i["stock_code"]}
                for i in kr[:MAX_UNIVERSE]]
    raise HTTPException(status_code=400, detail="알 수 없는 universe 소스입니다.")


def _scrape_mcap_ranking() -> list[dict]:
    """Scrape current market-cap ranking from Naver (KOSPI + KOSDAQ)."""
    import subprocess
    from bs4 import BeautifulSoup

    results: list[dict] = []
    seen: set[str] = set()
    # sosok=0: KOSPI, sosok=1: KOSDAQ
    for sosok in (0, 1):
        for page in range(1, 5):  # 4 pages × ~50 rows ≈ 200 per market
            if len(results) >= MCAP_POOL_SIZE:
                break
            url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page={page}"
            try:
                proc = subprocess.run(["curl", "-s", url], capture_output=True, timeout=10)
                soup = BeautifulSoup(proc.stdout, "html.parser", from_encoding="euc-kr")
            except Exception:
                continue
            for tr in soup.select("table.type_2 tbody tr"):
                tds = tr.find_all("td")
                if len(tds) < 7:
                    continue
                name_a = tds[1].find("a")
                if not name_a or "code=" not in (name_a.get("href") or ""):
                    continue
                code = name_a["href"].split("code=")[-1]
                if code in seen or not code.isdigit() or len(code) != 6:
                    continue
                seen.add(code)
                results.append({"stock_code": code, "name": name_a.get_text(strip=True)})
                if len(results) >= MCAP_POOL_SIZE:
                    break
    return results


async def _load_mcap_pool() -> tuple[list[dict], dict[str, str]]:
    """Load market-cap pool — top stocks by current market cap.

    Returns (universe_items, name_by_code).
    """
    items = await asyncio.get_event_loop().run_in_executor(None, _scrape_mcap_ranking)
    if not items:
        raise HTTPException(status_code=500, detail="시가총액 순위를 가져올 수 없습니다.")
    name_map = {it["stock_code"]: it["name"] for it in items}
    logger.info("Market-cap pool loaded: %d stocks", len(items))
    return items, name_map


async def _fetch_series(universe_items: list[dict], years: int) -> dict[str, list[dict]]:
    """Fetch weekly market data for all universe items in parallel."""
    sem = asyncio.Semaphore(FETCH_CONCURRENCY)

    async def _fetch(code: str):
        async with sem:
            try:
                bars = await stock_price.fetch_weekly_market_data(code, years=years)
            except Exception as e:
                logger.warning("backtest fetch failed (%s): %s", code, e)
                return code, []
            return code, [b for b in bars if b.get("close_price")]

    fetched = await asyncio.gather(*(_fetch(s["stock_code"]) for s in universe_items))
    return {code: bars for code, bars in fetched if bars}


# ---------------------------------------------------------------------------
# Backtest endpoint
# ---------------------------------------------------------------------------

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

    source = str(payload.get("universe") or "watchlist")
    is_mcap = source == "nps"

    # --- Load universe & fetch weekly data ---
    if is_mcap:
        cached = _mcap_cache.get(years)
        if cached:
            series, name_by_code = cached
            universe_items = [{"stock_code": c, "name": name_by_code.get(c, c)} for c in series]
        else:
            universe_items, name_by_code = await _load_mcap_pool()
            series = await _fetch_series(universe_items, years)
            _mcap_cache[years] = (series, name_by_code)
    else:
        universe_items = await _load_static_universe(user, source)
        name_by_code = {s["stock_code"]: s["name"] for s in universe_items}
        series = await _fetch_series(universe_items, years)

    if len(series) < 2:
        raise HTTPException(status_code=400, detail="시세 데이터를 불러올 수 있는 종목이 부족합니다.")

    # 마스터 주간 날짜 그리드
    all_dates = sorted({b["date"] for bars in series.values() for b in bars})

    # 종목별 forward-fill 정렬
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

    # 리밸런싱 인덱스
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
            return roe / pbr
        field, higher_better = SCORE_KEYS[score_key]
        v = bar.get(field)
        if v is None:
            return None
        if not higher_better and v <= 0:
            return None
        return v if higher_better else -v

    def _eligible_codes(i: int) -> list[str]:
        """Return eligible codes at date index i.

        For 시총 Top 30: rank pool by market_cap at this point, take top 30.
        For static universes: all codes with data.
        """
        if is_mcap:
            mcap_ranked: list[tuple[float, str]] = []
            for code in series:
                bar = aligned[code][i]
                if bar and bar.get("market_cap"):
                    mcap_ranked.append((bar["market_cap"], code))
            mcap_ranked.sort(reverse=True)
            return [c for _, c in mcap_ranked[:MAX_UNIVERSE]]
        return list(series.keys())

    # --- 전략 equity 시뮬레이션 ---
    n = len(all_dates)
    strat_curve: list[float] = [1.0] * n
    holdings: dict[str, float] = {}
    ref_prices: dict[str, float] = {}
    rebalance_set = set(rebalance_idx)
    current_equity = 1.0

    def _equity_at(i: int) -> float:
        if not holdings:
            return current_equity
        total = 0.0
        for code, dollars in holdings.items():
            bar = aligned[code][i]
            ref = ref_prices.get(code)
            if not bar or not ref:
                total += dollars
                continue
            total += dollars * (bar["close_price"] / ref)
        return total

    last_picks_for_display: list[dict] = []

    for i in range(n):
        if i in rebalance_set:
            current_equity = _equity_at(i)
            eligible = _eligible_codes(i)
            scored: list[tuple[float, str]] = []
            for code in eligible:
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

    # --- 벤치마크: eligible universe 전체 동일가중 리밸런싱 ---
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
            eligible = _eligible_codes(i)
            available = [
                code for code in eligible
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
        "universe_source": source,
        "universe_size": len(series),
        "universe": [
            {"stock_code": c, "corp_name": name_by_code.get(c, c)} for c in series.keys()
        ],
        "missing": [
            {"stock_code": c, "corp_name": name_by_code.get(c, c)}
            for c in (s["stock_code"] for s in universe_items)
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
