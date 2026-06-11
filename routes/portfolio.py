import asyncio
import json
import logging
import os
import re
import time
from datetime import date, timedelta
from pathlib import Path

from fastapi import APIRouter, Body, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse

import asset_insights
import cache
from repositories import benchmark_daily as benchmark_repo
from repositories import foreign_dividends as foreign_dividends_repo
from repositories import portfolio as portfolio_repo
from repositories import snapshots as snapshots_repo
from deps import get_current_user
from services import stock_quotes
from services.portfolio import ai_analysis
from services.portfolio import foreign
from services.portfolio import insights
from services.portfolio import quote_service
from services.portfolio.identifiers import (
    SPECIAL_ASSETS as _SPECIAL_ASSETS,
    common_stock_code as _common_stock_code,
    is_cash_asset as _is_cash_asset,
    is_korean_stock as _is_korean_stock,
    is_preferred_stock as _is_preferred_stock,
    is_special_asset as _is_special_asset,
    normalize_portfolio_code as _normalize_portfolio_code,
)
from services.portfolio import target_resolver
from services.portfolio.targets import parse_target_input as _parse_target_input
from services.portfolio.target_metrics import supplement_target_metrics as _supplement_target_metrics
from services.portfolio import benchmarks
from services.portfolio.benchmarks import (
    BENCHMARK_ENDPOINT_ITEM_TIMEOUT as _BENCHMARK_ENDPOINT_ITEM_TIMEOUT,
)
from services.portfolio import dividends
from services.portfolio import names
from services.portfolio.time_windows import (
    intraday_axis_window as _intraday_axis_window,
    portfolio_today_baseline_date as _portfolio_today_baseline_date,
)

_OPENROUTER_KEY = os.getenv("OPENROUTER_API_KEY", "")
_keys_file = Path(__file__).parent.parent / "keys.txt"
if _keys_file.exists():
    for line in _keys_file.read_text().splitlines():
        if line.startswith("OPENROUTER_API_KEY="):
            _OPENROUTER_KEY = line.split("=", 1)[1].strip()

logger = logging.getLogger(__name__)
router = APIRouter()


async def _fetch_cash_quote(stock_code: str) -> dict:
    """Fetch cash quote (delegates to the quote service).

    Kept as a thin module-level name so router-internal callers and tests can
    patch ``routes.portfolio._fetch_cash_quote``; the implementation lives in
    ``services.portfolio.quote_service``.
    """
    return await quote_service.fetch_cash_quote(stock_code)


async def _fetch_quote(
    stock_code: str,
    *,
    force_refresh: bool = False,
    use_ws_cache: bool = True,
) -> dict:
    """Stock-quote entrypoint (delegates to the quote service).

    Kept as a thin module-level name so router-internal callers and the ~18
    tests that patch ``routes.portfolio._fetch_quote`` keep working; the
    implementation lives in ``services.portfolio.quote_service``.
    """
    return await quote_service.fetch_quote(
        stock_code,
        force_refresh=force_refresh,
        use_ws_cache=use_ws_cache,
    )


def _cached_quote_for_code(code: str) -> dict:
    """Cached-quote lookup (delegates to the quote service).

    Kept as a thin module-level name for router-internal callers; the
    implementation lives in ``services.portfolio.quote_service``.
    """
    return quote_service.cached_quote_for_code(code)


async def _enrich_with_cached_quotes(items: list[dict]) -> list[dict]:
    """Attach cached quotes (delegates to the quote service)."""
    return await quote_service.enrich_with_cached_quotes(items)


async def _fill_snapshot_quotes(google_sub: str, items: list[dict]) -> None:
    if not items:
        return
    latest = await snapshots_repo.get_latest_snapshot(google_sub)
    snap_date = latest.get("date") if latest else None
    if not snap_date:
        return
    rows = await snapshots_repo.get_stock_snapshots_by_date(google_sub, snap_date)
    values = {row["stock_code"]: row["market_value"] for row in rows}
    for item in items:
        quote = item.get("quote") or {}
        if quote.get("price") is not None and not quote.get("_stale"):
            continue
        value = values.get(item.get("stock_code"))
        qty = item.get("quantity")
        try:
            if value is None or qty is None or float(qty) == 0:
                continue
            item["quote"] = {
                "date": snap_date,
                "price": round(float(value) / float(qty), 4),
                "change": 0,
                "change_pct": None,
                "_stale": True,
            }
        except (TypeError, ValueError, ZeroDivisionError):
            continue


QUOTE_RATE_INTERVAL = 0.22  # ~4.5 req/s, stays under 5/s limit


# --- Benchmark ---
#
# Implementation lives in services.portfolio.benchmarks; the router keeps thin
# delegators (and cache aliases pointing at the service's shared objects) so its
# endpoints and the tests that patch ``routes.portfolio._<fn>`` keep working.

_market_type_cache = benchmarks.market_type_cache
_benchmark_name_cache = benchmarks.benchmark_name_cache
_benchmark_quote_cache = benchmarks.benchmark_quote_cache


async def _detect_market_type(code: str) -> str:
    return await benchmarks.detect_market_type(code)


async def _prefetch_market_types(codes: list[str]):
    return await benchmarks.prefetch_market_types(codes)


async def _resolve_default_benchmark(code: str) -> str:
    return await benchmarks.resolve_default_benchmark(code)


def _resolve_default_benchmark_fast(code: str) -> str:
    return benchmarks.resolve_default_benchmark_fast(code)


async def _resolve_benchmark_name(code: str) -> str:
    return await benchmarks.resolve_benchmark_name(code)


def _cached_benchmark_quote(benchmark_code: str, *, allow_stale: bool = True) -> dict | None:
    return benchmarks.cached_benchmark_quote(benchmark_code, allow_stale=allow_stale)


def _resolve_benchmark_name_fast(code: str, items: list[dict] | None = None) -> str:
    return benchmarks.resolve_benchmark_name_fast(code, items)


def _resolve_benchmark_name_from_code_table(
    code: str,
    items: list[dict] | None,
    corp_code_table: dict[str, dict] | None,
) -> str:
    return benchmarks.resolve_benchmark_name_from_code_table(code, items, corp_code_table)


async def _fetch_benchmark_quote(benchmark_code: str) -> dict:
    return await benchmarks.fetch_benchmark_quote(benchmark_code)


def _normalize_portfolio_tags(raw_tags) -> list[str]:
    if raw_tags is None:
        return []
    if isinstance(raw_tags, str):
        parts = re.split(r"[,#\n]+", raw_tags)
    elif isinstance(raw_tags, list):
        parts = raw_tags
    else:
        raise HTTPException(status_code=400, detail="tags는 문자열 또는 배열이어야 합니다.")

    tags: list[str] = []
    seen: set[str] = set()
    for raw in parts:
        tag = re.sub(r"\s+", " ", str(raw or "").strip().lstrip("#"))[:30]
        if not tag:
            continue
        key = tag.casefold()
        if key in seen:
            continue
        seen.add(key)
        tags.append(tag)
        if len(tags) >= 12:
            break
    return tags


@router.get("/api/portfolio/asset-insight/{stock_code}")
async def asset_insight(stock_code: str, request: Request, response: Response):
    response.headers["Cache-Control"] = "no-store, max-age=0"
    response.headers["Pragma"] = "no-cache"
    user = _require_user(await get_current_user(request))
    stock_code = stock_code.strip()
    items = await portfolio_repo.get_portfolio(user["google_sub"])
    item = next((it for it in items if it["stock_code"] == stock_code), None)
    if not item:
        raise HTTPException(status_code=404, detail="포트폴리오에 없는 종목입니다.")

    quote_task = asyncio.create_task(insights.fetch_quote_for_insight(stock_code))
    asset_history_task = asyncio.create_task(insights.asset_history_for_insight(stock_code, item))
    effective_benchmark = await insights.resolve_insight_benchmark(item)

    profile = {
        "code": stock_code,
        "name": item.get("stock_name") or stock_code,
        "currency": item.get("currency") or "",
        "benchmarkCode": effective_benchmark,
        **asset_insights.classify_asset(stock_code, item.get("stock_name") or "", item.get("currency") or ""),
    }
    indicator_task = asyncio.create_task(insights.fetch_insight_indicators(insights.macro_codes_for_asset(profile, item.get("currency"))))
    valuation_task = asyncio.create_task(insights.fetch_insight_valuation_basis(stock_code))

    quote, history_payload, benchmark_quote, benchmark_name, benchmark_rows, indicators, valuation_basis = await asyncio.gather(
        quote_task,
        asset_history_task,
        _fetch_benchmark_quote(effective_benchmark),
        _resolve_benchmark_name(effective_benchmark),
        insights.benchmark_history_for_insight(effective_benchmark),
        indicator_task,
        valuation_task,
    )
    profile["benchmarkName"] = benchmark_name

    metrics = asset_insights.calculate_history_metrics(history_payload.get("rows") or [])
    benchmark_metrics = asset_insights.calculate_history_metrics(benchmark_rows)
    benchmark_returns = benchmark_metrics.get("returns") or {}
    relative = asset_insights.relative_returns(metrics.get("returns") or {}, benchmark_returns)
    position = asset_insights.calculate_position(item, quote)
    valuation = insights.build_insight_valuation(quote, valuation_basis)
    gold_gap = insights.gold_gap_for_asset(stock_code)
    holding = insights.holding_context_for_asset(stock_code)
    import external_tools
    etf = await external_tools.etf_link_for(stock_code)
    tags_task = asyncio.create_task(portfolio_repo.get_portfolio_tags(user["google_sub"], stock_code))
    tag_suggestions_task = asyncio.create_task(portfolio_repo.get_portfolio_tag_suggestions(user["google_sub"]))

    benchmark = {
        "code": effective_benchmark,
        "name": benchmark_name,
        "dayChangePct": benchmark_quote.get("change_pct") if benchmark_quote else None,
        "returns": benchmark_returns,
        "relativeReturns": relative,
    }
    return {
        "profile": profile,
        "position": position,
        "quote": quote or {},
        "valuation": valuation,
        "metrics": metrics,
        "benchmark": benchmark,
        "macro": insights.format_macro(indicators),
        "goldGap": gold_gap,
        "holding": holding,
        "etf": etf,
        "tags": await tags_task,
        "tagSuggestions": await tag_suggestions_task,
        "history": (history_payload.get("rows") or [])[-80:],
        "dataQuality": {
            "historyCurrency": history_payload.get("currency"),
            "historyPoints": metrics.get("historyPoints", 0),
            "benchmarkPoints": benchmark_metrics.get("historyPoints", 0),
        },
        "signals": asset_insights.build_signals(profile, position, metrics, benchmark, gold_gap),
    }


@router.put("/api/portfolio/{stock_code}/tags")
async def update_portfolio_tags(stock_code: str, request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    stock_code = stock_code.strip()
    item = await portfolio_repo.get_portfolio_item(user["google_sub"], stock_code)
    if not item:
        raise HTTPException(status_code=404, detail="포트폴리오에 없는 종목입니다.")
    tags = _normalize_portfolio_tags((payload or {}).get("tags"))
    saved = await portfolio_repo.set_portfolio_tags(user["google_sub"], stock_code, tags)
    return {
        "ok": True,
        "stock_code": stock_code,
        "tags": saved,
        "tagSuggestions": await portfolio_repo.get_portfolio_tag_suggestions(user["google_sub"]),
    }


@router.get("/api/portfolio/groups")
async def get_groups(request: Request):
    user = _require_user(await get_current_user(request))
    return await portfolio_repo.get_portfolio_groups(user["google_sub"])


@router.post("/api/portfolio/groups")
async def add_group(request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    name = str(payload.get("name") or "").strip()[:50]
    if not name:
        raise HTTPException(status_code=400, detail="그룹명을 입력해 주세요.")
    groups = await portfolio_repo.get_portfolio_groups(user["google_sub"])
    if any(g["group_name"] == name for g in groups):
        raise HTTPException(status_code=400, detail="이미 존재하는 그룹명입니다.")
    result = await portfolio_repo.add_portfolio_group(user["google_sub"], name)
    return {"ok": True, **result}


@router.put("/api/portfolio/groups/{group_name}")
async def rename_group(group_name: str, request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    new_name = str(payload.get("new_name") or "").strip()[:50]
    if not new_name:
        raise HTTPException(status_code=400, detail="새 그룹명을 입력해 주세요.")
    groups = await portfolio_repo.get_portfolio_groups(user["google_sub"])
    target = next((g for g in groups if g["group_name"] == group_name), None)
    if not target:
        raise HTTPException(status_code=404, detail="그룹을 찾을 수 없습니다.")
    if any(g["group_name"] == new_name for g in groups):
        raise HTTPException(status_code=400, detail="이미 존재하는 그룹명입니다.")
    await portfolio_repo.rename_portfolio_group(user["google_sub"], group_name, new_name)
    return {"ok": True}


@router.delete("/api/portfolio/groups/{group_name}")
async def delete_group(group_name: str, request: Request):
    user = _require_user(await get_current_user(request))
    groups = await portfolio_repo.get_portfolio_groups(user["google_sub"])
    target = next((g for g in groups if g["group_name"] == group_name), None)
    if not target:
        raise HTTPException(status_code=404, detail="그룹을 찾을 수 없습니다.")
    default_count = sum(1 for g in groups if g["is_default"])
    if target["is_default"] and default_count <= 3:
        raise HTTPException(status_code=400, detail="기본 그룹은 삭제할 수 없습니다.")
    await portfolio_repo.delete_portfolio_group(user["google_sub"], group_name)
    return {"ok": True}


@router.put("/api/portfolio/groups-order")
async def save_groups_order(request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    names = payload.get("group_names")
    if not isinstance(names, list) or not names or len(names) > 50:
        raise HTTPException(status_code=400, detail="그룹 목록이 필요합니다.")
    names = [str(n).strip()[:50] for n in names if str(n).strip()]
    await portfolio_repo.save_portfolio_groups_order(user["google_sub"], names)
    return {"ok": True}


@router.get("/api/portfolio/quotes")
async def stream_portfolio_quotes(request: Request):
    """Stream quote updates one by one with rate limiting."""
    user = _require_user(await get_current_user(request))
    items = await portfolio_repo.get_portfolio(user["google_sub"])

    # Prefetch market types before streaming
    needs_resolve = [it for it in items if not it.get("benchmark_code")]
    if needs_resolve:
        await _prefetch_market_types([it["stock_code"] for it in needs_resolve])

    async def generate():
        import json as _json

        # Fire every quote fetch in parallel — backpressure is enforced by
        # the per-upstream semaphores (foreign._NAVER_SEM, foreign._YF_SEM, KIS proxy sem),
        # so we don't serialize here. Stream results as they arrive.
        async def _one_quote(code: str) -> tuple[str, dict]:
            try:
                return code, await _fetch_quote(code)
            except Exception:
                return code, {}

        quote_tasks: list[asyncio.Task] = []
        bench_tasks: list[asyncio.Task] = []
        bc_task: asyncio.Future | None = None
        try:
            quote_tasks = [asyncio.create_task(_one_quote(it["stock_code"])) for it in items]

            # Resolve benchmark codes in parallel too (some need _detect_market_type).
            async def _resolve_bc(item: dict) -> str:
                return item.get("benchmark_code") or await _resolve_default_benchmark(item["stock_code"])

            bc_task = asyncio.gather(*[_resolve_bc(it) for it in items])

            for fut in asyncio.as_completed(quote_tasks):
                code, quote = await fut
                yield f"data: {_json.dumps({'stock_code': code, 'quote': quote}, ensure_ascii=False)}\n\n"

            try:
                benchmark_codes = set(await bc_task)
            except Exception:
                benchmark_codes = set()

            # Benchmark quotes in parallel as well.
            async def _one_bench(bc: str) -> tuple[str, dict]:
                try:
                    return bc, await _fetch_benchmark_quote(bc)
                except Exception:
                    return bc, {}

            bench_tasks = [asyncio.create_task(_one_bench(bc)) for bc in benchmark_codes]
            for fut in asyncio.as_completed(bench_tasks):
                bc, bq = await fut
                yield f"data: {_json.dumps({'benchmark_code': bc, 'benchmark_quote': bq}, ensure_ascii=False)}\n\n"

            yield "data: {\"done\": true}\n\n"
        finally:
            pending = [
                task
                for task in [*quote_tasks, *bench_tasks, bc_task]
                if task is not None and not task.done()
            ]
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)

    from fastapi.responses import StreamingResponse
    return StreamingResponse(generate(), media_type="text/event-stream")


@router.get("/api/asset-quote/{stock_code}")
async def asset_quote(stock_code: str):
    """Fetch quote for any asset type (Korean stock, cash, gold, crypto, foreign)."""
    try:
        q = await _fetch_quote(stock_code)
        if not q:
            raise HTTPException(status_code=404, detail="시세를 가져올 수 없습니다.")
        return q
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=404, detail="시세를 가져올 수 없습니다.")


_NON_QUOTABLE_PREFIXES = ("IDX_", "FX_")
_ASSET_QUOTES_BATCH_TIMEOUT = 45.0
_ASSET_QUOTES_ITEM_TIMEOUT = 30.0
_ASSET_QUOTES_CONCURRENCY = 2

@router.post("/api/asset-quotes")
async def asset_quotes_batch(payload: dict = Body(...)):
    """Fetch quotes for multiple codes in one request."""
    raw_codes = payload.get("codes", [])
    if not isinstance(raw_codes, list) or len(raw_codes) > 100:
        raise HTTPException(status_code=400, detail="최대 100개까지 조회 가능합니다.")
    codes = []
    seen_codes = set()
    for raw in raw_codes:
        code = str(raw).strip()
        if not code or code in seen_codes:
            continue
        seen_codes.add(code)
        codes.append(code)
    fresh = bool(payload.get("fresh", True))
    if not fresh:
        return {code: _cached_quote_for_code(code) for code in codes}

    results: dict[str, dict] = {code: {} for code in codes}

    # Fast path: pull every domestic (KRX) code in ONE bulk upstream call
    # instead of one rate-limited KIS quote call each. This is the dominant
    # cost on initial load for a domestic-heavy portfolio. Best-effort —
    # anything the bulk source can't resolve falls through to the per-code
    # path below, so there is no regression if Naver is unavailable.
    domestic = [code for code in codes if _is_korean_stock(code)]
    if domestic:
        try:
            bulk = await asyncio.wait_for(
                stock_quotes.get_bulk_quote_snapshots(domestic),
                timeout=_ASSET_QUOTES_ITEM_TIMEOUT,
            )
        except Exception as exc:
            logger.warning("벌크 시세 조회 실패; 개별 조회로 폴백: %s", exc)
            bulk = {}
        results.update(bulk)

    remaining = [code for code in codes if not results.get(code)]
    if not remaining:
        return results

    # Per-code path for foreign / special assets and any domestic code the
    # bulk call missed. Low concurrency keeps these upstreams rate-friendly;
    # the screen is already painted from snapshot + server cache.
    sem = asyncio.Semaphore(_ASSET_QUOTES_CONCURRENCY)

    async def _fetch_one(code):
        if code.startswith(_NON_QUOTABLE_PREFIXES):
            return code, {}
        def fallback_quote() -> dict:
            q = stock_quotes.stock_to_quote(stock_quotes.get_stock_cached(code, allow_stale=True))
            if q:
                q["_stale"] = True
            return q
        try:
            async with sem:
                force_upstream = _is_korean_stock(code)
                q = await asyncio.wait_for(
                    _fetch_quote(
                        code,
                        force_refresh=force_upstream,
                        use_ws_cache=not force_upstream,
                    ),
                    timeout=_ASSET_QUOTES_ITEM_TIMEOUT,
                )
            return code, q or {}
        except asyncio.CancelledError:
            return code, fallback_quote()
        except (asyncio.TimeoutError, Exception):
            return code, fallback_quote()

    task_codes = {}
    tasks = []
    for code in remaining:
        task = asyncio.create_task(_fetch_one(code))
        task_codes[task] = code
        tasks.append(task)
    done, pending = await asyncio.wait(tasks, timeout=_ASSET_QUOTES_BATCH_TIMEOUT)
    for task in pending:
        code = task_codes.get(task)
        if code:
            q = stock_quotes.stock_to_quote(stock_quotes.get_stock_cached(code, allow_stale=True))
            if q:
                q["_stale"] = True
            results[code] = q
        task.cancel()
    for task in done:
        if task.cancelled():
            continue
        try:
            code, quote = task.result()
            results[code] = quote
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
    return results


def _require_user(user):
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    return user


@router.get("/api/portfolio")
async def get_portfolio(request: Request):
    started = time.perf_counter()
    user = _require_user(await get_current_user(request))
    await portfolio_repo.get_portfolio_groups(user["google_sub"])  # ensure default groups
    items = await portfolio_repo.get_portfolio(user["google_sub"])
    needs_resolve = [it for it in items if not it.get("benchmark_code")]
    for item in needs_resolve:
        item["benchmark_code"] = _resolve_default_benchmark_fast(item["stock_code"])
    # Annotate with trailing dividend per share so the UI can show a
    # "배당액" column (= trailing_dps × quantity). Multiplying on the
    # client keeps the number fresh while the user edits quantity in
    # the inline edit row.
    codes = [it["stock_code"] for it in items]
    dividends.schedule_for_portfolio(codes)
    metric_codes = list(dict.fromkeys(
        codes + [
            _common_stock_code(code)
            for code in codes
            if _is_korean_stock(code) and _is_preferred_stock(code)
        ]
    ))
    dps_map, target_metrics_map = await asyncio.gather(
        portfolio_repo.get_trailing_dividends(codes),
        portfolio_repo.get_portfolio_target_metrics(metric_codes),
    )
    await _supplement_target_metrics(items, target_metrics_map)
    for it in items:
        code = it["stock_code"]
        metrics = dict(target_metrics_map.get(code) or {})
        if _is_korean_stock(code) and _is_preferred_stock(code):
            common_metrics = target_metrics_map.get(_common_stock_code(code)) or {}
            for key in ("eps", "bps", "dps"):
                if metrics.get(key) is None and common_metrics.get(key) is not None:
                    metrics[key] = common_metrics[key]
        trailing_dps = dps_map.get(code)
        it["trailing_dps"] = trailing_dps
        if trailing_dps is not None:
            metrics["dps"] = trailing_dps
        it["target_metrics"] = metrics
    enriched = await _enrich_with_cached_quotes(items)
    await _fill_snapshot_quotes(user["google_sub"], enriched)
    insights.schedule_asset_insight_warmup(enriched)
    elapsed_ms = (time.perf_counter() - started) * 1000
    if elapsed_ms > 1000:
        logger.warning("portfolio list slow: %.0fms items=%d user=%s", elapsed_ms, len(enriched), user.get("email") or user.get("google_sub"))
    return enriched


async def _resolve_target_formula_price(stock_code: str, formula: str, avg_price: float) -> float | None:
    """Save-time formula resolution (delegates to the target resolver service).

    Kept as a thin module-level name so the save handler stays readable; the
    implementation lives in ``services.portfolio.target_resolver`` and this
    wrapper only maps service errors to HTTP 400.
    """
    try:
        return await target_resolver.resolve_formula_target_at_save(stock_code, formula, avg_price)
    except target_resolver.TargetFormulaError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/api/portfolio/order")
async def save_portfolio_order(request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    stock_codes = payload.get("stock_codes")
    if not isinstance(stock_codes, list) or not stock_codes:
        raise HTTPException(status_code=400, detail="정렬할 종목 목록이 필요합니다.")
    codes: list[str] = []
    seen: set[str] = set()
    for raw in stock_codes:
        code = _normalize_portfolio_code(str(raw or ""))
        if code and code not in seen:
            seen.add(code)
            codes.append(code)
    if not codes:
        raise HTTPException(status_code=400, detail="정렬할 종목 목록이 필요합니다.")

    current = await portfolio_repo.get_portfolio(user["google_sub"])
    current_codes = [item["stock_code"] for item in current]
    current_set = set(current_codes)
    requested_set = set(codes)
    if len(codes) != len(current_codes) or requested_set != current_set:
        missing = [code for code in current_codes if code not in requested_set]
        unknown = [code for code in codes if code not in current_set]
        detail = "포트폴리오 전체 종목 순서와 맞지 않습니다."
        parts = []
        if missing:
            parts.append("missing=" + ",".join(missing[:8]))
        if unknown:
            parts.append("unknown=" + ",".join(unknown[:8]))
        if parts:
            detail += " " + " ".join(parts)
        raise HTTPException(status_code=400, detail=detail)

    await portfolio_repo.save_portfolio_order(user["google_sub"], codes)
    return {"ok": True, "count": len(codes)}


@router.put("/api/portfolio/{stock_code}")
async def save_portfolio_item(stock_code: str, request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    stock_code = _normalize_portfolio_code(stock_code)

    stock_name = str(payload.get("stock_name") or "").strip()
    domestic_alias = await foreign.resolve_domestic_code_alias(stock_code)
    if domestic_alias:
        stock_code = domestic_alias["stock_code"]
        if not stock_name:
            stock_name = domestic_alias["corp_name"]

    if not stock_name:
        resolved = await foreign.resolve_name(stock_code)
        if resolved:
            stock_name = resolved
        else:
            raise HTTPException(status_code=400, detail="종목명을 입력해 주세요.")

    quantity = payload.get("quantity")
    avg_price = payload.get("avg_price")
    if quantity is None or avg_price is None:
        raise HTTPException(status_code=400, detail="수량과 매입가를 입력해 주세요.")

    try:
        quantity = float(quantity)
        avg_price = float(avg_price)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="수량과 매입가는 숫자여야 합니다.")

    if quantity == 0:
        raise HTTPException(status_code=400, detail="수량은 0이 아닌 값이어야 합니다.")
    if abs(quantity) > 1_000_000_000:
        raise HTTPException(status_code=400, detail="수량이 너무 큽니다.")
    if avg_price < 0:
        raise HTTPException(status_code=400, detail="매입가는 0 이상이어야 합니다.")
    if avg_price > 1_000_000_000_000:
        raise HTTPException(status_code=400, detail="매입가가 너무 큽니다.")

    currency = str(payload.get("currency") or "").upper()
    if not currency:
        if _is_cash_asset(stock_code):
            currency = stock_code.replace("CASH_", "")
        elif _is_korean_stock(stock_code) or _is_special_asset(stock_code):
            currency = "KRW"
        else:
            currency = await foreign.detect_currency(stock_code)
    group_name = str(payload.get("group_name") or "").strip() or None
    if group_name:
        groups = await portfolio_repo.get_portfolio_groups(user["google_sub"])
        if not any(g["group_name"] == group_name for g in groups):
            raise HTTPException(status_code=400, detail=f"존재하지 않는 그룹명입니다: {group_name}")
    benchmark_code = str(payload.get("benchmark_code") or "").strip() or None
    # 등록일자 — accept "YYYY-MM-DD" from the UI edit form. Store as full
    # ISO so other columns (created_at DESC ordering) keep working, but
    # parse strictly so a malformed string can't overwrite the field
    # with garbage. None / empty string → leave existing value alone
    # (handled by portfolio_repo.save_portfolio_item default semantics).
    created_at_raw = str(payload.get("created_at") or "").strip()
    created_at: str | None = None
    if created_at_raw:
        try:
            from datetime import date as _date
            parsed = _date.fromisoformat(created_at_raw[:10])
            # Reconstruct as ISO datetime at 00:00 so downstream code that
            # expects datetime.fromisoformat still works.
            created_at = parsed.isoformat() + "T00:00:00"
        except ValueError:
            raise HTTPException(status_code=400, detail="등록일자는 YYYY-MM-DD 형식이어야 합니다.")

    # 목표가 (수동 override). target_price_formula 은 숫자 문자열 또는
    # BPS/EPS/DPS/보유지분/본주가격/매입가 기반 수식을 받는다. 빈 값은
    # 자동 계산으로 복귀한다. 기존 target_price payload 도 호환 유지.
    target_price_kwarg: dict = {}
    if "target_price_formula" in payload:
        try:
            parsed_target = _parse_target_input(payload.get("target_price_formula"))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        target_price_kwarg["target_price"] = parsed_target.price
        target_price_kwarg["target_price_formula"] = parsed_target.formula
        target_price_kwarg["target_price_disabled"] = False
        if parsed_target.formula:
            target_price_kwarg["target_price"] = await _resolve_target_formula_price(
                stock_code,
                parsed_target.formula,
                avg_price,
            )
    elif "target_price" in payload:
        raw_tp = payload.get("target_price")
        if raw_tp is None or (isinstance(raw_tp, str) and raw_tp.strip() == ""):
            target_price_kwarg["target_price"] = None
            target_price_kwarg["target_price_formula"] = None
        else:
            try:
                tp_val = float(raw_tp)
            except (TypeError, ValueError):
                raise HTTPException(status_code=400, detail="목표가는 숫자여야 합니다.")
            if tp_val < 0:
                raise HTTPException(status_code=400, detail="목표가는 0 이상이어야 합니다.")
            target_price_kwarg["target_price"] = tp_val
            target_price_kwarg["target_price_formula"] = None
    # target_price_disabled — True 면 자동 계산도 bypass, UI 는 '-'.
    if "target_price_disabled" in payload:
        raw_d = payload.get("target_price_disabled")
        target_price_kwarg["target_price_disabled"] = bool(raw_d)
        if bool(raw_d):
            target_price_kwarg["target_price_formula"] = None

    result = await portfolio_repo.save_portfolio_item(
        user["google_sub"], stock_code, stock_name, quantity, avg_price,
        currency, group_name, benchmark_code, created_at,
        **target_price_kwarg,
    )

    # 신규 해외 종목이면 yfinance 배당을 백그라운드로 fetch. 기존 동일
    # 코드에 대해 이미 foreign_dividends row 가 있으면 (auto/manual 무관)
    # 재 fetch 안 함 — 관리자 수동 override 보호 + 불필요한 yfinance 호출
    # 방지. fire-and-forget 이므로 PUT 응답 지연에 영향 없음. 실패 시
    # 로그만 남기고 포트폴리오 저장 자체는 성공 유지.
    try:
        if (not _is_korean_stock(stock_code)
                and not _is_cash_asset(stock_code)
                and stock_code not in _SPECIAL_ASSETS):
            existing_div = await foreign_dividends_repo.get_foreign_dividend(stock_code)
            if existing_div is None:
                async def _bg_fetch_dividend(code: str):
                    try:
                        import foreign_dividends
                        await foreign_dividends.refresh_foreign_dividends([code])
                    except Exception as exc:
                        logger.warning("auto foreign dividend fetch failed (%s): %s", code, exc)
                task = asyncio.create_task(_bg_fetch_dividend(stock_code))
                # Attach done callback so "Task exception was never retrieved"
                # doesn't pollute logs — the inner try/except already swallows.
                task.add_done_callback(lambda t: t.exception())
    except Exception as exc:
        logger.warning("foreign dividend dispatch guard failed (%s): %s", stock_code, exc)

    dividends.schedule_for_portfolio([stock_code])
    return {"ok": True, **result}


@router.put("/api/portfolio/{stock_code}/benchmark")
async def update_benchmark(stock_code: str, request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    stock_code = _normalize_portfolio_code(stock_code)
    benchmark_code = _normalize_portfolio_code(str(payload.get("benchmark_code") or "")) or None
    updated = await portfolio_repo.update_portfolio_benchmark(user["google_sub"], stock_code, benchmark_code)
    if not updated:
        raise HTTPException(status_code=404, detail="포트폴리오 종목을 찾을 수 없습니다.")
    # Return the effective benchmark and its quote without blocking the edit UI
    # on slow upstream index/FX sources.
    effective = benchmark_code or _resolve_default_benchmark_fast(stock_code)
    try:
        bq = await asyncio.wait_for(_fetch_benchmark_quote(effective), timeout=_BENCHMARK_ENDPOINT_ITEM_TIMEOUT)
    except Exception:
        bq = _cached_benchmark_quote(effective, allow_stale=True) or {}
    corp_code_table = await cache.load_corp_code_table()
    name = _resolve_benchmark_name_from_code_table(effective, None, corp_code_table)
    return {"ok": True, "benchmark_code": benchmark_code, "effective_benchmark": effective, "benchmark_name": name, "benchmark_quote": bq}


@router.get("/api/portfolio/benchmark-quotes")
async def get_benchmark_quotes(request: Request):
    """Fetch all unique benchmark quotes for the user's portfolio."""
    user = _require_user(await get_current_user(request))
    items = await portfolio_repo.get_portfolio(user["google_sub"])
    benchmark_codes = set()
    for item in items:
        bc = item.get("benchmark_code") or _resolve_default_benchmark_fast(item["stock_code"])
        if bc:
            benchmark_codes.add(bc)
    corp_code_table = await cache.load_corp_code_table()

    async def _fetch_one(bc):
        name = _resolve_benchmark_name_from_code_table(bc, items, corp_code_table)
        try:
            bq = await asyncio.wait_for(
                _fetch_benchmark_quote(bc),
                timeout=_BENCHMARK_ENDPOINT_ITEM_TIMEOUT,
            )
            return bc, {**bq, "name": name}
        except Exception:
            bq = _cached_benchmark_quote(bc, allow_stale=True) or {}
            return bc, {**bq, "name": name}

    codes = list(benchmark_codes)
    pairs = await asyncio.gather(*[_fetch_one(bc) for bc in codes], return_exceptions=True)
    response = {}
    for bc, pair in zip(codes, pairs):
        if isinstance(pair, BaseException):
            name = _resolve_benchmark_name_from_code_table(bc, items, corp_code_table)
            bq = _cached_benchmark_quote(bc, allow_stale=True) or {}
            response[bc] = {**bq, "name": name}
            continue
        code, data = pair
        response[code] = data
    return response


@router.delete("/api/portfolio/{stock_code}")
async def delete_portfolio_item(stock_code: str, request: Request):
    user = _require_user(await get_current_user(request))
    stock_code = _normalize_portfolio_code(stock_code)
    deleted = await portfolio_repo.delete_portfolio_item(user["google_sub"], stock_code)
    if not deleted:
        raise HTTPException(status_code=404, detail="포트폴리오에 없는 종목입니다.")
    return {"ok": True}


@router.post("/api/portfolio/bulk")
async def bulk_import(request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    mode = str(payload.get("mode", "add")).strip()
    rows = payload.get("items")
    if not isinstance(rows, list) or not rows:
        raise HTTPException(status_code=400, detail="등록할 종목이 없습니다.")

    # Validate all rows first
    parsed = []
    errors = []
    for i, row in enumerate(rows):
        code = _normalize_portfolio_code(str(row.get("stock_code") or ""))
        if not code:
            errors.append(f"행 {i+1}: 종목코드가 비어 있습니다.")
            continue
        try:
            qty = float(row.get("quantity", 0))
            price = float(row.get("avg_price", 0))
        except (TypeError, ValueError):
            errors.append(f"행 {i+1} ({code}): 수량/매입가가 올바르지 않습니다.")
            continue
        if qty == 0:
            errors.append(f"행 {i+1} ({code}): 수량은 0이 아닌 값이어야 합니다.")
            continue
        parsed.append({"stock_code": code, "quantity": qty, "avg_price": price})

    if errors:
        raise HTTPException(status_code=400, detail="\n".join(errors))

    # Resolve names concurrently
    async def resolve(item):
        name = await foreign.resolve_name(item["stock_code"])
        return {**item, "stock_name": name or item["stock_code"]}

    resolved = await asyncio.gather(*(resolve(p) for p in parsed))

    # Resolve currencies
    for item in resolved:
        code = item["stock_code"]
        item["currency"] = "KRW" if _is_korean_stock(code) or _is_special_asset(code) else await foreign.detect_currency(code)

    if mode == "replace":
        await portfolio_repo.replace_portfolio(user["google_sub"], resolved)
    else:
        for item in resolved:
            await portfolio_repo.save_portfolio_item(
                user["google_sub"], item["stock_code"], item["stock_name"], item["quantity"], item["avg_price"], item["currency"],
            )
    dividends.schedule_for_portfolio([item["stock_code"] for item in resolved])

    return {"ok": True, "imported": len(resolved), "mode": mode}


@router.get("/api/portfolio/resolve-name")
async def resolve_name(code: str = Query(..., min_length=1)):
    return await names.resolve_portfolio_name(code)


# --- NAV / Snapshots / Cashflows ---

@router.get("/api/portfolio/prev-day-snapshot")
async def get_prev_day_snapshot(request: Request):
    user = _require_user(await get_current_user(request))
    baseline_date = _portfolio_today_baseline_date()
    db = await cache.get_db()
    # Latest 22:00 settlement snapshot for the active Today window.
    cursor = await db.execute(
        "SELECT date, total_value, fx_usdkrw, nav FROM portfolio_snapshots WHERE google_sub = ? AND date <= ? ORDER BY date DESC LIMIT 1",
        (user["google_sub"], baseline_date),
    )
    snap_row = await cursor.fetchone()
    total_value = snap_row["total_value"] if snap_row else None
    fx_usdkrw = snap_row["fx_usdkrw"] if snap_row else None
    prev_nav = snap_row["nav"] if snap_row else None
    # Per-stock snapshots
    stock_snapshots = await snapshots_repo.get_stock_snapshots_by_date(user["google_sub"], baseline_date)
    stock_values = {s["stock_code"]: s["market_value"] for s in stock_snapshots}
    # Net cashflow not yet reflected in snapshot. Use created_at > snapshot
    # date 22:00 (snapshot runs at 22:00) to catch cashflows entered after
    # the snapshot was taken, regardless of their nominal date.
    snap_date = snap_row["date"] if snap_row else None
    if snap_date:
        created_after = f"{snap_date}T22:00:00"
    else:
        created_after = baseline_date
    cursor2 = await db.execute(
        "SELECT id, type, amount, nav_at_time, units_change, created_at FROM portfolio_cashflows WHERE google_sub = ? AND created_at > ? ORDER BY created_at ASC, id ASC",
        (user["google_sub"], created_after),
    )
    today_net_cashflow = 0.0
    today_cashflows_by_stock: dict[str, float] = {}
    today_cashflows: list[dict] = []
    for row in await cursor2.fetchall():
        signed_amount = 0.0
        if row["type"] == "deposit":
            signed_amount = row["amount"]
        elif row["type"] == "withdrawal":
            signed_amount = -row["amount"]
        today_net_cashflow += signed_amount
        if signed_amount:
            today_cashflows.append({
                "id": row["id"],
                "type": row["type"],
                "amount": row["amount"],
                "nav_at_time": row["nav_at_time"],
                "signed_amount": signed_amount,
                "units_change": row["units_change"],
                "created_at": row["created_at"],
            })
        if signed_amount:
            # Portfolio cashflow mutations are materialized through CASH_KRW.
            # Expose the attribution so filtered Today cards can remove
            # deposits/withdrawals from group return instead of treating cash
            # movement as investment performance.
            today_cashflows_by_stock["CASH_KRW"] = today_cashflows_by_stock.get("CASH_KRW", 0.0) + signed_amount
    return {
        # date is the baseline the UI's Today card compares against. Was
        # missing from the response, which made the frontend's baseline
        # label silently fall back to "기준 없음" while the numerical
        # value was being computed against nav/total_value anyway —
        # label and value disagreed.
        "date": snap_date,
        "total_value": total_value,
        "fx_usdkrw": fx_usdkrw,
        "nav": prev_nav,
        "stock_values": stock_values,
        "today_net_cashflow": today_net_cashflow,
        "today_cashflows_by_stock": today_cashflows_by_stock,
        "today_cashflows": today_cashflows,
    }


@router.get("/api/portfolio/month-end-value")
async def get_month_end_value(request: Request):
    user = _require_user(await get_current_user(request))
    month_end = (date.today().replace(day=1) - timedelta(days=1)).isoformat()
    snapshot = await snapshots_repo.get_month_end_snapshot(user["google_sub"])
    stock_snapshots = await snapshots_repo.get_stock_snapshots_by_date(user["google_sub"], month_end)
    result = dict(snapshot) if snapshot else {}
    result["stock_values"] = {s["stock_code"]: s["market_value"] for s in stock_snapshots}
    return result


@router.get("/api/portfolio/year-start-value")
async def get_year_start_value(request: Request):
    user = _require_user(await get_current_user(request))
    snapshot = await snapshots_repo.get_year_start_snapshot(user["google_sub"])
    result = dict(snapshot) if snapshot else {}
    if snapshot and snapshot.get("date"):
        stock_snapshots = await snapshots_repo.get_stock_snapshots_by_date(user["google_sub"], snapshot["date"])
        result["stock_values"] = {s["stock_code"]: s["market_value"] for s in stock_snapshots}
    else:
        result["stock_values"] = {}
    return result


@router.get("/api/portfolio/nav-history")
async def get_nav_history(request: Request):
    user = _require_user(await get_current_user(request))
    return await snapshots_repo.get_nav_history(user["google_sub"])


@router.get("/api/portfolio/group-weight-history")
async def get_group_weight_history(request: Request):
    user = _require_user(await get_current_user(request))
    return await snapshots_repo.get_group_weight_history(user["google_sub"])


@router.get("/api/portfolio/group-constituent-history")
async def get_group_constituent_history(request: Request, group: str = Query(..., min_length=1)):
    user = _require_user(await get_current_user(request))
    return await snapshots_repo.get_group_constituent_history(user["google_sub"], group.strip())


@router.get("/api/portfolio/benchmark-history")
async def get_benchmark_history(code: str = Query(...), start: str = Query(...)):
    """Return daily close prices for a benchmark index, served from the
    local `benchmark_daily` table. Performs a one-shot lazy backfill
    against yfinance the first time a code is requested, or when the
    requested `start` predates what we currently have.

    After that, the nightly `snapshot_nav.update_benchmark_today()` hook
    keeps the table fresh so normal requests hit SQLite only (~ms) and
    are immune to yfinance outages.
    """
    import asyncio
    import logging

    import benchmark_history

    code_up = code.upper()
    if code_up not in benchmark_history.YF_TICKER:
        raise HTTPException(status_code=400, detail=f"Unknown benchmark: {code}")

    logger = logging.getLogger(__name__)

    # Lazy backfill — no-op if DB already covers `start` or further back.
    # Failures here are swallowed (logged) inside backfill_benchmark; we
    # still try to serve whatever rows we have rather than 502-ing.
    try:
        await asyncio.wait_for(benchmark_history.backfill_benchmark(code_up, start), timeout=10)
    except asyncio.TimeoutError:
        logger.warning("Benchmark backfill timed out (%s start=%s); serving cached rows only", code_up, start)

    rows = await benchmark_repo.get_benchmark_rows(code_up, start=start)
    return rows


@router.get("/api/portfolio/intraday")
async def get_intraday(request: Request):
    user = _require_user(await get_current_user(request))
    axis_start, axis_end = _intraday_axis_window()
    baseline_date = axis_start[:10]
    points = await snapshots_repo.get_intraday_snapshots_between(user["google_sub"], axis_start, axis_end)
    # Prepend the active 22:00 settlement snapshot as the zero baseline.
    # The frontend maps x by elapsed time from this timestamp, so the API
    # should expose the real axis start instead of a synthetic midnight marker.
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT total_value FROM portfolio_snapshots WHERE google_sub = ? AND date <= ? ORDER BY date DESC LIMIT 1",
        (user["google_sub"], baseline_date),
    )
    row = await cursor.fetchone()
    if row and row["total_value"]:
        points = [{"ts": axis_start, "total_value": row["total_value"]}] + points
    return points


@router.get("/api/portfolio/cashflows")
async def get_cashflows(request: Request):
    user = _require_user(await get_current_user(request))
    return await snapshots_repo.get_cashflows(user["google_sub"])


@router.post("/api/portfolio/cashflows")
async def add_cashflow(request: Request, payload: dict = Body(...)):
    user = _require_user(await get_current_user(request))
    cf_type = str(payload.get("type") or "").strip()
    if cf_type not in ("deposit", "withdrawal"):
        raise HTTPException(status_code=400, detail="type은 deposit 또는 withdrawal이어야 합니다.")
    try:
        amount = float(payload.get("amount"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="금액은 숫자여야 합니다.")
    if amount <= 0:
        raise HTTPException(status_code=400, detail="금액은 0보다 커야 합니다.")
    cf_date = str(payload.get("date") or "").strip()
    if not cf_date:
        from datetime import date
        cf_date = date.today().isoformat()
    memo = str(payload.get("memo") or "").strip() or None

    google_sub = user["google_sub"]

    # Get latest NAV for units calculation
    latest = await snapshots_repo.get_latest_snapshot(google_sub)
    nav_at_time = latest["nav"] if latest else 1000.0
    units_change = amount / nav_at_time
    if cf_type == "withdrawal":
        units_change = -units_change

    try:
        result = await snapshots_repo.add_cashflow_and_sync_cash(
            google_sub,
            cf_date,
            cf_type,
            amount,
            memo,
            nav_at_time,
            units_change,
        )
    except snapshots_repo.CashflowBalanceError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"원화 잔액이 부족합니다. (잔액: {exc.balance:,.0f}원, 출금액: {exc.amount:,.0f}원)",
        )

    return {"ok": True, **result}


@router.delete("/api/portfolio/cashflows/{cf_id}")
async def delete_cashflow(cf_id: int, request: Request):
    user = _require_user(await get_current_user(request))
    google_sub = user["google_sub"]
    deleted = await snapshots_repo.delete_cashflow_and_sync_cash(google_sub, cf_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="현금흐름을 찾을 수 없습니다.")

    return {"ok": True}


# ---------------------------------------------------------------------------
# AI Portfolio Analysis (OpenRouter)
# ---------------------------------------------------------------------------
# Domain logic (model selection, prompt assembly, OpenRouter streaming call,
# usage-ledger writes) lives in services/portfolio/ai_analysis.py. The routes
# below keep only the HTTP shell: auth, typed-error -> HTTPException mapping
# and the SSE framing of the domain events the service yields.


@router.get("/api/portfolio/ai-models")
async def ai_model_list(request: Request):
    """Return available OpenRouter models (for admin model picker)."""
    _require_user(await get_current_user(request))
    return await ai_analysis.list_models()


@router.post("/api/portfolio/ai-analysis")
async def ai_portfolio_analysis(request: Request, payload: dict = Body(default={})):
    user = _require_user(await get_current_user(request))
    try:
        ctx = await ai_analysis.prepare_analysis(payload, user)
    except ai_analysis.MissingAPIKeyError:
        raise HTTPException(status_code=500, detail="AI API 키가 설정되지 않았습니다.")
    except ai_analysis.EmptyPortfolioError:
        raise HTTPException(status_code=400, detail="포트폴리오가 비어 있습니다.")

    async def _stream():
        async for event in ai_analysis.stream_analysis(ctx, is_disconnected=request.is_disconnected):
            yield f"data: {json.dumps(event)}\n\n"

    return StreamingResponse(_stream(), media_type="text/event-stream")
