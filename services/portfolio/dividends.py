from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime

import cache
import dart_client
import stock_price
from services.portfolio.identifiers import (
    common_stock_code,
    is_korean_stock,
    is_preferred_stock,
    normalize_portfolio_code,
)


logger = logging.getLogger(__name__)

DIVIDEND_WARMUP_TTL = 6 * 60 * 60
WARMUP_START_DELAY = 10  # seconds before a scheduled warmup hits upstreams


def dividend_warmup_targets(code: str) -> list[str]:
    normalized = normalize_portfolio_code(code)
    if not is_korean_stock(normalized):
        return []
    if is_preferred_stock(normalized):
        common = common_stock_code(normalized)
        return [normalized, common] if common != normalized else [normalized]
    return [normalized]


def due_dividend_warmup_targets(
    codes: list[str],
    now: float,
    last_warmup: dict[str, float],
    *,
    running_codes: set[str] | None = None,
    ttl_seconds: float = DIVIDEND_WARMUP_TTL,
) -> list[str]:
    running_codes = running_codes or set()
    seen: set[str] = set()
    targets: list[str] = []
    for code in codes:
        for target in dividend_warmup_targets(code):
            if target not in seen:
                seen.add(target)
                targets.append(target)

    due: list[str] = []
    for code in targets:
        if code in running_codes:
            continue
        last = last_warmup.get(code, 0)
        if now - last > ttl_seconds:
            due.append(code)
    return due


# --- Warmup state machine ---
#
# Moved from routes/portfolio.py: the module-level task registry plus the
# scheduling/refresh logic that keeps the portfolio dividend cache warm in the
# background. The TTL/dedup rules above feed directly into this scheduler, so
# both live in this module.

_warmup_last: dict[str, float] = {}
_warmup_tasks: dict[str, asyncio.Task] = {}


async def refresh_domestic_dividend_from_dart(code: str) -> int:
    corp_code = await cache.get_corp_code(code)
    if not corp_code:
        return 0
    current_year = datetime.now().year
    dividends = await dart_client.fetch_dividend_per_share_by_year(
        corp_code,
        start_year=max(current_year - 3, dart_client.DART_ANNUAL_DATA_START_YEAR),
        end_year=current_year - 1,
    )
    return await cache.upsert_market_dividends(code, dividends)


async def warm_market_data_for_dividend(code: str) -> None:
    code = normalize_portfolio_code(code)
    try:
        updated = await refresh_domestic_dividend_from_dart(code)
        if updated:
            logger.info("Portfolio DART dividend warmup completed (%s, %d rows)", code, updated)
            return
        latest_dividend_years = await cache.get_latest_dividend_years([code])
        if latest_dividend_years.get(code, 0) >= datetime.now().year - 1:
            return
        fin_data = await cache.get_financial_data(code)
        corp_code = await cache.get_corp_code(code)
        refreshed = await stock_price.fetch_market_data(code, fin_data, corp_code=corp_code)
        if refreshed:
            await cache.save_market_data(code, refreshed)
            logger.info("Portfolio dividend market-data warmup completed (%s, %d rows)", code, len(refreshed))
    except Exception as exc:
        logger.warning("Portfolio dividend market-data warmup failed (%s): %s", code, exc)
    finally:
        _warmup_tasks.pop(code, None)


def _consume_warmup_result(code: str, task: asyncio.Task) -> None:
    _warmup_tasks.pop(code, None)
    try:
        task.exception()
    except asyncio.CancelledError:
        pass


def running_warmup_codes() -> set[str]:
    return {
        code
        for code, task in _warmup_tasks.items()
        if task and not task.done()
    }


def start_warmup_task(code: str, now: float) -> asyncio.Task | None:
    _warmup_last[code] = now
    try:
        async def _delayed_warmup():
            await asyncio.sleep(WARMUP_START_DELAY)
            await warm_market_data_for_dividend(code)

        task = asyncio.create_task(_delayed_warmup())
    except RuntimeError:
        return None
    _warmup_tasks[code] = task
    task.add_done_callback(lambda t, c=code: _consume_warmup_result(c, t))
    return task


def schedule_for_portfolio(codes: list[str]) -> None:
    now = time.monotonic()
    due = due_dividend_warmup_targets(codes, now, _warmup_last, running_codes=running_warmup_codes())
    for code in due:
        start_warmup_task(code, now)


async def warm_for_response(codes: list[str], timeout: float = 2.5) -> None:
    now = time.monotonic()
    due = due_dividend_warmup_targets(codes, now, _warmup_last, running_codes=running_warmup_codes())
    tasks = [
        task
        for code in due
        if (task := start_warmup_task(code, now)) is not None
    ]
    if not tasks:
        return
    done, pending = await asyncio.wait(tasks, timeout=timeout)
    for task in done:
        try:
            task.exception()
        except asyncio.CancelledError:
            pass
    if pending:
        logger.info("Portfolio dividend warmup still running in background (%d pending)", len(pending))


def reset_warmup_state() -> None:
    """Test helper: drop pending warmup bookkeeping (does not cancel tasks)."""
    _warmup_last.clear()
    _warmup_tasks.clear()
