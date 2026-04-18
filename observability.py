"""Unified in-app event log.

Subsystems (snapshot_nav, wiki_ingestion, openrouter calls, benchmark
ingest, kis_ws, etc.) push structured status rows here via `record_event`
so the admin dashboard can answer "what has actually been happening
lately" without parsing systemd journal.

Design goals:

* **Cheap at the hot path.** Every event is a single INSERT against a
  tiny table. Default mode is fire-and-forget via `record_event_nowait`
  — caller does not await disk I/O. Tests that want assertions use
  `record_event(..., wait=True)` to block until written.

* **Never break the caller.** Any exception during recording is logged
  and swallowed. Observability must not be the thing that takes the
  service down.

* **Bounded storage.** A TTL loop trims rows older than 30 days and
  clamps at 100k rows as a safety net.

* **Structured but flexible.** Each event carries (level, source, kind,
  stock_code?, details). `details` is free-form JSON so new subsystems
  don't need schema changes — the dashboard renderer is expected to
  know the shape for a given (source, kind).
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Any

import cache

logger = logging.getLogger(__name__)


VALID_LEVELS = ("info", "warning", "error")


def _sanitize_details(details: dict[str, Any] | None) -> str | None:
    """Serialize details to JSON. Falls back to str() on unencodable types
    so a stray datetime doesn't fail the whole event."""
    if details is None:
        return None
    try:
        return json.dumps(details, ensure_ascii=False, default=str)
    except Exception as exc:
        logger.warning("observability: details serialization failed: %s", exc)
        try:
            return json.dumps({"_unserialized": str(details)[:500]})
        except Exception:
            return None


async def record_event(
    source: str,
    kind: str,
    *,
    level: str = "info",
    stock_code: str | None = None,
    details: dict[str, Any] | None = None,
    wait: bool = False,
) -> None:
    """Append an event. Never raises.

    Callers in a hot path (per-quote fetchers, LLM calls) should leave
    `wait=False` — the actual DB write is fired off and awaited as a
    detached task. Tests and startup paths that need to read the row
    back immediately pass `wait=True`.
    """
    if level not in VALID_LEVELS:
        level = "info"
    details_json = _sanitize_details(details)

    async def _do_write() -> None:
        try:
            await cache.insert_system_event(
                level=level,
                source=source,
                kind=kind,
                stock_code=stock_code,
                details=details_json,
            )
        except Exception as exc:
            # Can't write the event? Don't retry, don't raise — just log
            # to stderr. The service must keep running.
            logger.warning(
                "observability: insert_system_event failed (source=%s kind=%s): %s",
                source, kind, exc,
            )

    if wait:
        await _do_write()
    else:
        # Detach but still attach a no-op error handler so "Task exception
        # was never retrieved" warnings don't spam journal.
        task = asyncio.create_task(_do_write())
        task.add_done_callback(lambda t: t.exception())


async def run_prune_loop(
    stop_event: asyncio.Event,
    *,
    interval_seconds: float = 6 * 3600.0,
    max_age_days: int = 30,
    max_rows: int = 100_000,
    initial_delay_seconds: float = 300.0,
) -> None:
    """Background pruner. Runs every `interval_seconds`; exits cleanly on
    `stop_event`. Default cadence (6h) is plenty — events don't accrue
    fast enough to need more frequent cleanup.
    """
    if initial_delay_seconds > 0:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=initial_delay_seconds)
            return
        except asyncio.TimeoutError:
            pass
    while not stop_event.is_set():
        try:
            deleted = await cache.prune_system_events(
                max_age_days=max_age_days, max_rows=max_rows,
            )
            if deleted:
                logger.info("observability: pruned %d old system_events rows", deleted)
        except Exception as exc:
            logger.warning("observability: prune loop iteration failed: %s", exc)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            return
        except asyncio.TimeoutError:
            continue


def iso_hours_ago(hours: float) -> str:
    """Small convenience for `summarize_system_events(since=...)`."""
    from datetime import timedelta
    return (datetime.now() - timedelta(hours=hours)).isoformat(timespec="seconds")


__all__ = [
    "VALID_LEVELS",
    "iso_hours_ago",
    "record_event",
    "run_prune_loop",
]
