from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
from contextlib import AsyncExitStack, asynccontextmanager

import httpx
from fastapi import FastAPI

from core.config import AppSettings, load_environment
from core.errors import AppError
from core.runtime import RuntimeState, sd_notify, watchdog_loop

logger = logging.getLogger(__name__)


async def _stop_background_tasks(tasks: list[tuple[asyncio.Task, asyncio.Event | None]], *, timeout: float = 5.0):
    """중지 신호를 함께 보내고, 기한 뒤 취소한 작업까지 종료를 기다린다."""
    for task, stop in tasks:
        if stop is None:
            task.cancel()
        else:
            stop.set()
    if not tasks:
        return
    running = [task for task, _ in tasks]
    _, pending = await asyncio.wait(running, timeout=timeout)
    for task in pending:
        task.cancel()
    results = await asyncio.gather(*running, return_exceptions=True)
    for task, result in zip(running, results):
        if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
            logger.error("백그라운드 작업 종료 오류(%s): %s", task.get_name(), result,
                         exc_info=(type(result), result, result.__traceback__))


@asynccontextmanager
async def app_lifespan(app: FastAPI, settings: AppSettings, runtime: RuntimeState):
    """Application startup/shutdown orchestration.

    Feature modules are imported inside the lifecycle so this module remains
    infrastructure-only and does not freeze feature configuration before
    `load_environment()` has had a chance to run.
    """
    load_environment(settings.project_root)

    import ai_config
    import close_price_client
    import dart_client
    import kis_key_manager
    import kis_proxy_client
    import kis_ws_manager
    import observability

    # 공유 httpx 클라이언트 매니저 — ``async with httpx.AsyncClient()`` 패턴을
    # 쓰던 외부 호출이 core/http.get_http_client() 로 전환한다. kis_proxy/
    # close_price 는 자체 rate-limit 회로를 가진 싱글톤을 유지한다.
    from core import http as http_manager
    from repositories import bootstrap, corp_codes
    from repositories import users as users_repo
    from services.portfolio import insights as portfolio_insights

    kis_key_manager.load_keys()

    async with AsyncExitStack() as stack:
        stack.push_async_callback(http_manager.close_http_clients)
        await http_manager.init_http_clients()
        stack.push_async_callback(kis_proxy_client.close_client)
        await kis_proxy_client.init_client()
        stack.push_async_callback(close_price_client.close_client)
        await close_price_client.init_client()
        stack.push_async_callback(bootstrap.close_db)
        await bootstrap.init_db()
        stack.push_async_callback(observability.flush_events)
        await ai_config.migrate_legacy_model_defaults()
        await users_repo.delete_expired_sessions()

        (settings.project_root / "data" / "pdf_cache").mkdir(parents=True, exist_ok=True)
        try:
            needs_corp_refresh = not await corp_codes.is_corp_codes_loaded() or await corp_codes.corp_codes_need_refresh()
        except (sqlite3.Error, AppError, httpx.HTTPError, OSError, ValueError) as exc:
            logger.error("corp_codes status check failed: %s", exc)
            needs_corp_refresh = False
        if needs_corp_refresh:
            logger.info("Refreshing corp_codes from DART")
            try:
                codes = await asyncio.wait_for(dart_client.fetch_corp_codes(), timeout=45)
                await corp_codes.save_corp_codes(codes)
                logger.info("Saved %d corp codes", len(codes))
            except asyncio.TimeoutError:
                logger.error("corp_codes download timed out; continuing with cached data")
            except (sqlite3.Error, AppError, httpx.HTTPError, OSError, ValueError) as exc:
                logger.error("corp_codes download failed: %s", exc)

        stack.push_async_callback(kis_ws_manager.stop_all)
        tasks: list[tuple[asyncio.Task, asyncio.Event | None]] = []
        app.state.background_tasks = tasks
        stack.push_async_callback(_stop_background_tasks, tasks)
        stack.callback(sd_notify, "STOPPING=1")
        watchdog_task = asyncio.create_task(watchdog_loop(runtime), name="watchdog")
        tasks.append((watchdog_task, None))

        insight_warmup_task = None
        if os.environ.get("PORTFOLIO_INSIGHT_STARTUP_WARMUP", "0") == "1":
            insight_warmup_task = asyncio.create_task(
                portfolio_insights.warm_asset_insight_common(
                    initial_delay_seconds=float(os.environ.get("PORTFOLIO_INSIGHT_WARMUP_DELAY_S", "90"))
                )
            )

            tasks.append((insight_warmup_task, None))

        import wiki_ingestion
        wiki_stop = asyncio.Event()
        wiki_interval = float(os.environ.get("WIKI_INGEST_INTERVAL_S", "0"))
        wiki_task: asyncio.Task | None = None
        if wiki_interval > 0:
            wiki_task = asyncio.create_task(
                wiki_ingestion.run_background_loop(
                    wiki_stop,
                    interval_seconds=wiki_interval,
                    initial_delay_seconds=float(os.environ.get("WIKI_INGEST_INITIAL_DELAY_S", "60")),
                )
            )

            tasks.append((wiki_task, wiki_stop))

        import dart_report_review
        dart_review_stop = asyncio.Event()
        dart_review_interval = float(os.environ.get("DART_REVIEW_INTERVAL_S", "0"))
        dart_review_task: asyncio.Task | None = None
        if dart_review_interval > 0:
            dart_review_task = asyncio.create_task(
                dart_report_review.run_background_loop(
                    dart_review_stop,
                    interval_seconds=dart_review_interval,
                    target_limit=int(os.environ.get("DART_REVIEW_TARGET_LIMIT", "12")),
                    initial_delay_seconds=float(os.environ.get("DART_REVIEW_INITIAL_DELAY_S", "90")),
                )
            )

            tasks.append((dart_review_task, dart_review_stop))

        obs_stop = asyncio.Event()
        obs_task = asyncio.create_task(
            observability.run_prune_loop(
                obs_stop,
                interval_seconds=float(os.environ.get("OBS_PRUNE_INTERVAL_S", str(6 * 3600))),
                max_age_days=int(os.environ.get("OBS_MAX_AGE_DAYS", "30")),
                max_rows=int(os.environ.get("OBS_MAX_ROWS", "100000")),
            )
        )

        tasks.append((obs_task, obs_stop))

        # Portfolio alert evaluation loop. Credentials are per-user (stored in
        # notification_channels), so there is no server-wide bot poller — the loop
        # just needs NOTIFY_ALERT_INTERVAL_S > 0; evaluate_user skips users without
        # an active channel.
        from services.notifications import engine as notify_engine

        notify_stop = asyncio.Event()
        alert_interval = float(os.environ.get("NOTIFY_ALERT_INTERVAL_S", "0"))
        notify_alert_task: asyncio.Task | None = None
        if alert_interval > 0:
            notify_alert_task = asyncio.create_task(
                notify_engine.run_alert_loop(
                    notify_stop,
                    interval_seconds=alert_interval,
                    initial_delay_seconds=float(os.environ.get("NOTIFY_ALERT_INITIAL_DELAY_S", "30")),
                )
            )
            tasks.append((notify_alert_task, notify_stop))

        from services.market import indicator_health

        indicator_stop = asyncio.Event()
        tasks.append((asyncio.create_task(indicator_health.run_loop(indicator_stop), name="market-indicators"), indicator_stop))

        sd_notify("READY=1")
        yield
