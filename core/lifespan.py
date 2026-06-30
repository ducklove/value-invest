from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI

from core.config import AppSettings, load_environment
from core.runtime import RuntimeState, sd_notify, watchdog_loop

logger = logging.getLogger(__name__)


@asynccontextmanager
async def app_lifespan(app: FastAPI, settings: AppSettings, runtime: RuntimeState):
    """Application startup/shutdown orchestration.

    Feature modules are imported inside the lifecycle so this module remains
    infrastructure-only and does not freeze feature configuration before
    `load_environment()` has had a chance to run.
    """
    load_environment(settings.project_root)

    import ai_config
    import cache
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
    from repositories import users as users_repo
    from services.portfolio import insights as portfolio_insights

    kis_key_manager.load_keys()

    await http_manager.init_http_clients()
    await kis_proxy_client.init_client()
    await close_price_client.init_client()
    await cache.init_db()
    await ai_config.migrate_legacy_model_defaults()
    await users_repo.delete_expired_sessions()

    (settings.project_root / "data" / "pdf_cache").mkdir(parents=True, exist_ok=True)
    try:
        needs_corp_refresh = not await cache.is_corp_codes_loaded() or await cache.corp_codes_need_refresh()
    except Exception as exc:
        logger.error("corp_codes status check failed: %s", exc)
        needs_corp_refresh = False
    if needs_corp_refresh:
        logger.info("Refreshing corp_codes from DART")
        try:
            codes = await asyncio.wait_for(dart_client.fetch_corp_codes(), timeout=45)
            await cache.save_corp_codes(codes)
            logger.info("Saved %d corp codes", len(codes))
        except asyncio.TimeoutError:
            logger.error("corp_codes download timed out; continuing with cached data")
        except Exception as exc:
            logger.error("corp_codes download failed: %s", exc)

    sd_notify("READY=1")
    watchdog_task = asyncio.create_task(watchdog_loop(runtime))

    insight_warmup_task = None
    if os.environ.get("PORTFOLIO_INSIGHT_STARTUP_WARMUP", "0") == "1":
        insight_warmup_task = asyncio.create_task(
            portfolio_insights.warm_asset_insight_common(
                initial_delay_seconds=float(os.environ.get("PORTFOLIO_INSIGHT_WARMUP_DELAY_S", "90"))
            )
        )

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

    obs_stop = asyncio.Event()
    obs_task = asyncio.create_task(
        observability.run_prune_loop(
            obs_stop,
            interval_seconds=float(os.environ.get("OBS_PRUNE_INTERVAL_S", str(6 * 3600))),
            max_age_days=int(os.environ.get("OBS_MAX_AGE_DAYS", "30")),
            max_rows=int(os.environ.get("OBS_MAX_ROWS", "100000")),
        )
    )

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
    try:
        yield
    finally:
        sd_notify("STOPPING=1")
        watchdog_task.cancel()
        if insight_warmup_task:
            insight_warmup_task.cancel()
            try:
                await insight_warmup_task
            except (asyncio.CancelledError, Exception):
                pass
        try:
            await watchdog_task
        except (asyncio.CancelledError, Exception):
            pass

        wiki_stop.set()
        if wiki_task is not None:
            try:
                await asyncio.wait_for(wiki_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                wiki_task.cancel()

        dart_review_stop.set()
        if dart_review_task is not None:
            try:
                await asyncio.wait_for(dart_review_task, timeout=5.0)
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                dart_review_task.cancel()

        obs_stop.set()
        try:
            await asyncio.wait_for(obs_task, timeout=2.0)
        except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
            obs_task.cancel()

        notify_stop.set()
        if notify_alert_task is not None:
            try:
                await asyncio.wait_for(notify_alert_task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                notify_alert_task.cancel()

        await kis_ws_manager.stop_all()
        await kis_proxy_client.close_client()
        await close_price_client.close_client()
        await http_manager.close_http_clients()
        await cache.close_db()

