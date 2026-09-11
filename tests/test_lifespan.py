"""실제 앱 조립·임시 DB를 사용하고 외부 어댑터만 격리한 수명주기 검사."""

import asyncio
import sqlite3
from unittest.mock import AsyncMock

import httpx
import pytest

import ai_config
import close_price_client
import dart_client
import dart_report_review
import kis_key_manager
import kis_proxy_client
import kis_ws_manager
import observability
import wiki_ingestion
from core import http, lifespan
from core.app_factory import create_app
from core.config import AppSettings
from repositories import bootstrap, corp_codes, db
from services.notifications import engine
from services.portfolio import insights


@pytest.fixture
def lifecycle_setup(tmp_path, monkeypatch):
    for directory in ("js", "css"):
        (tmp_path / "static" / directory).mkdir(parents=True)
    settings = AppSettings("development", tmp_path, "수명주기 검사", "", ("https://app.example",), True)
    monkeypatch.setattr(db, "DB_PATH", tmp_path / "test.db")
    for name in ("WIKI_INGEST_INTERVAL_S", "DART_REVIEW_INTERVAL_S", "NOTIFY_ALERT_INTERVAL_S",
                 "PORTFOLIO_INSIGHT_STARTUP_WARMUP"):
        monkeypatch.setenv(name, "0")
    calls = []

    def operation(name):
        async def run(*args, **kwargs):
            calls.append(name)
        return run

    monkeypatch.setattr(kis_key_manager, "load_keys", lambda: None)
    for module, init, close, label in (
        (http, "init_http_clients", "close_http_clients", "http"),
        (kis_proxy_client, "init_client", "close_client", "kis"),
        (close_price_client, "init_client", "close_client", "price"),
    ):
        monkeypatch.setattr(module, init, operation(f"open:{label}"))
        monkeypatch.setattr(module, close, operation(f"close:{label}"))
    monkeypatch.setattr(kis_ws_manager, "stop_all", operation("close:ws"))
    monkeypatch.setattr(corp_codes, "is_corp_codes_loaded", AsyncMock(return_value=True))
    monkeypatch.setattr(corp_codes, "corp_codes_need_refresh", AsyncMock(return_value=False))
    monkeypatch.setattr(lifespan, "sd_notify", calls.append)

    async def loop(stop, **kwargs):
        calls.append("loop:started")
        await stop.wait()
        calls.append("loop:stopped")

    for module, name in ((observability, "run_prune_loop"), (wiki_ingestion, "run_background_loop"),
                         (dart_report_review, "run_background_loop"), (engine, "run_alert_loop")):
        monkeypatch.setattr(module, name, loop)

    async def warmup(**kwargs):
        try:
            await asyncio.Event().wait()
        finally:
            calls.append("warmup:stopped")

    monkeypatch.setattr(insights, "warm_asset_insight_common", warmup)
    return settings, calls


async def test_real_app_starts_database_and_joins_all_background_tasks(lifecycle_setup, monkeypatch):
    settings, calls = lifecycle_setup
    for name in ("WIKI_INGEST_INTERVAL_S", "DART_REVIEW_INTERVAL_S", "NOTIFY_ALERT_INTERVAL_S",
                 "PORTFOLIO_INSIGHT_STARTUP_WARMUP"):
        monkeypatch.setenv(name, "1")
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        await asyncio.sleep(0)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://app.example") as client:
            assert (await client.get("/healthz")).status_code == 200
            assert (await client.get("/readyz")).json()["database"] == "ok"
        assert "READY=1" in calls
        assert len(app.state.background_tasks) == 7
        assert any(task.get_name() == "market-indicators" and not task.done() for task, _ in app.state.background_tasks)
    assert all(task.done() for task, _ in app.state.background_tasks)
    assert calls.count("loop:stopped") == 4
    assert "warmup:stopped" in calls
    assert calls.index("STOPPING=1") < calls.index("close:ws") < calls.index("close:http")
    assert db._conn is None and db._writer_conn is None


@pytest.mark.parametrize("stage,closed", [
    ("http", ["http"]), ("kis", ["kis", "http"]), ("price", ["price", "kis", "http"]),
    ("db", ["price", "kis", "http"]), ("migration", ["price", "kis", "http"]),
])
async def test_partial_startup_closes_every_acquired_resource(lifecycle_setup, monkeypatch, stage, closed):
    settings, calls = lifecycle_setup

    async def fail(*args, **kwargs):
        if stage == "db":
            await db.get_db()  # 초기화 중 연결은 열렸지만 스키마 생성은 실패한 상황.
        raise OSError("시작 실패 주입")

    module, name = {
        "http": (http, "init_http_clients"), "kis": (kis_proxy_client, "init_client"),
        "price": (close_price_client, "init_client"), "db": (bootstrap, "init_db"),
        "migration": (ai_config, "migrate_legacy_model_defaults"),
    }[stage]
    monkeypatch.setattr(module, name, fail)
    app = create_app(settings)
    with pytest.raises(OSError, match="시작 실패"):
        async with app.router.lifespan_context(app):
            pytest.fail("시작 실패 뒤 요청을 받아서는 안 됩니다.")
    assert [item.removeprefix("close:") for item in calls if item.startswith("close:")] == closed
    assert "READY=1" not in calls
    assert db._conn is None and db._writer_conn is None


async def test_late_startup_failure_cancels_already_created_tasks(lifecycle_setup, monkeypatch):
    settings, calls = lifecycle_setup
    monkeypatch.setenv("WIKI_INGEST_INTERVAL_S", "invalid")
    app = create_app(settings)
    with pytest.raises(ValueError):
        async with app.router.lifespan_context(app):
            pytest.fail("잘못된 설정으로 시작하면 안 됩니다.")
    assert all(task.done() for task, _ in app.state.background_tasks)
    assert "READY=1" not in calls
    assert "close:http" in calls


async def test_one_cleanup_failure_does_not_skip_remaining_resources(lifecycle_setup, monkeypatch):
    settings, calls = lifecycle_setup

    async def failed_close():
        raise OSError("종료 실패 주입")

    monkeypatch.setattr(close_price_client, "close_client", failed_close)
    app = create_app(settings)
    with pytest.raises(OSError, match="종료 실패"):
        async with app.router.lifespan_context(app):
            pass
    assert "close:kis" in calls and "close:http" in calls
    assert db._conn is None and db._writer_conn is None


async def test_corp_refresh_failure_keeps_cached_service_available(lifecycle_setup, monkeypatch):
    settings, calls = lifecycle_setup
    monkeypatch.setattr(corp_codes, "corp_codes_need_refresh", AsyncMock(return_value=True))
    monkeypatch.setattr(dart_client, "fetch_corp_codes", AsyncMock(side_effect=httpx.ConnectError("외부 장애")))
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        assert "READY=1" in calls


async def test_shutdown_cancels_and_awaits_unresponsive_task():
    entered, finished = asyncio.Event(), asyncio.Event()

    async def task_body():
        try:
            entered.set()
            await asyncio.Event().wait()
        finally:
            finished.set()

    task = asyncio.create_task(task_body())
    await entered.wait()
    await lifespan._stop_background_tasks([(task, asyncio.Event())], timeout=0.01)
    assert task.done() and finished.is_set()


async def test_shutdown_flushes_detached_event_writes_before_closing_db(lifecycle_setup):
    settings, _ = lifecycle_setup
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        await observability.record_event("lifecycle-test", "shutdown-pending")
    with sqlite3.connect(db.DB_PATH) as conn:
        assert conn.execute("SELECT COUNT(*) FROM system_events WHERE source = 'lifecycle-test'").fetchone()[0] == 1
    assert not observability._event_tasks


async def test_event_flush_cancels_and_awaits_stalled_writes(monkeypatch):
    entered, finished = asyncio.Event(), asyncio.Event()

    async def stalled_write(**kwargs):
        try:
            entered.set()
            await asyncio.Event().wait()
        finally:
            finished.set()

    monkeypatch.setattr(observability.system_events_repo, "insert_system_event", stalled_write)
    await observability.record_event("lifecycle-test", "stalled")
    await entered.wait()
    await observability.flush_events(timeout=0.01)
    assert finished.is_set()
    assert not observability._event_tasks
