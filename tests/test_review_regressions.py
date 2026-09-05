"""동시 쓰기·외부 장애·비정상 입력의 실제 실패 경로를 고정한다."""

import asyncio
import sqlite3
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from _harness import seed_user
from fastapi import FastAPI, HTTPException
from starlette.requests import Request

from core.app_factory import create_app
from core.rate_limit import client_identity, enforce_rate_limit, reset_rate_limits
from repositories import bootstrap, db, users
from routes import internal, portfolio
from services.portfolio import fx


def test_session_write_cannot_commit_another_tasks_transaction():
    async def run():
        with tempfile.TemporaryDirectory() as tmp, patch.object(db, "DB_PATH", Path(tmp) / "review.db"):
            await db.close_db()
            conn = await db.get_db()
            await conn.executescript("CREATE TABLE probe(value TEXT); CREATE TABLE user_sessions(session_token_hash TEXT);")
            entered = asyncio.Event()
            attempted = asyncio.Event()

            async def writer():
                with pytest.raises(ValueError):
                    async with db.transaction() as tx:
                        await tx.execute("INSERT INTO probe VALUES ('rollback')")
                        entered.set()
                        await attempted.wait()
                        await asyncio.sleep(0)
                        raise ValueError("롤백")

            async def logout():
                await entered.wait()
                attempted.set()
                await users.delete_user_session("missing")

            try:
                await asyncio.wait_for(asyncio.gather(writer(), logout()), 5)
                assert await (await conn.execute("SELECT * FROM probe")).fetchall() == []
            finally:
                await db.close_db()
    asyncio.run(run())


def test_concurrent_connection_initialization_has_one_owner():
    async def run():
        with tempfile.TemporaryDirectory() as tmp, patch.object(db, "DB_PATH", Path(tmp) / "review.db"):
            await db.close_db()
            try:
                connections = await asyncio.gather(*(db.get_db() for _ in range(12)))
                assert len({id(conn) for conn in connections}) == 1
            finally:
                await db.close_db()
    asyncio.run(run())


@pytest.mark.parametrize("currency", ["USD", "JPY", "UNKNOWN"])
def test_missing_fx_never_becomes_one_to_one_conversion(currency):
    async def run():
        with patch.object(fx, "fx_rate_for_code", AsyncMock(return_value=None)):
            with pytest.raises(fx.FXUnavailableError):
                await fx.price_to_krw(100, currency)
    asyncio.run(run())


def test_stale_fx_cache_cannot_value_a_new_snapshot():
    async def run():
        fx._fx_cache.clear()
        fx._fx_cache.set("rates", {"FX_USDKRW": 1400}, ttl_seconds=-1)
        try:
            with patch.object(fx, "fetch_fx_daily_change", AsyncMock(return_value={"price": 1400, "_stale": True})), \
                 patch.object(fx, "get_fx_rates", AsyncMock(return_value={"FX_USDKRW": 1400})):
                assert await fx.fx_rate_for_code("FX_USDKRW") is None
            assert fx._fx_cache.get_entry("rates", allow_stale=True).cached_at
        finally:
            fx._fx_cache.clear()
    asyncio.run(run())


def test_forwarded_headers_do_not_bypass_ip_limit():
    reset_rate_limits()
    try:
        for index in range(2):
            request = Request({"type": "http", "headers": [(b"x-forwarded-for", f"198.51.100.{index}".encode())],
                               "client": ("203.0.113.20", 1234)})
            assert client_identity(request) == "203.0.113.20"
            if index:
                with pytest.raises(HTTPException) as exc:
                    enforce_rate_limit(request, scope="review", max_requests=1, window_seconds=60, detail="제한")
                assert exc.value.status_code == 429
            else:
                enforce_rate_limit(request, scope="review", max_requests=1, window_seconds=60, detail="제한")
    finally:
        reset_rate_limits()


@pytest.mark.parametrize("field,value", [("target_price", "Infinity"), ("quantity", "NaN"),
                                         ("avg_price", float("inf")), ("target_price_disabled", "false"),
                                         ("created_at", "2026-02-30")])
def test_invalid_holding_is_rejected_before_any_write(field, value):
    async def run():
        with tempfile.TemporaryDirectory() as tmp, patch.object(db, "DB_PATH", Path(tmp) / "review.db"):
            await bootstrap.close_db()
            await bootstrap.init_db()
            try:
                app = FastAPI()
                app.include_router(portfolio.router)
                body = {"stock_name": "검증", "quantity": 1, "avg_price": 100, field: value}
                # JSON 표준으로 표현할 수 없는 float도 문자열 입력과 동일하게 차단한다.
                if isinstance(value, float):
                    body[field] = "Infinity"
                with patch.object(portfolio, "get_current_user", AsyncMock(return_value={"google_sub": "u1"})):
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                        response = await client.put("/api/portfolio/005930", json=body)
                        assert response.status_code == 400
                conn = await db.get_db()
                assert (await (await conn.execute("SELECT COUNT(*) FROM user_portfolio")).fetchone())[0] == 0
            finally:
                await bootstrap.close_db()
    asyncio.run(run())


def test_data_quality_error_is_http_failure_for_systemd():
    async def run():
        app = FastAPI()
        app.include_router(internal.router)
        with patch.object(internal, "_require_loopback"), \
             patch("services.data_quality.run_all_checks", AsyncMock(return_value={"counts": {"error": 1}, "results": []})):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                response = await client.post("/api/internal/data-quality/check")
                assert response.status_code == 503
                assert response.json()["ok"] is False
    asyncio.run(run())


@pytest.mark.parametrize("database_available", [True, False])
def test_readiness_checks_database_separately_from_liveness(database_available):
    async def run():
        with tempfile.TemporaryDirectory() as tmp, patch.object(db, "DB_PATH", Path(tmp) / "review.db"):
            await bootstrap.close_db()
            await bootstrap.init_db()
            try:
                app = create_app()
                real_get_db = db.get_db
                probe = AsyncMock(side_effect=real_get_db if database_available else sqlite3.OperationalError("unavailable"))
                with patch.object(db, "get_db", probe):
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                        assert (await client.get("/healthz")).status_code == 200
                        response = await client.get("/readyz")
                assert response.status_code == (200 if database_available else 503)
                assert response.json()["database"] == ("ok" if database_available else "unavailable")
                probe.assert_awaited_once()
            finally:
                await bootstrap.close_db()
    asyncio.run(run())


@pytest.mark.parametrize("fx_available", [True, False])
def test_edit_preserves_foreign_cost_currency_and_resolves_fx_before_write(fx_available):
    async def run():
        with tempfile.TemporaryDirectory() as tmp, patch.object(db, "DB_PATH", Path(tmp) / "review.db"):
            await bootstrap.close_db()
            await bootstrap.init_db()
            try:
                await seed_user()
                await portfolio.portfolio_repo.save_portfolio_item(
                    "u1", "AAPL", "Apple", 1, 100, "USD", avg_price_currency="USD")
                rate = AsyncMock(side_effect=[1400, fx.FXUnavailableError("USD")] if fx_available
                                 else fx.FXUnavailableError("USD"))
                with patch.object(portfolio, "get_current_user", AsyncMock(return_value={"google_sub": "u1"})), \
                     patch.object(portfolio.foreign, "resolve_domestic_code_alias", AsyncMock(return_value=None)), \
                     patch.object(portfolio.foreign_dividends_repo, "get_foreign_dividend", AsyncMock(return_value={})), \
                     patch.object(portfolio.dividends, "schedule_for_portfolio"), \
                     patch.object(fx, "fx_rate_for_currency", rate):
                    body = {"stock_name": "Apple", "quantity": 2, "avg_price": 100, "currency": "USD"}
                    if fx_available:
                        result = await portfolio.save_portfolio_item("AAPL", None, body)
                        assert result["avg_price_currency"] == "USD"
                        assert result["avg_price_krw"] == 140000
                    else:
                        with pytest.raises(fx.FXUnavailableError):
                            await portfolio.save_portfolio_item("AAPL", None, body)
                rate.assert_awaited_once_with("USD")
                saved = await portfolio.portfolio_repo.get_portfolio_item("u1", "AAPL")
                assert saved["quantity"] == (2 if fx_available else 1)
                assert saved["avg_price_currency"] == "USD"
            finally:
                await bootstrap.close_db()
    asyncio.run(run())
