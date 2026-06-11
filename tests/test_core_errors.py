"""core.errors — 예외 계층, HTTP 매핑, transaction()의 DBError 변환."""

import httpx
import pytest
from fastapi import FastAPI

from core.errors import (
    AppError,
    DBError,
    ExternalServiceError,
    RateLimitError,
    register_exception_handlers,
)


def test_hierarchy_keeps_runtimeerror_compat_for_rebased_client_errors():
    import close_price_client
    import kis_proxy_client

    # 재베이스된 클라이언트 예외는 계층에 속하면서도 기존
    # ``except RuntimeError`` 핸들러에 계속 잡혀야 한다.
    for exc_type in (kis_proxy_client.KISProxyError, close_price_client.ClosePriceClientError):
        assert issubclass(exc_type, ExternalServiceError)
        assert issubclass(exc_type, RuntimeError)
    assert issubclass(RateLimitError, ExternalServiceError)
    assert issubclass(DBError, AppError)


def _app_with_handlers() -> FastAPI:
    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/external")
    async def external():
        raise ExternalServiceError("KIS proxy request failed")

    @app.get("/rate-limited")
    async def rate_limited():
        raise RateLimitError()

    @app.get("/db")
    async def db():
        raise DBError("UNIQUE constraint failed: users.email")

    return app


async def _get(app: FastAPI, path: str) -> httpx.Response:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get(path)


async def test_external_service_error_maps_to_502_with_message():
    resp = await _get(_app_with_handlers(), "/external")
    assert resp.status_code == 502
    assert resp.json() == {"detail": "KIS proxy request failed"}


async def test_rate_limit_error_maps_to_429_with_default_detail():
    resp = await _get(_app_with_handlers(), "/rate-limited")
    assert resp.status_code == 429
    assert "요청이 너무 많습니다" in resp.json()["detail"]


async def test_db_error_maps_to_500_with_message():
    resp = await _get(_app_with_handlers(), "/db")
    assert resp.status_code == 500
    assert "UNIQUE constraint failed" in resp.json()["detail"]


async def test_transaction_converts_sqlite_errors_to_dberror_and_rolls_back(temp_db):
    from repositories import db

    async with db.transaction() as conn:
        await conn.execute("CREATE TABLE txn_probe (id INTEGER PRIMARY KEY)")

    with pytest.raises(DBError) as excinfo:
        async with db.transaction() as conn:
            await conn.execute("INSERT INTO txn_probe (id) VALUES (1)")
            await conn.execute("INSERT INTO no_such_table (id) VALUES (1)")

    assert "no_such_table" in str(excinfo.value)

    conn = await db.get_db()
    cursor = await conn.execute("SELECT COUNT(*) FROM txn_probe")
    row = await cursor.fetchone()
    assert row[0] == 0  # 같은 트랜잭션의 선행 INSERT 도 롤백돼야 한다


async def test_transaction_passes_through_app_exceptions_unchanged(temp_db):
    from repositories import db

    with pytest.raises(ValueError):
        async with db.transaction():
            raise ValueError("domain failure")
