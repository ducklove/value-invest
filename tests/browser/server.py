"""브라우저 테스트 전용 앱. 임시 DB의 실제 인증·저장과 고정 시장 데이터를 사용한다."""

import asyncio
import os
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi import Body, FastAPI, Request, Response, WebSocket, WebSocketDisconnect

os.environ["SESSION_SECRET"] = "browser-test-only-secret"
os.environ["GOOGLE_CLIENT_ID"] = ""
os.environ["KIS_PROXY_BASE_URL"] = "http://127.0.0.1:1"
os.environ["CLOSE_PRICE_API_ENABLED"] = "0"

import auth_service
from core.config import PROJECT_ROOT, AppSettings
from core.static_routes import register_static_routes
from deps import get_current_user
from repositories import bootstrap, db, users
from repositories import portfolio as holdings
from routes import auth, portfolio


@asynccontextmanager
async def lifespan(app):
    with tempfile.TemporaryDirectory() as tmp:
        db.DB_PATH = Path(tmp) / "browser.db"
        await bootstrap.init_db()
        user = await users.create_local_user(email="browser@example.com", name="브라우저 검증", password_hash=auth_service.hash_password("browser-test-password"))
        await holdings.save_portfolio_item(user["google_sub"], "005930", "삼성전자", 10, 70000)
        try:
            yield
        finally:
            await bootstrap.close_db()


app = FastAPI(lifespan=lifespan)
app.include_router(auth.router)


@app.get("/healthz")
async def health():
    return {"status": "ok"}


@app.get("/app-config.js")
async def config():
    return Response("window.APP_CONFIG={apiBaseUrl:'',integrations:{}};", media_type="application/javascript")


@app.get("/api/portfolio")
async def get_holdings(request: Request):
    user = portfolio._require_user(await get_current_user(request))
    rows = await holdings.get_portfolio(user["google_sub"])
    for row in rows:
        row.update(avg_price_krw=row["avg_price"], quote={"price": 75000, "previous_close": 74000, "change_pct": 1.35})
    return rows


@app.put("/api/portfolio/{code}")
async def save_holding(code: str, request: Request, payload: dict = Body(...)):
    with patch.object(portfolio.foreign, "resolve_domestic_code_alias", AsyncMock(return_value=None)), \
         patch.object(portfolio.dividends, "schedule_for_portfolio"):
        return await portfolio.save_portfolio_item(code, request, payload)


@app.get("/api/portfolio/groups")
async def groups(request: Request):
    user = portfolio._require_user(await get_current_user(request))
    return await holdings.get_portfolio_groups(user["google_sub"])


@app.api_route("/api/{path:path}", methods=["GET", "POST", "PUT"])
async def auxiliary(path: str):
    if path == "market-indicators":
        await asyncio.sleep(2)
        return {}
    if path == "portfolio/prev-day-snapshot":
        return {"stock_values": {}, "total_value": None}
    if path in {"market-summary", "asset-quotes", "settings/market-bar", "wiki/stats"}:
        return {}
    return []


@app.websocket("/ws/quotes")
async def quotes(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            message = await ws.receive_json()
            if message.get("action") == "ping":
                await ws.send_json({"type": "pong"})
    except WebSocketDisconnect:
        pass


register_static_routes(app, AppSettings(environment="development", project_root=PROJECT_ROOT,
                                      app_title="브라우저 검증", public_api_base_url="",
                                      cors_allowed_origins=(), enable_docs=False), "browser-test")
