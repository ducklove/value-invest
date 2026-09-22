"""브라우저 테스트 전용 앱. 임시 DB의 실제 인증·저장과 고정 시장 데이터를 사용한다."""

import asyncio
import copy
import json
import os
import tempfile
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi import Body, FastAPI, Request, Response, WebSocket, WebSocketDisconnect

os.environ["SESSION_SECRET"] = "browser-test-only-secret"
os.environ["GOOGLE_CLIENT_ID"] = ""
os.environ["KIS_PROXY_BASE_URL"] = "http://127.0.0.1:1"
os.environ["CLOSE_PRICE_API_ENABLED"] = "0"
os.environ["CORS_ALLOWED_ORIGINS"] = "http://127.0.0.1:18765"

import auth_service
from core.config import PROJECT_ROOT, AppSettings
from core.errors import register_exception_handlers
from core.request_security import MutationOriginMiddleware
from core.static_routes import register_static_routes
from deps import get_current_user
from repositories import bootstrap, db, financial, snapshots, users
from repositories import portfolio as holdings
from routes import (
    accounts,
    auth,
    broker_accounts,
    dividend_receipts,
    investment_insights,
    notifications,
    portfolio,
    portfolio_distributions,
    portfolio_trades,
    quant,
)
from services.portfolio.time_windows import today_kst_date


@asynccontextmanager
async def lifespan(app):
    with tempfile.TemporaryDirectory() as tmp:
        db.DB_PATH = Path(tmp) / "browser.db"
        await bootstrap.init_db()
        user = await users.create_local_user(email="browser@example.com", name="브라우저 검증", password_hash=auth_service.hash_password("browser-test-password"))
        await holdings.save_portfolio_item(user["google_sub"], "005930", "삼성전자", 10, 70000)
        await holdings.save_portfolio_item(user["google_sub"], "CASH_KRW", "원화", 1000000, 1)
        today = today_kst_date()
        await financial.save_financial_data("005930", [{"year": today.year-1, "revenue": 100, "operating_profit": 9}])
        for age, price in ((5, 70000), (1, 75000)):
            day = (today-timedelta(days=age)).isoformat()
            await snapshots.save_snapshot(user["google_sub"], day, price*10, 700000, price/70, 700)
            await snapshots.save_stock_snapshots(user["google_sub"], day, [{"stock_code": "005930", "quantity": 10,
                "market_value": price*10, "unit_price": price, "currency": "KRW", "fx_rate": 1}])
        try:
            from services.brokers import activity, derivatives, kis, namuh, sync
            from services.brokers.registry import get_adapter
            from services.quant import scanner_feed
            nh_rows = [{"stock_code": "005930", "stock_name": "삼성전자", "quantity": 3,
                        "avg_price": 80000, "avg_price_currency": "KRW", "currency": "KRW"},
                       {"stock_code": "CASH_KRW", "stock_name": "원화 현금", "quantity": 5000,
                        "avg_price": 1, "avg_price_currency": "KRW", "currency": "KRW"}]
            fixtures = json.loads((Path(__file__).parents[1] / "fixtures/broker-api-responses.json").read_text(encoding="utf-8"))
            async def extra_pages(provider, user, cid, env, tr, inputs):
                if tr == "ka10100":
                    return [{"code": inputs["stk_cd"], "marketCode": "0" if inputs["stk_cd"] == "005930" else "30", "state": "거래정지"}]
                if tr == "ust21070":
                    return [{"result_list": [{"stk_cd": "BRK.B", "frgn_stk_nm": "버크셔", "poss_qty": "1.5", "crnc_code": "USD", "frgn_stk_book_uv": "450.25"}]
                             if inputs["stex_tp"] == "NY" else []}]
                return copy.deepcopy(fixtures[provider][tr])
            async def kiwoom_pages(*args):
                return await extra_pages("kiwoom", *args)
            async def ls_pages(*args):
                return await extra_pages("ls", *args)
            async def nh_snapshot(_user, link):
                if link.get("provider") in {"kiwoom", "ls"}:
                    return await get_adapter(link["provider"]).fetch_snapshot(_user, link)
                product = link.get("product", "stocks")
                if product == "gold":
                    return [{"stock_code": "KRX_GOLD", "stock_name": "KRX 금현물", "quantity": 12,
                             "avg_price": 120000, "avg_price_currency": "KRW", "currency": "KRW"}], {}
                if product.endswith("future"):
                    equity, pnl = (900, -100) if product == "krfuture" else (1490, 90)
                    return derivatives.valuation_rows(equity, pnl), {"_snapshot": {
                        "product": product, "equity": equity, "pnl": pnl, "currency": "KRW", "as_of_date": "2026-09-18",
                        "positions": [{"code": "101V9000" if product == "krfuture" else "ESU26", "name": "선물 계약",
                                       "side": "매도", "quantity": 2, "currency": "KRW" if product == "krfuture" else "USD",
                                       "average_price": 350 if product == "krfuture" else None, "current_price": None, "pnl": None}]}}
                return nh_rows, {}
            async def nh_activity(_user, link, start, end):
                return [activity.normalize({"act_no": link["account_no"], "trd_dt": today.strftime("%Y%m%d"),
                    "trd_sno": str(i), "cur_cd": "KRW", "sps_cd_krl_anm": label, "iem_cd": "005930", "iem_nm": "삼성전자",
                    "trd_bf_dca": "10000", "trd_af_dca": "10846", "trd_amt": "1000", "tax_sum": "154", "trd_orn_fee": "0", "int_amt": "0"}, link)
                    for i, label in enumerate(("현금배당 입금", "예탁금이용료", "이체입금"), 1)]
            with patch.object(scanner_feed, "catalog", AsyncMock(return_value=[])), \
                 patch.object(get_adapter("kiwoom"), "pages", side_effect=kiwoom_pages), \
                 patch.object(get_adapter("ls"), "pages", side_effect=ls_pages), \
                 patch.object(kis, "token", AsyncMock(return_value="test-kis-token")), \
                 patch.object(activity, "fetch", side_effect=nh_activity), \
                 patch.object(namuh, "accounts", AsyncMock(return_value=[{"account_no": no, "environment": "live"} for no in ("12345678901", "22222222222", "33333333333")])), \
                 patch.object(broker_accounts, "fetch_snapshot", side_effect=nh_snapshot), \
                 patch.object(sync, "fetch_snapshot", side_effect=nh_snapshot):
                yield
        finally:
            await bootstrap.close_db()


app = FastAPI(lifespan=lifespan)
register_exception_handlers(app)
app.add_middleware(MutationOriginMiddleware, allowed_origins=["http://127.0.0.1:18765"])
app.include_router(auth.router)
app.include_router(accounts.router)
app.include_router(broker_accounts.router)
app.include_router(investment_insights.router)
app.include_router(portfolio_trades.router)
app.include_router(dividend_receipts.router)
app.include_router(portfolio_distributions.router)
app.include_router(quant.router)
app.include_router(notifications.router)


@app.get("/healthz")
async def health():
    return {"status": "ok"}


@app.get("/app-config.js")
async def config():
    return Response("window.APP_CONFIG={apiBaseUrl:'',integrations:{}};", media_type="application/javascript")


@app.get("/api/portfolio")
async def get_holdings(request: Request):
    user = portfolio._require_user(await get_current_user(request))
    rows = await holdings.get_portfolio(user["google_sub"], request.query_params.get("account_id"))
    for row in rows:
        quote = {"price": 1, "previous_close": 1, "change_pct": 0} if row["stock_code"] in {"CASH_KRW", "FUTURES_BASE_KRW", "FUTURES_PNL_KRW"} else {"price": 75000, "previous_close": 74000, "change_pct": 1.35}
        row.update(avg_price_krw=row["avg_price"], quote=quote)
    return rows


app.add_api_route("/api/portfolio/order", portfolio.save_portfolio_order, methods=["PUT"])
app.add_api_route("/api/portfolio/{stock_code}/metadata", portfolio.save_holding_metadata, methods=["PUT"])
app.add_api_route("/api/portfolio/{stock_code}/group", portfolio.set_holding_group, methods=["PUT"])


@app.put("/api/portfolio/{stock_code}/benchmark")
async def edit_benchmark(stock_code: str, request: Request, payload: dict = Body(...)):
    with patch.object(portfolio, "_fetch_benchmark_quote", AsyncMock(return_value={})):
        return await portfolio.update_benchmark(stock_code, request, payload)


@app.put("/api/portfolio/{code}")
async def save_holding(code: str, request: Request, payload: dict = Body(...)):
    with patch.object(portfolio.foreign, "resolve_domestic_code_alias", AsyncMock(return_value=None)), \
         patch.object(portfolio.dividends, "schedule_for_portfolio"):
        return await portfolio.save_portfolio_item(code, request, payload)


@app.get("/api/portfolio/groups")
async def groups(request: Request):
    user = portfolio._require_user(await get_current_user(request))
    return await holdings.get_portfolio_groups(user["google_sub"])


@app.delete("/api/portfolio/{code}")
async def delete_holding(code: str, request: Request):
    return await portfolio.delete_portfolio_item(code, request)


@app.get("/api/portfolio/cashflows")
async def cashflows(request: Request):
    return await portfolio.get_cashflows(request)


@app.post("/api/portfolio/cashflows")
async def add_cashflow(request: Request, payload: dict = Body(...)):
    return await portfolio.add_cashflow(request, payload)


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
