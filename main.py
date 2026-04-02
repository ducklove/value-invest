import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

import cache
import dart_client
import kis_key_manager
import kis_proxy_client
import kis_ws_manager
from routes import auth_router, analysis_router, reports_router, stocks_router, cache_router, portfolio_router, ws_quotes_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    env_path = Path(__file__).parent / ".kis.env"
    if env_path.exists():
        load_dotenv(env_path, override=True)
    kis_key_manager.load_keys()

    await kis_proxy_client.init_client()
    await cache.init_db()
    await cache.delete_expired_sessions()
    needs_corp_refresh = not await cache.is_corp_codes_loaded() or await cache.corp_codes_need_refresh()
    if needs_corp_refresh:
        logger.info("corp_codes 테이블을 DART 기준으로 갱신합니다...")
        try:
            codes = await dart_client.fetch_corp_codes()
            await cache.save_corp_codes(codes)
            logger.info(f"{len(codes)}개 상장사 코드를 저장했습니다.")
        except Exception as e:
            logger.error(f"corp_codes 다운로드 실패: {e}")
    yield
    await kis_ws_manager.stop_all()
    await kis_proxy_client.close_client()
    await cache.close_db()


app = FastAPI(title="한국 주식 가치투자 분석", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost",
        "http://127.0.0.1",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
        "https://ducklove.github.io",
        "https://cantabile.tplinkdns.com:3691",
    ],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)

app.include_router(auth_router)
app.include_router(analysis_router)
app.include_router(reports_router)
app.include_router(stocks_router)
app.include_router(cache_router)
app.include_router(portfolio_router)
app.include_router(ws_quotes_router)


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/app-config.js")
async def app_config():
    payload = {"apiBaseUrl": ""}
    return Response(
        content=f"window.APP_CONFIG = {json.dumps(payload, ensure_ascii=False)};",
        media_type="application/javascript",
    )


@app.get("/styles.css")
async def styles():
    return FileResponse(STATIC_DIR / "styles.css", media_type="text/css")


# 정적 파일 서빙 (CSS, JS 등)
app.mount("/js", StaticFiles(directory=str(STATIC_DIR / "js")), name="js")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
