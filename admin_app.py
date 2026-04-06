"""Standalone admin server — runs on a separate port (localhost only)."""

import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse

import cache
from routes import admin as admin_module
from routes.admin import router as admin_router

admin_module._STANDALONE_MODE = True

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    await cache.init_db()
    yield
    await cache.close_db()


app = FastAPI(title="Value Invest Admin", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(admin_router)


@app.get("/")
async def admin_index():
    return FileResponse(STATIC_DIR / "admin.html")


@app.get("/styles.css")
async def styles():
    return FileResponse(STATIC_DIR / "styles.css", media_type="text/css")


@app.get("/js/{path:path}")
async def serve_js(path: str):
    return FileResponse(STATIC_DIR / "js" / path, media_type="application/javascript")
