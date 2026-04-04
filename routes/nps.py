from fastapi import APIRouter
from fastapi.responses import HTMLResponse, Response

import cache

router = APIRouter()


@router.get("/api/nps/html")
async def get_nps_html():
    html = await cache.get_latest_nps_html()
    if not html:
        return Response(
            content='<div style="text-align:center;padding:40px;color:var(--text-secondary);">데이터 준비 중입니다. 매일 22시에 갱신됩니다.</div>',
            media_type="text/html",
        )
    return Response(content=html, media_type="text/html")
