"""생태계 레지스트리 라우트 — ``/go/{tool_id}`` 딥링크 리다이렉트와 ``/api/ecosystem``.

``/go/{tool_id}?code=&view=&asset=&theme=&embed=`` 는 레지스트리(config/ecosystem.json)
템플릿으로만 목적지를 만든다(레지스트리 밖 URL 로는 절대 보내지 않는다).
``handoff: true`` 도구는 기존 ``/api/portfolio/open/{key}`` 와 같은 빌더로 보유 스냅샷을
fragment(``#vc-held``)에 싣는다 — 그 라우트는 별칭으로 계속 동작한다.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import RedirectResponse

from core import ecosystem
from deps import get_current_user
from repositories import portfolio as portfolio_repo
from services.ecosystem import links, siblings

logger = logging.getLogger(__name__)

router = APIRouter()

_REDIRECT_HEADERS = {"Cache-Control": "private, no-store", "Referrer-Policy": "no-referrer"}


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


@router.get("/go/{tool_id}")
async def go_tool(
    request: Request,
    tool_id: str,
    code: str = "",
    view: str = "",
    asset: str = "",
    theme: str = "",
    embed: str = "",
):
    """레지스트리 도구로 303. 알 수 없는/내부 도구는 404."""
    try:
        entry = links.public_tool(tool_id)
    except links.UnknownLinkTool:
        raise HTTPException(status_code=404, detail="알 수 없는 도구입니다.") from None
    theme_value = theme if theme in links.THEMES else None
    try:
        if entry.get("handoff") and entry.get("integrationKey"):
            key = entry["integrationKey"]
            user = await get_current_user(request)
            items = await portfolio_repo.get_portfolio(user["google_sub"]) if user else []
            url = links.handoff_url(
                ecosystem.resolved_url(entry) or "", key,
                code=code, theme=theme_value, positions=links.held_positions(key, items),
            )
        else:
            url = links.go_url(tool_id, code=code, view=view, asset=asset, theme=theme_value,
                               embed=_truthy(embed))
    except links.LinkError as exc:
        logger.warning("go/%s: cannot build destination: %s", tool_id, exc)
        raise HTTPException(status_code=503, detail="연결 도구 주소를 확인해 주세요.") from None
    return RedirectResponse(url, status_code=303, headers=dict(_REDIRECT_HEADERS))


@router.get("/api/ecosystem")
async def ecosystem_registry(response: Response):
    """공개 레지스트리 투영 + 형제별 데이터 신선도(summary 로더 기록, 모르면 null)."""
    response.headers["Cache-Control"] = "public, max-age=60"
    projection = ecosystem.public_projection()
    public_ids = {t["id"] for t in projection["tools"]}
    ids = [tool_id for tool_id in siblings.summary_tool_ids() if tool_id in public_ids]
    return {**projection, "freshness": siblings.freshness(ids)}
