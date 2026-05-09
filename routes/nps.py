import json
import re

from fastapi import APIRouter
from fastapi.responses import HTMLResponse, Response

import cache

router = APIRouter()


_JSON_CONST_RE = r"const\s+{name}\s*=\s*(\[.*?\]);"


def _extract_json_const(html: str, name: str) -> list[dict] | None:
    match = re.search(_JSON_CONST_RE.format(name=re.escape(name)), html, re.S)
    if not match:
        return None
    try:
        data = json.loads(match.group(1))
        return data if isinstance(data, list) else None
    except Exception:
        return None


def _replace_json_const(html: str, name: str, value: list[dict]) -> str:
    payload = json.dumps(value, ensure_ascii=False)
    pattern = _JSON_CONST_RE.format(name=re.escape(name))
    return re.sub(pattern, f"const {name}   = {payload};", html, count=1, flags=re.S)


async def _kospi_rows_for_dates(dates: list[str], embedded: list[dict]) -> list[dict]:
    if not dates:
        return []
    by_date: dict[str, float] = {}
    try:
        rows = await cache.get_benchmark_rows("KOSPI", start=min(dates))
        by_date.update({
            r["date"]: round(float(r["close"]), 2)
            for r in rows
            if r.get("date") in dates and r.get("close") is not None
        })
    except Exception:
        pass
    for row in embedded or []:
        d = row.get("date")
        v = row.get("value")
        if d in dates and d not in by_date and v is not None:
            try:
                by_date[d] = round(float(v), 2)
            except (TypeError, ValueError):
                pass
    return [{"date": d, "value": by_date[d]} for d in dates if d in by_date]


async def _repair_nps_chart_html(html: str) -> str:
    """Repair persisted NPS chart JSON so NAV/KOSPI dates stay aligned."""
    nav_data = _extract_json_const(html, "NPS_NAV_DATA")
    value_data = _extract_json_const(html, "NPS_VALUE_DATA")
    embedded_kospi = _extract_json_const(html, "NPS_KOSPI_DATA") or []
    if not nav_data:
        return html

    nav_dates = [row.get("date") for row in nav_data if row.get("date")]
    kospi_data = await _kospi_rows_for_dates(nav_dates, embedded_kospi)
    trading_dates = {row["date"] for row in kospi_data}
    if not trading_dates:
        return html

    nav_fixed = [row for row in nav_data if row.get("date") in trading_dates]
    value_fixed = [row for row in (value_data or []) if row.get("date") in trading_dates]
    fixed_dates = {row["date"] for row in nav_fixed}
    kospi_fixed = [row for row in kospi_data if row["date"] in fixed_dates]

    html = _replace_json_const(html, "NPS_NAV_DATA", nav_fixed)
    if value_data is not None:
        html = _replace_json_const(html, "NPS_VALUE_DATA", value_fixed)
    return _replace_json_const(html, "NPS_KOSPI_DATA", kospi_fixed)


@router.get("/api/nps/html")
async def get_nps_html():
    html = await cache.get_latest_nps_html()
    if not html:
        return Response(
            content='<div style="text-align:center;padding:40px;color:var(--text-secondary);">데이터 준비 중입니다. 매일 22시에 갱신됩니다.</div>',
            media_type="text/html",
        )
    return Response(content=await _repair_nps_chart_html(html), media_type="text/html")
