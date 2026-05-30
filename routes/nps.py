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


def _json_for_script(value) -> str:
    """``json.dumps`` for safe embedding inside an inline ``<script>`` block.

    Escapes ``<``/``>``/``&`` so a value can't terminate the script context
    (``</script>``). JSON/JS-equivalent (``\\u003c`` parses to ``<``), so the
    chart data is unchanged — this matches the escaping used when the HTML is
    first generated in snapshot_nps.py.
    """
    return (
        json.dumps(value, ensure_ascii=False)
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
    )


def _replace_json_const(html: str, name: str, value: list[dict]) -> str:
    payload = _json_for_script(value)
    pattern = _JSON_CONST_RE.format(name=re.escape(name))
    replacement = f"const {name}   = {payload};"
    # Use a function replacement so the payload (which contains \uXXXX escapes)
    # is inserted literally — a plain string replacement would treat \u as a
    # bad backreference and raise re.error.
    return re.sub(pattern, lambda _m: replacement, html, count=1, flags=re.S)


def _same_number(a, b, *, rel_tol: float = 1e-12) -> bool:
    try:
        av = float(a)
        bv = float(b)
    except (TypeError, ValueError):
        return False
    scale = max(abs(av), abs(bv), 1.0)
    return abs(av - bv) <= scale * rel_tol


def _drop_bad_nps_chart_points(
    nav_data: list[dict],
    value_data: list[dict],
    kospi_data: list[dict],
) -> tuple[list[dict], list[dict], list[dict]]:
    """Drop known-bad NPS chart points created from incomplete close prices."""
    values_by_date = {row.get("date"): row.get("total_value") for row in value_data or []}
    kospi_by_date = {row.get("date"): row.get("value") for row in kospi_data or []}
    kept_nav: list[dict] = []
    kept_dates: list[str] = []
    for row in nav_data or []:
        d = row.get("date")
        nav = row.get("nav")
        if not d or nav is None:
            continue
        if kept_nav:
            prev = kept_nav[-1]
            try:
                ratio = float(nav) / float(prev["nav"])
            except (TypeError, ValueError, ZeroDivisionError):
                ratio = 1.0
            if ratio < 0.70 or ratio > 1.35:
                continue
            prev_date = kept_dates[-1]
            if (
                _same_number(nav, prev.get("nav"))
                and _same_number(values_by_date.get(d), values_by_date.get(prev_date))
                and not _same_number(kospi_by_date.get(d), kospi_by_date.get(prev_date))
            ):
                continue
        kept_nav.append(row)
        kept_dates.append(d)
    kept = set(kept_dates)
    return (
        kept_nav,
        [row for row in (value_data or []) if row.get("date") in kept],
        [row for row in (kospi_data or []) if row.get("date") in kept],
    )


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
    nav_fixed, value_fixed, kospi_fixed = _drop_bad_nps_chart_points(nav_fixed, value_fixed, kospi_fixed)

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
