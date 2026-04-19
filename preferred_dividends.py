"""Fetch 우선주 (preferred stock) dividend data from the shared Google Sheet.

Rationale: market_data in our SQLite is indexed by common-stock code
(pulled from KRX/DART pipelines that largely ignore preferreds). So a
holding like 005935 or 00088K has no direct dividend record and the
portfolio page falls back to the common stock's dividend, which
typically understates the true payout by the preferred premium.

The user maintains a hand-curated Google Sheet with exact per-year
preferred dividends. Its `Data` tab column AI holds the most recent
year's preferred dividend per share. We fetch that tab via the public
CSV export, parse the rows, and upsert into the `preferred_dividends`
table. `cache.get_trailing_dividends` prefers this data over the
common-stock fallback when available.

Sync cadence: **manual only** via the admin dashboard. Preferred
dividends are announced once per year per stock, and the sheet is
human-edited — an automatic 12h loop was overkill and gets in the way
when the sheet is mid-edit. parse_sheet_csv dynamically picks the
newest year column present (regex `^(\\d{4})우$`, max-year wins), so
when 2026 rolls around and the sheet grows a `2026우` column the
admin refresh will pick it up automatically without code changes.
"""
from __future__ import annotations

import csv
import io
import logging
import os
import re
from datetime import datetime

import httpx

import cache

logger = logging.getLogger(__name__)


# Public CSV export URL for the Google Sheet. Override via env for tests
# or if the sheet is ever relocated.
DEFAULT_SHEET_ID = "1RKLAARnfVNsLKBxyXfdjHhw7AUE1Wv91OrEm868Q3Z8"
DEFAULT_GID = "1614045953"


def _build_url(sheet_id: str | None = None, gid: str | None = None) -> str:
    sid = sheet_id or os.environ.get("PREF_DIV_SHEET_ID") or DEFAULT_SHEET_ID
    g = gid or os.environ.get("PREF_DIV_SHEET_GID") or DEFAULT_GID
    return f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv&gid={g}"


# Header labels we look for. Matching by label rather than fixed offset
# keeps us resilient to the sheet owner inserting a column — AI today
# might be AJ tomorrow. _YEAR_RE matches the 배당 year columns like
# "2025우" / "2024보" so we can discover the current preferred year
# automatically instead of hard-coding "2025우".
_CODE_LABEL = "우선주종목코드"
_NAME_LABEL = "우선주"
_COMMON_LABEL = "보통주종목코드"
_PREF_YEAR_RE = re.compile(r"^(\d{4})우$")


def _find_header_indices(header: list[str]) -> dict:
    """Locate semantic columns by label. Returns dict with:
      code_idx, name_idx, common_idx, pref_year_idx, pref_year
    pref_year_idx / pref_year point at the MOST RECENT preferred-year
    column present (highest year number).
    """
    out = {
        "code_idx": -1,
        "name_idx": -1,
        "common_idx": -1,
        "pref_year_idx": -1,
        "pref_year": None,
    }
    for i, raw in enumerate(header):
        cell = (raw or "").strip()
        if cell == _CODE_LABEL:
            out["code_idx"] = i
        elif cell == _NAME_LABEL:
            out["name_idx"] = i
        elif cell == _COMMON_LABEL:
            out["common_idx"] = i
        m = _PREF_YEAR_RE.match(cell)
        if m:
            year = int(m.group(1))
            if out["pref_year"] is None or year > out["pref_year"]:
                out["pref_year"] = year
                out["pref_year_idx"] = i
    return out


def _clean_number(cell: str) -> float | None:
    """Parse a dividend cell. Google Sheets CSV encodes 1,000 as "1,000"
    but after the csv module strips quotes we get '1,000'. Empty /
    non-numeric → None; zero is legitimate ("no dividend this year")
    and preserved as 0.0 so callers can distinguish 'known zero' from
    'unknown'."""
    if cell is None:
        return None
    s = cell.strip().replace(",", "")
    if not s or s == "-":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_sheet_csv(csv_text: str) -> list[dict]:
    """Pure parsing — no I/O, no DB. Returns list of row dicts ready for
    cache.upsert_preferred_dividends.

    Rows with missing code are skipped (header row and blanks at the end).
    Rows with no recognizable dividend value go through with None, which
    tells the downstream upsert "we know about this preferred but have
    no figure" — preferable to silently dropping them so the next import
    knows to look for an update.
    """
    reader = csv.reader(io.StringIO(csv_text))
    rows = list(reader)
    if not rows:
        return []
    header = rows[0]
    idx = _find_header_indices(header)
    if idx["code_idx"] < 0 or idx["pref_year_idx"] < 0:
        logger.warning(
            "preferred_dividends: required columns missing — code_idx=%d pref_year_idx=%d",
            idx["code_idx"], idx["pref_year_idx"],
        )
        return []

    out: list[dict] = []
    for row in rows[1:]:
        if len(row) <= max(idx["code_idx"], idx["pref_year_idx"]):
            continue
        code = (row[idx["code_idx"]] or "").strip()
        if not code or not re.match(r"^[0-9A-Z]{5,7}$", code):
            continue  # skip blanks / non-code cells
        dps = _clean_number(row[idx["pref_year_idx"]])
        name = row[idx["name_idx"]].strip() if 0 <= idx["name_idx"] < len(row) else ""
        common = row[idx["common_idx"]].strip() if 0 <= idx["common_idx"] < len(row) else ""
        out.append({
            "stock_code": code,
            "dividend_per_share": dps,
            "source_name": name or None,
            "common_code": common or None,
            "sheet_year": idx["pref_year"],
        })
    return out


async def fetch_csv(timeout: float = 30.0) -> str:
    """Download the public CSV export. Raises on non-2xx."""
    url = _build_url()
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        r = await client.get(url)
        r.raise_for_status()
        return r.text


async def refresh_preferred_dividends() -> dict:
    """Full pipeline: fetch → parse → upsert. Returns a stats dict for
    logging / admin dashboard.

    Never raises — failures are logged and returned in the stats so a
    flaky Google Sheets outage doesn't crash the app's startup sequence.
    """
    started = datetime.now()
    try:
        csv_text = await fetch_csv()
    except Exception as exc:
        logger.warning("preferred_dividends: fetch failed: %s", exc)
        return {"ok": False, "error": str(exc)[:300], "rows_written": 0}
    try:
        rows = parse_sheet_csv(csv_text)
    except Exception as exc:
        logger.exception("preferred_dividends: parse failed")
        return {"ok": False, "error": f"parse: {exc}", "rows_written": 0}

    try:
        written = await cache.upsert_preferred_dividends(rows)
    except Exception as exc:
        logger.exception("preferred_dividends: upsert failed")
        return {"ok": False, "error": f"upsert: {exc}", "rows_written": 0}

    elapsed = (datetime.now() - started).total_seconds()
    sheet_year = rows[0].get("sheet_year") if rows else None
    logger.info(
        "preferred_dividends: %d rows refreshed in %.2fs (year=%s)",
        written, elapsed, sheet_year,
    )
    return {
        "ok": True,
        "rows_written": written,
        "sheet_year": sheet_year,
        "elapsed_seconds": round(elapsed, 2),
    }


__all__ = [
    "DEFAULT_SHEET_ID",
    "DEFAULT_GID",
    "fetch_csv",
    "parse_sheet_csv",
    "refresh_preferred_dividends",
]
