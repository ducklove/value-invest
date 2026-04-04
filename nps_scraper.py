"""Scrape NPS (국민연금공단) holdings from FnGuide."""

import logging
import re
import subprocess

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

FNGUIDE_URL = "https://comp.fnguide.com/SVO/WooriRenewal/Inst_Data.asp?strInstCD=49530"


def fetch_nps_holdings() -> list[dict]:
    """Fetch current NPS holdings from FnGuide. Returns list of dicts."""
    result = subprocess.run(
        ["curl", "-s", FNGUIDE_URL],
        capture_output=True, timeout=30,
    )
    soup = BeautifulSoup(result.stdout, "html.parser", from_encoding="euc-kr")
    rows = soup.find_all("tr")
    holdings = []
    for tr in rows:
        tds = tr.find_all("td")
        if len(tds) < 7:
            continue
        texts = [td.get_text(strip=True) for td in tds]
        try:
            rank = int(texts[0])
        except (ValueError, IndexError):
            continue
        name = texts[1]
        shares_str = texts[2].replace(",", "")
        shares = int(shares_str) if shares_str.lstrip("-").isdigit() else 0
        chg_str = texts[3].replace(",", "")
        shares_change = int(chg_str) if chg_str.lstrip("-").isdigit() else 0
        try:
            ownership_pct = float(texts[5])
        except (ValueError, IndexError):
            ownership_pct = 0.0
        try:
            total_ownership_pct = float(texts[6])
        except (ValueError, IndexError):
            total_ownership_pct = 0.0
        report_date = ""
        if len(texts) > 7 and texts[7]:
            report_date = texts[7].replace(".", "-")

        holdings.append({
            "rank": rank,
            "name": name,
            "shares": shares,
            "shares_change": shares_change,
            "ownership_pct": ownership_pct,
            "total_ownership_pct": total_ownership_pct,
            "report_date": report_date,
        })
    return holdings


async def resolve_stock_codes(holdings: list[dict]) -> list[dict]:
    """Match holding names to stock codes via corp_codes DB."""
    import cache

    for h in holdings:
        results = await cache.search_corp(h["name"])
        if results:
            exact = [r for r in results if r["corp_name"] == h["name"]]
            h["stock_code"] = exact[0]["stock_code"] if exact else results[0]["stock_code"]
        else:
            h["stock_code"] = ""
            logger.warning("NPS: no stock code match for '%s'", h["name"])
    return holdings
