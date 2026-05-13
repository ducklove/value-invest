"""Scrape/ingest NPS (국민연금공단) domestic equity holdings."""

from __future__ import annotations

import csv
import io
import logging
import os
import re
import subprocess
import urllib.request

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

PUBLIC_NPS_PAGE_URL = "https://www.data.go.kr/data/3070507/fileData.do"
PUBLIC_NPS_FALLBACK_CSV_URL = (
    "https://www.data.go.kr/cmm/cmm/fileDownload.do"
    "?atchFileId=FILE_000000003558824&fileDetailSn=1&insertDataPrcus=N"
)
PUBLIC_NPS_SOURCE_DATE = "2024-12-31"
FNGUIDE_URL = "https://comp.fnguide.com/SVO/WooriRenewal/Inst_Data.asp?strInstCD=49530"

_PUBLIC_DATASET_RE = re.compile(r"국민연금공단_국내주식 투자정보_(\d{8})")
_PUBLIC_CSV_URL_RE = re.compile(r'"contentUrl"\s*:\s*"([^"]+fileDownload\.do[^"]+)"')

# Public-data names are stock short names, while DART corp codes sometimes use
# legal names or English/Korean variants. Keep the critical high-weight aliases
# explicit so the NPS universe does not lose mega-caps such as Hyundai Motor.
_NPS_NAME_ALIASES = {
    "삼성전자": "005930",
    "SK하이닉스": "000660",
    "LG에너지솔루션": "373220",
    "삼성바이오로직스": "207940",
    "현대차": "005380",
    "기아": "000270",
    "NAVER": "035420",
    "셀트리온": "068270",
    "현대모비스": "012330",
    "POSCO홀딩스": "005490",
    "HD현대중공업": "329180",
    "HD한국조선해양": "009540",
    "삼성물산": "028260",
    "LG화학": "051910",
    "삼성생명": "032830",
    "한화에어로스페이스": "012450",
    "삼성SDI": "006400",
    "카카오": "035720",
    "크래프톤": "259960",
    "삼성화재": "000810",
    "두산에너빌리티": "034020",
    "기업은행": "024110",
    "삼성전기": "009150",
    "삼성에스디에스": "018260",
    "삼성중공업": "010140",
    "SK텔레콤": "017670",
    "LG전자": "066570",
    "한미반도체": "042700",
    "HD현대미포": "010620",
    "SK바이오팜": "326030",
    "LS ELECTRIC": "010120",
    "현대차2우B": "005387",
    "삼성전자우": "005935",
    "휠라홀딩스": "081660",
    "HD현대인프라코어": "042670",
    "아모레G": "002790",
    "HD현대건설기계": "267270",
    "DGB금융지주": "139130",
    "삼성화재우": "000815",
    "TKG휴켐스": "069260",
    "DI동일": "001530",
    "KCC글라스": "344820",
    "현대차우": "005385",
    "LG전자우": "066575",
    "LG화학우": "051915",
    "아모레퍼시픽우": "090435",
    "LG생활건강우": "051905",
    "미래에셋증권2우B": "00680K",
    "CJ제일제당 우": "097955",
    "금호석유우": "011785",
    "유나이티드제약": "033270",
    "CJ4우(전환)": "00104K",
    "현대차3우B": "005389",
    "삼성전기우": "009155",
    "신세계 I&C": "035510",
    "KB금융": "105560",
    "신한지주": "055550",
    "하나금융지주": "086790",
    "우리금융지주": "316140",
    "메리츠금융지주": "138040",
    "KT&G": "033780",
    "HMM": "011200",
    "LG": "003550",
    "SK": "034730",
    "LS": "006260",
    "GS": "078930",
    "CJ": "001040",
    "KT": "030200",
    "S-Oil": "010950",
}


def _parse_float(value: str | None) -> float | None:
    text = str(value or "").strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_int(value: str | None) -> int | None:
    parsed = _parse_float(value)
    return int(parsed) if parsed is not None else None


def _source_date_from_text(text: str) -> str:
    match = _PUBLIC_DATASET_RE.search(text)
    if not match:
        return PUBLIC_NPS_SOURCE_DATE
    raw = match.group(1)
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"


def _public_csv_url_from_page() -> tuple[str, str]:
    req = urllib.request.Request(
        PUBLIC_NPS_PAGE_URL,
        headers={"User-Agent": "Mozilla/5.0"},
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        html = resp.read().decode("utf-8", "replace")
    match = _PUBLIC_CSV_URL_RE.search(html)
    csv_url = match.group(1).replace("&amp;", "&") if match else PUBLIC_NPS_FALLBACK_CSV_URL
    return csv_url, _source_date_from_text(html)


def _download_public_csv() -> tuple[bytes, str]:
    env_url = os.getenv("NPS_PUBLIC_DATA_CSV_URL", "").strip()
    if env_url:
        csv_url = env_url
        source_date = os.getenv("NPS_PUBLIC_DATA_SOURCE_DATE", PUBLIC_NPS_SOURCE_DATE).strip() or PUBLIC_NPS_SOURCE_DATE
    elif os.getenv("NPS_PUBLIC_DATA_DISCOVER", "").strip().lower() in {"1", "true", "yes", "on"}:
        try:
            csv_url, source_date = _public_csv_url_from_page()
        except Exception as exc:
            logger.warning("NPS public-data page discovery failed; using fallback CSV URL: %s", exc)
            csv_url, source_date = PUBLIC_NPS_FALLBACK_CSV_URL, PUBLIC_NPS_SOURCE_DATE
    else:
        csv_url, source_date = PUBLIC_NPS_FALLBACK_CSV_URL, PUBLIC_NPS_SOURCE_DATE

    last_error = "public data CSV download failed"
    for _ in range(3):
        result = subprocess.run(
            [
                "curl",
                "-L",
                "-sS",
                "--max-time",
                "10",
                "-H",
                "User-Agent: Mozilla/5.0",
                "-H",
                f"Referer: {PUBLIC_NPS_PAGE_URL}",
                csv_url,
            ],
            capture_output=True,
            timeout=12,
        )
        if result.returncode == 0 and result.stdout:
            return result.stdout, source_date
        last_error = (result.stderr or b"public data CSV download failed").decode("utf-8", "replace")
    raise RuntimeError(last_error)


def fetch_public_nps_holdings() -> list[dict]:
    """Fetch official annual NPS domestic-equity investment data.

    Source: data.go.kr "국민연금공단_국내주식 투자정보". The file provides
    year-end market value, portfolio weight, and ownership percentage. It does
    not provide shares, so snapshot generation derives estimated shares from
    year-end prices before marking holdings to current closes.
    """
    data, source_date = _download_public_csv()
    text = None
    for encoding in ("utf-8-sig", "cp949", "euc-kr"):
        try:
            text = data.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        text = data.decode("utf-8", "replace")

    reader = csv.DictReader(io.StringIO(text))
    holdings: list[dict] = []
    for row in reader:
        rank = _parse_int(row.get("번호"))
        name = str(row.get("종목명") or "").strip()
        amount_eok = _parse_float(row.get("평가액(억 원)"))
        weight_pct = _parse_float(row.get("자산군 내 비중(퍼센트)"))
        ownership_pct = _parse_float(row.get("지분율(퍼센트)"))
        if not rank or not name or amount_eok is None:
            continue
        source_market_value = round(amount_eok * 100_000_000)
        holdings.append({
            "rank": rank,
            "name": name,
            "shares": 0,
            "shares_change": 0,
            "ownership_pct": ownership_pct or 0.0,
            "total_ownership_pct": ownership_pct or 0.0,
            "report_date": source_date,
            "source": "data.go.kr",
            "source_date": source_date,
            "source_market_value": source_market_value,
            "source_weight_pct": weight_pct,
        })
    return holdings


def fetch_fnguide_nps_holdings() -> list[dict]:
    """Fetch FnGuide institution holdings as a fallback/supplement source."""
    result = subprocess.run(
        ["curl", "-s", FNGUIDE_URL],
        capture_output=True, timeout=30,
    )
    soup = BeautifulSoup(result.stdout, "html.parser")
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
        report_date = texts[6].replace(".", "-") if len(texts) > 6 and texts[6] else ""

        holdings.append({
            "rank": rank,
            "name": name,
            "shares": shares,
            "shares_change": shares_change,
            "ownership_pct": ownership_pct,
            "total_ownership_pct": ownership_pct,
            "report_date": report_date,
            "source": "fnguide",
            "source_date": report_date,
        })
    return holdings


def fetch_nps_holdings() -> list[dict]:
    """Fetch the default NPS holdings universe, falling back to FnGuide."""
    try:
        holdings = fetch_public_nps_holdings()
        if holdings:
            logger.info("NPS: loaded %d official public-data holdings", len(holdings))
            return holdings
    except Exception as exc:
        logger.warning("NPS: public-data holdings load failed; falling back to FnGuide: %s", exc)
    return fetch_fnguide_nps_holdings()


async def resolve_stock_codes(holdings: list[dict]) -> list[dict]:
    """Match holding names to stock codes via aliases and corp_codes DB."""
    import cache

    code_table = await cache.load_corp_code_table()
    exact_name_map = {
        (row.get("corp_name") or "").strip(): code
        for code, row in code_table.items()
        if row.get("corp_name")
    }
    upper_name_map = {
        (row.get("corp_name") or "").strip().upper(): code
        for code, row in code_table.items()
        if row.get("corp_name")
    }

    for h in holdings:
        name = str(h.get("name") or "").strip()
        code = _NPS_NAME_ALIASES.get(name) or exact_name_map.get(name) or upper_name_map.get(name.upper())
        if code:
            h["stock_code"] = code
            continue

        results = await cache.search_corp(name)
        if results:
            exact = [r for r in results if r["corp_name"] == name]
            h["stock_code"] = exact[0]["stock_code"] if exact else results[0]["stock_code"]
        else:
            h["stock_code"] = ""
            logger.warning("NPS: no stock code match for '%s'", name)
    return holdings
