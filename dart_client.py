import httpx
import asyncio
import zipfile
import io
import xml.etree.ElementTree as ET
from pathlib import Path

API_KEY = ""
BASE_URL = "https://opendart.fss.or.kr/api"

# 재무제표 항목명 매핑
ACCOUNT_NAMES = {
    "revenue": ["매출액", "수익(매출액)", "영업수익"],
    "operating_profit": ["영업이익", "영업이익(손실)"],
    "net_income": ["당기순이익", "당기순이익(손실)"],
    "total_assets": ["자산총계"],
    "total_liabilities": ["부채총계"],
    "total_equity": ["자본총계"],
}


def load_api_key():
    global API_KEY
    keys_path = Path(__file__).parent / "keys.txt"
    if keys_path.exists():
        for line in keys_path.read_text().strip().splitlines():
            if line.startswith("OPENDART_API_KEY="):
                API_KEY = line.split("=", 1)[1].strip()
                break


load_api_key()


async def fetch_corp_codes() -> list[dict]:
    """DART에서 고유번호 XML을 다운로드하여 상장사 목록 반환."""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            f"{BASE_URL}/corpCode.xml", params={"crtfc_key": API_KEY}
        )
        resp.raise_for_status()

    codes = []
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        xml_name = zf.namelist()[0]
        tree = ET.parse(zf.open(xml_name))
        root = tree.getroot()
        for item in root.iter("list"):
            stock_code = item.findtext("stock_code", "").strip()
            if not stock_code:
                continue
            codes.append(
                {
                    "corp_code": item.findtext("corp_code", "").strip(),
                    "corp_name": item.findtext("corp_name", "").strip(),
                    "stock_code": stock_code,
                }
            )
    return codes


def _match_account(account_nm: str, target_key: str) -> bool:
    """계정과목명이 target_key에 매핑되는 항목인지 확인."""
    for pattern in ACCOUNT_NAMES.get(target_key, []):
        if account_nm.startswith(pattern):
            return True
    return False


def _parse_amount(value: str | None) -> float | None:
    """금액 문자열을 float로 변환. 빈값/파싱불가 시 None."""
    if not value:
        return None
    cleaned = value.replace(",", "").strip()
    if not cleaned or cleaned == "-":
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


async def fetch_financial_statement(
    corp_code: str, year: int
) -> dict | None:
    """단일 회사의 단일 연도 재무제표를 가져온다. CFS 우선, OFS 폴백."""
    result = {}

    for report_code in ["CFS", "OFS"]:
        params = {
            "crtfc_key": API_KEY,
            "corp_code": corp_code,
            "bsns_year": str(year),
            "reprt_code": "11011",  # 사업보고서(연간)
            "fs_div": report_code,
        }
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{BASE_URL}/fnlttSinglAcnt.json", params=params
            )

        if resp.status_code != 200:
            continue

        data = resp.json()
        if data.get("status") != "000":
            continue

        items = data.get("list", [])
        for item in items:
            account_nm = item.get("account_nm", "")
            amount_str = item.get("thstrm_amount")

            for key in ACCOUNT_NAMES:
                if key not in result and _match_account(account_nm, key):
                    val = _parse_amount(amount_str)
                    if val is not None:
                        result[key] = val

        if result:
            break

    if not result:
        return None

    result["year"] = year
    return result


async def fetch_financial_statements(
    corp_code: str, start_year: int = 2000, end_year: int | None = None,
    on_progress=None,
) -> list[dict]:
    """여러 연도의 재무제표를 순차 호출 (rate limit 준수)."""
    if end_year is None:
        from datetime import datetime
        end_year = datetime.now().year - 1

    total = end_year - start_year + 1
    results = []
    for i, year in enumerate(range(start_year, end_year + 1)):
        if on_progress:
            await on_progress(i + 1, total, year)
        stmt = await fetch_financial_statement(corp_code, year)
        if stmt:
            results.append(stmt)
        await asyncio.sleep(0.5)  # DART rate limit

    return results
