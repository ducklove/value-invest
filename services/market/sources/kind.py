"""KIND 상장법인목록 provider — 국내 상장사의 회사명·시장·표준산업분류 업종.

허브에는 업종 원천이 없다. 후보를 비교한 결과(2026-11 확인):

* KRX 업종분류현황: data.krx.co.kr 로그인이 필요하다.
* DART 기업개황(``induty_code``): 회사마다 한 번씩 호출해야 한다.
* KIND 상장법인목록: 로그인 없이 한 번의 요청으로 전 시장(약 2,700사)을 받는다.

그래서 KIND 를 쓴다. 클라우드 IP 는 차단될 수 있지만 운영 서버는 가정망에 있다.
영구 캐시와 실패 시 낡은 값 사용은 ``services.portfolio.exposure_data`` 가 맡는다.

응답은 EUC-KR HTML 표이고 열 순서가 바뀐 적이 있어(시장구분 열 추가) 머리글 이름으로
열을 찾는다. 우선주·ETF 는 목록에 없다.
"""

from __future__ import annotations

import asyncio
import re

import httpx

from core.errors import ExternalServiceError
from core.http import get_http_client

CORP_LIST_URL = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"
_HEADERS = {"User-Agent": "Mozilla/5.0", "Referer": "https://kind.krx.co.kr/"}
_TIMEOUT = httpx.Timeout(45.0, connect=8.0)
# 응답이 잘렸거나 다른 페이지가 왔는지 가려내는 하한.
MIN_COMPANIES = 1000

_ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
_CELL_RE = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.S | re.I)
_TAG_RE = re.compile(r"<[^>]+>")
_SPACE_RE = re.compile(r"\s+")
_CODE_RE = re.compile(r"^[0-9][0-9A-Z]{5}$")


class KindError(ExternalServiceError):
    default_detail = "KIND 상장법인목록을 받지 못했습니다."


def _cells(row_html: str) -> list[str]:
    return [_SPACE_RE.sub(" ", _TAG_RE.sub("", cell)).strip() for cell in _CELL_RE.findall(row_html)]


def parse_corp_list(html: str) -> dict[str, dict[str, str]]:
    """상장법인목록 HTML → {종목코드: {name, market, sector}}. 머리글이 없으면 빈 dict."""
    rows = _ROW_RE.findall(html or "")
    if not rows:
        return {}
    header = _cells(rows[0])
    try:
        name_index = header.index("회사명")
        code_index = header.index("종목코드")
        sector_index = header.index("업종")
    except ValueError:
        return {}
    market_index = header.index("시장구분") if "시장구분" in header else None
    required = max(name_index, code_index, sector_index)
    companies: dict[str, dict[str, str]] = {}
    for row in rows[1:]:
        cells = _cells(row)
        if len(cells) <= required:
            continue
        code = cells[code_index].upper()
        if not _CODE_RE.fullmatch(code):
            continue
        companies[code] = {
            "name": cells[name_index],
            "market": cells[market_index] if market_index is not None and market_index < len(cells) else "",
            "sector": cells[sector_index],
        }
    return companies


async def fetch_listed_companies() -> dict[str, dict[str, str]]:
    """KIND 전체 목록을 받아 파싱한다. 형식이 어긋나면 :class:`KindError`."""
    client = await get_http_client("kind")
    response = await client.get(CORP_LIST_URL, headers=_HEADERS, timeout=_TIMEOUT)
    response.raise_for_status()
    # EUC-KR 의 상위 집합인 CP949 로 읽어 일부 회사명의 확장 한글도 살린다.
    html = response.content.decode("cp949", "replace")
    companies = await asyncio.to_thread(parse_corp_list, html)
    if len(companies) < MIN_COMPANIES:
        raise KindError(f"KIND 상장법인목록 건수 부족: {len(companies)}")
    return companies
