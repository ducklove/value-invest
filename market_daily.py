from __future__ import annotations

import asyncio
import hashlib
import html
import json
import logging
import os
import re
import time
from datetime import date, datetime
from typing import Any
from urllib.parse import urljoin

import httpx

import ai_config
import cache
import dart_client
import market_indicators
import stock_price
from services.portfolio.identifiers import is_korean_stock, normalize_portfolio_code


logger = logging.getLogger(__name__)

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
DART_VIEWER_URL = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"

INTEREST_LIMIT = int(os.environ.get("MARKET_DAILY_INTEREST_LIMIT", "36"))
QUOTE_CONCURRENCY = int(os.environ.get("MARKET_DAILY_QUOTE_CONCURRENCY", "6"))
DISCLOSURE_CONCURRENCY = int(os.environ.get("MARKET_DAILY_DISCLOSURE_CONCURRENCY", "4"))
NOTABLE_MOVE_LIMIT = int(os.environ.get("MARKET_DAILY_NOTABLE_MOVE_LIMIT", "12"))
NEWS_STOCK_LIMIT = int(os.environ.get("MARKET_DAILY_NEWS_STOCK_LIMIT", "8"))
NEWS_PER_STOCK = int(os.environ.get("MARKET_DAILY_NEWS_PER_STOCK", "4"))
MARKET_DAILY_MAX_TOKENS = int(os.environ.get("MARKET_DAILY_MAX_TOKENS", "1800"))

_NEWS_CACHE: dict[str, tuple[float, list[dict[str, Any]]]] = {}
_NEWS_CACHE_TTL_SECONDS = 600

_MATERIAL_DISCLOSURE_KEYWORDS = [
    "유상증자",
    "무상증자",
    "전환사채",
    "신주인수권",
    "교환사채",
    "자사주",
    "주식소각",
    "배당",
    "현금배당",
    "실적",
    "잠정",
    "매출액",
    "영업이익",
    "공급계약",
    "단일판매",
    "수주",
    "합병",
    "분할",
    "영업양수",
    "영업양도",
    "타법인",
    "최대주주",
    "대표이사",
    "소송",
    "횡령",
    "배임",
    "회생",
    "파산",
    "상장폐지",
    "관리종목",
    "매매거래정지",
    "불성실공시",
]


def _today_iso() -> str:
    return date.today().isoformat()


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "").replace("%", "")
    if not text or text in {"-", "N/A"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _signed_change_pct(raw_pct: Any, direction: str | None = None) -> float | None:
    pct = _safe_float(raw_pct)
    if pct is None:
        return None
    direction = (direction or "").lower()
    if direction in {"down", "dn", "minus"}:
        return -abs(pct)
    if direction in {"up", "plus"}:
        return abs(pct)
    return pct


def detect_relative_move(stock_change_pct: float | None, kospi_change_pct: float | None) -> dict[str, Any]:
    """Apply the user-defined 급등/급락 rule against KOSPI movement."""
    if stock_change_pct is None:
        return {
            "is_notable": False,
            "relative_pct": None,
            "threshold_pct": None,
            "move_type": None,
        }
    if kospi_change_pct is None:
        relative = stock_change_pct
        threshold = 2.0
    else:
        relative = stock_change_pct - kospi_change_pct
        threshold = max(2.0, abs(kospi_change_pct))
    notable = abs(relative) >= threshold
    return {
        "is_notable": notable,
        "relative_pct": round(relative, 2),
        "threshold_pct": round(threshold, 2),
        "move_type": "급등" if notable and relative > 0 else "급락" if notable and relative < 0 else None,
    }


def estimate_gemini35_flash_cost(input_tokens: int = 0, output_tokens: int = 0, *, batch: bool = False) -> float:
    input_price = 0.75 if batch else 1.50
    output_price = 4.50 if batch else 9.00
    return (input_tokens * input_price + output_tokens * output_price) / 1_000_000


def _material_disclosure_reason(report_name: str) -> str | None:
    normalized = re.sub(r"\s+", "", report_name or "")
    for keyword in _MATERIAL_DISCLOSURE_KEYWORDS:
        if keyword in normalized:
            return keyword
    return None


async def _interest_universe(google_sub: str | None) -> list[dict[str, Any]]:
    if not google_sub:
        return []

    seen: dict[str, dict[str, Any]] = {}

    try:
        portfolio = await cache.get_portfolio(google_sub)
    except Exception:
        logger.exception("daily market: portfolio load failed")
        portfolio = []
    for item in portfolio:
        code = normalize_portfolio_code(item.get("stock_code"))
        if not is_korean_stock(code):
            continue
        seen.setdefault(
            code,
            {
                "stock_code": code,
                "stock_name": item.get("stock_name") or code,
                "sources": [],
                "quantity": item.get("quantity"),
            },
        )["sources"].append("portfolio")

    try:
        starred = await cache.get_cached_analyses(google_sub=google_sub, tab="starred")
    except Exception:
        logger.exception("daily market: starred list load failed")
        starred = []
    for item in starred:
        code = normalize_portfolio_code(item.get("stock_code"))
        if not is_korean_stock(code):
            continue
        seen.setdefault(
            code,
            {
                "stock_code": code,
                "stock_name": item.get("corp_name") or code,
                "sources": [],
                "quantity": None,
            },
        )["sources"].append("starred")

    items = list(seen.values())
    items.sort(key=lambda x: (0 if "portfolio" in x.get("sources", []) else 1, x["stock_code"]))
    return items[:INTEREST_LIMIT]


async def _market_snapshot() -> tuple[list[dict[str, Any]], float | None]:
    codes = ["KOSPI", "KOSDAQ", "KOSPI200", "USD_KRW", "SPX", "IXIC", "US10Y", "OIL_CL"]
    data = await market_indicators.fetch_indicators(codes)
    rows: list[dict[str, Any]] = []
    kospi_pct = None
    for code in codes:
        raw = data.get(code) or {}
        pct = _signed_change_pct(raw.get("change_pct"), raw.get("direction"))
        row = {
            "code": code,
            "label": market_indicators.CATALOG.get(code, {}).get("label", code),
            "value": raw.get("value") or "",
            "change": raw.get("change") or "",
            "change_pct": pct,
            "direction": raw.get("direction") or "",
        }
        if code == "KOSPI":
            kospi_pct = pct
        rows.append(row)
    return rows, kospi_pct


async def _quote_moves(interests: list[dict[str, Any]], kospi_pct: float | None) -> list[dict[str, Any]]:
    semaphore = asyncio.Semaphore(max(1, QUOTE_CONCURRENCY))

    async def fetch_one(item: dict[str, Any]) -> dict[str, Any]:
        async with semaphore:
            code = item["stock_code"]
            try:
                quote = await stock_price.fetch_quote_snapshot(code)
            except Exception as exc:
                logger.info("daily market: quote skipped for %s: %s", code, exc)
                quote = {}
            stock_pct = _safe_float(quote.get("change_pct"))
            detected = detect_relative_move(stock_pct, kospi_pct)
            return {
                "stock_code": code,
                "stock_name": item.get("stock_name") or code,
                "sources": sorted(set(item.get("sources") or [])),
                "price": quote.get("price"),
                "date": quote.get("date"),
                "change": quote.get("change"),
                "change_pct": stock_pct,
                "kospi_change_pct": kospi_pct,
                "relative_pct": detected["relative_pct"],
                "threshold_pct": detected["threshold_pct"],
                "move_type": detected["move_type"],
                "is_notable": detected["is_notable"],
                "trade_value": quote.get("trade_value"),
            }

    rows = await asyncio.gather(*(fetch_one(item) for item in interests))
    rows.sort(key=lambda row: abs(row.get("relative_pct") or 0), reverse=True)
    return rows


async def _fetch_dart_disclosures(interests: list[dict[str, Any]], brief_date: str) -> tuple[list[dict[str, Any]], list[str]]:
    warnings: list[str] = []
    if not dart_client.API_KEY:
        return [], ["OPENDART_API_KEY가 없어 관심종목 공시 조회를 건너뜀"]

    corp_rows: list[tuple[dict[str, Any], str]] = []
    for item in interests:
        try:
            corp_code = await cache.get_corp_code(item["stock_code"])
        except Exception:
            corp_code = None
        if corp_code:
            corp_rows.append((item, corp_code))

    yyyymmdd = brief_date.replace("-", "")
    semaphore = asyncio.Semaphore(max(1, DISCLOSURE_CONCURRENCY))
    disclosures: list[dict[str, Any]] = []

    async with httpx.AsyncClient(timeout=10.0) as client:
        async def fetch_one(item: dict[str, Any], corp_code: str) -> None:
            async with semaphore:
                params = {
                    "crtfc_key": dart_client.API_KEY,
                    "corp_code": corp_code,
                    "bgn_de": yyyymmdd,
                    "end_de": yyyymmdd,
                    "page_count": "20",
                }
                try:
                    resp = await client.get(f"{dart_client.BASE_URL}/list.json", params=params)
                    if resp.status_code != 200:
                        warnings.append(f"{item['stock_code']} DART HTTP {resp.status_code}")
                        return
                    payload = resp.json()
                except Exception as exc:
                    warnings.append(f"{item['stock_code']} DART 조회 실패: {exc}")
                    return
                if payload.get("status") not in {"000", "013"}:
                    warnings.append(f"{item['stock_code']} DART status {payload.get('status')}")
                    return
                for raw in payload.get("list") or []:
                    report_name = str(raw.get("report_nm") or "").strip()
                    rcept_no = str(raw.get("rcept_no") or "").strip()
                    reason = _material_disclosure_reason(report_name)
                    disclosures.append(
                        {
                            "stock_code": item["stock_code"],
                            "stock_name": item.get("stock_name") or raw.get("corp_name") or item["stock_code"],
                            "corp_name": raw.get("corp_name") or item.get("stock_name") or "",
                            "report_name": report_name,
                            "rcept_no": rcept_no,
                            "rcept_dt": raw.get("rcept_dt") or "",
                            "filer": raw.get("flr_nm") or "",
                            "remark": raw.get("rm") or "",
                            "url": DART_VIEWER_URL.format(rcept_no=rcept_no) if rcept_no else "",
                            "is_material": bool(reason),
                            "material_reason": reason,
                        }
                    )

        await asyncio.gather(*(fetch_one(item, corp_code) for item, corp_code in corp_rows))

    disclosures.sort(key=lambda row: (0 if row.get("is_material") else 1, row.get("stock_code") or ""))
    return disclosures[:30], warnings[:8]


def _clean_html_text(value: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", html.unescape(value or ""))).strip()


async def _fetch_stock_news(stock_code: str, limit: int = NEWS_PER_STOCK) -> list[dict[str, Any]]:
    cached = _NEWS_CACHE.get(stock_code)
    if cached and time.monotonic() - cached[0] < _NEWS_CACHE_TTL_SECONDS:
        return cached[1][:limit]

    try:
        url = f"https://finance.naver.com/item/news_news.naver?code={stock_code}&page=1"
        async with httpx.AsyncClient(
            timeout=8.0,
            headers={"User-Agent": "Mozilla/5.0", "Referer": "https://finance.naver.com/"},
        ) as client:
            resp = await client.get(url)
        html_body = resp.content.decode("euc-kr", errors="ignore")
        rows = re.findall(
            r'<td class="title">.*?<a href="([^"]+)"[^>]*>(.+?)</a>.*?'
            r'<td class="info">(.+?)</td>.*?<td class="date">([^<]+)</td>',
            html_body,
            re.DOTALL,
        )
        news: list[dict[str, Any]] = []
        for href, title, outlet, published_at in rows[:limit]:
            news.append(
                {
                    "stock_code": stock_code,
                    "title": _clean_html_text(title),
                    "outlet": _clean_html_text(outlet),
                    "published_at": _clean_html_text(published_at),
                    "url": urljoin("https://finance.naver.com", href),
                }
            )
        _NEWS_CACHE[stock_code] = (time.monotonic(), news)
        return news
    except Exception as exc:
        logger.info("daily market: news skipped for %s: %s", stock_code, exc)
        return []


async def _news_for_focus_codes(codes: list[str]) -> list[dict[str, Any]]:
    unique = []
    seen = set()
    for code in codes:
        if code and code not in seen:
            unique.append(code)
            seen.add(code)
    batches = await asyncio.gather(*(_fetch_stock_news(code) for code in unique[:NEWS_STOCK_LIMIT]))
    return [item for batch in batches for item in batch]


def _source_hash(payload: dict[str, Any]) -> str:
    source = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def _fallback_markdown(payload: dict[str, Any], message: str) -> str:
    market = payload.get("market") or []
    moves = [row for row in payload.get("moves") or [] if row.get("is_notable")]
    disclosures = payload.get("disclosures") or []
    lines = [f"### 금일 시황", "", message, ""]
    if market:
        lines.append("**시장 지표**")
        for row in market[:5]:
            pct = row.get("change_pct")
            pct_text = f"{pct:+.2f}%" if isinstance(pct, (int, float)) else "-"
            lines.append(f"- {row.get('label') or row.get('code')}: {row.get('value') or '-'} ({pct_text})")
        lines.append("")
    if moves:
        lines.append("**관심목록 급등/급락**")
        for row in moves[:8]:
            lines.append(
                f"- {row['stock_name']}({row['stock_code']}): {row.get('change_pct'):+.2f}% "
                f"/ KOSPI 대비 {row.get('relative_pct'):+.2f}%p"
            )
        lines.append("")
    if disclosures:
        lines.append("**관심목록 공시**")
        for row in disclosures[:8]:
            lines.append(f"- {row['stock_name']}({row['stock_code']}): {row['report_name']}")
    return "\n".join(lines).strip()


def _build_prompt(payload: dict[str, Any]) -> str:
    compact = dict(payload)
    compact["moves"] = [
        row
        for row in (payload.get("moves") or [])
        if row.get("is_notable") or abs(row.get("relative_pct") or 0) >= 1.2
    ][:NOTABLE_MOVE_LIMIT]
    compact["largest_moves"] = (payload.get("moves") or [])[: min(10, NOTABLE_MOVE_LIMIT)]
    compact["disclosures"] = (payload.get("disclosures") or [])[:30]
    compact["news"] = (payload.get("news") or [])[: NEWS_STOCK_LIMIT * NEWS_PER_STOCK]
    evidence = json.dumps(compact, ensure_ascii=False, indent=2, default=str)
    return f"""아래 evidence bundle만 근거로 한국어 금일 시황을 작성하세요.

요구사항:
- 근거 없는 단정 금지. 가격 변동의 원인은 공시/뉴스 제목상 가능한 연결만 "가능성"으로 표현하세요.
- 관심목록 공시와 급등/급락을 시장 요약보다 우선합니다.
- 링크가 있는 공시/뉴스는 종목명 뒤에 짧게 출처를 언급하되, 긴 URL은 본문에 노출하지 마세요.
- 출력은 마크다운으로 5개 섹션 이내, 900~1300자 정도로 간결하게 작성하세요.
- 반드시 마지막에 "확인 필요" 항목을 1~3개 적으세요.

evidence bundle:
{evidence}
"""


async def _call_openrouter(payload: dict[str, Any], google_sub: str | None) -> dict[str, Any]:
    openrouter_key = await ai_config.get_openrouter_key()
    model = await ai_config.get_model_for_feature("market_daily")
    if not openrouter_key:
        markdown = _fallback_markdown(payload, "AI API 키가 없어 수집된 근거만 표시합니다.")
        return {
            "markdown": markdown,
            "model": model,
            "tokens_in": 0,
            "tokens_out": 0,
            "cost_usd": 0.0,
            "llm_ok": False,
            "error": "missing_openrouter_key",
        }

    prompt = _build_prompt(payload)
    request_payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": "당신은 한국 주식 시황을 근거 중심으로 요약하는 투자 리서치 어시스턴트입니다.",
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
        "max_tokens": MARKET_DAILY_MAX_TOKENS,
        **ai_config.openrouter_reasoning_controls(model),
    }
    started = time.monotonic()
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(90.0, read=90.0)) as client:
            resp = await client.post(
                OPENROUTER_URL,
                headers={
                    "Authorization": f"Bearer {openrouter_key}",
                    "Content-Type": "application/json",
                },
                json=request_payload,
            )
            if resp.status_code in {400, 422} and "reasoning" in request_payload:
                retry_payload = dict(request_payload)
                retry_payload.pop("reasoning", None)
                retry_payload.pop("include_reasoning", None)
                resp = await client.post(
                    OPENROUTER_URL,
                    headers={
                        "Authorization": f"Bearer {openrouter_key}",
                        "Content-Type": "application/json",
                    },
                    json=retry_payload,
                )
        if resp.status_code != 200:
            raise RuntimeError(f"OpenRouter HTTP {resp.status_code}: {resp.text[:300]}")
        data = resp.json()
        message = (data.get("choices") or [{}])[0].get("message") or {}
        markdown = str(message.get("content") or "").strip()
        usage = data.get("usage") or {}
        tokens_in = int(usage.get("prompt_tokens") or usage.get("input_tokens") or 0)
        tokens_out = int(usage.get("completion_tokens") or usage.get("output_tokens") or 0)
        cost_usd = float(usage.get("cost") or usage.get("total_cost") or 0.0)
        if not cost_usd and (tokens_in or tokens_out):
            cost_usd = estimate_gemini35_flash_cost(tokens_in, tokens_out)
        await ai_config.record_usage(
            google_sub=google_sub,
            feature="market_daily",
            model=data.get("model") or model,
            model_profile="market_daily",
            input_tokens=tokens_in,
            output_tokens=tokens_out,
            cost_usd=cost_usd,
            latency_ms=int((time.monotonic() - started) * 1000),
            ok=bool(markdown),
            error=None if markdown else "empty_content",
        )
        had_content = bool(markdown)
        if not markdown:
            markdown = _fallback_markdown(payload, "AI 모델이 최종 본문을 반환하지 않아 수집된 근거만 표시합니다.")
        return {
            "markdown": markdown,
            "model": data.get("model") or model,
            "tokens_in": tokens_in,
            "tokens_out": tokens_out,
            "cost_usd": cost_usd,
            "llm_ok": had_content,
            "error": None if had_content else "empty_content",
        }
    except Exception as exc:
        logger.exception("daily market: LLM call failed")
        await ai_config.record_usage(
            google_sub=google_sub,
            feature="market_daily",
            model=model,
            model_profile="market_daily",
            ok=False,
            error=str(exc),
            latency_ms=int((time.monotonic() - started) * 1000),
        )
        return {
            "markdown": _fallback_markdown(payload, f"AI 호출에 실패해 수집된 근거만 표시합니다. ({exc})"),
            "model": model,
            "tokens_in": 0,
            "tokens_out": 0,
            "cost_usd": 0.0,
            "llm_ok": False,
            "error": str(exc),
        }


async def build_daily_market_brief(*, google_sub: str | None = None, brief_date: str | None = None) -> dict[str, Any]:
    brief_date = brief_date or _today_iso()
    interests = await _interest_universe(google_sub)
    market_rows, kospi_pct = await _market_snapshot()
    moves = await _quote_moves(interests, kospi_pct) if interests else []
    disclosures, disclosure_warnings = await _fetch_dart_disclosures(interests, brief_date) if interests else ([], [])

    focus_codes = [
        *(row["stock_code"] for row in moves if row.get("is_notable")),
        *(row["stock_code"] for row in disclosures if row.get("is_material")),
    ]
    news = await _news_for_focus_codes(focus_codes)

    payload: dict[str, Any] = {
        "brief_date": brief_date,
        "generated_at": datetime.now().isoformat(),
        "kospi_change_pct": kospi_pct,
        "relative_move_rule": {
            "threshold_pct": max(2.0, abs(kospi_pct or 0)),
            "description": "abs(stock_change_pct - kospi_change_pct) >= max(2%, abs(kospi_change_pct))",
        },
        "interest_count": len(interests),
        "interest_limit": INTEREST_LIMIT,
        "market": market_rows,
        "moves": moves,
        "disclosures": disclosures,
        "news": news,
        "source_warnings": disclosure_warnings,
    }
    source_hash = _source_hash(payload)
    llm_result = await _call_openrouter(payload, google_sub)
    return {
        "brief_date": brief_date,
        "source_hash": source_hash,
        "payload": payload,
        **llm_result,
    }
