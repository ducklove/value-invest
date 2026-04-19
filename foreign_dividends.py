"""Fetch trailing-annual dividend per share for overseas tickers via yfinance.

Scope limited to foreign stocks because:
  * Korean stocks are covered by the market_data pipeline (stock_price.py)
  * Preferred stocks have their own curated sheet (preferred_dividends.py)
  * Cash / gold / crypto obviously have no dividend concept

We write the trailingAnnualDividendRate (last 12 months actual payouts in
native currency), convert to KRW using the same FX path the quote
fetchers use, and upsert into `foreign_dividends` with source='yfinance'.
Rows that an admin has manually overridden (source='manual') are
preserved — see cache.upsert_foreign_dividends_auto for the guard clause.

Like the preferred-dividends module, this one has NO background loop —
admin triggers it on demand. yfinance responses for dividend fields are
typically stable over a reporting period, and the stock list is small
(a handful of overseas holdings per user), so manual refresh is fine.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime

import cache

logger = logging.getLogger(__name__)


def _is_korean_code(code: str) -> bool:
    """Same rule used in routes/portfolio._is_korean_stock — 6-digit
    numeric code = KOSPI/KOSDAQ. We duplicate it here (rather than
    importing) to avoid pulling routes.portfolio into this module and
    creating an import cycle via its indirect deps."""
    return len(code) == 6 and code[:5].isdigit()


def _is_cash_or_special(code: str) -> bool:
    if code.startswith("CASH_"):
        return True
    return code in {"KRX_GOLD", "CRYPTO_BTC", "CRYPTO_ETH"}


async def select_foreign_target_codes() -> list[str]:
    """Codes we should fetch dividends for = every distinct stock_code
    across all users' portfolios that's NOT a Korean common stock,
    preferred stock, cash asset, or crypto/gold. Callers typically
    pass this straight into fetch_many()."""
    db = await cache.get_db()
    cursor = await db.execute(
        "SELECT DISTINCT stock_code FROM user_portfolio WHERE stock_code IS NOT NULL"
    )
    codes = [row["stock_code"] for row in await cursor.fetchall()]
    return [
        c for c in codes
        if not _is_korean_code(c) and not _is_cash_or_special(c)
    ]


def _looks_like_empty_info(info: dict) -> bool:
    """yfinance 는 404 인 ticker 에도 None 대신 빈 dict 나 매우 부분적인
    dict 를 돌려줄 때가 있다. 의미 있는 응답인지 빠르게 판단하기 위해
    name + price 조합으로 본다. 둘 다 없으면 '사실상 invalid'."""
    if not info:
        return True
    has_name = info.get("longName") or info.get("shortName")
    has_price = (info.get("regularMarketPrice") or info.get("currentPrice")
                 or info.get("previousClose"))
    return not (has_name or has_price)


def _fetch_one_sync(ticker: str) -> dict | None:
    """Blocking yfinance call. Returns {dps_native, currency} or None if
    the ticker is invalid even after suffix-strip retry.

    Resolution order for the per-share dividend figure:
      (1) trailingAnnualDividendRate > 0  — 지난 12개월 실측. 가장 신뢰.
      (2) dividendRate > 0                — forward 추정치.
      (3) yield × regularMarketPrice      — 채권 ETF 등 trailing 이 0 이거나
          미제공인 케이스 (83199.HK, SCHP, QTUM, DAX 등 샘플 검증).
      (4) dividends Series 최근 365일 합산 — info 에 배당 필드가 전부
          None 이지만 실제 지급 이력이 있는 경우. EUN2.DE (iShares EURO
          STOXX 50 UCITS) 같은 UCITS ETF 가 대표 케이스.
      (5) 0.0 — '배당 없음' 확정으로 저장.

    Ticker 본체가 invalid 하면 (`.O`, `.K` 같은 Reuters 접미사 → 404)
    자동으로 접미사 제거 후 재시도. 영구 resolve 는 calling 쪽에서
    _save_ticker 로 ticker_map 에 넣어두면 이후로는 바로 정답으로 접근.
    """
    import yfinance as yf

    def _try(sym: str) -> tuple[dict, "yf.Ticker | None"]:
        try:
            t = yf.Ticker(sym)
            info = t.info or {}
            return info, t
        except Exception as exc:
            logger.warning("yfinance info failed (%s): %s", sym, exc)
            return {}, None

    info, t_obj = _try(ticker)
    # Reuters 접미사 (.O Nasdaq, .K Nasdaq Global Select, .OQ, .PK) 를 떨궈
    # 재시도. 한 번만 스트립 — 여러 번 해도 이득 없음.
    if _looks_like_empty_info(info):
        for suffix in (".O", ".K", ".OQ", ".PK"):
            if ticker.endswith(suffix):
                stripped = ticker[: -len(suffix)]
                logger.info("yfinance retry with suffix-stripped ticker: %s → %s", ticker, stripped)
                info, t_obj = _try(stripped)
                break
    if _looks_like_empty_info(info) or t_obj is None:
        return None

    dps: float = 0.0

    def _to_float(v) -> float | None:
        if v is None:
            return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    trailing = _to_float(info.get("trailingAnnualDividendRate"))
    if trailing is not None and trailing > 0:
        dps = trailing
    else:
        forward = _to_float(info.get("dividendRate"))
        if forward is not None and forward > 0:
            dps = forward
        else:
            # 채권 ETF 등 — yield × price 역산.
            y = _to_float(info.get("yield"))
            price = (_to_float(info.get("regularMarketPrice"))
                     or _to_float(info.get("currentPrice"))
                     or _to_float(info.get("previousClose")))
            if y is not None and y > 0 and price is not None and price > 0:
                dps = round(y * price, 4)

    # 마지막 fallback: info 가 배당 필드를 숨기는 경우에도 dividends
    # Series 에는 실제 지급 이력이 있는 경우가 있다. 최근 365일 합.
    if dps == 0.0:
        try:
            divs = t_obj.dividends
            if divs is not None and len(divs):
                import pandas as pd
                tz = divs.index[0].tz
                cutoff = pd.Timestamp.now(tz=tz) - pd.Timedelta(days=365)
                recent = divs[divs.index >= cutoff]
                if len(recent):
                    dps = round(float(recent.sum()), 4)
        except Exception as exc:
            logger.debug("dividends Series fallback failed (%s): %s", ticker, exc)

    currency = (info.get("currency") or "USD").upper()
    return {"dps_native": dps, "currency": currency}


async def _fetch_one(ticker: str) -> dict | None:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _fetch_one_sync, ticker)


async def refresh_foreign_dividends(
    stock_codes: list[str] | None = None,
) -> dict:
    """Full pipeline. Caller can pass an explicit list; if None we pull
    the full portfolio-wide target set.

    Returns a stats dict. Never raises — a yfinance outage on one
    ticker doesn't abort the rest, and upstream failures are reported
    rather than thrown.
    """
    started = datetime.now()
    if stock_codes is None:
        try:
            stock_codes = await select_foreign_target_codes()
        except Exception as exc:
            logger.warning("foreign_dividends: target selection failed: %s", exc)
            return {"ok": False, "error": str(exc)[:300], "rows_written": 0,
                    "total_attempted": 0}

    # Resolve each portfolio code to the yfinance ticker (ticker_map
    # handles forms like BRK.B vs BRK-B, suffixed HK tickers, etc).
    # We import routes.portfolio lazily to avoid a startup-time import
    # cycle — foreign_dividends gets imported early from cache / admin,
    # while routes.portfolio pulls in plenty of other modules.
    from routes.portfolio import _ticker_map, _ensure_ticker_map, _fx_to_krw
    await _ensure_ticker_map()

    rows: list[dict] = []
    failures: list[str] = []
    # 현지 통화 → '국적' 코드. _fx_to_krw 가 nation 기반이라 mapping 필요.
    # KIS 데이터에 맞춰 USA/HKG/JPN/CHN/VNM/TWN 정도만 실제 쓰임. 기타는
    # USA 로 fallback 하면 yfinance 의 `currency` 기준으로 일단 USD 환산
    # 되어 의도와 어긋나지만 희귀 케이스 (유럽 주식 등) 는 수동 override
    # 권장.
    _CUR_TO_NATION = {
        "USD": "USA", "HKD": "HKG", "JPY": "JPN", "CNY": "CHN", "CNH": "CHN",
        "VND": "VNM", "TWD": "TWN", "EUR": "EUR", "GBP": "GBR", "AUD": "AUS",
        "CAD": "CAN", "CHF": "CHE",
    }

    for code in stock_codes:
        ticker = _ticker_map.get(code, code)
        try:
            info = await _fetch_one(ticker)
        except Exception as exc:
            logger.warning("foreign_dividends: fetch_one crashed (%s): %s", code, exc)
            failures.append(code)
            continue
        if info is None:
            failures.append(code)
            continue
        dps_native = float(info["dps_native"])
        currency = info["currency"]
        nation = _CUR_TO_NATION.get(currency, "USA")
        try:
            dps_krw = await _fx_to_krw(nation, dps_native)
        except Exception as exc:
            logger.warning("foreign_dividends: fx to KRW failed (%s %s): %s", code, currency, exc)
            failures.append(code)
            continue
        rows.append({
            "stock_code": code,
            "dps_native": dps_native,
            "currency": currency,
            "dps_krw": round(float(dps_krw), 2),
        })

    try:
        written = await cache.upsert_foreign_dividends_auto(rows)
    except Exception as exc:
        logger.exception("foreign_dividends: upsert failed")
        return {"ok": False, "error": f"upsert: {exc}", "rows_written": 0,
                "total_attempted": len(stock_codes)}

    elapsed = (datetime.now() - started).total_seconds()
    logger.info(
        "foreign_dividends: %d/%d rows refreshed in %.2fs (%d failures)",
        written, len(stock_codes), elapsed, len(failures),
    )
    return {
        "ok": True,
        "rows_written": written,
        "total_attempted": len(stock_codes),
        "failures": failures,
        "elapsed_seconds": round(elapsed, 2),
    }


__all__ = [
    "select_foreign_target_codes",
    "refresh_foreign_dividends",
]
