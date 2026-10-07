"""Portfolio security-code normalization and classification rules.

This module deliberately has no route, service, repository, or I/O
dependencies.  Both the persistence and service layers need these rules, so
keeping them in a neutral domain layer prevents repositories from importing
upward into ``services``.
"""

from __future__ import annotations

import re

from domain.broker_assets import FUTURES_VALUE_CODES, is_futures_contract

SPECIAL_ASSETS = {"KRX_GOLD", "CMA_RP_KRW", "CRYPTO_BTC", "CRYPTO_ETH", "CRYPTO_USDT"} | FUTURES_VALUE_CODES

_KRX_CODE_RE = re.compile(r"^[0-9][0-9A-Z]{5}$")
_KRX_PREFERRED_CODE_RE = re.compile(r"^\d{5}[1-9A-Z]$")


def normalize_portfolio_code(code: str | None) -> str:
    normalized = (code or "").strip().upper()
    if normalized.endswith((".KS", ".KQ")) and _KRX_CODE_RE.fullmatch(normalized[:-3]):
        return normalized[:-3]
    return normalized


def is_cash_asset(code: str | None) -> bool:
    return normalize_portfolio_code(code).startswith("CASH_")


def is_special_asset(code: str | None) -> bool:
    normalized = normalize_portfolio_code(code)
    return normalized in SPECIAL_ASSETS or is_cash_asset(normalized) or is_futures_contract(normalized)


def is_korean_stock(code: str | None) -> bool:
    return bool(_KRX_CODE_RE.fullmatch(normalize_portfolio_code(code)))


def is_korean_listing(code: str | None) -> bool:
    """Recognize native KRX codes and Yahoo's Korean exchange suffixes."""
    normalized = normalize_portfolio_code(code)
    if normalized.endswith((".KS", ".KQ")):
        normalized = normalized[:-3]
    return is_korean_stock(normalized)


def is_hong_kong_rmb_counter(code: str | None) -> bool:
    """HKEX 80000~89999는 홍콩 상장 위안화 거래 종목이다."""
    # https://www.hkex.com.hk/Products/Securities/Stock-Code-Allocation-Plan
    return bool(re.fullmatch(r"8[0-9]{4}\.HK", normalize_portfolio_code(code)))


def is_preferred_stock(code: str | None) -> bool:
    return bool(_KRX_PREFERRED_CODE_RE.fullmatch(normalize_portfolio_code(code)))


def common_stock_code(code: str | None) -> str:
    normalized = normalize_portfolio_code(code)
    return normalized[:5] + "0"


# 허브 코드(Reuters/네이버 표기) → Yahoo 심볼 변환 규칙.
# 미국 상장 Reuters 거래소 접미사 — Yahoo 는 미국 종목에 접미사를 붙이지 않는다
# (GOOGL.O → GOOGL). .O/.OQ 나스닥, .N NYSE, .K NYSE Arca, .PK 장외.
# .A(NYSE American)는 일부러 뺀다: 클래스 주식 표기(BRK.A, BF.A → BRK-A, BF-A)와
# 구분할 수 없어서다.
_REUTERS_US_EXCHANGE_SUFFIXES = frozenset({"O", "OQ", "N", "K", "PK"})
# 허브와 Yahoo 가 다르게 쓰는 거래소 접미사 (.HM 호찌민 HOSE → Yahoo .VN).
_YAHOO_SUFFIX_ALIASES = {"HM": "VN"}
# 한 글자 Yahoo 거래소 접미사. 그 외 한 글자 접미사는 미국 클래스 주식이다.
_YAHOO_ONE_LETTER_EXCHANGES = frozenset({"L", "F", "T", "V"})


def _yahoo_class_share(symbol: str) -> str:
    root, dot, suffix = symbol.rpartition(".")
    if (dot and len(suffix) == 1 and suffix not in _YAHOO_ONE_LETTER_EXCHANGES
            and root.replace(".", "").isalpha()):
        return f"{root}-{suffix}"
    return symbol


def yahoo_symbol(code: str | None) -> str:
    """허브/Reuters 표기 종목 코드를 Yahoo chart·yfinance 심볼로 바꾼다.

    GOOGL.O → GOOGL, FUEVFVND.HM → FUEVFVND.VN, BRK.B·BRK/B → BRK-B.
    Yahoo 가 쓰는 거래소 접미사(7203.T, BP.L, A200.AX, 0005.HK …)와
    접미사 없는 코드는 그대로 둔다. 멱등이다.
    """
    # Yahoo 조회용 거래소 접미사는 유지한다. 포트폴리오 식별자는 국내
    # 6자리 코드로 통일하지만 provider 심볼까지 접미사를 지우면 안 된다.
    symbol = (code or "").strip().upper().replace("/", "-")
    root, dot, suffix = symbol.rpartition(".")
    if not dot or not root:
        return symbol
    if suffix in _REUTERS_US_EXCHANGE_SUFFIXES:
        return _yahoo_class_share(root)
    alias = _YAHOO_SUFFIX_ALIASES.get(suffix)
    if alias:
        return f"{root}.{alias}"
    return _yahoo_class_share(symbol)


# 거래소 접미사 없는 미국식 티커의 Yahoo 표기: 영문 1~5자 + 선택적 클래스(-B).
_PLAIN_US_TICKER_RE = re.compile(r"[A-Z]{1,5}(?:-[A-Z])?")


def is_plain_us_ticker(code: str | None) -> bool:
    """거래소 접미사 없는 미국식 티커인가 — AAPL, BRK.B·BRK-B·BRK/B, GOOGL.O.

    Yahoo 표기로 바꾼 뒤(미국 Reuters 접미사 제거, 클래스 주식 대시) 영문 1~5자
    (+클래스 한 글자)만 남는 코드다. 거래소 접미사(BP.L, EUN2.DE, VNM.HM),
    숫자가 섞인 코드(국내 6자리, 홍콩·일본 숫자 코드, A200·EUN2), 가상 코드는
    아니다. 이런 코드는 미국 상장을 먼저 찾아야 한다 — 미국 조회가 일시적으로
    실패했다고 독일·런던 등 해외 접미사로 넘어가면 AAPL → AAPL.DE 같은 엉뚱한
    매핑이 영구히 남는다.
    """
    return bool(_PLAIN_US_TICKER_RE.fullmatch(yahoo_symbol(code)))
