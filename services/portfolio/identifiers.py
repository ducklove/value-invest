from __future__ import annotations

import re
from typing import Any


SPECIAL_ASSETS = {"KRX_GOLD", "CRYPTO_BTC", "CRYPTO_ETH"}

CASH_NAMES = {
    "CASH_KRW": "원화",
    "CASH_USD": "미국 달러",
    "CASH_EUR": "유로",
    "CASH_JPY": "일본 엔",
    "CASH_CNY": "중국 위안",
    "CASH_HKD": "홍콩 달러",
    "CASH_GBP": "영국 파운드",
    "CASH_AUD": "호주 달러",
    "CASH_CAD": "캐나다 달러",
    "CASH_CHF": "스위스 프랑",
    "CASH_TWD": "대만 달러",
    "CASH_VND": "베트남 동",
    "CASH_SEK": "스웨덴 크로나",
    "CASH_DKK": "덴마크 크로네",
    "CASH_NOK": "노르웨이 크로네",
}

CASH_FX_CODE = {
    "CASH_USD": "FX_USDKRW",
    "CASH_EUR": "FX_EURKRW",
    "CASH_JPY": "FX_JPYKRW",
    "CASH_CNY": "FX_CNYKRW",
    "CASH_HKD": "FX_HKDKRW",
    "CASH_GBP": "FX_GBPKRW",
    "CASH_AUD": "FX_AUDKRW",
    "CASH_CAD": "FX_CADKRW",
    "CASH_CHF": "FX_CHFKRW",
    "CASH_TWD": "FX_TWDKRW",
    "CASH_VND": "FX_VNDKRW",
}

STATIC_FOREIGN_TICKERS: dict[str, dict[str, Any]] = {
    "BRK-A": {
        "ticker": "BRK-A",
        "name": "Berkshire Hathaway Inc. Class A",
        "currency": "USD",
    },
    "A200": {
        "ticker": "A200.AX",
        "name": "BetaShares Australia 200 ETF",
        "currency": "AUD",
    },
    "A200.AX": {
        "ticker": "A200.AX",
        "name": "BetaShares Australia 200 ETF",
        "currency": "AUD",
    },
    "EUN2": {
        "ticker": "EUN2.DE",
        "name": "iShares Core EURO STOXX 50 UCITS ETF EUR",
        "currency": "EUR",
    },
    "EUN2.DE": {
        "ticker": "EUN2.DE",
        "name": "iShares Core EURO STOXX 50 UCITS ETF EUR",
        "currency": "EUR",
    },
}

_KRX_CODE_RE = re.compile(r"^[0-9][0-9A-Z]{5}$")
_KRX_PREFERRED_CODE_RE = re.compile(r"^\d{5}[1-9A-Z]$")


def normalize_portfolio_code(code: str | None) -> str:
    return (code or "").strip().upper()


def static_foreign_ticker(code: str | None) -> dict[str, Any] | None:
    return STATIC_FOREIGN_TICKERS.get(normalize_portfolio_code(code))


def is_cash_asset(code: str | None) -> bool:
    return normalize_portfolio_code(code).startswith("CASH_")


def is_special_asset(code: str | None) -> bool:
    normalized = normalize_portfolio_code(code)
    return normalized in SPECIAL_ASSETS or is_cash_asset(normalized)


def is_korean_stock(code: str | None) -> bool:
    return bool(_KRX_CODE_RE.fullmatch(normalize_portfolio_code(code)))


def is_preferred_stock(code: str | None) -> bool:
    return bool(_KRX_PREFERRED_CODE_RE.fullmatch(normalize_portfolio_code(code)))


def common_stock_code(code: str | None) -> str:
    normalized = normalize_portfolio_code(code)
    return normalized[:5] + "0"
