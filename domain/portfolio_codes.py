"""Portfolio security-code normalization and classification rules.

This module deliberately has no route, service, repository, or I/O
dependencies.  Both the persistence and service layers need these rules, so
keeping them in a neutral domain layer prevents repositories from importing
upward into ``services``.
"""

from __future__ import annotations

import re

SPECIAL_ASSETS = {"KRX_GOLD", "CRYPTO_BTC", "CRYPTO_ETH", "CRYPTO_USDT"}

_KRX_CODE_RE = re.compile(r"^[0-9][0-9A-Z]{5}$")
_KRX_PREFERRED_CODE_RE = re.compile(r"^\d{5}[1-9A-Z]$")


def normalize_portfolio_code(code: str | None) -> str:
    return (code or "").strip().upper()


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
