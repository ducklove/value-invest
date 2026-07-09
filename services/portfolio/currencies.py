"""Single home for currency / nation / FX-code mappings.

Historically these tables were scattered through ``routes/portfolio.py``;
adding one new currency meant editing several dicts in a 2,900-line file.
They are consolidated here so currency knowledge lives in one place.

Naming:
- ``nation`` is the internal 3-letter code KIS/Naver responses are mapped to
  (e.g. "USA", "DEU"), not strictly ISO-3166.
- ``fx_code`` is the Naver ``marketindexCd`` (e.g. "FX_USDKRW").

Known asymmetries (preserved verbatim from the original code — do NOT "fix"
silently, as conversion output would change):
- ``NATION_TO_CURRENCY`` maps NLD/ITA/ESP → EUR, but ``NATION_TO_FX`` has no
  entry for those nations, so ``fx_to_krw`` currently leaves a EUR amount from
  an Italian/Spanish/Dutch listing unconverted. A deliberate fix belongs in a
  separate, reviewed change.
- ``CURRENCY_TO_NATION`` lists SEK/DKK/NOK, which have no FX code here, so they
  are not convertible to KRW yet.
"""

from __future__ import annotations

# Currency (ISO) → internal nation code.
CURRENCY_TO_NATION: dict[str, str] = {
    "USD": "USA", "EUR": "DEU", "GBP": "GBR", "JPY": "JPN",
    "HKD": "HKG", "CNY": "CHN", "AUD": "AUS", "CAD": "CAN",
    "CHF": "CHE", "SEK": "SWE", "DKK": "DNK", "NOK": "NOR",
    "TWD": "TWN", "VND": "VNM",
}

# Currency (ISO) → Naver FX marketindex code.
CURRENCY_TO_FX_CODE: dict[str, str] = {
    "USD": "FX_USDKRW",
    "EUR": "FX_EURKRW",
    "JPY": "FX_JPYKRW",
    "CNY": "FX_CNYKRW",
    "HKD": "FX_HKDKRW",
    "GBP": "FX_GBPKRW",
    "AUD": "FX_AUDKRW",
    "CAD": "FX_CADKRW",
    "CHF": "FX_CHFKRW",
    "TWD": "FX_TWDKRW",
    "VND": "FX_VNDKRW",
}

# Internal nation code → currency (ISO). Several nations share EUR.
NATION_TO_CURRENCY: dict[str, str] = {
    "USA": "USD", "VNM": "VND", "JPN": "JPY", "CHN": "CNY",
    "HKG": "HKD", "GBR": "GBP", "TWN": "TWD", "AUS": "AUD",
    "CAN": "CAD", "CHE": "CHF", "DEU": "EUR", "FRA": "EUR",
    "NLD": "EUR", "ITA": "EUR", "ESP": "EUR",
}

# Internal nation code → Naver FX marketindex code.
NATION_TO_FX: dict[str, str] = {
    "USA": "FX_USDKRW", "VNM": "FX_VNDKRW", "JPN": "FX_JPYKRW",
    "CHN": "FX_CNYKRW", "HKG": "FX_HKDKRW", "GBR": "FX_GBPKRW",
    "EUR": "FX_EURKRW", "DEU": "FX_EURKRW", "FRA": "FX_EURKRW",
    "TWN": "FX_TWDKRW", "AUS": "FX_AUDKRW", "CAN": "FX_CADKRW",
    "CHE": "FX_CHFKRW",
}

# FX codes quoted per 100 units rather than per 1.
FX_UNIT: dict[str, int] = {"FX_JPYKRW": 100, "FX_VNDKRW": 100}


def infer_yf_currency(ticker: str) -> str:
    """Infer Yahoo Finance quote currency from an exchange suffix."""
    ticker = (ticker or "").upper()
    if ticker.endswith(".T"):
        return "JPY"
    if ticker.endswith(".HK"):
        return "HKD"
    if ticker.endswith((".SS", ".SZ")):
        return "CNY"
    if ticker.endswith(".L"):
        return "GBP"
    if ticker.endswith(".AX"):
        return "AUD"
    if ticker.endswith(".TO"):
        return "CAD"
    if ticker.endswith((".DE", ".F", ".PA", ".AS", ".MI", ".MC")):
        return "EUR"
    return "USD"
