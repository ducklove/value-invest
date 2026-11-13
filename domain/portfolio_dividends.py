"""Verified adjustments to put past dividends on the portfolio's share basis."""

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class DividendBasisCorrection:
    reported_dps: float
    new_listing_date: date
    old_shares_per_new_share: float


# 대교/대교우B: 2025-12-31 기준 보통주·우선주 DPS 60원, 2026-09-10 2:1 병합 변경상장.
# 배당: https://kind.krx.co.kr/external/2026/03/09/001004/20260304000281/61500.htm
# 병합: https://kind.krx.co.kr/external/2026/07/01/000274/20260630000334/91129.htm
_DAEKYO_2025 = DividendBasisCorrection(60.0, date(2026, 9, 10), 2.0)
_VERIFIED_CORRECTIONS = {
    ("019680", 2025): _DAEKYO_2025,
    ("019685", 2025): _DAEKYO_2025,
}


def dividend_on_current_share_basis(stock_code: str, year: int | None, dps: float, as_of: date) -> float:
    correction = _VERIFIED_CORRECTIONS.get((stock_code, year))
    if correction and as_of >= correction.new_listing_date:
        # Annual caches can hold either reported or already adjusted DPS.
        # Start from the disclosed amount to avoid applying the ratio twice.
        return correction.reported_dps * correction.old_shares_per_new_share
    return dps
