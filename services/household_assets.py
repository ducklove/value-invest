"""Reference data and defaults for household-wide asset management.

The wealth thresholds come from Statistics Korea's 2025 Household Finance and
Welfare Survey appendix, table 12.  Official published thresholds stop at P90;
the optional upper-tail curve is therefore labelled as an estimate everywhere
it is exposed to the UI.
"""

from __future__ import annotations

ASSET_CATEGORIES = {
    "real_estate": {"label": "부동산", "kind": "asset", "retirement_default": False},
    "cash": {"label": "현금·예적금", "kind": "asset", "retirement_default": True},
    "pension": {"label": "연금·퇴직금", "kind": "asset", "retirement_default": True},
    "insurance": {"label": "보험 환급금", "kind": "asset", "retirement_default": True},
    "business": {"label": "사업·비상장 지분", "kind": "asset", "retirement_default": False},
    "vehicle": {"label": "자동차·귀중품", "kind": "asset", "retirement_default": False},
    "other": {"label": "기타 자산", "kind": "asset", "retirement_default": False},
    "liability": {"label": "대출·부채", "kind": "liability", "retirement_default": True},
}

OWNER_LABELS = {
    "household": "가구 공동",
    "self": "본인",
    "spouse": "배우자",
    "other": "기타",
}

DEFAULT_RETIREMENT_PROFILE = {
    "household_type": "couple",
    "current_age": None,
    "retirement_age": 65,
    "plan_to_age": 90,
    "monthly_spending": 2_981_000,
    "monthly_public_pension": 0,
    "monthly_other_income": 0,
    "monthly_contribution": 0,
    "annual_return_pct": 4.0,
    "inflation_pct": 2.0,
}


WEALTH_DISTRIBUTION = {
    "title": "2025년 가계금융복지조사",
    "measure": "가구 순자산",
    "as_of": "2025-03-31",
    "published_at": "2025-12-04",
    "unit": "KRW",
    "mean": 471_437_032,
    "median": 238_600_000,
    "official_percentiles": [
        {"percentile": 10, "amount": 12_100_000},
        {"percentile": 20, "amount": 51_080_000},
        {"percentile": 30, "amount": 102_960_000},
        {"percentile": 40, "amount": 164_720_000},
        {"percentile": 50, "amount": 238_600_000},
        {"percentile": 60, "amount": 330_500_000},
        {"percentile": 70, "amount": 461_800_000},
        {"percentile": 80, "amount": 693_800_000},
        {"percentile": 90, "amount": 1_100_200_000},
    ],
    "top_quintile_mean": 1_520_854_320,
    # Generalized Pareto excess model above P80.  It is calibrated to the
    # official P80/P90 boundaries and the official top-quintile mean.  These
    # values support a smooth visual estimate only; they are not official
    # Statistics Korea percentile boundaries.
    "estimated_tail": {
        "threshold_percentile": 80,
        "threshold_amount": 693_800_000,
        "shape": 0.3804559967071106,
        "scale": 512_396_544.4154327,
        "calibration": "official P80, P90 and top-quintile mean",
    },
    # Appendix tables 2-4, all-household averages.  Shares use total assets as
    # the denominator, so financial + real estate + other physical = 100%.
    "asset_composition": {
        "total_assets": 566_775_030,
        "financial_assets": {"amount": 136_898_461, "share_pct": 24.15},
        "real_estate": {"amount": 402_983_360, "share_pct": 71.10},
        "other_physical_assets": {"amount": 26_893_209, "share_pct": 4.74},
        "as_of": "2025-03-31",
        "source_note": "부록 통계표 2~4의 전체가구 평균",
    },
    "source_url": "https://www.mods.go.kr/board.es?act=view&bid=215&list_no=439535&mid=a10301040300",
    "source_note": "부록 통계표 12의 2025년 순자산 분위 경계값",
}


RETIREMENT_REFERENCE = {
    "title": "2024년 국민노후보장패널조사 제10차 부가조사",
    "survey_year": 2024,
    "published_at": "2025-12-31",
    "adequate_monthly_spending": {"single": 1_976_000, "couple": 2_981_000},
    "minimum_monthly_spending": {"single": 1_392_000, "couple": 2_166_000},
    "average_retirement_start_age": 68.5,
    "source_url": (
        "https://m.nps.or.kr/pnsgdnc/nscvrgdata/getOHAE0002M1.do?"
        "hmpgBbsCd=BS20240145&hmpgCd=01&menuId=MN24000898&pstId=ZZ202500000000001624"
    ),
    "source_note": "50세 이상 가구원이 있는 5,138가구·8,394명 조사",
}


def reference_payload() -> dict:
    """Return JSON-safe copies so callers cannot mutate module constants."""

    return {
        "categories": {key: dict(value) for key, value in ASSET_CATEGORIES.items()},
        "owners": dict(OWNER_LABELS),
        "distribution": {
            **WEALTH_DISTRIBUTION,
            "official_percentiles": [dict(row) for row in WEALTH_DISTRIBUTION["official_percentiles"]],
            "estimated_tail": dict(WEALTH_DISTRIBUTION["estimated_tail"]),
            "asset_composition": {
                **WEALTH_DISTRIBUTION["asset_composition"],
                "financial_assets": dict(WEALTH_DISTRIBUTION["asset_composition"]["financial_assets"]),
                "real_estate": dict(WEALTH_DISTRIBUTION["asset_composition"]["real_estate"]),
                "other_physical_assets": dict(WEALTH_DISTRIBUTION["asset_composition"]["other_physical_assets"]),
            },
        },
        "retirement_reference": {
            **RETIREMENT_REFERENCE,
            "adequate_monthly_spending": dict(RETIREMENT_REFERENCE["adequate_monthly_spending"]),
            "minimum_monthly_spending": dict(RETIREMENT_REFERENCE["minimum_monthly_spending"]),
        },
    }
