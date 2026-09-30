"""domain.numbers.parse_number — 1단계(콤마 제거 파서) 표 기반 계약."""

import math
from decimal import Decimal

import pytest

from domain.numbers import parse_number

CASES = [
    (None, None),
    ("", None),
    ("1,234", 1234.0),
    (" 1,234.5 ", 1234.5),
    ("-1,000", -1000.0),
    ("-", None),
    ("abc", None),
    ("12%", None),
    (0, 0.0),
    ("0", 0.0),
    (42, 42.0),
    (3.5, 3.5),
    (Decimal("7.25"), 7.25),
    (True, None),
    ([], None),
]


@pytest.mark.parametrize(("value", "expected"), CASES)
def test_parse_number_table(value, expected):
    assert parse_number(value) == expected


def test_parse_number_passes_nan_and_inf_through():
    assert math.isnan(parse_number("NaN"))
    assert math.isnan(parse_number(float("nan")))
    assert parse_number("inf") == math.inf


@pytest.mark.parametrize("value", [0, "0", "0.0", "0,000"])
def test_zero_as_none(value):
    assert parse_number(value, zero_as_none=True) is None
    assert parse_number(value) == 0.0


def test_hub_copies_are_the_single_parser():
    import snapshot_nav
    from services import daily_briefing
    from services.market.sources import close_price as close_price_client

    assert snapshot_nav._safe_float is parse_number
    assert daily_briefing._safe_float is parse_number
    assert close_price_client.parse_number is parse_number
