import pytest

from services.portfolio import targets


def test_parse_target_input_accepts_direct_number():
    parsed = targets.parse_target_input("123,456")

    assert parsed.price == 123456
    assert parsed.formula is None


def test_parse_target_input_accepts_allowed_formula():
    parsed = targets.parse_target_input(" BPS*0.4 + DPS*10 ")

    assert parsed.price is None
    assert parsed.formula == "BPS*0.4 + DPS*10"


def test_parse_target_input_rejects_unknown_variable():
    with pytest.raises(ValueError, match="사용할 수 없는 변수"):
        targets.parse_target_input("PBR*BPS")


def test_parse_target_input_rejects_function_calls():
    with pytest.raises(ValueError, match="숫자, 허용 변수"):
        targets.parse_target_input("__import__('os')")


def test_evaluate_target_formula_uses_allowed_variables():
    value = targets.evaluate_target_formula(
        "BPS*0.5 + DPS*10 + 매입가",
        {"BPS": 18000, "DPS": 250, "매입가": 1000},
    )

    assert value == 12500


def test_extract_target_variables():
    assert targets.extract_target_variables("BPS*0.5 + EPS + 매입가") == {"BPS", "EPS", "매입가"}
