"""화면 기준일과 계산액이 계좌 대조 결과에 영향을 받지 않는지 검증한다."""

import pytest

from domain.dividend_calendar_display import display_values


@pytest.mark.parametrize("record_day, expected", [
    ("2026-09-30", "2026-09-28"),
    ("2026-09-28", "2026-09-22"),  # 추석 휴장일과 주말을 건너뛴다.
    ("2026-06-30", "2026-06-26"),
])
def test_domestic_basis_is_two_trading_days_before_record(record_day, expected):
    raw = {"stock_code": "005935.KS", "record_date": record_day}
    result = display_values(raw)
    assert result["dividend_basis_date"] == expected
    assert result["dividend_basis_rule"] == "krx_record_t2"
    assert raw["record_date"] == record_day


def test_ex_date_is_the_basis_even_when_record_and_payment_dates_are_available():
    result = display_values({"stock_code": "AAPL", "ex_date": "2026-09-08", "record_date": "2026-09-09", "pay_date": "2026-09-15"})
    assert (result["dividend_basis_date"], result["dividend_basis_rule"]) == ("2026-09-08", "ex_date")
    # 지급일만 있으면 권리 기준일을 유추하지 않는다.
    assert display_values({"stock_code": "83188.HK", "pay_date": "2026-09-03"})["dividend_basis_date"] is None
    assert display_values({"stock_code": "AAA.AX", "date_kind": "ex_date", "date": "2026-09-01"})["dividend_basis_date"] == "2026-09-01"


@pytest.mark.parametrize("code,currency,dps,shares,expected", [
    ("005930", "KRW", 370, 100, (37000, 5698, 31302)),
    ("GOOGL.O", "USD", 0.21, 15, (3.15, 0.47, 2.68)),
    ("AAPL", "USD", 0.333, 5, (1.67, 0.25, 1.42)),
    ("83188.HK", "CNY", 0.12, 100, (12, 1.72, 10.28)),
    ("02800.HK", "HKD", 0.12, 100, (12, 1.84, 10.16)),
    ("AAA.AX", "AUD", 0.2, 100, (20, 0, 20)),  # 수취 입력의 OTHER 기본값.
    ("005930", "KRW", 0, 100, (0, 0, 0)),
])
def test_calculated_amounts_use_inputs_and_default_rates_not_actual_deposits(code, currency, dps, shares, expected):
    event = {"stock_code": code, "currency": currency, "amount_per_share": dps, "shares": shares,
             "verification": "nh_partial", "nh_match": {"gross_amount": 999, "tax_amount": 99, "net_amount": 900}}
    result = display_values(event)
    assert tuple(result[key] for key in ("calculated_gross_amount", "calculated_tax_amount", "calculated_net_amount")) == expected
    assert event["nh_match"]["net_amount"] == 900


@pytest.mark.parametrize("missing", [{"shares": None}, {"amount_per_share": None}, {"amount_status": "unknown"}, {"shares": float("nan")}])
def test_missing_inputs_do_not_become_zero_or_actual_deposit_amounts(missing):
    event = {"stock_code": "005935", "currency": "KRW", "amount_per_share": 370, "shares": 100,
             "gross_amount": 37400, "tax_amount": 5750, "net_amount": 31650, **missing}
    result = display_values(event)
    assert all(result[key] is None for key in ("calculated_gross_amount", "calculated_tax_amount", "calculated_net_amount"))
