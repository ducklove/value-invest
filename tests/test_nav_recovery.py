import copy

import pytest

from deploy.repairs.repair_2026_09_23_missing_nav import holdings_signature, valued_rows


def candidate():
    return {
        "date": "2026-09-23", "cutoff": "2026-09-23T20:00:00",
        "items": [{"stock_code": "ABC", "quantity": 2, "avg_price": 10,
                   "avg_price_currency": "USD", "currency": "USD"}],
        "quotes": {"ABC": {"price": 30000, "source": "dated source"}},
        "fx": {"USD": {"rate": 1300}},
    }


def test_recovery_values_price_and_cost_in_krw():
    rows = valued_rows(candidate())
    assert rows[0]["market_value"] == 60000
    assert rows[0]["cost_basis"] == 26000
    assert rows[0]["priced_from_fallback"] is False


@pytest.mark.parametrize("bad", ["missing", "nan", "undated", "wrong_day"])
def test_recovery_refuses_incomplete_or_invalid_evidence(bad):
    data = candidate()
    if bad == "missing": data["quotes"].clear()
    if bad == "nan": data["quotes"]["ABC"]["price"] = float("nan")
    if bad == "undated": data["quotes"]["ABC"]["source"] = ""
    if bad == "wrong_day": data["date"] = "2026-09-24"
    with pytest.raises(ValueError):
        valued_rows(data)


def test_recovery_signature_detects_real_position_changes_only():
    rows = candidate()["items"]
    updated = copy.deepcopy(rows)
    updated[0]["updated_at"] = "2026-09-24"
    assert holdings_signature(rows) == holdings_signature(updated)
    updated[0]["quantity"] = 3
    assert holdings_signature(rows) != holdings_signature(updated)
