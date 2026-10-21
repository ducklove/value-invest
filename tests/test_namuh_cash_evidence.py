from datetime import date
from unittest.mock import AsyncMock, patch

import pytest

from domain.cash_review import minor_limit
from repositories.broker_secrets import BrokerError
from services.brokers import activity

LINK = {"account_no": "12345678901", "account_fingerprint": "test", "environment": "live", "credential_id": "test", "product": "gold"}


def income(**extra):
    return {"trd_dt": "20261021", "cur_cd": "", "sps_cd_krl_anm": "예탁금이용료", "trd_amt": "10000",
            "int_amt": "10000", "tax_sum": "1540", "trd_orn_fee": "0", "trd_af_dca": "18460", **extra}


def test_nh_interest_summary_uses_official_fields_without_fabricating_transaction_ids():
    row = activity.cash_income_summary(income(), LINK, {"trd_af_dca": "10000"})
    assert row == {"booked_date": "2026-10-21", "date": "2026-10-21", "currency": "KRW", "kind": "interest", "amount": 8460}
    assert "source_key" not in row and "serial" not in row
    assert activity.cash_income_summary(income(), LINK, None)["amount"] == 8460
    assert activity.cash_income_summary(income(sps_cd_krl_anm="대여수수료 입금", int_amt="0"), LINK, None)["kind"] == "other_income"


@pytest.mark.parametrize("change", [{"tax_sum": "nan"}, {"trd_amt": ""}, {"trd_orn_fee": None}, {"tax_sum": "-1"},
                                      {"tax_sum": "10000"}, {"int_amt": "9000"}, {"trd_af_dca": "20000"}])
def test_invalid_or_unreconciled_income_is_not_used_as_evidence(change):
    assert activity.cash_income_summary(income(**change), LINK, {"trd_af_dca": "10000"})["amount"] is None


@pytest.mark.parametrize("label", ["RP상환이자", "CMA RP매도", "예탁금이용료취소", "이자정정", "외화예탁금이용료", "이체입금", "이자소득세"])
def test_mixed_principal_cancellations_tax_and_unknown_labels_are_not_guessed(label):
    assert activity.cash_income_summary(income(sps_cd_krl_anm=label), LINK, None) is None


def test_foreign_amounts_and_wrong_account_are_not_interpreted_as_krw_interest():
    assert activity.cash_income_summary(income(cur_cd="USD"), LINK, None) is None
    with pytest.raises(BrokerError):
        activity.cash_income_summary(income(act_no="another-account"), LINK, None)


async def test_all_pages_are_summed_and_repeated_fetch_replaces_period_instead_of_adding():
    # 동일한 날짜·유형의 원장은 거래번호 없이 합계로만 반환한다. 개인정보를 가져가지 않는다.
    pages = [{"Output_0": [income(opi_cus_fnm="private-person", cli_pe_fnm="private-sender")]},
             {"Output_0": [income(trd_af_dca="26920")]}]
    with patch.object(activity.namuh, "pages", AsyncMock(return_value=pages)):
        first = await activity.fetch("user", LINK, date(2026, 10, 21), date(2026, 10, 21))
        second = await activity.fetch("user", LINK, date(2026, 10, 21), date(2026, 10, 21))
    assert list(first) == []
    assert first.cash_summaries == second.cash_summaries
    assert first.cash_summaries[0]["amount"] == 16920
    assert "private" not in str(first.cash_summaries)


async def test_one_bad_row_prevents_partial_daily_income_evidence():
    pages = [{"Output_0": [income(), income(tax_sum="invalid")]}]
    with patch.object(activity.namuh, "pages", AsyncMock(return_value=pages)):
        batch = await activity.fetch("user", LINK, date(2026, 10, 21), date(2026, 10, 21))
    assert batch.cash_summaries == []


async def test_failed_later_page_does_not_return_partial_income_evidence():
    pages = [{"Output_0": [income()]}, {"message": {"usr_msg": "조회 실패"}}]
    with patch.object(activity.namuh, "pages", AsyncMock(return_value=pages)):
        with pytest.raises(BrokerError):
            await activity.fetch("user", LINK, date(2026, 10, 21), date(2026, 10, 21))


def test_minor_limits_have_absolute_floor_ratio_and_hard_cap():
    assert minor_limit("KRW", 0) == 1000
    assert minor_limit("KRW", 50000000) == 5000
    assert minor_limit("KRW", 100000000000) == 10000
    assert minor_limit("JPY", 10000, 10) == 100
    assert minor_limit("USD", 100, None) == 0
