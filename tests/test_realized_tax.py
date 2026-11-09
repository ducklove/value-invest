"""해외 양도세·금융소득 추정의 순수 계산. 기대값은 모두 손으로 계산한 값이다."""

from datetime import date
from decimal import Decimal

from domain import realized_tax as rules
from domain.realized_tax import CapitalGainsRule, FinancialIncomeRule
from services.portfolio import realized_tax as service


def test_capital_gains_applies_deduction_then_rate():
    # (10,000,000 − 2,500,000) × 22% = 1,650,000
    result = rules.capital_gains(10_000_000, 2026)
    assert result["deduction_used_krw"] == 2_500_000
    assert result["deduction_remaining_krw"] == 0
    assert result["taxable_krw"] == 7_500_000
    assert result["estimated_tax_krw"] == 1_650_000
    assert result["rate_pct"] == 22.0


def test_capital_gains_keeps_unused_deduction_below_limit():
    result = rules.capital_gains(1_200_000, 2026)
    assert result["deduction_used_krw"] == 1_200_000
    assert result["deduction_remaining_krw"] == 1_300_000
    assert result["taxable_krw"] == 0
    assert result["estimated_tax_krw"] == 0


def test_capital_gains_boundary_and_rounding():
    exact = rules.capital_gains(2_500_000, 2026)
    assert (exact["deduction_remaining_krw"], exact["estimated_tax_krw"]) == (0, 0)
    assert rules.capital_gains(2_500_001, 2026)["estimated_tax_krw"] == 0  # 1원 × 22% = 0.22원
    assert rules.capital_gains(2_500_010, 2026)["estimated_tax_krw"] == 2  # 10원 × 22% = 2.2원
    assert rules.capital_gains(2_500_012.5, 2026)["estimated_tax_krw"] == 3  # 12.5원 × 22% = 2.75원


def test_capital_gains_net_loss_owes_nothing_and_uses_no_deduction():
    result = rules.capital_gains(-3_000_000, 2026)
    assert result["deduction_used_krw"] == 0
    assert result["deduction_remaining_krw"] == 2_500_000
    assert result["estimated_tax_krw"] == 0


def test_rules_are_selected_by_effective_date():
    table = (CapitalGainsRule(date(2020, 1, 1), 2_500_000, Decimal("0.22")),
             CapitalGainsRule(date(2027, 1, 1), 5_000_000, Decimal("0.11")))
    assert rules.capital_gains(10_000_000, 2026, rules=table)["estimated_tax_krw"] == 1_650_000
    # (10,000,000 − 5,000,000) × 11% = 550,000
    assert rules.capital_gains(10_000_000, 2027, rules=table)["estimated_tax_krw"] == 550_000
    assert rules.capital_gains(10_000_000, 2019, rules=table) is None
    assert rules.capital_gains(10_000_000, 2019) is None
    income = (FinancialIncomeRule(date(2013, 1, 1), 20_000_000, Decimal("0.154")),
              FinancialIncomeRule(date(2027, 1, 1), 30_000_000, Decimal("0.154")))
    assert rules.financial_income(2026, dividends_krw=25_000_000, interest_krw=0, rules=income)["status"] == "over"
    assert rules.financial_income(2027, dividends_krw=25_000_000, interest_krw=0, rules=income)["status"] == "near"


def test_loss_harvest_reduces_tax_per_stock_and_in_total():
    positions = [{"stock_code": "A", "unrealized_krw": -2_000_000}, {"stock_code": "B", "unrealized_krw": -9_000_000},
                 {"stock_code": "C", "unrealized_krw": 1_000_000}]
    result = rules.loss_harvest(10_000_000, positions, 2026)
    assert result["current_tax_krw"] == 1_650_000
    assert [row["stock_code"] for row in result["candidates"]] == ["B", "A"]  # 이익 종목은 제외, 손실이 큰 순서
    # B만: 실현 1,000,000 → 세액 0 → 1,650,000 감소(현재 세액을 넘지 않는다)
    assert result["candidates"][0]["tax_saving_krw"] == 1_650_000
    # A만: 실현 8,000,000 → (8,000,000 − 2,500,000) × 22% = 1,210,000 → 440,000 감소
    assert result["candidates"][1]["tax_saving_krw"] == 440_000
    assert result["total_loss_krw"] == -11_000_000
    assert result["max_saving_krw"] == 1_650_000
    assert result["tax_after_all_krw"] == 0
    assert result["loss_to_zero_tax_krw"] == 7_500_000


def test_loss_harvest_saves_nothing_without_taxable_gain():
    result = rules.loss_harvest(1_000_000, [{"stock_code": "A", "unrealized_krw": -500_000}], 2026)
    assert result["current_tax_krw"] == 0
    assert result["candidates"][0]["tax_saving_krw"] == 0
    assert result["max_saving_krw"] == 0


def test_interest_is_grossed_up_with_withholding_rate():
    # 세후 846원 ÷ (1 − 15.4%) = 세전 1,000원
    assert rules.gross_interest(846, 2026) == 1000
    assert rules.gross_interest(84_600, 2026) == 100_000


def test_financial_income_status_thresholds():
    def status(**values):
        return rules.financial_income(2026, **{"dividends_krw": 0, "interest_krw": 0, **values})

    low = status(dividends_krw=12_000_000, interest_krw=500_000, upcoming_krw=3_000_000)
    assert low["received_krw"] == 12_500_000
    assert low["projected_krw"] == 15_500_000
    assert low["ratio_pct"] == 77.5
    assert low["remaining_krw"] == 4_500_000
    assert low["status"] == "ok"
    assert status(dividends_krw=16_000_000)["status"] == "near"  # 80%
    assert status(dividends_krw=15_999_999)["status"] == "ok"
    # 지급일 미확인 일정은 합계에는 넣지 않고 근접 판단에만 쓴다.
    uncertain = status(dividends_krw=15_500_000, unscheduled_krw=500_000)
    assert (uncertain["projected_krw"], uncertain["status"]) == (15_500_000, "near")
    # 2,000만원 '초과'가 기준이므로 정확히 같으면 근접이다.
    assert status(dividends_krw=20_000_000)["status"] == "near"
    over = status(dividends_krw=20_000_001)
    assert (over["status"], over["remaining_krw"]) == ("over", 0)


def test_upcoming_income_counts_only_nh_share_of_remaining_schedule():
    today = date(2026, 11, 8)
    events = [
        # 전 계좌 10주 중 NH 6주: 100,000원 중 60,000원만 합계, 40,000원은 NH 외.
        {"date": "2026-12-10", "stock_code": "AAPL", "expected_amount_krw": 100_000, "shares": 10, "cashflow": True, "date_status": "announced"},
        {"date": "2026-11-20", "stock_code": "GOOGL.O", "expected_amount_krw": 50_000, "shares": 5, "cashflow": True, "date_status": "estimated"},
        {"date": "2026-12-01", "stock_code": "005930", "expected_amount_krw": 30_000, "shares": 3, "cashflow": False, "date_status": "observed"},
        {"date": "2026-11-08", "stock_code": "AAPL", "expected_amount_krw": 999, "shares": 10, "cashflow": True},  # 오늘 이전
        {"date": "2027-01-05", "stock_code": "AAPL", "expected_amount_krw": 999, "shares": 10, "cashflow": True},  # 내년
        {"date": "2026-11-30", "stock_code": "AAPL", "expected_amount_krw": 999, "shares": 10, "cashflow": True, "nh_match": {"date": "2026-11-05"}},  # 이미 입금
        {"date": "2026-11-30", "stock_code": "AAPL", "expected_amount_krw": 999, "shares": 10, "cashflow": True, "date_status": "nh"},
        {"date": "2026-12-20", "stock_code": "MSFT", "expected_amount_krw": None, "shares": 1, "cashflow": True},
        {"date": "2026-12-21", "stock_code": "TSLA", "expected_amount_krw": 70_000, "shares": 7, "cashflow": True},  # NH 보유 없음
    ]
    result = rules.upcoming_income(events, {"AAPL": 6, "GOOGL": 5, "005930": 3}, today)
    assert result["upcoming_krw"] == 110_000  # 60,000 + 50,000 (표기가 달라도 같은 종목)
    assert result["unscheduled_krw"] == 30_000
    assert result["other_accounts_krw"] == 110_000  # 40,000 + 70,000
    assert (result["scheduled"], result["unscheduled"], result["estimated"], result["unknown_amount"]) == (2, 1, 1, 1)


def test_summarize_year_excludes_unconfirmed_krw_and_counts_flags():
    rows = [
        {"market": "overseas", "stock_code": "AAPL", "stock_name": "애플", "currency": "USD", "realized_krw": 4_191_600.0, "realized_native": 2_994.0, "issues": []},
        {"market": "overseas", "stock_code": "AAPL", "stock_name": "애플", "currency": "USD", "realized_krw": 100.0, "realized_native": 1.0, "issues": ["arith_mismatch"]},
        {"market": "overseas", "stock_code": "TSLA", "stock_name": "테슬라", "currency": "USD", "realized_krw": None, "realized_native": -1_004.0, "issues": ["fx_implausible"]},
        {"market": "overseas", "stock_code": "", "stock_name": "", "currency": "HKD", "realized_krw": 50.0, "realized_native": 5.0, "issues": ["detail_pending"]},
        {"market": "domestic", "stock_code": "005930", "stock_name": "삼성전자", "currency": "KRW", "realized_krw": 100_000.0, "realized_native": 100_000.0, "issues": []},
    ]
    result = rules.summarize_year(rows)
    overseas = result["overseas"]
    assert overseas["realized_krw"] == 4_191_750.0  # 4,191,600 + 100 + 50, 환율 비정상 행 제외
    assert (overseas["rows"], overseas["excluded_rows"], overseas["review_rows"], overseas["pending_rows"]) == (4, 1, 1, 1)
    assert overseas["by_currency"] == [{"currency": "HKD", "realized_native": 5.0}, {"currency": "USD", "realized_native": 1_991.0}]
    assert result["domestic"]["realized_krw"] == 100_000.0
    assert result["domestic"]["by_currency"] == []
    assert result["stocks"][0]["stock_code"] == "AAPL"
    assert result["stocks"][0]["realized_krw"] == 4_191_700.0
    assert next(row for row in result["stocks"] if row["stock_code"] == "TSLA")["realized_krw"] is None


def test_approx_krw_never_reads_as_an_exact_amount():
    assert rules.approx_krw(1_650_000) == "약 165만원"
    assert rules.approx_krw(1_601_380) == "약 160만원"
    assert rules.approx_krw(-4_400_000) == "약 -440만원"
    assert rules.approx_krw(8_349) == "약 8,300원"


def test_approx_krw_zero_matches_screen():
    assert rules.approx_krw(0) == "0원"


def test_incomplete_estimate_is_said_in_action_detail():
    item = service.action_items(_summary(status="incomplete"), date(2026, 11, 8))[0]
    assert "일부 계좌·내역이 빠져 있습니다" in item["detail"]


def _summary(*, realized=1_200_000, income=None, verified=False, status="estimated", saving=0, rows=1):
    tax = rules.capital_gains(realized, 2026)
    return {"available": True, "fields_verified": verified,
            "current": {"year": 2026, "tax": tax, "tax_status": status, "overseas": {"rows": rows, "realized_krw": realized}},
            "loss_harvest": {"available": True, "position_count": 1, "max_saving_krw": saving},
            "financial_income": income}


def test_deduction_action_appears_only_in_november_and_december():
    summary = _summary()
    assert service.action_items(summary, date(2026, 10, 31)) == []
    for today in (date(2026, 11, 1), date(2026, 12, 31)):
        items = service.action_items(summary, today)
        assert [item["key"] for item in items] == ["tax:overseas:2026:remaining"]
        assert items[0]["title"] == "해외 양도차익 공제 잔여 약 130만원"
        assert items[0]["url"] == "#pfTaxWrap"
        assert "미검증" in items[0]["detail"] and "신고용" in items[0]["detail"]


def test_deduction_action_turns_into_tax_item_when_exhausted():
    item = service.action_items(_summary(realized=10_000_000, saving=440_000), date(2026, 11, 8))[0]
    assert item["key"] == "tax:overseas:2026:exhausted"
    assert item["title"] == "해외 양도세 추정 약 165만원"
    assert "최대 약 44만원" in item["detail"]
    assert "1,650,000" not in item["title"]  # 미검증 세액을 정확한 금액으로 보이지 않는다.
    verified = service.action_items(_summary(realized=10_000_000, verified=True), date(2026, 11, 8))[0]
    assert verified["title"] == "해외 양도세 추정 1,650,000원"
    assert "미검증" not in verified["detail"]
    assert "신고용" in verified["detail"]


def test_deduction_action_needs_collected_overseas_context():
    missing_tax = _summary()
    missing_tax["current"]["tax"] = None
    assert service.action_items(missing_tax, date(2026, 11, 8)) == []
    no_overseas = _summary(rows=0)
    no_overseas["loss_harvest"]["position_count"] = 0
    assert service.action_items(no_overseas, date(2026, 11, 8)) == []
    assert service.action_items({"available": False}, date(2026, 11, 8)) == []


def test_financial_income_action_marks_near_and_over_with_distinct_keys():
    def income(dividends):
        return {**rules.financial_income(2026, dividends_krw=dividends, interest_krw=0, upcoming_krw=1_000_000), "year": 2026}

    assert service.action_items(_summary(income=income(10_000_000)), date(2026, 3, 1)) == []
    near = service.action_items(_summary(income=income(15_000_000)), date(2026, 3, 1))[0]
    assert (near["key"], near["severity"]) == ("tax:financial-income:2026:near", "watch")
    assert near["title"] == "금융소득 2,000만원 근접 (80%)"
    assert "NH 외 계좌는 포함하지 않았고" in near["detail"]
    over = service.action_items(_summary(income=income(25_000_000)), date(2026, 3, 1))[0]
    assert (over["key"], over["severity"]) == ("tax:financial-income:2026:over", "high")
    assert over["title"] == "금융소득 2,000만원 초과 예상 (130%)"
