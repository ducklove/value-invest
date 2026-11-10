"""services/portfolio/exposure.py 순수 계산 테스트.

기대값은 손으로 계산했다. 수식 규약(단순수익률, 표본 공분산 n-1, stdev × sqrt(252),
HHI = Σ w² 등)을 바꾸면 여기 값도 의도적으로 함께 바꿔야 한다.
"""

import json
import math
from datetime import date, datetime, timedelta

import pytest

from domain.timeutil import KST
from services.portfolio import exposure

EPS = 1e-3


def approx(value, abs_tol=EPS):
    return pytest.approx(value, abs=abs_tol)


# ---------------------------------------------------------------------------
# 발행사 코드
# ---------------------------------------------------------------------------

def test_issuer_code_prefers_published_pair_over_code_rule():
    pairs = {"005935": "005930", "03473K": "034730"}
    assert exposure.issuer_code("005935", pairs) == "005930"
    assert exposure.issuer_code("03473K", pairs) == "034730"
    # 쌍 목록에 없는 우선주는 끝자리 0 규칙으로 합친다.
    assert exposure.issuer_code("000157", pairs) == "000150"
    # 보통주·ETF·스팩 코드는 그대로다.
    assert exposure.issuer_code("000660", pairs) == "000660"
    assert exposure.issuer_code("0074K0", pairs) == "0074K0"
    assert exposure.issuer_code("0209J0", pairs) == "0209J0"


def test_issuer_code_rule_needs_a_listed_common_when_list_is_known():
    # ETN 처럼 끝자리만 0 이 아닌 코드는 상장법인 목록에 보통주가 없으면 합치지 않는다.
    companies = {"000150": {"name": "두산"}}
    assert exposure.issuer_code("000157", None, companies) == "000150"
    assert exposure.issuer_code("530031", None, companies) == "530031"


def test_issuer_code_unifies_foreign_listing_spellings_and_share_classes():
    assert exposure.issuer_code("GOOGL.O") == "GOOGL"
    assert exposure.issuer_code("GOOG") == "GOOGL"
    assert exposure.issuer_code("BRK.B") == "BRK-B"
    assert exposure.issuer_code("BRK-A") == "BRK-B"
    assert exposure.issuer_code("FUEVFVND.HM") == "FUEVFVND.VN"


@pytest.mark.parametrize(
    ("raw", "normalized", "is_issuer"),
    [
        ("005930", "005930", True),
        ("NVDA", "NVDA", True),
        ("BRK.B", "BRK.B", True),
        ("7203.T", "7203.T", True),
        ("ETR: SIE", "ETR:SIE", True),
        ("n/a", None, False),
        ("-", None, False),
        (None, None, False),
        ("B.0 10.20.26", "B.0 10.20.26", False),
        ("TII.1.875 07.15.35", "TII.1.875 07.15.35", False),
        ("META.4.75 08.15.34", "META.4.75 08.15.34", False),
    ],
)
def test_constituent_ticker_rules(raw, normalized, is_issuer):
    assert exposure.normalize_constituent_ticker(raw) == normalized
    assert exposure.is_issuer_ticker(normalized) is is_issuer


# ---------------------------------------------------------------------------
# ETF 풀어 보기
# ---------------------------------------------------------------------------

def test_flatten_etf_hand_calculated_with_nested_etf_and_pseudo_rows():
    etfs = {
        "AAA": {
            "unweighted": 1,
            "rows": [
                ["005930", "삼성전자", 30.0],
                ["BBB", "안쪽 ETF", 20.0],
                [None, "원화현금", 5.0],
                [None, "설정현금액", 100.0],  # 설정 단위 의사 행 — 빠져야 한다.
                ["000660", "SK하이닉스", None],
            ],
        },
        "BBB": {"rows": [["000660", "SK하이닉스", 50.0], ["005935", "삼성전자우", 10.0]]},
    }
    flat = exposure.flatten_etf("AAA", etfs, preferred_pairs={"005935": "005930"})
    # 005930 = 30 + 20 × 10% = 32, 000660 = 20 × 50% = 10.
    assert flat["issuers"]["005930"]["pct"] == approx(32.0)
    assert flat["issuers"]["000660"]["pct"] == approx(10.0)
    assert flat["other_pct"] == approx(5.0)
    # 커버리지 = 30 + 20 × 60% + 5 = 47 (설정현금액과 비중 없는 행은 세지 않는다).
    assert flat["covered_pct"] == approx(47.0)
    assert flat["unweighted"] == 1


def test_flatten_etf_guards_cycles_and_keeps_leverage_uncapped_in_gross():
    etfs = {
        "AAA": {"rows": [["BBB", "b", 100.0], ["005930", "삼성전자", 90.0]]},
        "BBB": {"rows": [["AAA", "a", 100.0], ["000660", "SK하이닉스", 50.0]]},
    }
    flat = exposure.flatten_etf("AAA", etfs)
    # BBB 안의 AAA 는 다시 펴지 않고 일반 행으로 처리되어 끝난다.
    assert flat["issuers"]["005930"]["pct"] == approx(90.0)
    assert flat["issuers"]["000660"]["pct"] == approx(50.0)
    assert flat["gross_pct"] == approx(240.0)
    assert flat["covered_pct"] == 100.0


def test_flatten_etf_nets_negative_weights_and_treats_bond_lines_as_other():
    etfs = {"SQ": {"rows": [["NVDA", "NVIDIA", -30.0], ["n/a", "Swap", -40.0], ["T.4.5 02.15.36", "Note", 10.0]]}}
    flat = exposure.flatten_etf("SQ", etfs)
    assert flat["issuers"]["NVDA"]["pct"] == approx(-30.0)
    assert flat["other_pct"] == approx(-30.0)
    assert flat["gross_pct"] == approx(80.0)


@pytest.mark.parametrize(
    ("entry", "expected"),
    [
        ({"name": "KODEX 200", "asset_class": "주식"}, exposure.ASSET_EQUITY),
        ({"name": "SCHP", "asset_class": "채권"}, exposure.ASSET_BOND),
        ({"name": "혼합", "asset_class": "혼합자산"}, exposure.ASSET_MIXED),
        ({"name": "abrdn Physical Platinum Shares ETF", "asset_class": "주식"}, exposure.ASSET_COMMODITY),
        ({"name": "BetaShares Australian High Interest Cash ETF", "asset_class": "주식"}, exposure.ASSET_BOND),
        ({"name": "AAA", "full_name": "State Street 1-3 Month T-Bill ETF", "asset_class": "주식"}, exposure.ASSET_BOND),
        ({"name": "iShares Bitcoin Trust ETF", "asset_class": "대체자산"}, exposure.ASSET_CRYPTO),
        ({"name": "SPDR Gold Shares", "asset_class": "대체자산"}, exposure.ASSET_COMMODITY),
        ({"name": "KODEX 미국달러선물", "asset_class": "ETF"}, exposure.ASSET_OTHER_ETF),
        (None, exposure.ASSET_ETF_UNKNOWN),
    ],
)
def test_etf_asset_class_corrects_obvious_mislabels(entry, expected):
    assert exposure.etf_asset_class(entry) == expected


# ---------------------------------------------------------------------------
# 집중도
# ---------------------------------------------------------------------------

def test_concentration_hand_calculated():
    metrics = exposure.concentration_metrics([50.0, 30.0, 20.0])
    assert metrics["top1_pct"] == approx(50.0)
    assert metrics["top3_pct"] == approx(100.0)
    assert metrics["top5_pct"] == approx(100.0)
    # 50² + 30² + 20² = 3800, 유효 종목 수 = 10000 / 3800.
    assert metrics["hhi"] == approx(3800.0)
    assert metrics["effective_n"] == approx(2.63, 0.005)
    assert metrics["count"] == 3


def test_concentration_residual_only_enters_denominator():
    # 미공개 20 은 분모에만 든다 — 완전 분산 가정이라 HHI 는 하한이다.
    metrics = exposure.concentration_metrics([50.0, 30.0], residual=20.0)
    assert metrics["top1_pct"] == approx(50.0)
    assert metrics["top3_pct"] == approx(80.0)
    assert metrics["hhi"] == approx(3400.0)


def test_concentration_uses_magnitude_for_shorts_and_null_when_empty():
    metrics = exposure.concentration_metrics([75.0, -25.0])
    assert metrics["top1_pct"] == approx(75.0)
    assert metrics["hhi"] == approx(6250.0)
    empty = exposure.concentration_metrics([0.0, 0.0])
    assert empty["hhi"] is None and empty["top1_pct"] is None and empty["effective_n"] is None
    assert empty["count"] == 0


# ---------------------------------------------------------------------------
# 위험 모델 — 손계산 기준: 수익률 A = +2%, -1%, +3% / B = +1%, +1%, -2%
# ---------------------------------------------------------------------------

DATES = ["2026-01-02", "2026-01-05", "2026-01-06", "2026-01-07"]
PRICES_A = [100.0, 102.0, 100.98, 104.0094]
PRICES_B = [200.0, 202.0, 204.02, 199.9396]


def _rows(prices, dates=DATES):
    return [{"date": d, "close": p} for d, p in zip(dates, prices, strict=True)]


def test_risk_model_hand_calculated_contributions_and_correlation():
    positions = [
        {"code": "A", "name": "에이", "value": 750.0, "series_code": "A"},
        {"code": "B", "name": "비", "value": 250.0, "series_code": "B"},
    ]
    result = exposure.compute_risk_model(
        positions, {"A": _rows(PRICES_A), "B": _rows(PRICES_B)}, min_returns=3, min_history_ratio=0.5,
    )
    assert result["available"] is True
    assert result["sample_returns"] == 3
    assert (result["start_date"], result["end_date"]) == ("2026-01-02", "2026-01-07")
    # Var(A)=4.3333, Var(B)=3, Cov=-2.5 (%²), w=(0.75, 0.25)
    # Var_p = 0.5625×4.3333 + 0.0625×3 + 2×0.1875×(-2.5) = 1.6875 → 일 1.29904%
    assert result["portfolio_volatility_pct"] == approx(1.29904 * math.sqrt(252))
    by_code = {row["code"]: row for row in result["rows"]}
    # RC_A = 0.75 × (0.75×4.3333 + 0.25×(-2.5)) / 1.6875 = 116.667%
    assert by_code["A"]["risk_contribution_pct"] == approx(116.6667, 0.01)
    assert by_code["B"]["risk_contribution_pct"] == approx(-16.6667, 0.01)
    assert by_code["A"]["weight_pct"] == approx(75.0)
    assert by_code["A"]["volatility_pct"] == approx(math.sqrt(4.33333) * math.sqrt(252), 0.01)
    assert sum(row["risk_contribution_pct"] for row in result["rows"]) == approx(100.0, 0.01)
    # 상관 = -2.5 / sqrt(13) = -0.69338. 자기 상관은 1.
    corr = result["correlation"]
    assert corr["codes"] == ["A", "B"]
    assert corr["matrix"][0][0] == approx(1.0)
    assert corr["matrix"][0][1] == approx(-0.69338)
    assert corr["matrix"][1][0] == approx(-0.69338)
    # 분산 비율 = (0.75×2.08167 + 0.25×1.73205) / 1.29904 = 1.53518
    assert result["diversification_ratio"] == approx(1.53518)
    # 두 종목이면 내재 평균 상관 = 실제 상관.
    assert result["implied_avg_correlation"] == approx(-0.69338)
    assert result["coverage_pct"] == approx(100.0)


def test_risk_model_uses_date_intersection_and_consecutive_returns():
    # A 는 01-06 이 비어 있다 → B 도 01-05 → 01-07 두 영업일 수익률로 계산한다.
    positions = [
        {"code": "A", "value": 500.0, "series_code": "A"},
        {"code": "B", "value": 500.0, "series_code": "B"},
    ]
    closes = {
        "A": [{"date": "2026-01-02", "close": 100.0}, {"date": "2026-01-05", "close": 110.0},
              {"date": "2026-01-07", "close": 99.0}],
        "B": _rows([100.0, 105.0, 200.0, 94.5]),
    }
    result = exposure.compute_risk_model(positions, closes, min_returns=2, min_history_ratio=0.5)
    assert result["sample_returns"] == 2
    # A 수익률 = (+10%, -10%), B = (+5%, -10%). 01-06 의 200 은 교집합 밖이라 무시된다.
    assert result["rows"][0]["volatility_pct"] is not None
    assert result["correlation"]["matrix"][0][1] == approx(1.0)


def test_risk_model_reports_null_not_zero_for_short_samples():
    long_dates = [f"2026-01-{day:02d}" for day in range(1, 27)]
    closes = {
        "LONG": [{"date": d, "close": 100.0 + i} for i, d in enumerate(long_dates)],
        "NEW": [{"date": d, "close": 50.0 + i} for i, d in enumerate(long_dates[-5:])],
    }
    positions = [
        {"code": "LONG", "name": "오래된", "value": 900.0, "series_code": "LONG"},
        {"code": "NEW", "name": "신규상장", "value": 50.0, "series_code": "NEW"},
        {"code": "VOO", "name": "해외", "value": 50.0, "series_code": None},
        {"code": "GONE", "name": "응답 없음", "value": 0.5, "series_code": "GONE"},
    ]
    result = exposure.compute_risk_model(positions, closes, min_returns=20)
    assert [row["code"] for row in result["rows"]] == ["LONG"]
    assert result["rows"][0]["risk_contribution_pct"] == approx(100.0)
    assert result["sample_returns"] == 25
    excluded = {row["code"]: row for row in result["excluded"]}
    assert excluded["NEW"]["reason"] == "short_history"
    assert excluded["NEW"]["sample_points"] == 5
    assert excluded["VOO"]["reason"] == "no_series"
    assert excluded["GONE"]["reason"] == "no_rows"
    assert result["correlation"] is None  # 한 종목으로는 상관이 없다.
    # 커버리지 = 900 / 1000.5
    assert result["coverage_pct"] == approx(89.955, 0.01)


def test_risk_model_null_when_no_daily_series_or_no_variance():
    positions = [{"code": "A", "value": 1.0, "series_code": "A"}]
    unavailable = exposure.compute_risk_model(positions, None)
    assert unavailable["available"] is False
    assert unavailable["portfolio_volatility_pct"] is None
    assert unavailable["rows"] == []
    assert unavailable["excluded_count"] == 1

    too_short = exposure.compute_risk_model(positions, {"A": _rows(PRICES_A)}, min_returns=20)
    assert too_short["available"] is False
    assert too_short["portfolio_volatility_pct"] is None
    assert too_short["excluded"][0]["reason"] == "short_history"

    flat_dates = [f"2026-01-{day:02d}" for day in range(1, 25)]
    flat = exposure.compute_risk_model(
        [{"code": "A", "value": 1.0, "series_code": "A"}, {"code": "B", "value": 1.0, "series_code": "B"}],
        {code: [{"date": d, "close": 100.0} for d in flat_dates] for code in ("A", "B")},
    )
    # 수익률이 전부 0 이면 변동성은 실제로 0 이지만, 비율은 정의되지 않으므로 null 이다.
    assert flat["portfolio_volatility_pct"] == 0
    assert all(row["risk_contribution_pct"] is None for row in flat["rows"])
    assert flat["diversification_ratio"] is None
    assert flat["correlation"]["matrix"][0][1] is None


def test_risk_model_hedge_leg_has_negative_contribution_and_sums_to_100():
    # 롱 B(0.5) + 같은 일봉을 대리로 쓰는 숏(-0.25): 순비중으로 위험이 줄고 기여가 음수다.
    dates = [f"2026-02-{day:02d}" for day in range(1, 25)]
    closes = {"X": [{"date": d, "close": 100.0 + (i % 2) * 2.0} for i, d in enumerate(dates)]}
    positions = [
        {"code": "LONG", "value": 1000.0, "series_code": "X"},
        {"code": "KRFUT_S", "value": -500.0, "series_code": "X"},
    ]
    result = exposure.compute_risk_model(positions, closes)
    by_code = {row["code"]: row for row in result["rows"]}
    assert by_code["LONG"]["risk_contribution_pct"] == approx(200.0)
    assert by_code["KRFUT_S"]["risk_contribution_pct"] == approx(-100.0)
    assert by_code["KRFUT_S"]["proxy_code"] == "X"
    assert by_code["LONG"]["weight_pct"] == approx(200.0)


# ---------------------------------------------------------------------------
# 해외 일봉 — 원화 환산과 국내 거래일 정렬
# ---------------------------------------------------------------------------

def test_fx_symbols_use_the_dollar_rate_and_dollar_crosses():
    assert exposure.fx_symbols("USD") == ("KRW=X",)
    assert exposure.fx_symbols("jpy") == ("KRW=X", "JPY=X")
    assert exposure.fx_symbols("GBp") == ("KRW=X", "GBP=X")  # 펜스 표기도 파운드 환율이다(수익률은 단위와 무관).
    assert exposure.fx_symbols("ZAc") == ("KRW=X", "ZAR=X")
    assert exposure.fx_symbols("KRW") == ()
    assert exposure.fx_symbols(None) == ()


def test_krw_fx_rows_hand_calculated_cross_rate():
    rows = {
        "KRW=X": [["2026-11-05", 1400.0], ["2026-11-06", 1410.0], ["2026-11-09", 1420.0]],
        "CNY=X": [["2026-11-05", 7.0], ["2026-11-06", 7.05]],  # 11-09 가 비면 교차도 그날은 없다.
    }
    # 원/위안 = (원/달러) ÷ (위안/달러): 1400 ÷ 7 = 200, 1410 ÷ 7.05 = 200
    assert exposure.krw_fx_rows("CNY", rows) == [["2026-11-05", approx(200.0)], ["2026-11-06", approx(200.0)]]
    assert exposure.krw_fx_rows("USD", rows) == rows["KRW=X"]
    assert exposure.krw_fx_rows("VND", rows) is None  # VND=X 를 못 받았다.
    assert exposure.krw_fx_rows("USD", {}) is None
    assert exposure.krw_fx_rows("KRW", rows) is None


def test_foreign_series_codes_cover_only_foreign_listings():
    positions = [
        {"code": "aapl"}, {"code": "005930"}, {"code": "VWO"}, {"code": "CASH_USD"}, {"code": "KRX_GOLD"},
        {"code": "CRYPTO_BTC"}, {"code": "KRFUT_A"}, {"code": "IDX_SP500"}, {"code": "AAPL"}, {"code": ""},
    ]
    assert exposure.foreign_series_codes(positions) == ["AAPL", "VWO"]


def test_krw_asof_closes_hand_calculated_for_a_us_listing():
    # 2026-11-01 서머타임 종료 뒤라 뉴욕 16:00 = 서울 다음 날 06:00 이다.
    rows = [["2026-11-05", 100.0], ["2026-11-06", 110.0], ["2026-11-09", 99.0], ["2026-11-10", 120.0]]
    fx = [["2026-11-05", 1400.0], ["2026-11-06", 1410.0], ["2026-11-09", 1420.0], ["2026-11-10", 1430.0]]
    now = datetime(2026, 11, 10, 12, 0, tzinfo=KST)
    result = exposure.krw_asof_closes(rows, timezone="America/New_York", close_time="16:00", fx_rows=fx, now=now)
    assert result == [
        # 목요일 종가는 금요일 06:00 에 알려져 금요일 국내 마감 기준 값이다. 100 × 1400.
        {"date": "2026-11-06", "close": 140_000.0, "session_date": "2026-11-05"},
        # 금요일 종가는 토요일 06:00 → 다음 국내 거래일(월)의 값. 110 × 1410.
        {"date": "2026-11-09", "close": 155_100.0, "session_date": "2026-11-06"},
        {"date": "2026-11-10", "close": 140_580.0, "session_date": "2026-11-09"},
        # 11-10 세션은 서울 11-11 06:00 에 끝난다 — now(11-10 12:00)에는 진행 중이라 뺀다.
    ]


def test_krw_asof_closes_aligns_asian_closes_and_bridges_fx_gaps():
    # 도쿄는 서울과 같은 15:30 에 끝나 같은 날, 홍콩은 서울 17:10 에 끝나 다음 거래일 값이다.
    tokyo = exposure.krw_asof_closes(
        [["2026-11-06", 3000.0], ["2026-11-09", 3100.0]], timezone="Asia/Tokyo", close_time="15:30",
        fx_rows=[["2026-11-06", 9.0]],  # 11-09 환율이 비어 3일 전 환율을 쓴다.
    )
    assert tokyo == [
        {"date": "2026-11-06", "close": 27_000.0, "session_date": "2026-11-06"},
        {"date": "2026-11-09", "close": 27_900.0, "session_date": "2026-11-09"},
    ]
    hong_kong = exposure.krw_asof_closes([["2026-11-09", 140.0]], timezone="Asia/Hong_Kong", close_time="16:10")
    assert hong_kong == [{"date": "2026-11-10", "close": 140.0, "session_date": "2026-11-09"}]
    # 환율이 FX_MAX_GAP_DAYS 보다 오래 비면 그 날을 뺀다(원화가 아닌 값을 섞지 않는다).
    assert exposure.krw_asof_closes(
        [["2026-11-20", 50.0]], timezone="Asia/Tokyo", close_time="15:30", fx_rows=[["2026-11-09", 9.0]],
    ) == []


def test_krw_asof_closes_rejects_unknown_timezone_or_clock():
    rows = [["2026-11-09", 1.0]]
    assert exposure.krw_asof_closes(rows, timezone="Mars/Base", close_time="16:00") == []
    assert exposure.krw_asof_closes(rows, timezone="America/New_York", close_time="late") == []


def test_risk_model_carries_foreign_closes_onto_domestic_trading_days():
    # 국내 거래일 11-02~11-05. 해외 값은 11-02·11-04 에만 새로 알려져 11-03·11-05 는 직전 값을 잇는다.
    days = ["2026-11-02", "2026-11-03", "2026-11-04", "2026-11-05"]
    closes = {"005930": _rows([100.0, 110.0, 99.0, 99.0], days)}
    foreign = {"AAPL": [{"date": "2026-11-02", "close": 1000.0}, {"date": "2026-11-04", "close": 1100.0}]}
    positions = [
        {"code": "005930", "name": "삼성전자", "value": 100.0, "series_code": "005930"},
        {"code": "AAPL", "name": "애플", "value": 100.0, "series_code": "AAPL", "foreign": True},
    ]
    result = exposure.compute_risk_model(
        positions, closes, foreign_closes=foreign, min_returns=2, min_history_ratio=0.5,
    )
    assert result["available"] is True
    assert result["sample_returns"] == 3
    assert (result["start_date"], result["end_date"]) == ("2026-11-02", "2026-11-05")
    assert result["foreign_count"] == 1
    # 국내 r = (+10%, -10%, 0), 해외 r = (0, +10%, 0), w = (0.5, 0.5) → r_p = (5%, 0, 0)
    # Var_p = 1/1200, Cov(국내, p) = 1/400 → RC 150%, Cov(해외, p) = -1/1200 → RC -50%
    by_code = {row["code"]: row for row in result["rows"]}
    assert by_code["005930"]["risk_contribution_pct"] == approx(150.0)
    assert by_code["AAPL"]["risk_contribution_pct"] == approx(-50.0)
    assert by_code["AAPL"]["volatility_pct"] == approx(math.sqrt(1 / 300) * math.sqrt(252) * 100)
    assert by_code["AAPL"]["proxy_code"] is None
    assert result["portfolio_volatility_pct"] == approx(math.sqrt(1 / 1200) * math.sqrt(252) * 100)
    # 상관 = Cov(-1/200) / (0.1 × sqrt(1/300)) = -sqrt(3)/2
    assert result["correlation"]["matrix"][0][1] == approx(-math.sqrt(3) / 2)


def test_risk_model_drops_stale_foreign_series_instead_of_shortening_the_sample():
    dates = [(date(2026, 3, 2) + timedelta(days=i)).isoformat() for i in range(30)]
    closes = {"005930": [{"date": d, "close": 100.0 + (i % 3)} for i, d in enumerate(dates)]}
    # 해외 값이 표본 끝보다 열흘 넘게 묵었다 — 이어 쓰지 않고, 공통 구간을 과거로 끌지도 않는다.
    foreign = {"OLD": [{"date": d, "close": 50.0 + (i % 2)} for i, d in enumerate(dates[:15])]}
    positions = [
        {"code": "005930", "value": 100.0, "series_code": "005930"},
        {"code": "OLD", "value": 100.0, "series_code": "OLD", "foreign": True},
        {"code": "NONE", "value": 100.0, "series_code": "NONE", "foreign": True},
    ]
    result = exposure.compute_risk_model(positions, closes, foreign_closes=foreign)
    assert result["sample_returns"] == 29
    assert result["end_date"] == dates[-1]
    assert result["foreign_count"] == 0
    assert {row["code"]: row["reason"] for row in result["excluded"]} == {"OLD": "stale_rows", "NONE": "no_rows"}


def test_risk_model_runs_on_foreign_series_alone():
    # 국내 일봉이 없으면(해외 전용 계좌·finance-pi 장애) 해외 종가가 알려진 날들이 표본 날짜다.
    dates = [f"2026-03-{day:02d}" for day in range(2, 28)]
    foreign = {
        "AAPL": [{"date": d, "close": 100.0 + (i % 3)} for i, d in enumerate(dates)],
        "7203.T": [{"date": d, "close": 50.0 + (i % 2)} for i, d in enumerate(dates[1:])],
    }
    positions = [
        {"code": "AAPL", "value": 100.0, "series_code": "AAPL", "foreign": True},
        {"code": "7203.T", "value": 100.0, "series_code": "7203.T", "foreign": True},
        {"code": "005930", "value": 100.0, "series_code": "005930"},
    ]
    result = exposure.compute_risk_model(positions, None, foreign_closes=foreign)
    assert result["available"] is True
    assert result["foreign_count"] == 2
    assert result["sample_returns"] == len(dates) - 2  # 7203.T 는 첫날 값이 없다.
    assert {row["code"]: row["reason"] for row in result["excluded"]} == {"005930": "no_rows"}
    assert exposure.compute_risk_model(positions, None, foreign_closes={})["available"] is False


def test_report_sends_foreign_listings_to_the_foreign_series():
    dates = [f"2026-03-{day:02d}" for day in range(2, 28)]
    positions = [
        {"code": "005930", "name": "삼성전자", "quantity": 1, "value": 300.0, "currency": "KRW"},
        {"code": "AAPL", "name": "애플", "quantity": 1, "value": 100.0, "currency": "USD"},
        {"code": "KRX_GOLD", "name": "금", "quantity": 1, "value": 100.0, "currency": "KRW"},
    ]
    result = exposure.build_exposure_report(
        positions,
        closes={"005930": [{"date": d, "close": 100.0 + (i % 3)} for i, d in enumerate(dates)]},
        foreign_closes={"AAPL": [{"date": d, "close": 1000.0 + (i % 4)} for i, d in enumerate(dates)]},
    )
    block = result["risk"]
    assert {row["code"] for row in block["rows"]} == {"005930", "AAPL"}
    assert block["foreign_count"] == 1
    assert block["excluded"] == [{"code": "KRX_GOLD", "name": "금", "reason": "no_series", "sample_points": 0}]
    assert block["coverage_pct"] == approx(80.0)  # (300 + 100) / 500


# ---------------------------------------------------------------------------
# 보고서 — 손계산 시나리오
# ---------------------------------------------------------------------------

ETFS = {
    "069500": {"name": "KODEX 200", "asset_class": "주식", "rows": [["005930", "삼성전자", 30.0], ["000660", "SK하이닉스", 20.0]]},
    "SCHP": {"name": "Schwab TIPS", "asset_class": "채권", "rows": [["TII.1.875 07.15.35", "Note", 79.22]]},
    "VWO": {"name": "Vanguard EM", "asset_class": "주식", "rows": []},
}
HOLDCOS = {
    "000670": {
        "name": "영풍",
        "adjusted_shares": 1000,
        "subsidiaries": [{"code": "010130", "name": "고려아연", "shares_held": 200}],
    },
}
COMPANIES = {
    "005930": {"name": "삼성전자", "sector": "통신 및 방송 장비 제조업"},
    "000660": {"name": "SK하이닉스", "sector": "반도체 제조업"},
    "000670": {"name": "영풍", "sector": "1차 비철금속 제조업"},
    "010130": {"name": "고려아연", "sector": "1차 비철금속 제조업"},
}
PAIRS = {"005935": "005930"}
POSITIONS = [
    {"code": "005930", "name": "삼성전자", "quantity": 10, "value": 1_000_000.0, "currency": "KRW"},
    {"code": "005935", "name": "삼성전자우", "quantity": 10, "value": 800_000.0, "currency": "KRW"},
    {"code": "069500", "name": "KODEX 200", "quantity": 10, "value": 1_000_000.0, "currency": "KRW"},
    {"code": "SCHP", "name": "미국 물가채", "quantity": 10, "value": 500_000.0, "currency": "USD"},
    {"code": "VWO", "name": "신흥국", "quantity": 10, "value": 200_000.0, "currency": "USD"},
    {"code": "000670", "name": "영풍", "quantity": 100, "value": 500_000.0, "currency": "KRW"},
    {"code": "CASH_USD", "name": "달러", "quantity": 700, "value": 1_000_000.0, "currency": "USD"},
    {"code": "KRX_GOLD", "name": "금", "quantity": 5, "value": 500_000.0, "currency": "KRW"},
    {"code": "KRFUT_KA1", "name": "하이닉스 F", "quantity": -5, "value": -300_000.0, "currency": "KRW",
     "underlying_code": "000660"},
    {"code": "035720", "name": "시세 없음", "quantity": 3, "value": None, "currency": "KRW"},
    {"code": "IDX_KOSPI", "name": "참조 행", "quantity": 1, "value": None, "currency": "KRW"},
]
SUB_PRICES = {"010130": 100_000.0}


@pytest.fixture
def report():
    return exposure.build_exposure_report(
        POSITIONS, etfs=ETFS, holdcos=HOLDCOS, preferred_pairs=PAIRS, companies=COMPANIES, sub_prices=SUB_PRICES,
    )


def _row(rows, key, field="code"):
    return next(row for row in rows if row[field] == key)


def test_report_totals_separate_cash_and_ignore_unpriced(report):
    totals = report["totals"]
    # 총액 520만 = 투자 420만 + 현금 100만 (시세 없는 종목은 합계에서 빠진다).
    assert totals["total_value"] == 5_200_000
    assert totals["cash_value"] == 1_000_000
    assert totals["cash_pct"] == approx(19.2308)
    assert totals["invested_value"] == 4_200_000
    assert totals["priced_count"] == 9
    assert totals["position_count"] == 10  # IDX_ 참조 행은 보유가 아니다.
    assert totals["unpriced_count"] == 1
    assert totals["unpriced"] == [{"code": "035720", "name": "시세 없음"}]


def test_report_merges_common_preferred_and_etf_into_one_issuer(report):
    rows = report["exposures"]["rows"]
    samsung = _row(rows, "005930")
    assert samsung["direct_value"] == 1_800_000  # 보통주 100만 + 우선주 80만
    assert samsung["etf_value"] == 300_000       # KODEX 200 100만 × 30%
    assert samsung["net_value"] == 2_100_000
    assert samsung["net_pct"] == approx(50.0)
    assert samsung["name"] == "삼성전자"
    assert {member["code"] for member in samsung["members"]} == {"005930", "005935", "069500"}

    hynix = _row(rows, "000660")
    assert hynix["etf_value"] == 200_000
    assert hynix["derivative_value"] == -300_000  # 선물 숏은 기초자산 발행사로 들어간다.
    assert hynix["net_value"] == -100_000
    assert hynix["net_pct"] == approx(-2.381)
    # 평가액을 쪼갠 순노출 크기로 줄 세운다.
    assert rows[0]["code"] == "005930"


def test_report_never_double_counts_look_through(report):
    exposures = report["exposures"]
    assert exposures["etf_unknown_value"] == 700_000  # 50만(미공개 50%) + VWO 20만
    assert exposures["etf_unknown_pct"] == approx(16.6667)
    assert exposures["etf_other_value"] == 0
    # 투자자산 = 발행사·자산 줄 + ETF 기타 + 미공개 → 차이 0.
    assert exposures["reconciliation_error"] == 0
    # 지주사 환산 200만은 어떤 합계에도 들어가지 않는다.
    assert report["totals"]["total_value"] == 5_200_000
    assert report["holdcos"]["total_look_through_value"] == 2_000_000
    net_sum = sum(row["net_value"] for row in exposures["rows"])
    assert net_sum == 3_500_000
    assert all(row["code"] != "010130" for row in exposures["rows"])  # 순노출 0 이면 표에 없다.


def test_report_holdco_look_through_hand_calculated(report):
    holdcos = report["holdcos"]
    row = _row(holdcos["rows"], "000670")
    # 100주 / 1000주 × 200주 = 20주 × 100,000원 = 200만. 평가액 50만의 4배.
    assert row["quantity"] == 100
    assert row["value"] == 500_000
    assert row["look_through_value"] == 2_000_000
    assert row["multiple"] == 4.0
    assert row["hedged"] is False
    assert row["missing_prices"] == 0
    sub = row["subsidiaries"][0]
    assert sub["attributed_shares"] == 20
    assert sub["value"] == 2_000_000
    assert sub["pct"] == approx(47.619)
    summary = _row(holdcos["subsidiary_rows"], "010130")
    assert summary["value"] == 2_000_000
    assert summary["net_pct"] == 0
    assert summary["via"] == ["영풍"]
    assert holdcos["total_look_through_pct"] == approx(47.619)


def test_report_holdco_uses_preferred_shares_and_nets_hedges():
    positions = [
        {"code": "00088K", "name": "한화3우B", "quantity": 4000, "value": 100_000_000.0, "currency": "KRW"},
        {"code": "KRFUT_KAC", "name": "한화 F", "quantity": -1000, "value": -80_000_000.0, "currency": "KRW",
         "underlying_code": "000880"},
    ]
    holdcos = {"000880": {"name": "한화", "adjusted_shares": 75_000_000,
                          "subsidiaries": [{"code": "012450", "name": "한화에어로스페이스", "shares_held": 25_000_000}]}}
    result = exposure.build_exposure_report(
        positions, holdcos=holdcos, preferred_pairs={"00088K": "000880"}, sub_prices={"012450": 900_000.0},
    )
    row = result["holdcos"]["rows"][0]
    # 순주수 3000 / 7500만 × 2500만 = 1000주 × 90만원 = 9억.
    assert row["quantity"] == 3000
    assert row["look_through_value"] == approx(900_000_000.0, 0.01)
    assert row["hedged"] is True
    assert row["multiple"] is None  # 순평가액이 0 근처면 발산하므로 내지 않는다.
    issuer = _row(result["exposures"]["rows"], "000880")
    assert issuer["direct_value"] == 100_000_000
    assert issuer["derivative_value"] == -80_000_000
    assert issuer["net_value"] == 20_000_000


def test_report_holdco_missing_price_is_null_not_zero():
    positions = [{"code": "000670", "name": "영풍", "quantity": 100, "value": 500_000.0, "currency": "KRW"}]
    result = exposure.build_exposure_report(positions, holdcos=HOLDCOS, sub_prices={"010130": None})
    row = result["holdcos"]["rows"][0]
    assert row["missing_prices"] == 1
    assert row["subsidiaries"][0]["value"] is None
    assert row["subsidiaries"][0]["pct"] is None
    assert row["look_through_value"] is None
    assert row["multiple"] is None
    assert result["holdcos"]["total_look_through_value"] == 0


def test_report_concentration_hand_calculated(report):
    look = report["concentration"]["look_through"]
    # 줄: 210만, 50만×3, |-10만|, 분모 추가 미공개 70만 → 분모 440만.
    assert look["top1_pct"] == approx(47.7273)
    assert look["top3_pct"] == approx(70.4545)
    assert look["top5_pct"] == approx(84.0909)
    assert look["hhi"] == approx(2670.45, 0.01)
    assert look["effective_n"] == approx(3.74, 0.005)
    assert look["count"] == 5
    direct = report["concentration"]["direct"]
    # 100, 80, 100, 50, 20, 50, 50, |-30| (만원), 분모 480만.
    assert direct["top1_pct"] == approx(20.8333)
    assert direct["hhi"] == approx(1527.78, 0.01)
    assert direct["count"] == 8


def test_report_etf_table_always_has_coverage(report):
    etfs = report["etfs"]
    kodex = _row(etfs["rows"], "069500")
    assert kodex["status"] == exposure.STATUS_COVERED
    assert kodex["coverage_pct"] == approx(50.0)
    assert kodex["weight_pct"] == approx(23.8095)
    assert kodex["top"][0] == {"code": "005930", "name": "삼성전자", "weight_pct": 30.0}
    schp = _row(etfs["rows"], "SCHP")
    assert schp["status"] == exposure.STATUS_ASSET
    assert schp["coverage_pct"] == approx(79.22)
    vwo = _row(etfs["rows"], "VWO")
    assert vwo["status"] == exposure.STATUS_NO_HOLDINGS
    assert vwo["coverage_pct"] == 0
    # 전체 커버리지 범위는 주식·혼합형 ETF(120만) 중 아는 50만 = 41.67%.
    assert etfs["scope_value"] == 1_200_000
    assert etfs["covered_value"] == 500_000
    assert etfs["coverage_pct"] == approx(41.6667)


def test_report_currency_and_asset_class_show_cash_separately(report):
    currency = report["breakdowns"]["currency"]
    usd = _row(currency, "USD", "label")
    assert usd["value"] == 1_700_000
    assert usd["cash_value"] == 1_000_000
    assert usd["pct"] == approx(32.6923)
    assert _row(currency, "KRW", "label")["value"] == 3_500_000
    assets = report["breakdowns"]["asset_class"]
    assert _row(assets, exposure.ASSET_EQUITY, "label")["value"] == 3_500_000
    assert _row(assets, exposure.ASSET_BOND, "label")["value"] == 500_000
    assert _row(assets, exposure.ASSET_COMMODITY, "label")["value"] == 500_000
    assert _row(assets, exposure.ASSET_DERIVATIVE, "label")["value"] == -300_000
    cash = assets[-1]
    assert cash["label"] == exposure.ASSET_CASH and cash["is_cash"] is True
    assert cash["pct"] == approx(19.2308)
    assert sum(row["value"] for row in assets) == 5_200_000


def test_report_sector_keeps_unclassified_visible_and_sums_to_invested(report):
    sector = report["breakdowns"]["sector"]
    rows = sector["rows"]
    assert _row(rows, "통신 및 방송 장비 제조업", "label")["pct"] == approx(50.0)
    assert _row(rows, "반도체 제조업", "label")["value"] == -100_000
    assert _row(rows, exposure.SECTOR_UNDISCLOSED, "label")["kind"] == "undisclosed"
    assert _row(rows, exposure.SECTOR_UNDISCLOSED, "label")["value"] == 700_000
    assert _row(rows, exposure.SECTOR_NON_EQUITY, "label")["value"] == 1_000_000
    assert sum(row["value"] for row in rows) == 4_200_000
    assert sector["classified_pct"] == approx(59.5238)


def test_report_marks_foreign_and_unmapped_sector_as_unclassified():
    positions = [
        {"code": "GOOGL.O", "name": "알파벳", "quantity": 1, "value": 300.0, "currency": "USD"},
        {"code": "005930", "name": "삼성전자", "quantity": 1, "value": 700.0, "currency": "KRW"},
    ]
    result = exposure.build_exposure_report(positions, companies={"005930": COMPANIES["005930"]})
    rows = result["breakdowns"]["sector"]["rows"]
    unclassified = _row(rows, exposure.SECTOR_UNCLASSIFIED, "label")
    assert unclassified["kind"] == "unclassified"
    assert unclassified["pct"] == approx(30.0)
    assert _row(result["exposures"]["rows"], "GOOGL")["name"] == "알파벳"


def test_report_spac_gets_its_own_asset_class_and_sector():
    positions = [{"code": "0209J0", "name": "KB제34호스팩", "quantity": 10, "value": 100.0, "currency": "KRW"}]
    companies = {"0209J0": {"name": "KB제34호스팩", "sector": "금융 지원 서비스업"}}
    result = exposure.build_exposure_report(positions, companies=companies)
    assert result["breakdowns"]["asset_class"][0]["label"] == exposure.ASSET_SPAC
    assert result["breakdowns"]["sector"]["rows"][0]["label"] == exposure.SECTOR_SPAC


def test_report_flags_leverage_only_for_look_through_etfs():
    etfs = {
        "LEV": {"name": "레버리지", "asset_class": "주식", "rows": [["005930", "삼성전자", 190.0]]},
        "BND": {"name": "채권", "asset_class": "채권", "rows": [["T.1 01.01.30", "Note", 110.0], ["n/a", "Cash", -10.0]]},
    }
    positions = [
        {"code": "LEV", "name": "레버리지", "quantity": 1, "value": 100.0, "currency": "KRW"},
        {"code": "BND", "name": "채권", "quantity": 1, "value": 100.0, "currency": "KRW"},
    ]
    result = exposure.build_exposure_report(positions, etfs=etfs)
    assert _row(result["etfs"]["rows"], "LEV")["leveraged"] is True
    assert _row(result["etfs"]["rows"], "BND")["leveraged"] is False
    assert result["exposures"]["leveraged_etf"] is True
    assert _row(result["exposures"]["rows"], "005930")["etf_value"] == 190
    # 레버리지가 있으면 줄 합이 평가액과 달라지고, 그 차이를 그대로 드러낸다.
    assert result["exposures"]["reconciliation_error"] == -90


def test_report_detects_unknown_etfs_without_splitting_them():
    # 상장법인 목록에 없는 국내 코드와 이름에 ETF 가 든 해외 코드는 ETF 로 본다(커버리지 0).
    positions = [
        {"code": "0999X0", "name": "새 ETF", "quantity": 1, "value": 100.0, "currency": "KRW"},
        {"code": "QQQM.O", "name": "Invesco NASDAQ 100 ETF", "quantity": 1, "value": 100.0, "currency": "USD"},
    ]
    result = exposure.build_exposure_report(positions, etfs={"069500": ETFS["069500"]}, companies=COMPANIES)
    for row in result["etfs"]["rows"]:
        assert row["status"] == exposure.STATUS_NOT_IN_UNIVERSE
        assert row["coverage_pct"] == 0
    assert result["exposures"]["etf_unknown_value"] == 200
    assert result["exposures"]["rows"] == []


def test_report_recognizes_renamed_foreign_etf_by_instrument_type():
    # eiayn 목록에서 빠진 VWO 를 사용자가 '신흥국 주식'으로 부른다 — 개별 종목으로 잡으면
    # ETF 커버리지가 실제보다 좋아 보인다.
    positions = [
        {"code": "VWO", "name": "신흥국 주식", "quantity": 1, "value": 300.0, "currency": "USD", "instrument_type": "ETF"},
        {"code": "AGNC", "name": "AGNC", "quantity": 1, "value": 100.0, "currency": "USD", "instrument_type": "EQUITY"},
        {"code": "QTUM", "name": "양자컴퓨터", "quantity": 1, "value": 100.0, "currency": "USD"},
    ]
    result = exposure.build_exposure_report(positions, etfs={"069500": ETFS["069500"]})
    assert [row["code"] for row in result["etfs"]["rows"]] == ["VWO"]
    assert result["etfs"]["rows"][0]["status"] == exposure.STATUS_NOT_IN_UNIVERSE
    assert result["etfs"]["coverage_pct"] == 0
    assert result["exposures"]["etf_unknown_value"] == 300
    # 유형을 모르면(조회 실패) 개별 종목으로 남고 숨기지 않는다.
    assert {row["code"] for row in result["exposures"]["rows"]} == {"AGNC", "QTUM"}


def test_foreign_symbols_needing_type_skips_what_is_already_known():
    positions = [
        {"code": "VWO", "name": "신흥국 주식"},
        {"code": "QTUM.O", "name": "Defiance Quantum ETF"},
        {"code": "VOO", "name": "S&P500"},
        {"code": "GOOGL.O", "name": "구글"},
        {"code": "GOOGL", "name": "구글"},
        {"code": "005930", "name": "삼성전자"},
        {"code": "KRX_GOLD", "name": "금"},
        {"code": "CASH_USD", "name": "달러"},
        {"code": "KRFUT_A", "name": "선물"},
    ]
    assert exposure.foreign_symbols_needing_type(positions, {"VOO": {}}) == ["VWO", "GOOGL"]
    assert exposure.foreign_symbols_needing_type(positions, None) == ["VWO", "VOO", "GOOGL"]


def test_report_marks_holdco_look_through_as_estimate_with_its_file_date():
    positions = [{"code": "000670", "name": "영풍", "quantity": 100, "value": 500_000.0, "currency": "KRW"}]
    result = exposure.build_exposure_report(
        positions, holdcos=HOLDCOS, sub_prices=SUB_PRICES, holdco_as_of="2026-11-09",
    )
    assert result["holdcos"]["estimated"] is True
    assert result["holdcos"]["as_of"] == "2026-11-09"
    assert any("추정치" in note and "공시와 다를 수 있습니다" in note for note in result["notes"])


def test_report_without_sources_says_so_and_stays_computable():
    positions = [
        {"code": "069500", "name": "KODEX 200", "quantity": 1, "value": 100.0, "currency": "KRW"},
        {"code": "005930", "name": "삼성전자", "quantity": 1, "value": 300.0, "currency": "KRW"},
        {"code": "CASH_KRW", "name": "원화", "quantity": 100, "value": 100.0, "currency": "KRW"},
    ]
    result = exposure.build_exposure_report(positions)
    assert result["available"] is True
    assert _row(result["etfs"]["rows"], "069500")["status"] == exposure.STATUS_NO_SOURCE
    assert result["etfs"]["coverage_pct"] == 0
    assert result["risk"]["available"] is False
    assert result["risk"]["portfolio_volatility_pct"] is None
    assert result["totals"]["invested_value"] == 400


def test_report_empty_portfolio_is_null_not_zero_percent():
    result = exposure.build_exposure_report([])
    assert result["available"] is False
    assert result["totals"]["cash_pct"] is None
    assert result["concentration"]["look_through"]["hhi"] is None
    assert result["etfs"]["coverage_pct"] is None
    assert result["holdcos"]["total_look_through_pct"] is None
    assert result["breakdowns"]["sector"]["classified_pct"] is None
    # 응답은 JSON 으로 직렬화된다(NaN 금지).
    assert json.loads(json.dumps(result, allow_nan=False))


def test_required_codes_cover_only_what_finance_pi_and_holdcos_need():
    positions = [
        {"code": "005935"},
        {"code": "069500"},
        {"code": "VOO"},
        {"code": "KRX_GOLD"},
        {"code": "CASH_KRW"},
        {"code": "KRFUT_A", "underlying_code": "000660"},
        {"code": "KRFUT_B", "pair_long_code": "00088K"},
        {"code": "KRFUT_C"},
    ]
    assert exposure.required_series_codes(positions, {"00088K": "000880"}) == [
        "005935", "069500", "000660", "000880",
    ]
    holdcos = {
        "000880": {"subsidiaries": [{"code": "012450"}]},
        "005930": {"subsidiaries": [{"code": "BRK-A"}, {"code": ""}]},
    }
    assert exposure.holdco_subsidiary_codes(positions, holdcos, {"00088K": "000880", "005935": "005930"}) == [
        "BRK-A", "012450",
    ]
    assert exposure.holdco_subsidiary_codes(positions, None) == []


def test_source_freshness_judges_by_data_date():
    today = date(2026, 11, 8)
    stale = exposure.source_freshness("eiayn", "ETF 구성", "2026-09-30", today=today, max_age_days=3)
    assert stale["as_of"] == "2026-09-30"
    assert stale["age_days"] == 39
    assert stale["stale"] is True
    fresh = exposure.source_freshness("kind", "업종", "2026-11-08T01:00:00", today=today, max_age_days=45)
    assert fresh["age_days"] == 0
    assert fresh["stale"] is False
    unknown = exposure.source_freshness("x", "x", None, today=today, max_age_days=1, available=False)
    assert unknown["as_of"] is None and unknown["age_days"] is None
    assert unknown["stale"] is False and unknown["available"] is False
    garbled = exposure.source_freshness("x", "x", "not-a-date", today=today, max_age_days=1)
    assert garbled["as_of"] is None
