"""routes/analysis.py::_compute_beta_from_pairs 단위 테스트.

API 엔드포인트 자체는 KIS 프록시 / benchmark_history 에 대한 I/O 가
있어 통합 테스트 성격이 강하므로, 여기서는 순수 계산 로직만 고정한다.
회귀 시 수식 변경이 의도치 않게 beta 값을 바꾸는 걸 잡아준다.
"""
from __future__ import annotations

import math
import pytest


def _beta(pairs):
    from routes.analysis import _compute_beta_from_pairs
    return _compute_beta_from_pairs(pairs)


def test_insufficient_samples_returns_none():
    # 20 개 미만 pair → 수익률 계산 후 samples < 20 → None
    pairs = [(100.0 + i, 200.0 + i) for i in range(15)]
    assert _beta(pairs) is None


def test_perfect_correlation_slope_one():
    # 종목과 시장이 정확히 같은 수익률 → β = 1.0
    n = 60
    market = [100.0 * (1.01 ** i) for i in range(n)]
    stock = list(market)  # 같은 시리즈
    pairs = list(zip(stock, market))
    beta = _beta(pairs)
    assert beta is not None
    assert math.isclose(beta, 1.0, abs_tol=1e-6)


def test_double_amplitude_slope_two():
    # 종목이 시장 대비 2배 진폭 → β ≈ 2.0
    n = 60
    market_rets = [0.01 * math.sin(i) for i in range(n)]
    stock_rets = [2.0 * r for r in market_rets]
    # 가격 시리즈 복원
    m = [100.0]
    s = [100.0]
    for i in range(n - 1):
        m.append(m[-1] * (1 + market_rets[i]))
        s.append(s[-1] * (1 + stock_rets[i]))
    pairs = list(zip(s, m))
    beta = _beta(pairs)
    assert beta is not None
    assert math.isclose(beta, 2.0, abs_tol=0.05)


def test_zero_variance_market_returns_none():
    # 시장 수익률 변동 0 → Var=0 → None (0-division 방지)
    n = 60
    pairs = [(100.0 + i * 0.5, 200.0) for i in range(n)]  # 시장 일정
    assert _beta(pairs) is None


def test_handles_invalid_close_prices_gracefully():
    # 0 이나 None 가 섞여 있으면 해당 수익률 계산을 skip.
    # 유효 수익률이 20 개 미만이면 None 반환.
    n = 30
    pairs = []
    for i in range(n):
        s = 100.0 + i
        m = 200.0 + i
        if i in (5, 10, 15):
            s = None  # type: ignore
        pairs.append((s, m))
    # 유효 pair 가 줄어 sample < 20 이면 None, 그 이상이면 숫자.
    beta = _beta(pairs)
    # 구현상 이전 pair 의 close 가 None 이면 수익률 하나 건너뛰므로 20 개
    # 이하일 수 있음. 여기서는 '예외 없이 처리' 만 확인.
    assert beta is None or isinstance(beta, float)
