"""포트폴리오 X-Ray — 입력 없이 실제 노출을 계산하는 순수 모듈.

DB/네트워크 의존이 없다. services.portfolio.exposure_service 가 보유 잔고·시세와
형제 데이터(services.portfolio.exposure_data)를 모아 :func:`build_exposure_report`
에 넘긴다. 수식을 바꾸면 tests/test_portfolio_exposure.py 의 손계산 기대값도 같이 고친다.

금액 규약:

* 평가액은 허브 보유 화면과 같은 ``수량 × 원화 시세``다. 숏·선물 다리는 음수다.
* ``total_value`` = 전 포지션 합(현금 포함), ``cash_value`` = ``CASH_*`` 합,
  ``invested_value`` = 둘의 차. 시세가 없는 포지션은 모든 합계에서 빠지고
  ``unpriced`` 로 드러난다.

노출 원장(이중 계상 금지):

* 비현금 포지션의 평가액은 **한 번만** 원장에 들어간다 — 발행사 줄(직접·선물·ETF 경유),
  자산 줄(금·가상자산·채권형 ETF 등), ETF 내 기타 구성, ETF 미공개 구성 중 하나다.
* 지주사 환산(``holdco_value``)은 지주사 평가액과 **별개의 참고치**다. 합계·비중·HHI
  어디에도 더하지 않는다. 지주사 시가총액이 보유 지분가치보다 훨씬 작을 수 있어
  (영풍→고려아연 등) 평가액을 쪼개는 값이 될 수 없기 때문이다.
* 레버리지·인버스 ETF는 구성 비중 합이 100%를 넘거나 음수다. 실제 노출이 그만큼이므로
  그대로 반영하고 ``leveraged`` 로 표시한다. 이때만 원장 합이 평가액과 달라진다.

집중도:

* ``HHI = Σ w²`` (w 는 %, 범위 0~10000). ``effective_n = 10000 / HHI``.
* direct: 포지션 그대로(ETF 는 한 종목, 보통주·우선주는 각각). look_through: 발행사 합산 +
  ETF 풀어 보기. 미공개 구성은 완전히 분산됐다고 보고 분자에서 빼므로(분모에는 포함)
  look_through HHI 는 **하한**이다.
* 가중치 분모는 |노출| 합이다. 롱 전용 포트폴리오에서는 invested_value 와 같다.

위험 모델(services/portfolio/risk.py 규약):

* 단순 수익률, 날짜 교집합 위 연속 포인트 사이의 수익률, 표본 분산·공분산(n-1),
  연환산 stdev × sqrt(252) × 100.
* 비중 w 는 모델에 든 포지션의 평가액 합 대비 현재 비중(과거에도 같은 비중이었다고 본다).
* 위험 기여율 ``RC_i = w_i × Cov(r_i, r_p) / Var(r_p) × 100``. 합은 100%이고 헤지는 음수다.
* 수익률 표본이 ``min_returns`` 미만이거나 공통 구간을 ``min_history_ratio`` 아래로 줄이는
  포지션은 0 이 아니라 null 이다.
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime
from typing import Any, Iterable, Mapping

from domain.broker_assets import is_futures_contract
from domain.portfolio_codes import (
    common_stock_code,
    is_cash_asset,
    is_korean_stock,
    is_preferred_stock,
    is_special_asset,
    normalize_portfolio_code,
    yahoo_symbol,
)
from services.portfolio import risk

TOP_N_LEVELS = (1, 3, 5, 10)
MAX_EXPOSURE_ROWS = 50
MAX_ETF_DEPTH = 3
MAX_RISK_ROWS = 40
CORRELATION_TOP_K = 10
# 짧은 표본 한 종목이 공통 구간 전체를 줄이지 않게 하는 하한(최장 이력 대비). 신규 상장
# 스팩 하나 때문에 나머지 전 종목의 표본이 석 달로 줄어드는 일을 막는다.
MIN_HISTORY_RATIO = 1 / 2
# 레버리지 판정 여유 — 일반 ETF 도 반올림·현금으로 합이 100을 조금 넘는다.
LEVERAGE_THRESHOLD_PCT = 105.0

SECTOR_UNCLASSIFIED = "미분류"
SECTOR_SPAC = "스팩"
SECTOR_UNDISCLOSED = "ETF 미공개 구성"
SECTOR_ETF_OTHER = "ETF 내 현금·기타"
SECTOR_NON_EQUITY = "비주식 자산"

ASSET_CASH = "현금"
ASSET_EQUITY = "주식"
ASSET_BOND = "채권"
ASSET_MIXED = "혼합"
ASSET_COMMODITY = "금·원자재"
ASSET_CRYPTO = "가상자산"
ASSET_CASH_LIKE = "현금성(RP)"
ASSET_DERIVATIVE = "선물·파생"
ASSET_SPAC = "스팩"
ASSET_ETF_UNKNOWN = "ETF(유형 미상)"
ASSET_OTHER_ETF = "통화·기타 ETF"

_ETF_ASSET_CLASS = {
    "주식": ASSET_EQUITY,
    "채권": ASSET_BOND,
    "혼합자산": ASSET_MIXED,
    "원자재": ASSET_COMMODITY,
    "대체자산": ASSET_COMMODITY,
    "ETF": ASSET_OTHER_ETF,
}
# eiayn 은 구성 자료가 없는 일부 해외 ETF 를 '주식'으로 표기한다(PPLT, AAA.AX, BIL).
_COMMODITY_NAME_RE = re.compile(r"physical|bullion|\bgold\b|\bsilver\b|platinum|palladium", re.I)
_BOND_NAME_RE = re.compile(r"\bcash\b|t-bill|treasury|\bbond\b", re.I)
_CRYPTO_NAME_RE = re.compile(r"bitcoin|ethereum|\bether\b", re.I)
_SPAC_NAME_RE = re.compile(r"스팩|\bSPAC\b", re.I)
_ETF_NAME_RE = re.compile(
    r"\bETF\b|^(KODEX|TIGER|RISE|ACE|SOL|PLUS|HANARO|KOSEF|KIWOOM|KoAct|TIME|WON|1Q|ITF|BNK|TREX)\s", re.I,
)

# Yahoo instrumentType 중 구성을 풀어야 하는 펀드 유형.
_FUND_INSTRUMENT_TYPES = frozenset({"ETF", "MUTUALFUND"})

# 합산에서 빼는 의사 행: 설정 단위 표시용이라 비중이 늘 100이다(실제 구성이 아니다).
_PSEUDO_ROW_NAMES = frozenset({"설정현금액"})
_NO_TICKER = frozenset({"", "N/A", "NA", "-", "NONE", "NULL"})
_EXCHANGE_PREFIX_RE = re.compile(r"^([A-Z]{2,6}):\s*([A-Z0-9.\-]+)$")
_PLAIN_TICKER_RE = re.compile(r"^[A-Z]{1,6}(?:[.\-][A-Z]{1,2})?$")
_SUFFIX_TICKER_RE = re.compile(r"^[0-9A-Z]{1,6}\.[A-Z]{1,3}$")
# 같은 회사의 다른 주식 종류(발행사 합산).
_SHARE_CLASS_ALIASES = {"GOOG": "GOOGL", "BRK-A": "BRK-B"}
_NON_QUOTABLE_PREFIXES = ("IDX_", "FX_")

STATUS_COVERED = "covered"          # 비중 있는 구성이 있어 풀어 봤다.
STATUS_ASSET = "asset_line"         # 채권·원자재형 — 발행사 합산 대상이 아니라 ETF 한 줄로 둔다.
STATUS_NO_WEIGHTS = "no_weights"    # 구성 이름만 있고 비중이 없다.
STATUS_NO_HOLDINGS = "no_holdings"  # 구성 자료가 없다.
STATUS_NOT_IN_UNIVERSE = "not_in_universe"  # eiayn 이 다루지 않는 ETF 다.
STATUS_NO_SOURCE = "no_source"      # eiayn 자료를 받지 못했다.

METHOD_NOTES = (
    "금액은 보유 화면과 같은 '수량 × 원화 시세'이고, 숏·선물 다리는 음수입니다. 비중은 현금을 뺀 투자자산 대비입니다.",
    "ETF는 공개된 상위 구성만 풉니다. 커버리지 밖의 평가액은 'ETF 미공개 구성'으로 남기며 추정해 채우지 않습니다. EIAYN에 없는 ETF는 시세 제공자 유형이나 이름으로 ETF임만 확인하고 커버리지 0%로 둡니다.",
    "지주사 환산은 '보유 주수 ÷ 자사주 제외 주식수 × 자회사 보유 주수 × 자회사 시세'로 구한 추정치입니다. 주식수는 지주사 지분가치 도구의 설정값이라 공시와 다를 수 있습니다. 1단계 자회사까지만 보고, 우선주는 보통주 1주와 같은 지분으로 환산합니다. 지주사 평가액과 별개의 참고치라 합계·비중·HHI에 더하지 않습니다.",
    "보통주·우선주 합산은 우선주 괴리율 도구의 쌍을 쓰고, 목록에 없으면 코드 규칙(끝자리 0)을 씁니다.",
    "통화는 거래 통화 기준입니다. 국내 상장 해외 ETF는 원화로 잡힙니다.",
    "업종은 KIND 상장법인목록의 표준산업분류입니다. 해외 종목과 목록에 없는 종목은 '미분류'로 둡니다.",
    "HHI는 상위 구성만 아는 ETF의 나머지를 완전 분산으로 보므로 하한입니다.",
    "위험 기여는 finance-pi 국내 일봉 최근 1년에 현재 비중을 곱한 과거 추정치입니다. 해외·금·가상자산은 일봉이 없어 빠지고, 선물은 기초자산 일봉으로 대신합니다.",
)


def _round(value: float | None, digits: int = 4) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def _money(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 2)


def _pct(value: float | None, base: float | None) -> float | None:
    if value is None or not base or base <= 0:
        return None
    return round(value / base * 100.0, 4)


def _as_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


# ---------------------------------------------------------------------------
# 코드 → 발행사
# ---------------------------------------------------------------------------

def normalize_constituent_ticker(raw: Any) -> str | None:
    """ETF 구성 티커 정리. 'n/a' 류는 None, 'ETR: SIE' → 'ETR:SIE'."""
    text = str(raw or "").strip().upper()
    if text in _NO_TICKER:
        return None
    prefixed = _EXCHANGE_PREFIX_RE.fullmatch(text)
    if prefixed:
        return f"{prefixed.group(1)}:{prefixed.group(2)}"
    return text


def is_issuer_ticker(ticker: str | None) -> bool:
    """주식 발행사로 볼 수 있는 티커인가. 채권 라인('T.4.5 02.15.36')·스왑은 아니다."""
    if not ticker:
        return False
    if is_korean_stock(ticker):
        return True
    return bool(
        _EXCHANGE_PREFIX_RE.fullmatch(ticker)
        or _PLAIN_TICKER_RE.fullmatch(ticker)
        or _SUFFIX_TICKER_RE.fullmatch(ticker)
    )


def issuer_code(
    code: str | None,
    preferred_pairs: Mapping[str, str] | None = None,
    known_codes: Mapping[str, Any] | None = None,
) -> str:
    """종목 코드 → 발행사 키. 국내 우선주는 보통주 코드, 해외는 Yahoo 표기로 합친다.

    쌍 목록(``preferred_pairs``)을 먼저 보고, 없으면 코드 규칙(끝자리 0)을 쓴다. 규칙은
    ETN 같은 비주식 코드도 맞으므로, 상장법인 목록이 있으면 그 안에 있을 때만 받아들인다.
    """
    normalized = normalize_portfolio_code(code)
    if not normalized:
        return ""
    paired = (preferred_pairs or {}).get(normalized)
    if paired:
        return normalize_portfolio_code(paired)
    if is_korean_stock(normalized):
        if is_preferred_stock(normalized):
            candidate = common_stock_code(normalized)
            if not known_codes or candidate in known_codes:
                return candidate
        return normalized
    symbol = yahoo_symbol(normalized)
    return _SHARE_CLASS_ALIASES.get(symbol, symbol)


def etf_lookup_key(code: str | None) -> str:
    """보유 코드 → eiayn id. QQQM.O → QQQM, FUEVFVND.HM → FUEVFVND.VN."""
    return yahoo_symbol(normalize_portfolio_code(code))


# ---------------------------------------------------------------------------
# ETF 풀어 보기
# ---------------------------------------------------------------------------

def etf_asset_class(entry: Mapping[str, Any] | None) -> str:
    """eiayn assetClass → X-Ray 자산군. 이름으로 드러나는 오표기는 바로잡는다."""
    if not entry:
        return ASSET_ETF_UNKNOWN
    name = f'{entry.get("name") or ""} {entry.get("full_name") or ""}'
    raw = entry.get("asset_class")
    if _CRYPTO_NAME_RE.search(name) and raw in ("대체자산", "주식"):
        return ASSET_CRYPTO
    if raw == "주식":
        if _COMMODITY_NAME_RE.search(name):
            return ASSET_COMMODITY
        if _BOND_NAME_RE.search(name):
            return ASSET_BOND
    return _ETF_ASSET_CLASS.get(raw, ASSET_ETF_UNKNOWN)


def flatten_etf(
    code: str,
    etfs: Mapping[str, Mapping[str, Any]],
    *,
    preferred_pairs: Mapping[str, str] | None = None,
    known_codes: Mapping[str, Any] | None = None,
    _seen: tuple[str, ...] = (),
) -> dict:
    """ETF 구성을 발행사 단위로 편다(ETF 안의 ETF 는 ``MAX_ETF_DEPTH`` 단계까지 재귀).

    반환 비중은 ETF 평가액 대비 %다.

    * ``issuers``: {발행사 키: {"pct", "name"}}.
    * ``other_pct``: 비중은 있으나 발행사가 아닌 구성(원화현금·채권 라인·스왑).
    * ``gross_pct``: |비중| 합(상한 없음). 레버리지 판정용.
    * ``covered_pct``: 구성을 아는 비중 = min(100, gross_pct). 안쪽 ETF 는 그 커버리지만큼만 센다.
    * ``unweighted``: 비중 없이 이름만 있는 행 수.
    """
    entry = etfs.get(code) or {}
    issuers: dict[str, dict] = {}
    other_pct = 0.0
    gross_pct = 0.0
    seen = (*_seen, code)

    def add_issuer(key: str, pct: float, name: str | None) -> None:
        slot = issuers.setdefault(key, {"pct": 0.0, "name": name or key})
        slot["pct"] += pct

    for row in entry.get("rows") or []:
        ticker = normalize_constituent_ticker(row[0])
        name = str(row[1] or "").strip()
        weight = _as_float(row[2])
        if weight is None or name in _PSEUDO_ROW_NAMES:
            continue
        nested_key = etf_lookup_key(ticker) if ticker else ""
        if nested_key and nested_key in etfs and nested_key not in seen and len(seen) < MAX_ETF_DEPTH:
            inner = flatten_etf(
                nested_key, etfs, preferred_pairs=preferred_pairs, known_codes=known_codes, _seen=seen,
            )
            scale = weight / 100.0
            for key, slot in inner["issuers"].items():
                add_issuer(key, slot["pct"] * scale, slot["name"])
            other_pct += inner["other_pct"] * scale
            gross_pct += inner["gross_pct"] * abs(scale)
        elif is_issuer_ticker(ticker):
            add_issuer(issuer_code(ticker, preferred_pairs, known_codes), weight, name)
            gross_pct += abs(weight)
        else:
            other_pct += weight
            gross_pct += abs(weight)
    return {
        "issuers": issuers,
        "other_pct": other_pct,
        "gross_pct": gross_pct,
        "covered_pct": min(100.0, gross_pct),
        "unweighted": int(entry.get("unweighted") or 0),
    }


# ---------------------------------------------------------------------------
# 집중도
# ---------------------------------------------------------------------------

def concentration_metrics(values: Iterable[float], *, residual: float = 0.0) -> dict:
    """|값| 기준 상위 N 비중·HHI·유효 종목 수.

    ``residual`` 은 분모에만 들어가는 금액(미공개 구성)이다. 표본이 없으면 전부 null.
    """
    magnitudes = sorted((abs(v) for v in values if v), reverse=True)
    denominator = sum(magnitudes) + abs(residual)
    empty = {f"top{n}_pct": None for n in TOP_N_LEVELS}
    if not magnitudes or denominator <= 0:
        return {**empty, "hhi": None, "effective_n": None, "count": len(magnitudes)}
    weights = [m / denominator * 100.0 for m in magnitudes]
    hhi = sum(w * w for w in weights)
    return {
        **{f"top{n}_pct": _round(sum(weights[:n])) for n in TOP_N_LEVELS},
        "hhi": _round(hhi, 2),
        "effective_n": _round(10000.0 / hhi, 2) if hhi > 0 else None,
        "count": len(magnitudes),
    }


# ---------------------------------------------------------------------------
# 위험 모델
# ---------------------------------------------------------------------------

def _mean(values: list[float]) -> float:
    return sum(values) / len(values)


def _sample_cov(xs: list[float], ys: list[float], mean_x: float | None = None, mean_y: float | None = None) -> float:
    n = len(xs)
    mx = _mean(xs) if mean_x is None else mean_x
    my = _mean(ys) if mean_y is None else mean_y
    return sum((xs[i] - mx) * (ys[i] - my) for i in range(n)) / (n - 1)


def _returns_on_dates(points: list[tuple[str, float]], dates: list[str]) -> list[float]:
    by_date = dict(points)
    return [r for _, r in risk.simple_returns([(d, by_date[d]) for d in dates])]


def _pick_common_sample(
    series: dict[str, list[tuple[str, float]]],
    min_returns: int,
    min_history_ratio: float,
) -> tuple[list[str], list[str]]:
    """(모델에 넣을 시리즈 코드, 공통 날짜). 공통 표본이 모자라면 가장 짧은 시리즈부터 뺀다."""
    if not series:
        return [], []
    longest = max(len(points) for points in series.values())
    floor = max(min_returns + 1, math.ceil(longest * min_history_ratio))
    codes = [code for code, points in series.items() if len(points) >= floor]
    while len(codes) >= 1:
        common = set.intersection(*(set(d for d, _ in series[code]) for code in codes))
        if len(common) >= min_returns + 1:
            return codes, sorted(common)
        codes.remove(min(codes, key=lambda code: len(series[code])))
    return [], []


def compute_risk_model(
    positions: list[dict],
    closes: Mapping[str, list[dict]] | None,
    *,
    min_returns: int = risk.MIN_BENCHMARK_RETURNS,
    min_history_ratio: float = MIN_HISTORY_RATIO,
    top_k: int = CORRELATION_TOP_K,
    invested_gross: float | None = None,
) -> dict:
    """포지션별 위험 기여·변동성과 종목 간 상관.

    ``positions``: {code, name, value, series_code}. series_code 가 None 이면 일봉이 없는
    자산이다. ``closes``: {series_code: [{date, close}]}. 응답이 통째로 없으면 available=False.
    """
    rows: list[dict] = []
    gross_all = invested_gross if invested_gross is not None else sum(abs(p["value"]) for p in positions)
    empty = {
        "available": False,
        "portfolio_volatility_pct": None,
        "diversification_ratio": None,
        "implied_avg_correlation": None,
        "sample_returns": 0,
        "start_date": None,
        "end_date": None,
        "coverage_pct": None,
        "modeled_count": 0,
        "excluded_count": len(positions),
        "rows": [],
        "excluded": [],
        "correlation": None,
    }
    if closes is None:
        return empty

    series: dict[str, list[tuple[str, float]]] = {}
    for position in positions:
        code = position.get("series_code")
        if code and code not in series:
            cleaned = risk.clean_series(closes.get(code), "close")
            if cleaned:
                series[code] = cleaned
    modeled_codes, dates = _pick_common_sample(series, min_returns, min_history_ratio)
    modeled_set = set(modeled_codes)

    modeled: list[dict] = []
    excluded: list[dict] = []
    for position in positions:
        code = position.get("series_code")
        if code and code in modeled_set:
            modeled.append(position)
            continue
        if not code:
            reason = "no_series"
        elif code not in series:
            reason = "no_rows"
        else:
            reason = "short_history"
        excluded.append({
            "code": position["code"],
            "name": position.get("name") or position["code"],
            "reason": reason,
            "sample_points": len(series.get(code) or []) if code else 0,
        })

    result = {**empty, "excluded": excluded[:MAX_RISK_ROWS], "excluded_count": len(excluded)}
    modeled_value = sum(p["value"] for p in modeled)
    if len(modeled) < 1 or modeled_value <= 0 or len(dates) < 2:
        return result

    returns_by_series = {code: _returns_on_dates(series[code], dates) for code in modeled_codes}
    n_obs = len(dates) - 1
    weights = [p["value"] / modeled_value for p in modeled]
    position_returns = [returns_by_series[p["series_code"]] for p in modeled]
    portfolio_returns = [
        sum(weights[i] * position_returns[i][t] for i in range(len(modeled))) for t in range(n_obs)
    ]
    mean_p = _mean(portfolio_returns)
    var_p = _sample_cov(portfolio_returns, portfolio_returns, mean_p, mean_p)
    annual = math.sqrt(risk.TRADING_DAYS_PER_YEAR) * 100.0

    daily_sigmas: list[float] = []
    for i, position in enumerate(modeled):
        returns_i = position_returns[i]
        mean_i = _mean(returns_i)
        var_i = _sample_cov(returns_i, returns_i, mean_i, mean_i)
        sigma_i = math.sqrt(max(var_i, 0.0))
        daily_sigmas.append(sigma_i)
        cov_ip = _sample_cov(returns_i, portfolio_returns, mean_i, mean_p)
        rows.append({
            "code": position["code"],
            "name": position.get("name") or position["code"],
            "weight_pct": _round(weights[i] * 100.0),
            "volatility_pct": _round(sigma_i * annual),
            "risk_contribution_pct": _round(weights[i] * cov_ip / var_p * 100.0) if var_p > 0 else None,
            "proxy_code": position["series_code"] if position["series_code"] != position["code"] else None,
        })
    rows.sort(key=lambda r: abs(r["risk_contribution_pct"] or 0.0), reverse=True)

    sigma_p = math.sqrt(max(var_p, 0.0))
    weighted_sigma_abs = sum(abs(weights[i]) * daily_sigmas[i] for i in range(len(modeled)))
    signed = [weights[i] * daily_sigmas[i] for i in range(len(modeled))]
    squared = sum(a * a for a in signed)
    implied_denominator = sum(signed) ** 2 - squared
    implied = (var_p - squared) / implied_denominator if len(modeled) >= 2 and implied_denominator > 0 else None
    if implied is not None and abs(implied) > 1.0 + 1e-9:
        implied = None

    result.update({
        "available": True,
        "portfolio_volatility_pct": _round(sigma_p * annual),
        "diversification_ratio": _round(weighted_sigma_abs / sigma_p) if sigma_p > 0 else None,
        "implied_avg_correlation": _round(implied),
        "sample_returns": n_obs,
        "start_date": dates[0],
        "end_date": dates[-1],
        "coverage_pct": _pct(sum(abs(p["value"]) for p in modeled), gross_all),
        "modeled_count": len(modeled),
        "rows": rows[:MAX_RISK_ROWS],
        "correlation": _correlation_matrix(rows, modeled, position_returns, top_k),
    })
    return result


def _correlation_matrix(
    rows: list[dict],
    modeled: list[dict],
    position_returns: list[list[float]],
    top_k: int,
) -> dict | None:
    """위험 기여 절댓값 상위 ``top_k`` 포지션의 Pearson 상관. 분산 0 이면 null."""
    if len(modeled) < 2:
        return None
    index = {p["code"]: i for i, p in enumerate(modeled)}
    picked = [row["code"] for row in rows[:top_k]]
    cov_cache: dict[tuple[int, int], float] = {}

    def cov(a: int, b: int) -> float:
        key = (a, b) if a <= b else (b, a)
        if key not in cov_cache:
            cov_cache[key] = _sample_cov(position_returns[a], position_returns[b])
        return cov_cache[key]

    matrix: list[list[float | None]] = []
    for code_a in picked:
        a = index[code_a]
        row: list[float | None] = []
        for code_b in picked:
            b = index[code_b]
            denominator = cov(a, a) * cov(b, b)
            row.append(_round(cov(a, b) / math.sqrt(denominator)) if denominator > 0 else None)
        matrix.append(row)
    names = {p["code"]: p.get("name") or p["code"] for p in modeled}
    return {"codes": picked, "names": [names[c] for c in picked], "matrix": matrix}


# ---------------------------------------------------------------------------
# 포지션 분류
# ---------------------------------------------------------------------------

def _classify(
    position: Mapping[str, Any],
    etfs: Mapping[str, Mapping[str, Any]] | None,
    companies: Mapping[str, Mapping[str, Any]] | None,
) -> str:
    """cash | asset | derivative | etf | direct."""
    code = normalize_portfolio_code(position.get("code"))
    if is_cash_asset(code):
        return "cash"
    if is_futures_contract(code):
        return "derivative"
    if is_special_asset(code):
        return "asset"
    if etfs and etf_lookup_key(code) in etfs:
        return "etf"
    name = str(position.get("name") or "")
    if is_korean_stock(code):
        if is_preferred_stock(code):
            return "direct"
        # 상장법인 목록(KIND)에는 회사만 있다. 목록에 없는 국내 코드는 ETF·ETN 이다.
        if companies:
            return "direct" if code in companies else "etf"
        return "etf" if _ETF_NAME_RE.search(name) else "direct"
    # eiayn 해외 목록은 거래량 상위 위주라 들고 나간다. 사용자가 이름을 바꾼 ETF(VWO '신흥국 주식')는
    # 시세 제공자의 유형으로 알아본다. 개별 종목으로 잡으면 ETF 커버리지가 실제보다 좋아 보인다.
    if str(position.get("instrument_type") or "").upper() in _FUND_INSTRUMENT_TYPES:
        return "etf"
    return "etf" if _ETF_NAME_RE.search(name) else "direct"


def foreign_symbols_needing_type(
    positions: Iterable[Mapping[str, Any]],
    etfs: Mapping[str, Mapping[str, Any]] | None,
) -> list[str]:
    """eiayn 에 없고 이름으로도 ETF 인지 알 수 없는 해외 보유 종목의 Yahoo 심볼."""
    symbols: dict[str, None] = {}
    for position in positions:
        code = normalize_portfolio_code(position.get("code"))
        if not code or is_special_asset(code) or is_korean_stock(code) or code.startswith(_NON_QUOTABLE_PREFIXES):
            continue
        symbol = etf_lookup_key(code)
        if (etfs and symbol in etfs) or _ETF_NAME_RE.search(str(position.get("name") or "")):
            continue
        symbols[symbol] = None
    return list(symbols)


def _position_asset_class(
    position: Mapping[str, Any],
    kind: str,
    etf_entry: Mapping[str, Any] | None,
    is_spac: bool,
) -> str:
    code = normalize_portfolio_code(position.get("code"))
    if kind == "cash":
        return ASSET_CASH
    if kind == "derivative" or code.startswith("FUTURES_"):
        return ASSET_DERIVATIVE
    if code == "CMA_RP_KRW":
        return ASSET_CASH_LIKE
    if code == "KRX_GOLD":
        return ASSET_COMMODITY
    if code.startswith("CRYPTO_"):
        return ASSET_CRYPTO
    if kind == "etf":
        return etf_asset_class(etf_entry)
    if is_spac:
        return ASSET_SPAC
    return ASSET_EQUITY


def _resolve_status(entry: Mapping[str, Any] | None, flat: dict | None, asset_class: str, source_ok: bool) -> str:
    if not source_ok:
        return STATUS_NO_SOURCE
    if not entry:
        return STATUS_NOT_IN_UNIVERSE
    if asset_class not in (ASSET_EQUITY, ASSET_MIXED):
        return STATUS_ASSET
    if flat and flat["gross_pct"] > 0:
        return STATUS_COVERED
    if flat and flat["unweighted"]:
        return STATUS_NO_WEIGHTS
    return STATUS_NO_HOLDINGS


def _derivative_series_code(position: Mapping[str, Any], preferred_pairs: Mapping[str, str] | None) -> str | None:
    """선물 다리의 대리 일봉: 기초자산, 없으면 페어 롱의 보통주."""
    underlying = normalize_portfolio_code(position.get("underlying_code"))
    if underlying:
        return underlying if is_korean_stock(underlying) else None
    long_code = normalize_portfolio_code(position.get("pair_long_code"))
    if long_code:
        common = issuer_code(long_code, preferred_pairs)
        return common if is_korean_stock(common) else None
    return None


def required_series_codes(positions: Iterable[Mapping[str, Any]], preferred_pairs: Mapping[str, str] | None = None) -> list[str]:
    """위험 모델에 필요한 finance-pi 일봉 코드(국내만 — 해외는 일봉이 없다)."""
    codes: dict[str, None] = {}
    for position in positions:
        code = normalize_portfolio_code(position.get("code"))
        if is_futures_contract(code):
            series = _derivative_series_code(position, preferred_pairs)
            if series:
                codes[series] = None
        elif is_korean_stock(code):
            codes[code] = None
    return list(codes)


def holdco_subsidiary_codes(
    positions: Iterable[Mapping[str, Any]],
    holdcos: Mapping[str, Mapping[str, Any]] | None,
    preferred_pairs: Mapping[str, str] | None = None,
    known_codes: Mapping[str, Any] | None = None,
) -> list[str]:
    """보유 지주사의 자회사 코드 — 환산에 필요한 시세 목록."""
    codes: dict[str, None] = {}
    if not holdcos:
        return []
    for position in positions:
        code = normalize_portfolio_code(position.get("code"))
        if is_futures_contract(code):
            target = position.get("underlying_code") or position.get("pair_long_code")
            key = issuer_code(target, preferred_pairs, known_codes) if target else ""
        else:
            key = issuer_code(code, preferred_pairs, known_codes)
        for sub in (holdcos.get(key) or {}).get("subsidiaries") or []:
            sub_code = normalize_portfolio_code(sub.get("code"))
            if sub_code:
                codes[sub_code] = None
    return list(codes)


# ---------------------------------------------------------------------------
# 보고서
# ---------------------------------------------------------------------------

def _new_line(key: str, name: str, kind: str) -> dict:
    return {
        "key": key,
        "name": name,
        "kind": kind,
        "direct_value": 0.0,
        "derivative_value": 0.0,
        "etf_value": 0.0,
        "holdco_value": None,
        "members": [],
        "_named_by_position": False,
    }


def _line_net(line: Mapping[str, Any]) -> float:
    return line["direct_value"] + line["derivative_value"] + line["etf_value"]


def _bucket_rows(buckets: Mapping[str, Mapping[str, float]], base: float | None) -> list[dict]:
    rows = [
        {"label": label, "value": _money(slot["value"]), "pct": _pct(slot["value"], base), **{
            k: (_money(v) if k.endswith("_value") else v) for k, v in slot.items() if k != "value"
        }}
        for label, slot in buckets.items()
    ]
    rows.sort(key=lambda row: abs(row["value"] or 0.0), reverse=True)
    return rows


def build_exposure_report(
    positions: list[dict],
    *,
    etfs: Mapping[str, Mapping[str, Any]] | None = None,
    holdcos: Mapping[str, Mapping[str, Any]] | None = None,
    preferred_pairs: Mapping[str, str] | None = None,
    companies: Mapping[str, Mapping[str, Any]] | None = None,
    closes: Mapping[str, list[dict]] | None = None,
    sub_prices: Mapping[str, float | None] | None = None,
    sources: list[dict] | None = None,
    holdco_as_of: str | None = None,
    account_id: str | None = None,
    min_returns: int = risk.MIN_BENCHMARK_RETURNS,
    min_history_ratio: float = MIN_HISTORY_RATIO,
) -> dict:
    """포지션과 형제 데이터 → X-Ray 응답.

    ``positions``: {code, name, quantity, value(원화, 시세 없으면 None), currency,
    underlying_code?, pair_long_code?, instrument_type?, stale?}. 형제 데이터가 None 이면
    그 자료 없이 계산하고, 화면이 ``sources`` 로 이유를 보여 준다.
    """
    etf_source_ok = etfs is not None
    etfs = etfs or {}
    holdcos = holdcos or {}
    preferred_pairs = preferred_pairs or {}
    companies_map = companies or {}

    def issuer_of(code: str | None) -> str:
        return issuer_code(code, preferred_pairs, companies_map)

    def company_of(key: str) -> Mapping[str, Any]:
        return companies_map.get(key) or {}

    lines: dict[str, dict] = {}
    currency_buckets: dict[str, dict] = {}
    asset_buckets: dict[str, dict] = {}
    sector_extra = {SECTOR_UNDISCLOSED: 0.0, SECTOR_ETF_OTHER: 0.0, SECTOR_NON_EQUITY: 0.0}
    etf_rows: list[dict] = []
    unpriced: list[dict] = []
    direct_values: list[float] = []
    risk_inputs: list[dict] = []
    holdco_quantities: dict[str, float] = {}
    holdco_values: dict[str, float] = {}
    holdco_hedged: set[str] = set()
    total_value = cash_value = etf_unknown = etf_other = 0.0
    leveraged_any = False
    cash_count = 0
    stale_quotes = 0
    priced_count = 0
    position_count = 0

    def line_for(key: str, name: str, kind: str) -> dict:
        line = lines.get(key)
        if line is None:
            line = lines[key] = _new_line(key, name, kind)
        return line

    for position in positions:
        code = normalize_portfolio_code(position.get("code"))
        if not code or code.startswith(_NON_QUOTABLE_PREFIXES):
            continue
        name = str(position.get("name") or code)
        quantity = _as_float(position.get("quantity")) or 0.0
        if quantity == 0:
            continue
        position_count += 1
        value = _as_float(position.get("value"))
        if value is None:
            unpriced.append({"code": code, "name": name})
            continue
        priced_count += 1
        if position.get("stale"):
            stale_quotes += 1
        total_value += value
        currency = str(position.get("currency") or "KRW").upper()
        kind = _classify(position, etfs if etf_source_ok else None, companies_map or None)

        if kind == "cash":
            cash_value += value
            cash_count += 1
            currency_slot = currency_buckets.setdefault(currency, {"value": 0.0, "cash_value": 0.0})
            currency_slot["value"] += value
            currency_slot["cash_value"] += value
            continue

        currency_slot = currency_buckets.setdefault(currency, {"value": 0.0, "cash_value": 0.0})
        currency_slot["value"] += value
        direct_values.append(value)

        etf_key = etf_lookup_key(code)
        etf_entry = etfs.get(etf_key) if kind == "etf" else None
        issuer_key = issuer_of(code)
        company = company_of(issuer_key)
        is_spac = bool(_SPAC_NAME_RE.search(name) or _SPAC_NAME_RE.search(str(company.get("name") or "")))
        asset_class = _position_asset_class(position, kind, etf_entry, is_spac)
        asset_slot = asset_buckets.setdefault(asset_class, {"value": 0.0, "count": 0})
        asset_slot["value"] += value
        asset_slot["count"] += 1

        member = {"code": code, "name": name, "value": _money(value)}
        series_code = code if is_korean_stock(code) else None

        if kind == "etf":
            flat = None
            if etf_entry:
                flat = flatten_etf(etf_key, etfs, preferred_pairs=preferred_pairs, known_codes=companies_map or None)
            status = _resolve_status(etf_entry, flat, asset_class, etf_source_ok)
            coverage = flat["covered_pct"] if flat else 0.0
            # 채권형의 음수 현금 행 같은 것은 레버리지가 아니다 — 풀어 본 ETF 만 판정한다.
            leveraged = bool(status == STATUS_COVERED and flat["gross_pct"] > LEVERAGE_THRESHOLD_PCT)
            leveraged_any = leveraged_any or leveraged
            top: list[dict] = []
            if status == STATUS_COVERED:
                for key, slot in flat["issuers"].items():
                    exposure = value * slot["pct"] / 100.0
                    line = line_for(key, company_of(key).get("name") or slot["name"], "issuer")
                    line["etf_value"] += exposure
                    line["members"].append({"code": code, "name": name, "value": _money(exposure), "via": "etf"})
                etf_other += value * flat["other_pct"] / 100.0
                etf_unknown += value * (100.0 - coverage) / 100.0
                sector_extra[SECTOR_ETF_OTHER] += value * flat["other_pct"] / 100.0
                sector_extra[SECTOR_UNDISCLOSED] += value * (100.0 - coverage) / 100.0
                ordered = sorted(flat["issuers"].items(), key=lambda item: abs(item[1]["pct"]), reverse=True)
                top = [
                    {"code": key, "name": company_of(key).get("name") or slot["name"], "weight_pct": _round(slot["pct"])}
                    for key, slot in ordered[:5]
                ]
            elif status == STATUS_ASSET:
                line = line_for(code, name, "asset")
                line["direct_value"] += value
                line["members"].append(member)
                sector_extra[SECTOR_NON_EQUITY] += value
            else:
                etf_unknown += value
                sector_extra[SECTOR_UNDISCLOSED] += value
            etf_rows.append({
                "code": code,
                "name": name,
                "value": _money(value),
                "asset_class": asset_class,
                "status": status,
                "coverage_pct": _round(coverage),
                "unweighted": flat["unweighted"] if flat else 0,
                "leveraged": leveraged,
                "top": top,
            })
        elif kind == "derivative":
            target = normalize_portfolio_code(position.get("underlying_code") or position.get("pair_long_code"))
            if target:
                key = issuer_of(target)
                series_code = _derivative_series_code(position, preferred_pairs)
                line = line_for(key, company_of(key).get("name") or name, "issuer")
                line["derivative_value"] += value
                line["members"].append({**member, "via": "derivative"})
                holdco_quantities[key] = holdco_quantities.get(key, 0.0) + quantity
                holdco_values[key] = holdco_values.get(key, 0.0) + value
                holdco_hedged.add(key)
            else:
                line = line_for(code, name, "asset")
                line["derivative_value"] += value
                line["members"].append(member)
                sector_extra[SECTOR_NON_EQUITY] += value
        elif kind == "direct":
            line = line_for(issuer_key, company.get("name") or name, "issuer")
            # 상장법인 이름이 없으면 사용자가 보는 이름을 쓴다. 보통주 이름이 우선주보다 앞선다.
            if not company.get("name") and (code == issuer_key or not line["_named_by_position"]):
                line["name"] = name
                line["_named_by_position"] = True
            line["direct_value"] += value
            line["members"].append(member)
            holdco_quantities[issuer_key] = holdco_quantities.get(issuer_key, 0.0) + quantity
            holdco_values[issuer_key] = holdco_values.get(issuer_key, 0.0) + value
        else:  # asset
            line = line_for(code, name, "asset")
            line["direct_value"] += value
            line["members"].append(member)
            sector_extra[SECTOR_NON_EQUITY] += value

        risk_inputs.append({"code": code, "name": name, "value": value, "series_code": series_code})

    invested_value = total_value - cash_value

    # --- 지주사 환산: 합계에 더하지 않는 별도 참고치 ------------------------------
    holdco_rows: list[dict] = []
    total_look_through = 0.0
    prices = sub_prices or {}
    for holdco_code, quantity in holdco_quantities.items():
        structure = holdcos.get(holdco_code)
        adjusted = _as_float((structure or {}).get("adjusted_shares"))
        if not structure or not adjusted or adjusted <= 0:
            continue
        subs: list[dict] = []
        look_through = 0.0
        priced = 0
        missing = 0
        for sub in structure.get("subsidiaries") or []:
            sub_code = normalize_portfolio_code(sub.get("code"))
            shares_held = _as_float(sub.get("shares_held"))
            if not sub_code or not shares_held:
                continue
            price = _as_float(prices.get(sub_code))
            attributed_shares = quantity / adjusted * shares_held
            sub_value = attributed_shares * price if price is not None else None
            if sub_value is None:
                missing += 1
            else:
                priced += 1
                look_through += sub_value
                key = issuer_of(sub_code)
                line = line_for(key, company_of(key).get("name") or sub.get("name") or key, "issuer")
                line["holdco_value"] = (line["holdco_value"] or 0.0) + sub_value
                line["members"].append({
                    "code": holdco_code, "name": structure.get("name") or holdco_code,
                    "value": _money(sub_value), "via": "holdco",
                })
            subs.append({
                "code": sub_code,
                "name": sub.get("name") or sub_code,
                "attributed_shares": _round(attributed_shares, 2),
                "value": _money(sub_value),
                "pct": _pct(sub_value, invested_value),
            })
        subs.sort(key=lambda row: abs(row["value"] or 0.0), reverse=True)
        total_look_through += look_through
        holding_value = holdco_values.get(holdco_code, 0.0)
        holdco_rows.append({
            "code": holdco_code,
            "name": company_of(holdco_code).get("name") or structure.get("name") or holdco_code,
            "quantity": _round(quantity, 2),
            "value": _money(holding_value),
            # 시세를 하나도 모르면 0 이 아니라 null 이다(일부만 모르면 missing_prices 로 드러난다).
            "look_through_value": _money(look_through) if priced else None,
            "multiple": (
                _round(look_through / holding_value, 2)
                if holding_value > 0 and look_through and holdco_code not in holdco_hedged else None
            ),
            "hedged": holdco_code in holdco_hedged,
            "missing_prices": missing,
            "subsidiaries": subs,
        })
    holdco_rows.sort(key=lambda row: abs(row["look_through_value"] or 0.0), reverse=True)

    # --- 업종: 발행사 줄을 KIND 업종으로 합산 ----------------------------------------
    sector_buckets: dict[str, dict] = {}
    for line in lines.values():
        net = _line_net(line)
        if line["kind"] != "issuer" or not net:
            continue
        company = company_of(line["key"])
        if _SPAC_NAME_RE.search(str(company.get("name") or line["name"])):
            sector = SECTOR_SPAC
        else:
            sector = company.get("sector") or SECTOR_UNCLASSIFIED
        slot = sector_buckets.setdefault(sector, {"value": 0.0, "count": 0})
        slot["value"] += net
        slot["count"] += 1

    # --- 집중도 ----------------------------------------------------------------
    nets = [_line_net(line) for line in lines.values()]
    look_through_residual = abs(etf_unknown) + abs(etf_other)
    concentration = {
        "direct": concentration_metrics(direct_values),
        "look_through": concentration_metrics(nets, residual=look_through_residual),
    }

    # --- 노출 표 ----------------------------------------------------------------
    # 평가액을 쪼갠 순노출로 줄 세운다. 지주사 환산은 지주사 할인 때문에 투자금보다 훨씬
    # 클 수 있어 같은 잣대로 경쟁시키지 않고, 자회사 환산 표(subsidiary_rows)로 따로 보여 준다.
    ranked = sorted(
        (line for line in lines.values() if _line_net(line)),
        key=lambda line: abs(_line_net(line)),
        reverse=True,
    )
    subsidiary_rows = [
        {
            "code": line["key"],
            "name": line["name"],
            "value": _money(line["holdco_value"]),
            "pct": _pct(line["holdco_value"], invested_value),
            "net_pct": _pct(_line_net(line), invested_value),
            "via": [m["name"] for m in sorted(
                (m for m in line["members"] if m.get("via") == "holdco"),
                key=lambda m: abs(m["value"] or 0.0),
                reverse=True,
            )],
        }
        for line in sorted(
            (line for line in lines.values() if line["holdco_value"]),
            key=lambda line: abs(line["holdco_value"]),
            reverse=True,
        )[:MAX_EXPOSURE_ROWS]
    ]
    exposure_rows = []
    for line in ranked[:MAX_EXPOSURE_ROWS]:
        net = _line_net(line)
        members = sorted(line["members"], key=lambda m: abs(m["value"] or 0.0), reverse=True)
        exposure_rows.append({
            "code": line["key"],
            "name": line["name"],
            "kind": line["kind"],
            "direct_value": _money(line["direct_value"]),
            "derivative_value": _money(line["derivative_value"]),
            "etf_value": _money(line["etf_value"]),
            "net_value": _money(net),
            "net_pct": _pct(net, invested_value),
            "holdco_value": _money(line["holdco_value"]),
            "holdco_pct": _pct(line["holdco_value"], invested_value),
            "sector": company_of(line["key"]).get("sector"),
            "members": members[:12],
        })

    # --- ETF 합계 ----------------------------------------------------------------
    etf_rows.sort(key=lambda row: abs(row["value"] or 0.0), reverse=True)
    for row in etf_rows:
        row["weight_pct"] = _pct(row["value"], invested_value)
    look_through_scope = [row for row in etf_rows if row["status"] != STATUS_ASSET]
    scope_value = sum(abs(row["value"]) for row in look_through_scope)
    covered_value = sum(abs(row["value"]) * row["coverage_pct"] / 100.0 for row in look_through_scope)

    # --- 통화·자산군·업종 표 ---------------------------------------------------------
    asset_rows = _bucket_rows(asset_buckets, total_value)
    if cash_count:
        asset_rows.append({
            "label": ASSET_CASH, "value": _money(cash_value), "pct": _pct(cash_value, total_value),
            "count": cash_count, "is_cash": True,
        })

    sector_rows = [{**row, "kind": "sector"} for row in _bucket_rows(sector_buckets, invested_value)]
    for row in sector_rows:
        if row["label"] == SECTOR_UNCLASSIFIED:
            row["kind"] = "unclassified"
    for label, kind_name in (
        (SECTOR_UNDISCLOSED, "undisclosed"),
        (SECTOR_ETF_OTHER, "etf_other"),
        (SECTOR_NON_EQUITY, "non_equity"),
    ):
        amount = sector_extra[label]
        if amount:
            sector_rows.append({
                "label": label, "value": _money(amount), "pct": _pct(amount, invested_value),
                "count": None, "kind": kind_name,
            })
    classified = sum(row["value"] for row in sector_rows if row["kind"] == "sector")

    # --- 위험 모델 ---------------------------------------------------------------
    risk_model = compute_risk_model(
        risk_inputs,
        closes,
        min_returns=min_returns,
        min_history_ratio=min_history_ratio,
        invested_gross=sum(abs(v) for v in direct_values),
    )

    return {
        "available": priced_count > 0,
        "account_id": account_id,
        "totals": {
            "total_value": _money(total_value),
            "cash_value": _money(cash_value),
            "cash_pct": _pct(cash_value, total_value),
            "invested_value": _money(invested_value),
            "position_count": position_count,
            "priced_count": priced_count,
            "stale_quotes": stale_quotes,
            "unpriced": unpriced[:20],
            "unpriced_count": len(unpriced),
        },
        "exposures": {
            "rows": exposure_rows,
            "row_count": len(ranked),
            "etf_unknown_value": _money(etf_unknown),
            "etf_unknown_pct": _pct(etf_unknown, invested_value),
            "etf_other_value": _money(etf_other),
            "etf_other_pct": _pct(etf_other, invested_value),
            "leveraged_etf": leveraged_any,
            # 투자자산 − (발행사·자산 줄 + ETF 기타 + 미공개). 레버리지·인버스 ETF 가 없으면 0.
            "reconciliation_error": _money(invested_value - sum(nets) - etf_other - etf_unknown),
        },
        "concentration": concentration,
        "etfs": {
            "value": _money(sum(row["value"] for row in etf_rows)),
            "scope_value": _money(scope_value),
            "covered_value": _money(covered_value),
            "coverage_pct": _pct(covered_value, scope_value),
            "rows": etf_rows,
        },
        "holdcos": {
            # 설정값 주식수로 구한 추정치다. as_of 는 공시일이 아니라 지분 파일을 만든 날짜다.
            "estimated": True,
            "as_of": holdco_as_of,
            "total_look_through_value": _money(total_look_through),
            "total_look_through_pct": _pct(total_look_through, invested_value),
            "rows": holdco_rows,
            "subsidiary_rows": subsidiary_rows,
        },
        "breakdowns": {
            "currency": _bucket_rows(currency_buckets, total_value),
            "asset_class": asset_rows,
            "sector": {
                "rows": sector_rows,
                "classified_pct": _pct(classified, invested_value),
            },
        },
        "risk": risk_model,
        "sources": sources or [],
        "notes": list(METHOD_NOTES),
    }


# ---------------------------------------------------------------------------
# 데이터 기준일
# ---------------------------------------------------------------------------

def source_freshness(
    source_id: str,
    label: str,
    as_of: str | None,
    *,
    today: date,
    max_age_days: int,
    available: bool = True,
    detail: str | None = None,
) -> dict:
    """화면의 '데이터 기준일' 한 줄. 판정은 실행 시각이 아니라 자료 기준일로 한다."""
    parsed: date | None = None
    if as_of:
        try:
            parsed = datetime.fromisoformat(str(as_of)[:10]).date()
        except ValueError:
            parsed = None
    age = (today - parsed).days if parsed else None
    return {
        "id": source_id,
        "label": label,
        "as_of": parsed.isoformat() if parsed else None,
        "age_days": age,
        "stale": bool(age is not None and age > max_age_days),
        "available": available,
        "detail": detail,
    }
