from __future__ import annotations

import math
import statistics
from typing import Any


RETURN_WINDOWS = {
    "1m": 21,
    "3m": 63,
    "6m": 126,
    "1y": 252,
}

BOND_CODE_HINTS = {
    "AGG", "BND", "BNDX", "BSV", "HYG", "IEF", "IEI", "IGIB", "JNK",
    "LQD", "MBB", "SHV", "SHY", "TLT", "TIP", "TLH", "VGIT", "VCIT",
}

BOND_NAME_HINTS = (
    "bond", "treasury", "aggregate", "income", "duration", "채권", "국채",
    "회사채", "하이일드", "단기채", "장기채",
)

ETF_NAME_HINTS = ("etf", "fund", "trust", "ishares", "vanguard", "spdr", "invesco")

ASSET_CLASS_LABELS = {
    "korean_stock": "국내주식",
    "foreign_stock": "해외주식",
    "foreign_etf": "해외 ETF",
    "bond_etf": "채권/채권 ETF",
    "cash": "현금/통화",
    "gold": "금",
    "crypto": "가상자산",
    "other": "기타자산",
}


def safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number) or math.isinf(number):
        return None
    return number


def percent_change(current: float | None, base: float | None) -> float | None:
    if current is None or base is None or base == 0:
        return None
    return round((current / base - 1) * 100, 2)


def is_korean_stock(code: str) -> bool:
    return len(code) == 6 and code[:5].isdigit()


def classify_asset(code: str, name: str = "", currency: str = "") -> dict[str, str]:
    code_up = (code or "").upper()
    text = f"{code_up} {name or ''}".lower()
    currency_up = (currency or "").upper()

    if code_up.startswith("CASH_"):
        asset_class = "cash"
    elif code_up == "KRX_GOLD":
        asset_class = "gold"
    elif code_up.startswith("CRYPTO_"):
        asset_class = "crypto"
    elif is_korean_stock(code_up):
        asset_class = "korean_stock"
    elif code_up.split(".", 1)[0] in BOND_CODE_HINTS or any(hint in text for hint in BOND_NAME_HINTS):
        asset_class = "bond_etf"
    elif any(hint in text for hint in ETF_NAME_HINTS):
        asset_class = "foreign_etf"
    elif currency_up and currency_up != "KRW":
        asset_class = "foreign_stock"
    elif any(ch.isalpha() for ch in code_up):
        asset_class = "foreign_stock"
    else:
        asset_class = "other"

    return {
        "assetClass": asset_class,
        "assetClassLabel": ASSET_CLASS_LABELS.get(asset_class, "기타자산"),
    }


def yfinance_ticker_for_special_asset(code: str) -> str | None:
    return {
        "KRX_GOLD": "GC=F",
        "CRYPTO_BTC": "BTC-USD",
        "CRYPTO_ETH": "ETH-USD",
        "CASH_USD": "KRW=X",
        "CASH_EUR": "EURKRW=X",
        "CASH_JPY": "JPYKRW=X",
        "CASH_CNY": "CNYKRW=X",
        "CASH_HKD": "HKDKRW=X",
        "CASH_GBP": "GBPKRW=X",
        "CASH_AUD": "AUDKRW=X",
        "CASH_CAD": "CADKRW=X",
        "CASH_CHF": "CHFKRW=X",
    }.get((code or "").upper())


def calculate_position(item: dict[str, Any], quote: dict[str, Any] | None) -> dict[str, Any]:
    quote = quote or {}
    qty = safe_float(item.get("quantity")) or 0.0
    avg_price = safe_float(item.get("avg_price")) or 0.0
    current_price = safe_float(quote.get("price"))
    change = safe_float(quote.get("change"))
    change_pct = safe_float(quote.get("change_pct"))

    invested = qty * avg_price
    market_value = qty * current_price if current_price is not None else None
    pnl = market_value - invested if market_value is not None else None
    return_pct = percent_change(market_value, invested) if invested else None
    daily_pnl = qty * change if change is not None else None

    return {
        "quantity": qty,
        "avgPrice": avg_price,
        "currentPrice": current_price,
        "invested": round(invested, 2),
        "marketValue": round(market_value, 2) if market_value is not None else None,
        "pnl": round(pnl, 2) if pnl is not None else None,
        "returnPct": return_pct,
        "dailyChangePct": change_pct,
        "dailyPnl": round(daily_pnl, 2) if daily_pnl is not None else None,
    }


def calculate_history_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    clean_rows = [
        {"date": str(row.get("date")), "close": safe_float(row.get("close"))}
        for row in rows
        if row.get("date") and safe_float(row.get("close")) is not None
    ]
    clean_rows.sort(key=lambda row: row["date"])
    closes = [row["close"] for row in clean_rows]
    if not closes:
        return {
            "historyPoints": 0,
            "returns": {},
            "volatility": {},
            "maxDrawdownPct": None,
            "high52": None,
            "low52": None,
            "fromHigh52Pct": None,
            "fromLow52Pct": None,
            "latestDate": None,
        }

    latest = closes[-1]
    returns = {}
    for label, window in RETURN_WINDOWS.items():
        if len(closes) > window:
            returns[label] = percent_change(latest, closes[-window - 1])
        else:
            returns[label] = None

    daily_returns = [
        (closes[i] / closes[i - 1] - 1)
        for i in range(1, len(closes))
        if closes[i - 1]
    ]
    volatility = {}
    for label, window in (("20d", 20), ("60d", 60)):
        sample = daily_returns[-window:]
        volatility[label] = (
            round(statistics.stdev(sample) * math.sqrt(252) * 100, 2)
            if len(sample) >= 2 else None
        )

    high52 = max(closes)
    low52 = min(closes)
    max_drawdown = _max_drawdown_pct(closes)

    return {
        "historyPoints": len(clean_rows),
        "returns": returns,
        "volatility": volatility,
        "maxDrawdownPct": max_drawdown,
        "high52": round(high52, 4),
        "low52": round(low52, 4),
        "fromHigh52Pct": percent_change(latest, high52),
        "fromLow52Pct": percent_change(latest, low52),
        "latestDate": clean_rows[-1]["date"],
    }


def relative_returns(asset_returns: dict[str, Any], benchmark_returns: dict[str, Any]) -> dict[str, float | None]:
    result: dict[str, float | None] = {}
    for key, value in asset_returns.items():
        bench_value = benchmark_returns.get(key)
        if value is None or bench_value is None:
            result[key] = None
        else:
            result[key] = round(float(value) - float(bench_value), 2)
    return result


def build_signals(
    profile: dict[str, Any],
    position: dict[str, Any],
    metrics: dict[str, Any],
    benchmark: dict[str, Any],
    gold_gap: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    signals: list[dict[str, str]] = []
    asset_class = profile.get("assetClass")

    return_pct = position.get("returnPct")
    if return_pct is not None:
        if return_pct >= 20:
            signals.append({
                "level": "positive",
                "title": "수익 구간",
                "body": "평균 매입가 대비 여유가 있습니다. 추가매수보다 리밸런싱 기준을 먼저 점검해볼 만합니다.",
            })
        elif return_pct <= -15:
            signals.append({
                "level": "warning",
                "title": "손실 구간",
                "body": "가격 하락인지 환율/금리 요인인지 분리해서 확인하는 편이 좋습니다.",
            })

    from_high = metrics.get("fromHigh52Pct")
    if from_high is not None and from_high <= -20:
        signals.append({
            "level": "warning",
            "title": "52주 고점 대비 큰 낙폭",
            "body": f"최근 1년 고점 대비 {abs(from_high):.1f}% 낮습니다. 추세 회복 전 분할 접근이 안전합니다.",
        })

    vol60 = (metrics.get("volatility") or {}).get("60d")
    if vol60 is not None and vol60 >= 35:
        signals.append({
            "level": "warning",
            "title": "높은 변동성",
            "body": f"60일 연율화 변동성이 {vol60:.1f}%입니다. 포지션 크기와 손실 허용폭을 함께 봐야 합니다.",
        })

    rel_3m = (benchmark.get("relativeReturns") or {}).get("3m")
    if rel_3m is not None:
        if rel_3m >= 5:
            signals.append({
                "level": "positive",
                "title": "벤치마크 대비 강세",
                "body": f"최근 3개월 기준 벤치마크를 {rel_3m:.1f}%p 앞서고 있습니다.",
            })
        elif rel_3m <= -5:
            signals.append({
                "level": "warning",
                "title": "벤치마크 대비 약세",
                "body": f"최근 3개월 기준 벤치마크보다 {abs(rel_3m):.1f}%p 뒤처집니다.",
            })

    if asset_class == "bond_etf":
        signals.append({
            "level": "info",
            "title": "채권형 자산 체크포인트",
            "body": "가격보다 금리 방향, 듀레이션, 환헤지 여부가 수익률을 크게 좌우합니다.",
        })

    if gold_gap and gold_gap.get("latestGapPct") is not None:
        latest_gap = float(gold_gap["latestGapPct"])
        threshold = float(gold_gap.get("thresholdPct") or 5)
        level = "warning" if abs(latest_gap) >= threshold else "info"
        signals.append({
            "level": level,
            "title": "국내 프리미엄 점검",
            "body": f"현재 괴리율은 {latest_gap:+.2f}%입니다. 높은 괴리율에서는 자산 가격 상승과 국내 프리미엄을 구분해야 합니다.",
        })

    if not signals:
        signals.append({
            "level": "neutral",
            "title": "특이 신호 없음",
            "body": "가격, 변동성, 벤치마크 대비 흐름에서 큰 경고 신호는 감지되지 않았습니다.",
        })
    return signals


def _max_drawdown_pct(closes: list[float]) -> float | None:
    peak = None
    max_drawdown = 0.0
    for close in closes:
        if peak is None or close > peak:
            peak = close
        if peak:
            drawdown = close / peak - 1
            max_drawdown = min(max_drawdown, drawdown)
    return round(max_drawdown * 100, 2)
