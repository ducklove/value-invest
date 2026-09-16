"""전체 순회 후보와 실시간 기회를 구분하는 현선물 관찰 모델."""

import math
import re
from datetime import datetime, timedelta, timezone

from pydantic import BaseModel, ConfigDict, Field, model_validator

from repositories.quant import QuantError

KST = timezone(timedelta(hours=9))


class ScannerConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    account_id: str = Field(min_length=1, max_length=100)
    enabled: bool = False
    interval_minutes: int = Field(default=60, ge=5, le=240)
    watch_bps: float = Field(default=30, ge=0, le=5000)
    signal_bps: float = Field(default=50, ge=0, le=5000)
    max_pairs: int = Field(default=10, ge=1, le=15)
    hold_minutes: int = Field(default=5, ge=1, le=60)
    spot_fee_bps: float = Field(default=1, ge=0, le=100)
    future_fee_bps: float = Field(default=0.6, ge=0, le=100)
    sell_tax_bps: float = Field(default=20, ge=0, le=100)
    slippage_bps: float = Field(default=2, ge=0, le=100)
    funding_pct: float = Field(default=4, ge=0, le=40)
    margin_pct: float = Field(default=20, ge=1, le=100)
    leg_risk_bps: float = Field(default=10, ge=0, le=100)

    @model_validator(mode="after")
    def thresholds(self):
        if self.signal_bps < self.watch_bps:
            raise ValueError("기회 기준은 감시 편입 기준 이상이어야 합니다.")
        return self


def master_rows(raw: bytes) -> list[dict]:
    if not raw or len(raw) > 2_000_000 or len(raw) % 97:
        raise QuantError("나무 주식선물 마스터 길이가 명세와 다릅니다.")
    rows, seen = [], set()
    for offset in range(0, len(raw), 97):
        rec = raw[offset:offset + 97]
        code = rec[:9].decode("ascii").rstrip()
        stock = rec[69:75].decode("ascii").rstrip()
        if rec[-1:] != b"\n" or not re.fullmatch(r"K[A-Z0-9]{8}", code) or not re.fullmatch(r"[0-9A-Z]{6}", stock) or code in seen:
            raise QuantError("나무 주식선물 마스터 코드·경계·중복 검사 실패")
        seen.add(code)
        rows.append({"contract": code, "spot_code": stock,
                     "name": rec[75:95].decode("cp949", errors="replace").rstrip(),
                     "contract_name": rec[9:39].decode("cp949", errors="replace").rstrip(),
                     "market": rec[95:96].decode("ascii")})
    return rows


def number(value) -> float:
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ValueError("호가 숫자 오류")
    return result


def quote_time(raw, now):
    value = str(raw).replace(":", "")
    if not re.fullmatch(r"\d{6}", value):
        raise ValueError("호가시각 없음")
    parsed = datetime.strptime(value, "%H%M%S")
    at = now.replace(hour=parsed.hour, minute=parsed.minute, second=parsed.second, microsecond=0)
    if at > now + timedelta(seconds=2):
        raise ValueError("미래 호가시각")
    return at.timestamp()


def book(bid, ask, bid_size, ask_size, raw_time, now):
    b, a = number(bid), number(ask)
    bs, az = number(bid_size), number(ask_size)
    if not (0 < b <= a) or not bs.is_integer() or not az.is_integer():
        raise ValueError("호가·잔량 오류")
    return {"bid": b, "ask": a, "bid_size": int(bs), "ask_size": int(az),
            "at": quote_time(raw_time, now), "received_at": now.timestamp()}


def realtime_book(message, now):
    h, b = message.get("header", {}), message.get("body", {})
    if not isinstance(h, dict) or not isinstance(b, dict) or "rsp_cd" in h or "tr_type" in h:
        return None
    channel = h.get("tr_cd")
    if channel not in {"ob", "vH"}:
        return None
    key = b.get("fuitem" if channel == "vH" else "code")
    if key != h.get("tr_key"):
        return None
    try:
        q = book(b["bid"], b["offer"], b["bidjan" if channel == "vH" else "bidrem"],
                 b["offerjan" if channel == "vH" else "offerrem"],
                 b["futime" if channel == "vH" else "hotime"], now)
    except (KeyError, ValueError, TypeError, OverflowError):
        return None
    return (channel, key), q


def edge(spot, future, expiry, config, now, *, realtime=False):
    """현물 매수/선물 매도의 보수적 만기 수렴 추정. 배당 수입 0, 실주문 불가."""
    if now.weekday() >= 5 or not ("09:00" <= now.strftime("%H:%M") < "15:20"):
        raise ValueError("동시 거래시간 밖")
    days = (datetime.strptime(expiry, "%Y%m%d").date() - now.date()).days
    if not 0 < days <= 366:
        raise ValueError("만기 당일·허용 기간 밖")
    age, skew = (5, 2) if realtime else (120, 90)
    if any(not 0 <= now.timestamp() - q["at"] <= age for q in (spot, future)) or abs(spot["at"] - future["at"]) > skew:
        raise ValueError("호가 지연·시각 불일치")
    if not spot["ask_size"] or not future["bid_size"]:
        raise ValueError("양쪽 잔량 부족")
    s, f = spot["ask"], future["bid"]
    # 주당 값이다. 계약별 조정승수·권리 검증 전에는 주문 수량으로 변환하지 않는다.
    commission = (s * 2 * config.spot_fee_bps + (s + f) * config.future_fee_bps) / 10000
    tax = s * config.sell_tax_bps / 10000
    financing = (s + f * config.margin_pct / 100) * config.funding_pct / 100 * days / 365
    allowance = (s + f) * 2 * config.slippage_bps / 10000 + s * config.leg_risk_bps / 10000
    net = f - s - commission - tax - financing - allowance
    return {"gross_bps": (f - s) / s * 10000, "net_bps": net / s * 10000,
            "net_per_share": net, "cost_per_share": commission + tax + financing + allowance,
            "days": days, "spot": spot, "future": future, "live_eligible": False}


def watch_list(rows, previous, config, now):
    """최소 보유시간과 신규 진입 문턱을 다르게 적용한다. 최대 30개 호가 등록."""
    selected = {}
    valid = {r["contract"]: r for r in rows if r.get("net_bps") is not None
             and now - r["observed_at"] < config.interval_minutes * 120}
    for code, entered in previous.items():
        if code in valid and (now - entered < config.hold_minutes * 60 or valid[code]["net_bps"] >= config.watch_bps * .5):
            selected[code] = entered
    selected = dict(list(selected.items())[:config.max_pairs])
    for row in sorted(valid.values(), key=lambda r: r["net_bps"], reverse=True):
        if len(selected) >= config.max_pairs:
            break
        if row["net_bps"] >= config.watch_bps:
            selected.setdefault(row["contract"], now)
    return selected
