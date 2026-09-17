"""실시간 호가 기반 가상 현선물 원장. 브로커 주문을 호출하지 않는 순수 계산기."""

import math
import re
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, model_validator

from services.quant import rollover
from services.quant.scanner_model import KST, edge

MODEL = "live-book-pair-fok-v1"


class PaperConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    capital: float = Field(default=100_000_000, ge=100_000, le=1_000_000_000_000)
    allocation_pct: float = Field(default=10, ge=1, le=20)
    max_positions: int = Field(default=5, ge=1, le=10)
    participation_pct: float = Field(default=25, ge=1, le=50)
    delay_seconds: int = Field(default=1, ge=1, le=10)
    timeout_seconds: int = Field(default=10, ge=5, le=60)
    exit_basis_bps: float = Field(default=10, ge=-100, le=100)
    stop_loss_bps: float = Field(default=100, ge=10, le=1000)

    @model_validator(mode="after")
    def timing(self):
        if self.timeout_seconds <= self.delay_seconds:
            raise ValueError("대기 만료는 체결 지연보다 길어야 합니다.")
        return self


def initial(config, account_id, now):
    return {"model": MODEL, "config": config.model_dump(), "account_id": account_id,
            "enabled": True, "started_at": now, "positions": {}, "pending": {},
            "realized_pnl": 0., "total_costs": 0., "closed": 0, "wins": 0,
            "fills": 0, "cancels": 0, "last_quote_at": None, "cooldowns": {}}


def unit(row):
    # 표준 주식선물만. 조정 계약을 기본 승수 10으로 추측하지 않는다.
    if not re.search(r"\(\s*10\s*\)\s*$", row.get("contract_name", "")):
        raise ValueError("표준 거래승수 10 미확인")
    return 10


def fresh(spot, future, now):
    if not rollover.trading_day(now.date()) or not "09:00" <= now.strftime("%H:%M") < "15:20":
        raise ValueError("연속매매 관찰시간 밖")
    for q in (spot, future):
        if not all(math.isfinite(float(q[k])) for k in ("bid", "ask", "bid_size", "ask_size", "at", "received_at")):
            raise ValueError("유효하지 않은 호가")
        if not 0 < q["bid"] <= q["ask"] or min(q["bid_size"], q["ask_size"]) < 0:
            raise ValueError("호가·잔량 오류")
        if not all(float(q[k]).is_integer() for k in ("bid_size", "ask_size")):
            raise ValueError("정수 잔량 필요")
        if not 0 <= now.timestamp() - q["at"] <= 5 or not 0 <= now.timestamp() - q["received_at"] <= 5:
            raise ValueError("호가 지연")
    if abs(spot["at"] - future["at"]) > 2:
        raise ValueError("양쪽 호가 시각 불일치")


def costs(s, f, c, *, closing=False):
    return {"commission": (s * c["spot_fee_bps"] + f * c["future_fee_bps"]) / 10000,
            "tax": s * c["sell_tax_bps"] / 10000 if closing else 0.,
            "slippage": (s + f) * c["slippage_bps"] / 10000}


def mark(position, spot, future, now):
    """현물 bid와 선물 ask에 지금 청산했을 때의 비용 차감 평가액."""
    shares = position["contracts"] * 10
    c = position["cost_config"]
    exit_cost = costs(spot["bid"], future["ask"], c, closing=True)
    held_days = max(0., (now.timestamp() - position["entered_at"]) / 86400)
    funding = position["funded_per_share"] * c["funding_pct"] / 100 * held_days / 365
    gross = spot["bid"] - position["entry_spot"] + position["entry_future"] - future["ask"]
    total = position["entry_cost_per_share"] + sum(exit_cost.values()) + funding
    return {"at": now.timestamp(), "net_pnl": shares * (gross - total), "gross_pnl": shares * gross,
            "total_costs": shares * total, "funding": shares * funding, "exit_costs": exit_cost,
            "spot_bid": spot["bid"], "future_ask": future["ask"], "spot": spot, "future": future}


def available(state):
    reserved = sum(p["contracts"] * 10 * (p["funded_per_share"] + p["entry_cost_per_share"])
                   for p in state["positions"].values())
    losses = sum(min(0., p.get("mark", {}).get("net_pnl", 0.)) for p in state["positions"].values())
    return max(0., state["config"]["capital"] + state["realized_pnl"] - reserved + losses)


def exit_reason(position, spot, future, now, cfg):
    if now.date().isoformat() >= position["row"]["roll_on"]:
        return "월물 전환"
    m = mark(position, spot, future, now)
    if m["net_pnl"] <= -position["contracts"] * 10 * position["entry_spot"] * cfg.stop_loss_bps / 10000:
        return "손실 한도"
    if (future["ask"] - spot["bid"]) / spot["bid"] * 10000 <= cfg.exit_basis_bps:
        return "베이시스 축소"
    return None


def event(kind, code, now, **details):
    return {"type": kind, "contract": code, "at": now.timestamp(), "model": MODEL, **details}


def expire_pending(state, now):
    events = []
    for code, pending in list(state["pending"].items()):
        if now.timestamp() - pending["at"] >= state["config"]["timeout_seconds"]:
            state["pending"].pop(code)
            state["cancels"] += 1
            events.append(event("cancel", code, now, reason="후속 양쪽 호가 확인 시간 초과", pending=pending))
    return events


def step(state, row, spot, future, config, now):
    """판단 후 최소 1초 + 양쪽의 새 거래소 호가에서만 전량 조건 가상 체결."""
    events = expire_pending(state, now)
    code, stamp = row["contract"], now.timestamp()
    cfg = PaperConfig(**state["config"])
    position = state["positions"].get(code)
    pending = state["pending"].get(code)
    try:
        fresh(spot, future, now)
        unit(row)
        if now.date().strftime("%Y%m%d") > row["expiry"] or (not position and now.date().strftime("%Y%m%d") == row["expiry"]):
            raise ValueError("만기 도달: 자동 결제 가격을 가정하지 않음")
        if not row.get("expiry_verified") or stamp - row.get("observed_at", 0) > 180:
            raise ValueError("최근 REST 계약·시장 상태 확인 대기")
        if row.get("error"):
            raise ValueError("REST 관측 오류")
    except (ValueError, KeyError, TypeError, OverflowError) as exc:
        state["last_rejection"] = {"contract": code, "at": stamp, "reason": str(exc)}
        if position:
            position["blocked"] = str(exc)
        return events
    state["last_quote_at"] = stamp
    if position:
        position.pop("blocked", None)
        position["row"] = dict(row)
        position["mark"] = mark(position, spot, future, now)
        reason = exit_reason(position, spot, future, now, cfg)
        kind = "exit"
    else:
        reason, kind = "순우위 기준 충족", "entry"
        if not state["enabled"] or not rollover.active(row, now.date()) or state["cooldowns"].get(code, 0) > stamp:
            reason = None
        elif any(p.get("blocked") or stamp - p.get("mark", {}).get("at", 0) > 5 for p in state["positions"].values()):
            reason = None
            state["last_rejection"] = {"contract": code, "at": stamp, "reason": "기존 가상 포지션의 새 청산호가 평가 대기"}
        else:
            try:
                if edge(spot, future, row["expiry"], config, now, realtime=True)["net_bps"] < config.signal_bps:
                    reason = None
            except ValueError:
                reason = None
    if pending and (pending["kind"] != kind or reason is None):
        state["pending"].pop(code)
        state["cancels"] += 1
        events.append(event("cancel", code, now, reason="체결 전 조건 해제", pending=pending))
        pending = None
    if reason is None:
        return events
    if not pending:
        if kind == "entry" and (len(state["positions"]) >= cfg.max_positions
                                or any(p["row"]["spot_code"] == row["spot_code"] for p in state["positions"].values())):
            return events
        pending = {"kind": kind, "at": stamp, "spot_at": spot["at"], "future_at": future["at"], "reason": reason}
        state["pending"][code] = pending
        events.append(event("pending", code, now, pending=pending, spot=spot, future=future))
        return events
    if (stamp - pending["at"] < cfg.delay_seconds or spot["at"] <= pending["spot_at"]
            or future["at"] <= pending["future_at"]):
        return events
    # 잔량은 현물 주수, 선물 계약수다. 최우선 잔량의 일부만 사용한다.
    rate = cfg.participation_pct / 100
    liquidity = math.floor(min(spot["ask_size" if kind == "entry" else "bid_size"] / 10,
                               future["bid_size" if kind == "entry" else "ask_size"]) * rate)
    if kind == "entry":
        if len(state["positions"]) >= cfg.max_positions or any(p["row"]["spot_code"] == row["spot_code"] for p in state["positions"].values()):
            state["pending"].pop(code)
            state["cancels"] += 1
            events.append(event("cancel", code, now, reason="보유 종목·개수 한도"))
            return events
        c = config.model_dump()
        entry_cost = costs(spot["ask"], future["bid"], c)
        funded = spot["ask"] + future["bid"] * c["margin_pct"] / 100
        budget = min(available(state), cfg.capital * cfg.allocation_pct / 100)
        qty = min(liquidity, math.floor(budget / (10 * (funded + sum(entry_cost.values())))))
    else:
        qty = min(liquidity, position["contracts"])
    state["pending"].pop(code)
    if qty < 1:
        if position:
            position["blocked"] = "청산 잔량 부족: 잔여 포지션 유지"
        state["cancels"] += 1
        events.append(event("cancel", code, now, reason="체결 잔량 또는 가상자금 부족", pending=pending, spot=spot, future=future))
        state["cooldowns"][code] = stamp + 10
        return events
    shares = qty * 10
    if kind == "entry":
        position = {"row": dict(row), "contracts": qty, "initial_contracts": qty,
                    "entered_at": stamp, "entry_spot": spot["ask"], "entry_future": future["bid"],
                    "entry_cost_per_share": sum(entry_cost.values()), "funded_per_share": funded,
                    "cost_config": c, "realized_pnl": 0.}
        state["positions"][code] = position
        position["mark"] = mark(position, spot, future, now)
        state["total_costs"] += sum(entry_cost.values()) * shares
        details = {"spot_side": "buy", "spot_price": spot["ask"], "future_side": "sell", "future_price": future["bid"],
                   "costs": {k: v * shares for k, v in entry_cost.items()}, "net_pnl": None, "cost_config": c}
    else:
        m = mark(position, spot, future, now)
        ratio = qty / position["contracts"]
        pnl = m["net_pnl"] * ratio
        state["realized_pnl"] += pnl
        position["realized_pnl"] += pnl
        state["total_costs"] += sum(m["exit_costs"].values()) * shares + m["funding"] * ratio
        details = {"spot_side": "sell", "spot_price": spot["bid"], "future_side": "buy", "future_price": future["ask"],
                   "costs": {k: v * shares for k, v in m["exit_costs"].items()} | {"funding": m["funding"] * ratio},
                   "net_pnl": pnl, "gross_pnl": m["gross_pnl"] * ratio, "cost_config": position["cost_config"]}
        position["contracts"] -= qty
        if not position["contracts"]:
            state["closed"] += 1
            state["wins"] += int(position["realized_pnl"] > 0)
            state["positions"].pop(code)
            state["cooldowns"][code] = stamp + 60
        else:
            position["mark"] = mark(position, spot, future, now)
    state["fills"] += 1
    events.append(event("fill", code, now, action=kind, name=row["name"], reason=reason,
                        contracts=qty, shares=shares, spot=spot, future=future, decision_at=pending["at"], **details))
    return events


def summary(state, now=None):
    now = now or datetime.now(KST)
    positions = list(state["positions"].values())
    stale = sum(now.timestamp() - p.get("mark", {}).get("at", 0) > 5 or bool(p.get("blocked")) for p in positions)
    pnl = sum(p.get("mark", {}).get("net_pnl", 0.) for p in positions)
    equity = state["config"]["capital"] + state["realized_pnl"] + pnl
    return {"equity": equity, "unrealized_pnl": pnl, "realized_pnl": state["realized_pnl"],
            "return_pct": (equity / state["config"]["capital"] - 1) * 100, "available": available(state),
            "stale_positions": stale, "positions": positions, "valuation_at": min((p.get("mark", {}).get("at", 0) for p in positions), default=None)}
