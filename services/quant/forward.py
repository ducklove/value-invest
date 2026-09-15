"""설정 고정 이후의 일봉 전진 평가를 검증하고 저장한다."""

import json
import math

from core.errors import ExternalServiceError
from repositories import quant, quant_forward
from services.quant.models import completed_date

VERSION = "daily-forward-1"


def verify_ledger(ledger, snapshot, config, start):
    if (
        ledger.get("version") != VERSION
        or ledger.get("start") != start
        or ledger.get("orders_sent") != 0
        or ledger.get("execution_model") != "next_observation_close_replay"
        or ledger.get("ledger_hash") != quant.digest({k: v for k, v in ledger.items() if k != "ledger_hash"})
    ):
        raise quant.QuantError("전진 평가 버전·시작일·원장 검증에 실패했습니다.")
    selected = [b for b in snapshot["bars"] if b["date"] >= start]
    bars = {b["date"]: b for b in selected}
    if len(bars) != len(selected):
        raise quant.QuantError("전진 평가 입력에 중복 일봉이 있습니다.")
    scenarios = ledger.get("scenarios", [])
    if not bars:
        if ledger.get("status") != "waiting" or scenarios:
            raise quant.QuantError("미래 기간에 평가 결과가 생성됐습니다.")
        return
    if ledger.get("status") != "available" or [s["mode"] for s in scenarios] != ["switch", "mixed"]:
        raise quant.QuantError("전진 평가 비교 원장이 누락됐습니다.")
    days = list(bars)
    if days != sorted(set(days)):
        raise quant.QuantError("일봉 순서가 올바르지 않습니다.")
    for scenario in scenarios:
        if [n["date"] for n in scenario["nav"]] != days:
            raise quant.QuantError("평가 일자와 입력 일자가 다릅니다.")
        trades = scenario["trades"]
        if [t["date"] for t in trades] != sorted(t["date"] for t in trades):
            raise quant.QuantError("체결 원장의 순서가 올바르지 않습니다.")
        by_day = {}
        for t in trades:
            if (
                t["date"] not in bars
                or not start <= t["signal_date"] < t["date"]
                or t["date"] == days[0]
                or t["leg"] not in ("common", "preferred")
                or t["side"] not in ("buy", "sell")
                or type(t["quantity"]) is not int
                or t["quantity"] <= 0
                or t["price"] <= 0
                or t["fee"] < 0
            ):
                raise quant.QuantError("전진 평가 체결 계약을 위반했습니다.")
            by_day.setdefault(t["date"], []).append(t)
        cash, cost = config["capital"], 0.0
        peak, drawdown = cash, 0.0
        qty = {"common": 0, "preferred": 0}
        for n in scenario["nav"]:
            used = {"common": 0, "preferred": 0}
            for t in by_day.get(n["date"], []):
                sign = 1 if t["side"] == "buy" else -1
                source = bars[n["date"]][t["leg"]]
                price = source["price"] * (1 + sign * config["slippage_bps"] / 10000)
                rate = (config["commission_bps"] + (config["sell_tax_bps"] if sign < 0 else 0)) / 10000
                fee = t["quantity"] * price * rate
                expected_cost = fee + t["quantity"] * abs(price - source["price"])
                used[t["leg"]] += t["quantity"]
                limit = int(source["trading_value"] * config["participation"] / source["price"])
                if (
                    source["tradable"] is not True
                    or used[t["leg"]] > limit
                    or not math.isclose(t["price"], price, abs_tol=1e-8)
                    or not math.isclose(t["fee"], fee, abs_tol=1e-5)
                    or not math.isclose(t["cost"], expected_cost, abs_tol=1e-5)
                ):
                    raise quant.QuantError("전진 평가의 비용·거래 상태·참여율이 설정과 다릅니다.")
                cash -= sign * t["quantity"] * t["price"] + t["fee"]
                qty[t["leg"]] += sign * t["quantity"]
                cost += t["cost"]
                if cash < -1e-6 or min(qty.values()) < 0:
                    raise quant.QuantError("가상 현금·보유수량이 음수입니다.")
            nav = cash + sum(qty[k] * bars[n["date"]][k]["price"] for k in qty)
            peak = max(peak, nav)
            drawdown = min(drawdown, (nav / peak - 1) * 100)
            if (
                not math.isclose(n["cash"], cash, abs_tol=1e-5)
                or not math.isclose(n["nav"], nav, abs_tol=1e-5)
                or any(n[k + "_quantity"] != qty[k] for k in qty)
            ):
                raise quant.QuantError("전진 평가의 체결·현금·보유수량이 맞지 않습니다.")
        if (
            scenario["trade_count"] != len(trades)
            or not math.isclose(scenario["max_drawdown_pct"], drawdown, abs_tol=1e-8)
            or not math.isclose(scenario["ending_cash"], cash, abs_tol=1e-5)
            or not math.isclose(scenario["cost"], cost, abs_tol=1e-5)
            or not math.isclose(scenario["return_pct"], (nav / config["capital"] - 1) * 100, abs_tol=1e-8)
        ):
            raise quant.QuantError("전진 평가의 요약과 원장이 다릅니다.")
    quant.encode(ledger)


async def run_one(session):
    from services.quant import service

    try:
        original = json.loads(session["result_json"])
        config = json.loads(session["config_json"])
        service.verify(original, config)
        config["end"] = str(completed_date())
        if config["end"] < session["start_date"]:
            await quant_forward.save(session)
            return
        ready = await service.fetch("/api/research/readiness")
        if ready.get("status") != "ready" or ready.get("scope") != "pair_daily_prices":
            raise quant.QuantError("필요한 일봉 수집이 완전하지 않아 전진 평가를 보류했습니다.")
        result = await service.fetch("/api/research/pair-forward", {**config, "forward_start": session["start_date"]})
        service.verify(result, config)
        snapshot = result["snapshot"]
        if result["latest_signal"]["date"] != ready.get("checks", {}).get("latest_price_date"):
            raise quant.QuantError("최근 완료 일봉과 전진 평가의 시점이 다릅니다.")
        current = {b["date"]: b for b in snapshot["bars"]}
        old = original["snapshot"]
        previous = json.loads(session["payload_json"]) if session["payload_json"] else None
        protected = old["bars"] + (previous["input_extension"] if previous else [])
        if old.get("catalog") != snapshot.get("catalog"):
            raise quant.QuantError("전문 데이터 입력이 변경됐습니다. 새 연구가 필요합니다.")
        if any(current.get(b["date"]) != b for b in protected) or old.get("instrument_review") != snapshot.get(
            "instrument_review"
        ):
            raise quant.QuantError("이미 사용한 입력이 변경됐습니다. 기존 원장을 보존하고 평가를 보류합니다.")
        ledger = result["forward"]
        verify_ledger(ledger, snapshot, config, session["start_date"])
        if previous and previous["ledger"]["status"] == "available":
            for past, new in zip(previous["ledger"]["scenarios"], ledger["scenarios"], strict=True):
                if any(new[k][: len(past[k])] != past[k] for k in ("nav", "trades")):
                    raise quant.QuantError("이미 기록한 전진 평가 원장이 달라졌습니다.")
        await quant_forward.save(
            session,
            {
                "ledger": ledger,
                "base_snapshot_id": old["snapshot_id"],
                "snapshot_id": snapshot["snapshot_id"],
                "config_hash": original["config_hash"],
                "config": original["config"],
                "engine_version": result["engine_version"],
                "input_extension": [b for b in snapshot["bars"] if b["date"] > original["config"]["end"]],
                "data_readiness": ready,
            },
        )
    except (ExternalServiceError, quant.QuantError, ValueError, KeyError, TypeError, IndexError) as exc:
        detail = (
            str(exc)
            if isinstance(exc, (ExternalServiceError, quant.QuantError))
            else "전진 평가 자료 검증에 실패했습니다."
        )
        await quant_forward.save(session, error=detail)
