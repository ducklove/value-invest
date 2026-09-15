"""finance-pi 연구 연결과 재시작 가능한 대기열. 증권사 호출은 하지 않는다."""

import asyncio
import json
import logging

import aiosqlite
import httpx

import close_price_client
from core.errors import ExternalServiceError
from core.http import get_http_client
from repositories import quant, quant_forward
from services.quant.models import completed_date

logger = logging.getLogger(__name__)
EXPECTED_ENGINE = "preferred-switch-2"
EXPECTED_ENGINES = {"preferred_switch": EXPECTED_ENGINE, "etf_switch": "etf-switch-2"}


async def fetch(path, params=None):
    if not close_price_client.ENABLED:
        raise ExternalServiceError("finance-pi 연결이 비활성화돼 있습니다.")
    client = await get_http_client("quant_research")
    headers = {"X-Admin-Token": close_price_client.API_TOKEN} if close_price_client.API_TOKEN else {}
    try:
        response = await client.get(close_price_client.BASE_URL + path, params=params, headers=headers)
        if response.status_code == 400:
            raise quant.QuantError(
                "연구 입력 또는 가격 자료를 확인해 주세요. 자료 부족·중복·미지원 관계일 수 있습니다."
            )
        if path != "/api/ready":
            response.raise_for_status()
        data = response.json()
    except (httpx.HTTPError, ValueError):
        raise ExternalServiceError("finance-pi 연구 응답을 받지 못했습니다. 기존 결과는 유지됩니다.") from None
    if not isinstance(data, dict):
        raise ExternalServiceError("finance-pi 응답 형식이 올바르지 않습니다.")
    return data


def verify(result, config):
    if result.get("engine_version") != EXPECTED_ENGINES.get(config["strategy"]):
        raise quant.QuantError("연구 엔진 버전이 다릅니다. 재검증이 필요합니다.")
    if result.get("config_hash") != quant.digest(config) or result.get("config") != config:
        raise quant.QuantError("요청과 다른 전략 설정의 결과를 거절했습니다.")
    snapshot = result.get("snapshot", {})
    raw = {k: v for k, v in snapshot.items() if k != "snapshot_id"}
    if snapshot.get("snapshot_id") != quant.digest(raw):
        raise quant.QuantError("데이터 스냅샷 검증에 실패했습니다.")
    if snapshot.get("common") != config["common"] or snapshot.get("preferred") != config["preferred"]:
        raise quant.QuantError("다른 종목의 결과를 거절했습니다.")
    if result.get("live_eligible") is not False or not result.get("signals"):
        raise quant.QuantError("연구 결과의 실행 제한 또는 신호가 유효하지 않습니다.")
    scenarios = result.get("scenarios", [])
    if [s.get("mode") for s in scenarios] != ["switch", "common", "preferred", "mixed"]:
        raise quant.QuantError("비교전략 결과가 완전하지 않습니다.")
    for scenario in scenarios:
        if not scenario.get("nav") or not isinstance(scenario.get("trades"), list):
            raise quant.QuantError("평가 원장이 누락됐습니다.")
        for key in ("return_pct", "max_drawdown_pct", "cost", "trade_count"):
            if not isinstance(scenario.get(key), (int, float)):
                raise quant.QuantError("비교전략 성과가 유효하지 않습니다.")
    latest = result.get("latest_signal")
    if latest != result["signals"][-1] or not config["start"] <= latest["date"] <= config["end"]:
        raise quant.QuantError("최신 신호의 날짜가 연구 기간과 다릅니다.")
    if not isinstance(result.get("stress", {}).get("return_pct"), (int, float)):
        raise quant.QuantError("비용 스트레스 결과가 누락됐습니다.")
    quant.encode(result)  # 비정상 실수도 저장 전에 거절한다.


async def run_one():
    row = await quant.claim()
    if row is None:
        return False
    try:
        config = json.loads(row["config_json"])
        result = await fetch("/api/research/pair-analysis", config)
        verify(result, config)
        await quant.finish(row["id"], result)
    except (ExternalServiceError, quant.QuantError, ValueError, KeyError, TypeError) as exc:
        detail = (
            str(exc) if isinstance(exc, (ExternalServiceError, quant.QuantError)) else "연구 결과 검증에 실패했습니다."
        )
        await quant.finish(row["id"], error=detail)
    return True


async def observe_one(watch):
    try:
        original = json.loads(watch["result_json"])
        if original.get("engine_version") != EXPECTED_ENGINES.get(original["config"]["strategy"]):
            raise quant.QuantError("연구 엔진이 변경됐습니다. 새 실험으로 재검증해 주세요.")
        ready = await fetch("/api/research/readiness")
        if ready.get("status") != "ready" or ready.get("scope") != "pair_daily_prices":
            raise quant.QuantError("필요한 일봉 수집이 완전하지 않아 신규 관찰을 보류했습니다.")
        config = json.loads(watch["config_json"])
        original_end = config["end"]
        config["end"] = str(completed_date())
        if config["end"] <= original_end:
            await quant.record_observation(watch)
            return
        result = await fetch("/api/research/pair-analysis", config)
        verify(result, config)
        if original["snapshot"].get("instrument_review") != result["snapshot"].get("instrument_review"):
            raise quant.QuantError("ETF 상품 검토 기준이 변경됐습니다. 새 실험으로 재검증해 주세요.")
        old = {r["date"]: r for r in original["snapshot"]["bars"]}
        previous = await quant.latest_observation(watch)
        if previous:
            old.update({r["date"]: r for r in previous.get("input_extension", [])})
        current = {r["date"]: r for r in result["snapshot"]["bars"]}
        if any(current.get(day) != bar for day, bar in old.items()):
            raise quant.QuantError("기존 입력 자료가 수정됐습니다. 새 실험으로 재검증해 주세요.")
        signal = result["latest_signal"]
        if signal["date"] <= original_end:
            await quant.record_observation(watch)
            return
        latest = ready.get("checks", {}).get("latest_price_date")
        if signal["date"] != latest:
            raise quant.QuantError("최근 완료 일봉과 관찰 시점이 다릅니다.")
        await quant.record_observation(
            watch,
            {
                "signal": signal,
                "snapshot_id": result["snapshot"]["snapshot_id"],
                "engine_version": result["engine_version"],
                "base_snapshot_id": original["snapshot"]["snapshot_id"],
                "config": config,
                "input_extension": [b for b in result["snapshot"]["bars"] if b["date"] > original_end],
                "mode": "signal_observation",
                "orders_sent": 0,
                "data_readiness": ready,
            },
        )
    except (ExternalServiceError, quant.QuantError, ValueError, KeyError, TypeError) as exc:
        detail = (
            str(exc) if isinstance(exc, (ExternalServiceError, quant.QuantError)) else "관찰 자료 검증에 실패했습니다."
        )
        await quant.record_observation(watch, error=detail)


async def run_loop(stop):
    await quant.recover()
    while not stop.is_set():
        try:
            if not await run_one():
                for watch in await quant.watches():
                    if stop.is_set():
                        break
                    await observe_one(watch)
                session = await quant_forward.due()
                if session and not stop.is_set():
                    from services.quant.forward import run_one as forward_one

                    await forward_one(session)
        except (aiosqlite.Error, ExternalServiceError, quant.QuantError) as exc:
            logger.warning("퀀트 작업 보류: %s", type(exc).__name__)
        try:
            await asyncio.wait_for(stop.wait(), timeout=5)
        except TimeoutError:
            pass


async def capabilities():
    try:
        pairs = await fetch("/api/research/pairs")
        ready = await fetch("/api/ready")
        research_ready = await fetch("/api/research/readiness")
        return {
            **pairs,
            "readiness": ready,
            "research_readiness": research_ready,
            "live_enabled": False,
            "mode": "research_and_observation",
            "error": None,
        }
    except ExternalServiceError as exc:
        return {
            "pairs": [],
            "readiness": None,
            "live_enabled": False,
            "mode": "research_and_observation",
            "error": str(exc),
        }
