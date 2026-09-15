"""현선물 연구 전용 finance-pi 연결. 증권사 주문 API와 분리한다."""

import asyncio

import httpx

import close_price_client
from core.errors import ExternalServiceError
from core.http import get_http_client
from repositories import quant_basis
from repositories.quant import QuantError, digest, encode

VERSION = "cash-futures-1"
_slot = asyncio.Semaphore(2)


def verify(result, payload):
    if (
        result.get("engine_version") != VERSION
        or result.get("input_hash") != digest(payload)
        or result.get("live_eligible") is not False
        or result.get("orders_sent") != 0
        or result.get("source_verified") is not False
    ):
        raise QuantError("현선물 입력·엔진·실행 제한 검증에 실패했습니다.")
    if result.get("result_hash") != digest({k: v for k, v in result.items() if k != "result_hash"}):
        raise QuantError("현선물 결과 해시가 일치하지 않습니다.")
    if not isinstance(result.get("scenarios"), list) or not result["scenarios"]:
        raise QuantError("시나리오 결과가 없습니다.")
    for item in result["scenarios"]:
        if item.get("orders_sent") != 0 or not isinstance(item.get("ledger"), list):
            raise QuantError("현선물 가상 원장 검증에 실패했습니다.")
        if item.get("pnl") is not None:
            if not item["ledger"] or abs(item["pnl"] - item["ledger"][-1]["pnl"]) > 0.01:
                raise QuantError("가상 원장과 손익이 일치하지 않습니다.")
        for row in item["ledger"]:
            components = row["spot_pnl"] + row["future_pnl"] + row["dividends"] - row["costs"]
            if (
                abs(row["pnl"] - components) > 0.01
                or abs(row["equity"] - result["config"]["capital"] - row["pnl"]) > 0.01
            ):
                raise QuantError("현물·선물·비용의 회계 합계가 일치하지 않습니다.")
    if len(encode(result).encode()) > 4_000_000:
        raise QuantError("현선물 보고서 크기 제한을 초과했습니다.")


async def run(user, key, payload):
    if len(encode(payload).encode()) > 500_000:
        raise QuantError("입력은 500KB 이하로 제한됩니다.")
    old = await quant_basis.existing(user, key, payload)
    if old:
        return old
    if not close_price_client.ENABLED:
        raise ExternalServiceError("finance-pi 연결이 비활성입니다.")
    async with _slot:
        client = await get_http_client("quant_research")
        headers = {"X-Admin-Token": close_price_client.API_TOKEN} if close_price_client.API_TOKEN else {}
        try:
            response = await client.post(
                close_price_client.BASE_URL + "/api/research/basis-analysis", json=payload, headers=headers
            )
            if response.status_code == 400:
                detail = response.json().get("error", "연구 입력을 확인하세요.")
                raise QuantError(str(detail)[:800])
            response.raise_for_status()
            result = response.json()
        except (httpx.HTTPError, ValueError):
            raise ExternalServiceError(
                "현선물 연구 서버 응답을 받지 못했습니다. 같은 입력으로 재시도할 수 있습니다."
            ) from None
        if not isinstance(result, dict):
            raise QuantError("현선물 응답 형식이 유효하지 않습니다.")
        try:
            verify(result, payload)
        except (KeyError, TypeError, ValueError):
            raise QuantError("현선물 결과가 불완전하거나 잘못된 숫자를 포함합니다.") from None
        return await quant_basis.save(user, key, payload, result)
