"""계좌 조회 숫자와 응답 블록 검증. 원본 금액은 로그에 남기지 않는다."""

import logging
import math

from repositories.broker_secrets import BrokerError

logger = logging.getLogger("services.brokers.sync")


def number(row: dict, key: str) -> float:
    try:
        raw = row[key]
        if isinstance(raw, bool) or raw is None or raw == "":
            raise ValueError
        value = float(str(raw).replace(",", ""))
        if not math.isfinite(value):
            raise ValueError
        return value
    except (KeyError, TypeError, ValueError):
        logger.warning("NH 잔고 숫자 검증 실패: field=%s, present=%s", key, key in row)
        raise BrokerError("나무 잔고의 수량·금액이 누락되거나 올바르지 않아 갱신하지 않았습니다.") from None


def object_block(page: dict, name: str) -> dict:
    block = page.get(name)
    if isinstance(block, list) and len(block) == 1:
        block = block[0]
    if not isinstance(block, dict) or not block:
        raise BrokerError("나무 잔고 합계가 없어 동기화를 완료할 수 없습니다.")
    return block


def record_block(page: dict, name: str) -> list[dict]:
    rows = page.get(name, [])
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise BrokerError("나무 보유종목 목록 형식이 올바르지 않습니다.")
    return rows
