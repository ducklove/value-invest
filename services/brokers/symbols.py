"""증권사 종목 식별자와 기존 NH 응답 블록 변환."""

import re

from repositories.broker_secrets import BrokerError
from services.brokers.parsing import object_block, record_block


def summary(page: dict) -> dict:
    return object_block(page, "Output_0")


def records(page: dict) -> list[dict]:
    return record_block(page, "Output_1")


def domestic_code(raw: str) -> str:
    code = str(raw).strip()
    if code == "M04020000":
        return "KRX_GOLD"
    if re.fullmatch(r"KR7[0-9A-Z]{6}[0-9]{3}", code):
        code = code[3:9]
    if re.fullmatch(r"A[0-9A-Z]{6}", code):
        code = code[1:]
    if not re.fullmatch(r"[0-9][0-9A-Z]{5}", code):
        raise BrokerError("주식 외 상품 또는 알 수 없는 NH 종목코드가 있어 자동 동기화를 보류했습니다.")
    return code


def foreign_code(raw: str, country: str) -> str:
    code = str(raw).strip().upper()
    if not re.fullmatch(r"[A-Z0-9.-]{1,16}", code):
        raise BrokerError("해외 종목코드를 해석할 수 없습니다.")
    suffix = {"200": "", "070": ".T", "120": ".HK", "160": ".SS", "170": ".SZ"}.get(country)
    if suffix is None:
        raise BrokerError("지원되지 않는 해외시장입니다.")
    if country == "200":
        return code.replace(".", "-")
    if suffix and not code.endswith(suffix):
        code = ((code.lstrip("0") or "0").zfill(4) if country in {"070", "120"} else code) + suffix
    return code
