"""NH 국내선물 계약의 거래단위와 최종거래일. 가격·월물로 만기를 추정하지 않는다."""

import re
from datetime import datetime

from cache_layer import MemoryTTLCache
from repositories.broker_secrets import BrokerError
from services.brokers import namuh
from services.brokers.parsing import number, object_block

_expiries = MemoryTTLCache("namuh.futures_expiry", 6 * 3600)


async def contract_terms(user: str, link: dict, code: str, name: str) -> dict:
    # NH 종목명 끝의 괄호는 거래승수다. 운영 주식선물 응답 예: F 202611 (  10).
    match = re.search(r"\(\s*([0-9]+(?:\.[0-9]+)?)\s*\)\s*$", name)
    if not match:
        raise BrokerError("선물 거래단위를 확인할 수 없어 기존 잔고를 유지합니다.")
    multiplier = number({"multiplier": match[1]}, "multiplier")
    if multiplier <= 0:
        raise BrokerError("선물 거래단위를 확인할 수 없어 기존 잔고를 유지합니다.")
    expiry = _expiries.get(code)
    if not expiry:
        pages = await namuh.pages(user, link["credential_id"], "/krfuture/quote/v1/day", {"iem_cd": code}, link["environment"])
        quote = object_block(pages[-1], "Output_0")
        # 시세 응답은 계좌 잔고의 9자리 코드에서 선두 K를 제외한 8자리 코드다.
        if str(quote.get("iem_cd", "")).strip() not in {code, code.removeprefix("K")}:
            raise BrokerError("선물 만기 조회의 종목코드가 일치하지 않습니다.")
        raw = str(quote.get("last_tr_date", "")).strip()
        try:
            if not re.fullmatch(r"[0-9]{8}", raw):
                raise ValueError
            expiry = datetime.strptime(raw, "%Y%m%d").date().isoformat()
        except ValueError:
            raise BrokerError("선물 만기일을 확인할 수 없어 기존 잔고를 유지합니다.") from None
        _expiries.set(code, expiry)
    return {"multiplier": multiplier, "expiry_date": expiry}
