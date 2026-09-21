"""사용자별 한국투자증권 조회 전용 REST. 공용 시세 프록시의 키와 분리한다."""

import asyncio
import hashlib
import json
import logging
import math
import re
import time

import httpx

from core.http import get_http_client
from repositories import brokers
from repositories.broker_secrets import BrokerError

BASE = {"live": "https://openapi.koreainvestment.com:9443", "mock": "https://openapivts.koreainvestment.com:29443"}
DOMESTIC = "/uapi/domestic-stock/v1/trading/inquire-balance"
OVERSEAS = "/uapi/overseas-stock/v1/trading/inquire-balance"
PRESENT = "/uapi/overseas-stock/v1/trading/inquire-present-balance"
STOCK_INFO = "/uapi/domestic-stock/v1/quotations/search-stock-info"
READ_PATHS = {DOMESTIC: ("TTTC8434R", "VTTC8434R", "100"),
              OVERSEAS: ("TTTS3012R", "VTTS3012R", "200"),
              PRESENT: ("CTRP6504R", "VTRP6504R", None),
              STOCK_INFO: ("CTPF1002R", None, None)}
MIN_INTERVAL = 0.6
_locks: dict[str, asyncio.Lock] = {}
_token_locks: dict[str, asyncio.Lock] = {}
_last_call: dict[str, float] = {}


class _HideAccountQuery(logging.Filter):
    def filter(self, record):
        message = record.getMessage()
        if "koreainvestment.com" in message and "/uapi/" in message:
            record.msg = re.sub(r"(/uapi/[^?\s]+)\?[^\s\"']+", r"\1?[보호됨]", message)
            record.args = ()
        return True


logging.getLogger("httpx").addFilter(_HideAccountQuery())


async def credential(user: str, cid: str, environment: str) -> dict:
    value = await brokers.get_credential(user, cid)
    if value["provider"] != "kis" or environment not in BASE or value["environment"] != environment:
        raise BrokerError("한국투자증권 앱키의 투자 환경이 계좌와 다릅니다.")
    return value


async def token(user: str, cid: str, environment: str, *, expired: str | None = None) -> str:
    async with _token_locks.setdefault(cid, asyncio.Lock()):
        secret = await credential(user, cid, environment)
        if secret["token"] and secret["token"] != expired and (secret["token_expires_at"] or 0) > time.time() + 60:
            return secret["token"]
        client = await get_http_client("kis_account")
        try:
            response = await client.post(BASE[environment] + "/oauth2/tokenP", json={
                "grant_type": "client_credentials", "appkey": secret["app_key"], "appsecret": secret["app_secret"]})
            response.raise_for_status()
            data = response.json()
            access, duration = data["access_token"], float(data["expires_in"])
            if not isinstance(access, str) or not access or not math.isfinite(duration) or duration <= 60:
                raise ValueError
        except (httpx.HTTPError, ValueError, TypeError, KeyError):
            raise BrokerError("한국투자증권 인증에 실패했습니다. 앱키·시크릿과 실전/모의 환경을 확인해 주세요.") from None
        await brokers.save_token(user, cid, access, time.time() + min(duration, 86400))
        return access


async def pages(user: str, cid: str, path: str, params: dict, environment: str = "live") -> list[dict]:
    if path not in READ_PATHS or environment not in BASE:
        raise BrokerError("허용되지 않은 한국투자증권 조회입니다.")
    live_tr, mock_tr, cursor_size = READ_PATHS[path]
    tr_id = mock_tr if environment == "mock" else live_tr
    if not tr_id:
        raise BrokerError("이 한국투자증권 조회는 모의투자에서 지원되지 않습니다.")
    secret = await credential(user, cid, environment)
    access = await token(user, cid, environment)
    client = await get_http_client("kis_account")
    query, output, seen, continuation = dict(params), [], set(), ""
    if cursor_size:
        query.update({"CTX_AREA_FK" + cursor_size: "", "CTX_AREA_NK" + cursor_size: ""})
    refreshed, retries = False, 0
    for _ in range(100):
        async with _locks.setdefault(cid, asyncio.Lock()):
            await asyncio.sleep(max(0, MIN_INTERVAL - (time.monotonic() - _last_call.get(cid, 0))))
            _last_call[cid] = time.monotonic()
            try:
                response = await client.get(BASE[environment] + path, params=query, headers={
                    "authorization": "Bearer " + access, "appkey": secret["app_key"], "appsecret": secret["app_secret"],
                    "tr_id": tr_id, "tr_cont": continuation, "custtype": "P"})
                data = response.json()
            except (httpx.HTTPError, ValueError):
                raise BrokerError("한국투자증권 조회 서버의 응답을 확인하지 못했습니다. 기존 잔고를 유지합니다.") from None
        if not isinstance(data, dict):
            raise BrokerError("한국투자증권 응답 형식이 올바르지 않습니다.")
        code = data.get("msg_cd")
        if (response.status_code == 401 or code in {"EGW00121", "EGW00123"}) and not refreshed:
            access = await token(user, cid, environment, expired=access)
            refreshed = True
            continue
        if response.status_code == 429 or code == "EGW00201":
            if retries < 2:
                retries += 1
                await asyncio.sleep(2 ** retries)
                continue
            raise BrokerError("한국투자증권 조회 한도를 초과했습니다. 잠시 후 다시 확인합니다.")
        if response.is_error or data.get("rt_cd") != "0":
            # 증권사의 원문 메시지는 계좌번호를 포함할 수 있어 반환하지 않는다.
            safe_code = code if isinstance(code, str) and re.fullmatch(r"[A-Z0-9]{1,16}", code) else "조회 실패"
            raise BrokerError(f"한국투자증권 조회를 완료하지 못했습니다 ({safe_code}). 계좌번호·상품코드와 API 이용 권한을 확인해 주세요.")
        digest = hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
        if digest in seen:
            raise BrokerError("한국투자증권 연속조회가 반복되어 기존 잔고를 유지합니다.")
        seen.add(digest)
        output.append(data)
        flag = response.headers.get("tr_cont", "").strip()
        if flag not in {"M", "F"}:
            return output
        if cursor_size:
            fk, nk = (data.get("ctx_area_" + part + cursor_size) for part in ("fk", "nk"))
            if not isinstance(fk, str) or not isinstance(nk, str) or not nk.strip():
                raise BrokerError("한국투자증권 연속조회 키가 없어 기존 잔고를 유지합니다.")
            query.update({"CTX_AREA_FK" + cursor_size: fk, "CTX_AREA_NK" + cursor_size: nk})
        continuation = "N"
    raise BrokerError("한국투자증권 전체 잔고 조회가 끝나지 않아 기존 잔고를 유지합니다.")


async def approval(user: str, cid: str, environment: str) -> str:
    secret = await credential(user, cid, environment)
    client = await get_http_client("kis_account")
    try:
        response = await client.post(BASE[environment] + "/oauth2/Approval", json={
            "grant_type": "client_credentials", "appkey": secret["app_key"], "secretkey": secret["app_secret"]})
        response.raise_for_status()
        value = response.json()["approval_key"]
        if not isinstance(value, str) or not value:
            raise ValueError
        return value
    except (httpx.HTTPError, KeyError, TypeError, ValueError):
        raise BrokerError("한국투자증권 체결 통보 인증에 실패했습니다. 60초 잔고 조회로 보완합니다.") from None


async def fetch_snapshot(user: str, link: dict) -> tuple[list[dict], dict]:
    from services.brokers.kis_balance import fetch_snapshot as fetch
    return await fetch(user, link)
