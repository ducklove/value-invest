"""NH 공식 REST 조회 전용 어댑터. 경로 허용 목록에 주문 API는 없다."""

import asyncio
import logging
import math
import re
import time

import httpx

from core.http import get_http_client
from repositories import brokers
from repositories.broker_secrets import BrokerError

LIVE = "https://api.nhplug.com:8443"
MOCK = "https://moapi.nhplug.com:8443"
READ_PATHS = frozenset({"/n2/acctinfo", "/krstock/inquiry/v1/balance", "/gbstock/inquiry/v1/balance",
                        "/gbstock/inquiry/v1/margin", "/krstock/quote/v1/currentPrice", "/krfuture/quote/v1/day"})
_locks: dict[str, asyncio.Lock] = {}
_last_call: dict[str, float] = {}
# 실계좌 잔고의 시장별·연속 조회는 공개 SDK의 4회/초에서도 429가 발생한다.
MIN_CALL_INTERVAL = 1.1


class _HideTokenQuery(logging.Filter):
    def filter(self, record):
        message = record.getMessage()
        if "/oauth2/token" in message and "nhplug.com" in message:
            record.msg = re.sub(r"(/oauth2/token)\?[^\s\"']+", r"\1?[보호됨]", message)
            record.args = ()
        return True


logging.getLogger("httpx").addFilter(_HideTokenQuery())


def lock(cid: str) -> asyncio.Lock:
    return _locks.setdefault(cid, asyncio.Lock())


async def token(user: str, cid: str, *, force=False) -> str:
    async with lock(cid):
        secret = await brokers.get_credential(user, cid)
        if not force and secret["token"] and (secret["token_expires_at"] or 0) > time.time() + 60:
            return secret["token"]
        client = await get_http_client("namuh")
        try:
            response = await client.post(LIVE + "/oauth2/token", params={"appkey": secret["app_key"],
                "appsecretkey": secret["app_secret"], "grant_type": "client_credentials", "scope": "oob"},
                headers={"Content-Type": "application/x-www-form-urlencoded"})
            response.raise_for_status()
            data = response.json()
        except (httpx.HTTPError, ValueError):
            raise BrokerError("나무 인증에 실패했습니다. 앱키·시크릿과 API 신청 상태를 확인해 주세요.") from None
        if not isinstance(data, dict) or not isinstance(data.get("access_token"), str):
            raise BrokerError("나무 접근토큰을 발급받지 못했습니다.")
        try:
            duration = min(float(data["expires_in"]), 86400)
        except (KeyError, TypeError, ValueError):
            raise BrokerError("나무 토큰 만료시각을 확인할 수 없습니다.") from None
        await brokers.save_token(user, cid, data["access_token"], time.time() + duration)
        return data["access_token"]


def continuation(data: dict, headers) -> str | None:
    flag = str(headers.get("cts_flag", "")).strip().upper()
    if flag == "N":
        return None
    key = str(headers.get("cts") or "").strip()
    from_header = bool(key)
    if not key:
        for name, block in data.items():
            if not name.startswith("Output_"):
                continue
            for row in block if isinstance(block, list) else [block]:
                if isinstance(row, dict):
                    key = next((str(v).strip() for k, v in row.items() if re.fullmatch(r"cts(?:z\d+)?", k, re.I) and v), None)
                    if key:
                        break
            if key:
                break
    if flag == "Y" and not key:
        raise BrokerError("나무 연속조회 키가 누락되어 잔고를 갱신하지 않았습니다.")
    # 일부 API는 마지막 응답에도 헤더 키를 돌려준다. 공식 SDK와 같이
    # 플래그가 없으면 연속조회 업무코드일 때만 헤더 키를 따라간다.
    if from_header and not flag and str(data.get("rsp_cd")) not in {"00165", "00218"}:
        return None
    return str(key).strip() if key else None


async def pages(user: str, cid: str, path: str, body: dict, environment="live") -> list[dict]:
    if path not in READ_PATHS or environment not in {"live", "mock"}:
        raise BrokerError("허용되지 않은 나무 조회입니다.")
    access = await token(user, cid)
    secret = await brokers.get_credential(user, cid)
    client = await get_http_client("namuh")
    results, seen, cts, cts_flag = [], set(), None, None
    refreshed = False
    rate_retries = 0
    for _ in range(100):
        headers = {"Authorization": "Bearer " + access, "x-client-id": secret["app_key"], "x-client-secret": secret["app_secret"]}
        if cts:
            headers["cts"] = cts
            if cts_flag:
                headers["cts_flag"] = cts_flag
        async with lock(cid):
            await asyncio.sleep(max(0, MIN_CALL_INTERVAL - (time.monotonic() - _last_call.get(cid, 0))))
            _last_call[cid] = time.monotonic()
            try:
                response = await client.post((MOCK if environment == "mock" and path != "/n2/acctinfo" else LIVE) + path,
                                             json={"Input_0": body}, headers=headers)
            except httpx.HTTPError:
                raise BrokerError("나무 조회 서버에 연결하지 못했습니다. 기존 잔고를 유지합니다.") from None
        if response.status_code == 401 and not refreshed:
            access = await token(user, cid, force=True)
            refreshed = True
            continue
        if response.status_code == 429:
            if rate_retries < 2:
                rate_retries += 1
                try:
                    retry_after = float(response.headers.get("Retry-After", "0"))
                except ValueError:
                    retry_after = 0
                if not math.isfinite(retry_after):
                    retry_after = 0
                delay = min(10, max(2 ** rate_retries, retry_after))
                # 같은 키를 사용하는 다른 조회도 서버의 대기 시간을 지킨다.
                async with lock(cid):
                    _last_call[cid] = max(_last_call.get(cid, 0), time.monotonic() + delay - MIN_CALL_INTERVAL)
                continue
            raise BrokerError("나무 조회 한도를 초과했습니다. 잠시 후 다시 동기화해 주세요.")
        try:
            response.raise_for_status()
            data = response.json()
        except (httpx.HTTPError, ValueError):
            raise BrokerError("나무 조회 응답을 확인할 수 없습니다. 기존 잔고를 유지합니다.") from None
        if not isinstance(data, dict):
            raise BrokerError("나무 조회 형식이 올바르지 않습니다.")
        message = str(data.get("rsp_msg", ""))
        if any(word in message for word in ("오류", "실패", "불가", "잘못", "초과", "유효하지", "권한", "ERROR")):
            raise BrokerError("나무에서 조회를 처리하지 못했습니다. 계좌와 이용 권한을 확인해 주세요.")
        results.append(data)
        cts = continuation(data, response.headers)
        cts_flag = str(response.headers.get("cts_flag", "")).strip().upper()
        if not cts:
            return results
        if cts in seen:
            raise BrokerError("나무 연속조회가 반복되어 잔고를 갱신하지 않았습니다.")
        seen.add(cts)
    raise BrokerError("나무 잔고 조회가 완료되지 않아 기존 잔고를 유지합니다.")


async def accounts(user: str, cid: str) -> list[dict]:
    output = []
    for page in await pages(user, cid, "/n2/acctinfo", {}):
        rows = page.get("Output_0")
        if not isinstance(rows, list):
            raise BrokerError("나무 계좌목록을 확인할 수 없습니다.")
        for row in rows:
            number = str(row.get("acct_no", ""))
            kind = str(row.get("acct_type", ""))
            if re.fullmatch(r"\d{11}", number) and kind in {"01", "02", "03"}:
                output.append({"account_no": number, "environment": "mock" if kind == "03" else "live"})
    return output
