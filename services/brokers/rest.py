"""REST 어댑터 공통 인증·호출 제한·연속조회. URL은 어댑터 허용 목록에서만 선택한다."""

import asyncio
import math
import time
from dataclasses import dataclass

import httpx

from core.http import get_http_client
from repositories import brokers
from repositories.broker_secrets import BrokerError
from services.brokers.base import BrokerAdapter


@dataclass(frozen=True)
class Endpoint:
    path: str
    block: str | None = None
    success: tuple[str, ...] = ("0",)


class RestAdapter(BrokerAdapter):
    bases: dict[str, str]
    endpoints: dict[str, Endpoint]
    api_header: str
    flag_header: str
    cursor_header: str
    result_field: str
    token_errors: frozenset[str] = frozenset()
    rate_errors: frozenset[str] = frozenset()
    interval = 1.1

    def __init__(self):
        self._locks: dict[str, asyncio.Lock] = {}
        self._last_call: dict[str, float] = {}

    def lock(self, cid):
        return self._locks.setdefault(cid, asyncio.Lock())

    async def credential(self, user, cid, env):
        secret = await brokers.get_credential(user, cid)
        if env not in self.definition.environments or secret["provider"] != self.definition.id or secret["environment"] != env:
            raise BrokerError("앱키의 증권사·투자 환경과 계좌가 일치하지 않습니다.")
        return secret

    def token_request(self, secret) -> dict:
        return {"data": {"grant_type": "client_credentials", "appkey": secret["app_key"], "appsecretkey": secret["app_secret"], "scope": "oob"}}

    def token_result(self, data) -> tuple[str, float]:
        return data["access_token"], time.time() + min(float(data["expires_in"]), 86400)

    async def token(self, user, cid, env, *, expired=None):
        async with self.lock(cid):
            secret = await self.credential(user, cid, env)
            if secret["token"] and secret["token"] != expired and (secret["token_expires_at"] or 0) > time.time() + 30:
                return secret["token"]
            try:
                client = await get_http_client(self.definition.id + "_account")
                response = await client.post(self.bases[env] + "/oauth2/token", **self.token_request(secret))
                response.raise_for_status()
                data = response.json()
                access, expiry = self.token_result(data)
                if not isinstance(access, str) or not access or not math.isfinite(expiry) or expiry <= time.time():
                    raise ValueError
            except (httpx.HTTPError, KeyError, TypeError, ValueError, OverflowError):
                raise BrokerError(f"{self.definition.name} 인증에 실패했습니다. 앱키·시크릿과 API 신청 상태를 확인해 주세요.") from None
            await brokers.save_token(user, cid, access, expiry)
            return access

    def next_page(self, tr, data, headers, inputs):
        flag = str(headers.get(self.flag_header, "")).strip().upper()
        if flag not in {"", "Y", "N"}:
            raise BrokerError("증권사 연속조회 상태를 확인하지 못했습니다.")
        cursor = str(headers.get(self.cursor_header, "")).strip()
        if flag == "Y" and not cursor:
            raise BrokerError("증권사 연속조회 키가 누락되어 기존 잔고를 유지합니다.")
        return (cursor if flag == "Y" else ""), inputs

    def error_code(self, data):
        return str(data.get(self.result_field, "")) if isinstance(data, dict) else ""

    async def pages(self, user, cid, env, tr, inputs):
        if tr not in self.endpoints:
            raise BrokerError("허용되지 않은 증권사 조회입니다.")
        endpoint = self.endpoints[tr]
        access = await self.token(user, cid, env)
        client = await get_http_client(self.definition.id + "_account")
        cursor, seen, results = "", set(), []
        refreshed, retries = False, 0
        for _ in range(100):
            headers = {"authorization": "Bearer " + access, self.api_header: tr,
                       self.flag_header: "Y" if cursor else "N", self.cursor_header: cursor}
            async with self.lock(cid):
                await asyncio.sleep(max(0, self.interval - (time.monotonic() - self._last_call.get(cid, 0))))
                self._last_call[cid] = time.monotonic()
                try:
                    response = await client.post(self.bases[env] + endpoint.path, headers=headers,
                                                 json={endpoint.block: inputs} if endpoint.block else inputs)
                    data = response.json()
                except (httpx.HTTPError, ValueError):
                    raise BrokerError(f"{self.definition.name} 조회 응답을 확인할 수 없어 기존 잔고를 유지합니다.") from None
            code = self.error_code(data)
            if (response.status_code == 401 or code in self.token_errors) and not refreshed:
                access = await self.token(user, cid, env, expired=access)
                refreshed = True
                continue
            if (response.status_code == 429 or code in self.rate_errors) and retries < 2:
                retries += 1
                async with self.lock(cid):
                    self._last_call[cid] = time.monotonic() + 2 ** retries
                continue
            if not response.is_success or code not in endpoint.success:
                # 원문 오류에는 계좌번호·자격증명이 포함될 수 있어 사용자 응답과 로그에서 제외한다.
                raise BrokerError(f"{self.definition.name} 조회를 완료하지 못했습니다. 계좌·상품·API 이용 권한을 확인해 주세요.")
            results.append(data)
            cursor, inputs = self.next_page(tr, data, response.headers, dict(inputs))
            if not cursor:
                return results
            if cursor in seen:
                raise BrokerError("증권사 연속조회가 반복되어 기존 잔고를 유지합니다.")
            seen.add(cursor)
        raise BrokerError("증권사 전체 잔고 조회가 완료되지 않아 기존 잔고를 유지합니다.")
