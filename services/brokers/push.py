"""키움·LS 서버 통보. ACK와 실제 통보를 구분하고 소유 계좌만 REST로 재확인한다."""

import asyncio
import json
import re

import aiosqlite
import websockets

from repositories import brokers
from repositories.broker_secrets import BrokerError

_states: dict[tuple[str, str, str], dict] = {}


def status(provider, user):
    rows = [value for (source, owner, _), value in _states.items() if (source, owner) == (provider, user)]
    return {"type": "broker_account_status", "provider": provider, "connections": len(rows),
            "state": "subscribed" if rows and all(row["state"] == "subscribed" for row in rows) else "polling",
            "poll_seconds": 60}


def forget(provider, user, cid):
    _states.pop((provider, user, cid), None)


class KiwoomProtocol:
    def __init__(self, env):
        host = "api.kiwoom.com" if env == "live" else "mockapi.kiwoom.com"
        self.url = f"wss://{host}:10000/api/dostk/websocket"
        self.logged_in = False
        self.approved = False

    async def start(self, ws, token):
        await ws.send(json.dumps({"trnm": "LOGIN", "token": token}))

    async def receive(self, ws, message):
        name = message.get("trnm")
        if name == "PING":
            await ws.send(json.dumps(message))
        elif name == "LOGIN":
            if str(message.get("return_code")) != "0":
                raise BrokerError("키움 통보 인증을 다시 확인합니다.")
            self.logged_in = True
            await ws.send(json.dumps({"trnm": "REG", "grp_no": "1", "refresh": "1", "data": [{"item": [""], "type": ["00", "04"]}]}))
        elif name == "REG" and self.logged_in:
            self.approved = str(message.get("return_code")) == "0"
        elif name == "REAL" and self.approved:
            rows = message.get("data")
            if isinstance(rows, list):
                return {str(row["values"].get("9201", "")) for row in rows if isinstance(row, dict)
                        and row.get("type") in {"00", "04"} and isinstance(row.get("values"), dict)}
        return set()


class LsProtocol:
    url = "wss://openapi.ls-sec.co.kr:9443/websocket"
    channels = {"SC1", "AS1"}

    def __init__(self, env):
        self.accepted = set()

    @property
    def approved(self):
        return self.accepted == self.channels

    async def start(self, ws, token):
        for channel in sorted(self.channels):
            await ws.send(json.dumps({"header": {"token": token, "tr_type": "1"}, "body": {"tr_cd": channel, "tr_key": ""}}))

    async def receive(self, ws, message):
        head, body = message.get("header", {}), message.get("body")
        if not isinstance(head, dict) or head.get("tr_cd") not in self.channels:
            return set()
        channel = head["tr_cd"]
        if "rsp_cd" in head:
            if head["rsp_cd"] == "00000":
                self.accepted.add(channel)
            else:
                self.accepted.discard(channel)
            return set()
        if channel in self.accepted and isinstance(body, dict):
            key = "ordacntno" if channel == "SC1" else "sAcntNo"
            return {str(body.get(key, ""))}
        return set()


async def _stream(adapter, protocol_type, user, cid, env, changed):
    provider, delay = adapter.definition.id, 2
    key = (provider, user, cid)
    while True:
        try:
            token = await adapter.token(user, cid, env)
            protocol = protocol_type(env)
            _states[key] = {"state": "connecting"}
            async with websockets.connect(protocol.url, open_timeout=15, ping_interval=20, ping_timeout=20, max_size=256000) as ws:
                await protocol.start(ws, token)
                changed(None)
                async for raw in ws:
                    if not isinstance(raw, str):
                        continue
                    message = json.loads(raw)
                    if not isinstance(message, dict):
                        continue
                    numbers = await protocol.receive(ws, message)
                    _states[key] = {"state": "subscribed" if protocol.approved else "polling"}
                    numbers = {number for number in numbers if re.fullmatch(r"[0-9]{10,11}", number)}
                    if not numbers:
                        continue
                    for row in await brokers.list_links():
                        if (row.get("provider"), row["google_sub"], row["credential_id"], row["environment"]) != (provider, user, cid, env):
                            continue
                        link = await brokers.get_link(user, row["account_id"])
                        if link["account_no"] in numbers:
                            changed(row["account_id"])
                            delay = 2
        except (BrokerError, OSError, ValueError, TypeError, websockets.exceptions.WebSocketException, TimeoutError, aiosqlite.Error):
            _states[key] = {"state": "polling"}
        # 정상 종료 프레임도 단절 상태로 표시하고 주기 조회를 계속한다.
        _states[key] = {"state": "polling"}
        await asyncio.sleep(delay)
        delay = min(60, delay * 2)


async def kiwoom_stream(adapter, user, cid, env, changed):
    await _stream(adapter, KiwoomProtocol, user, cid, env, changed)


async def ls_stream(adapter, user, cid, env, changed):
    await _stream(adapter, LsProtocol, user, cid, env, changed)
