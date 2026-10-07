"""NH 앱키의 시세·현선물 감시가 공유하는 연결·전송 예산."""

import asyncio
import json
from collections import Counter
from contextlib import asynccontextmanager

from websockets.exceptions import WebSocketException

_slots: dict[str, asyncio.Semaphore] = {}
_users: dict[str, int] = {}
_send_locks: dict[str, asyncio.Lock] = {}
_requests: dict[str, Counter[str]] = {}


def requested(cid: str, role: str) -> bool:
    return bool(_requests.get(cid, {}).get(role))


@asynccontextmanager
async def slot(cid: str, role: str = "quotes"):
    semaphore = _slots.setdefault(cid, asyncio.Semaphore(2))
    _users[cid] = _users.get(cid, 0) + 1
    _requests.setdefault(cid, Counter())[role] += 1
    try:
        async with semaphore:
            yield
    finally:
        _users[cid] -= 1
        _requests[cid][role] -= 1
        if not _users[cid]:
            _users.pop(cid)
            _slots.pop(cid)
            _requests.pop(cid)


async def subscribe(ws, cid: str, access: str, channel: str, key: str) -> None:
    await _send(ws, cid, access, channel, key, "1")


async def _send(ws, cid: str, access: str, channel: str, key: str, action: str) -> None:
    async with _send_locks.setdefault(cid, asyncio.Lock()):
        await ws.send(json.dumps({"header": {"token": access, "tr_type": action},
                                  "body": {"tr_cd": channel, "tr_key": key}}))
        await asyncio.sleep(.12)


@asynccontextmanager
async def registrations(ws, cid: str, access: str, pairs):
    """소켓을 닫기 전에 이 소켓의 구독을 반납한다. 취소·종목 변경에도 적용한다."""
    try:
        yield
    finally:
        try:
            async with asyncio.timeout(10):
                for channel, key in dict.fromkeys(pairs):
                    await _send(ws, cid, access, channel, key, "2")
        except (OSError, WebSocketException, TimeoutError):
            # 이미 끊긴 소켓에서는 해제가 불가능하므로 원래 종료 사유를 보존한다.
            pass
