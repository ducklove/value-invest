"""NH 앱키의 시세·현선물 감시가 공유하는 연결·전송 예산."""

import asyncio
import json
from collections import Counter
from contextlib import asynccontextmanager

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
    async with _send_locks.setdefault(cid, asyncio.Lock()):
        await ws.send(json.dumps({"header": {"token": access, "tr_type": "1"},
                                  "body": {"tr_cd": channel, "tr_key": key}}))
        await asyncio.sleep(.12)
