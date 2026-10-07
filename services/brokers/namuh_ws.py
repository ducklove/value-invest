"""NH 앱키의 시세·현선물 감시가 공유하는 연결·전송 예산."""

import asyncio
import json
import logging
import time
from collections import Counter
from contextlib import AsyncExitStack, asynccontextmanager

import websockets
from websockets.exceptions import WebSocketException

from repositories import brokers
from repositories.broker_secrets import BrokerError
from services.brokers import namuh

logger = logging.getLogger(__name__)
_slots: dict[str, asyncio.Semaphore] = {}
_users: dict[str, int] = {}
_send_locks: dict[str, asyncio.Lock] = {}
_requests: dict[str, Counter[str]] = {}
_connect_locks: dict[str, asyncio.Lock] = {}
_connections: dict[str, set] = {}
_generations: dict[str, int] = {}
_last_recovery: dict[str, float] = {}


def generation(cid: str) -> int:
    return _generations.get(cid, 0)


@asynccontextmanager
async def connect(cid: str, role: str, endpoint: str, **options):
    """한 키의 연결 시작과 전체 세션 복구를 직렬화한다."""
    async with slot(cid, role), AsyncExitStack() as stack:
        async with _connect_locks.setdefault(cid, asyncio.Lock()):
            ws = await stack.enter_async_context(websockets.connect(endpoint, **options))
            _connections.setdefault(cid, set()).add(ws)
        try:
            yield ws
        finally:
            try:
                await stack.aclose()
            finally:
                _connections[cid].discard(ws)
                if not _connections[cid]:
                    _connections.pop(cid)


async def recover(user: str, cid: str, failed_generation: int) -> bool:
    """WSS10015 이후 전용 키의 잔류 세션을 한 번 정리한다.

    같은 실패에서 여러 작업이 중복 해제하지 않는다. 복구 중 새 소켓은 대기하고,
    관리 중인 다른 소켓도 먼저 닫아서 해제 API와 정상 연결이 충돌하지 않게 한다.
    """
    async with _connect_locks.setdefault(cid, asyncio.Lock()):
        if failed_generation != generation(cid) or time.monotonic() - _last_recovery.get(cid, -60) < 60:
            return False
        try:
            secret = await brokers.get_credential(user, cid)
            if secret.get("ws_session_management") is not True:
                return False
            _last_recovery[cid] = time.monotonic()
            _generations[cid] = generation(cid) + 1
            for ws in tuple(_connections.get(cid, ())):
                try:
                    await asyncio.wait_for(ws.close(), timeout=5)
                except (OSError, WebSocketException, TimeoutError):
                    pass
            await namuh.close_ws_sessions(user, cid)
        except BrokerError:
            logger.warning("NH WebSocket session recovery failed")
            return False
        logger.info("NH WebSocket stale sessions released")
        return True


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


@asynccontextmanager
async def subscribing(ws, cid: str, access: str, pairs):
    """등록 중 연결이 닫혀도 수신자가 거절 ACK를 먼저 읽게 한다."""
    async def send():
        try:
            for channel, key in pairs:
                await subscribe(ws, cid, access, channel, key)
        except websockets.exceptions.ConnectionClosed:
            pass

    async with registrations(ws, cid, access, pairs):
        sender = asyncio.create_task(send())
        try:
            yield
        finally:
            sender.cancel()
            await asyncio.gather(sender, return_exceptions=True)


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
