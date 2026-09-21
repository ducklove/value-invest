"""브라우저와 무관하게 한국투자증권 계좌를 감시하고 확인된 REST 잔고로 갱신한다."""

import asyncio
import base64
import binascii
import json
import logging
import re
import time

import aiosqlite
import websockets
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from repositories import brokers
from repositories.broker_secrets import BrokerError
from services.brokers import kis
from services.brokers.sync import sync_account

logger = logging.getLogger(__name__)
_states: dict[tuple[str, str], dict] = {}
WS = {"live": "ws://ops.koreainvestment.com:21000", "mock": "ws://ops.koreainvestment.com:31000"}


def status(user: str) -> dict:
    rows = [value for (owner, _), value in _states.items() if owner == user]
    return {"connections": len(rows), "subscribed": sum(row.get("subscribed", 0) for row in rows),
            "state": "subscribed" if rows and all(row["state"] == "subscribed" for row in rows) else "polling",
            "poll_seconds": 60}


def decode_accounts(frame: str, secrets: dict) -> set[str]:
    """체결 원문은 저장하지 않고 계좌번호만 확인한다. ACK는 이 함수로 전달하지 않는다."""
    pieces = frame.split("|", 3)
    if len(pieces) != 4 or pieces[0] != "1" or pieces[1] not in secrets:
        return set()
    channel, count, ciphertext = pieces[1:]
    try:
        key, iv = secrets[channel]
        decryptor = Cipher(algorithms.AES(key.encode()), modes.CBC(iv.encode())).decryptor()
        plain = decryptor.update(base64.b64decode(ciphertext, validate=True)) + decryptor.finalize()
        unpad = padding.PKCS7(128).unpadder()
        fields = (unpad.update(plain) + unpad.finalize()).decode().split("^")
        size = 26 if channel.startswith("H0ST") else 25
        if not count.isdigit() or not 1 <= int(count) <= 100 or len(fields) != size * int(count):
            return set()
        return {fields[offset + 1] for offset in range(0, len(fields), size) if re.fullmatch(r"[0-9]{10}", fields[offset + 1])}
    except (ValueError, TypeError, UnicodeError, binascii.Error):
        return set()


async def stream(user: str, cid: str, env: str, changed):
    delay = 2
    while True:
        try:
            credential = await kis.credential(user, cid, env)
            hts = credential.get("hts_id")
            if not hts:
                _states[(user, cid)] = {"state": "polling", "subscribed": 0}
                return
            approval = await kis.approval(user, cid, env)
            channels = ("H0STCNI9",) if env == "mock" else ("H0STCNI0", "H0GSCNI0")
            secrets, approved = {}, set()
            _states[(user, cid)] = {"state": "connecting", "subscribed": 0}
            async with websockets.connect(WS[env], open_timeout=15, ping_interval=20, ping_timeout=20, max_size=256000) as ws:
                for channel in channels:
                    await ws.send(json.dumps({"header": {"approval_key": approval, "custtype": "P", "tr_type": "1", "content-type": "utf-8"},
                                              "body": {"input": {"tr_id": channel, "tr_key": hts}}}))
                changed(None)  # 재접속 동안 놓친 변경도 조회한다.
                async for raw in ws:
                    if not isinstance(raw, str):
                        continue
                    if raw.startswith("{"):
                        message = json.loads(raw)
                        if not isinstance(message, dict):
                            continue
                        head, body = message.get("header", {}), message.get("body", {})
                        if not isinstance(head, dict) or not isinstance(body, dict):
                            continue
                        channel = head.get("tr_id")
                        if channel == "PINGPONG":
                            await ws.pong(raw.encode())
                        elif channel in channels:
                            output = body.get("output", {})
                            if body.get("rt_cd") == "0" and isinstance(output, dict) and isinstance(output.get("key"), str) and isinstance(output.get("iv"), str):
                                secrets[channel] = (output["key"], output["iv"])
                                approved.add(channel)
                            else:
                                approved.discard(channel)
                                secrets.pop(channel, None)
                            _states[(user, cid)] = {"state": "subscribed" if len(approved) == len(channels) else "polling", "subscribed": len(approved)}
                        continue
                    numbers = decode_accounts(raw, secrets)
                    if not numbers:
                        continue
                    for row in await brokers.list_links():
                        if row.get("provider") == "kis" and (row["google_sub"], row["credential_id"], row["environment"]) == (user, cid, env):
                            link = await brokers.get_link(user, row["account_id"])
                            if link["account_no"] in numbers:
                                changed(row["account_id"])
                                delay = 2
        except (BrokerError, OSError, ValueError, TypeError, websockets.exceptions.WebSocketException, TimeoutError, aiosqlite.Error):
            # 키·계좌·통보 원문 및 연결 예외 문자열은 기록하지 않는다.
            _states[(user, cid)] = {"state": "polling", "subscribed": 0}
        await asyncio.sleep(delay)
        delay = min(60, delay * 2)


async def run(stop: asyncio.Event):
    sockets, refreshes, due, followups = {}, {}, {}, {}
    wake = asyncio.Event()

    def changed(aid, account_ids):
        for target in ([aid] if aid else account_ids):
            due[target] = 0
            followups[target] = time.monotonic() + 12
        wake.set()

    async def refresh(user, aid):
        try:
            await sync_account(user, aid)
        except (BrokerError, aiosqlite.Error, ValueError):
            logger.warning("한국투자증권 잔고 갱신 보류. 기존 잔고를 유지합니다.")

    try:
        while not stop.is_set():
            wake.clear()
            try:
                links = [row for row in await brokers.list_links() if row.get("provider") == "kis"]
                active = {row["account_id"] for row in links}
                for aid in set(refreshes) - active:
                    task = refreshes.pop(aid)
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                    due.pop(aid, None)
                    followups.pop(aid, None)
                desired = {}
                for row in links:
                    user, cid, aid, env = (row[k] for k in ("google_sub", "credential_id", "account_id", "environment"))
                    desired[cid] = (user, env, tuple(sorted(r["account_id"] for r in links if r["credential_id"] == cid)))
                    now = time.monotonic()
                    if now >= followups.get(aid, float("inf")):
                        due[aid] = 0
                        followups.pop(aid)
                    if now >= due.get(aid, 0) and (aid not in refreshes or refreshes[aid].done()):
                        # 통보 폭주도 최소 2초 간격으로 합친다.
                        due[aid] = now + 60
                        async def delayed_refresh(owner=user, account_id=aid):
                            await asyncio.sleep(2)
                            await refresh(owner, account_id)
                        refreshes[aid] = asyncio.create_task(delayed_refresh())
                for cid, (signature, task) in list(sockets.items()):
                    if desired.get(cid) != signature:
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                        sockets.pop(cid)
                        _states.pop((signature[0], cid), None)
                for cid, signature in desired.items():
                    if cid not in sockets:
                        user, env, aids = signature
                        sockets[cid] = (signature, asyncio.create_task(stream(user, cid, env, lambda aid, targets=aids: changed(aid, targets))))
            except (BrokerError, aiosqlite.Error):
                logger.warning("한국투자증권 연결 목록을 다시 확인합니다.")
            try:
                await asyncio.wait_for(wake.wait(), timeout=1)
            except TimeoutError:
                pass
    finally:
        tasks = [task for _, task in sockets.values()] + list(refreshes.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        _states.clear()
