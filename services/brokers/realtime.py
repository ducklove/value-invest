"""사용자 키별 NH 시세 연결을 공유한다. 브라우저 수와 증권사 연결 수는 독립적이다."""

import asyncio
import json
import logging
import math
import os
import random
import re
import ssl
import time
from datetime import datetime, timedelta, timezone

import aiosqlite
import truststore
import websockets

from cache_layer import MemoryTTLCache
from repositories import account_holdings, brokers
from repositories.broker_secrets import BrokerError
from services.brokers import namuh
from services.brokers.sync import sync_account

_quotes = MemoryTTLCache("namuh.realtime", 90)
_status: dict[str, dict] = {}
_KST = timezone(timedelta(hours=9))
logger = logging.getLogger(__name__)


def normalize(message: dict, now: datetime | None = None) -> dict | None:
    head = message.get("header", {})
    if not isinstance(head, dict):
        return None
    if head.get("tr_cd") != "mc" or "rsp_cd" in head or "tr_type" in head:
        return None
    body = message.get("body")
    if not isinstance(body, dict):
        return None
    code = str(body.get("code", ""))
    if not re.fullmatch(r"[0-9][0-9A-Z]{5}", code):
        return None
    now = now or datetime.now(_KST)
    raw_time = str(body.get("time", "")).replace(":", "")
    try:
        at = datetime.strptime(raw_time, "%H%M%S").replace(year=now.year, month=now.month, day=now.day, tzinfo=_KST)
        price = float(body["price"])
        change = abs(float(body["change"])) * (-1 if str(body.get("sign")) in {"4", "5"} else 1)
        previous = price - change
        if not 0 < price < 1e12 or not math.isfinite(previous) or previous <= 0 or not 0 <= (now - at).total_seconds() < 90:
            return None
        return {"type": "quote", "code": code, "price": price, "previous_close": previous, "change": change,
                "change_pct": change / previous * 100, "source": "namuh_ws", "market": "UN", "currency": "KRW",
                "date": at.date().isoformat(), "as_of": at.isoformat(), "ts": now.timestamp(),
                "received_at": now.isoformat(), "trade_value": float(body.get("value_won") or 0)}
    except (KeyError, ValueError, TypeError, OverflowError):
        return None


def quote(user: str, code: str) -> dict | None:
    result = _quotes.get((user, code))
    if result and result["date"] == datetime.now(_KST).date().isoformat():
        return dict(result)
    return None


def status(user: str) -> dict:
    result = dict(_status.get(user, {"state": "waiting", "message": "NH 시세 연결 대기"}))
    if result.get("state") == "live" and (datetime.now(_KST) - datetime.fromisoformat(result["last_tick_at"])).total_seconds() >= 90:
        result["state"] = "subscribed"
    return result


async def stream(user: str, cid: str, codes: list[str], environment: str):
    delay = 2
    endpoint = "wss://moapi.nhplug.com:17070/websocket" if environment == "mock" else "wss://api.nhplug.com:7070/websocket"
    context = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    while True:
        try:
            access = await namuh.token(user, cid)
            _status[user] = {"state": "connecting", "subscribed": 0, "requested": len(codes)}
            async with websockets.connect(endpoint, ssl=context, ping_interval=None, open_timeout=15, close_timeout=3, max_size=2**20) as ws:
                for code in codes:
                    await ws.send(json.dumps({"header": {"token": access, "tr_type": "1"}, "body": {"tr_cd": "mc", "tr_key": code}}))
                    await asyncio.sleep(.12)
                approved = set()
                async for raw in ws:
                    try:
                        message = json.loads(raw)
                    except (json.JSONDecodeError, UnicodeError):
                        continue
                    if not isinstance(message, dict):
                        continue
                    head = message.get("header") or {}
                    if not isinstance(head, dict):
                        continue
                    if "rsp_cd" in head:
                        if str(head["rsp_cd"]) != "00000":
                            raise BrokerError("NH 시세 구독을 승인받지 못했습니다. 세션 한도와 권한을 확인해 주세요.")
                        keys = (message.get("body") or {}).get("tr_key", [])
                        approved.update(keys if isinstance(keys, list) else [keys])
                        _status[user] = {"state": "subscribed", "subscribed": len(approved), "requested": len(codes)}
                        continue
                    tick = normalize(message)
                    if tick and tick["code"] in codes:
                        _quotes.set((user, tick["code"]), tick)
                        _status[user] = {"state": "live", "subscribed": len(approved), "requested": len(codes), "last_tick_at": tick["received_at"]}
                        delay = 2
        except (BrokerError, OSError, websockets.exceptions.WebSocketException, TimeoutError):
            _status[user] = {"state": "degraded", "message": "NH 시세 연결 재시도 중 · 기존 시세 경로 사용"}
        await asyncio.sleep(delay + random.random())
        delay = min(delay * 2, 60)


async def run(stop: asyncio.Event):
    jobs = {}
    sync_times = {}
    sync_jobs = {}
    try:
        limit = max(1, min(30, int(os.environ.get("NAMUH_WS_MAX_REGISTRATIONS", "30"))))
    except ValueError:
        limit = 30

    async def refresh(user, aid):
        try:
            await sync_account(user, aid)
        except (BrokerError, aiosqlite.Error, ValueError) as exc:
            logger.warning("NH 잔고 동기화 보류: %s", type(exc).__name__)
    try:
        while not stop.is_set():
            try:
                links = await brokers.list_links()
                desired = {}
                for link in links:
                    user, cid = link["google_sub"], link["credential_id"]
                    if cid not in desired:
                        rows = await account_holdings.list_positions(user)
                        codes = sorted({r["stock_code"] for r in rows if re.fullmatch(r"[0-9][0-9A-Z]{5}", r["stock_code"])})
                        if codes:
                            desired[cid] = (user, tuple(codes[:limit]), link["environment"])
                    aid = link["account_id"]
                    if time.monotonic() - sync_times.get(aid, 0) > 300 and (aid not in sync_jobs or sync_jobs[aid].done()):
                        sync_times[aid] = time.monotonic()
                        sync_jobs[aid] = asyncio.create_task(refresh(user, aid))
                active_accounts = {link["account_id"] for link in links}
                for aid in list(sync_jobs):
                    if aid not in active_accounts:
                        sync_jobs[aid].cancel()
                        await asyncio.gather(sync_jobs.pop(aid), return_exceptions=True)
                        sync_times.pop(aid, None)
                for cid, (signature, task) in list(jobs.items()):
                    if desired.get(cid) != signature or task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                        jobs.pop(cid)
                for cid, signature in desired.items():
                    if cid not in jobs:
                        user, codes, env = signature
                        jobs[cid] = (signature, asyncio.create_task(stream(user, cid, list(codes), env)))
            except (aiosqlite.Error, BrokerError, ValueError) as exc:
                logger.warning("NH 연결 목록 재확인 예정: %s", type(exc).__name__)
            try:
                await asyncio.wait_for(stop.wait(), timeout=30)
            except TimeoutError:
                pass
    finally:
        for _, task in jobs.values():
            task.cancel()
        for task in sync_jobs.values():
            task.cancel()
        await asyncio.gather(*(task for _, task in jobs.values()), *sync_jobs.values(), return_exceptions=True)
