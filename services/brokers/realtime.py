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
from services.brokers import namuh, notifications, overseas_realtime
from services.brokers.sync import sync_account
from services.portfolio.quotes import should_accept_quote_snapshot

_quotes = MemoryTTLCache("namuh.realtime", 90)
_status: dict[object, dict] = {}
_KST = timezone(timedelta(hours=9))
logger = logging.getLogger(__name__)


def subscription(code: str) -> tuple[str, str] | None:
    if code == "KRX_GOLD":
        return "g4", "M04020000"
    if re.fullmatch(r"[0-9][0-9A-Z]{5}", code):
        return "mc", code
    info = overseas_realtime.instrument(code)
    if info:
        return "RC", info["gic"]
    return None


def select_codes(rows: list[dict], limit: int, *, foreign: bool = False) -> list[str]:
    # 금현물도 동일한 등록 한도를 사용하되 국내 주식에 밀려 제외되지 않게 한다.
    codes = {row["stock_code"] for row in rows if (pair := subscription(row["stock_code"])) and (pair[0] == "RC") == foreign}
    selected, registrations = [], set()
    for code in sorted(codes, key=lambda code: (code != "KRX_GOLD", code)):
        pair = subscription(code)
        if pair in registrations or len(registrations) < limit:
            selected.append(code)
            registrations.add(pair)
    return selected


def normalize(message: dict, now: datetime | None = None) -> dict | None:
    head = message.get("header", {})
    if not isinstance(head, dict):
        return None
    channel = head.get("tr_cd")
    if channel not in ("mc", "g4") or "rsp_cd" in head or "tr_type" in head:
        return None
    body = message.get("body")
    if not isinstance(body, dict):
        return None
    gold = channel == "g4"
    key = str(body.get("shcode" if gold else "code", ""))
    code = "KRX_GOLD" if gold and key == "M04020000" else key
    if subscription(code) != (channel, key):
        return None
    if head.get("tr_key") not in (None, key):
        return None
    now = now or datetime.now(_KST)
    raw_time = str(body.get("time", "")).replace(":", "")
    try:
        at = datetime.strptime(raw_time, "%H%M%S").replace(year=now.year, month=now.month, day=now.day, tzinfo=_KST)
        price = float(body["cheprice" if gold else "price"])
        change = abs(float(body["change"])) * (-1 if str(body.get("sign")) in {"4", "5"} else 1)
        previous = price - change
        if not 0 < price < 1e12 or not math.isfinite(previous) or previous <= 0 or not 0 <= (now - at).total_seconds() < 90:
            return None
        value = float(body.get("totvalue" if gold else "value_won") or 0)
        if not math.isfinite(value) or value < 0:
            return None
        return {"type": "quote", "code": code, "price": price, "previous_close": previous, "change": change,
                "change_pct": change / previous * 100, "source": "namuh_ws", "market": "KRX" if gold else "UN", "currency": "KRW",
                "date": at.date().isoformat(), "as_of": at.isoformat(), "ts": now.timestamp(),
                "received_at": now.isoformat(), "trade_value": value}
    except (KeyError, ValueError, TypeError, OverflowError):
        return None


def quote(user: str, code: str) -> dict | None:
    result = _quotes.get((user, code))
    now = datetime.now(_KST)
    if result and 0 <= (now - datetime.fromisoformat(result["as_of"])).total_seconds() < 90:
        return dict(result)
    return None


def status(user: str) -> dict:
    parts = {}
    for name, key in (("domestic", user), ("foreign", (user, "foreign"))):
        if key not in _status:
            continue
        part = dict(_status[key])
        if part.get("state") == "live" and (datetime.now(_KST) - datetime.fromisoformat(part["last_tick_at"])).total_seconds() >= 90:
            part["state"] = "subscribed"
        parts[name] = part
    if not parts:
        return {"state": "waiting", "message": "NH 시세 연결 대기"}
    state = next((state for state in ("live", "subscribed", "connecting", "degraded")
                  if any(part.get("state") == state for part in parts.values())), "waiting")
    return {"state": state, "subscribed": sum(p.get("subscribed", 0) for p in parts.values()),
            "requested": sum(p.get("requested", 0) for p in parts.values()), **parts}


async def stream(user: str, cid: str, codes: list[str], environment: str, *, foreign: bool = False,
                 notice_channels: tuple[str, ...] = (), changed=None):
    registrations = {code: subscription(code) for code in codes if subscription(code)}
    if not registrations and not notice_channels:
        return
    delay = 2
    endpoint = "wss://moapi.nhplug.com:17070/websocket" if environment == "mock" else f"wss://api.nhplug.com:{7080 if foreign else 7070}/websocket"
    state_key = (user, "foreign") if foreign else user
    if not registrations:
        state_key = (user, cid, "notices")
    infos = {code: overseas_realtime.instrument(code) for code in codes} if foreign else {}
    context = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    while True:
        try:
            access = await namuh.token(user, cid)
            if notice_channels:
                notifications._states[(user, cid)] = {"state": "connecting", "approved": set(), "rejected": set()}
            _status[state_key] = {"state": "connecting", "subscribed": 0, "requested": len(set(registrations.values()))}
            async with websockets.connect(endpoint, ssl=context, ping_interval=None, open_timeout=15, close_timeout=3, max_size=2**20) as ws:
                for channel in notice_channels:
                    await ws.send(json.dumps({"header": {"token": access, "tr_type": "1"}, "body": {"tr_cd": channel, "tr_key": ""}}))
                    await asyncio.sleep(.12)
                if changed:
                    # 재접속 동안 놓친 통보는 REST 재조회로 복구한다.
                    changed(None)
                for channel, key in dict.fromkeys(registrations.values()):
                    await ws.send(json.dumps({"header": {"token": access, "tr_type": "1"}, "body": {"tr_cd": channel, "tr_key": key}}))
                    await asyncio.sleep(.12)
                approved = set()
                rejected = set()
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
                        body = message.get("body") or {}
                        channel = head.get("tr_cd") or (body.get("tr_cd") if isinstance(body, dict) else None)
                        if channel in notice_channels:
                            state = notifications._states[(user, cid)]
                            if str(head["rsp_cd"]) == "00000":
                                state["approved"].add(channel)
                                if len(state["approved"]) == len(notice_channels) and not state["rejected"]:
                                    state["state"] = "subscribed"
                            else:
                                state["rejected"].add(channel)
                                state["state"] = "degraded"
                            continue
                        keys = body.get("tr_key", head.get("tr_key", [])) if isinstance(body, dict) else []
                        keys = keys if isinstance(keys, list) else [keys]
                        if str(head["rsp_cd"]) != "00000":
                            rejected.update(keys or [pair[1] for pair in registrations.values() if pair not in approved])
                            _status[state_key] = {"state": "degraded", "subscribed": len(approved),
                                                  "requested": len(set(registrations.values())), "rejected": len(rejected),
                                                  "message": "해외 실시간 시세 권한·구독 한도 확인 필요" if foreign else "NH 시세 구독 권한·한도 확인 필요"}
                            continue
                        approved.update(pair for pair in registrations.values() if pair[1] in keys)
                        _status[state_key] = {"state": "subscribed", "subscribed": len(approved), "requested": len(set(registrations.values())), "rejected": len(rejected)}
                        continue
                    if head.get("tr_cd") in notice_channels:
                        aid = await notifications.account_for_message(user, cid, environment, message)
                        if aid and changed:
                            notice = notifications._states[(user, cid)]
                            notice.update(state="degraded" if notice["rejected"] else "received", last_event_at=datetime.now(_KST).isoformat())
                            changed(aid)
                        continue
                    if foreign:
                        body = message.get("body")
                        gic = str(body.get("gicz15") or "").strip() if isinstance(body, dict) else None
                        ticks = []
                        for code, info in infos.items():
                            if info and info["gic"] == gic:
                                tick = overseas_realtime.normalize(message, code, info, datetime.now(_KST))
                                if tick:
                                    tick = await overseas_realtime.to_won(tick)
                                    if tick:
                                        ticks.append(tick)
                    else:
                        ticks = [normalize(message)]
                    for tick in ticks:
                        if tick and tick["code"] in registrations and should_accept_quote_snapshot(quote(user, tick["code"]), tick):
                            _quotes.set((user, tick["code"]), tick)
                            _status[state_key] = {"state": "live", "subscribed": len(approved), "requested": len(set(registrations.values())),
                                                  "rejected": len(rejected), "last_tick_at": tick["received_at"]}
                            delay = 2
        except (BrokerError, OSError, websockets.exceptions.WebSocketException, TimeoutError):
            if notice_channels:
                notifications._states[(user, cid)] = {"state": "degraded", "approved": set()}
            _status[state_key] = {"state": "degraded", "message": "NH 시세 연결 재시도 중 · 기존 시세 경로 사용"}
        await asyncio.sleep(delay + random.random())
        delay = min(delay * 2, 60)


async def run(stop: asyncio.Event):
    jobs, sync_jobs, sync_times, followups = {}, {}, {}, {}
    pending = set()
    wake = asyncio.Event()
    try:
        limit = max(1, min(30, int(os.environ.get("NAMUH_WS_MAX_REGISTRATIONS", "30"))))
    except ValueError:
        limit = 30

    async def refresh(user, aid):
        try:
            await sync_account(user, aid, include_activity=True)
        except (BrokerError, aiosqlite.Error, ValueError) as exc:
            logger.warning("NH 계좌 동기화 보류: %s", type(exc).__name__)
        finally:
            wake.set()

    def invalidate(aid, account_ids):
        for target in ([aid] if aid else account_ids):
            pending.add(target)
            followups[target] = time.monotonic() + 12
        wake.set()

    try:
        while not stop.is_set():
            wake.clear()
            try:
                links = await brokers.list_links()
                desired, users, credentials = {}, set(), set()
                ordered = sorted(links, key=lambda row: (row["google_sub"], row["environment"] != "live", row["credential_id"]))
                for link in ordered:
                    user, cid, env = link["google_sub"], link["credential_id"], link["environment"]
                    if cid not in credentials:
                        credentials.add(cid)
                        linked = [r for r in links if r["credential_id"] == cid and r["environment"] == env and r["google_sub"] == user]
                        notices = notifications.channels(linked)
                        codes = []
                        if user not in users:
                            users.add(user)
                            rows = await account_holdings.list_positions(user)
                            if any(not re.fullmatch(r"[0-9][0-9A-Z]{5}", r["stock_code"]) and not r["stock_code"].startswith(("CASH_", "CRYPTO_", "FUTURES_", "CMA_", "KRX_")) for r in rows):
                                await overseas_realtime.ensure_master()
                            codes = select_codes(rows, max(0, limit - len(notices)))
                            foreign_codes = select_codes(rows, limit, foreign=True)
                            if foreign_codes:
                                desired[(cid, True)] = (user, tuple(foreign_codes), env, (), ())
                        # 같은 키의 국내 시세와 모든 통보를 한 소켓에 합쳐 연결 2개 한도를 지킨다.
                        if codes or notices:
                            desired[(cid, False)] = (user, tuple(codes), env, notices, tuple(r["account_id"] for r in linked))
                    aid = link["account_id"]
                    now = time.monotonic()
                    due = now - sync_times.get(aid, -1000) >= 60 or aid in pending or now >= followups.get(aid, float("inf"))
                    if due and now - sync_times.get(aid, -1000) >= 2 and (aid not in sync_jobs or sync_jobs[aid].done()):
                        pending.discard(aid)
                        if now >= followups.get(aid, float("inf")):
                            followups.pop(aid, None)
                        sync_times[aid] = now
                        sync_jobs[aid] = asyncio.create_task(refresh(user, aid))
                active_accounts = {link["account_id"] for link in links}
                pending.intersection_update(active_accounts)
                for aid in list(sync_jobs):
                    if aid not in active_accounts:
                        sync_jobs[aid].cancel()
                        await asyncio.gather(sync_jobs.pop(aid), return_exceptions=True)
                        sync_times.pop(aid, None)
                        followups.pop(aid, None)
                for key, (signature, task) in list(jobs.items()):
                    if desired.get(key) != signature or task.done():
                        task.cancel()
                        await asyncio.gather(task, return_exceptions=True)
                        jobs.pop(key)
                        old_user, old_codes, _, _, _ = signature
                        for code in old_codes:
                            _quotes.delete((old_user, code))
                        _status.pop((old_user, "foreign") if key[1] else old_user if old_codes else (old_user, key[0], "notices"), None)
                        if not key[1]:
                            notifications._states.pop((old_user, key[0]), None)
                for key, signature in desired.items():
                    if key not in jobs:
                        user, codes, env, notices, aids = signature
                        options = {"foreign": True} if key[1] else {
                            "notice_channels": notices,
                            "changed": lambda aid, targets=aids: invalidate(aid, targets),
                        }
                        jobs[key] = (signature, asyncio.create_task(stream(user, key[0], list(codes), env, **options)))
            except (aiosqlite.Error, BrokerError, ValueError) as exc:
                logger.warning("NH 연결 목록 재확인 예정: %s", type(exc).__name__)
            try:
                await asyncio.wait_for(wake.wait(), timeout=1)
            except TimeoutError:
                pass
    finally:
        for _, task in jobs.values():
            task.cancel()
        for task in sync_jobs.values():
            task.cancel()
        await asyncio.gather(*(task for _, task in jobs.values()), *sync_jobs.values(), return_exceptions=True)
        notifications._states.clear()
