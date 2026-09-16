"""종목별 근월물 순회 → 후보 호가 감시. 최종거래일 2거래일 전 월물 전환."""

import asyncio
import json
import logging
import ssl
import time
from datetime import datetime

import aiosqlite
import truststore
import websockets

from repositories import account_holdings, brokers, quant_scanner
from repositories.broker_secrets import BrokerError
from repositories.quant import QuantError, digest
from services.brokers import namuh
from services.quant import rollover, scanner_feed
from services.quant.scanner_model import KST, ScannerConfig, edge, realtime_book, watch_list

logger = logging.getLogger(__name__)
_runtime = {}


async def configure(user, config):
    links = await brokers.list_links()
    if not any(x["google_sub"] == user and x["account_id"] == config.account_id for x in links):
        raise QuantError("본인의 나무 연결 계좌를 선택하세요.")
    await quant_scanner.configure(user, config.model_dump())
    _runtime.pop(user, None)


async def status(user):
    settings = await quant_scanner.settings(user)
    links = [x for x in await brokers.list_links() if x["google_sub"] == user]
    observed = {r["contract"]: r for r in await quant_scanner.rows(user)}
    settings = settings[0] if settings else None
    progress = dict(settings["progress"]) if settings else {}
    excluded = []
    try:
        selection = rollover.universe(await scanner_feed.catalog(), datetime.now(KST).date())
        contracts, excluded = selection["contracts"], selection["excluded"]
        catalog_hash = digest(contracts)
        data = [{**c, **observed[c["contract"]]} if observed.get(c["contract"], {}).get("catalog_hash") == catalog_hash
                else {**c, "observed_at": None, "net_bps": None, "error": "아직 관측하지 않음"} for c in contracts]
        if progress.get("catalog_hash") != catalog_hash:
            progress.update(cursor=0, rounds=0, last_scan_at=None, last_cycle_seconds=None)
        progress.update({k: v for k, v in selection.items() if k not in {"contracts", "excluded"}})
        progress.update(total=len(contracts), excluded_count=len(excluded), catalog_at=scanner_feed._master_at)
    except QuantError as exc:
        data = []
        progress.update(total=0, cursor=0, state="degraded", catalog_error=str(exc))
    data.sort(key=lambda r: (r.get("net_bps") is not None, r.get("net_bps") if r.get("net_bps") is not None else -1e10), reverse=True)
    accounts = []
    for link in links:
        account = await account_holdings.require_account(user, link["account_id"])
        accounts.append({"account_id": link["account_id"], "environment": link["environment"], "name": account.get("name", "나무 연결 계좌")})
    valid_codes = {r["contract"] for r in data if r.get("observed_at") and not r.get("error")}
    runtime = dict(_runtime.get(user, {}))
    for field in ("watched", "signals"):
        runtime[field] = [code for code in runtime.get(field, []) if code in valid_codes]
    return {"config": settings["config"] if settings else None,
            "progress": progress, "runtime": runtime,
            "accounts": accounts,
            "rows": data, "excluded": excluded, "events": await quant_scanner.events(user),
            "orders_sent": 0, "live_eligible": False, "market_data_environment": "live",
            "limits": {"sessions_used_for_holdings": 1, "sessions_for_scanner": 1, "registrations": 30}}


class Watcher:
    def __init__(self, user, cid, env, config, generation, progress):
        self.user, self.cid, self.env = user, cid, env
        self.config, self.generation, self.progress = config, generation, progress
        self.selected = {}
        self.rows = {}
        self.books = {}
        self.active_signals = set()
        self.state = {"state": "waiting", "requested": 0, "approved": 0}

    def public(self):
        if time.time() - self.state.get("last_tick_at", 0) > 5 and self.state.get("state") == "receiving":
            self.state["state"] = "subscribed"
        _runtime[self.user] = {**self.state, "watched": list(self.selected),
                               "books": len(self.books), "signals": sorted(self.active_signals),
                               "updated_at": time.time(), "orders_sent": 0}

    async def run(self):
        delay = 2
        while True:
            if not self.selected:
                self.books.clear()
                self.active_signals.clear()
                self.state = {"state": "waiting", "requested": 0, "approved": 0}
                self.public()
                await asyncio.sleep(1)
                continue
            regs = set()
            for code in self.selected:
                regs.update({("vH", code[1:]), ("ob", self.rows[code]["spot_code"])})
            if len(regs) > 30:
                raise QuantError("실시간 구독 예산 초과")
            # 모의 시세 소켓은 ob/vH를 WSS10006으로 거절한다. 계좌·체결 통보와 별도다.
            endpoint = "wss://api.nhplug.com:7070/websocket"
            self.books.clear()
            self.active_signals.clear()
            self.state = {"state": "connecting", "requested": len(regs), "approved": 0}
            self.public()
            try:
                access = await namuh.token(self.user, self.cid)
                context = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                async with websockets.connect(endpoint, ssl=context, ping_interval=None, open_timeout=15, close_timeout=3, max_size=2**20) as ws:
                    for channel, key in sorted(regs):
                        await ws.send(json.dumps({"header": {"token": access, "tr_type": "1"}, "body": {"tr_cd": channel, "tr_key": key}}))
                        await asyncio.sleep(.15)
                    approved, written = set(), {}
                    connected_at = time.monotonic()
                    while True:
                        desired = {(ch, key) for c in self.selected for ch, key in (("vH", c[1:]), ("ob", self.rows[c]["spot_code"]))}
                        if regs != desired:
                            break
                        if regs != approved and time.monotonic() - connected_at > 20:
                            raise BrokerError("호가 구독 승인 확인 실패")
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1)
                        except TimeoutError:
                            self.active_signals.clear()
                            self.public()
                            continue
                        try:
                            msg = json.loads(raw)
                        except (json.JSONDecodeError, UnicodeError):
                            continue
                        if not isinstance(msg, dict):
                            continue
                        h = msg.get("header", {})
                        if not isinstance(h, dict):
                            continue
                        if "rsp_cd" in h:
                            if str(h["rsp_cd"]) != "00000":
                                raise BrokerError("실시간 호가 구독 거절")
                            b = msg.get("body", {})
                            keys = b.get("tr_key", h.get("tr_key", [])) if isinstance(b, dict) else []
                            keys = keys if isinstance(keys, list) else [keys]
                            channel = h.get("tr_cd") or (b.get("tr_cd") if isinstance(b, dict) else None)
                            approved.update(pair for pair in regs if pair[1] in keys and (channel is None or pair[0] == channel))
                            self.state.update(state="subscribed", approved=len(approved))
                            self.public()
                            continue
                        now = datetime.now(KST)
                        parsed = realtime_book(msg, now)
                        if not parsed or parsed[0] not in approved:
                            continue
                        key, quote = parsed
                        if key in self.books and quote["at"] < self.books[key]["at"]:
                            continue
                        self.books[key] = quote
                        self.state.update(state="receiving", last_tick_at=now.timestamp())
                        delay = 2
                        for code in list(self.selected):
                            row = self.rows.get(code)
                            if row is None:
                                continue
                            spot, future = self.books.get(("ob", row["spot_code"])), self.books.get(("vH", code[1:]))
                            try:
                                if not spot or not future:
                                    raise ValueError("호가 대기")
                                if not rollover.active(row, now.date()):
                                    raise ValueError("월물 전환일 도달 또는 월물 정책 미확인")
                                opportunity = edge(spot, future, row["expiry"], self.config, now, realtime=True)
                            except (ValueError, QuantError):
                                self.active_signals.discard(code)
                                continue
                            crossed = opportunity["net_bps"] >= self.config.signal_bps
                            newly = crossed and code not in self.active_signals
                            if crossed:
                                self.active_signals.add(code)
                            else:
                                self.active_signals.discard(code)
                            # 동일 호가 재송신으로 기록을 부풀리지 않는다. 연결 공백 뒤엔 새 양쪽 호가 필요.
                            stamp = (spot["at"], future["at"], spot["ask"], future["bid"])
                            if newly or (written.get(code, (0, None))[1] != stamp and now.timestamp() - written.get(code, (0, None))[0] >= 10):
                                event = {"contract": code, "spot_code": row["spot_code"], "name": row["name"],
                                         "type": "opportunity" if crossed else "watch", "config": self.config.model_dump(),
                                         "expiry": row["expiry"], "roll_on": row["roll_on"], "roll_policy": rollover.POLICY,
                                         "calendar_version": rollover.CALENDAR_VERSION, "generation": self.generation, **opportunity}
                                if not await quant_scanner.record(self.user, self.generation, self.progress, event=event):
                                    return
                                written[code] = (now.timestamp(), stamp)
                        self.public()
            except (BrokerError, OSError, websockets.exceptions.WebSocketException, TimeoutError):
                self.books.clear()
                self.active_signals.clear()
                self.state.update(state="degraded", approved=0, message="실시간 연결 보류 · 한도·권한·연결 확인 후 재시도")
                self.public()
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)


def sync_universe(observer, selection, progress):
    """전환 시 과거 호가·후보는 비우고, 원래 월물별 이벤트 이력은 보존한다."""
    contracts = selection["contracts"]
    catalog_hash = digest(contracts)
    if progress.get("catalog_hash") != catalog_hash:
        observer.selected = {}
        observer.books.clear()
        observer.active_signals.clear()
        progress.update(cursor=0, rounds=0, catalog_hash=catalog_hash, cycle_started_at=time.time(),
                        last_scan_at=None, last_cycle_seconds=None)
    codes = {c["contract"] for c in contracts}
    observer.rows = {code: row for code, row in observer.rows.items()
                     if code in codes and row.get("catalog_hash") == catalog_hash}
    observer.selected = {code: at for code, at in observer.selected.items() if code in observer.rows}
    progress.update({k: v for k, v in selection.items() if k not in {"contracts", "excluded"}})
    progress.update(total=len(contracts), excluded_count=len(selection["excluded"]), catalog_at=scanner_feed._master_at)
    observer.public()
    return contracts, catalog_hash


async def scan(setting, link):
    user, generation = setting["google_sub"], setting["generation"]
    config = ScannerConfig(**setting["config"])
    progress = setting["progress"]
    observer = Watcher(user, link["credential_id"], link["environment"], config, generation, progress)
    observer.rows = {r["contract"]: r for r in await quant_scanner.rows(user)}
    watch_task = asyncio.create_task(observer.run())
    cached = {}
    try:
        while True:
            if watch_task.done():
                await watch_task
                return
            try:
                selection = rollover.universe(await scanner_feed.catalog(), datetime.now(KST).date())
                contracts, catalog_hash = sync_universe(observer, selection, progress)
                if not contracts:
                    raise QuantError("감시할 근월물 없음: 다음 월물·계약 정보·달력을 확인하세요.")
                now = datetime.now(KST)
                if not rollover.trading_day(now.date()) or not ("09:00" <= now.strftime("%H:%M") < "15:20"):
                    observer.selected = {}
                    observer.public()
                    progress.update(state="market_closed", message="휴장일·장외에는 대기합니다. 거래일 장 시작부터 근월물 순회를 재개합니다.")
                    if not await quant_scanner.record(user, generation, progress):
                        return
                    await asyncio.sleep(30)
                    continue
                cursor = progress.get("cursor", 0) % len(contracts)
                started = time.monotonic()
                contract = contracts[cursor]
                row = {**contract, "observed_at": time.time(), "catalog_hash": catalog_hash, "net_bps": None, "error": None}
                try:
                    spot, future, expiry = await scanner_feed.snapshot(user, link["credential_id"], contract, link["environment"], cached)
                    quote_now = datetime.now(KST)
                    if not rollover.active(contract, quote_now.date()):
                        raise ValueError("월물 전환일 도달: 이전 월물 관측 제외")
                    row.update(expiry=expiry, expiry_verified=True, **edge(spot, future, expiry, config, quote_now))
                except (BrokerError, QuantError, ValueError, TypeError, KeyError, OverflowError) as exc:
                    row["error"] = str(exc)[:160]
                row["observed_at"] = time.time()
                observer.rows[row["contract"]] = row
                observer.selected = watch_list(list(observer.rows.values()), observer.selected, config, time.time())
                observer.public()
                cursor += 1
                progress.update(state="scanning", cursor=cursor % len(contracts), total=len(contracts),
                                last_scan_at=time.time(),
                                requested_cycle_minutes=config.interval_minutes, message="순회 관측은 주문 신호가 아닙니다.",
                                watched=len(observer.selected), catalog_at=scanner_feed._master_at)
                if cursor == len(contracts):
                    progress.update(rounds=progress.get("rounds", 0) + 1,
                                    last_cycle_seconds=time.time() - progress.get("cycle_started_at", time.time()), cycle_started_at=time.time())
                if not await quant_scanner.record(user, generation, progress, row=row):
                    return
                # 목표보다 조회가 느리면 주기가 늘어난다. 기존 계좌 호출의 공유 제한을 우회하지 않는다.
                await asyncio.sleep(max(.1, config.interval_minutes * 60 / len(contracts) - (time.monotonic() - started)))
            except (QuantError, aiosqlite.Error, OSError) as exc:
                observer.selected = {}
                observer.public()
                progress.update(state="degraded", message=str(exc)[:160])
                if not await quant_scanner.record(user, generation, progress):
                    return
                await asyncio.sleep(30)
    finally:
        watch_task.cancel()
        await asyncio.gather(watch_task, return_exceptions=True)
        _runtime.pop(user, None)


async def run_loop(stop):
    jobs = {}
    try:
        while not stop.is_set():
            try:
                links = {(x["google_sub"], x["account_id"]): x for x in await brokers.list_links()}
                desired = {s["google_sub"]: s for s in await quant_scanner.settings()
                           if s["config"]["enabled"] and (s["google_sub"], s["config"]["account_id"]) in links}
                signatures = {user: (s["generation"], links[(user, s["config"]["account_id"])]["credential_id"],
                                     links[(user, s["config"]["account_id"])]["environment"]) for user, s in desired.items()}
                for user, (signature, task) in list(jobs.items()):
                    if signatures.get(user) != signature or task.done():
                        task.cancel()
                        results = await asyncio.gather(task, return_exceptions=True)
                        if results and isinstance(results[0], Exception):
                            logger.warning("현선물 감시 작업 재시작: %s", type(results[0]).__name__)
                        jobs.pop(user)
                for user, setting in desired.items():
                    if user not in jobs:
                        jobs[user] = (signatures[user], asyncio.create_task(scan(setting, links[(user, setting["config"]["account_id"])])))
            except (aiosqlite.Error, QuantError) as exc:
                logger.warning("현선물 감시 설정 재확인: %s", type(exc).__name__)
            try:
                await asyncio.wait_for(stop.wait(), timeout=5)
            except TimeoutError:
                pass
    finally:
        for _, task in jobs.values():
            task.cancel()
        await asyncio.gather(*(task for _, task in jobs.values()), return_exceptions=True)
