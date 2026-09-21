"""증권사 공통 서버 감시: 연결 변경·재조회 병합·보완 조회·종료 정리."""

import asyncio
import logging
import time

import aiosqlite

from repositories import brokers
from repositories.broker_secrets import BrokerError
from services.brokers.sync import sync_account

logger = logging.getLogger(__name__)


async def run_accounts(adapter, stop: asyncio.Event, *, synchronize=sync_account):
    sockets, refreshes, due, followups = {}, {}, {}, {}
    wake = asyncio.Event()

    def changed(aid, account_ids):
        for target in ([aid] if aid else account_ids):
            due[target] = 0
            followups[target] = time.monotonic() + 12
        wake.set()

    async def refresh(user, aid):
        try:
            await synchronize(user, aid, include_activity=adapter.definition.activity)
        except (BrokerError, aiosqlite.Error, ValueError):
            logger.warning("증권사 잔고 갱신 보류: %s", adapter.definition.id)

    try:
        while not stop.is_set():
            wake.clear()
            try:
                links = [row for row in await brokers.list_links() if row.get("provider") == adapter.definition.id]
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
                        adapter.forget(signature[0], cid)
                for cid, signature in desired.items():
                    if cid not in sockets:
                        user, env, aids = signature
                        sockets[cid] = (signature, asyncio.create_task(adapter.stream(user, cid, env, lambda aid, targets=aids: changed(aid, targets))))
            except (BrokerError, aiosqlite.Error):
                logger.warning("증권사 연결 목록 재확인: %s", adapter.definition.id)
            try:
                await asyncio.wait_for(wake.wait(), timeout=1)
            except TimeoutError:
                pass
    finally:
        tasks = [task for _, task in sockets.values()] + list(refreshes.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for cid, (signature, _) in sockets.items():
            adapter.forget(signature[0], cid)
