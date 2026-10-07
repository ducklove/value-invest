"""KIS shared price socket, including account notices for matching server keys."""

from __future__ import annotations

import asyncio
import time

import kis_ws_manager
from repositories import brokers
from services.brokers import kis_realtime
from services.market.quote_policy import trade_timestamp


class KisSource:
    provider = "kis"

    def __init__(self, slot, publish):
        self.id = f"kis:{slot.slot_id}"
        self.slot = slot
        self.publish = publish
        self.conn = kis_ws_manager.WsConnection(slot)
        self.conn.on_control = self._control
        self.conn.on_frame = self._frame
        self.codes = []
        self.bindings = {}
        self.secrets = {}
        self._task = None
        self._blocked_until = 0.0

    @property
    def capacity(self):
        return max(0, kis_ws_manager.MAX_SUBSCRIPTIONS - len(self.conn.extra_subscriptions))

    @property
    def approved(self):
        return {code for code, channel in self.conn._confirmed_subs if channel in kis_ws_manager._ACCEPTED_TR_IDS}

    def supports(self, code):
        return kis_ws_manager.is_korean_stock(code)

    def available(self, code):
        return time.monotonic() >= self._blocked_until and not self.conn._integrated_rejected and not any(c == code for c, _ in self.conn._rejected_subs)

    async def set_codes(self, codes):
        self.codes = list(codes)
        self.conn.update_subscriptions({"portfolio": self.codes})
        await self._sync()

    async def _sync(self):
        if self._task is None and (self.codes or self.bindings):
            await self.conn.start()
            self._task = asyncio.create_task(self._relay(), name=f"{self.id}:relay")
        await self.conn.sync_subscriptions()

    async def watch_account(self, user, cid, hts, changed):
        self.bindings[(user, cid)] = (hts, changed)
        self.conn.extra_subscriptions = {(hts_id, channel) for hts_id, _ in self.bindings.values()
                                         for channel in ("H0STCNI0", "H0GSCNI0")}
        try:
            await self._sync()
            changed(None)
            await asyncio.Future()
        finally:
            self.bindings.pop((user, cid), None)
            self.conn.extra_subscriptions = {(hts_id, channel) for hts_id, _ in self.bindings.values()
                                             for channel in ("H0STCNI0", "H0GSCNI0")}
            kis_realtime._states.pop((user, cid), None)

    async def _control(self, message):
        channel = (message.get("header") or {}).get("tr_id")
        if channel not in {"H0STCNI0", "H0GSCNI0"}:
            return
        body = message.get("body") or {}
        output = body.get("output") or {}
        if body.get("rt_cd") == "0" and isinstance(output, dict) and isinstance(output.get("key"), str) and isinstance(output.get("iv"), str):
            self.secrets[channel] = (output["key"], output["iv"])
        else:
            self.secrets.pop(channel, None)
        self._notice_status()

    def _notice_status(self):
        for (user, cid), (hts, _) in self.bindings.items():
            approved = sum((hts, channel) in self.conn._confirmed_subs for channel in ("H0STCNI0", "H0GSCNI0"))
            kis_realtime._states[(user, cid)] = {"state": "subscribed" if approved == 2 else "polling", "subscribed": approved}

    async def _frame(self, raw):
        numbers = kis_realtime.decode_accounts(raw, self.secrets)
        if not numbers:
            return
        for row in await brokers.list_links():
            callback = self.bindings.get((row["google_sub"], row["credential_id"]))
            if row.get("provider") == "kis" and callback:
                link = await brokers.get_link(row["google_sub"], row["account_id"])
                if link["account_no"] in numbers:
                    callback[1](row["account_id"])

    async def _relay(self):
        while True:
            message = await self.conn.listener.get()
            if message.get("type") == "stream_status":
                if self.conn.state == "reconnecting":
                    self._blocked_until = time.monotonic() + 15
                if self.conn.state != "connected":
                    self.secrets.clear()
                self._notice_status()
                if self.conn.state == "connected":
                    for _, changed in self.bindings.values():
                        changed(None)
            elif message.get("type") == "quote" or message.get("price") is not None:
                if message.get("code") in self.approved:
                    self.publish({**message, "source": "kis_ws", "currency": "KRW"})

    async def close(self):
        await self.conn.stop()
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        self._task = None
        self.secrets.clear()
        self._notice_status()

    def snapshot(self):
        state = self.conn.state
        if state == "connected":
            tick_at = trade_timestamp(self.conn.last_tick_at)
            state = "live" if self.approved and tick_at and 0 <= time.time() - tick_at < 90 else "subscribed" if self.approved else "connected"
        return {"id": self.id, "provider": self.provider, "state": state, "capacity": self.capacity,
                "requested": len(self.codes), "subscribed": len(self.approved),
                "reserved": len(self.conn.extra_subscriptions), "rejected": len(self.conn._rejected_subs),
                "reason": "integrated_subscription_rejected" if self.conn._integrated_rejected else None,
                "last_frame_at": self.conn.last_frame_at, "last_tick_at": self.conn.last_tick_at,
                "reconnects": self.conn.reconnects}
