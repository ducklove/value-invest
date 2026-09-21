"""주문·체결 통보는 잔고 재조회 신호다. 통보 금액을 잔고에 직접 더하지 않는다."""

import re

from repositories import brokers

CHANNELS = {"stocks": ("d2", "d3", "d0", "d1"), "gold": ("de", "d3"),
            "krfuture": ("d2", "d3", "dv", "dn"), "gbfuture": ("dk", "dj")}
_states: dict[tuple[str, str], dict] = {}


def channels(links: list[dict]) -> tuple[str, ...]:
    return tuple(sorted({channel for link in links for channel in CHANNELS.get(link.get("product", "stocks"), ())
                         if link.get("include_overseas", True) or channel not in {"d0", "d1"}}))


def status(user: str) -> dict:
    rows = [state for (owner, _), state in _states.items() if owner == user]
    return {"connections": len(rows), "subscribed": sum(len(row.get("approved", [])) for row in rows),
            "state": "degraded" if any(row["state"] == "degraded" for row in rows) else
            "subscribed" if rows and all(row["state"] in {"subscribed", "received"} for row in rows) else "connecting",
            "last_event_at": max((row.get("last_event_at", "") for row in rows), default=""),
            "cash_poll_seconds": 60}


async def account_for_message(user: str, cid: str, environment: str, message: dict) -> str | None:
    head, body = message.get("header"), message.get("body")
    if not isinstance(head, dict) or not isinstance(body, dict) or "rsp_cd" in head or "tr_type" in head:
        return None
    channel = head.get("tr_cd")
    if channel not in {code for values in CHANNELS.values() for code in values}:
        return None
    number = str(body.get("accountno") or body.get("act_no") or "").strip()
    if not re.fullmatch(r"\d{11}", number):
        return None
    # 현재 연결 상태로 다시 확인하여 연결 해제/다른 사용자/다른 키의 통보를 격리한다.
    for row in await brokers.list_links():
        if row["google_sub"] != user or row["credential_id"] != cid or row["environment"] != environment:
            continue
        link = await brokers.get_link(user, row["account_id"])
        if link["account_no"] == number and channel in channels([link]):
            return link["account_id"]
    return None
