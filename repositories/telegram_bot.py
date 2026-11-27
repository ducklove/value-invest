"""텔레그램 양방향 비서 저장소 — 메시지→종목 매핑, 수신 대상, getUpdates offset.

* ``telegram_message_refs`` — 봇이 보낸 종목 메시지의 ``message_id`` → 종목코드.
* 수신 대상은 ``notification_channels`` (channel='telegram', enabled·verified) 에서 읽는다.
* offset 은 ``cache_values`` (namespace ``telegram_bot.offset``, key=bot_key) 에 영속한다.

봇 토큰 원문은 어디에도 새로 저장하지 않는다 — 키는 :func:`bot_key` 해시만 쓴다.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta

from repositories import cache_values
from repositories.db import get_db, transaction

OFFSET_NAMESPACE = "telegram_bot.offset"
REF_RETENTION_DAYS = 30


def bot_key(token: str) -> str:
    """토큰 → 로그·DB 에 써도 되는 짧은 식별자(sha256 앞 16자)."""
    return hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()[:16]


async def save_message_ref(key: str, chat_id: str, message_id: int, google_sub: str, stock_code: str) -> None:
    async with transaction() as db:
        await db.execute(
            """
            INSERT INTO telegram_message_refs (bot_key, chat_id, message_id, google_sub, stock_code, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(bot_key, chat_id, message_id) DO UPDATE SET
                google_sub = excluded.google_sub,
                stock_code = excluded.stock_code,
                created_at = excluded.created_at
            """,
            (key, str(chat_id), int(message_id), google_sub, stock_code, datetime.now().isoformat()),
        )


async def get_message_ref(key: str, chat_id: str, message_id: int, google_sub: str) -> str | None:
    """답장 대상 메시지의 종목코드. 다른 사용자의 매핑은 절대 돌려주지 않는다."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT stock_code FROM telegram_message_refs"
        " WHERE bot_key = ? AND chat_id = ? AND message_id = ? AND google_sub = ?",
        (key, str(chat_id), int(message_id), google_sub),
    )
    row = await cursor.fetchone()
    return row["stock_code"] if row else None


async def prune_message_refs(days: int = REF_RETENTION_DAYS) -> int:
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    async with transaction() as db:
        cursor = await db.execute("DELETE FROM telegram_message_refs WHERE created_at < ?", (cutoff,))
        return cursor.rowcount or 0


async def list_bot_channels() -> list[dict]:
    """켜져 있고 chat_id 가 확인된 텔레그램 채널 전부 → [{google_sub, config}]."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT google_sub, config_json FROM notification_channels"
        " WHERE channel = 'telegram' AND enabled = 1 AND verified = 1"
    )
    items: list[dict] = []
    for row in await cursor.fetchall():
        try:
            config = json.loads(row["config_json"] or "{}")
        except (TypeError, ValueError):
            config = {}
        if isinstance(config, dict):
            items.append({"google_sub": row["google_sub"], "config": config})
    return items


async def get_offset(key: str) -> int | None:
    entry = await cache_values.get_cache_value_entry(OFFSET_NAMESPACE, key, allow_stale=True)
    if entry is None:
        return None
    try:
        return int(entry.value)
    except (TypeError, ValueError):
        return None


async def set_offset(key: str, offset: int) -> None:
    await cache_values.set_cache_value(OFFSET_NAMESPACE, key, int(offset))


async def held_memo(google_sub: str, stock_code: str) -> tuple[bool, str]:
    """(보유 여부, 현재 보유 메모). 합산 보유 테이블(user_portfolio) 기준."""
    db = await get_db()
    cursor = await db.execute(
        "SELECT COALESCE(memo, '') AS memo FROM user_portfolio WHERE google_sub = ? AND stock_code = ?",
        (google_sub, stock_code),
    )
    row = await cursor.fetchone()
    return (True, row["memo"]) if row else (False, "")
