"""종목 알림 메시지 → 종목코드 매핑 기록 (텔레그램 답장 = 메모).

발송 경로(engine → alert_delivery → channels)의 기존 시그니처를 깨지 않도록,
종목코드는 :func:`stock_context` 컨텍스트 변수 또는 ``channels.dispatch`` 의 선택
인자로 전달된다. 종목코드가 있으면 텔레그램 응답의 ``message_id`` 를
``telegram_message_refs`` 에 저장하고, 없으면 기존 ``telegram.send_message`` 를 그대로 쓴다.
"""

from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from contextvars import ContextVar

from core.errors import DBError
from repositories import telegram_bot as telegram_bot_repo
from services.notifications import telegram

logger = logging.getLogger(__name__)

_current_stock_code: ContextVar[str | None] = ContextVar("telegram_stock_code", default=None)


@contextmanager
def stock_context(stock_code: str | None):
    """이 블록 안의 텔레그램 발송을 ``stock_code`` 종목 메시지로 기록한다."""
    token = _current_stock_code.set(str(stock_code).strip().upper() if stock_code else None)
    try:
        yield
    finally:
        _current_stock_code.reset(token)


def current_stock_code() -> str | None:
    return _current_stock_code.get()


async def remember(google_sub: str, token: str, chat_id, message_id: int | None, stock_code: str | None) -> None:
    """보낸 종목 메시지의 매핑을 저장한다. 실패해도 발송 결과에는 영향 없음."""
    if not stock_code or not message_id:
        return
    try:
        await telegram_bot_repo.save_message_ref(
            telegram_bot_repo.bot_key(token), str(chat_id), int(message_id), google_sub, stock_code
        )
    except (DBError, sqlite3.Error, OSError, ValueError, TypeError) as exc:
        logger.warning("telegram message ref save failed user=%s: %s", google_sub[:8], exc)


async def send_message(google_sub: str, token: str, chat_id, text: str, *, stock_code: str | None = None) -> bool:
    """channels.dispatch 의 텔레그램 발송. 종목코드가 있으면 message_id 를 기록한다."""
    code = stock_code or current_stock_code()
    if not code:
        return await telegram.send_message(token, chat_id, text)
    sent_ids: list[int] = []
    if not await telegram.send_message(token, chat_id, text, sent_ids=sent_ids):
        return False
    if sent_ids:
        await remember(google_sub, token, chat_id, sent_ids[0], code)
    return True
