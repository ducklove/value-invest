"""양방향 텔레그램 비서 — 알림에 답장하면 메모가 된다 (LLM 호출 없음).

제스처는 두 개뿐이다.

1. 종목명/코드 한 줄 → 즉시 브리핑 회신: 현재가·등락, 내 보유(수량·평단·수익률),
   메모, 최근 뉴스/공시 2건, 허브 종목분석 링크. 후보가 여럿이면 상위 3개를 제시.
2. 봇이 보낸 종목 메시지(알림·브리핑)에 답장 → 그 종목 보유 메모에 ``MM-DD 내용``
   한 줄을 덧붙인다(500자 초과 시 오래된 줄부터 버림). 보유하지 않은 종목이면
   리서치 메모(``user_stock_preferences.note``)에 저장한다.

수신은 공개 인바운드 없이 long polling(getUpdates). 등록된 봇 토큰마다 워커 하나,
offset 은 ``cache_values`` 에 영속한다. offset 은 업데이트를 **처리하기 전에** 저장한다
(at-most-once) — 재시작해도 같은 답장이 메모에 두 번 붙지 않는다.

보안: ``notification_channels`` 에 등록된 chat_id 와 정확히 일치하는 **개인 채팅**의
사람 메시지에만 응답한다. 그 외(미등록 chat·그룹·채널·봇)는 무시하고 사유만 로그로
남긴다. 토큰은 ``bot_key`` 해시로만, 메시지 본문은 어디에도 로그하지 않는다. 같은
(봇, chat_id)가 여러 계정에 연결돼 있으면 데이터를 섞지 않도록 처리하지 않는다.

``get_recent_chat_id``(채널 연결 시 getUpdates)와의 관계: 그 호출은 offset 없이
조회만 하므로 이 루프의 확인 위치를 바꾸지 않는다. 반대로 이 루프가 업데이트를 먼저
확인하면 연결 화면이 빈 결과를 받으므로, 미등록 개인 채팅은
``telegram.remember_unregistered_chat`` 로 10분간 기억해 연결이 계속 동작하게 한다.
"""

from __future__ import annotations

import asyncio
import logging
import re
import sqlite3
import time
from typing import Any

import httpx

from core.errors import AppError, DBError, ExternalServiceError
from domain.portfolio_codes import is_korean_listing
from domain.timeutil import now_kst
from repositories import corp_codes, portfolio_metadata, user_stocks
from repositories import portfolio as portfolio_repo
from repositories import telegram_bot as repo
from repositories.db import transaction
from services.notifications import telegram, telegram_refs

logger = logging.getLogger(__name__)

POLL_TIMEOUT_S = 25
RESCAN_INTERVAL_S = 60.0
HALT_RETRY_S = 3600.0
PRUNE_INTERVAL_S = 6 * 3600.0
BACKOFF_MIN_S = 5.0
BACKOFF_MAX_S = 300.0

MAX_INPUT_CHARS = 1000      # 이보다 긴 메시지는 처리하지 않는다
MAX_QUERY_CHARS = 30        # 종목 검색어 최대 길이
MEMO_MAX_LEN = portfolio_repo.MEMO_MAX_LEN
NOTE_MAX_LEN = 2000         # user_stocks.save_user_stock_preference 의 note 한도
_DATE_PREFIX_LEN = len("MM-DD ")
MEMO_CONTENT_MAX = MEMO_MAX_LEN - _DATE_PREFIX_LEN
_CODE_RE = re.compile(r"^[0-9][0-9A-Z]{5}$")

HELP_TEXT = (
    "🤖 Value Compass 비서\n"
    "• 종목명이나 코드를 한 줄로 보내면 바로 브리핑해 드려요. (예: 삼성전자, 005930)\n"
    "• 봇이 보낸 종목 알림·브리핑에 답장하면 그 종목 메모에 날짜와 함께 저장돼요."
)

_HANDLE_ERRORS = (
    DBError, sqlite3.Error, httpx.HTTPError, AppError, ValueError, TypeError, KeyError, OSError, TimeoutError,
)
_LOOKUP_ERRORS = (httpx.HTTPError, AppError, sqlite3.Error, ValueError, TypeError, KeyError, OSError, TimeoutError)


# --- 순수 헬퍼 -----------------------------------------------------------------

def clean_text(text: str) -> str:
    """공백·줄바꿈을 한 칸으로 정리."""
    return " ".join(str(text or "").split())


def append_memo_line(existing: str | None, line: str, max_len: int) -> str:
    """기존 메모 끝에 ``line`` 을 새 줄로 붙이고, ``max_len`` 을 넘으면 오래된 줄부터 버린다."""
    lines = [part.strip() for part in str(existing or "").splitlines() if part.strip()]
    lines.append(line.strip())
    while len("\n".join(lines)) > max_len and len(lines) > 1:
        lines.pop(0)
    return "\n".join(lines)[:max_len]


def _fmt_qty(value: Any) -> str:
    try:
        qty = float(value)
    except (TypeError, ValueError):
        return "-"
    text = f"{qty:,.4f}".rstrip("0").rstrip(".")
    return text or "0"


def _fmt_won(value: float) -> str:
    return f"{value:,.0f}원"


def _to_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None  # NaN 제외


def _price_line(code: str, quote: dict | None) -> tuple[str, float | None]:
    """(현재가 줄, 원화 현재가 — 국내 종목만). 시세가 없으면 숫자를 지어내지 않는다."""
    price = _to_float((quote or {}).get("price"))
    if price is None or price <= 0:
        return "현재가: 미확인 (시세 조회 실패)", None
    pct = _to_float(quote.get("change_pct"))
    change = _to_float(quote.get("change"))
    stale = " · 지연/마지막 시세" if quote.get("_stale") else ""
    if is_korean_listing(code):
        text = f"현재가 {_fmt_won(price)}"
        if pct is not None:
            arrow = "▲" if pct > 0 else ("▼" if pct < 0 else "―")
            text += f" {arrow}{abs(pct):.2f}%"
            if change is not None:
                text += f" ({change:+,.0f})"
        return text + stale, price
    orig = _to_float(quote.get("original_price"))
    currency = quote.get("original_currency")
    text = f"현재가 {orig:,.2f} {currency}" if orig is not None and currency else f"현재가 {price:,.2f}"
    if pct is not None:
        text += f" ({pct:+.2f}%)"
    return text + stale, None


def _holding_line(code: str, holding: dict, krw_price: float | None) -> str:
    qty = _fmt_qty(holding.get("quantity"))
    avg = _to_float(holding.get("avg_price"))
    if not is_korean_listing(code):
        avg_text = f"{avg:,.2f}" if avg is not None else "-"
        return f"보유 {qty}주 · 평단 {avg_text} · 수익률 미계산(해외 종목 환산은 허브에서 확인)"
    avg_text = _fmt_won(avg) if avg is not None else "-"
    if avg is None or avg <= 0:
        ret = "수익률 미계산(평단 없음)"
    elif krw_price is None:
        ret = "수익률 미계산(시세 없음)"
    else:
        ret = f"수익률 {(krw_price / avg - 1) * 100:+.2f}%"
    return f"보유 {qty}주 · 평단 {avg_text} · {ret}"


def _memo_preview(memo: str, limit: int = 300) -> str:
    lines = [part.strip() for part in str(memo or "").splitlines() if part.strip()]
    text = "\n".join(lines[-3:])
    return text if len(text) <= limit else "…" + text[-limit:]


def _analysis_url(code: str) -> str | None:
    from services.ecosystem import links

    try:
        return links.go_url("value-invest", code=code)
    except (links.LinkError, KeyError, ValueError):
        return None


# --- 조회 ---------------------------------------------------------------------

async def _holdings(google_sub: str) -> list[dict]:
    try:
        return await portfolio_repo.get_portfolio(google_sub)
    except _LOOKUP_ERRORS as exc:
        logger.warning("telegram bot holdings lookup failed user=%s: %s", google_sub[:8], type(exc).__name__)
        return []


async def resolve_query(google_sub: str, query: str) -> tuple[list[dict], bool]:
    """검색어 → ([{code, name}], 정확 일치 여부). 보유 종목을 먼저 본다."""
    q = clean_text(query)
    upper = q.upper()
    holdings = await _holdings(google_sub)
    for item in holdings:
        code = str(item.get("stock_code") or "").upper()
        name = str(item.get("stock_name") or "")
        if upper == code or (name and q.casefold() == name.casefold()):
            return [{"code": code, "name": name or code}], True
    exact = await corp_codes.resolve_corp_search_query(q)
    if exact:
        return [{"code": exact["stock_code"], "name": exact["corp_name"]}], True
    if _CODE_RE.match(upper):
        name = await corp_codes.get_corp_name(upper)
        if name:
            return [{"code": upper, "name": name}], True
    found: list[dict] = []
    seen: set[str] = set()
    for item in holdings:
        name = str(item.get("stock_name") or "")
        code = str(item.get("stock_code") or "").upper()
        if name and q.casefold() in name.casefold() and code not in seen:
            seen.add(code)
            found.append({"code": code, "name": name})
    for row in await corp_codes.search_corp(q):
        code = str(row.get("stock_code") or "").upper()
        if code and code not in seen:
            seen.add(code)
            found.append({"code": code, "name": row.get("corp_name") or code})
    return found[:3], False


async def _quote(code: str) -> dict | None:
    from services import stock_quotes

    try:
        return await asyncio.wait_for(stock_quotes.get_quote_snapshot(code), timeout=10)
    except _LOOKUP_ERRORS as exc:
        logger.info("telegram bot quote failed code=%s: %s", code, type(exc).__name__)
        return None


async def _timeline(code: str) -> dict | None:
    from services.market import stock_timeline

    if not stock_timeline.is_supported_code(code):
        return None
    try:
        return await asyncio.wait_for(stock_timeline.get_stock_timeline(code, limit=2), timeout=12)
    except _LOOKUP_ERRORS as exc:
        logger.info("telegram bot timeline failed code=%s: %s", code, type(exc).__name__)
        return {"items": [], "error": True}


async def build_briefing(google_sub: str, code: str, name: str) -> str:
    holdings = {str(h.get("stock_code") or "").upper(): h for h in await _holdings(google_sub)}
    holding = holdings.get(code)
    if holding and holding.get("stock_name"):
        name = holding["stock_name"]
    quote, timeline = await asyncio.gather(_quote(code), _timeline(code))
    price_text, krw_price = _price_line(code, quote)
    lines = [f"📌 {name} ({code})", price_text]
    if holding:
        lines.append(_holding_line(code, holding, krw_price))
        memo = holding.get("memo") or ""
        if memo:
            lines.append(f"📝 메모\n{_memo_preview(memo)}")
    else:
        lines.append("보유: 없음")
        pref = await user_stocks.get_user_stock_preference(google_sub, code)
        if pref.get("note"):
            lines.append(f"📝 리서치 메모\n{_memo_preview(pref['note'])}")
    if timeline is not None:
        items = (timeline.get("items") or [])[:2]
        if items:
            lines.append("📰 최근 소식")
            for row in items:
                kind = "공시" if row.get("type") == "disclosure" else "뉴스"
                date = str(row.get("date") or "")[5:]
                lines.append(f"· {date} [{kind}] {clean_text(row.get('title'))[:80]}")
                # 원문 링크 — 일반 텍스트의 URL 은 텔레그램이 링크로 바꾼다(미리보기는 꺼 둠).
                link = str(row.get("url") or "").strip()
                if link.startswith("https://") and not any(c.isspace() for c in link):
                    lines.append(f"  {link}")
        elif timeline.get("error"):
            lines.append("📰 최근 소식: 조회 실패")
        else:
            lines.append("📰 최근 소식: 없음")
    url = _analysis_url(code)
    if url:
        lines.append(f"🔗 {url}")
    lines.append("↩️ 이 메시지에 답장하면 메모로 저장돼요.")
    return "\n".join(lines)


async def save_reply_memo(google_sub: str, code: str, content: str) -> tuple[str, str]:
    """답장 내용을 메모에 덧붙인다 → (``holding``|``research``, 저장한 줄)."""
    line = f"{now_kst():%m-%d} {content}"
    async with transaction():
        held, memo = await repo.held_memo(google_sub, code)
        if held:
            saved = await portfolio_metadata.save(google_sub, code, memo=append_memo_line(memo, line, MEMO_MAX_LEN))
            if saved is not None:
                return "holding", line
        pref = await user_stocks.get_user_stock_preference(google_sub, code)
        await user_stocks.save_user_stock_preference(
            google_sub, code, note=append_memo_line(pref.get("note"), line, NOTE_MAX_LEN)
        )
    return "research", line


async def _stock_name(google_sub: str, code: str) -> str:
    for item in await _holdings(google_sub):
        if str(item.get("stock_code") or "").upper() == code and item.get("stock_name"):
            return item["stock_name"]
    try:
        return (await corp_codes.get_corp_name(code)) or code
    except _LOOKUP_ERRORS:
        return code


# --- 업데이트 처리 ---------------------------------------------------------------

async def _reply(token: str, chat_id: str, text: str, reply_to: int | None, *,
                 google_sub: str | None = None, stock_code: str | None = None) -> None:
    message_id = await telegram.send_message_get_id(token, chat_id, text, reply_to_message_id=reply_to)
    if google_sub and stock_code:
        await telegram_refs.remember(google_sub, token, chat_id, message_id, stock_code)


async def handle_update(token: str, chats: dict[str, set[str]], update: dict) -> str:
    """업데이트 하나를 처리하고 결과 태그를 돌려준다(테스트·로그용, 본문 미포함)."""
    msg = update.get("message")
    if not isinstance(msg, dict):
        return "ignored:no_message"
    chat = msg.get("chat") if isinstance(msg.get("chat"), dict) else {}
    sender = msg.get("from") if isinstance(msg.get("from"), dict) else {}
    if chat.get("type") != "private":
        logger.info("telegram bot ignored update: non-private chat type=%s", chat.get("type"))
        return "ignored:not_private"
    if sender.get("is_bot") or sender.get("id") is None or str(sender.get("id")) != str(chat.get("id")):
        logger.info("telegram bot ignored update: sender is a bot or mismatched")
        return "ignored:bot"
    chat_id = str(chat.get("id"))
    subs = chats.get(chat_id)
    if not subs:
        telegram.remember_unregistered_chat(token, chat_id, chat.get("username") or chat.get("first_name") or "")
        logger.info("telegram bot ignored update: unregistered private chat")
        return "ignored:unregistered"
    message_id = msg.get("message_id") if isinstance(msg.get("message_id"), int) else None
    if len(subs) != 1:
        await _reply(token, chat_id, "이 채팅이 여러 계정에 연결돼 있어 처리하지 않았어요. 허브 알림 설정에서 한 계정만 연결해 주세요.", message_id)
        return "ignored:ambiguous"
    google_sub = next(iter(subs))

    raw = msg.get("text")
    if not isinstance(raw, str) or not raw.strip():
        await _reply(token, chat_id, HELP_TEXT, message_id)
        return "help"
    if len(raw) > MAX_INPUT_CHARS:
        await _reply(token, chat_id, f"메시지가 너무 길어요. {MEMO_CONTENT_MAX}자 이내로 보내 주세요.", message_id)
        return "too_long"
    text = clean_text(raw)

    reply_to = msg.get("reply_to_message")
    if isinstance(reply_to, dict):
        target_id = reply_to.get("message_id")
        from_bot = isinstance(reply_to.get("from"), dict) and reply_to["from"].get("is_bot")
        code = None
        if from_bot and isinstance(target_id, int):
            code = await repo.get_message_ref(repo.bot_key(token), chat_id, target_id, google_sub)
        if not code:
            await _reply(token, chat_id, "어느 종목 메시지인지 알 수 없어요. 봇이 보낸 종목 알림이나 브리핑에 답장해 주세요. (30일 지난 메시지는 연결이 끊겨요)", message_id)
            return "reply_unmapped"
        if len(text) > MEMO_CONTENT_MAX:
            await _reply(token, chat_id, f"메모는 {MEMO_CONTENT_MAX}자 이내로 보내 주세요. (지금 {len(text)}자)", message_id)
            return "too_long"
        kind, line = await save_reply_memo(google_sub, code, text)
        name = await _stock_name(google_sub, code)
        if kind == "holding":
            confirm = f"📝 메모 저장됨: {name} ({code})\n{line}"
        else:
            confirm = f"📝 리서치 메모 저장됨: {name} ({code})\n{line}\n(보유하지 않은 종목이라 리서치 메모에 넣었어요)"
        await _reply(token, chat_id, confirm, message_id, google_sub=google_sub, stock_code=code)
        return "memo" if kind == "holding" else "research_note"

    if text.startswith("/"):
        await _reply(token, chat_id, HELP_TEXT, message_id)
        return "help"
    if len(text) > MAX_QUERY_CHARS:
        await _reply(token, chat_id, "종목명이나 코드를 한 줄로 짧게 보내 주세요.\n메모는 종목 메시지에 답장하면 저장돼요.", message_id)
        return "help"

    matches, exact = await resolve_query(google_sub, text)
    if exact or len(matches) == 1:
        top = matches[0]
        briefing = await build_briefing(google_sub, top["code"], top["name"])
        await _reply(token, chat_id, briefing, message_id, google_sub=google_sub, stock_code=top["code"])
        return "briefing"
    if matches:
        lines = ["여러 종목이 있어요. 코드나 정확한 이름으로 다시 보내 주세요."]
        lines += [f"· {m['name']} ({m['code']})" for m in matches[:3]]
        await _reply(token, chat_id, "\n".join(lines), message_id)
        return "candidates"
    await _reply(token, chat_id, "종목을 찾지 못했어요. 종목명이나 6자리 코드로 보내 주세요.", message_id)
    return "not_found"


# --- 수신 루프 -------------------------------------------------------------------

async def load_targets() -> dict[str, dict]:
    """bot_key → {"token", "chats": {chat_id: {google_sub, …}}}."""
    targets: dict[str, dict] = {}
    for ch in await repo.list_bot_channels():
        config = ch["config"]
        token = str(config.get("bot_token") or telegram.default_token() or "").strip()
        chat_id = str(config.get("chat_id") or "").strip()
        if not token or not chat_id:
            continue
        entry = targets.setdefault(repo.bot_key(token), {"token": token, "chats": {}})
        entry["chats"].setdefault(chat_id, set()).add(ch["google_sub"])
    return targets


_BACKLOG_SKIP_MAX_PAGES = 50


async def _notify_failure(token: str, key: str, update: dict) -> None:
    """처리 실패(offset 은 이미 전진)를 등록된 주인에게만 한 번 알린다 — 메모가 조용히 사라지지 않게."""
    msg = update.get("message") if isinstance(update.get("message"), dict) else {}
    chat = msg.get("chat") if isinstance(msg.get("chat"), dict) else {}
    chat_id = str(chat.get("id") or "")
    try:
        chats = (await load_targets()).get(key, {}).get("chats", {})
    except _HANDLE_ERRORS:
        return
    if chat.get("type") != "private" or len(chats.get(chat_id) or ()) != 1:
        return
    await telegram.send_message(token, chat_id, "처리 중 오류가 나서 저장하지 못했어요. 잠시 뒤 다시 보내 주세요.")


async def poll_once(key: str, target: dict, offset: int | None, *, timeout: int = POLL_TIMEOUT_S) -> int | None:
    """getUpdates 한 번 + 처리. 새 offset 을 돌려준다. API 오류는 호출자에게 올린다.

    offset 이 없으면(첫 실행) 쌓여 있던 과거 메시지는 처리하지 않고 건너뛴다.
    """
    updates = await telegram.get_updates(target["token"], offset=offset, timeout=0 if offset is None else timeout)
    if offset is None:
        # getUpdates 는 한 번에 최대 100건 — 쌓인 과거 메시지가 비워질 때까지 건너뛴다.
        skipped, new_offset = 0, 0
        for _ in range(_BACKLOG_SKIP_MAX_PAGES):
            ids = [u["update_id"] for u in updates if isinstance(u.get("update_id"), int) and u["update_id"] >= new_offset]
            if not ids:
                break
            skipped += len(ids)
            new_offset = max(ids) + 1
            updates = await telegram.get_updates(target["token"], offset=new_offset, timeout=0)
        await repo.set_offset(key, new_offset)
        if skipped:
            logger.info("telegram bot bot=%s skipped %d backlog updates on first start", key, skipped)
        return new_offset
    for update in updates:
        update_id = update.get("update_id")
        if not isinstance(update_id, int) or update_id < offset:
            continue
        offset = update_id + 1
        await repo.set_offset(key, offset)  # 처리 전에 저장 — 재시작 후 중복 처리 없음
        try:
            # 연결 해제·비활성화가 바로 반영되도록 처리 직전에 등록 채팅을 다시 읽는다.
            chats = (await load_targets()).get(key, {}).get("chats", {})
            outcome = await handle_update(target["token"], chats, update)
        except _HANDLE_ERRORS as exc:
            logger.warning("telegram bot bot=%s update handling failed: %s", key, type(exc).__name__)
            await _notify_failure(target["token"], key, update)
            continue
        logger.info("telegram bot bot=%s update outcome=%s", key, outcome)
    return offset


async def _poll_worker(key: str, targets: dict[str, dict], stop: asyncio.Event) -> str:
    """토큰 하나의 long polling 루프. 멈춘 사유(webhook/unauthorized/removed/stopped)를 돌려준다."""
    backoff = BACKOFF_MIN_S
    offset = await repo.get_offset(key)
    while not stop.is_set():
        target = targets.get(key)
        if target is None:
            return "removed"
        try:
            offset = await poll_once(key, target, offset)
            backoff = BACKOFF_MIN_S
            continue
        except telegram.TelegramApiError as exc:
            if exc.error_code == 409 and "webhook" in exc.description.lower():
                logger.warning(
                    "telegram bot bot=%s: webhook is set, getUpdates disabled — stopping this bot's polling "
                    "(deleteWebhook 후 재시도됨)", key,
                )
                return "webhook"
            if exc.error_code in (401, 403, 404):
                logger.warning("telegram bot bot=%s: token rejected (%s) — stopping polling", key, exc.error_code)
                return "unauthorized"
            delay = exc.retry_after or backoff
            logger.info("telegram bot bot=%s getUpdates error %s, retry in %.0fs", key, exc.error_code, delay)
        except (httpx.HTTPError, ExternalServiceError, ValueError, TypeError) as exc:
            delay = backoff
            logger.info("telegram bot bot=%s getUpdates failed (%s), retry in %.0fs", key, type(exc).__name__, delay)
        except (DBError, sqlite3.Error) as exc:
            delay = backoff
            logger.warning("telegram bot bot=%s offset store failed (%s), retry in %.0fs", key, type(exc).__name__, delay)
        backoff = min(backoff * 2, BACKOFF_MAX_S)
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except TimeoutError:
            pass
    return "stopped"


async def run_loop(
    stop: asyncio.Event,
    *,
    rescan_interval_seconds: float = RESCAN_INTERVAL_S,
    initial_delay_seconds: float = 20.0,
) -> None:
    """수신 감독 루프 — 등록된 봇 토큰마다 워커를 띄우고, 채널 변경을 주기적으로 반영한다."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=initial_delay_seconds)
        return
    except TimeoutError:
        pass
    targets: dict[str, dict] = {}
    workers: dict[str, asyncio.Task] = {}
    halted: dict[str, float] = {}
    last_prune = 0.0
    try:
        while not stop.is_set():
            try:
                fresh = await load_targets()
            except (DBError, sqlite3.Error, OSError) as exc:
                logger.warning("telegram bot target load failed: %s", type(exc).__name__)
                fresh = targets
            # 기존 dict 를 제자리 갱신 — 워커는 같은 객체에서 최신 chat 목록을 읽는다.
            for key in list(targets):
                if key not in fresh:
                    targets.pop(key)
            targets.update(fresh)
            now = time.monotonic()
            for key, task in list(workers.items()):
                if task.done():
                    workers.pop(key)
                    reason = "error" if task.cancelled() or task.exception() else task.result()
                    if reason in ("webhook", "unauthorized"):
                        halted[key] = now
                    elif reason == "error" and not task.cancelled():
                        logger.warning("telegram bot bot=%s worker crashed: %s", key, type(task.exception()).__name__)
            for key in targets:
                if key in workers:
                    continue
                if key in halted and now - halted[key] < HALT_RETRY_S:
                    continue
                halted.pop(key, None)
                workers[key] = asyncio.create_task(_poll_worker(key, targets, stop), name=f"telegram-bot-{key}")
            if now - last_prune > PRUNE_INTERVAL_S:
                last_prune = now
                try:
                    await repo.prune_message_refs()
                except (DBError, sqlite3.Error) as exc:
                    logger.warning("telegram message ref prune failed: %s", type(exc).__name__)
            try:
                await asyncio.wait_for(stop.wait(), timeout=rescan_interval_seconds)
            except TimeoutError:
                pass
    finally:
        for task in workers.values():
            task.cancel()
        if workers:
            await asyncio.gather(*workers.values(), return_exceptions=True)
