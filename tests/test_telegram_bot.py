"""양방향 텔레그램 비서 — 수신 보안, 브리핑, 답장 메모, offset 영속, 409 처리.

텔레그램 API 는 전부 목(``telegram.get_updates`` / ``telegram.send_message_get_id``).
"""

import asyncio
from datetime import datetime
from itertools import count
from unittest.mock import AsyncMock, patch

from repositories import notifications as notifications_repo
from repositories import portfolio as portfolio_repo
from repositories import telegram_bot as repo
from repositories import user_stocks
from repositories.db import get_db
from services.notifications import alert_delivery, channels, telegram, telegram_bot, telegram_refs
from tests._harness import TempDbMixin, seed_corp_codes, seed_user

TOKEN = "123456:ABCdef_token"
KEY = repo.bot_key(TOKEN)
CHAT = "5550001"
FIXED_NOW = datetime(2026, 11, 28, 10, 0, 0)


def _msg(text, *, chat_id=CHAT, chat_type="private", sender_id=None, is_bot=False, reply_to=None, message_id=10):
    msg = {
        "message_id": message_id,
        "chat": {"id": int(chat_id), "type": chat_type},
        "from": {"id": int(sender_id if sender_id is not None else chat_id), "is_bot": is_bot},
        "text": text,
    }
    if reply_to is not None:
        msg["reply_to_message"] = {"message_id": reply_to, "from": {"id": 999, "is_bot": True}}
    return {"update_id": 1, "message": msg}


class TelegramBotTestBase(TempDbMixin):
    async def seed(self):
        await seed_user("u1")
        await seed_user("u2", email="u2@example.com")
        await seed_corp_codes([
            ("005930", "00126380", "삼성전자", "t"),
            ("005935", "00126381", "삼성전자우", "t"),
            ("028260", "00126382", "삼성물산", "t"),
            ("032830", "00126383", "삼성생명", "t"),
            ("000660", "00164779", "SK하이닉스", "t"),
        ])
        await notifications_repo.upsert_notification_channel(
            "u1", "telegram", config={"bot_token": TOKEN, "chat_id": CHAT}, enabled=True, verified=True,
        )
        self.ids = count(500)
        self.sent: list[dict] = []

        async def fake_send(token, chat_id, text, *, reply_to_message_id=None):
            mid = next(self.ids)
            self.sent.append({"token": token, "chat_id": chat_id, "text": text, "reply_to": reply_to_message_id, "id": mid})
            return mid

        self.patches = [
            patch.object(telegram, "send_message_get_id", new=fake_send),
            patch.object(telegram_bot, "_quote", new=AsyncMock(return_value={"price": 72000.0, "change": 850.0, "change_pct": 1.19})),
            patch.object(telegram_bot, "_timeline", new=AsyncMock(return_value={"items": [
                {"type": "disclosure", "date": "2026-11-27", "title": "주요사항보고서(자기주식취득결정)",
                 "url": "https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261127000001"},
                {"type": "news", "date": "2026-11-27", "title": "삼성전자, 신제품 발표",
                 "url": "javascript:alert(1)"},
                {"type": "news", "date": "2026-11-26", "title": "세 번째 기사는 보이지 않음"},
            ]})),
            patch.object(telegram_bot, "now_kst", new=lambda: FIXED_NOW),
        ]
        for p in self.patches:
            p.start()

    async def asyncTearDown(self):
        for p in self.patches:
            p.stop()
        await super().asyncTearDown()

    async def chats(self):
        return (await telegram_bot.load_targets())[KEY]["chats"]

    async def handle(self, update):
        return await telegram_bot.handle_update(TOKEN, await self.chats(), update)

    async def add_holding(self, code="005930", name="삼성전자", qty=10, avg=65000, memo=None):
        await portfolio_repo.save_portfolio_item("u1", code, name, qty, avg, memo=memo)


class SecurityTests(TelegramBotTestBase):
    async def test_unregistered_chat_is_ignored_but_remembered_for_linking(self):
        outcome = await self.handle(_msg("삼성전자", chat_id="777"))
        self.assertEqual(outcome, "ignored:unregistered")
        self.assertEqual(self.sent, [])
        # 채널 연결 화면: getUpdates 가 비어도 루프가 본 미등록 개인 채팅으로 보완.
        with patch.object(telegram, "_api", new=AsyncMock(return_value=[])):
            self.assertEqual((await telegram.get_recent_chat_id(TOKEN))[0], "777")

    async def test_two_strangers_block_auto_linking(self):
        # 봇 이름을 아는 다른 사람이 먼저 말을 걸어 둔 경우 — 누가 주인인지 모르므로 자동 연결 안 함.
        telegram._recent_unregistered.clear()
        await self.handle(_msg("안녕", chat_id="777"))
        await self.handle(_msg("나도", chat_id="888"))
        with patch.object(telegram, "_api", new=AsyncMock(return_value=[])):
            self.assertIsNone(await telegram.get_recent_chat_id(TOKEN))
        telegram._recent_unregistered.clear()

    async def test_get_updates_with_several_chats_does_not_auto_link(self):
        updates = [_msg("a", chat_id="777"), _msg("b", chat_id="888")]
        with patch.object(telegram, "_api", new=AsyncMock(return_value=updates)):
            self.assertIsNone(await telegram.get_recent_chat_id(TOKEN))
        with patch.object(telegram, "_api", new=AsyncMock(return_value=[updates[0], _msg("c", chat_id="777")])):
            self.assertEqual((await telegram.get_recent_chat_id(TOKEN))[0], "777")

    async def test_group_and_channel_messages_are_ignored(self):
        self.assertEqual(await self.handle(_msg("삼성전자", chat_type="group")), "ignored:not_private")
        self.assertEqual(await self.handle(_msg("삼성전자", chat_type="supergroup")), "ignored:not_private")
        channel_post = {"update_id": 2, "channel_post": {"chat": {"id": int(CHAT), "type": "channel"}, "text": "x"}}
        self.assertEqual(await self.handle(channel_post), "ignored:no_message")
        self.assertEqual(self.sent, [])

    async def test_bot_sender_and_mismatched_sender_are_ignored(self):
        self.assertEqual(await self.handle(_msg("삼성전자", is_bot=True)), "ignored:bot")
        self.assertEqual(await self.handle(_msg("삼성전자", sender_id=42)), "ignored:bot")
        self.assertEqual(self.sent, [])

    async def test_disabled_or_unverified_channel_is_not_a_target(self):
        await notifications_repo.set_notification_channel_enabled("u1", "telegram", False)
        self.assertEqual(await telegram_bot.load_targets(), {})

    async def test_chat_linked_to_two_accounts_is_not_processed(self):
        await notifications_repo.upsert_notification_channel(
            "u2", "telegram", config={"bot_token": TOKEN, "chat_id": CHAT}, enabled=True, verified=True,
        )
        await self.add_holding(memo="u1 비밀 메모")
        outcome = await self.handle(_msg("삼성전자"))
        self.assertEqual(outcome, "ignored:ambiguous")
        self.assertNotIn("비밀", self.sent[0]["text"])

    async def test_shared_bot_routes_each_chat_to_its_own_user(self):
        await notifications_repo.upsert_notification_channel(
            "u2", "telegram", config={"bot_token": TOKEN, "chat_id": "6660002"}, enabled=True, verified=True,
        )
        await self.add_holding(memo="u1 전용 메모")
        await self.handle(_msg("삼성전자", chat_id="6660002"))
        self.assertIn("보유: 없음", self.sent[-1]["text"])
        self.assertNotIn("u1 전용 메모", self.sent[-1]["text"])

    async def test_reply_to_other_users_message_ref_is_not_used(self):
        await self.add_holding()
        await repo.save_message_ref(KEY, CHAT, 300, "u2", "005930")
        outcome = await self.handle(_msg("메모", reply_to=300))
        self.assertEqual(outcome, "reply_unmapped")
        self.assertIsNone((await portfolio_repo.get_portfolio("u1"))[0]["memo"])


class BriefingTests(TelegramBotTestBase):
    async def test_held_stock_briefing(self):
        await self.add_holding(memo="11-20 배당 확인")
        outcome = await self.handle(_msg("삼성전자"))
        self.assertEqual(outcome, "briefing")
        text = self.sent[0]["text"]
        self.assertIn("삼성전자 (005930)", text)
        self.assertIn("현재가 72,000원 ▲1.19% (+850)", text)
        self.assertIn("보유 10주 · 평단 65,000원 · 수익률 +10.77%", text)
        self.assertIn("11-20 배당 확인", text)
        self.assertIn("[공시] 주요사항보고서", text)
        self.assertIn("[뉴스] 삼성전자, 신제품 발표", text)
        self.assertNotIn("세 번째 기사", text)
        # 원문 링크는 https 만, 해당 소식 바로 아래 줄에.
        lines = text.splitlines()
        disc = next(i for i, ln in enumerate(lines) if "[공시]" in ln)
        self.assertEqual(lines[disc + 1].strip(), "https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20261127000001")
        self.assertNotIn("javascript:", text)
        self.assertIn("/analysis?code=005930", text)
        self.assertEqual(self.sent[0]["reply_to"], 10)
        # 브리핑 메시지에 답장하면 메모가 되도록 매핑 저장.
        self.assertEqual(await repo.get_message_ref(KEY, CHAT, self.sent[0]["id"], "u1"), "005930")

    async def test_not_held_stock_briefing_by_code(self):
        outcome = await self.handle(_msg("000660"))
        self.assertEqual(outcome, "briefing")
        text = self.sent[0]["text"]
        self.assertIn("SK하이닉스 (000660)", text)
        self.assertIn("보유: 없음", text)
        self.assertNotIn("수익률", text)

    async def test_quote_failure_does_not_invent_numbers(self):
        await self.add_holding()
        with patch.object(telegram_bot, "_quote", new=AsyncMock(return_value=None)):
            await self.handle(_msg("005930"))
        text = self.sent[0]["text"]
        self.assertIn("현재가: 미확인", text)
        self.assertIn("수익률 미계산(시세 없음)", text)

    async def test_ambiguous_name_lists_top_three(self):
        outcome = await self.handle(_msg("삼성"))
        self.assertEqual(outcome, "candidates")
        lines = [line for line in self.sent[0]["text"].splitlines() if line.startswith("· ")]
        self.assertEqual(len(lines), 3)

    async def test_unknown_name(self):
        self.assertEqual(await self.handle(_msg("없는회사이름")), "not_found")

    async def test_long_text_without_reply_is_not_searched(self):
        self.assertEqual(await self.handle(_msg("가" * 40)), "help")


class ReplyMemoTests(TelegramBotTestBase):
    async def test_reply_appends_dated_line_to_holding_memo(self):
        await self.add_holding(memo="11-20 배당 확인")
        await repo.save_message_ref(KEY, CHAT, 300, "u1", "005930")
        outcome = await self.handle(_msg("  자사주   매입\n발표 확인 ", reply_to=300))
        self.assertEqual(outcome, "memo")
        memo = (await portfolio_repo.get_portfolio("u1"))[0]["memo"]
        self.assertEqual(memo, "11-20 배당 확인\n11-28 자사주 매입 발표 확인")
        self.assertIn("📝 메모 저장됨", self.sent[0]["text"])
        # 확인 메시지에 다시 답장해도 같은 종목으로 이어진다.
        self.assertEqual(await repo.get_message_ref(KEY, CHAT, self.sent[0]["id"], "u1"), "005930")

    async def test_reply_trims_oldest_lines_over_500_chars(self):
        old_lines = [f"11-{i:02d} " + "가" * 90 for i in range(1, 6)]  # 5 × 96자 + 줄바꿈 = 484자
        await self.add_holding(memo="\n".join(old_lines))
        await repo.save_message_ref(KEY, CHAT, 300, "u1", "005930")
        await self.handle(_msg("나" * 60, reply_to=300))
        memo = (await portfolio_repo.get_portfolio("u1"))[0]["memo"]
        self.assertLessEqual(len(memo), portfolio_repo.MEMO_MAX_LEN)
        self.assertTrue(memo.endswith("11-28 " + "나" * 60))
        self.assertNotIn("11-01 ", memo)
        self.assertIn("11-05 ", memo)

    async def test_reply_on_not_held_stock_goes_to_research_note(self):
        await repo.save_message_ref(KEY, CHAT, 300, "u1", "000660")
        outcome = await self.handle(_msg("HBM 점유율 체크", reply_to=300))
        self.assertEqual(outcome, "research_note")
        pref = await user_stocks.get_user_stock_preference("u1", "000660")
        self.assertEqual(pref["note"], "11-28 HBM 점유율 체크")
        self.assertIn("리서치 메모", self.sent[0]["text"])

    async def test_reply_without_mapping_gets_guidance(self):
        await self.add_holding()
        outcome = await self.handle(_msg("메모", reply_to=12345))
        self.assertEqual(outcome, "reply_unmapped")
        self.assertIn("어느 종목", self.sent[0]["text"])
        self.assertIsNone((await portfolio_repo.get_portfolio("u1"))[0]["memo"])

    async def test_too_long_memo_is_rejected(self):
        await self.add_holding()
        await repo.save_message_ref(KEY, CHAT, 300, "u1", "005930")
        outcome = await self.handle(_msg("가" * (telegram_bot.MEMO_CONTENT_MAX + 1), reply_to=300))
        self.assertEqual(outcome, "too_long")
        self.assertIsNone((await portfolio_repo.get_portfolio("u1"))[0]["memo"])

    def test_append_memo_line(self):
        self.assertEqual(telegram_bot.append_memo_line(None, "11-28 a", 500), "11-28 a")
        self.assertEqual(telegram_bot.append_memo_line("x\n\n y ", "z", 500), "x\ny\nz")
        self.assertEqual(telegram_bot.append_memo_line("aaaa\nbbbb", "cccc", 9), "bbbb\ncccc")


class PollingTests(TelegramBotTestBase):
    async def test_first_start_skips_backlog_then_offset_persists_across_restart(self):
        await self.add_holding()
        target = (await telegram_bot.load_targets())[KEY]
        backlog = [dict(_msg("삼성전자"), update_id=40)]
        with patch.object(telegram, "get_updates", new=AsyncMock(return_value=backlog)):
            offset = await telegram_bot.poll_once(KEY, target, None)
        self.assertEqual(offset, 41)
        self.assertEqual(self.sent, [])  # 과거 메시지는 처리하지 않음

        await repo.save_message_ref(KEY, CHAT, 300, "u1", "005930")
        new = [dict(_msg("첫 메모", reply_to=300), update_id=41)]
        with patch.object(telegram, "get_updates", new=AsyncMock(return_value=new)):
            offset = await telegram_bot.poll_once(KEY, target, await repo.get_offset(KEY))
        self.assertEqual(offset, 42)
        self.assertEqual(await repo.get_offset(KEY), 42)

        # 재시작: Telegram 이 확인 전 업데이트를 다시 보내도 영속 offset 으로 건너뛴다.
        with patch.object(telegram, "get_updates", new=AsyncMock(return_value=new)) as gu:
            await telegram_bot.poll_once(KEY, target, await repo.get_offset(KEY))
        self.assertEqual(gu.await_args.kwargs["offset"], 42)
        memo = (await portfolio_repo.get_portfolio("u1"))[0]["memo"]
        self.assertEqual(memo, "11-28 첫 메모")

    async def test_first_start_skips_every_backlog_page(self):
        target = (await telegram_bot.load_targets())[KEY]
        page1 = [dict(_msg("old"), update_id=i) for i in range(1, 101)]
        page2 = [dict(_msg("old"), update_id=i) for i in range(101, 121)]
        with patch.object(telegram, "get_updates", new=AsyncMock(side_effect=[page1, page2, []])):
            offset = await telegram_bot.poll_once(KEY, target, None)
        self.assertEqual(offset, 121)
        self.assertEqual(self.sent, [])

    async def test_unlinked_chat_stops_being_served_within_the_same_poll(self):
        await self.add_holding()
        target = (await telegram_bot.load_targets())[KEY]  # 연결된 상태로 시작한 폴링
        await notifications_repo.upsert_notification_channel(
            "u1", "telegram", config={"bot_token": TOKEN, "chat_id": CHAT}, enabled=False, verified=True,
        )
        with patch.object(telegram, "get_updates", new=AsyncMock(return_value=[dict(_msg("삼성전자"), update_id=5)])):
            await telegram_bot.poll_once(KEY, target, 5)
        self.assertEqual(self.sent, [])

    async def test_handling_failure_tells_the_owner(self):
        target = (await telegram_bot.load_targets())[KEY]
        plain = AsyncMock(return_value=True)
        with patch.object(telegram_bot, "handle_update", new=AsyncMock(side_effect=ValueError("boom"))), \
             patch.object(telegram, "send_message", new=plain), \
             patch.object(telegram, "get_updates", new=AsyncMock(return_value=[dict(_msg("메모"), update_id=7)])):
            await telegram_bot.poll_once(KEY, target, 7)
        self.assertEqual(plain.await_count, 1)
        self.assertIn("저장하지 못했어요", plain.await_args.args[2])

    async def test_webhook_conflict_stops_worker(self):
        targets = await telegram_bot.load_targets()
        await repo.set_offset(KEY, 5)
        err = telegram.TelegramApiError(
            "getUpdates", 409, "Conflict: can't use getUpdates method while webhook is active; use deleteWebhook",
        )
        with patch.object(telegram, "get_updates", new=AsyncMock(side_effect=err)):
            reason = await asyncio.wait_for(telegram_bot._poll_worker(KEY, targets, asyncio.Event()), timeout=2)
        self.assertEqual(reason, "webhook")

    async def test_other_conflict_backs_off_and_retries(self):
        targets = await telegram_bot.load_targets()
        await repo.set_offset(KEY, 5)
        stop = asyncio.Event()
        calls = []

        async def fake_get_updates(token, *, offset, timeout):
            calls.append(offset)
            if len(calls) == 1:
                raise telegram.TelegramApiError("getUpdates", 409, "Conflict: terminated by other getUpdates request")
            stop.set()
            return []

        with patch.object(telegram, "get_updates", new=fake_get_updates), \
             patch.object(telegram_bot, "BACKOFF_MIN_S", 0.01):
            reason = await asyncio.wait_for(telegram_bot._poll_worker(KEY, targets, stop), timeout=2)
        self.assertEqual(reason, "stopped")
        self.assertEqual(calls, [5, 5])

    async def test_unauthorized_token_stops_worker(self):
        targets = await telegram_bot.load_targets()
        await repo.set_offset(KEY, 5)
        err = telegram.TelegramApiError("getUpdates", 401, "Unauthorized")
        with patch.object(telegram, "get_updates", new=AsyncMock(side_effect=err)):
            reason = await asyncio.wait_for(telegram_bot._poll_worker(KEY, targets, asyncio.Event()), timeout=2)
        self.assertEqual(reason, "unauthorized")

    async def test_run_loop_cancels_workers_on_stop(self):
        stop = asyncio.Event()
        started = asyncio.Event()

        async def hanging_get_updates(token, *, offset, timeout):
            started.set()
            await asyncio.sleep(3600)

        with patch.object(telegram, "get_updates", new=hanging_get_updates):
            task = asyncio.create_task(telegram_bot.run_loop(stop, initial_delay_seconds=0, rescan_interval_seconds=60))
            await asyncio.wait_for(started.wait(), timeout=2)
            stop.set()
            await asyncio.wait_for(task, timeout=2)

    async def test_prune_old_refs(self):
        await repo.save_message_ref(KEY, CHAT, 1, "u1", "005930")
        db = await get_db()
        await db.execute("UPDATE telegram_message_refs SET created_at = '2020-01-01T00:00:00'")
        await db.commit()
        await repo.save_message_ref(KEY, CHAT, 2, "u1", "005930")
        self.assertEqual(await repo.prune_message_refs(), 1)
        self.assertIsNone(await repo.get_message_ref(KEY, CHAT, 1, "u1"))
        self.assertEqual(await repo.get_message_ref(KEY, CHAT, 2, "u1"), "005930")


class DispatchRefTests(TelegramBotTestBase):
    async def test_stock_alert_dispatch_records_message_ref(self):
        api = AsyncMock(return_value={"message_id": 901})
        quiet_off = {**alert_delivery.DEFAULT_SETTINGS, "enabled": False}
        with patch.object(alert_delivery, "get_settings", new=AsyncMock(return_value=quiet_off)), \
             patch.object(telegram, "_api", new=api):
            await alert_delivery.dispatch("u1", 1, "📢 [삼성전자] 신규 공시", stock_code="005930")
        api.assert_awaited_once()
        self.assertEqual(await repo.get_message_ref(KEY, CHAT, 901, "u1"), "005930")
        self.assertIsNone(telegram_refs.current_stock_code())

    async def test_non_stock_dispatch_records_nothing(self):
        with patch.object(telegram, "_api", new=AsyncMock(return_value={"message_id": 902})):
            self.assertEqual(await channels.dispatch("u1", "포트폴리오 알림"), 1)
        self.assertIsNone(await repo.get_message_ref(KEY, CHAT, 902, "u1"))

    async def test_explicit_stock_code_argument(self):
        with patch.object(telegram, "_api", new=AsyncMock(return_value={"message_id": 903})):
            self.assertEqual(await channels.dispatch("u1", "가격 알림", stock_code="000660"), 1)
        self.assertEqual(await repo.get_message_ref(KEY, CHAT, 903, "u1"), "000660")

    async def test_mocked_plain_send_still_counts_as_sent(self):
        # 기존 테스트처럼 send_message 를 목으로 바꿔도 종목 알림 발송 수는 그대로.
        with patch.object(telegram, "send_message", new=AsyncMock(return_value=True)):
            self.assertEqual(await channels.dispatch("u1", "가격 알림", stock_code="000660"), 1)
