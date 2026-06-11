"""POST /api/internal/notify — 연결 프로젝트 공용 알림 발송 엔드포인트."""

import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException, Request

from routes import internal


def _request(path: str = "/api/internal/notify", headers: dict[str, str] | None = None,
             client_host: str = "127.0.0.1") -> Request:
    encoded = [
        (k.lower().encode("latin-1"), v.encode("latin-1"))
        for k, v in (headers or {}).items()
    ]
    scope = {
        "type": "http",
        "method": "POST",
        "path": path,
        "headers": encoded,
        "query_string": b"",
        "client": (client_host, 12345),
        "server": ("testserver", 80),
    }
    return Request(scope)


class InternalNotifyTests(unittest.IsolatedAsyncioTestCase):
    async def test_rejects_forwarded_request_without_token(self):
        request = _request(headers={"X-Forwarded-For": "203.0.113.10"})
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(HTTPException) as exc_info:
                await internal.send_notification(request, payload={"text": "hi"})
        self.assertEqual(exc_info.exception.status_code, 403)

    async def test_accepts_cross_host_caller_with_internal_token(self):
        # finance-pi 처럼 다른 호스트의 서브프로젝트는 토큰 헤더로 인증한다.
        request = _request(
            headers={"X-Forwarded-For": "203.0.113.10", "X-Internal-Token": "secret"},
            client_host="192.168.68.84",
        )
        with patch.dict("os.environ", {"INTERNAL_API_TOKEN": "secret"}, clear=True), \
             patch("repositories.users.get_all_users", new=AsyncMock(return_value=[{"google_sub": "u1"}])), \
             patch("services.notifications.channels.dispatch", new=AsyncMock(return_value=1)):
            result = await internal.send_notification(request, payload={"text": "hi"})
        self.assertEqual(result, {"ok": True, "sent": 1, "users": 1})

    async def test_requires_text(self):
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaises(HTTPException) as exc_info:
                await internal.send_notification(_request(), payload={"text": "  "})
        self.assertEqual(exc_info.exception.status_code, 400)

    async def test_broadcasts_to_all_users_and_counts_only_reached_ones(self):
        users = [{"google_sub": "u1"}, {"google_sub": "u2"}]
        dispatch = AsyncMock(side_effect=[2, 0])  # u2 는 활성 채널 없음 → 0건
        with patch.dict("os.environ", {}, clear=True), \
             patch("repositories.users.get_all_users", new=AsyncMock(return_value=users)), \
             patch("services.notifications.channels.dispatch", new=dispatch):
            result = await internal.send_notification(_request(), payload={
                "text": "금 시세 괴리 5% 초과",
                "title": "gold_gap 알림",
                "source": "gold_gap",
            })

        self.assertEqual(result, {"ok": True, "sent": 2, "users": 1})
        self.assertEqual([c.args[0] for c in dispatch.await_args_list], ["u1", "u2"])
        message = dispatch.await_args_list[0].args[1]
        self.assertEqual(message, "📌 gold_gap 알림\n금 시세 괴리 5% 초과\n— gold_gap")

    async def test_explicit_google_sub_targets_single_user(self):
        dispatch = AsyncMock(return_value=1)
        get_all = AsyncMock()
        with patch.dict("os.environ", {}, clear=True), \
             patch("repositories.users.get_all_users", new=get_all), \
             patch("services.notifications.channels.dispatch", new=dispatch):
            result = await internal.send_notification(_request(), payload={
                "text": "hi", "google_sub": "u7",
            })

        self.assertEqual(result, {"ok": True, "sent": 1, "users": 1})
        dispatch.assert_awaited_once_with("u7", "hi")
        get_all.assert_not_awaited()  # 대상 지정 시 전체 사용자 조회 없음

    async def test_truncates_overlong_message_under_telegram_limit(self):
        dispatch = AsyncMock(return_value=1)
        with patch.dict("os.environ", {}, clear=True), \
             patch("repositories.users.get_all_users", new=AsyncMock(return_value=[{"google_sub": "u1"}])), \
             patch("services.notifications.channels.dispatch", new=dispatch):
            await internal.send_notification(_request(), payload={"text": "x" * 5000})

        message = dispatch.await_args.args[1]
        self.assertEqual(len(message), internal._NOTIFY_TEXT_MAX)
        self.assertTrue(message.endswith("…"))


if __name__ == "__main__":
    unittest.main()
