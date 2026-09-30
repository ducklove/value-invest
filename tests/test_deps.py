"""deps 공용 로그인 가드 — 라우트 전역이 공유하는 401 계약."""

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException

import deps


class RequireUserTests(unittest.TestCase):
    def test_require_user_returns_user_or_401(self):
        user = {"google_sub": "u1"}
        self.assertIs(deps.require_user(user), user)
        for missing in (None, {}):
            with self.assertRaises(HTTPException) as ctx:
                deps.require_user(missing)
            self.assertEqual(ctx.exception.status_code, 401)
            self.assertEqual(ctx.exception.detail, "로그인이 필요합니다.")

    def test_require_user_id_returns_google_sub_or_401(self):
        with patch("deps.get_current_user", AsyncMock(return_value={"google_sub": "u1"})):
            self.assertEqual(asyncio.run(deps.require_user_id(object())), "u1")
        with patch("deps.get_current_user", AsyncMock(return_value=None)):
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(deps.require_user_id(object()))
        self.assertEqual(ctx.exception.status_code, 401)
        self.assertEqual(ctx.exception.detail, "로그인이 필요합니다.")


if __name__ == "__main__":
    unittest.main()
