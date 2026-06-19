import unittest
from unittest.mock import AsyncMock, patch

from fastapi import HTTPException, Response
from starlette.requests import Request

import auth_service
from _harness import TempDbMixin
from repositories import users as users_repo
from routes import auth as auth_route


def _request(path: str = "/api/auth/register") -> Request:
    return Request({
        "type": "http",
        "method": "POST",
        "path": path,
        "headers": [],
        "query_string": b"",
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
        "scheme": "http",
    })


class PasswordAuthTests(TempDbMixin):
    async def test_register_and_login_with_email_password(self):
        with patch("auth_service.session_secret", return_value="test-secret"):
            response = Response()
            registered = await auth_route.auth_register(
                _request(),
                response,
                {"email": "USER@Example.com", "name": "Local User", "password": "strong-pass"},
            )

            self.assertTrue(registered["user"]["password_set"])
            self.assertFalse(registered["user"]["google_linked"])
            self.assertEqual(registered["user"]["email"], "user@example.com")
            self.assertIn(auth_service.SESSION_COOKIE_NAME, response.headers.get("set-cookie", ""))

            login_response = Response()
            logged_in = await auth_route.auth_password_login(
                _request("/api/auth/password/login"),
                login_response,
                {"email": "user@example.com", "password": "strong-pass"},
            )

        self.assertEqual(logged_in["user"]["google_sub"], registered["user"]["google_sub"])
        self.assertIn(auth_service.SESSION_COOKIE_NAME, login_response.headers.get("set-cookie", ""))

    async def test_google_user_can_register_password_and_login_both_ways(self):
        with patch("auth_service.session_secret", return_value="test-secret"):
            google_user = await users_repo.upsert_user({
                "google_sub": "google-sub-1",
                "email": "google@example.com",
                "name": "Google User",
                "picture": "",
                "email_verified": True,
            })

            with patch("routes.auth.get_current_user", AsyncMock(return_value=google_user)):
                updated = await auth_route.auth_update_password(
                    _request("/api/auth/me/password"),
                    {"new_password": "google-pass-1"},
                )

            self.assertTrue(updated["user"]["password_set"])
            self.assertTrue(updated["user"]["google_linked"])

            login_response = Response()
            logged_in = await auth_route.auth_password_login(
                _request("/api/auth/password/login"),
                login_response,
                {"email": "google@example.com", "password": "google-pass-1"},
            )

        self.assertEqual(logged_in["user"]["google_sub"], google_user["google_sub"])

    async def test_password_change_requires_current_password_when_already_set(self):
        with patch("auth_service.session_secret", return_value="test-secret"):
            user = await users_repo.create_local_user(
                email="change@example.com",
                name="Change Me",
                password_hash=auth_service.hash_password("old-pass-1"),
            )

            with patch("routes.auth.get_current_user", AsyncMock(return_value=user)):
                with self.assertRaises(HTTPException) as exc_info:
                    await auth_route.auth_update_password(
                        _request("/api/auth/me/password"),
                        {"new_password": "new-pass-1"},
                    )
                self.assertEqual(exc_info.exception.status_code, 400)

                updated = await auth_route.auth_update_password(
                    _request("/api/auth/me/password"),
                    {"current_password": "old-pass-1", "new_password": "new-pass-1"},
                )

            self.assertTrue(updated["user"]["password_set"])

            with self.assertRaises(HTTPException):
                await auth_route.auth_password_login(
                    _request("/api/auth/password/login"),
                    Response(),
                    {"email": "change@example.com", "password": "old-pass-1"},
                )

            logged_in = await auth_route.auth_password_login(
                _request("/api/auth/password/login"),
                Response(),
                {"email": "change@example.com", "password": "new-pass-1"},
            )

        self.assertEqual(logged_in["user"]["google_sub"], user["google_sub"])

    async def test_signup_cannot_claim_existing_google_email(self):
        with patch("auth_service.session_secret", return_value="test-secret"):
            await users_repo.upsert_user({
                "google_sub": "google-sub-2",
                "email": "claimed@example.com",
                "name": "Claimed",
                "picture": "",
                "email_verified": True,
            })

            with self.assertRaises(HTTPException) as exc_info:
                await auth_route.auth_register(
                    _request(),
                    Response(),
                    {"email": "claimed@example.com", "name": "Other", "password": "strong-pass"},
                )

        self.assertEqual(exc_info.exception.status_code, 409)


if __name__ == "__main__":
    unittest.main()
