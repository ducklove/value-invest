"""전체 앱의 변경 요청에서 출처와 실제 세션 삭제 여부를 검증한다."""

from dataclasses import replace
from unittest.mock import patch

import httpx
import pytest
from starlette.requests import Request

import auth_service
from core.app_factory import create_app
from core.config import get_settings
from deps import is_secure_request
from repositories import users


@pytest.mark.parametrize("headers,expected", [
    ({"Origin": "https://untrusted.example", "Content-Type": "application/x-www-form-urlencoded"}, 403),
    ({"Origin": "null", "X-Requested-With": "fetch"}, 403),
    ({"Origin": "https://untrusted.example", "Referer": "https://app.example/", "X-Requested-With": "fetch"}, 403),
    ({"Origin": "https://app.example.untrusted.example"}, 403),
    ({"Origin": "https://untrusted.example", "Host": "untrusted.example", "X-Forwarded-Host": "untrusted.example"}, 403),
    ({}, 403),
    ({"Sec-Fetch-Site": "cross-site", "X-Requested-With": "fetch"}, 403),
    ({"Origin": "https://app.example"}, 200),
    ({"Referer": "https://app.example/portfolio"}, 200),
    ({"X-Requested-With": "fetch"}, 200),
])
async def test_logout_requires_a_trusted_browser_request(temp_db, headers, expected):
    with patch("auth_service.session_secret", return_value="request-test-secret"):
        user = await users.create_local_user(email="request@example.test", name="검증", password_hash="unused")
        token = "request-test-token"
        digest = auth_service.hash_session_token(token)
        await users.create_user_session(digest, user["google_sub"], auth_service.session_expiry_iso())
        settings = replace(get_settings(), cors_allowed_origins=("https://app.example",))
        app = create_app(settings)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://api.example",
                                    cookies={auth_service.SESSION_COOKIE_NAME: token}) as client:
            response = await client.post("/api/auth/logout", headers=headers)
        assert response.status_code == expected
        assert bool(await users.get_user_by_session(digest)) is (expected == 403)


async def test_preflight_and_cookie_free_internal_clients_are_preserved():
    settings = replace(get_settings(), cors_allowed_origins=("https://app.example",))
    app = create_app(settings)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://api.example") as client:
        response = await client.options("/api/portfolio/005930", headers={
            "Origin": "https://app.example", "Access-Control-Request-Method": "PUT",
            "Access-Control-Request-Headers": "content-type,x-requested-with",
        })
        assert response.status_code == 200
        assert response.headers["access-control-allow-origin"] == "https://app.example"
        assert (await client.post("/api/auth/logout")).status_code == 200
        # 독립 CSRF 검증을 수행하는 Google 콜백까지 공통 검사로 차단하지 않는다.
        with patch("auth_service.is_google_enabled", return_value=True):
            callback = await client.post("/api/auth/google/callback", headers={"Origin": "https://accounts.google.com"})
        assert callback.status_code == 303
        assert "auth_error=csrf" in callback.headers["location"]


def test_raw_forwarded_proto_cannot_change_session_cookie_security():
    request = Request({"type": "http", "scheme": "http", "headers": [(b"x-forwarded-proto", b"https")],
                       "server": ("app.example", 80), "path": "/", "query_string": b""})
    assert is_secure_request(request) is False
