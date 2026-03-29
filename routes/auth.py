import json
import logging
from urllib.parse import parse_qs, parse_qsl, urlencode, urlparse, urlunparse

from fastapi import APIRouter, Body, HTTPException, Request, Response
from fastapi.responses import RedirectResponse

import auth_service
import cache
from deps import (
    SESSION_COOKIE_NAME,
    TRUSTED_RETURN_ORIGINS,
    clear_session_cookie,
    get_current_user,
    serialize_user,
    set_session_cookie,
)

logger = logging.getLogger(__name__)
router = APIRouter()


def _normalize_return_to(value: str | None) -> str:
    if not value:
        return "/"
    parsed = urlparse(value)
    if not parsed.scheme and value.startswith("/"):
        return value
    origin = f"{parsed.scheme}://{parsed.netloc}"
    if origin in TRUSTED_RETURN_ORIGINS:
        return value
    return "/"


def _append_query_value(url: str, key: str, value: str) -> str:
    parsed = urlparse(url)
    query_pairs = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if k != key]
    query_pairs.append((key, value))
    return urlunparse(parsed._replace(query=urlencode(query_pairs)))


async def _read_post_fields(request: Request) -> dict[str, str]:
    body = (await request.body()).decode("utf-8", errors="ignore")
    parsed = parse_qs(body, keep_blank_values=True)
    return {key: values[-1] for key, values in parsed.items() if values}


def _render_login_page(return_to: str | None) -> str:
    normalized_return_to = _normalize_return_to(return_to)
    escaped_return_to = json.dumps(normalized_return_to, ensure_ascii=False)
    escaped_google_client_id = json.dumps(auth_service.public_config()["google_client_id"], ensure_ascii=False)
    auth_url = json.dumps("/api/auth/google", ensure_ascii=False)
    home_url = json.dumps(normalized_return_to or "/", ensure_ascii=False)
    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Google 로그인</title>
  <script src="https://accounts.google.com/gsi/client" async defer></script>
  <style>
    body {{
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: #f5f7fb;
      color: #111827;
      min-height: 100vh;
      display: grid;
      place-items: center;
      padding: 24px;
    }}
    .card {{
      width: min(100%, 420px);
      background: white;
      border: 1px solid #e5e7eb;
      border-radius: 20px;
      box-shadow: 0 18px 50px rgba(15, 23, 42, 0.08);
      padding: 28px;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 24px;
    }}
    p {{
      margin: 0;
      color: #4b5563;
      line-height: 1.6;
      font-size: 14px;
    }}
    .actions {{
      display: grid;
      gap: 12px;
      margin-top: 20px;
    }}
    .back-link {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 44px;
      border-radius: 999px;
      border: 1px solid #d1d5db;
      text-decoration: none;
      color: #111827;
      font-weight: 600;
    }}
    .note {{
      margin-top: 14px;
      font-size: 12px;
      color: #6b7280;
    }}
  </style>
</head>
<body>
  <main class="card">
    <h1>Google 로그인</h1>
    <p>로그인하면 최근 분석 종목, 관심종목, 핀 고정, 메모를 계정 기준으로 저장합니다.</p>
    <div class="actions">
      <div id="googleSignInButton"></div>
      <a class="back-link" href={home_url}>분석 화면으로 돌아가기</a>
    </div>
    <div class="note">버튼이 보이지 않으면 브라우저의 Google 계정 스크립트 차단 여부를 확인하세요.</div>
  </main>
  <script>
    const RETURN_TO = {escaped_return_to};
    const GOOGLE_CLIENT_ID = {escaped_google_client_id};
    const AUTH_URL = {auth_url};

    async function submitGoogleCredential(credential) {{
      const resp = await fetch(AUTH_URL, {{
        method: 'POST',
        credentials: 'include',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify({{ credential }}),
      }});
      const data = await resp.json().catch(() => ({{}}));
      if (!resp.ok) {{
        throw new Error(data.detail || 'Google 로그인에 실패했습니다.');
      }}
      return data;
    }}

    async function handleGoogleCredentialResponse(response) {{
      if (!response?.credential) {{
        alert('Google 로그인 토큰을 받지 못했습니다.');
        return;
      }}
      try {{
        await submitGoogleCredential(response.credential);
        window.location.replace(RETURN_TO || '/');
      }} catch (error) {{
        alert(error.message || 'Google 로그인에 실패했습니다.');
      }}
    }}

    function renderLoginButton() {{
      if (!GOOGLE_CLIENT_ID || !window.google?.accounts?.id) {{
        window.setTimeout(renderLoginButton, 250);
        return;
      }}
      window.google.accounts.id.initialize({{
        client_id: GOOGLE_CLIENT_ID,
        auto_select: false,
        callback: handleGoogleCredentialResponse,
      }});
      window.google.accounts.id.renderButton(
        document.getElementById('googleSignInButton'),
        {{
          theme: 'outline',
          size: 'large',
          shape: 'pill',
          text: 'signin_with',
          width: 320,
          locale: 'ko',
        }},
      );
    }}

    renderLoginButton();
  </script>
</body>
</html>"""


@router.get("/api/auth/config")
async def auth_config():
    config = auth_service.public_config()
    return {
        "enabled": config["enabled"],
        "googleClientId": config["google_client_id"],
    }


@router.get("/api/auth/me")
async def auth_me(request: Request):
    user = await get_current_user(request)
    return {
        "enabled": auth_service.is_enabled(),
        "authenticated": bool(user),
        "user": serialize_user(user),
    }


@router.get("/login", response_class=Response)
async def login_page(request: Request):
    if not auth_service.is_enabled():
        return Response(
            content="<h1>Google 로그인이 아직 설정되지 않았습니다.</h1>",
            media_type="text/html; charset=utf-8",
            status_code=503,
        )
    current_user = await get_current_user(request)
    return_to = request.query_params.get("return_to")
    if current_user:
        return RedirectResponse(_normalize_return_to(return_to), status_code=303)
    return Response(
        content=_render_login_page(return_to),
        media_type="text/html; charset=utf-8",
    )


@router.post("/api/auth/google")
async def auth_google(request: Request, response: Response, payload: dict = Body(...)):
    if not auth_service.is_enabled():
        raise HTTPException(status_code=503, detail="Google 로그인이 아직 설정되지 않았습니다.")

    credential = str((payload or {}).get("credential") or "").strip()
    if not credential:
        raise HTTPException(status_code=400, detail="Google 로그인 토큰이 없습니다.")

    try:
        user = await auth_service.verify_google_credential(credential)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail="Google 로그인 검증에 실패했습니다.") from exc
    except Exception as exc:
        logger.warning("Google 로그인 검증 실패: %s", exc)
        raise HTTPException(status_code=502, detail="Google 인증 서버를 확인하지 못했습니다.") from exc

    await cache.upsert_user(user)
    await cache.delete_expired_sessions()

    session_token = auth_service.new_session_token()
    await cache.create_user_session(
        auth_service.hash_session_token(session_token),
        user["google_sub"],
        auth_service.session_expiry_iso(),
    )
    set_session_cookie(response, request, session_token)
    return {"ok": True, "user": serialize_user(user)}


@router.post("/api/auth/logout")
async def auth_logout(request: Request, response: Response):
    session_token = request.cookies.get(SESSION_COOKIE_NAME)
    if session_token and auth_service.is_enabled():
        try:
            await cache.delete_user_session(auth_service.hash_session_token(session_token))
        except RuntimeError:
            pass
    clear_session_cookie(response, request)
    return {"ok": True}


@router.post("/api/auth/google/callback")
async def auth_google_callback(request: Request):
    return_to = _normalize_return_to(request.query_params.get("return_to"))

    if not auth_service.is_enabled():
        return RedirectResponse(_append_query_value(return_to, "auth_error", "not_configured"), status_code=303)

    form = await _read_post_fields(request)
    credential = str(form.get("credential") or "").strip()
    csrf_cookie = str(request.cookies.get("g_csrf_token") or "").strip()
    csrf_form = str(form.get("g_csrf_token") or "").strip()

    if not credential or not csrf_cookie or csrf_cookie != csrf_form:
        return RedirectResponse(_append_query_value(return_to, "auth_error", "csrf"), status_code=303)

    try:
        user = await auth_service.verify_google_credential(credential)
    except Exception as exc:
        logger.warning("Google redirect login verification failed: %s", exc)
        return RedirectResponse(_append_query_value(return_to, "auth_error", "google"), status_code=303)

    await cache.upsert_user(user)
    await cache.delete_expired_sessions()

    session_token = auth_service.new_session_token()
    await cache.create_user_session(
        auth_service.hash_session_token(session_token),
        user["google_sub"],
        auth_service.session_expiry_iso(),
    )

    redirect_target = _append_query_value(return_to, "auth", "success")
    response = RedirectResponse(redirect_target, status_code=303)
    set_session_cookie(response, request, session_token)
    return response
