import json
import logging
from urllib.parse import parse_qs, parse_qsl, urlencode, urlparse, urlunparse

from fastapi import APIRouter, Body, HTTPException, Request, Response
from fastapi.responses import RedirectResponse

import auth_service
from repositories import users as users_repo
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
    if not parsed.scheme and not parsed.netloc and value.startswith("/"):
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


async def _sign_in_user(request: Request, response: Response, user: dict) -> str:
    await users_repo.delete_expired_sessions()
    session_token = auth_service.new_session_token()
    await users_repo.create_user_session(
        auth_service.hash_session_token(session_token),
        user["google_sub"],
        auth_service.session_expiry_iso(),
    )
    set_session_cookie(response, request, session_token)
    return session_token


def _clean_name(value: str | None, fallback_email: str) -> str:
    name = " ".join(str(value or "").strip().split())
    return (name[:80] if name else fallback_email.split("@", 1)[0])


def _render_login_page(return_to: str | None) -> str:
    normalized_return_to = _normalize_return_to(return_to)
    escaped_return_to = json.dumps(normalized_return_to, ensure_ascii=False)
    config = auth_service.public_config()
    escaped_google_client_id = json.dumps(config["google_client_id"], ensure_ascii=False)
    google_enabled = json.dumps(bool(config["google_enabled"]))
    google_auth_url = json.dumps("/api/auth/google", ensure_ascii=False)
    password_login_url = json.dumps("/api/auth/password/login", ensure_ascii=False)
    register_url = json.dumps("/api/auth/register", ensure_ascii=False)
    home_url = json.dumps(normalized_return_to or "/", ensure_ascii=False)
    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>로그인</title>
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
      width: min(100%, 460px);
      background: white;
      border: 1px solid #e5e7eb;
      border-radius: 12px;
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
    .tabs {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 4px;
      padding: 4px;
      margin-top: 20px;
      border: 1px solid #e5e7eb;
      border-radius: 10px;
      background: #f9fafb;
    }}
    .tab {{
      border: 0;
      border-radius: 8px;
      padding: 10px 12px;
      background: transparent;
      color: #4b5563;
      font-weight: 700;
      cursor: pointer;
    }}
    .tab.active {{
      background: #2563eb;
      color: #fff;
    }}
    form {{
      display: grid;
      gap: 10px;
      margin-top: 14px;
    }}
    label {{
      display: grid;
      gap: 6px;
      color: #374151;
      font-size: 12px;
      font-weight: 700;
    }}
    input {{
      width: 100%;
      min-height: 42px;
      border: 1px solid #d1d5db;
      border-radius: 8px;
      padding: 0 12px;
      color: #111827;
      font: inherit;
      font-size: 14px;
    }}
    input:focus {{
      outline: none;
      border-color: #2563eb;
      box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.16);
    }}
    .primary {{
      min-height: 44px;
      border: 0;
      border-radius: 8px;
      background: #2563eb;
      color: #fff;
      font-weight: 800;
      cursor: pointer;
    }}
    .primary:disabled {{
      opacity: 0.6;
      cursor: wait;
    }}
    .divider {{
      display: grid;
      grid-template-columns: 1fr auto 1fr;
      gap: 10px;
      align-items: center;
      margin: 18px 0 12px;
      color: #9ca3af;
      font-size: 12px;
    }}
    .divider::before,
    .divider::after {{
      content: "";
      height: 1px;
      background: #e5e7eb;
    }}
    .google-box {{
      display: grid;
      justify-items: center;
      gap: 8px;
    }}
    .back-link {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-height: 44px;
      border-radius: 8px;
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
    .error {{
      min-height: 18px;
      color: #dc2626;
      font-size: 12px;
      line-height: 1.45;
    }}
    .hidden {{
      display: none;
    }}
  </style>
</head>
<body>
  <main class="card">
    <h1>로그인</h1>
    <p>이메일/비밀번호 또는 Google 계정으로 로그인해 최근 분석, 관심종목, 개인 메모를 저장합니다.</p>
    <div class="tabs" role="tablist" aria-label="로그인 방식">
      <button class="tab active" id="loginTab" type="button" onclick="setMode('login')">로그인</button>
      <button class="tab" id="registerTab" type="button" onclick="setMode('register')">가입</button>
    </div>

    <form id="loginForm">
      <label>이메일
        <input id="loginEmail" type="email" autocomplete="username" required>
      </label>
      <label>비밀번호
        <input id="loginPassword" type="password" autocomplete="current-password" required>
      </label>
      <button class="primary" type="submit">이메일로 로그인</button>
      <div class="error" id="loginError" aria-live="polite"></div>
    </form>

    <form id="registerForm" class="hidden">
      <label>이름
        <input id="registerName" type="text" autocomplete="name" maxlength="80" placeholder="표시 이름">
      </label>
      <label>이메일
        <input id="registerEmail" type="email" autocomplete="username" required>
      </label>
      <label>비밀번호
        <input id="registerPassword" type="password" autocomplete="new-password" minlength="8" required>
      </label>
      <button class="primary" type="submit">가입하고 시작</button>
      <div class="error" id="registerError" aria-live="polite"></div>
    </form>

    <div class="divider">또는</div>
    <div class="google-box">
      <div id="googleSignInButton"></div>
      <div class="note" id="googleNote">Google 로그인도 계속 사용할 수 있습니다.</div>
    </div>

    <div class="actions">
      <a class="back-link" href={home_url}>분석 화면으로 돌아가기</a>
    </div>
  </main>
  <script>
    const RETURN_TO = {escaped_return_to};
    const GOOGLE_CLIENT_ID = {escaped_google_client_id};
    const GOOGLE_ENABLED = {google_enabled};
    const GOOGLE_AUTH_URL = {google_auth_url};
    const PASSWORD_LOGIN_URL = {password_login_url};
    const REGISTER_URL = {register_url};

    function setMode(mode) {{
      const register = mode === 'register';
      document.getElementById('loginTab').classList.toggle('active', !register);
      document.getElementById('registerTab').classList.toggle('active', register);
      document.getElementById('loginForm').classList.toggle('hidden', register);
      document.getElementById('registerForm').classList.toggle('hidden', !register);
      document.getElementById('loginError').textContent = '';
      document.getElementById('registerError').textContent = '';
    }}

    function setBusy(form, busy) {{
      form.querySelectorAll('button, input').forEach(el => {{ el.disabled = busy; }});
    }}

    async function submitJson(url, payload) {{
      const resp = await fetch(url, {{
        method: 'POST',
        credentials: 'include',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(payload),
      }});
      const data = await resp.json().catch(() => ({{}}));
      if (!resp.ok) {{
        throw new Error(data.detail || '요청을 처리하지 못했습니다.');
      }}
      return data;
    }}

    document.getElementById('loginForm').addEventListener('submit', async (event) => {{
      event.preventDefault();
      const form = event.currentTarget;
      const error = document.getElementById('loginError');
      error.textContent = '';
      setBusy(form, true);
      try {{
        await submitJson(PASSWORD_LOGIN_URL, {{
          email: document.getElementById('loginEmail').value,
          password: document.getElementById('loginPassword').value,
        }});
        window.location.replace(RETURN_TO || '/');
      }} catch (err) {{
        error.textContent = err.message || '로그인에 실패했습니다.';
      }} finally {{
        setBusy(form, false);
      }}
    }});

    document.getElementById('registerForm').addEventListener('submit', async (event) => {{
      event.preventDefault();
      const form = event.currentTarget;
      const error = document.getElementById('registerError');
      error.textContent = '';
      setBusy(form, true);
      try {{
        await submitJson(REGISTER_URL, {{
          name: document.getElementById('registerName').value,
          email: document.getElementById('registerEmail').value,
          password: document.getElementById('registerPassword').value,
        }});
        window.location.replace(RETURN_TO || '/');
      }} catch (err) {{
        error.textContent = err.message || '가입에 실패했습니다.';
      }} finally {{
        setBusy(form, false);
      }}
    }});

    async function submitGoogleCredential(credential) {{
      const resp = await fetch(GOOGLE_AUTH_URL, {{
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
      const note = document.getElementById('googleNote');
      if (!GOOGLE_ENABLED || !GOOGLE_CLIENT_ID) {{
        note.textContent = '이 서버는 Google 로그인이 설정되어 있지 않습니다.';
        return;
      }}
      if (!window.google?.accounts?.id) {{
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
        "passwordEnabled": config["password_enabled"],
        "googleEnabled": config["google_enabled"],
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
            content="<h1>로그인 시스템이 아직 설정되지 않았습니다.</h1>",
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
    if not auth_service.is_google_enabled():
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

    stored_user = await users_repo.upsert_user(user)
    await _sign_in_user(request, response, stored_user)
    return {"ok": True, "user": serialize_user(stored_user)}


@router.post("/api/auth/register")
async def auth_register(request: Request, response: Response, payload: dict = Body(...)):
    if not auth_service.is_enabled():
        raise HTTPException(status_code=503, detail="로그인 시스템이 아직 설정되지 않았습니다.")

    email = auth_service.normalize_email((payload or {}).get("email"))
    password = str((payload or {}).get("password") or "")
    if not auth_service.validate_email(email):
        raise HTTPException(status_code=400, detail="올바른 이메일을 입력해 주세요.")
    password_error = auth_service.validate_password(password)
    if password_error:
        raise HTTPException(status_code=400, detail=password_error)

    existing = await users_repo.get_user_by_email(email)
    if existing and existing.get("password_set"):
        raise HTTPException(status_code=409, detail="이미 가입된 이메일입니다. 로그인해 주세요.")
    if existing:
        raise HTTPException(
            status_code=409,
            detail="이미 Google 계정으로 사용 중인 이메일입니다. Google로 로그인한 뒤 프로필에서 비밀번호를 등록해 주세요.",
        )

    user = await users_repo.create_local_user(
        email=email,
        name=_clean_name((payload or {}).get("name"), email),
        password_hash=auth_service.hash_password(password),
    )
    await _sign_in_user(request, response, user)
    return {"ok": True, "user": serialize_user(user)}


@router.post("/api/auth/password/login")
async def auth_password_login(request: Request, response: Response, payload: dict = Body(...)):
    if not auth_service.is_enabled():
        raise HTTPException(status_code=503, detail="로그인 시스템이 아직 설정되지 않았습니다.")

    email = auth_service.normalize_email((payload or {}).get("email"))
    password = str((payload or {}).get("password") or "")
    if not auth_service.validate_email(email) or not password:
        raise HTTPException(status_code=401, detail="이메일 또는 비밀번호가 올바르지 않습니다.")

    user = await users_repo.get_password_user_by_email(email)
    if not user or not auth_service.verify_password(password, user.get("password_hash")):
        raise HTTPException(status_code=401, detail="이메일 또는 비밀번호가 올바르지 않습니다.")

    user = await users_repo.touch_user_login(user["google_sub"]) or user
    await _sign_in_user(request, response, user)
    return {"ok": True, "user": serialize_user(user)}


@router.put("/api/auth/me/password")
async def auth_update_password(request: Request, payload: dict = Body(...)):
    if not auth_service.is_enabled():
        raise HTTPException(status_code=503, detail="로그인 시스템이 아직 설정되지 않았습니다.")

    current_user = await get_current_user(request)
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    fresh_user = await users_repo.get_user_for_auth(current_user["google_sub"])
    if not fresh_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    current_password = str((payload or {}).get("current_password") or "")
    new_password = str((payload or {}).get("new_password") or "")
    password_error = auth_service.validate_password(new_password)
    if password_error:
        raise HTTPException(status_code=400, detail=password_error)

    if fresh_user.get("password_hash") and not auth_service.verify_password(
        current_password,
        fresh_user.get("password_hash"),
    ):
        raise HTTPException(status_code=400, detail="현재 비밀번호가 올바르지 않습니다.")

    updated = await users_repo.set_user_password(
        fresh_user["google_sub"],
        auth_service.hash_password(new_password),
    )
    return {"ok": True, "user": serialize_user(updated)}


@router.post("/api/auth/logout")
async def auth_logout(request: Request, response: Response):
    session_token = request.cookies.get(SESSION_COOKIE_NAME)
    if session_token and auth_service.is_enabled():
        try:
            await users_repo.delete_user_session(auth_service.hash_session_token(session_token))
        except RuntimeError:
            pass
    clear_session_cookie(response, request)
    return {"ok": True}


@router.post("/api/auth/google/callback")
async def auth_google_callback(request: Request):
    return_to = _normalize_return_to(request.query_params.get("return_to"))

    if not auth_service.is_google_enabled():
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

    stored_user = await users_repo.upsert_user(user)

    redirect_target = _append_query_value(return_to, "auth", "success")
    response = RedirectResponse(redirect_target, status_code=303)
    await _sign_in_user(request, response, stored_user)
    return response
