import asyncio
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, parse_qsl, urlencode, urlparse, urlunparse

import httpx
from fastapi import Body, FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

import auth_service
import cache
import dart_client
import stock_price
import analyzer
import kis_proxy_client
import report_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="한국 주식 가치투자 분석")

STATIC_DIR = Path(__file__).parent / "static"
ANALYSIS_SEMAPHORE = asyncio.Semaphore(2)
ANALYSIS_LOCKS: dict[str, asyncio.Lock] = {}
ANALYSIS_LOCKS_GUARD = asyncio.Lock()
LATEST_REPORT_CACHE_TTL_MINUTES = 15
REPORT_LIST_CACHE_TTL_MINUTES = 60
ANALYSIS_SNAPSHOT_TTL_MINUTES = 60
RECENT_QUOTES_SEMAPHORE = asyncio.Semaphore(4)
SESSION_COOKIE_NAME = auth_service.SESSION_COOKIE_NAME
TRUSTED_RETURN_ORIGINS = {
    "http://localhost:8000",
    "http://127.0.0.1:8000",
    "https://ducklove.github.io",
    "https://cantabile.tplinkdns.com:3691",
}

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost",
        "http://127.0.0.1",
        "http://localhost:8000",
        "http://127.0.0.1:8000",
        "https://ducklove.github.io",
        "https://cantabile.tplinkdns.com:3691",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    await kis_proxy_client.init_client()
    await cache.init_db()
    await cache.delete_expired_sessions()
    needs_corp_refresh = not await cache.is_corp_codes_loaded() or await cache.corp_codes_need_refresh()
    if needs_corp_refresh:
        logger.info("corp_codes 테이블을 DART 기준으로 갱신합니다...")
        try:
            codes = await dart_client.fetch_corp_codes()
            await cache.save_corp_codes(codes)
            logger.info(f"{len(codes)}개 상장사 코드를 저장했습니다.")
        except Exception as e:
            logger.error(f"corp_codes 다운로드 실패: {e}")


@app.on_event("shutdown")
async def shutdown():
    await kis_proxy_client.close_client()


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/app-config.js")
async def app_config():
    payload = {"apiBaseUrl": ""}
    return Response(
        content=f"window.APP_CONFIG = {json.dumps(payload, ensure_ascii=False)};",
        media_type="application/javascript",
    )


@app.get("/styles.css")
async def styles():
    return FileResponse(STATIC_DIR / "styles.css", media_type="text/css")


@app.get("/app.js")
async def app_js():
    return FileResponse(STATIC_DIR / "app.js", media_type="application/javascript")


def _is_secure_request(request: Request) -> bool:
    forwarded_proto = request.headers.get("x-forwarded-proto", "").split(",", 1)[0].strip()
    return request.url.scheme == "https" or forwarded_proto == "https"


def _session_cookie_samesite(request: Request) -> str:
    return "none" if _is_secure_request(request) else "lax"


def _serialize_user(user: dict | None) -> dict | None:
    if not user:
        return None
    return {
        "google_sub": user["google_sub"],
        "email": user["email"],
        "name": user["name"],
        "picture": user.get("picture") or "",
        "email_verified": bool(user.get("email_verified")),
    }


async def _get_current_user(request: Request) -> dict | None:
    if not auth_service.is_enabled():
        return None

    session_token = request.cookies.get(SESSION_COOKIE_NAME)
    if not session_token:
        return None

    try:
        token_hash = auth_service.hash_session_token(session_token)
    except RuntimeError:
        return None
    return await cache.get_user_by_session(token_hash)


def _set_session_cookie(response: Response, request: Request, session_token: str):
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=session_token,
        max_age=auth_service.SESSION_MAX_AGE_SECONDS,
        httponly=True,
        secure=_is_secure_request(request),
        samesite=_session_cookie_samesite(request),
        path="/",
    )


def _clear_session_cookie(response: Response, request: Request):
    response.delete_cookie(
        key=SESSION_COOKIE_NAME,
        path="/",
        secure=_is_secure_request(request),
        samesite=_session_cookie_samesite(request),
    )


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


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _analysis_snapshot_is_stale(analyzed_at: str | None) -> bool:
    analyzed_dt = _parse_iso_datetime(analyzed_at)
    if analyzed_dt is None:
        return True
    return datetime.now() - analyzed_dt > timedelta(minutes=ANALYSIS_SNAPSHOT_TTL_MINUTES)


def _render_login_page(return_to: str | None) -> str:
    normalized_return_to = _normalize_return_to(return_to)
    escaped_return_to = json.dumps(normalized_return_to, ensure_ascii=False)
    escaped_google_client_id = json.dumps(auth_service.public_config()["google_client_id"], ensure_ascii=False)
    callback_url = json.dumps("/api/auth/google/callback", ensure_ascii=False)
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
    const LOGIN_URI = {callback_url} + (RETURN_TO && RETURN_TO !== '/' ? '?return_to=' + encodeURIComponent(RETURN_TO) : '');

    function renderLoginButton() {{
      if (!GOOGLE_CLIENT_ID || !window.google?.accounts?.id) {{
        window.setTimeout(renderLoginButton, 250);
        return;
      }}
      window.google.accounts.id.initialize({{
        client_id: GOOGLE_CLIENT_ID,
        auto_select: false,
        ux_mode: 'redirect',
        login_uri: LOGIN_URI,
        use_fedcm_for_button: true,
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


def _default_user_preference() -> dict:
    return {
        "is_starred": False,
        "is_pinned": False,
        "note": "",
        "updated_at": None,
    }


async def _decorate_analysis_payload(payload: dict, user: dict | None) -> dict:
    enriched = dict(payload)
    enriched["authenticated"] = bool(user)
    if user:
        enriched["user_preference"] = await cache.get_user_stock_preference(
            user["google_sub"],
            payload["stock_code"],
        )
    else:
        enriched["user_preference"] = _default_user_preference()
    return enriched


async def _remember_recent_analysis(user: dict | None, stock_code: str):
    if user:
        await cache.touch_user_recent_analysis(user["google_sub"], stock_code)


@app.get("/api/auth/config")
async def auth_config():
    config = auth_service.public_config()
    return {
        "enabled": config["enabled"],
        "googleClientId": config["google_client_id"],
    }


@app.get("/api/auth/me")
async def auth_me(request: Request):
    user = await _get_current_user(request)
    return {
        "enabled": auth_service.is_enabled(),
        "authenticated": bool(user),
        "user": _serialize_user(user),
    }


@app.get("/login", response_class=Response)
async def login_page(request: Request):
    if not auth_service.is_enabled():
        return Response(
            content="<h1>Google 로그인이 아직 설정되지 않았습니다.</h1>",
            media_type="text/html; charset=utf-8",
            status_code=503,
        )

    current_user = await _get_current_user(request)
    return_to = request.query_params.get("return_to")
    if current_user:
        return RedirectResponse(_normalize_return_to(return_to), status_code=303)

    return Response(
        content=_render_login_page(return_to),
        media_type="text/html; charset=utf-8",
    )


@app.post("/api/auth/google")
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
    _set_session_cookie(response, request, session_token)
    return {"ok": True, "user": _serialize_user(user)}


@app.post("/api/auth/logout")
async def auth_logout(request: Request, response: Response):
    session_token = request.cookies.get(SESSION_COOKIE_NAME)
    if session_token and auth_service.is_enabled():
        try:
            await cache.delete_user_session(auth_service.hash_session_token(session_token))
        except RuntimeError:
            pass
    _clear_session_cookie(response, request)
    return {"ok": True}


@app.post("/api/auth/google/callback")
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
    _set_session_cookie(response, request, session_token)
    return response


@app.get("/api/search")
async def search(q: str = Query(..., min_length=1)):
    results = await cache.search_corp(q)
    return results


@app.get("/api/preferences/{stock_code}")
async def get_stock_preference(stock_code: str, request: Request):
    current_user = await _get_current_user(request)
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")
    preference = await cache.get_user_stock_preference(current_user["google_sub"], stock_code)
    return {
        "stock_code": stock_code,
        "authenticated": True,
        "user_preference": preference,
    }


@app.put("/api/preferences/{stock_code}")
async def update_stock_preference(stock_code: str, request: Request, payload: dict = Body(...)):
    current_user = await _get_current_user(request)
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다.")

    preference = await cache.save_user_stock_preference(
        current_user["google_sub"],
        stock_code,
        is_starred=payload.get("is_starred") if "is_starred" in payload else None,
        is_pinned=payload.get("is_pinned") if "is_pinned" in payload else None,
        note=payload.get("note") if "note" in payload else None,
    )
    return {
        "stock_code": stock_code,
        "authenticated": True,
        "user_preference": preference,
    }


@app.get("/api/quote/{stock_code}")
async def quote_snapshot(stock_code: str):
    corp_code = await cache.get_corp_code(stock_code)
    if not corp_code:
        raise HTTPException(status_code=404, detail="종목코드를 찾을 수 없습니다.")
    return await stock_price.fetch_quote_snapshot(stock_code)


async def _get_analysis_lock(stock_code: str) -> asyncio.Lock:
    async with ANALYSIS_LOCKS_GUARD:
        lock = ANALYSIS_LOCKS.get(stock_code)
        if lock is None:
            lock = asyncio.Lock()
            ANALYSIS_LOCKS[stock_code] = lock
        return lock


def _sse_event(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def _is_allowed_report_pdf_url(url: str) -> bool:
    parsed = urlparse(url)
    return (
        parsed.scheme == "https"
        and parsed.netloc == "stock.pstatic.net"
        and parsed.path.startswith("/stock-research/")
        and parsed.path.endswith(".pdf")
    )


def _report_signature(report: dict | None) -> tuple:
    if not report:
        return ()
    return (
        report.get("date"),
        report.get("title"),
        report.get("firm"),
        report.get("pdf_url"),
    )


async def _build_analysis_response(
    stock_code: str,
    corp_name: str,
    fin_data: list[dict],
    mkt_data: list[dict],
    cached: bool,
    analyzed_at: str | None = None,
) -> dict:
    weekly_mkt_data, quote_snapshot = await asyncio.gather(
        stock_price.fetch_weekly_market_data(stock_code, fin_data),
        stock_price.fetch_quote_snapshot(stock_code),
        return_exceptions=True,
    )
    if isinstance(weekly_mkt_data, Exception):
        logger.warning(f"주간 시장 데이터 계산 실패({stock_code}): {weekly_mkt_data}")
        weekly_mkt_data = []
    if isinstance(quote_snapshot, Exception):
        logger.warning(f"현재가 스냅샷 계산 실패({stock_code}): {quote_snapshot}")
        quote_snapshot = {}

    result = analyzer.analyze(fin_data, mkt_data, weekly_mkt_data)
    payload = {
        "stock_code": stock_code,
        "corp_name": corp_name,
        "cached": cached,
        "quote_snapshot": quote_snapshot,
        **result,
    }
    if analyzed_at:
        payload["analyzed_at"] = analyzed_at
    return payload


def _has_quote_snapshot(quote_snapshot: dict | None) -> bool:
    return isinstance(quote_snapshot, dict) and quote_snapshot.get("price") is not None


async def _attach_quote_snapshots(items: list[dict]) -> list[dict]:
    async def enrich(item: dict) -> dict:
        enriched = dict(item)
        if _has_quote_snapshot(enriched.get("quote_snapshot")):
            return enriched
        try:
            async with RECENT_QUOTES_SEMAPHORE:
                enriched["quote_snapshot"] = await stock_price.fetch_quote_snapshot(item["stock_code"])
        except Exception as exc:
            logger.warning("사이드바 현재가 조회 실패(%s): %s", item.get("stock_code"), exc)
            enriched["quote_snapshot"] = {}
        return enriched

    return await asyncio.gather(*(enrich(item) for item in items))


async def _ensure_financial_report_dates(stock_code: str, corp_code: str | None, fin_data: list[dict]) -> list[dict]:
    if not fin_data or all(item.get("report_date") for item in fin_data):
        return fin_data
    if not corp_code:
        return fin_data

    years = [item["year"] for item in fin_data if item.get("year") is not None]
    if not years:
        return fin_data

    report_dates = await dart_client.fetch_annual_report_dates(corp_code, min(years), max(years))
    updated = False
    for item in fin_data:
        if item.get("report_date"):
            continue
        report_date = report_dates.get(item["year"])
        if report_date:
            item["report_date"] = report_date
            updated = True

    if updated:
        await cache.save_financial_data(stock_code, fin_data)

    return fin_data


async def _ensure_financial_coverage(stock_code: str, corp_code: str | None, fin_data: list[dict]) -> list[dict]:
    merged = await stock_price.ensure_financial_data_coverage(stock_code, fin_data)
    if merged != fin_data:
        await cache.save_financial_data(stock_code, merged)
    return await _ensure_financial_report_dates(stock_code, corp_code, merged)


async def _load_cached_analysis_payload(
    stock_code: str,
    corp_code: str | None,
    corp_name: str,
    analyzed_at: str | None,
) -> dict:
    fin_data = await cache.get_financial_data(stock_code)
    if corp_code:
        fin_data = await _ensure_financial_report_dates(stock_code, corp_code, fin_data)

    mkt_data = await cache.get_market_data(stock_code)
    needs_market_refresh = stock_price.market_data_needs_refresh(mkt_data)

    if corp_code and (not fin_data or needs_market_refresh):
        fin_data = await _ensure_financial_coverage(stock_code, corp_code, fin_data)

    if needs_market_refresh:
        try:
            refreshed = await stock_price.fetch_market_data(stock_code, fin_data)
            if refreshed:
                mkt_data = refreshed
                await cache.save_market_data(stock_code, refreshed)
        except Exception as e:
            logger.warning(f"시장 데이터 갱신 실패({stock_code}): {e}")

    payload = await _build_analysis_response(
        stock_code,
        corp_name,
        fin_data,
        mkt_data,
        cached=True,
        analyzed_at=analyzed_at,
    )
    await cache.save_analysis_snapshot(stock_code, corp_name, payload)
    return payload


@app.get("/api/analyze/{stock_code}")
async def analyze_stock(stock_code: str, request: Request):
    current_user = await _get_current_user(request)
    snapshot = await cache.get_analysis_snapshot(stock_code)
    if snapshot and not _analysis_snapshot_is_stale(snapshot.get("analyzed_at")):
        await _remember_recent_analysis(current_user, stock_code)
        return await _decorate_analysis_payload(snapshot, current_user)

    meta = await cache.get_analysis_meta(stock_code)
    if meta:
        corp_code = await cache.get_corp_code(stock_code)
        payload = await _load_cached_analysis_payload(
            stock_code,
            corp_code,
            meta["corp_name"],
            meta.get("analyzed_at"),
        )
        await _remember_recent_analysis(current_user, stock_code)
        return await _decorate_analysis_payload(payload, current_user)

    corp_code = await cache.get_corp_code(stock_code)
    if not corp_code:
        raise HTTPException(status_code=404, detail="종목코드를 찾을 수 없습니다.")

    corp_name = await cache.get_corp_name(stock_code)

    async def stream():
        stock_lock = await _get_analysis_lock(stock_code)

        if stock_lock.locked():
            yield _sse_event("progress", {"step": "queued", "message": "같은 종목 분석이 진행 중입니다. 완료 후 캐시 결과를 사용합니다..."})
        if ANALYSIS_SEMAPHORE.locked():
            yield _sse_event("progress", {"step": "queued_global", "message": "다른 분석 작업이 많아 잠시 대기합니다..."})

        async with stock_lock:
            snapshot = await cache.get_analysis_snapshot(stock_code)
            if snapshot and not _analysis_snapshot_is_stale(snapshot.get("analyzed_at")):
                await _remember_recent_analysis(current_user, stock_code)
                yield _sse_event("result", await _decorate_analysis_payload(snapshot, current_user))
                return

            meta = await cache.get_analysis_meta(stock_code)
            if meta:
                payload = await _load_cached_analysis_payload(
                    stock_code,
                    corp_code,
                    meta["corp_name"],
                    meta.get("analyzed_at"),
                )
                await _remember_recent_analysis(current_user, stock_code)
                yield _sse_event("result", await _decorate_analysis_payload(payload, current_user))
                return

            async with ANALYSIS_SEMAPHORE:
                yield _sse_event("progress", {"step": "start", "message": f"{corp_name} 분석을 시작합니다..."})
                report_dates = {}

                # DART 재무제표 수집
                yield _sse_event("progress", {"step": "dart_start", "message": "DART 재무제표를 수집합니다..."})
                fin_data = []
                try:
                    from datetime import datetime as dt
                    end_year = dt.now().year - 1
                    start_year = dart_client.DART_ANNUAL_DATA_START_YEAR
                    total_years = end_year - start_year + 1
                    report_dates = await dart_client.fetch_annual_report_dates(corp_code, start_year, end_year)

                    for i, year in enumerate(range(start_year, end_year + 1)):
                        yield _sse_event("progress", {
                            "step": "dart_fetch",
                            "message": f"DART 재무제표 조회 중... ({i+1}/{total_years}) - {year}년",
                            "current": i + 1, "total": total_years,
                        })
                        stmt = await dart_client.fetch_financial_statement(corp_code, year)
                        if stmt:
                            report_date = report_dates.get(year)
                            if report_date:
                                stmt["report_date"] = report_date
                            fin_data.append(stmt)
                        await asyncio.sleep(0.5)

                    yield _sse_event("progress", {
                        "step": "dart_done",
                        "message": f"DART 재무제표 수집 완료 ({len(fin_data)}개년 데이터)",
                    })
                except Exception as e:
                    logger.error(f"DART 재무제표 조회 실패: {e}")
                    yield _sse_event("progress", {"step": "dart_error", "message": f"DART 조회 실패: {e}"})

                try:
                    fin_data = await _ensure_financial_coverage(stock_code, corp_code, fin_data)
                    yield _sse_event("progress", {
                        "step": "dart_done",
                        "message": f"연간 재무 범위 보강 완료 ({len(fin_data)}개년 데이터)",
                    })
                except Exception as e:
                    logger.warning(f"재무 범위 보강 실패({stock_code}): {e}")

                yield _sse_event("progress", {"step": "market_start", "message": "시장 데이터와 파생 지표를 계산합니다..."})
                mkt_data = []
                try:
                    mkt_data = await stock_price.fetch_market_data(stock_code, fin_data)
                    yield _sse_event("progress", {
                        "step": "market_done",
                        "message": f"시장 데이터 수집 완료 ({len(mkt_data)}개년 데이터)",
                    })
                except Exception as e:
                    logger.error(f"시장 데이터 조회 실패: {e}")
                    yield _sse_event("progress", {"step": "market_error", "message": f"시장 데이터 조회 실패: {e}"})

                if not fin_data and not mkt_data:
                    yield _sse_event("error", {"message": "데이터를 가져올 수 없습니다."})
                    return

                yield _sse_event("progress", {"step": "saving", "message": "데이터를 캐시에 저장합니다..."})
                if fin_data:
                    await cache.save_financial_data(stock_code, fin_data)
                if mkt_data:
                    await cache.save_market_data(stock_code, mkt_data)

                yield _sse_event("progress", {"step": "analyzing", "message": "지표를 계산합니다..."})
                payload = await _build_analysis_response(
                    stock_code,
                    corp_name,
                    fin_data,
                    mkt_data,
                    cached=False,
                )
                await cache.save_analysis_snapshot(stock_code, corp_name or stock_code, payload)
                await _remember_recent_analysis(current_user, stock_code)
                yield _sse_event("result", await _decorate_analysis_payload(payload, current_user))

    return StreamingResponse(stream(), media_type="text/event-stream")


@app.get("/api/reports/{stock_code}")
async def get_reports(stock_code: str, refresh: bool = False):
    """증권사 리포트 목록 (네이버 금융, 최근 3년)."""
    try:
        if not refresh:
            cached_reports = await cache.get_report_list(stock_code, REPORT_LIST_CACHE_TTL_MINUTES)
            if cached_reports:
                return {
                    "stock_code": stock_code,
                    "reports": cached_reports["reports"],
                    "cached": True,
                    "cached_at": cached_reports["fetched_at"],
                }

        reports = await report_client.fetch_reports(stock_code)
        await cache.save_report_list(stock_code, reports)
        if reports:
            await cache.save_latest_report(stock_code, reports[0])
        return {"stock_code": stock_code, "reports": reports, "cached": False}
    except Exception as e:
        logger.error(f"증권사 리포트 조회 실패: {e}")
        return {"stock_code": stock_code, "reports": [], "error": str(e)}


@app.get("/api/reports/{stock_code}/latest")
async def get_latest_report(stock_code: str, refresh: bool = False):
    try:
        cached_report = await cache.get_latest_report(
            stock_code,
            None if refresh else LATEST_REPORT_CACHE_TTL_MINUTES,
        )
        if not refresh and cached_report:
            cached_at = cached_report.pop("_cached_at", None)
            return {"stock_code": stock_code, "report": cached_report, "cached": True, "cached_at": cached_at}

        report = await report_client.fetch_latest_report(stock_code)
        if report:
            changed = _report_signature(cached_report) != _report_signature(report)
            await cache.save_latest_report(stock_code, report)
            return {"stock_code": stock_code, "report": report, "cached": False, "changed": changed}
        if cached_report:
            cached_at = cached_report.pop("_cached_at", None)
            return {"stock_code": stock_code, "report": cached_report, "cached": True, "cached_at": cached_at, "stale": True}
        return {"stock_code": stock_code, "report": None, "cached": False, "changed": False}
    except Exception as e:
        logger.error(f"최신 증권사 리포트 조회 실패: {e}")
        return {"stock_code": stock_code, "report": None, "cached": False, "error": str(e)}


@app.get("/api/report-pdf")
async def proxy_report_pdf(url: str = Query(..., min_length=1)):
    if not _is_allowed_report_pdf_url(url):
        raise HTTPException(status_code=400, detail="허용되지 않은 리포트 URL입니다.")

    try:
        async with httpx.AsyncClient(timeout=30, headers=report_client.HEADERS, follow_redirects=True) as client:
            resp = await client.get(url)
    except Exception as e:
        logger.error(f"리포트 PDF 프록시 실패: {e}")
        raise HTTPException(status_code=502, detail="리포트 원문을 불러오지 못했습니다.") from e

    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail="리포트 원문 응답이 올바르지 않습니다.")

    filename = Path(urlparse(url).path).name or "report.pdf"
    headers = {
        "Content-Disposition": f'inline; filename="{filename}"',
        "Cache-Control": "public, max-age=86400",
    }
    return Response(content=resp.content, media_type="application/pdf", headers=headers)


@app.delete("/api/cache/{stock_code}")
async def delete_cache(stock_code: str, request: Request):
    current_user = await _get_current_user(request)
    if not current_user:
        raise HTTPException(status_code=401, detail="로그인 후 내 목록에서만 삭제할 수 있습니다.")
    await cache.delete_user_recent_analysis(current_user["google_sub"], stock_code)
    return {"ok": True, "scope": "user"}


@app.get("/api/cache/list")
async def cache_list(request: Request, include_quotes: bool = Query(False)):
    current_user = await _get_current_user(request)
    items = await cache.get_cached_analyses(
        limit=20,
        include_quotes=include_quotes,
        google_sub=current_user["google_sub"] if current_user else None,
    )
    if include_quotes:
        return await _attach_quote_snapshots(items)
    return items


# 정적 파일 서빙 (CSS, JS 등 추가 시 대비)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
