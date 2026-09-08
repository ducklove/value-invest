"""세션 쿠키를 사용하는 변경 요청의 출처 검증."""

from urllib.parse import urlsplit

from starlette.datastructures import Headers
from starlette.requests import Request
from starlette.responses import JSONResponse

from auth_service import SESSION_COOKIE_NAME


def normalize_origin(value: str) -> str:
    try:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
            return ""
        port = parsed.port
    except ValueError:
        return ""
    host = parsed.hostname.lower()
    if ":" in host:
        host = f"[{host}]"
    suffix = f":{port}" if port and port != (443 if parsed.scheme == "https" else 80) else ""
    return f"{parsed.scheme}://{host}{suffix}"


class MutationOriginMiddleware:
    """허용 출처는 설정에서만 가져오며 Host/전달 헤더로 확장하지 않는다.

    Origin/Referer가 없는 세션 요청에는 비단순 헤더를 요구한다. 브라우저는
    이 헤더를 교차 출처로 보내기 전에 CORS preflight를 통과해야 한다.
    쿠키 없는 CLI·기기·내부 작업은 각 엔드포인트의 기존 인증을 유지한다.
    Google의 form 콜백은 라우터가 별도의 double-submit 토큰을 검증한다.
    """

    def __init__(self, app, allowed_origins: list[str]):
        self.app = app
        self.allowed_origins = {origin for value in allowed_origins if (origin := normalize_origin(value))}

    async def __call__(self, scope, receive, send):
        if (scope["type"] != "http" or scope.get("method") not in {"POST", "PUT", "PATCH", "DELETE"}
                or not scope.get("path", "").startswith("/api/")
                or scope["path"] == "/api/auth/google/callback"):
            await self.app(scope, receive, send)
            return
        headers = Headers(scope=scope)
        source = headers.get("origin") if "origin" in headers else headers.get("referer")
        if source is not None:
            allowed = normalize_origin(source) in self.allowed_origins
        else:
            request = Request(scope)
            allowed = (headers.get("sec-fetch-site") != "cross-site"
                       and (SESSION_COOKIE_NAME not in request.cookies
                            or headers.get("x-requested-with") == "fetch"))
        if not allowed:
            await JSONResponse({"detail": "허용되지 않은 변경 요청 출처입니다."}, status_code=403)(scope, receive, send)
            return
        await self.app(scope, receive, send)
