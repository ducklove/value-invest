"""레지스트리 기반 도구 딥링크 조립 — ``/go/{tool_id}`` 와 보유 스냅샷 handoff.

규칙:

- 목적지는 레지스트리 도구의 (env 반영) ``url`` 뿐이다. 템플릿(``stockLink``/``viewLink``/
  ``assetLink``/``embed``)은 상대 경로·쿼리·fragment 만 허용하고 호스트를 바꿀 수 없다.
- 값은 각 템플릿의 ``accepts`` 정규식을 통과해야 붙는다. 통과하지 못하면 조용히 뺀다
  (기존 ``/api/portfolio/open`` 의 동작과 같다 — 도구 홈으로 간다).
- ``theme`` 는 ``light``/``dark`` 만 전달한다.
- handoff 도구는 보유 스냅샷을 **fragment**(``#vc-held=CODE:qty,…``)로만 싣는다. fragment 는
  서버 로그·Referer 로 새지 않는다.
"""

from __future__ import annotations

import re
from typing import Any, Iterable
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

from core import ecosystem
from domain.portfolio_codes import is_korean_stock

THEMES = frozenset({"light", "dark"})
_ETF_CODE_RE = re.compile(r"[A-Z0-9][A-Z0-9.-]{0,29}")
_PLACEHOLDER_RE = re.compile(r"\{[a-z]+\}")


class LinkError(ValueError):
    """레지스트리로 목적지를 만들 수 없다(알 수 없는 도구·잘못된 기본 URL)."""


class UnknownLinkTool(LinkError, LookupError):
    """공개 레지스트리에 없는 도구 id."""


def public_tool(tool_id: str) -> dict[str, Any]:
    entry = ecosystem.tool(tool_id)
    if entry is None or entry.get("visibility") != "public" or not entry.get("url"):
        raise UnknownLinkTool(tool_id)
    return entry


def tool_for_integration(integration_key: str) -> dict[str, Any] | None:
    return next((t for t in ecosystem.tools() if t.get("integrationKey") == integration_key), None)


def _base_parts(url: str | None):
    parts = urlsplit(url or "")
    if parts.scheme not in {"https", "http"} or not parts.netloc:
        raise LinkError(f"invalid tool base url: {url!r}")
    return parts


def _accepted(link: dict | None, value: str | None) -> bool:
    if not link or not value:
        return False
    try:
        return bool(re.fullmatch(str(link.get("accepts") or ""), value))
    except re.error:
        return False


def _fill(template: str, values: dict[str, str]) -> str | None:
    filled = template
    for key, value in values.items():
        filled = filled.replace("{" + key + "}", quote(value, safe=""))
    return None if _PLACEHOLDER_RE.search(filled) else filled


class _UrlBuilder:
    def __init__(self, base: str, *, trailing_slash: bool):
        parts = _base_parts(base)
        self.scheme, self.netloc = parts.scheme, parts.netloc
        path = parts.path or "/"
        if trailing_slash and not path.endswith("/"):
            path += "/"
        self.path = path
        self.query: list[tuple[str, str]] = parse_qsl(parts.query, keep_blank_values=True)
        self.fragment = parts.fragment

    def apply(self, template: str | None, values: dict[str, str]) -> bool:
        if not template:
            return False
        filled = _fill(template, values)
        if filled is None:
            return False
        rel = urlsplit(filled)
        if rel.scheme or rel.netloc:  # 템플릿이 호스트를 바꿀 수 없다
            raise LinkError(f"template must be relative: {template!r}")
        if rel.path:
            self.path = self.path.rstrip("/") + (rel.path if rel.path.startswith("/") else "/" + rel.path)
        for name, value in parse_qsl(rel.query, keep_blank_values=True):
            self.query = [(k, v) for k, v in self.query if k != name] + [(name, value)]
        if rel.fragment:
            self.fragment = rel.fragment
        return True

    def build(self) -> str:
        return urlunsplit((self.scheme, self.netloc, self.path, urlencode(self.query), self.fragment))


def go_url(
    tool_id: str,
    *,
    code: str | None = None,
    view: str | None = None,
    asset: str | None = None,
    theme: str | None = None,
    embed: bool = False,
) -> str:
    """handoff 가 아닌 도구의 목적지 URL. 알 수 없는 도구면 :class:`UnknownLinkTool`."""
    entry = public_tool(tool_id)
    base = ecosystem.resolved_url(entry)
    builder = _UrlBuilder(base, trailing_slash=entry.get("deploy") == "github-pages")
    code = (code or "").strip().upper()
    view = (view or "").strip()
    asset = (asset or "").strip()
    values: dict[str, str] = {}
    if _accepted(entry.get("stockLink"), code):
        values["code"] = code
        builder.apply(entry["stockLink"]["template"], {"code": code})
    if _accepted(entry.get("viewLink"), view):
        values["view"] = view
        builder.apply(entry["viewLink"]["template"], {"view": view})
    if _accepted(entry.get("assetLink"), asset):
        values["asset"] = asset
        builder.apply(entry["assetLink"]["template"], {"asset": asset})
    if embed and isinstance(entry.get("embed"), dict):
        # ``?embed={view}`` 처럼 값 자리가 있는 템플릿은 값이 없으면 빈 값(``?embed=``)으로
        # 채운다 — 딥링크 계약 §5-1: embed 값은 선택적 뷰 이름이고 빈 값도 embed 다.
        template = entry["embed"].get("template")
        blanks = {name[1:-1]: "" for name in _PLACEHOLDER_RE.findall(str(template or ""))}
        builder.apply(template, {**blanks, **values})
    if entry.get("themeParam") and theme in THEMES:
        builder.apply("?theme={theme}", {"theme": str(theme)})
    return builder.build()


def handoff_code_supported(integration_key: str, value: str) -> bool:
    """handoff 도구가 받는 종목코드(기존 /api/portfolio/open 규칙 그대로)."""
    if integration_key == "eiayn":
        return bool(_ETF_CODE_RE.fullmatch(value))
    return is_korean_stock(value)


def handoff_stock_param(integration_key: str) -> str:
    """종목 쿼리 이름 — 레지스트리 stockLink 템플릿에서(buybacks 는 ``stock``)."""
    entry = tool_for_integration(integration_key) or {}
    template = ((entry.get("stockLink") or {}).get("template")) or "?code={code}"
    for name, value in parse_qsl(urlsplit(template).query, keep_blank_values=True):
        if value == "{code}":
            return name
    return "code"


def held_positions(integration_key: str, items: Iterable[dict]) -> list[str]:
    return sorted(
        f'{item["stock_code"]}:{item["quantity"]}' for item in items
        if item.get("quantity", 0) > 0 and handoff_code_supported(integration_key, str(item["stock_code"]))
    )


def handoff_url(base_url: str, integration_key: str, *, code: str, theme: str | None, positions: list[str]) -> str:
    """보유 스냅샷 handoff 목적지. ``theme`` 가 None 이면 싣지 않는다."""
    target = _base_parts(base_url)
    query: dict[str, str] = {}
    if theme in THEMES:
        query["theme"] = str(theme)
    code = (code or "").strip().upper()
    if handoff_code_supported(integration_key, code):
        query[handoff_stock_param(integration_key)] = code
    return urlunsplit((target.scheme, target.netloc, target.path.rstrip("/") + "/",
                       urlencode(query), urlencode({"vc-held": ",".join(positions)})))
