"""Value Compass 생태계 도구 레지스트리 (``config/ecosystem.json``) 로더.

레지스트리는 허브와 형제 대시보드 목록의 유일한 정본이다. 여기서 파생되는 것:

- ``services.ecosystem.integrations.DEFAULT_BASE_URLS`` (``integrationKey`` 가 있는 항목, env override 유지)
- ``routes/portfolio.py`` ``/api/portfolio/open/{key}`` handoff 허용 목록 (``handoff: true``)
- ``/app-config.js`` 의 ``APP_CONFIG.ecosystem`` — :func:`public_projection` (공개 항목만)
- ``static/ecosystem/vc-shell.js`` 인라인 레지스트리 블록 (``scripts/sync-ecosystem.mjs`` 가
  같은 투영 규칙으로 생성하고, 테스트가 이 모듈의 결과와 바이트 비교한다)

``visibility: "internal"`` 항목(finance-pi, kis-proxy 등)은 서버 내부용이다. 브라우저로
나가는 투영과 형제 저장소에 vendoring 되는 파일에는 절대 포함하지 않는다.
"""

from __future__ import annotations

import ipaddress
import json
import os
import re
import threading
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

REGISTRY_PATH = Path(__file__).resolve().parent.parent / "config" / "ecosystem.json"

ICONS = frozenset({
    "building", "split", "rocket", "buyback", "etf", "gold", "coin", "pension",
    "bond", "chart", "compass", "grid", "gauge", "server",
})
VISIBILITIES = frozenset({"public", "internal"})
# 공개 항목에 들어가면 안 되는 운영 포트 (kis-proxy, finance-pi 등). 허브 3691 과
# index-popup 3358 은 공개 서비스라 허용한다.
INTERNAL_PORTS = frozenset({3288, 8400, 8765, 8790, 8801})
# 브라우저 투영에 싣는 필드와 순서. scripts/sync-ecosystem.mjs 의 PROJECTION_FIELDS 와
# 반드시 같아야 한다(test_ecosystem_registry 가 vc-shell.js 블록과 비교해 검증).
PROJECTION_FIELDS = (
    "id", "integrationKey", "name", "description", "category", "icon", "accent", "url",
    "deploy", "stockLink", "viewLink", "assetLink", "hubView", "embed", "themeParam",
    "handoff", "heldBadges",
)
_LINK_FIELDS = ("stockLink", "viewLink", "assetLink")


class EcosystemRegistryError(ValueError):
    """레지스트리 파일이 스키마를 어겼다 — 배포 전에 테스트/동기화 스크립트가 잡는다."""


_lock = threading.Lock()
_cache: dict[str, Any] = {"key": None, "data": None}


def load(path: Path | None = None) -> dict[str, Any]:
    """레지스트리를 읽어 검증한다. 파일 mtime 이 바뀔 때만 다시 읽는다."""
    target = Path(path) if path is not None else REGISTRY_PATH
    stat = target.stat()
    key = (str(target), stat.st_mtime_ns, stat.st_size)
    with _lock:
        if _cache["key"] == key:
            return _cache["data"]
    data = json.loads(target.read_text(encoding="utf-8"))
    validate(data)
    with _lock:
        _cache["key"] = key
        _cache["data"] = data
    return data


def _is_private_host(host: str) -> bool:
    try:
        addr = ipaddress.ip_address(host)
    except ValueError:
        return host in {"localhost"} or host.endswith(".local") or host.endswith(".lan")
    return addr.is_private or addr.is_loopback or addr.is_link_local


def _check_public_url(where: str, url: str, errors: list[str]) -> None:
    parts = urlsplit(url)
    if parts.scheme != "https" or not parts.hostname:
        errors.append(f"{where}: 공개 항목 URL 은 https 여야 합니다 ({url})")
        return
    if _is_private_host(parts.hostname):
        errors.append(f"{where}: 공개 항목에 사설 주소가 있습니다 ({url})")
    if parts.port in INTERNAL_PORTS:
        errors.append(f"{where}: 공개 항목에 내부 포트가 있습니다 ({url})")
    if parts.username or parts.password or parts.query:
        errors.append(f"{where}: URL 에 자격증명/쿼리를 넣지 마세요 ({url})")


def validate(data: Any) -> None:
    """스키마 검증. 문제를 모두 모아 한 번에 :class:`EcosystemRegistryError` 로 던진다."""
    errors: list[str] = []
    if not isinstance(data, dict):
        raise EcosystemRegistryError("레지스트리 최상위는 객체여야 합니다")
    if data.get("version") != 1:
        errors.append("version 은 1 이어야 합니다")
    hub = data.get("hub")
    if not isinstance(hub, str):
        errors.append("hub URL 이 없습니다")
    else:
        _check_public_url("hub", hub, errors)
    held = data.get("heldBadges")
    if not (isinstance(held, dict) and isinstance(held.get("version"), str) and isinstance(held.get("path"), str)):
        errors.append("heldBadges {version, path} 가 필요합니다")
    categories = data.get("categories")
    category_ids = [c.get("id") for c in categories if isinstance(c, dict)] if isinstance(categories, list) else []
    if not category_ids or len(set(category_ids)) != len(category_ids):
        errors.append("categories id 가 비었거나 중복입니다")
    tools = data.get("tools")
    if not isinstance(tools, list) or not tools:
        raise EcosystemRegistryError("; ".join(errors + ["tools 가 비었습니다"]))
    seen_ids: set[str] = set()
    seen_keys: set[str] = set()
    for index, tool in enumerate(tools):
        where = f"tools[{index}]"
        if not isinstance(tool, dict):
            errors.append(f"{where}: 객체가 아닙니다")
            continue
        tool_id = tool.get("id")
        where = f"tools[{tool_id or index}]"
        if not isinstance(tool_id, str) or not re.fullmatch(r"[a-z0-9][a-z0-9_:-]*", tool_id):
            errors.append(f"{where}: id 형식이 잘못됐습니다")
        elif tool_id in seen_ids:
            errors.append(f"{where}: id 중복")
        else:
            seen_ids.add(tool_id)
        key = tool.get("integrationKey")
        if key is not None:
            if not isinstance(key, str) or key in seen_keys:
                errors.append(f"{where}: integrationKey 중복/형식 오류")
            else:
                seen_keys.add(key)
        for field in ("name", "description"):
            if not isinstance(tool.get(field), str) or not tool.get(field):
                errors.append(f"{where}: {field} 가 필요합니다")
        if tool.get("category") not in category_ids:
            errors.append(f"{where}: 알 수 없는 category {tool.get('category')!r}")
        if tool.get("icon") not in ICONS:
            errors.append(f"{where}: 알 수 없는 icon {tool.get('icon')!r}")
        if not re.fullmatch(r"#[0-9a-fA-F]{6}", str(tool.get("accent") or "")):
            errors.append(f"{where}: accent 는 #rrggbb")
        visibility = tool.get("visibility")
        if visibility not in VISIBILITIES:
            errors.append(f"{where}: visibility 는 public|internal")
        url = tool.get("url")
        if visibility == "public":
            if not isinstance(url, str):
                errors.append(f"{where}: 공개 항목은 url 이 필요합니다")
            else:
                _check_public_url(where, url, errors)
        elif url is not None and not isinstance(url, str):
            errors.append(f"{where}: url 형식 오류")
        if key is not None and not isinstance(url, str):
            errors.append(f"{where}: integrationKey 가 있으면 url 이 필요합니다")
        env = tool.get("envOverride")
        if env is not None and not re.fullmatch(r"[A-Z][A-Z0-9_]*", str(env)):
            errors.append(f"{where}: envOverride 형식 오류")
        for field in _LINK_FIELDS:
            link = tool.get(field)
            if link is None:
                continue
            if not isinstance(link, dict) or not isinstance(link.get("template"), str):
                errors.append(f"{where}: {field}.template 이 필요합니다")
                continue
            try:
                re.compile(str(link.get("accepts")))
            except re.error as exc:
                errors.append(f"{where}: {field}.accepts 정규식 오류 ({exc})")
        for flag in ("themeParam", "handoff", "heldBadges"):
            if not isinstance(tool.get(flag), bool):
                errors.append(f"{where}: {flag} 는 bool")
        if tool.get("handoff") and not key:
            errors.append(f"{where}: handoff 도구는 integrationKey 가 필요합니다")
        if not isinstance(tool.get("data"), list):
            errors.append(f"{where}: data 는 배열")
        vendor = tool.get("vendor")
        if vendor is not None:
            if not isinstance(vendor, dict) or not all(isinstance(vendor.get(k), str) for k in ("html", "dir", "src")):
                errors.append(f"{where}: vendor {{html, dir, src}} 가 필요합니다")
            elif not all(isinstance(vendor.get(k), bool) for k in ("shell", "themeBoot")):
                errors.append(f"{where}: vendor.shell / vendor.themeBoot 는 bool")
            for k in ("python", "js"):
                if isinstance(vendor, dict) and vendor.get(k) is not None and not isinstance(vendor.get(k), str):
                    errors.append(f"{where}: vendor.{k} 는 경로 문자열 또는 null")
            if visibility != "public":
                errors.append(f"{where}: 내부 항목은 vendoring 대상이 아닙니다")
        if visibility == "public":
            _check_no_internal_leak(where, tool, errors)
    if errors:
        raise EcosystemRegistryError("; ".join(errors))


def _check_no_internal_leak(where: str, tool: dict[str, Any], errors: list[str]) -> None:
    text = json.dumps(tool, ensure_ascii=False)
    if re.search(r"\b(?:10|127|192\.168|172\.(?:1[6-9]|2\d|3[01]))\.\d{1,3}\.\d{1,3}", text):
        errors.append(f"{where}: 공개 항목에 사설 IP 가 있습니다")
    for port in INTERNAL_PORTS:
        if re.search(rf":{port}\b", text):
            errors.append(f"{where}: 공개 항목에 내부 포트 :{port} 가 있습니다")


def registry() -> dict[str, Any]:
    return load()


def tools() -> list[dict[str, Any]]:
    return list(load()["tools"])


def public_tools() -> list[dict[str, Any]]:
    return [t for t in tools() if t.get("visibility") == "public"]


def tool(tool_id: str) -> dict[str, Any] | None:
    return next((t for t in tools() if t["id"] == tool_id), None)


def resolved_url(entry: dict[str, Any]) -> str | None:
    """env override 를 반영한 기본 URL (끝 슬래시 제거). integrations._base_url 과 같은 규칙."""
    url = entry.get("url")
    if url is None:
        return None
    env = entry.get("envOverride")
    return (os.getenv(env, url) if env else url).rstrip("/")


def default_base_urls() -> dict[str, str]:
    """integrationKey → 레지스트리 기본 URL (env 미반영). integrations.DEFAULT_BASE_URLS 의 원천."""
    return {t["integrationKey"]: t["url"] for t in tools() if t.get("integrationKey")}


def handoff_keys() -> frozenset[str]:
    return frozenset(t["integrationKey"] for t in tools() if t.get("handoff") and t.get("integrationKey"))


def held_badges() -> dict[str, str]:
    return dict(load()["heldBadges"])


def project_tool(entry: dict[str, Any], *, resolve_env: bool = True) -> dict[str, Any]:
    """브라우저용 도구 한 항목. None 필드는 뺀다(키 순서 = PROJECTION_FIELDS)."""
    out: dict[str, Any] = {}
    for field in PROJECTION_FIELDS:
        value = entry.get(field)
        if field == "url" and resolve_env:
            value = resolved_url(entry)
        if value is None:
            continue
        out[field] = value
    return out


def public_projection(*, resolve_env: bool = True) -> dict[str, Any]:
    """``APP_CONFIG.ecosystem`` 과 vc-shell.js 인라인 블록의 공통 모양.

    ``resolve_env=False`` 는 vendoring 용(형제 저장소에는 기본 공개 URL 만 들어간다).
    """
    data = load()
    public = [t for t in data["tools"] if t.get("visibility") == "public"]
    used = {t["category"] for t in public}
    return {
        "version": data["version"],
        "hub": data["hub"],
        "categories": [dict(c) for c in data["categories"] if c["id"] in used],
        "tools": [project_tool(t, resolve_env=resolve_env) for t in public],
    }
