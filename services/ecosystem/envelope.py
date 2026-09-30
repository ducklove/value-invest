"""발행 데이터 계약 v1 envelope 검증 (docs/ecosystem/data-contract.md §2, §7).

구조·해시 검증은 허브 정본 헬퍼 ``ecosystem/python/vc_publish.py`` 의
``validate_envelope`` 를 그대로 쓴다. 그 파일은 형제 저장소로 바이트 그대로
벤더링되는 단일 파일이라 패키지가 아니다 — 파일 경로로 한 번 로드한다.

그 위에 허브 소비 규칙을 더한다: ``schemaVersion == 1``, ``tool`` 이 요청한 도구와
같을 것, ``data`` 가 ``config/schemas/summary/<tool>.schema.json`` 의 필수 키를 갖출 것
(jsonschema 의존성 없이 required/컨테이너 타입만 본다). 하나라도 어기면
:class:`SummaryRejected` — 호출부는 그 도구만 레거시 파일로 폴백한다.
"""

from __future__ import annotations

import importlib.util
import json
import threading
from pathlib import Path
from types import ModuleType
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[2]
VC_PUBLISH_PATH = ROOT / "ecosystem" / "python" / "vc_publish.py"
SCHEMA_DIR = ROOT / "config" / "schemas" / "summary"

_lock = threading.Lock()
_module: ModuleType | None = None
_schemas: dict[str, dict | None] = {}


class SummaryRejected(ValueError):
    """summary.json 이 계약을 어겼다 — 그 도구만 레거시로 폴백한다."""


def vc_publish() -> ModuleType:
    global _module
    with _lock:
        if _module is None:
            spec = importlib.util.spec_from_file_location("vc_publish_hub", VC_PUBLISH_PATH)
            if spec is None or spec.loader is None:  # pragma: no cover - 저장소 손상
                raise ImportError(f"cannot load {VC_PUBLISH_PATH}")
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            _module = module
        return _module


def summary_schema(tool_id: str) -> dict | None:
    """도구별 summary 스키마(없으면 None — 그 도구는 summary 를 소비하지 않는다)."""
    with _lock:
        if tool_id not in _schemas:
            path = SCHEMA_DIR / f"{tool_id}.schema.json"
            _schemas[tool_id] = json.loads(path.read_text(encoding="utf-8")) if path.exists() else None
        return _schemas[tool_id]


def summary_tool_ids() -> list[str]:
    """summary 스키마가 정의된 도구 id 목록(정렬)."""
    return sorted(p.name[: -len(".schema.json")] for p in SCHEMA_DIR.glob("*.schema.json"))


def _types(schema: Mapping[str, Any]) -> set[str]:
    t = schema.get("type")
    if isinstance(t, str):
        return {t}
    if isinstance(t, list):
        return {str(x) for x in t}
    return set()


def _check_required(value: Any, schema: Mapping[str, Any], where: str) -> None:
    types = _types(schema)
    if value is None:
        if types and "null" not in types:
            raise SummaryRejected(f"{where}: null 이 허용되지 않습니다")
        return
    if "object" in types and not isinstance(value, Mapping):
        if types <= {"object", "null"}:
            raise SummaryRejected(f"{where}: 객체여야 합니다")
        return
    if "array" in types and not isinstance(value, list):
        if types <= {"array", "null"}:
            raise SummaryRejected(f"{where}: 배열이어야 합니다")
        return
    if isinstance(value, Mapping):
        missing = [k for k in schema.get("required") or [] if k not in value]
        if missing:
            raise SummaryRejected(f"{where}: 필수 키 누락 {', '.join(missing)}")
        props = schema.get("properties") or {}
        for key, sub in props.items():
            if key in value and isinstance(sub, Mapping):
                _check_required(value[key], sub, f"{where}.{key}")
    elif isinstance(value, list):
        items = schema.get("items")
        if isinstance(items, Mapping):
            for i, item in enumerate(value):
                _check_required(item, items, f"{where}[{i}]")


def validate_summary(obj: Any, tool_id: str) -> dict[str, Any]:
    """검증된 envelope 를 돌려준다. 계약 위반이면 :class:`SummaryRejected`."""
    vp = vc_publish()
    try:
        vp.validate_envelope(obj)
    except vp.EnvelopeError as exc:
        raise SummaryRejected(str(exc)) from exc
    if obj.get("schemaVersion") != 1:
        raise SummaryRejected("schemaVersion 1 만 지원합니다")
    if obj.get("tool") != tool_id:
        raise SummaryRejected(f"tool 불일치: {obj.get('tool')!r} != {tool_id!r}")
    schema = summary_schema(tool_id)
    if schema is None:
        raise SummaryRejected(f"{tool_id}: summary 스키마가 없습니다")
    _check_required(obj["data"], schema, "data")
    return dict(obj)
