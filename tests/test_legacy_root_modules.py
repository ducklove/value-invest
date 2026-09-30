"""레거시 루트 모듈 → services 추출(D-18) 회귀 방지.

services/ 로 옮긴 루트 모듈은 (1) 루트에 다시 생기지 않고, (2) 저장소 코드가
옛 최상위 이름으로 import 하지 않아야 한다. 경로로 import 하는 외부 사용처
(deploy/repairs 의 1회성 복구 스크립트 등)가 있어 호환 shim 을 남긴 모듈은
``SHIMMED`` 에 두고, shim 이 정본을 그대로 가리키는지만 확인한다.
"""

from __future__ import annotations

import ast
import importlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# 옛 루트 모듈 이름 → 새 정본 모듈.
MOVED: dict[str, str] = {
    "linked_project_admin": "services.ecosystem.linked_admin",
    "market_movers": "services.market.movers",
    "market_news": "services.market.news",
    "market_sessions": "services.market.sessions",
    "preferred_dividends": "services.dividends.preferred",
    "foreign_dividends": "services.dividends.foreign",
    "snapshot_intraday": "services.portfolio.intraday_snapshot",
}

# 루트 shim 을 남긴 모듈(외부가 경로로 import). 저장소 안 코드는 여전히 정본을 쓴다.
SHIMMED: frozenset[str] = frozenset()

# 이미 실행된 1회성 복구 스크립트는 리팩터링하지 않는다(마커 기반, 경로 import 유지).
_SKIP_DIRS = {"node_modules", "__pycache__", "static"}  # + 모든 dot 디렉터리(.venv, .claude 워크트리 …)
_SKIP_PREFIXES = (("deploy", "repairs"),)


def _repo_python_files():
    for path in ROOT.rglob("*.py"):
        rel = path.relative_to(ROOT).parts
        if _SKIP_DIRS.intersection(rel) or any(part.startswith(".") for part in rel[:-1]):
            continue
        if any(rel[: len(prefix)] == prefix for prefix in _SKIP_PREFIXES):
            continue
        yield path


def test_moved_modules_are_not_back_at_repo_root():
    for name in MOVED:
        exists = (ROOT / f"{name}.py").exists()
        assert exists == (name in SHIMMED), name


def test_repo_code_imports_moved_modules_from_canonical_location():
    violations: list[str] = []
    for path in _repo_python_files():
        if path.parent == ROOT and path.stem in SHIMMED:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0:
                names = [node.module or ""]
            else:
                continue
            for name in names:
                if name.split(".", 1)[0] in MOVED:
                    violations.append(f"{path.relative_to(ROOT)}:{node.lineno} {name}")
    assert violations == []


def test_moved_modules_import_from_canonical_location():
    for canonical in MOVED.values():
        assert importlib.import_module(canonical).__name__ == canonical


def test_shims_point_at_canonical_module():
    for name in SHIMMED:
        assert importlib.import_module(name) is importlib.import_module(MOVED[name])
