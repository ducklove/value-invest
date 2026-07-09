from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _requirement_lines(path: str) -> list[str]:
    lines: list[str] = []
    for raw in (ROOT / path).read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line or line.startswith("-r "):
            continue
        lines.append(line)
    return lines


def _is_bounded_requirement(line: str) -> bool:
    if "==" in line:
        return True
    return ">=" in line and "<" in line


def test_python_direct_dependencies_have_floor_and_ceiling():
    for path in ("requirements.txt", "requirements-dev.txt"):
        unbounded = [line for line in _requirement_lines(path) if not _is_bounded_requirement(line)]
        assert unbounded == []


def test_dev_requirements_include_runtime_requirements():
    text = (ROOT / "requirements-dev.txt").read_text(encoding="utf-8")
    assert "-r requirements.txt" in text


def test_ci_and_deploy_use_locked_install_paths():
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    deploy = (ROOT / "deploy" / "deploy.sh").read_text(encoding="utf-8")

    assert "python -m pip install -r requirements-dev.txt" in ci
    assert "npm ci" in ci
    assert "-r requirements-dev.txt" in deploy
    assert "npm ci --no-audit --no-fund" in deploy
    assert (ROOT / "package-lock.json").exists()


def test_ci_runs_both_test_suites():
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert "python -m pytest -q" in ci
    assert "npm test" in ci


def test_deploy_gates_block_bad_deploys():
    """deploy.sh 의 3대 게이트가 무력화되지 않았는지 문자열로 고정한다.

    - JS 테스트: node 부재 시 조용히 skip 하던 구멍을 막았다 — 명시적
      SKIP_JS_TESTS=1 없이는 하드 게이트.
    - healthz: 실패 시 OLD_SHA 로 되돌리고 exit 1 로 배포를 차단한다.
    """
    deploy = (ROOT / "deploy" / "deploy.sh").read_text(encoding="utf-8")

    assert "npm test" in deploy
    assert "SKIP_JS_TESTS" in deploy
    assert "JS tests SKIPPED" not in deploy  # 과거 soft-skip 경고 문구의 부활 방지

    assert "wait_for_healthz" in deploy
    assert 'git reset --hard "$OLD_SHA"' in deploy


def test_repositories_do_not_import_service_layer():
    violations: list[str] = []
    for path in (ROOT / "repositories").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and (node.module or "").split(".", 1)[0] == "services":
                violations.append(f"{path.relative_to(ROOT)}:{node.lineno}")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.split(".", 1)[0] == "services":
                        violations.append(f"{path.relative_to(ROOT)}:{node.lineno}")
    assert violations == []
