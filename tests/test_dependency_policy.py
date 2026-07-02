from __future__ import annotations

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
