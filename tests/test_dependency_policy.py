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

    assert "python -m pip install --require-hashes -r requirements-dev.lock" in ci
    assert "npm ci" in ci
    assert "--require-hashes -r requirements-dev.lock" in deploy
    assert "npm ci --no-audit --no-fund" in deploy
    assert (ROOT / "package-lock.json").exists()
    assert (ROOT / "requirements.lock").exists()
    assert (ROOT / "requirements-dev.lock").exists()


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


def test_all_repo_units_are_installed_by_deploy():
    """저장소 루트의 systemd 유닛은 전부 deploy.sh 의 REPO_UNITS 에 있어야 한다.

    등록을 빠뜨리면 파일만 커밋되고 서버에는 영영 설치되지 않는다 — 조용히
    안 도는 타이머가 된다.
    """
    deploy = (ROOT / "deploy" / "deploy.sh").read_text(encoding="utf-8")
    units = sorted(p.name for p in ROOT.glob("*.service")) + sorted(p.name for p in ROOT.glob("*.timer"))

    assert units, "루트에 유닛 파일이 하나도 없다 — 글롭이 깨졌는지 확인"
    missing = [unit for unit in units if f'"{unit}"' not in deploy]
    assert missing == []


def test_linked_project_sync_never_overwrites_admin_edited_config():
    """연계 프로젝트 동기화는 데이터 파일만 덮는다.

    config.json 은 /admin.html 이 서버 위에서 직접 편집하는 파일이라 pull/merge
    나 config.json 덮어쓰기가 들어오면 관리자 편집이 날아간다.
    """
    script = (ROOT / "scripts" / "sync_linked_projects.sh").read_text(encoding="utf-8")
    code = "\n".join(
        line for line in script.splitlines() if line.strip() and not line.lstrip().startswith("#")
    )

    assert "config.json" not in code
    assert "git pull" not in code
    assert "reset --hard" not in code


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


def test_shared_connection_writers_never_commit_directly():
    violations = []
    for path in (ROOT / "repositories").glob("*.py"):
        # 부트스트랩은 요청 수락 전 스키마를 만들고, db가 트랜잭션을 소유한다.
        if path.name in {"bootstrap.py", "db.py"}:
            continue
        source = path.read_text(encoding="utf-8")
        for function in ast.parse(source).body:
            if not isinstance(function, ast.AsyncFunctionDef):
                continue
            body = ast.get_source_segment(source, function)
            if "get_db()" in body and ".commit()" in body:
                violations.append(f"{path.name}:{function.name}")
    assert violations == []
