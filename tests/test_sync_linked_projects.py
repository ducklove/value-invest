"""scripts/sync_linked_projects.sh — 형제 저장소 데이터 파일 동기화(실제 git 저장소로 검증)."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts" / "sync_linked_projects.sh"

pytestmark = pytest.mark.skipif(shutil.which("git") is None or shutil.which("bash") is None,
                                reason="git/bash 필요")

GIT_ENV = {
    "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.com",
    "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.com",
    "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_SYSTEM": os.devnull,
}


def git(cwd: Path, *args: str) -> str:
    env = {**os.environ, **GIT_ENV}
    return subprocess.run(["git", *args], cwd=cwd, env=env, check=True, capture_output=True, text=True).stdout


def commit_file(repo: Path, name: str, text: str, message: str) -> None:
    (repo / name).write_text(text, encoding="utf-8")
    git(repo, "add", name)
    git(repo, "commit", "-q", "-m", message)


def make_gold_gap_origin(path: Path) -> None:
    """gold_gap 모양: master 에는 config.json 만, data.json 은 orphan data 브랜치에만."""
    path.mkdir()
    git(path, "init", "-q", "-b", "master")
    commit_file(path, "config.json", '{"assets": {}}\n', "config")
    git(path, "checkout", "-q", "--orphan", "data")
    git(path, "rm", "-q", "-rf", ".")
    commit_file(path, "data.json", '{"v": 1}\n', "data v1")
    git(path, "checkout", "-q", "master")


def run_sync(root: Path) -> subprocess.CompletedProcess:
    env = {**os.environ, **GIT_ENV, "LINKED_PROJECTS_ROOT": str(root)}
    return subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True, timeout=60)


@pytest.mark.parametrize("single_branch", [False, True])
def test_gold_gap_data_is_refreshed_from_the_data_branch(tmp_path, single_branch):
    origin = tmp_path / "origin-gold_gap"
    make_gold_gap_origin(origin)
    root = tmp_path / "works"
    root.mkdir()
    clone_args = ["clone", "-q", "-b", "master"] + (["--single-branch"] if single_branch else [])
    git(tmp_path, *clone_args, str(origin), str(root / "gold_gap"))
    local = root / "gold_gap"
    # 관리자 편집으로 dirty 한 config.json 은 그대로 남아야 한다.
    (local / "config.json").write_text('{"assets": {"gold": {"thresholdPct": 7}}}\n', encoding="utf-8")

    git(origin, "checkout", "-q", "data")
    commit_file(origin, "data.json", '{"v": 2}\n', "data v2")
    git(origin, "checkout", "-q", "master")

    result = run_sync(root)

    assert result.returncode == 0, result.stdout + result.stderr
    assert (local / "data.json").read_text(encoding="utf-8") == '{"v": 2}\n'
    assert "origin/data" in result.stdout
    assert "thresholdPct" in (local / "config.json").read_text(encoding="utf-8")
    assert "skip: hodling-value|holding_value" in result.stdout
    # 인덱스·브랜치는 건드리지 않는다.
    assert git(local, "rev-parse", "--abbrev-ref", "HEAD").strip() == "master"
    assert not (local / "data.json.sync.tmp").exists()


def test_missing_data_branch_is_reported(tmp_path):
    origin = tmp_path / "origin-gold_gap"
    origin.mkdir()
    git(origin, "init", "-q", "-b", "master")
    commit_file(origin, "config.json", "{}\n", "config")
    root = tmp_path / "works"
    root.mkdir()
    git(tmp_path, "clone", "-q", str(origin), str(root / "gold_gap"))

    result = run_sync(root)

    assert result.returncode == 1
    assert "origin/data" in result.stdout
    assert not (root / "gold_gap" / "data.json").exists()
