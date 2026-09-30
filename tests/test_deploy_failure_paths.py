"""격리된 Git 저장소와 가짜 systemctl로 실제 배포 스크립트의 복구를 검사한다."""

import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BASH = shutil.which("bash") if os.name != "nt" else r"C:\Program Files\Git\bin\bash.exe"


def _run(args, cwd, **kwargs):
    return subprocess.run(args, cwd=cwd, check=True, capture_output=True, text=True, encoding="utf-8", **kwargs)


def _script(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/usr/bin/env bash\n" + text, encoding="utf-8", newline="\n")
    path.chmod(0o755)


def _prepare_and_run_deploy(tmp_path, failure, seed=None):
    """Build origin/app repos + fake binaries, optionally seed app state, run deploy.sh.

    Returns (code, log_text, app, units, old_sha, new_sha).
    """
    source, app, units, bins = [tmp_path / name for name in ("origin", "app", "units", "bin")]
    source.mkdir()
    units.mkdir()
    bins.mkdir()
    _run(["git", "init", "-b", "master"], source)
    _run(["git", "config", "user.email", "test@example.invalid"], source)
    _run(["git", "config", "user.name", "배포 검증"], source)
    (source / "deploy").mkdir()
    shutil.copyfile(ROOT / "deploy/deploy.sh", source / "deploy/deploy.sh")
    _script(source / "deploy/migrate_env_to_single_file.sh", "exit 0\n")
    _script(source / "deploy/repairs/run_one_time_repairs.sh", "exit 0\n")
    (source / "deploy/value-invest.service").write_text("old-unit\n")
    (source / "portfolio-snapshot.timer").write_text("old-timer\n")
    (source / "requirements-dev.lock").write_text("")
    _run(["git", "add", "."], source)
    _run(["git", "commit", "-m", "old"], source)
    old = _run(["git", "rev-parse", "HEAD"], source).stdout.strip()
    _run(["git", "clone", str(source), str(app)], tmp_path)
    (units / "value-invest.service").write_text("old-unit\n")
    (units / "portfolio-snapshot.timer").write_text("old-timer\n")
    (source / "deploy/value-invest.service").write_text("new-unit\n")
    (source / "portfolio-snapshot.timer").write_text("new-timer\n")
    for name in ("portfolio-after-close.timer", "portfolio-after-close.service"):
        (source / name).write_text("new-after-close-unit\n")
    _run(["git", "add", "."], source)
    _run(["git", "commit", "-m", "new"], source)
    new = _run(["git", "rev-parse", "HEAD"], source).stdout.strip()

    _script(bins / "python3", '''
if [[ "$1" == -m && "$2" == venv ]]; then
  mkdir -p "$3/bin"
  cat >"$3/bin/python" <<'PY'
#!/usr/bin/env bash
[[ "$FAILURE" != python || "$*" != *pytest* ]]
PY
  chmod +x "$3/bin/python"
fi
''')
    _script(bins / "sudo", '''
printf '%s\\n' "$*" >>"$TEST_STATE/unit-commands"
if [[ "$1" == /bin/systemctl ]]; then
  shift
  if [[ "$1" == is-enabled || "$1" == is-active ]]; then echo disabled; exit 1; fi
  if [[ "$1" == stop && "$2" == portfolio-after-close.timer && ! -f "$UNIT_DST/$2" ]]; then exit 5; fi
  if [[ "$1" == restart && "$2" == value-invest.service ]]; then
    if [[ ! -f "$TEST_STATE/restarted" ]]; then
      touch "$TEST_STATE/restarted"
      [[ "$FAILURE" != restart ]] || exit 1
    else touch "$TEST_STATE/rollback-restart"; fi
  fi
  exit 0
fi
if [[ "$1" == cp && "$FAILURE" == unit && "$2" != *"/units/"* ]]; then exit 1; fi
exec "$@"
''')
    _script(bins / "curl", '''
[[ "$FAILURE" != health || -f "$TEST_STATE/rollback-restart" ]]
''')
    for name in ("npm", "node", "sleep"):
        _script(bins / name, "exit 0\n")
    if sys.platform == "darwin":
        # BSD mv에는 -T가 없다. 이 테스트의 GNU mv -Tf(원자적 링크 교체)를
        # 같은 rename 동작으로 모의한다. Linux CI에서는 실제 GNU mv를 쓴다.
        _script(bins / "mv", '[ "$#" = 3 ] && [ "$1" = -Tf ] || exit 1\n'
                + f'exec {shlex.quote(sys.executable)} -c \'import os, sys; os.replace(sys.argv[1], sys.argv[2])\' "$2" "$3"\n')
    if os.name == "nt":
        # MSYS는 권한 없는 symlink를 디렉터리 복사로 흉내 낸다. 환경 선택만
        # 파일 포인터로 모의하고 실제 Linux symlink는 CI에서 검증한다.
        _script(bins / "ln", 'printf "%s\\n" "$2" >"$3"\n')
        _script(bins / "readlink", '[[ ! -f "$1" ]] || cat "$1"\n')
    if seed is not None:
        seed(app)
    env = {**os.environ, "FAILURE": failure, "TEST_STATE": tmp_path.as_posix(),
           "APP_DIR": app.as_posix(), "UNIT_DST": units.as_posix(),
           "PATH": str(bins) + os.pathsep + os.environ["PATH"]}
    env["TEST_BIN"] = ("/" + bins.drive[0].lower() + bins.as_posix()[2:]) if os.name == "nt" else str(bins)
    log_path = tmp_path / "deploy.log"
    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen([BASH, "-c", 'export PATH="$TEST_BIN:$PATH"; exec bash -x deploy/deploy.sh'],
                                   cwd=app, env=env, stdout=log, stderr=log)
        try:
            code = process.wait(timeout=90)
        except subprocess.TimeoutExpired:
            if os.name == "nt":
                subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"], capture_output=True)
            else:
                process.kill()
            raise AssertionError(log_path.read_text(encoding="utf-8")) from None
    return code, log_path.read_text(encoding="utf-8"), app, units, old, new


@pytest.mark.skipif(not Path(BASH or "").is_file(), reason="Bash 실행 환경 필요")
@pytest.mark.parametrize("failure", ["restart", "health", "unit", "python", "none"])
def test_deploy_restores_code_units_and_environment(tmp_path, failure):
    code, log_text, app, units, old, new = _prepare_and_run_deploy(tmp_path, failure)
    assert code == (0 if failure == "none" else 1), log_text
    head = _run(["git", "rev-parse", "HEAD"], app).stdout.strip()
    assert head == (new if failure == "none" else old)
    assert (units / "value-invest.service").read_text() == ("new-unit\n" if failure == "none" else "old-unit\n")
    assert (units / "portfolio-snapshot.timer").read_text() == ("new-timer\n" if failure == "none" else "old-timer\n")
    for name in ("portfolio-after-close.timer", "portfolio-after-close.service"):
        assert (units / name).exists() == (failure == "none")
    if failure == "none":
        commands = (tmp_path / "unit-commands").read_text().splitlines()
        assert commands.index("/bin/systemctl stop portfolio-snapshot.timer") < commands.index("/bin/systemctl daemon-reload")
        assert commands.index("/bin/systemctl restart value-invest.service") < commands.index("/bin/systemctl restart portfolio-snapshot.timer")
        assert "/bin/systemctl enable --now portfolio-snapshot.timer" not in commands
        assert "/bin/systemctl stop portfolio-after-close.timer" not in commands
        assert commands.index("/bin/systemctl daemon-reload") < commands.index("/bin/systemctl enable portfolio-after-close.timer")
        assert commands.index("/bin/systemctl restart value-invest.service") < commands.index("/bin/systemctl restart portfolio-after-close.timer")
    if failure != "none":
        assert not (app / ".venv-current").exists()
    if failure in {"restart", "health"}:
        assert (tmp_path / "rollback-restart").exists()


_SHA = "{:040x}".format


def _seed_old_deploys(app):
    """Three old venvs + a non-SHA dir, .venv-current -> the previous venv, and
    four old .deploy-state dirs (one with a staged source tree)."""
    venvs = app / ".venvs"
    for n in (1, 2, 3):
        (venvs / _SHA(n) / "bin").mkdir(parents=True)
        (venvs / _SHA(n) / ".installed").touch()
    (venvs / "keep-me-not-a-sha").mkdir()
    os.symlink(str(venvs / _SHA(3)), str(app / ".venv-current"))
    for stamp in ("100-1", "200-2", "300-3", "300-4"):
        state = app / ".deploy-state" / stamp
        (state / "units").mkdir(parents=True)
        (state / "env").write_text(f"SECRET={stamp}\n")
    (app / ".deploy-state" / "300-4" / "source" / "node_modules").mkdir(parents=True)
    # 이번 배포보다 나중(epoch 가 미래)인 디렉터리는 절대 건드리지 않는다.
    (app / ".deploy-state" / "9999999999-1").mkdir()


@pytest.mark.skipif(not Path(BASH or "").is_file() or os.name == "nt", reason="Bash + symlink 필요")
def test_healthy_deploy_prunes_old_venvs_and_deploy_state(tmp_path):
    code, log_text, app, _units, _old, new = _prepare_and_run_deploy(tmp_path, "none", seed=_seed_old_deploys)
    assert code == 0, log_text
    assert "log 'WARNING: pruning" not in log_text, log_text
    venvs = sorted(p.name for p in (app / ".venvs").iterdir())
    # 현재(new) + 직전(.venv-current 였던 SHA 3 — 롤백 대상) + SHA 형식 아닌 디렉터리만 남는다.
    assert venvs == sorted([new, _SHA(3), "keep-me-not-a-sha"]), log_text
    assert os.readlink(app / ".venv-current") == str(app / ".venvs" / new)
    states = sorted(p.name for p in (app / ".deploy-state").iterdir())
    assert "300-4" in states and "9999999999-1" in states, states
    assert not {"100-1", "200-2", "300-3"} & set(states), states
    assert len(states) == 3, states  # current + previous + future
    current = [s for s in states if s not in {"300-4", "9999999999-1"}]
    assert len(current) == 1
    # 스테이징 소스(node_modules 포함)는 이번 것도 직전 것도 지운다. .env 사본은 유지.
    assert not (app / ".deploy-state" / current[0] / "source").exists()
    assert not (app / ".deploy-state" / "300-4" / "source").exists()
    assert (app / ".deploy-state" / "300-4" / "env").read_text() == "SECRET=300-4\n"


@pytest.mark.skipif(not Path(BASH or "").is_file() or os.name == "nt", reason="Bash + symlink 필요")
@pytest.mark.parametrize("failure", ["health", "python"])
def test_failed_deploy_prunes_nothing_and_rolls_back_to_previous_venv(tmp_path, failure):
    code, log_text, app, _units, old, _new = _prepare_and_run_deploy(tmp_path, failure, seed=_seed_old_deploys)
    assert code == 1, log_text
    assert _run(["git", "rev-parse", "HEAD"], app).stdout.strip() == old
    # 롤백 대상 venv 는 그대로 있고 .venv-current 는 다시 그것을 가리킨다.
    assert os.readlink(app / ".venv-current") == str(app / ".venvs" / _SHA(3))
    assert (app / ".venvs" / _SHA(3) / ".installed").exists()
    for n in (1, 2):
        assert (app / ".venvs" / _SHA(n)).is_dir()
    for stamp in ("100-1", "200-2", "300-3", "300-4", "9999999999-1"):
        assert (app / ".deploy-state" / stamp).is_dir()
    assert "Pruning" not in log_text
