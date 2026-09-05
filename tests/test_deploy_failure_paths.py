"""격리된 Git 저장소와 가짜 systemctl로 실제 배포 스크립트의 복구를 검사한다."""

import os
import shutil
import subprocess
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


@pytest.mark.skipif(not Path(BASH or "").is_file(), reason="Bash 실행 환경 필요")
@pytest.mark.parametrize("failure", ["restart", "health", "unit", "python", "none"])
def test_deploy_restores_code_units_and_environment(tmp_path, failure):
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
    (source / "requirements-dev.lock").write_text("")
    _run(["git", "add", "."], source)
    _run(["git", "commit", "-m", "old"], source)
    old = _run(["git", "rev-parse", "HEAD"], source).stdout.strip()
    _run(["git", "clone", str(source), str(app)], tmp_path)
    (units / "value-invest.service").write_text("old-unit\n")
    (source / "deploy/value-invest.service").write_text("new-unit\n")
    _run(["git", "commit", "-am", "new"], source)
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
if [[ "$1" == /bin/systemctl ]]; then
  shift
  if [[ "$1" == is-enabled || "$1" == is-active ]]; then echo disabled; exit 1; fi
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
    if os.name == "nt":
        # MSYS는 권한 없는 symlink를 디렉터리 복사로 흉내 낸다. 환경 선택만
        # 파일 포인터로 모의하고 실제 Linux symlink는 CI에서 검증한다.
        _script(bins / "ln", 'printf "%s\\n" "$2" >"$3"\n')
        _script(bins / "readlink", '[[ ! -f "$1" ]] || cat "$1"\n')
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
    assert code == (0 if failure == "none" else 1), log_path.read_text(encoding="utf-8")
    head = _run(["git", "rev-parse", "HEAD"], app).stdout.strip()
    assert head == (new if failure == "none" else old)
    assert (units / "value-invest.service").read_text() == ("new-unit\n" if failure == "none" else "old-unit\n")
    if failure != "none":
        assert not (app / ".venv-current").exists()
    if failure in {"restart", "health"}:
        assert (tmp_path / "rollback-restart").exists()
