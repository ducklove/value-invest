"""scripts/notify_failure.sh — systemd 실패 알림이 허브 관리자 채널로 가는지."""

import json
import os
import stat
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "notify_failure.sh"


def _fake_bin(tmp_path: Path, curl_body: str) -> tuple[Path, Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    calls = tmp_path / "curl_calls.jsonl"
    (bin_dir / "curl").write_text(
        "#!/usr/bin/env python3\n"
        "import json, sys\n"
        f"open({str(calls)!r}, 'a').write(json.dumps(sys.argv[1:]) + '\\n')\n"
        f"sys.stdout.write({curl_body!r})\n"
    )
    (bin_dir / "journalctl").write_text("#!/bin/sh\necho 'line1'\necho 'curl: (22) error: 500'\n")
    (bin_dir / "hostname").write_text("#!/bin/sh\necho pi-test\n")
    for name in ("curl", "journalctl", "hostname"):
        path = bin_dir / name
        path.chmod(path.stat().st_mode | stat.S_IEXEC)
    return bin_dir, calls


def _run(tmp_path: Path, bin_dir: Path, extra_env: dict[str, str]) -> subprocess.CompletedProcess:
    env = {"PATH": f"{bin_dir}:{os.environ['PATH']}", **extra_env}
    return subprocess.run(
        ["bash", str(SCRIPT), "portfolio-snapshot.service"],
        env=env, capture_output=True, text=True, timeout=30, check=False,
    )


def _calls(path: Path) -> list[list[str]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_posts_to_hub_admins_and_skips_ntfy_when_delivered(tmp_path):
    bin_dir, calls = _fake_bin(tmp_path, '{"ok": true, "sent": 1, "users": 1}')
    result = _run(tmp_path, bin_dir, {"NTFY_TOPIC": "topic-x"})
    assert result.returncode == 0
    made = _calls(calls)
    assert len(made) == 1  # 허브가 보냈으면 ntfy 는 부르지 않는다
    args = made[0]
    assert "https://127.0.0.1:3691/api/internal/notify" in args
    payload = json.loads(args[args.index("--data") + 1])
    assert payload["audience"] == "admins"
    assert payload["source"] == "systemd"
    assert "portfolio-snapshot.service" in payload["title"]
    assert "error: 500" in payload["text"]


def test_falls_back_to_ntfy_when_hub_sends_nothing(tmp_path):
    bin_dir, calls = _fake_bin(tmp_path, '{"ok": true, "sent": 0, "users": 0}')
    result = _run(tmp_path, bin_dir, {"NTFY_TOPIC": "topic-x"})
    assert result.returncode == 0
    made = _calls(calls)
    assert len(made) == 2
    assert made[1][-1] == "https://ntfy.sh/topic-x"


def test_sends_internal_token_header_when_configured(tmp_path):
    bin_dir, calls = _fake_bin(tmp_path, '{"ok": true, "sent": 2}')
    _run(tmp_path, bin_dir, {"INTERNAL_API_TOKEN": "tok"})
    assert "X-Internal-Token: tok" in _calls(calls)[0]


def test_no_ntfy_topic_and_hub_down_exits_quietly(tmp_path):
    bin_dir, calls = _fake_bin(tmp_path, "")
    result = _run(tmp_path, bin_dir, {})
    assert result.returncode == 0
    assert "NTFY_TOPIC unset" in result.stderr
