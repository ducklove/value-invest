"""switchView 가 pushState 하는 경로는 서버도 index.html 로 서빙해야 한다.

PF_VIEW_PATHS(static/js/portfolio-shell.js)에만 있고 SPA_PATHS(core/static_routes.py)에
없는 경로가 생기면, 그 탭에서 새로고침하거나 주소를 직접 열 때 404 JSON 이 뜬다
(/investing 이 실제 사례).
"""
from __future__ import annotations

import re
from pathlib import Path

from core.static_routes import SPA_PATHS

ROOT = Path(__file__).resolve().parents[1]


def _client_view_paths() -> set[str]:
    shell = (ROOT / "static" / "js" / "portfolio-shell.js").read_text(encoding="utf-8")
    match = re.search(r"const PF_VIEW_PATHS = \{(.*?)\};", shell, re.S)
    assert match, "PF_VIEW_PATHS 블록을 찾지 못했다"
    return set(re.findall(r"'(/[a-z-]+)'", match.group(1)))


def test_every_client_view_path_is_served_as_spa_route():
    missing = _client_view_paths() - set(SPA_PATHS)
    assert not missing, f"SPA_PATHS 에 빠진 경로: {sorted(missing)}"
