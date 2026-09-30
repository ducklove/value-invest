import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
REGISTRY = json.loads((ROOT / "config" / "ecosystem.json").read_text(encoding="utf-8"))
_LINK_RE = re.compile(r"\]\(([^)\s]+)\)")


def _registry_ids(*, include_hub_views: bool = False) -> list[str]:
    ids = [tool["id"] for tool in REGISTRY["tools"] if tool["id"] != "value-invest"]
    return ids if include_hub_views else [i for i in ids if not i.startswith("hub:")]


def test_linked_projects_documents_all_public_dashboard_integrations():
    docs = (DOCS / "linked-projects.md").read_text(encoding="utf-8")

    for project in (
        "holding_value",
        "common_preferred_spread",
        "spac-hunter",
        "buybacks",
        "gold_gap",
        "all-about-gold",
        "nps-tracker",
        "eiayn",
        "bond-mate",
        "index-popup",
        "kis-proxy",
        "finance-pi",
    ):
        assert project in docs, project

    # eiayn: ?theme 는 시각 테마, ETF 카테고리 필터는 ?etf_theme= (eiayn src/lib/searchState.js)
    assert "?etf_theme=" in docs
    # 보유 배지를 로드하는 대시보드 수는 레지스트리 heldBadges:true 항목 수와 같아야 한다.
    held = [t["id"] for t in REGISTRY["tools"] if t.get("heldBadges")]
    assert len(held) == 5
    assert "**다섯** 대시보드" in docs
    assert all(tool_id in docs for tool_id in held)
    assert REGISTRY["heldBadges"]["version"] in docs


def test_linked_projects_finance_pi_default_matches_code():
    from services.market.sources import finance_pi

    docs = (DOCS / "linked-projects.md").read_text(encoding="utf-8")
    assert finance_pi.DEFAULT_BASE_URL in docs


def test_ecosystem_index_lists_every_registry_tool():
    index = (DOCS / "ecosystem" / "README.md").read_text(encoding="utf-8")
    for tool_id in _registry_ids():
        assert f"| {tool_id} |" in index, tool_id


def test_ecosystem_docs_relative_links_resolve():
    pages = [
        *sorted((DOCS / "ecosystem").glob("*.md")),
        DOCS / "linked-projects.md",
        DOCS / "project-architecture-graph.md",
        DOCS / "archive" / "README.md",
    ]
    broken = []
    for page in pages:
        for target in _LINK_RE.findall(page.read_text(encoding="utf-8")):
            if target.startswith(("http://", "https://", "mailto:", "#")):
                continue
            path = target.split("#", 1)[0]
            if path and not (page.parent / path).resolve().exists():
                broken.append(f"{page.relative_to(ROOT)} -> {target}")
    assert not broken, broken
