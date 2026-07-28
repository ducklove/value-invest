from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_linked_projects_documents_all_public_dashboard_integrations():
    docs = (ROOT / "docs" / "linked-projects.md").read_text(encoding="utf-8")

    for project in (
        "holding_value",
        "common_preferred_spread",
        "spac-hunter",
        "buybacks",
        "gold_gap",
        "nps-tracker",
        "eiayn",
    ):
        assert project in docs

    assert "?etf_theme=" in docs
