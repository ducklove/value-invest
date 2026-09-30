"""linked_project_admin 공개 config.json 주소 — 레지스트리(config/ecosystem.json) + envOverride."""

from services.ecosystem import linked_admin as linked_project_admin


def test_public_config_urls_come_from_registry():
    urls = {key: linked_project_admin._remote_config_url(spec) for key, spec in linked_project_admin.PROJECT_SPECS.items()}
    assert urls == {
        "holdingValue": "https://ducklove.github.io/holding_value/config.json",
        "preferredSpread": "https://ducklove.github.io/common_preferred_spread/config.json",
        "goldGap": "https://ducklove.github.io/gold_gap/config.json",
    }


def test_public_config_url_honours_env_override(monkeypatch):
    monkeypatch.setenv("GOLD_GAP_BASE_URL", "https://mirror.example/gg/")
    spec = linked_project_admin.PROJECT_SPECS["goldGap"]
    assert linked_project_admin._remote_config_url(spec) == "https://mirror.example/gg/config.json"
    assert linked_project_admin._remote_config_url({"base_url_key": "unknown"}) == ""
    assert linked_project_admin._remote_config_url({}) == ""
