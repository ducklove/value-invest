"""생태계 레지스트리(config/ecosystem.json) 스키마와 파생값 드리프트 검사."""

import copy
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

import integrations
from core import ecosystem

ROOT = Path(__file__).resolve().parent.parent

# 레지스트리 도입 전 integrations.py 에 하드코딩돼 있던 값 — 파생 결과가 이와 같아야 한다.
LEGACY_DEFAULT_BASE_URLS = {
    "holdingValue": "https://ducklove.github.io/holding_value",
    "preferredSpread": "https://ducklove.github.io/common_preferred_spread",
    "spacHunter": "https://ducklove.github.io/spac-hunter",
    "buybacks": "https://ducklove.github.io/buybacks",
    "eiayn": "https://ducklove.github.io/eiayn",
    "goldGap": "https://ducklove.github.io/gold_gap",
    "allAboutGold": "https://ducklove.github.io/all-about-gold",
    "npsTracker": "https://ducklove.github.io/nps-tracker",
    "bondMate": "https://ducklove.github.io/bond-mate",
    "kisProxy": "http://ducklove.duckdns.org:3288",
}
LEGACY_HANDOFF_KEYS = {"holdingValue", "preferredSpread", "spacHunter", "buybacks", "eiayn"}
LEGACY_ENV = {
    "holdingValue": "HOLDING_VALUE_BASE_URL",
    "preferredSpread": "PREFERRED_SPREAD_BASE_URL",
    "spacHunter": "SPAC_HUNTER_BASE_URL",
    "buybacks": "BUYBACKS_BASE_URL",
    "eiayn": "EIAYN_BASE_URL",
    "goldGap": "GOLD_GAP_BASE_URL",
    "allAboutGold": "ALL_ABOUT_GOLD_BASE_URL",
    "npsTracker": "NPS_TRACKER_BASE_URL",
    "bondMate": "BOND_MATE_BASE_URL",
    "kisProxy": "KIS_PROXY_BASE_URL",
}
EXPECTED_IDS = {
    "value-invest", "holding_value", "common_preferred_spread", "spac-hunter", "buybacks", "eiayn",
    "gold_gap", "all-about-gold", "nps-tracker", "bond-mate", "index-popup",
    "hub:screener", "hub:quant", "hub:insights", "hub:masters",
    "finance-pi", "kis-proxy", "the_admin", "portfolio-epaper", "x3", "morning-bell",
}
INTERNAL_IDS = {"finance-pi", "kis-proxy", "the_admin", "portfolio-epaper", "x3", "morning-bell"}


def _registry() -> dict:
    return json.loads((ROOT / "config" / "ecosystem.json").read_text(encoding="utf-8"))


def test_registry_is_valid_and_lists_every_project():
    data = ecosystem.load()
    ids = [t["id"] for t in data["tools"]]
    assert len(ids) == len(set(ids))
    assert set(ids) == EXPECTED_IDS
    assert {t["id"] for t in data["tools"] if t["visibility"] == "internal"} == INTERNAL_IDS
    assert data["heldBadges"]["path"] == "/js/portfolio-held-badges.js"
    for tool in data["tools"]:
        for field in ("stockLink", "viewLink", "assetLink"):
            if tool.get(field):
                re.compile(tool[field]["accepts"])
        vendor = tool.get("vendor")
        if vendor:
            # 형제 채택은 orchestrator 가 플래그를 뒤집을 때까지 꺼 둔다.
            assert vendor["shell"] is False and vendor["themeBoot"] is False


@pytest.mark.parametrize("mutate, message", [
    (lambda d: d["tools"].append(copy.deepcopy(d["tools"][1])), "id"),
    (lambda d: d["tools"][1].update(url="http://ducklove.github.io/holding_value"), "https"),
    (lambda d: d["tools"][1].update(url="https://192.168.68.84/x"), "사설"),
    (lambda d: d["tools"][1].update(url="https://ducklove.duckdns.org:3288"), "내부 포트"),
    (lambda d: d["tools"][1]["stockLink"].update(accepts="([0-9"), "정규식"),
    (lambda d: d["tools"][1].update(icon="emoji"), "icon"),
    (lambda d: d["tools"][1].update(handoff=True, integrationKey=None), "integrationKey"),
])
def test_validate_rejects_bad_registries(mutate, message):
    data = _registry()
    mutate(data)
    with pytest.raises(ecosystem.EcosystemRegistryError, match=message):
        ecosystem.validate(data)


def test_default_base_urls_are_derived_identically():
    assert integrations.DEFAULT_BASE_URLS == LEGACY_DEFAULT_BASE_URLS
    assert list(integrations.DEFAULT_BASE_URLS) == list(LEGACY_DEFAULT_BASE_URLS)
    by_key = {t["integrationKey"]: t for t in ecosystem.tools() if t.get("integrationKey")}
    assert {key: tool["envOverride"] for key, tool in by_key.items()} == LEGACY_ENV


def test_env_override_still_wins(monkeypatch):
    monkeypatch.setenv("HOLDING_VALUE_BASE_URL", "https://example.test/hv/")
    assert integrations.build_public_integrations()["holdingValue"]["baseUrl"] == "https://example.test/hv"
    projection = integrations.build_app_config()["ecosystem"]
    tool = next(t for t in projection["tools"] if t["id"] == "holding_value")
    assert tool["url"] == "https://example.test/hv"
    vendored = ecosystem.public_projection(resolve_env=False)
    assert next(t for t in vendored["tools"] if t["id"] == "holding_value")["url"] == LEGACY_DEFAULT_BASE_URLS["holdingValue"]


def test_handoff_keys_match_legacy_allowlist_in_python_and_js():
    assert ecosystem.handoff_keys() == LEGACY_HANDOFF_KEYS
    assert integrations.handoff_integration_keys() == LEGACY_HANDOFF_KEYS
    utils = (ROOT / "static" / "js" / "utils.js").read_text(encoding="utf-8")
    fallback = re.search(r"PORTFOLIO_HANDOFF_FALLBACK_KEYS = \[([^\]]*)\]", utils)
    assert fallback, "utils.js fallback handoff list missing"
    assert set(re.findall(r"'([^']+)'", fallback.group(1))) == LEGACY_HANDOFF_KEYS
    projection = integrations.build_app_config()["ecosystem"]
    assert {t["integrationKey"] for t in projection["tools"] if t.get("handoff")} == LEGACY_HANDOFF_KEYS


def test_app_config_projection_hides_internal_tools(monkeypatch):
    monkeypatch.delenv("KIS_PROXY_BASE_URL", raising=False)
    config = integrations.build_app_config()
    projection = config["ecosystem"]
    ids = {t["id"] for t in projection["tools"]}
    assert ids == EXPECTED_IDS - INTERNAL_IDS
    assert "kis-proxy" not in ids and "kisProxy" not in {t.get("integrationKey") for t in projection["tools"]}
    assert "infra" not in {c["id"] for c in projection["categories"]}
    text = json.dumps(projection, ensure_ascii=False)
    for needle in ("192.168.", ":3288", ":8400", "vendor", "envOverride", "repo"):
        assert needle not in text
    for tool in projection["tools"]:
        assert tool["url"].startswith("https://")
    # kisProxy 서버 설정은 그대로 유지된다(서버 전용).
    assert config["integrations"]["kisProxy"]["baseUrl"] == "http://ducklove.duckdns.org:3288"


def test_analytics_projects_match_registry_vendor_dirs():
    tools = {t["id"]: t for t in ecosystem.tools()}
    entries = json.loads((ROOT / "config" / "analytics-projects.json").read_text(encoding="utf-8"))
    assert {e["project"] for e in entries} == {tid for tid, t in tools.items() if t.get("vendor")}
    for entry in entries:
        vendor = tools[entry["project"]]["vendor"]
        assert vendor["html"] == entry["html"]
        if entry["project"] == "value-invest":
            continue
        asset_dir = entry["asset"].rsplit("/", 1)[0] if "/" in entry["asset"] else "."
        assert asset_dir == vendor["dir"], entry["project"]
        assert entry["src"].startswith(vendor["src"]), entry["project"]


def test_vc_shell_registry_block_is_up_to_date():
    shell = (ROOT / "static" / "ecosystem" / "vc-shell.js").read_text(encoding="utf-8")
    match = re.search(r"/\* vc:registry:start \*/ (.*?) /\* vc:registry:end \*/", shell)
    assert match, "vc:registry markers missing"
    expected = json.dumps(ecosystem.public_projection(resolve_env=False), ensure_ascii=False, separators=(",", ":"))
    assert match.group(1) == expected, "run: npm run sync:ecosystem:write"
    for needle in ("192.168.", ":3288", ":8400", "kis-proxy", "finance-pi"):
        assert needle not in shell


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_sync_ecosystem_hub_verify_passes():
    result = subprocess.run(
        ["node", "scripts/sync-ecosystem.mjs", "--hub-only"], cwd=ROOT, capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_vc_tokens_use_hub_canonical_values():
    base = (ROOT / "static" / "css" / "base.css").read_text(encoding="utf-8")
    tokens = (ROOT / "static" / "ecosystem" / "vc-tokens.css").read_text(encoding="utf-8")
    light = tokens.split(':root[data-theme="dark"]')[0]
    for hub_value, token in (("--bg: #f5f5f5", "--vc-bg: #f5f5f5"), ("--primary: #2563eb", "--vc-brand: #2563eb"),
                             ("--up: #b91c1c", "--vc-up: #b91c1c"), ("--down: #1d4ed8", "--vc-down: #1d4ed8"),
                             ("--text: #1a1a1a", "--vc-text: #1a1a1a")):
        assert hub_value in base
        assert token in light
    assert "--vc-bg: #0f172a" in tokens and "--vc-up: #fca5a5" in tokens and "--vc-down: #93c5fd" in tokens
    assert "@media (prefers-color-scheme: dark)" in tokens


def test_theme_boot_snippet_stays_tiny():
    boot = (ROOT / "static" / "ecosystem" / "vc-theme-boot.js").read_text(encoding="utf-8")
    assert len(boot.strip().splitlines()) < 25
    assert "</script" not in boot


def test_every_integration_key_is_served_by_build_public_integrations():
    # handoff 라우트는 build_public_integrations()[key] 로 주소를 찾는다 — 레지스트리에만 있는 키가 생기면 404.
    served = integrations.build_public_integrations()
    for tool in ecosystem.tools():
        if tool.get("integrationKey"):
            assert tool["integrationKey"] in served, tool["id"]
