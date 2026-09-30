import json
from pathlib import Path

import pytest

from services.ecosystem import external_tools, integrations


@pytest.fixture(autouse=True)
def _isolate_gold_cache():
    # goldGap 최신 갭은 external_tools 의 gold_gap 캐시를 우선한다 — 다른 테스트가 채운
    # 캐시가 로컬 파일 검증을 가리지 않게 비운다.
    external_tools._raw_cache.delete("gold_gap/latest")
    yield
    external_tools._raw_cache.delete("gold_gap/latest")


def test_build_public_integrations_reads_sibling_project_configs(tmp_path):
    holding_dir = tmp_path / "holding_value"
    holding_dir.mkdir()
    (holding_dir / "config.json").write_text(
        json.dumps(
            [
                {
                    "id": "sample_holding",
                    "name": "Sample Holding",
                    "holdingName": "Sample",
                    "holdingTicker": "123450.KS",
                    "holdingTotalShares": 1000,
                    "holdingTreasuryShares": 10,
                    "subsidiaries": [
                        {"name": "Child", "ticker": "543210.KS", "sharesHeld": 200}
                    ],
                }
            ]
        ),
        encoding="utf-8",
    )
    (holding_dir / "current.js").write_text(
        'const CURRENT_DATA = {"lastUpdated":"2026-05-10 22:00:00","pairs":[{"id":"sample_holding","holdingValue":9.9,"quoteSource":"mixed"}]};',
        encoding="utf-8",
    )

    preferred_dir = tmp_path / "common_preferred_spread"
    preferred_dir.mkdir()
    (preferred_dir / "config.json").write_text(
        json.dumps(
            [
                {
                    "id": "sample_pref",
                    "name": "Sample Pref",
                    "commonTicker": "005930.KS",
                    "preferredTicker": "005935.KS",
                    "commonName": "Common",
                    "preferredName": "Preferred",
                }
            ]
        ),
        encoding="utf-8",
    )

    gold_dir = tmp_path / "gold_gap"
    gold_dir.mkdir()
    (gold_dir / "config.json").write_text(
        json.dumps(
            {
                "assets": {
                    "gold": {
                        "label": "Gold",
                        "portfolioCodes": ["KRX_GOLD"],
                        "thresholdPct": 5,
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    (gold_dir / "data.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-04-25 09:00 KST",
                "gold": {"dates": ["2026-04-24"], "gap_pct": [4.25]},
            }
        ),
        encoding="utf-8",
    )

    public_config = integrations.build_public_integrations(workspace_root=tmp_path)

    holding = public_config["holdingValue"]
    assert holding["settings"] == {"source": "local", "available": True}
    assert holding["codes"] == ["123450"]
    assert holding["meta"]["123450"]["subsidiaries"] == [
        {"code": "543210", "sharesHeld": 200}
    ]
    assert holding["meta"]["123450"]["holdingValuePerShare"] == 1_000_000
    assert holding["items"][0]["current"]["holdingValueUnit"] == "억원"

    preferred = public_config["preferredSpread"]
    assert preferred["pairsByPreferredCode"]["005935"]["commonCode"] == "005930"

    assert public_config["buybacks"]["baseUrl"] == "https://ducklove.github.io/buybacks"
    assert public_config["eiayn"]["baseUrl"] == "https://ducklove.github.io/eiayn"

    gold = public_config["goldGap"]
    assert gold["assetByPortfolioCode"]["KRX_GOLD"] == "gold"
    assert gold["assets"]["gold"]["latestGapPct"] == 4.25
    assert gold["updatedAt"] == "2026-04-25 09:00 KST"


def test_holding_value_snapshot_reads_current_json(tmp_path):
    """hodling-value 는 현재 스냅샷을 current.json 으로 낸다 — 구 current.js 만
    읽으면 지분가치 스냅샷이 통째로 비어 목표가 폴백이 사라진다."""
    holding_dir = tmp_path / "hodling-value"
    holding_dir.mkdir()
    (holding_dir / "config.json").write_text(
        json.dumps(
            [
                {
                    "id": "samsung_life",
                    "holdingName": "삼성생명",
                    "holdingTicker": "032830.KS",
                    "holdingTotalShares": 200_000_000,
                    "holdingTreasuryShares": 0,
                    "subsidiaries": [{"name": "삼성전자", "ticker": "005930.KS", "sharesHeld": 503_905_000}],
                }
            ]
        ),
        encoding="utf-8",
    )
    (holding_dir / "current.json").write_text(
        json.dumps(
            {
                "lastUpdated": "2026-07-29 09:12:11",
                "pairs": [{"id": "samsung_life", "holdingValue": 1_557_567.8, "quoteSource": "kis_proxy"}],
            }
        ),
        encoding="utf-8",
    )

    holding = integrations.build_public_integrations(workspace_root=tmp_path)["holdingValue"]

    # 1,557,567.8억원 / 2억주 = 778,783.9원
    assert holding["meta"]["032830"]["holdingValuePerShare"] == 778_783.9
    assert holding["meta"]["032830"]["holdingValueUpdatedAt"] == "2026-07-29 09:12:11"


def test_public_integrations_do_not_expose_local_paths(tmp_path):
    config = integrations.build_app_config(workspace_root=tmp_path)

    assert str(tmp_path) not in json.dumps(config)
    assert config["integrations"]["holdingValue"]["settings"]["source"] == "remote-fallback"
    assert config["integrations"]["buybacks"]["baseUrl"] == "https://ducklove.github.io/buybacks"


def test_bond_mate_exposes_data_and_embed_urls():
    """bond-mate 는 로컬 config 없이 baseUrl 계열만 노출한다.

    브라우저가 published JSON 을 직접 읽고(dataUrl), 필요하면 화면을 통째로
    iframe 임베드한다(embedUrl + views). 키 이름은 프론트가 의존하는 계약이다.
    """
    config = integrations.build_public_integrations()["bondMate"]

    assert config["baseUrl"] == "https://ducklove.github.io/bond-mate"
    assert config["dataUrl"] == "https://ducklove.github.io/bond-mate/data/current.json"
    assert config["embedUrl"] == "https://ducklove.github.io/bond-mate/?embed="
    assert "government" in config["views"]
    assert "fx" in config["views"]


def test_bond_mate_base_url_is_overridable(monkeypatch):
    monkeypatch.setenv("BOND_MATE_BASE_URL", "http://127.0.0.1:8731/")
    config = integrations.build_public_integrations()["bondMate"]

    assert config["baseUrl"] == "http://127.0.0.1:8731"
    assert config["dataUrl"] == "http://127.0.0.1:8731/data/current.json"


def test_all_about_gold_publication_url_and_override(monkeypatch):
    config = integrations.build_public_integrations()["allAboutGold"]
    assert config["baseUrl"] == "https://ducklove.github.io/all-about-gold"
    assert config["dataUrl"].endswith("/all-about-gold/data/current.json")
    monkeypatch.setenv("ALL_ABOUT_GOLD_BASE_URL", "http://localhost:8765/")
    config = integrations.build_public_integrations()["allAboutGold"]
    assert config["baseUrl"] == "http://localhost:8765"


def _write_gold(tmp_path, gap=4.25, date="2026-04-24"):
    gold_dir = tmp_path / "gold_gap"
    gold_dir.mkdir(exist_ok=True)
    (gold_dir / "data.json").write_text(
        json.dumps({"updated_at": "2026-04-25 09:00 KST", "gold": {"dates": [date], "gap_pct": [gap]}}),
        encoding="utf-8",
    )
    return gold_dir


def test_sibling_files_are_memoized_by_mtime(tmp_path, monkeypatch):
    gold_dir = _write_gold(tmp_path)
    reads: list[str] = []
    original = Path.read_text

    def counting(self, *args, **kwargs):
        reads.append(str(self))
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", counting)
    first = integrations.build_public_integrations(workspace_root=tmp_path)
    data_path = str(gold_dir / "data.json")
    assert reads.count(data_path) == 1
    second = integrations.build_public_integrations(workspace_root=tmp_path)
    assert reads.count(data_path) == 1  # 두 번째 호출은 파일을 다시 열지 않는다
    assert first == second
    second["goldGap"]["assets"]["gold"]["portfolioCodes"].append("MUTATED")
    third = integrations.build_public_integrations(workspace_root=tmp_path)
    assert "MUTATED" not in third["goldGap"]["assets"]["gold"]["portfolioCodes"]
    assert "MUTATED" not in integrations.DEFAULT_GOLD_GAP_ASSETS["gold"]["portfolioCodes"]

    # 파일이 바뀌면(mtime/크기) 다시 읽는다.
    _write_gold(tmp_path, gap=-12.75, date="2026-04-26")  # 크기도 달라진다
    changed = integrations.build_public_integrations(workspace_root=tmp_path)
    assert reads.count(data_path) == 2
    assert changed["goldGap"]["assets"]["gold"]["latestGapPct"] == -12.75


def test_gold_gap_latest_prefers_cached_published_data(tmp_path):
    _write_gold(tmp_path)  # 로컬 data.json 은 오래된 값(4.25)
    local = integrations.build_public_integrations(workspace_root=tmp_path)["goldGap"]
    assert local["assets"]["gold"]["latestGapPct"] == 4.25

    external_tools._raw_cache.set("gold_gap/latest", {
        "updated_at": "2026-09-27 08:47 KST",
        "gold": {"gap_pct": [1.07], "dates": ["2026-09-27"]},
        "bitcoin": {"gap_pct": [0.42], "dates": ["2026-09-27"]},
    })
    gold = integrations.build_public_integrations(workspace_root=tmp_path)["goldGap"]
    assert gold["assets"]["gold"]["latestGapPct"] == 1.07
    assert gold["assets"]["gold"]["latestDate"] == "2026-09-27"
    assert gold["assets"]["bitcoin"]["latestGapPct"] == 0.42
    assert "latestGapPct" not in gold["assets"]["usdt"]
    assert gold["updatedAt"] == "2026-09-27 08:47 KST"


def test_gold_gap_stale_cache_does_not_override_newer_local_file(tmp_path):
    _write_gold(tmp_path)
    local = integrations.build_public_integrations(workspace_root=tmp_path)["goldGap"]["assets"]["gold"]
    external_tools._raw_cache.set("gold_gap/latest", {
        "updated_at": "2000-01-01 08:47 KST",
        "gold": {"gap_pct": [9.99], "dates": ["2000-01-01"]},
    })
    gold = integrations.build_public_integrations(workspace_root=tmp_path)["goldGap"]
    assert gold["assets"]["gold"]["latestGapPct"] == local["latestGapPct"]
    assert gold["assets"]["gold"]["latestDate"] == local["latestDate"]
    assert gold["updatedAt"] != "2000-01-01 08:47 KST"


def test_kis_proxy_is_server_side_only(monkeypatch):
    monkeypatch.setenv("KIS_PROXY_BASE_URL", "http://127.0.0.1:3288/")
    config = integrations.build_app_config()
    assert "kisProxy" not in config["integrations"]
    assert "3288" not in json.dumps(config)
    assert integrations.build_server_integrations()["kisProxy"]["baseUrl"] == "http://127.0.0.1:3288"


def test_default_workspace_root_is_repo_parent_after_move():
    # services/ecosystem/ 로 옮긴 뒤에도 형제 체크아웃 기본 위치는 저장소의 부모 디렉터리다.
    repo_root = Path(__file__).resolve().parents[1]
    assert integrations.PROJECT_ROOT == repo_root
    assert integrations.DEFAULT_WORKSPACE_ROOT == repo_root.parent


def test_server_kis_proxy_follows_profile_default(monkeypatch):
    # env 미설정이면 실제 클라이언트와 같은 프로필 기본값(production=loopback)을 보여 준다.
    monkeypatch.delenv("KIS_PROXY_BASE_URL", raising=False)
    monkeypatch.setenv("VALUE_INVEST_ENV", "production")
    assert integrations.build_server_integrations()["kisProxy"]["baseUrl"] == "http://127.0.0.1:3288"
    monkeypatch.setenv("VALUE_INVEST_ENV", "development")
    assert integrations.build_server_integrations()["kisProxy"]["baseUrl"] == "http://ducklove.duckdns.org:3288"
