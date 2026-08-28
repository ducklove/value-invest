import json

import integrations


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
