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

    preferred = public_config["preferredSpread"]
    assert preferred["pairsByPreferredCode"]["005935"]["commonCode"] == "005930"

    gold = public_config["goldGap"]
    assert gold["assetByPortfolioCode"]["KRX_GOLD"] == "gold"
    assert gold["assets"]["gold"]["latestGapPct"] == 4.25
    assert gold["updatedAt"] == "2026-04-25 09:00 KST"


def test_public_integrations_do_not_expose_local_paths(tmp_path):
    config = integrations.build_app_config(workspace_root=tmp_path)

    assert str(tmp_path) not in json.dumps(config)
    assert config["integrations"]["holdingValue"]["settings"]["source"] == "remote-fallback"
