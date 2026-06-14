import market_indicators as mi


def test_japan_short_bond_catalog_entries_exist():
    assert mi.CATALOG["JP_TONA"]["maturity"] == 0
    assert mi.CATALOG["JP3M"]["maturity"] == 0.25
    assert mi.CATALOG["JP1Y"]["maturity"] == 1
    assert mi._CNBC_BOND_MAP["JP3M"] == "JP3M-JP"


def test_parse_mof_jgb_csv_ignores_footer_and_uses_latest_data_row():
    csv_text = (
        "国債金利情報 (令和8年6月),,,\n"
        "基準日,1年,2年\n"
        "R8.6.11,1.140,1.410\n"
        "R8.6.12,1.148,1.417\n"
        "※最新のcsvデータがダウンロードできない場合,,,,\n"
    )

    out = mi._parse_mof_jgb_csv(csv_text.encode("cp932"), {"JP1Y": "1年"})

    assert out["JP1Y"]["value"] == "1.15"
    assert out["JP1Y"]["change"] == "0.01"
    assert out["JP1Y"]["direction"] == "up"
