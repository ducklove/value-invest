import market_indicators as mi


def test_japan_short_bond_catalog_entries_exist():
    assert mi.CATALOG["US_BASE"]["maturity"] == -1
    assert mi.CATALOG["KR_BASE"]["maturity"] == -1
    assert mi.CATALOG["JP_BASE"]["maturity"] == -1
    assert mi.CATALOG["JP_TONA"]["maturity"] == 0
    assert mi.CATALOG["JP3M"]["maturity"] == 0.25
    assert mi.CATALOG["JP1Y"]["maturity"] == 1
    assert mi._CNBC_BOND_MAP["JP3M"] == "JP3M-JP"
    assert mi.CATALOG["ES10Y"]["country"] == "ES"
    assert mi.CATALOG["RU10Y"]["country"] == "RU"
    assert mi.CATALOG["ID10Y"]["country"] == "ID"
    assert mi._CNBC_BOND_MAP["ES10Y"] == "ES10Y-ES"
    assert mi._CNBC_BOND_MAP["RU10Y"] == "RU10Y-RU"
    assert mi._CNBC_BOND_MAP["ID10Y"] == "ID10Y-ID"


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


def test_parse_fred_policy_csv_uses_latest_nonblank_observation():
    csv_text = (
        "observation_date,DFEDTARU\n"
        "2026-06-24,4.50\n"
        "2026-06-25,.\n"
        "2026-06-26,4.75\n"
    )

    out = mi._parse_fred_policy_csv(csv_text)

    assert out["value"] == "4.75"
    assert out["change"] == "0.25"
    assert out["direction"] == "up"


def test_parse_fed_openmarket_page_uses_latest_target_range_upper_bound():
    html = """
    <h4>2025</h4>
    <table><tbody>
      <tr><td>December 11</td><td>0</td><td>25</td><td>3.50-3.75</td></tr>
      <tr><td>October 30</td><td>0</td><td>25</td><td>3.75-4.00</td></tr>
    </tbody></table>
    <h4>2024</h4>
    """

    out = mi._parse_fed_openmarket_page(html)

    assert out["value"] == "3.75"
    assert out["change"] == "0.25"
    assert out["direction"] == "down"


def test_parse_bok_base_rate_page_reads_latest_chart_value():
    html = '''
    <script>
    var chartObj2_s = [["2025/05/29 ", 2.50],["2026/06/27", 2.75]]
    var chartObj2Labels = []
    </script>
    '''

    out = mi._parse_bok_base_rate_page(html)

    assert out["value"] == "2.75"
    assert out["change"] == "0.25"
    assert out["direction"] == "up"


def test_parse_boj_policy_rate_page_reads_guideline():
    html = """
    <section class="policy">
      <p>The Bank will encourage the uncollateralized overnight call rate
      to remain at around 1.0 percent.</p>
    </section>
    """

    out = mi._parse_boj_policy_rate_page(html)

    assert out["value"] == "1.00"
    assert out["change"] == ""
