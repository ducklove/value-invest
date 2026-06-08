"""Regression tests for Naver KR index parsing (KOSPI / KOSDAQ / KOSPI200).

Guards the "변동 없이 0.00%" bug: the KPI200 layout differs from KOSPI/KOSDAQ,
and the old fallback grabbed a stray ``width="7"`` digit and an unrelated
``0.00%`` instead of the real change. These tests pin the correct selectors and
the value+change reconciliation guard.
"""

import pytest

import market_indicators as mi


class _FakeResp:
    def __init__(self, html: str):
        # Function decodes with euc-kr; ASCII-only markup round-trips cleanly.
        self.content = html.encode("euc-kr", errors="ignore")


class _FakeClient:
    def __init__(self, html: str):
        self._html = html

    async def get(self, url, headers=None):
        return _FakeResp(self._html)


# Mirrors the real KPI200 page: value nested in <strong>, magnitude in a <span>
# inside id="change_value" (preceded by an <img width="7">), signed rate in a
# <strong> inside id="change_rate". Includes decoy "0.00%" and width="7" that
# must NOT be picked up.
_KPI200_DOWN = """
<td id="now_value"><strong class="nv01">1,186.54</strong></td>
<td></td><th>volume</th><td>0</td>
<td class="imp_txt" id="change_value">
  <img src="https://ssl.pstatic.net/imgstock/ico_down.gif" width="7" height="6" alt="d">
  <span class="tah p11 nv01"> 110.48 </span>
</td>
<td class="imp_txt" id="change_rate">
  <strong class="tah nv01"> -8.52% </strong>
</td>
<td>turnover 0.00%</td>
"""

# KOSPI/KOSDAQ layout: direct now_value text, quotient class, change_value_and_rate.
_KOSPI_UP = """
<span id="now_value" class="num">2,700.00</span>
<span class="quotient up"></span>
<em class="change_value_and_rate"><span>40.50</span> +1.52%</em>
"""

# KPI200 page where the change elements are absent: must emit no rate at all
# rather than fabricating 0.00%.
_KPI200_NO_CHANGE = """
<td id="now_value"><strong class="nv01">1,186.54</strong></td>
<td>turnover 0.00%</td>
"""


@pytest.mark.asyncio
async def test_kpi200_parses_real_change_not_stray_zero():
    r = await mi._fetch_kr_index(_FakeClient(_KPI200_DOWN), "KPI200")
    assert r["value"] == "1,186.54"
    assert r["change"] == "110.48"
    assert r["change_pct"] == "8.52%"
    assert r["direction"] == "down"
    # The whole point: never the stray decoys.
    assert r["change_pct"] != "0.00%"
    assert r["change"] != "7"


@pytest.mark.asyncio
async def test_kpi200_signed_change_pct_is_negative_downstream():
    r = await mi._fetch_kr_index(_FakeClient(_KPI200_DOWN), "KPI200")
    import market_daily

    signed = market_daily._signed_change_pct(r["change_pct"], r["direction"])
    assert signed == pytest.approx(-8.52)


@pytest.mark.asyncio
async def test_kospi_primary_path_still_works():
    r = await mi._fetch_kr_index(_FakeClient(_KOSPI_UP), "KOSPI")
    assert r["value"] == "2,700.00"
    assert r["change_pct"] == "1.52%"
    assert r["direction"] == "up"


@pytest.mark.asyncio
async def test_missing_change_emits_blank_not_zero():
    r = await mi._fetch_kr_index(_FakeClient(_KPI200_NO_CHANGE), "KPI200")
    assert r["value"] == "1,186.54"
    assert r["change"] == ""
    assert r["change_pct"] == ""  # never a fabricated 0.00%
