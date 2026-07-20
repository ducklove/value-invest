"""종목 hover 일봉 캔들 툴팁 구조 계약 — 스크립트 등록 순서·데스크톱 가드·표면 커버리지."""
from _frontend_structure import CSS, JS, STATIC


def test_stock_hover_chart_is_registered_after_utils():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    utils_pos = html.index('src="./js/utils.js"')
    hover_pos = html.index('src="./js/stock-hover-chart.js"')
    # apiFetchJson/escapeHtml 를 쓰므로 utils.js 뒤에 로드돼야 한다.
    assert utils_pos < hover_pos


def test_hover_tooltip_is_desktop_only_and_covers_stock_surfaces():
    js = (JS / "stock-hover-chart.js").read_text(encoding="utf-8")

    # 터치 기기 제외 — hover 가능한 정밀 포인터 환경에서만 동작.
    assert "(hover: hover) and (pointer: fine)" in js

    # 종목이 나열되는 표면 계약: 보유종목 테이블·최근/관심 목록·시장 랭킹 +
    # 신규 표면용 opt-in 속성.
    assert "'[data-candle-code]'" in js
    assert "'#pfBody tr[data-code] .pf-stock-cell'" in js
    assert "'#recentList .sidebar-item[data-code]'" in js
    assert "'.mv-row[data-code]'" in js

    # 특수자산은 gold_gap 일봉 API 연결 전까지 요청 자체를 걸러낸다.
    assert "/^(CASH_|KRX_GOLD$|CRYPTO_)/" in js
    assert "/api/stocks/" in js
    assert "daily-candles" in js


def test_tooltip_styles_live_in_base_css_and_never_capture_pointer():
    base = (CSS / "base.css").read_text(encoding="utf-8")
    block_start = base.index(".stock-candle-tip {")
    block = base[block_start:base.index("}", block_start)]
    assert "position: fixed;" in block
    assert "pointer-events: none;" in block
    assert ".stock-candle-tip.visible { display: block; }" in base
