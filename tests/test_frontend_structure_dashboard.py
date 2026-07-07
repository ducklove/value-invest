"""투자정보 대시보드 구조 계약 — 경제 캘린더 등."""
# 구 test_frontend_structure.py(1,500줄 단일 파일)를 2026-07-07 화면별로 분할.
# 새 화면 계약 테스트는 해당 화면 파일에 추가한다. 공용 경로/상수/헬퍼는
# tests/_frontend_structure.py 에 있다.
from _frontend_structure import JS, STATIC, _all_css


def test_economic_calendar_section_and_script_present():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    js = (JS / "economic-calendar.js").read_text(encoding="utf-8")
    dashboard = (JS / "market-dashboard.js").read_text(encoding="utf-8")
    styles = _all_css()

    # 투자정보 뷰 안, 금일 시황 섹션 다음의 풀폭 섹션으로 산다.
    assert 'id="econCalSection"' in html
    assert 'id="econCalContent"' in html
    assert "./js/economic-calendar.js" in html
    assert html.find('id="dailyMarketSection"') < html.find('id="econCalSection"')
    assert html.find('id="econCalSection"') < html.find("<!-- /investingView -->")
    # 투자정보 대시보드 로드 시 함께 로드된다.
    assert "if (typeof loadEconomicCalendar === 'function') loadEconomicCalendar();" in dashboard
    assert "async function loadEconomicCalendar()" in js
    assert ".econ-cal-section" in styles
    assert ".ec-row" in styles

def test_economic_calendar_filter_is_dates_plus_per_importance_settings():
    js = (JS / "economic-calendar.js").read_text(encoding="utf-8")
    styles = _all_css()

    # 기간은 시작/종료일 입력 + 기본 이번 주. 빠른 범위 버튼(오늘/이번주/다음주)은 제거.
    assert "function _ecThisWeek()" in js
    assert "id=\"econCalStart\"" in js
    assert "id=\"econCalEnd\"" in js
    assert "ec-range-btn" not in js
    # 국가·중요도는 ⚙ 설정 패널의 '중요도별 국가 선택'으로.
    assert "function _ecRenderSettings()" in js
    assert "id=\"econCalSettingsToggle\"" in js
    assert "function _ecLevelParam(lvl)" in js
    # 기본값: 상=전체, 중·하=한국만.
    assert "high: 'all', mid: new Set(['kr']), low: new Set(['kr'])" in js
    assert "params.set('high'" in js
    assert ".ec-settings" in styles
    assert ".ec-set-row" in styles
