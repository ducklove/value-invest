"""종목분석 화면 구조 계약 — analysis 분할 파일·위키 Q&A·투자 일지·딥링크."""
# 구 test_frontend_structure.py(1,500줄 단일 파일)를 2026-07-07 화면별로 분할.
# 새 화면 계약 테스트는 해당 화면 파일에 추가한다. 공용 경로/상수/헬퍼는
# tests/_frontend_structure.py 에 있다.
from _frontend_structure import ANALYSIS_SPLIT_FILES, CSS, JS, STATIC, _all_css


def test_etf_deep_links_to_eiayn_in_analysis_and_portfolio():
    analysis = (JS / "analysis.js").read_text(encoding="utf-8")
    insights = (JS / "portfolio-insights.js").read_text(encoding="utf-8")

    # 분석뷰 밸류에이션 그리드: ETF면 외부 카드 합류(우선주/지주사와 동일 패턴).
    assert "const e = links.etf;" in analysis
    assert "ETF 상세" in analysis
    assert "data.preferred || data.holding || data.etf" in analysis
    # 포트폴리오 인사이트 모달: ETF 액션 링크(eiayn 새 탭).
    assert "function _etfInfoAction(etf)" in insights
    assert "_renderInsightActionLinks(code, goldGap, holding, etf)" in insights
    assert "const etf = data.etf || null;" in insights

def test_index_loads_analysis_split_scripts_in_contract_order():
    html = (STATIC / "index.html").read_text(encoding="utf-8")

    positions = []
    for name in ANALYSIS_SPLIT_FILES:
        marker = f'./js/{name}'
        pos = html.find(marker)
        assert pos != -1, f"{name} is not loaded by index.html"
        positions.append(pos)
    assert positions == sorted(positions), "analysis split script order changed"
    # 분할 파일은 검색(search.js) 다음, 나머지 뷰 스크립트(stock-alerts.js) 앞에 묶여 산다.
    assert html.find("./js/search.js") < positions[0]
    assert positions[-1] < html.find("./js/stock-alerts.js")

def test_analysis_split_files_keep_feature_homes():
    charts = (JS / "analysis-charts.js").read_text(encoding="utf-8")
    filings = (JS / "analysis-filings.js").read_text(encoding="utf-8")
    analysis = (JS / "analysis.js").read_text(encoding="utf-8")

    # 차트: 주간/연간 그리드, 목표가 오버레이, 차트 모달, 기간 전환.
    assert "async function renderChartGrid(" in charts
    assert "async function _overlayTargetPrices(" in charts
    assert "function openChartModal(" in charts
    assert "function closeChartModal()" in charts
    assert "async function switchValuationPeriod(" in charts
    # 공시/리포트: DART AI 리뷰, 리포트 테이블, 위키 요약.
    assert "function renderFilingReview(" in filings
    assert "async function generateFilingReview(" in filings
    assert "const FILING_REVIEW_STATUS_TIMEOUT_MS = 60000;" in filings
    assert "const FILING_REVIEW_GENERATE_TIMEOUT_MS = 10 * 60 * 1000;" in filings
    assert "function renderReportsTable(" in filings
    assert "async function loadWiki(" in filings
    # 본체: 분석 SSE 오케스트레이션, 시세 요약, 위키 Q&A.
    assert "async function analyzeStock(" in analysis
    assert "async function renderResult(" in analysis
    assert "function renderQuoteSnapshot(" in analysis
    assert "async function askWikiQuestion()" in analysis
    # SSE 스트리밍 호출은 apiFetch 타임아웃 제외 플래그를 유지한다.
    assert "stream: true" in analysis

def test_analysis_split_files_stay_below_maintenance_ceiling():
    for name in ANALYSIS_SPLIT_FILES:
        lines = (JS / name).read_text(encoding="utf-8").splitlines()
        assert len(lines) < 1000, f"{name} grew to {len(lines)} lines; split it before extending"

def test_investment_journal_lives_in_analysis_view_and_performance_tab():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = _all_css()
    analysis = (JS / "analysis.js").read_text(encoding="utf-8")
    performance = (JS / "portfolio-performance.js").read_text(encoding="utf-8")
    journal = (JS / "portfolio-journal.js").read_text(encoding="utf-8")

    # 표면 ① 종목 분석 화면: companyInfo 안(위키 Q&A 다음)의 '📝 투자 일지'
    # 섹션 — renderResult 가 활성 종목으로 loadStockJournal 을 호출한다.
    assert 'id="stockJournalSection"' in html
    assert 'id="stockJournalForm"' in html
    assert 'id="stockJournalList"' in html
    assert "📝 투자 일지" in html
    assert html.find('id="wikiQa"') < html.find('id="stockJournalSection"')
    assert html.find('id="stockJournalSection"') < html.find('id="emptyState"')
    assert "if (typeof loadStockJournal === 'function') loadStockJournal(data.stock_code);" in analysis
    # 표면 ② 성과 탭 '투자 일지' 카드: 배당 캘린더 다음, 평가금액 추이 앞.
    # 전 종목 타임라인(읽기/복기) — 성과 탭이 보일 때만 lazy 로드.
    assert 'id="pfJournalWrap"' in html
    assert 'id="pfJournalContent"' in html
    assert html.find('id="pfDivCalWrap"') < html.find('id="pfJournalWrap"')
    assert html.find('id="pfJournalWrap"') < html.find('id="pfValueChart"')
    assert "if (typeof pfLoadJournalPanel === 'function') pfLoadJournalPanel();" in performance
    # 기능 홈: fetch/렌더/폼/인라인 수정/삭제는 portfolio-journal.js.
    assert "async function pfLoadJournalPanel(" in journal
    assert "async function loadStockJournal(" in journal
    assert "async function stockJournalSubmit()" in journal
    assert "async function pfJournalSaveNote(" in journal
    assert "async function pfJournalDelete(" in journal
    assert "'/api/portfolio/journal'" in journal
    assert "/api/portfolio/journal?stock_code=" in journal
    assert "method: 'PATCH'" in journal
    assert "method: 'DELETE'" in journal
    # 백그라운드 로드는 silent, 사용자 조작(기록/수정/삭제)은 토스트.
    assert "reportApiError(e, '투자 일지', { silent: true });" in journal
    assert "reportApiError(e, '투자 일지 기록');" in journal
    assert "reportApiError(e, '투자 일지 수정');" in journal
    assert "reportApiError(e, '투자 일지 삭제');" in journal
    # note 는 escapeHtml 로만 렌더(원시 HTML 금지) + 삭제는 confirm 게이트.
    assert "${escapeHtml(entry.note)}" in journal
    assert "window.confirm(" in journal
    # 빈 상태/로그인 안내 문구.
    assert "아직 기록이 없습니다" in journal
    assert "기록된 투자 일지가 없습니다" in journal
    # 포맷터는 공용 헬퍼 재사용(중복 정의 금지).
    assert "function fmtPct(" not in journal
    assert "function returnClass(" not in journal
    assert "function escapeHtml(" not in journal
    # 스타일: 카드/배지/폼 + 모바일 2열 접기(.pf-rebal-editor-row 패턴).
    assert ".pf-journal-card" in styles
    assert ".pf-journal-badge.buy" in styles
    assert ".stock-journal" in styles
    assert ".pf-journal-form-row { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); }" in styles

def test_reports_scatter_buy_color_uses_semantic_tokens():
    """Buy 의미색 이원화 해소 — 스캐터/범례는 배지(badge-buy 초록)와 같은 CSS 토큰."""
    charts = (JS / "analysis-charts.js").read_text(encoding="utf-8")
    analysis_css = (CSS / "analysis.css").read_text(encoding="utf-8")

    # Buy=빨강(#dc2626) 하드코딩 금지 — 국내 시세 규약(빨강=상승)과 혼동된다.
    assert "#dc2626" not in charts
    assert "function _recommScatterColors()" in charts
    assert "params.data.buy ? recomm.buy : recomm.hold" in charts
    # 토큰은 analysis.css 에 라이트/다크 쌍으로 정의된다.
    assert "--recomm-buy:" in analysis_css
    assert "--recomm-hold:" in analysis_css
    dark = analysis_css[analysis_css.rfind('[data-theme="dark"]'):]
    assert "--recomm-buy:" in dark and "--recomm-hold:" in dark

def test_reports_table_headers_get_col_scope_at_render():
    """리포트 테이블 thead 는 index.html 정적 마크업 — 렌더 시점 scope=col 보강."""
    filings = (JS / "analysis-filings.js").read_text(encoding="utf-8")
    assert "th.setAttribute('scope', 'col')" in filings

def test_analysis_number_formatting_pins_ko_kr_locale():
    """toLocaleString 로케일 혼재(ko-KR vs 미지정) 금지 — analysis.js 는 ko-KR 통일."""
    analysis = (JS / "analysis.js").read_text(encoding="utf-8")
    assert ".toLocaleString()" not in analysis
    assert "toLocaleString(undefined" not in analysis

def test_analysis_wiki_qa_and_journal_are_collapsed_by_default():
    html = (STATIC / "index.html").read_text(encoding="utf-8")
    styles = _all_css()

    # 위키 Q&A·투자 일지는 매번 쓰는 요소가 아니라 밸류에이션 차트보다 화면 우선순위가
    # 낮다(UX 감사 P2⑦). <details> 는 JS 없이 접힌 채로 시작하고, 내부 id 는 그대로라
    # 기존 스크립트(wiki.js/portfolio-journal.js)가 값을 읽고 쓰는 데 영향이 없다.
    assert '<details class="analysis-collapsible" id="wikiQaDetails">' in html
    assert '<details class="analysis-collapsible" id="stockJournalDetails">' in html
    assert '<summary class="analysis-collapsible-summary">💬 이 종목에 대해 질문하기</summary>' in html
    assert '<summary class="analysis-collapsible-summary">📝 투자 일지</summary>' in html
    assert 'id="wikiQa"' in html
    assert 'id="stockJournalSection"' in html
    # 순서 계약은 기존 테스트(investment_journal)가 이미 지킨다 — 여기서는 접힘 래퍼
    # 자체와, summary 로 대체된 라벨이 접근성 이름으로는 남아있는지만 확인한다.
    assert '<label for="wikiQaInput" class="wiki-qa-label sr-only">' in html
    assert "list-style: none" in styles
    assert "::-webkit-details-marker" in styles
