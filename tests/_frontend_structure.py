"""frontend-structure 테스트 공용 경로·상수·헬퍼.

화면별 test_frontend_structure_*.py 가 공유한다. 분할 파일 목록
(*_SPLIT_FILES)은 index.html/admin.html 의 <script>/<link> 순서 계약이다.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STATIC = ROOT / "static"
JS = STATIC / "js"
CSS = STATIC / "css"

# 구 styles.css 의 연속 분할 결과 — index.html/admin.html <link> 순서와
# 동일해야 한다 (캐스케이드 계약). test_css_split_files_load_in_contract_order
# 가 강제한다.


CSS_SPLIT_FILES = [
    "base.css",
    "dashboard.css",
    "analysis.css",
    "portfolio.css",
    "mobile-overrides.css",
    "admin-wiki.css",
    "mobile-shell.css",
    "labs.css",
]


def _all_css() -> str:
    """분할된 CSS 를 로드 순서대로 이어붙여 반환 (구 styles.css 등가물)."""
    return "\n".join((CSS / name).read_text(encoding="utf-8") for name in CSS_SPLIT_FILES)

PORTFOLIO_SPLIT_FILES = [
    "portfolio-shell.js",
    "portfolio-data.js",
    "portfolio-order.js",
    "portfolio-sparklines.js",
    "portfolio-render.js",
    "portfolio-action-board.js",
    "portfolio-add-search.js",
    "portfolio-actions.js",
    "portfolio-insights.js",
    "portfolio-groups-market.js",
    "portfolio-ai.js",
    "portfolio-reports.js",
    "portfolio-performance.js",
    "portfolio-risk.js",
    "portfolio-rebalance.js",
    "portfolio-dividends-calendar.js",
    "portfolio-journal.js",
    "portfolio-trends-benchmark.js",
    "portfolio-trends.js",
    "portfolio-trends-group-weight.js",
    "portfolio-group-composition.js",
    "portfolio-cashflows.js",
    "portfolio-tag-summary.js",
    "portfolio-events.js",
]

# Dependencies-first: charts and filings define globals (chart state,
# allReports, loadWiki, ...) that analysis.js orchestrates at runtime.
ANALYSIS_SPLIT_FILES = [
    "analysis-charts.js",
    "analysis-filings.js",
    "analysis.js",
]

# Dependencies-first: admin.js provides the shared helpers (_esc,
# _adminInputStyle) and bootstrapping; the panel modules load after it.
ADMIN_SPLIT_FILES = [
    "admin.js",
    "admin-charts.js",
    "admin-observability.js",
    "admin-linked-projects.js",
]
