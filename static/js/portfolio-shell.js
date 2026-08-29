// Portfolio shell: shared state, columns, view switching, NPS entrypoint.
// Split from static/js/portfolio.js to keep portfolio features maintainable.
// --- Portfolio ---
// Cross-file portfolio view state lives in PfStore (portfolio-store.js):
// items, benchmarkQuotes, navHistory, activeView, loading, groups, sort,
// filters, edit, manualOrder, snapshots, currency, prefs.
// Only file-local UI plumbing (timers, style element refs, pointer guards)
// stays as top-level declarations in the owning split file.
const PF_QUOTE_REFRESH_MS = 60_000;
// "도구" 허브(labsView)와 그 하위 딥링크 화면들 — switchView 의 상단 탭 활성 표시를 묶는 데 쓰인다.
const PF_TOOLS_HUB_VIEWS = new Set(['labs', 'nps', 'insights', 'screener', 'masters', 'bonds']);
// view ↔ URL 경로 매핑. switchView 의 history.pushState(정방향)와 app-main.js 의 최초 진입
// 라우팅·popstate 복원(역방향)이 이 표 하나를 공유한다 — 표가 두 곳에 따로 있으면 한쪽만
// 고치고 잊는 사고가 나기 쉽다(예: 새 뷰 추가 시 pushState 는 되는데 새로고침 복원은 안 되는 식).
const PF_VIEW_PATHS = {
  investing: '/investing',
  analysis: '/analysis',
  portfolio: '/portfolio',
  nps: '/nps',
  labs: '/labs',
  insights: '/insights',
  screener: '/screener',
  masters: '/masters',
  bonds: '/bonds',
};
const PF_PATH_TO_VIEW = {
  '/investing': 'investing',
  '/analysis': 'analysis',
  '/portfolio': 'portfolio',
  '/nps': 'nps',
  '/labs': 'labs',
  '/tools': 'labs',
  '/insights': 'insights',
  '/screener': 'screener',
  '/masters': 'masters',
  '/bonds': 'bonds',
};
let _pfPointerGuardUntil = 0;
const PF_SIMPLE_MODE_KEY = 'pf_mobile_simple_mode';
// 컴팩트 보기: 종목명을 한 줄로, 태그·순서이동 핸들을 숨기고 행 간격을 좁힌다.
// 모바일 전용인 pf-mobile-simple 과 달리 데스크톱에서도 동작하는 보기 옵션.
const PF_COMPACT_ROWS_KEY = 'pf_compact_rows';
try { PfStore.prefs.compactRows = localStorage.getItem(PF_COMPACT_ROWS_KEY) === '1'; } catch (e) {}

function pfBenchmarkQuoteHasChange(q) {
  return q && q.change_pct !== null && q.change_pct !== undefined && q.change_pct !== '';
}

function pfMergeBenchmarkQuote(code, incoming) {
  if (!code || !incoming || typeof incoming !== 'object') return false;
  const current = PfStore.benchmarkQuotes[code] || {};
  const merged = { ...current };
  const incomingHasChange = pfBenchmarkQuoteHasChange(incoming);
  const currentHasChange = pfBenchmarkQuoteHasChange(current);
  const canReplaceChange = incomingHasChange && (incoming._stale !== true || !currentHasChange);

  for (const [key, value] of Object.entries(incoming)) {
    if (value === null || value === undefined || value === '') continue;
    if (['change_pct', 'change', 'direction', 'value'].includes(key) && !canReplaceChange) continue;
    merged[key] = value;
  }

  PfStore.benchmarkQuotes[code] = merged;
  return true;
}

function _pfMarkPointerInteraction(ms = 450) {
  _pfPointerGuardUntil = performance.now() + ms;
}

function _pfIsPointerInteractionActive() {
  return performance.now() < _pfPointerGuardUntil;
}

function _pfIsSimpleModeViewport() {
  return window.matchMedia('(max-width: 900px), (max-height: 520px) and (max-width: 1180px)').matches;
}

function _pfShouldAutoUseSimpleMode() {
  return window.matchMedia('(max-width: 760px), (max-height: 520px) and (max-width: 1180px)').matches;
}

function _mobileFixedView() {
  // 과거에는 로그인한 모바일 사용자를 포트폴리오에 강제 고정했지만, 하단 탭바로
  // 4개 뷰를 자유롭게 오가도록 바뀌면서 lock 을 해제한다. 첫 진입 기본값(로그인 시
  // 포트폴리오)은 initApp 에서 별도로 처리하므로 여기서 강제할 필요가 없다.
  return null;
}

function pfSyncMobileFixedView() {
  const lockedView = _mobileFixedView();
  document.body.classList.toggle('mobile-fixed-portfolio', lockedView === 'portfolio');
  document.body.classList.toggle('mobile-fixed-analysis', lockedView === 'analysis');
  if (lockedView && PfStore.activeView !== lockedView) {
    switchView(lockedView, { allowMobileLockOverride: true });
  }
}

function _pfLoadSimpleModePreference() {
  try {
    const raw = localStorage.getItem(PF_SIMPLE_MODE_KEY);
    if (raw === '1') return true;
    if (raw === '0') return false;
  } catch (e) {}
  return null;
}

function _pfSaveSimpleModePreference(enabled) {
  try { localStorage.setItem(PF_SIMPLE_MODE_KEY, enabled ? '1' : '0'); } catch (e) {}
}

function _pfApplySimpleMode(enabled, { persist = false } = {}) {
  const compactViewport = _pfIsSimpleModeViewport();
  const active = compactViewport && !!enabled;
  PfStore.prefs.simpleMode = active;
  document.body.classList.toggle('pf-mobile-simple', active);
  // 간편 모드에서는 무거운 심층 분석만 보유종목으로 되돌린다. 가계 자산
  // 통합관리는 모바일 레이아웃을 따로 제공하므로 회전/리사이즈 때 유지한다.
  if (active && typeof pfSwitchTab === 'function' && typeof pfActiveTab !== 'undefined' && pfActiveTab === 'performance') {
    pfSwitchTab('holdings');
  }
  if (persist) _pfSaveSimpleModePreference(active);

  const toggle = document.getElementById('pfSimpleToggle');
  if (toggle) {
    toggle.hidden = !compactViewport;
    toggle.classList.toggle('active', active);
    toggle.setAttribute('aria-pressed', active ? 'true' : 'false');
    toggle.textContent = active ? '일반' : '간편';
    toggle.title = active ? '일반 보기' : '모바일 간편 보기';
  }
  pfScheduleSimpleTableHeightSync();
}

// 간편 모드 표 박스는 화면 하단(고정 탭바 바로 위)까지만 차야 한다. 남으면 표 아래에
// 빈 띠가 생기고, 넘치면 마지막 행이 탭바에 가리는 동시에 페이지 자체에 몇십 px 짜리
// 스크롤이 생겨 맨 위 요약 카드(집계 숫자)가 밀려 올라간다.
// CSS 상수(calc(100dvh - 182px))만으로는 위/아래 크롬 높이 변화 — 터치영역 44px 확보로
// 커진 pf-tab-bar, iOS safe-area 만큼 두꺼워지는 하단 탭바 — 를 따라가지 못해 실제로
// 60px 가까이 어긋났다. 그래서 실측값을 CSS 변수로 넣고 상수는 폴백으로만 둔다.
// 하한은 "가로모드에서도 실측값이 그대로 쓰일 만큼" 낮게 둔다 — 하한이 실측값보다
// 크면 그만큼 표가 하단 탭바에 다시 가린다(가로 375px 기준 실측 ≈ 144px).
const PF_SIMPLE_TABLE_MIN_PX = 120;
const PF_SIMPLE_TABLE_GAP_PX = 4;     // 하단 탭바와 맞닿지 않을 만큼의 여백
let _pfSimpleTableSyncFrame = 0;

function _pfVisibleRect(el) {
  if (!el) return null;
  const rect = el.getBoundingClientRect();
  // display:none 인 요소는 0×0 — 숨은 탭/데스크톱 폭에서의 오측정을 여기서 걸러낸다.
  return (rect.width > 0 || rect.height > 0) ? rect : null;
}

function pfSyncSimpleTableHeight() {
  const wrap = document.querySelector('#pfHoldingsTab .pf-table-wrap');
  if (!wrap) return;
  const rect = PfStore.prefs.simpleMode ? _pfVisibleRect(wrap) : null;
  if (!rect) {
    wrap.style.removeProperty('--pf-simple-table-max');
    return;
  }
  const tabbar = _pfVisibleRect(document.querySelector('.mobile-tabbar'));
  const top = rect.top + (window.scrollY || window.pageYOffset || 0);
  const available = Math.round(window.innerHeight - top - (tabbar ? tabbar.height : 0) - PF_SIMPLE_TABLE_GAP_PX);
  wrap.style.setProperty('--pf-simple-table-max', `${Math.max(PF_SIMPLE_TABLE_MIN_PX, available)}px`);
}

// 호출부는 DOM 을 막 바꾼 직후일 수 있어 레이아웃이 잡힌 다음에 한 번만 실측한다
// (같은 프레임의 여러 호출은 하나로 합쳐진다). 숨은 탭에서는 rAF 가 돌지 않으므로
// 타이머로 대신한다 — 그때는 rect 가 0 이라 CSS 폴백으로 돌아갔다가, 화면에 다시
// 나타날 때(visibilitychange) 재실측된다.
function pfScheduleSimpleTableHeightSync() {
  if (_pfSimpleTableSyncFrame) return;
  _pfSimpleTableSyncFrame = 1;
  const run = () => {
    _pfSimpleTableSyncFrame = 0;
    pfSyncSimpleTableHeight();
  };
  if (typeof requestAnimationFrame === 'function' && !document.hidden) {
    requestAnimationFrame(run);
  } else {
    setTimeout(run, 0);
  }
}

function pfSyncSimpleModeForViewport() {
  const preference = _pfLoadSimpleModePreference();
  _pfApplySimpleMode(preference === null ? _pfShouldAutoUseSimpleMode() : preference);
  pfSyncMobileFixedView();
}

function pfToggleSimpleMode() {
  _pfApplySimpleMode(!PfStore.prefs.simpleMode, { persist: true });
}

(function initPfSimpleMode() {
  const onReady = () => {
    pfSyncSimpleModeForViewport();
    window.addEventListener('resize', pfSyncSimpleModeForViewport);
    window.addEventListener('orientationchange', pfSyncSimpleModeForViewport);
  };
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', onReady);
  } else {
    onReady();
  }
})();

// --- Column visibility ---
// `defaultVisible: false` 는 "처음 방문하는 사용자에게 기본 숨김".
// 기존 사용자가 이미 localStorage 에 visibility 선택을 저장해둔 경우엔
// 그 선택이 우선 (_pfGetColVisibility 로직 참조).
const PF_COL_DEFS = [
  { key: 'group',     cls: 'pf-col-group',     label: '그룹' },
  { key: 'curprice',  cls: 'pf-col-curprice',   label: '현재가' },
  { key: 'benchmark', cls: 'pf-col-benchmark',  label: '벤치마크' },
  { key: 'invested',  cls: 'pf-col-invested',   label: '거래대금',  defaultVisible: false },
  { key: 'buyprice',  cls: 'pf-col-buyprice',   label: '매입가' },
  { key: 'target',    cls: 'pf-col-target',     label: '목표가',     defaultVisible: false },
  { key: 'achiev',    cls: 'pf-col-achiev',     label: '달성률',     defaultVisible: false },
  { key: 'qty',       cls: 'pf-col-qty',        label: '수량' },
  { key: 'return',    cls: 'pf-col-return',      label: '수익률' },
  { key: 'mktval',    cls: 'pf-col-mktval',     label: '평가금액' },
  { key: 'dividend',  cls: 'pf-col-dividend',   label: '배당액' },
  { key: 'divyield',  cls: 'pf-col-divyield',   label: '배당수익률' },
  { key: 'weight',    cls: 'pf-col-weight',      label: '비중' },
  { key: 'date',      cls: 'pf-col-date',       label: '등록일자',  defaultVisible: false },
  { key: 'memo',      cls: 'pf-col-memo',       label: '메모',      defaultVisible: false },
];
let _pfColStyleEl = null;

function _pfLoadColVisibility() {
  try { return JSON.parse(localStorage.getItem('pf_col_vis') || 'null'); } catch { return null; }
}
function _pfSaveColVisibility(vis) {
  localStorage.setItem('pf_col_vis', JSON.stringify(vis));
}
function _pfGetColVisibility() {
  // Stored per-user overrides take priority; columns not yet known to
  // the stored map (newly added) fall back to their defaultVisible
  // setting. This lets us ship a new column hidden-by-default without
  // wiping existing users' customizations. defaultVisible defaults to
  // true when unspecified so bulk of the columns don't need the flag.
  const stored = _pfLoadColVisibility() || {};
  const result = {};
  for (const c of PF_COL_DEFS) {
    if (stored[c.key] !== undefined) {
      result[c.key] = !!stored[c.key];
    } else {
      result[c.key] = c.defaultVisible !== false;
    }
  }
  return result;
}
function _pfApplyColVisibility(vis) {
  if (!_pfColStyleEl) {
    _pfColStyleEl = document.createElement('style');
    document.head.appendChild(_pfColStyleEl);
  }
  const rules = PF_COL_DEFS
    .filter(c => !vis[c.key])
    .map(c => `.${c.cls} { display: none !important; }`)
    .join('\n');
  _pfColStyleEl.textContent = rules;
}
function pfToggleCol(key, checked) {
  const vis = _pfGetColVisibility();
  vis[key] = checked;
  _pfSaveColVisibility(vis);
  _pfApplyColVisibility(vis);
  // Sync checkbox UI
  const wrap = document.getElementById('pfColToggles');
  if (wrap) {
    wrap.querySelectorAll('input[type="checkbox"]').forEach(cb => {
      const k = PF_COL_DEFS.find(c => c.label === cb.parentElement.textContent.trim());
      if (k) cb.checked = vis[k.key];
    });
  }
}
function _pfApplyCompactRows(enabled) {
  PfStore.prefs.compactRows = !!enabled;
  document.body.classList.toggle('pf-compact-rows', PfStore.prefs.compactRows);
  const cb = document.getElementById('pfCompactToggle');
  if (cb && cb.checked !== PfStore.prefs.compactRows) cb.checked = PfStore.prefs.compactRows;
}
function pfToggleCompactRows(checked) {
  _pfApplyCompactRows(checked);
  try { localStorage.setItem(PF_COMPACT_ROWS_KEY, PfStore.prefs.compactRows ? '1' : '0'); } catch (e) {}
}
(function initPfCompactRows() {
  const apply = () => _pfApplyCompactRows(PfStore.prefs.compactRows);
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', apply);
  } else {
    apply();
  }
})();

let _pfColTogglesRendered = false;
function _pfRenderColToggles() {
  const vis = _pfGetColVisibility();
  _pfApplyColVisibility(vis);
  if (_pfColTogglesRendered) return;
  const wrap = document.getElementById('pfColToggles');
  if (!wrap) return;
  // 컬러 모드는 체크박스가 아니라 버튼 — 화면을 크게 바꾸는 보기 모드라
  // 컬럼 on/off 체크박스들과 구분되게 두고, 옆에 오늘의 급등/급락 요약을 붙인다.
  const heatOn = !!PfStore.prefs.heatMode;
  const heatToggle =
    `<button type="button" class="pf-heat-toggle js-pf-heat-toggle${heatOn ? ' active' : ''}" id="pfHeatToggle"`
    + ` aria-pressed="${heatOn ? 'true' : 'false'}"`
    + ` title="일간 등락 폭을 색·크기·애니메이션으로 강조 (상/하한가는 별도 표시)">🔥 컬러</button>`
    + `<span class="pf-heat-summary" id="pfHeatSummary" role="status" aria-live="polite"${heatOn ? '' : ' hidden'}></span>`
    + `<span class="pf-col-toggle-sep" aria-hidden="true"></span>`;
  const compactToggle =
    `<label class="pf-compact-toggle" title="태그·순서이동 아이콘을 숨기고 종목명을 한 줄로, 행 간격을 좁게 표시">`
    + `<input type="checkbox" id="pfCompactToggle" class="js-pf-compact-toggle"${PfStore.prefs.compactRows ? ' checked' : ''}> 컴팩트</label>`
    + `<span class="pf-col-toggle-sep" aria-hidden="true"></span>`;
  const colToggles = PF_COL_DEFS.map(c =>
    `<label><input type="checkbox" class="js-pf-col-toggle" data-col-key="${escapeHtml(c.key)}" ${vis[c.key] ? 'checked' : ''}> ${c.label}</label>`
  ).join('');
  wrap.innerHTML = heatToggle + compactToggle + colToggles;
  _pfColTogglesRendered = true;
}

function switchView(view, options = {}) {
  const lockedView = options.allowMobileLockOverride ? null : _mobileFixedView();
  if (lockedView && view !== lockedView) view = lockedView;
  PfStore.activeView = view;
  // 모달을 연 채로 화면을 바꾸는 경로가 여럿 있다(인사이트 모달의 "분석 화면"
  // 버튼, 투자일지 카드의 종목 링크 등). 모달 마크업은 자기 화면 안에 있어
  // 화면과 함께 가려지지만 body 의 스크롤 잠금은 남아 새 화면에서 휠·스크롤바가
  // 모두 죽는다. 전환 시점에 모달 상태를 확실히 되감는다.
  if (typeof closeAllManagedModals === 'function') closeAllManagedModals();
  // 브라우저 히스토리 동기화 — 이게 없으면 탭 전환은 DOM 만 바뀌고 URL/히스토리는 그대로라
  // 뒤로가기를 누르면 이전 탭이 아니라 앱 밖으로 나가버린다. skipHistory 는 (a) 최초 진입
  // 라우팅(app-main.js initApp, 이미 그 URL에 있으므로 다시 쓸 필요 없음)과 (b) popstate
  // 핸들러 자신(브라우저가 이미 URL을 바꿨으므로 여기서 또 pushState 하면 안 됨) 에서 쓴다.
  if (!options.skipHistory) {
    const path = PF_VIEW_PATHS[view] || '/investing';
    if (window.location.pathname.replace(/\/+$/, '') !== path) {
      history.pushState({ pfView: view }, '', path);
    }
  }
  // 데스크톱 상단 탭(.nav-btn)과 모바일 하단 탭바(.mnav-btn) 활성 상태를 함께 동기화.
  // 국민연금/인사이트 보드/스크리너는 최상위 탭이 아니라 "도구" 허브의 하위 화면이므로
  // 이 화면들에 있을 때도 "도구" 탭(data-view="labs")이 활성으로 표시된다.
  const navHighlightView = PF_TOOLS_HUB_VIEWS.has(view) ? 'labs' : view;
  document.querySelectorAll('.nav-btn, .mnav-btn').forEach(btn => btn.classList.toggle('active', btn.dataset.view === navHighlightView));
  const investingView = document.getElementById('investingView');
  const analysisView = document.getElementById('analysisView');
  const portfolioView = document.getElementById('portfolioView');
  const npsView = document.getElementById('npsView');
  const labsView = document.getElementById('labsView');
  const insightsView = document.getElementById('insightsView');
  const screenerView = document.getElementById('screenerView');
  const mastersView = document.getElementById('mastersView');
  const bondsView = document.getElementById('bondsView');
  if (investingView) investingView.style.display = view === 'investing' ? 'block' : 'none';
  analysisView.style.display = view === 'analysis' ? 'block' : 'none';
  portfolioView.style.display = view === 'portfolio' ? 'block' : 'none';
  if (npsView) npsView.style.display = view === 'nps' ? 'block' : 'none';
  if (labsView) labsView.style.display = view === 'labs' ? 'block' : 'none';
  if (insightsView) insightsView.style.display = view === 'insights' ? 'block' : 'none';
  if (screenerView) screenerView.style.display = view === 'screener' ? 'block' : 'none';
  if (mastersView) mastersView.style.display = view === 'masters' ? 'block' : 'none';
  if (bondsView) bondsView.style.display = view === 'bonds' ? 'block' : 'none';
  const activeEl = view === 'investing' ? investingView
                  : view === 'analysis' ? analysisView
                  : view === 'portfolio' ? portfolioView
                  : view === 'labs' ? labsView
                  : view === 'insights' ? insightsView
                  : view === 'screener' ? screenerView
                  : view === 'masters' ? mastersView
                  : view === 'bonds' ? bondsView
                  : npsView;
  if (activeEl) {
    activeEl.classList.remove('fade-in');
    void activeEl.offsetWidth;
    activeEl.classList.add('fade-in');
  }
  if (view === 'investing' && typeof loadInvestingDashboard === 'function') {
    loadInvestingDashboard();
  } else if (view === 'portfolio') {
    // 숨어 있던 동안에는 실측이 불가능(0×0)했으므로 보이게 된 지금 다시 잰다.
    pfScheduleSimpleTableHeightSync();
    loadPortfolio();
  } else if (view === 'nps') {
    loadNpsView();
  } else if (view === 'insights' && typeof loadInsightsBoard === 'function') {
    loadInsightsBoard();
  } else if (view === 'screener' && typeof loadScreener === 'function') {
    loadScreener();
  } else if (view === 'masters' && typeof loadMasters === 'function') {
    loadMasters();
  } else if (view === 'bonds' && typeof loadBondsView === 'function') {
    loadBondsView();
  }
  _updateQuoteSubscriptions();
  // 모바일에서 탭 전환 시 이전 뷰의 스크롤 위치가 남아 본문이 중간에서 시작하는
  // 어색함을 막기 위해 최상단으로 이동.
  if (typeof isCompactMobileViewport === 'function' && isCompactMobileViewport()) {
    window.scrollTo(0, 0);
  }
}

// 진입 직후 몇 차례에 걸쳐 최상단을 유지할 시점(ms).
const PF_SCROLL_TOP_HOLD_STEPS_MS = [60, 200, 500, 1000];

// 모바일 첫 진입은 반드시 최상단에서 시작해야 한다 — 포트폴리오 요약(집계 숫자)이
// 화면 맨 위라 조금만 밀려도 가장 먼저 가려진다.
//
// scrollTo(0, 0) 한 번으로는 부족했다. 진입 시점에는 표가 아직 비어 있어 문서가
// 화면보다 짧고(=스크롤할 여지가 없어 호출이 사실상 무의미), 정작 데이터가 채워져
// 문서가 길어진 뒤에 모바일 브라우저의 위치 복원(탭 되살리기·bfcache·iOS 백그라운드
// 복귀)이 이전 스크롤을 되살린다. history.scrollRestoration='manual' 은 새로고침·
// 히스토리 이동만 막을 뿐 이 경로들은 막지 못한다.
//
// 그래서 짧은 구간 동안 몇 번 더 되돌리되, 사용자가 먼저 손을 대면(스크롤·터치·키)
// 즉시 물러난다 — 의도적으로 내려본 화면을 도로 끌어올리지 않기 위해서다.
function holdPageScrollTop() {
  if (typeof window === 'undefined' || typeof window.scrollTo !== 'function') return;
  const events = ['touchstart', 'wheel', 'pointerdown', 'keydown'];
  const timers = [];
  const release = () => {
    while (timers.length) clearTimeout(timers.pop());
    events.forEach(type => window.removeEventListener(type, release));
  };
  const toTop = () => {
    if ((window.scrollY || window.pageYOffset || 0) !== 0) window.scrollTo(0, 0);
  };
  events.forEach(type => window.addEventListener(type, release, { passive: true }));
  toTop();
  PF_SCROLL_TOP_HOLD_STEPS_MS.forEach(delay => timers.push(setTimeout(toTop, delay)));
  timers.push(setTimeout(release, PF_SCROLL_TOP_HOLD_STEPS_MS[PF_SCROLL_TOP_HOLD_STEPS_MS.length - 1] + 1));
}

// nps-tracker 임베드 URL — 임베드 모드(embed=true) + 현재 앱 테마를 쿼리로 전달.
function _npsFrameSrc() {
  const cfg = window.APP_CONFIG && window.APP_CONFIG.integrations && window.APP_CONFIG.integrations.npsTracker;
  const base = (cfg && cfg.baseUrl) || 'https://ducklove.github.io/nps-tracker';
  const theme = document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light';
  // _=nonce: 부모 새로고침·탭 전환 시 iframe HTML 도 새로 받아 GitHub Pages 캐시로 인한 stale 방지.
  return base.replace(/\/+$/, '') + '/?embed=true&theme=' + theme + '&_=' + Date.now();
}
function loadNpsView({ force = false } = {}) {
  const container = document.getElementById('npsContent');
  if (!container) return;
  // 탭을 열 때마다 새 nonce 로 다시 로드하면 스크롤 위치가 매번 초기화됐다 —
  // 세션 내 최초 1회만 로드하고, 이후는 명시적 새로고침 버튼(force)에서만
  // 다시 받는다(UX 감사 P3). 테마 토글(syncNpsFrameTheme)은 별도로 항상 갱신.
  const existing = container.querySelector('iframe.nps-frame');
  if (existing && !force) return;
  // 국민연금 포트폴리오는 별도 정적 대시보드(nps-tracker)로 분리됐다. 허브는
  // 이를 iframe 으로 임베드만 하고, 요약은 투자정보 '분석 도구' 카드가 보여준다.
  const iframe = document.createElement('iframe');
  iframe.src = _npsFrameSrc();
  iframe.title = '국민연금 국내주식 포트폴리오';
  iframe.loading = 'lazy';
  iframe.className = 'nps-frame';
  iframe.setAttribute('referrerpolicy', 'no-referrer');
  container.classList.add('is-frame');
  container.innerHTML = '';
  container.appendChild(iframe);
}
// 테마 토글 시 임베드된 nps-tracker 도 같은 테마로 다시 로드한다(쿼리 갱신).
function syncNpsFrameTheme() {
  const ifr = document.querySelector('#npsContent iframe.nps-frame');
  if (ifr) ifr.src = _npsFrameSrc();
}
