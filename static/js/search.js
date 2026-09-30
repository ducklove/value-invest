// Theme
// 우선순위(생태계 계약, docs/ecosystem): ?theme=light|dark(적용만·저장 안 함) >
// localStorage 'theme' > prefers-color-scheme. 첫 페인트 전 적용은 index.html <head> 의
// vc:theme-boot 블록(static/ecosystem/vc-theme-boot.js 정본)이 이미 했다 — 여기서는 같은
// 규칙으로 다시 맞추고(부트 블록이 없던 캐시 HTML 대비), 토글·OS 설정 변경·다른 탭의
// 변경(vc-shell 의 storage 리스너가 보내는 vc:themechange)을 따라간다.
function _themeValid(t) { return t === 'light' || t === 'dark' ? t : null; }
function _themeFromUrl() {
  try { return _themeValid(new URLSearchParams(window.location.search).get('theme')); } catch (e) { return null; }
}
function _themeStored() {
  try { return _themeValid(localStorage.getItem('theme')); } catch (e) { return null; }
}
function _themeSystemQuery() {
  return window.matchMedia ? window.matchMedia('(prefers-color-scheme: dark)') : null;
}
function resolveTheme() {
  const mq = _themeSystemQuery();
  return _themeFromUrl() || _themeStored() || (mq && mq.matches ? 'dark' : 'light');
}
let _themeSyncedFor = null;
// 테마가 실제로 바뀐 경우에만 차트·임베드 iframe·생태계 링크를 한 번 동기화한다
// (토글·OS 변경·vc:themechange 가 같은 변경을 겹쳐 알려도 iframe 을 두 번 다시 받지 않는다).
function _syncThemeDependents() {
  const theme = document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light';
  if (theme === _themeSyncedFor) return;
  _themeSyncedFor = theme;
  if (typeof charts !== 'undefined') Object.values(charts).forEach(c => { if (c && c.resize) c.resize(); });
  if (typeof syncNpsFrameTheme === 'function') syncNpsFrameTheme();
  if (typeof syncBondsFrameTheme === 'function') syncBondsFrameTheme();
  if (typeof syncMarketDashboardFrameTheme === 'function') syncMarketDashboardFrameTheme();
  if (typeof ecoRefreshLinks === 'function') ecoRefreshLinks();
}
function applyTheme(theme) {
  const next = _themeValid(theme) || resolveTheme();
  if (document.documentElement.getAttribute('data-theme') !== next) {
    document.documentElement.setAttribute('data-theme', next);
  }
  _syncThemeDependents();
  return next;
}
// 사용자가 고른 테마는 저장하고, 한 번만 쓰는 ?theme 은 URL 에서 걷어낸다
// (남겨 두면 새로고침·공유 때 방금 고른 테마를 덮는다).
function _dropThemeParam() {
  try {
    const params = new URLSearchParams(window.location.search);
    if (!params.has('theme')) return;
    params.delete('theme');
    const query = params.toString();
    history.replaceState(history.state, '', window.location.pathname + (query ? '?' + query : '') + window.location.hash);
  } catch (e) { /* URL 정리는 부가 기능 */ }
}
function toggleTheme() {
  const next = document.documentElement.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
  try { localStorage.setItem('theme', next); } catch (e) { /* 사생활 보호 모드: 이 페이지에만 적용 */ }
  _dropThemeParam();
  applyTheme(next);
  trackEvent('theme_toggle', { theme: next });
}
(function initTheme() {
  document.documentElement.setAttribute('data-theme', resolveTheme());
  _themeSyncedFor = document.documentElement.getAttribute('data-theme');
  // 저장된 선택도 ?theme 도 없으면 OS 다크모드 설정을 실시간으로 따라간다(UX 감사 P3).
  // localStorage 에 저장하지 않으므로 다음 방문에도 계속 OS 설정을 따른다.
  const mq = _themeSystemQuery();
  const onSystemChange = () => { if (!_themeFromUrl() && !_themeStored()) applyTheme(); };
  if (mq && mq.addEventListener) mq.addEventListener('change', onSystemChange);
  else if (mq && mq.addListener) mq.addListener(onSystemChange);
  document.addEventListener('vc:themechange', () => _syncThemeDependents());
})();

// Search
const searchInput = document.getElementById('searchInput');
const dropdown = document.getElementById('dropdown');

// 검색창은 combobox(ARIA 1.2) — 정적 role/aria-controls 는 index.html 이 갖고,
// 열림/닫힘 상태(aria-expanded)와 활성 옵션(aria-activedescendant)만 여기서
// 동기화한다. 열 때는 항상 새 목록이 렌더된 직후라 활성 옵션을 초기화한다.
function setSearchDropdownExpanded(open) {
  dropdown.classList.toggle('show', open);
  searchInput.setAttribute('aria-expanded', open ? 'true' : 'false');
  searchInput.removeAttribute('aria-activedescendant');
}

function updateActiveItem() {
  const items = dropdown.querySelectorAll('.dropdown-item[data-stock]');
  items.forEach((el, i) => {
    el.classList.toggle('active', i === selectedIdx);
    el.setAttribute('aria-selected', i === selectedIdx ? 'true' : 'false');
    if (!el.id) el.id = `searchOption-${i}`;
  });
  if (selectedIdx >= 0 && items[selectedIdx]) {
    searchInput.setAttribute('aria-activedescendant', items[selectedIdx].id);
    items[selectedIdx].scrollIntoView({ block: 'nearest' });
  } else {
    searchInput.removeAttribute('aria-activedescendant');
  }
}

searchInput.addEventListener('input', () => {
  clearTimeout(searchTimeout);
  selectedIdx = -1;
  const q = searchInput.value.trim();
  if (q.length < 1) {
    setSearchDropdownExpanded(false); // 검색 결과 지우기 — 칩 패널이 적용되면(모바일) 아래에서 다시 켠다.
    showRecentStarredSearchPanel();
    return;
  }
  searchTimeout = setTimeout(() => doSearch(q), 250);
});

// 모바일(≤900px)은 사이드바가 숨겨져 최근 검색/관심 목록을 볼 방법이 없다 — 검색창을
// 빈 채로 포커스하면 같은 데이터를 드롭다운에 보여준다(UX 감사 P1③). 데스크톱은 이미
// 사이드바가 항상 보이므로 여기서는 아무 것도 하지 않는다.
searchInput.addEventListener('focus', () => { showRecentStarredSearchPanel(); });

searchInput.addEventListener('keydown', (e) => {
  const items = dropdown.querySelectorAll('.dropdown-item[data-stock]');
  if (e.key === 'Escape') { setSearchDropdownExpanded(false); selectedIdx = -1; return; }
  if (e.key === 'ArrowDown') {
    e.preventDefault();
    if (items.length > 0) { selectedIdx = Math.min(selectedIdx + 1, items.length - 1); updateActiveItem(); }
    return;
  }
  if (e.key === 'ArrowUp') {
    e.preventDefault();
    if (items.length > 0) { selectedIdx = Math.max(selectedIdx - 1, 0); updateActiveItem(); }
    return;
  }
  if (e.key === 'Enter') {
    e.preventDefault();
    setSearchDropdownExpanded(false);
    if (selectedIdx >= 0 && items[selectedIdx]) {
      items[selectedIdx].click();
    } else if (items.length > 0) {
      items[0].click();
    } else {
      const q = searchInput.value.trim();
      if (q.length > 0) doSearchAndAnalyze(q);
    }
    selectedIdx = -1;
  }
});

document.addEventListener('click', (e) => {
  if (!e.target.closest('.search-container')) setSearchDropdownExpanded(false);
});

async function doSearchAndAnalyze(q) {
  try {
    requireApiConfiguration();
    const data = await apiFetchJson(`/api/search?q=${encodeURIComponent(q)}`);
    if (data.length > 0) {
      searchInput.value = data[0].corp_name;
      trackEvent('stock_select', { stock_code: data[0].stock_code, source: 'enter' });
      analyzeStock(data[0].stock_code);
    }
  } catch (error) {
    showToast(error.message || '검색 중 오류가 발생했습니다.');
  }
}

async function doSearch(q) {
  try {
    requireApiConfiguration();
    const data = await apiFetchJson(`/api/search?q=${encodeURIComponent(q)}`);
    dropdown.innerHTML = '';
    if (data.length === 0) {
      dropdown.innerHTML = '<div class="dropdown-item" style="color:var(--text-secondary)">검색 결과 없음</div>';
    } else {
      data.forEach(item => {
        const div = document.createElement('div');
        div.className = 'dropdown-item';
        div.dataset.stock = item.stock_code;
        div.setAttribute('role', 'option');
        div.setAttribute('aria-selected', 'false');
        const name = document.createElement('span');
        name.textContent = item.corp_name;
        const code = document.createElement('span');
        code.style.color = 'var(--text-secondary)';
        code.textContent = item.stock_code;
        div.append(name, code);
        div.addEventListener('click', () => {
          setSearchDropdownExpanded(false);
          searchInput.value = item.corp_name;
          trackEvent('stock_select', { stock_code: item.stock_code, source: 'dropdown' });
          analyzeStock(item.stock_code);
        });
        dropdown.appendChild(div);
      });
    }
    trackEvent('search_results', { result_count: data.length });
    setSearchDropdownExpanded(true);
  } catch (error) {
    dropdown.innerHTML = `<div class="dropdown-item" style="color:var(--text-secondary)">${escapeHtml(error.message || '검색 중 오류가 발생했습니다.')}</div>`;
    setSearchDropdownExpanded(true);
  }
}

// 관심 목록은 sidebar 의 activeTab 전환(desktop 전용) 이 아니면 fetch 되지 않으므로,
// 모바일 칩 패널을 위해 별도로 한 번 가져와 세션 동안 캐시한다. recentListItems 는
// initApp() 이 이미 채워두므로("최근 검색") 재요청 없이 그대로 재사용한다.
let _searchStarredChipsCache = null;
let _searchStarredChipsLoading = false;

// 관심종목 토글(auth.js saveUserPreference) 직후 호출돼 다음 패널 오픈 시 새로 받아오게 한다.
function invalidateSearchStarredChipsCache() {
  _searchStarredChipsCache = null;
}

async function _fetchStarredChipsForSearch() {
  if (!currentUser) return [];
  if (_searchStarredChipsCache) return _searchStarredChipsCache;
  if (_searchStarredChipsLoading) return [];
  _searchStarredChipsLoading = true;
  try {
    const resp = await apiFetch('/api/cache/list?tab=starred');
    const data = await resp.json();
    _searchStarredChipsCache = Array.isArray(data) ? data : [];
  } catch (error) {
    _searchStarredChipsCache = [];
  } finally {
    _searchStarredChipsLoading = false;
  }
  return _searchStarredChipsCache;
}

function _dropdownStockChip(item, source) {
  const div = document.createElement('div');
  div.className = 'dropdown-item';
  div.dataset.stock = item.stock_code;
  div.setAttribute('role', 'option');
  div.setAttribute('aria-selected', 'false');
  const name = document.createElement('span');
  name.textContent = item.corp_name;
  const code = document.createElement('span');
  code.style.color = 'var(--text-secondary)';
  code.textContent = item.stock_code;
  div.append(name, code);
  div.addEventListener('click', () => {
    setSearchDropdownExpanded(false);
    searchInput.value = item.corp_name;
    trackEvent('stock_select', { stock_code: item.stock_code, source });
    switchView('analysis');
    analyzeStock(item.stock_code);
  });
  return div;
}

async function showRecentStarredSearchPanel() {
  if (!isCompactMobileViewport()) return;
  if (searchInput.value.trim().length > 0) return;
  const recent = recentListItems.slice(0, 8);
  const starred = await _fetchStarredChipsForSearch();
  // 응답을 기다리는 사이 사용자가 입력을 시작했다면 검색창 로직(input 리스너)이
  // 이미 처리 중이므로 이 패널로 덮어쓰지 않는다.
  if (searchInput.value.trim().length > 0) return;
  if (recent.length === 0 && starred.length === 0) return;

  dropdown.innerHTML = '';
  const addSection = (label, items, source) => {
    if (items.length === 0) return;
    const heading = document.createElement('div');
    heading.className = 'dropdown-section-label';
    heading.setAttribute('role', 'presentation'); // listbox 안의 비옵션 구분 라벨
    heading.textContent = label;
    dropdown.appendChild(heading);
    items.forEach(item => dropdown.appendChild(_dropdownStockChip(item, source)));
  };
  addSection('최근 검색', recent, 'search_chip_recent');
  addSection('관심 목록', starred.slice(0, 8), 'search_chip_starred');
  setSearchDropdownExpanded(true);
}
