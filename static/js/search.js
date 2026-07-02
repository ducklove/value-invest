// Theme
function toggleTheme() {
  const html = document.documentElement;
  const next = html.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
  html.setAttribute('data-theme', next);
  localStorage.setItem('theme', next);
  Object.values(charts).forEach(c => { if (c && c.resize) c.resize(); });
  if (typeof syncNpsFrameTheme === 'function') syncNpsFrameTheme();
  if (typeof syncMarketDashboardFrameTheme === 'function') syncMarketDashboardFrameTheme();
  trackEvent('theme_toggle', { theme: next });
}
(function initTheme() {
  const saved = localStorage.getItem('theme');
  if (saved) document.documentElement.setAttribute('data-theme', saved);
})();

// Search
const searchInput = document.getElementById('searchInput');
const dropdown = document.getElementById('dropdown');

function updateActiveItem() {
  const items = dropdown.querySelectorAll('.dropdown-item[data-stock]');
  items.forEach((el, i) => el.classList.toggle('active', i === selectedIdx));
  if (selectedIdx >= 0 && items[selectedIdx]) {
    items[selectedIdx].scrollIntoView({ block: 'nearest' });
  }
}

searchInput.addEventListener('input', () => {
  clearTimeout(searchTimeout);
  selectedIdx = -1;
  const q = searchInput.value.trim();
  if (q.length < 1) {
    dropdown.classList.remove('show'); // 검색 결과 지우기 — 칩 패널이 적용되면(모바일) 아래에서 다시 켠다.
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
  if (e.key === 'Escape') { dropdown.classList.remove('show'); selectedIdx = -1; return; }
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
    dropdown.classList.remove('show');
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
  if (!e.target.closest('.search-container')) dropdown.classList.remove('show');
});

async function doSearchAndAnalyze(q) {
  try {
    requireApiConfiguration();
    const resp = await apiFetch(`/api/search?q=${encodeURIComponent(q)}`);
    const data = await resp.json();
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
    const resp = await apiFetch(`/api/search?q=${encodeURIComponent(q)}`);
    const data = await resp.json();
    dropdown.innerHTML = '';
    if (data.length === 0) {
      dropdown.innerHTML = '<div class="dropdown-item" style="color:var(--text-secondary)">검색 결과 없음</div>';
    } else {
      data.forEach(item => {
        const div = document.createElement('div');
        div.className = 'dropdown-item';
        div.dataset.stock = item.stock_code;
        const name = document.createElement('span');
        name.textContent = item.corp_name;
        const code = document.createElement('span');
        code.style.color = 'var(--text-secondary)';
        code.textContent = item.stock_code;
        div.append(name, code);
        div.addEventListener('click', () => {
          dropdown.classList.remove('show');
          searchInput.value = item.corp_name;
          trackEvent('stock_select', { stock_code: item.stock_code, source: 'dropdown' });
          analyzeStock(item.stock_code);
        });
        dropdown.appendChild(div);
      });
    }
    trackEvent('search_results', { result_count: data.length });
    dropdown.classList.add('show');
  } catch (error) {
    dropdown.innerHTML = `<div class="dropdown-item" style="color:var(--text-secondary)">${escapeHtml(error.message || '검색 중 오류가 발생했습니다.')}</div>`;
    dropdown.classList.add('show');
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
  const name = document.createElement('span');
  name.textContent = item.corp_name;
  const code = document.createElement('span');
  code.style.color = 'var(--text-secondary)';
  code.textContent = item.stock_code;
  div.append(name, code);
  div.addEventListener('click', () => {
    dropdown.classList.remove('show');
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
    heading.textContent = label;
    dropdown.appendChild(heading);
    items.forEach(item => dropdown.appendChild(_dropdownStockChip(item, source)));
  };
  addSection('최근 검색', recent, 'search_chip_recent');
  addSection('관심 목록', starred.slice(0, 8), 'search_chip_starred');
  dropdown.classList.add('show');
}
