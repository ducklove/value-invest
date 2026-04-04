// Theme
function toggleTheme() {
  const html = document.documentElement;
  const next = html.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
  html.setAttribute('data-theme', next);
  localStorage.setItem('theme', next);
  Object.values(charts).forEach(c => { if (c && c.resize) c.resize(); });
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
  if (q.length < 1) { dropdown.classList.remove('show'); return; }
  searchTimeout = setTimeout(() => doSearch(q), 250);
});

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
