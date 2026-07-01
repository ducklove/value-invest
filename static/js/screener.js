// 밸류 스크리너 — P/E·P/B·ROE·배당수익률 등 조건으로 종목을 거르는 실험실 패널.
// 인사이트 보드와 동일한 Labs 진입 패턴을 따른다: /labs 카드 → /screener 직접 URL.
// 포맷터는 portfolio-render.js 의 전역(fmtKrw/fmtPct)에 의존하지 않는다 —
// 스크리너는 독립 뷰이므로 로드 순서 계약에 묶이지 않는 자체 헬퍼를 쓴다.

let _screenerLoaded = false;
let _screenerSpec = null; // {filters, sorts, default_sort, default_sort_dir, max_limit, coverage}
let _screenerPage = 0;
const _SCREENER_PAGE_SIZE = 50;

// 서버 필터 라벨(한글) → UI 표시용 짧은 라벨/단위. spec 의 label 을 그대로 쓰되
// 입력 placeholder 용 단위는 여기서 보강한다.
const FILTER_UNITS = {
  per: '배',
  pbr: '배',
  dividend_yield: '%',
  market_cap: '억원',
  roe: '%',
  debt_ratio: '%',
  operating_margin: '%',
  eps: '원',
  bps: '원',
};

function _scFmtNum(n, decimals = 2) {
  if (n === null || n === undefined || n === '' || Number.isNaN(Number(n))) return '-';
  return Number(n).toLocaleString(undefined, { maximumFractionDigits: decimals });
}

function _scFmtPct(n) {
  if (n === null || n === undefined || n === '' || Number.isNaN(Number(n))) return '-';
  return Number(n).toFixed(2) + '%';
}

function _scFmtMarketCap(n) {
  // 서버는 market_cap 을 '억원' 단위로 저장한다(analyzer.py 기준). 억 → 조/억 보기.
  const v = Number(n);
  if (!Number.isFinite(v)) return '-';
  if (Math.abs(v) >= 10000) return (v / 10000).toFixed(2) + '조';
  return _scFmtNum(v, 0) + '억';
}

async function loadScreener({ force = false } = {}) {
  const root = document.getElementById('screenerFilters');
  if (!root) return;
  if (_screenerLoaded && !force) return;
  try {
    const resp = await apiFetch('/api/screener/spec');
    if (!resp.ok) throw new Error('스크리너 설정을 불러오지 못했습니다.');
    _screenerSpec = await resp.json();
    _screenerLoaded = true;
    _renderScreenerCoverage();
    _renderScreenerFilters();
    _renderScreenerSortControls();
    _wireScreenerControls();
  } catch (err) {
    root.innerHTML = `<div class="screener-empty error">${escapeHtml(err.message)}</div>`;
  }
}

function _renderScreenerCoverage() {
  const el = document.getElementById('screenerCoverage');
  if (!el || !_screenerSpec) return;
  const c = _screenerSpec.coverage || {};
  // 분석 이력이 쌓인 종목만 검색 대상임을 명시 — 허위 기대를 막는다.
  el.innerHTML = `
    <div class="screener-coverage-row">
      <span>검색 대상: <strong>${_scFmtNum(c.valued || 0, 0)}</strong>개 종목 (시세 데이터 보유)</span>
      <span class="screener-coverage-muted">전체 상장 ${_scFmtNum(c.universe || 0, 0)}개 중 / 재무 데이터 ${_scFmtNum(c.fundamentals || 0, 0)}개</span>
    </div>
    <div class="screener-coverage-hint">종목을 분석(종목분석 탭)할 때마다 해당 종목이 스크리너 대상에 추가됩니다.</div>
  `;
}

function _renderScreenerFilters() {
  const root = document.getElementById('screenerFilters');
  if (!root || !_screenerSpec) return;
  const filters = _screenerSpec.filters || {};
  const cards = Object.entries(filters).map(([key, spec]) => {
    const unit = FILTER_UNITS[key] || '';
    return `
      <div class="screener-filter-card" data-filter="${escapeHtml(key)}">
        <div class="screener-filter-label">${escapeHtml(spec.label || key)}</div>
        <div class="screener-filter-inputs">
          <input type="number" class="screener-filter-min" data-key="${escapeHtml(key)}" data-op="min" placeholder="최소 ${escapeHtml(unit)}" step="any">
          <span class="screener-filter-sep">~</span>
          <input type="number" class="screener-filter-max" data-key="${escapeHtml(key)}" data-op="max" placeholder="최대 ${escapeHtml(unit)}" step="any">
        </div>
      </div>
    `;
  });
  root.innerHTML = cards.join('');
}

function _renderScreenerSortControls() {
  if (!_screenerSpec) return;
  const sortBy = document.getElementById('screenerSortBy');
  const sortDir = document.getElementById('screenerSortDir');
  if (sortBy) {
    const sorts = _screenerSpec.sorts || [];
    const labels = _screenerSpec.filters || {};
    sortBy.innerHTML = sorts.map(k => {
      const label = (labels[k] && labels[k].label) || k;
      return `<option value="${escapeHtml(k)}">${escapeHtml(label)}</option>`;
    }).join('');
    sortBy.value = _screenerSpec.default_sort || 'market_cap';
  }
  if (sortDir) {
    sortDir.value = _screenerSpec.default_sort_dir || 'desc';
  }
}

function _wireScreenerControls() {
  const runBtn = document.getElementById('screenerRunBtn');
  const resetBtn = document.getElementById('screenerResetBtn');
  if (runBtn && !runBtn.dataset.wired) {
    runBtn.dataset.wired = '1';
    runBtn.addEventListener('click', () => { _screenerPage = 0; _runScreener(); });
  }
  if (resetBtn && !resetBtn.dataset.wired) {
    resetBtn.dataset.wired = '1';
    resetBtn.addEventListener('click', _resetScreenerFilters);
  }
}

function _collectScreenerFilters() {
  const filters = {};
  document.querySelectorAll('#screenerFilters .screener-filter-inputs input').forEach(input => {
    const key = input.dataset.key;
    const op = input.dataset.op;
    const raw = input.value.trim();
    if (!key || !op || raw === '') return;
    const val = Number(raw);
    if (Number.isNaN(val)) return;
    if (!filters[key]) filters[key] = {};
    filters[key][op] = val;
  });
  return filters;
}

function _resetScreenerFilters() {
  document.querySelectorAll('#screenerFilters .screener-filter-inputs input').forEach(input => { input.value = ''; });
  _screenerPage = 0;
  const results = document.getElementById('screenerResults');
  const status = document.getElementById('screenerStatus');
  const pager = document.getElementById('screenerPager');
  if (results) results.innerHTML = '<div class="screener-empty">조건을 설정하고 \'스크린 실행\'을 누르세요.</div>';
  if (status) status.textContent = '';
  if (pager) pager.innerHTML = '';
}

async function _runScreener() {
  const filters = _collectScreenerFilters();
  const sortBy = document.getElementById('screenerSortBy');
  const sortDir = document.getElementById('screenerSortDir');
  const results = document.getElementById('screenerResults');
  const status = document.getElementById('screenerStatus');
  const pager = document.getElementById('screenerPager');
  if (!results) return;
  results.innerHTML = '<div class="screener-empty">스크리닝 중...</div>';
  if (status) status.textContent = '';
  if (pager) pager.innerHTML = '';
  const params = new URLSearchParams({
    sort_by: sortBy ? sortBy.value : 'market_cap',
    sort_dir: sortDir ? sortDir.value : 'desc',
    limit: String(_SCREENER_PAGE_SIZE),
    offset: String(_screenerPage * _SCREENER_PAGE_SIZE),
  });
  try {
    const resp = await apiFetch(`/api/screener/run?${params.toString()}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ filters }),
    });
    if (!resp.ok) {
      const data = await resp.json().catch(() => ({}));
      throw new Error(data.detail || `스크리닝에 실패했습니다. (${resp.status})`);
    }
    const data = await resp.json();
    _renderScreenerResults(data);
  } catch (err) {
    results.innerHTML = `<div class="screener-empty error">${escapeHtml(err.message)}</div>`;
  }
}

function _renderScreenerResults(data) {
  const results = document.getElementById('screenerResults');
  const status = document.getElementById('screenerStatus');
  const pager = document.getElementById('screenerPager');
  const rows = data.rows || [];
  const total = data.total || 0;
  if (status) status.textContent = `총 ${total.toLocaleString()}개 종목 일치`;
  if (rows.length === 0) {
    if (results) results.innerHTML = '<div class="screener-empty">조건에 맞는 종목이 없습니다.</div>';
    if (pager) pager.innerHTML = '';
    return;
  }
  const offset = data.offset || 0;
  const showingFrom = offset + 1;
  const showingTo = offset + rows.length;
  if (results) {
    results.innerHTML = `
      <div class="screener-table-wrap">
        <table class="screener-table">
          <thead>
            <tr>
              <th>코드</th>
              <th>종목명</th>
              <th class="num">종가</th>
              <th class="num">P/E</th>
              <th class="num">P/B</th>
              <th class="num">ROE</th>
              <th class="num">배당수익률</th>
              <th class="num">영업이익률</th>
              <th class="num">부채비율</th>
              <th class="num">시가총액</th>
            </tr>
          </thead>
          <tbody>
            ${rows.map(r => `
              <tr>
                <td class="mono"><a href="/analysis?code=${encodeURIComponent(r.stock_code || '')}" class="screener-code-link">${escapeHtml(r.stock_code || '')}</a></td>
                <td>${escapeHtml(r.corp_name || '')}</td>
                <td class="num">${_scFmtNum(r.close_price)}</td>
                <td class="num">${_scFmtNum(r.per)}</td>
                <td class="num">${_scFmtNum(r.pbr)}</td>
                <td class="num">${_scFmtPct(r.roe)}</td>
                <td class="num">${_scFmtPct(r.dividend_yield)}</td>
                <td class="num">${_scFmtPct(r.operating_margin)}</td>
                <td class="num">${_scFmtPct(r.debt_ratio)}</td>
                <td class="num">${_scFmtMarketCap(r.market_cap)}</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    `;
  }
  _renderScreenerPager(pager, total, showingFrom, showingTo);
}

function _renderScreenerPager(pager, total, showingFrom, showingTo) {
  if (!pager) return;
  const pageSize = _SCREENER_PAGE_SIZE;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const currentPage = _screenerPage;
  const parts = [];
  if (currentPage > 0) {
    parts.push(`<button class="sc-btn-ghost" data-page="0">«</button>`);
    parts.push(`<button class="sc-btn-ghost" data-page="${currentPage - 1}">‹</button>`);
  }
  parts.push(`<span class="screener-pager-info">${showingFrom.toLocaleString()}-${showingTo.toLocaleString()} / ${total.toLocaleString()} (페이지 ${currentPage + 1}/${totalPages})</span>`);
  if (currentPage < totalPages - 1) {
    parts.push(`<button class="sc-btn-ghost" data-page="${currentPage + 1}">›</button>`);
    parts.push(`<button class="sc-btn-ghost" data-page="${totalPages - 1}">»</button>`);
  }
  pager.innerHTML = parts.join('');
  pager.querySelectorAll('button[data-page]').forEach(btn => {
    btn.addEventListener('click', () => {
      _screenerPage = Number(btn.dataset.page);
      _runScreener();
      // 페이지 전환 후 상단으로.
      const top = document.getElementById('screenerView');
      if (top) top.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
  });
}
