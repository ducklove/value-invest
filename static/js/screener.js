// 밸류 스크리너 — 현재 지표(P/E·P/B·ROE·배당수익률 …)와 최근 사업연도 추세
// (턴어라운드·배당 증가 …)로 종목을 거르는 패널.
// "도구" 허브(/labs 카드) 또는 /screener 직접 URL로 진입한다.
// 포맷터는 portfolio-render.js 의 전역(fmtKrw/fmtPct)에 의존하지 않는다 —
// 스크리너는 독립 뷰이므로 로드 순서 계약에 묶이지 않는 자체 헬퍼를 쓴다.
//
// 화면 구성:
//   · 히스토리 조건 칩 — 입력 없이 한 번 눌러 켜고 끄며, 누르면 바로 실행한다.
//   · 수치 조건 — 최소/최대 입력 후 '스크린 실행'(또는 Enter).
//   · 결과 표 — 종가를 뺀 모든 열 헤더로 오름·내림 정렬(서버 정렬, 결측은 항상 뒤).
//   · 관심종목 — 행의 별 버튼으로 담고, '관심종목만 보기'로 모아 본다.
//     저장소는 분석 화면의 관심종목과 같다(/api/screener/watchlist).

let _screenerLoaded = false;
let _screenerSpec = null; // {filters, conditions, sorts, default_sort, default_sort_dir, coverage, history, as_of}
let _screenerPage = 0;
const _SCREENER_PAGE_SIZE = 50;

let _screenerSort = { by: 'market_cap', dir: 'desc' };
const _screenerConditions = new Set();
let _screenerWatchOnly = false;
const _screenerWatch = { authenticated: false, codes: new Set() };
// 실행 요청 순번 — 늦게 도착한 이전 응답이 최신 화면(다른 정렬·초기화)을 덮지 않게 한다.
let _screenerRunSeq = 0;
let _screenerHistoryTimer = null;
let _screenerHistoryPolls = 0;
const _SCREENER_HISTORY_POLL_MS = 8000;
const _SCREENER_HISTORY_MAX_POLLS = 60;

// 서버 필터 라벨(한글) → 입력 placeholder 용 단위.
const FILTER_UNITS = {
  per: '배',
  pbr: '배',
  dividend_yield: '%',
  market_cap: '억원',
  roe: '%',
  debt_ratio: '%',
  operating_margin: '%',
  revenue_growth: '%',
  op_profit_growth: '%',
  dps_growth: '%',
};

// 결과 표 열. sort 가 없는 열(종가)은 정렬하지 않는다. text 열은 처음 누르면 오름차순,
// 숫자 열은 내림차순으로 시작한다.
const SCREENER_COLUMNS = [
  { key: 'stock_code', label: '코드', sort: 'stock_code', text: true },
  { key: 'corp_name', label: '종목명', sort: 'corp_name', text: true },
  { key: 'close_price', label: '종가', num: true },
  { key: 'market_cap', label: '시가총액', sort: 'market_cap', num: true },
  { key: 'per', label: 'P/E', sort: 'per', num: true },
  { key: 'pbr', label: 'P/B', sort: 'pbr', num: true },
  { key: 'roe', label: 'ROE', sort: 'roe', num: true },
  { key: 'operating_margin', label: '영업이익률', sort: 'operating_margin', num: true },
  { key: 'debt_ratio', label: '부채비율', sort: 'debt_ratio', num: true },
  { key: 'dividend_yield', label: '배당수익률', sort: 'dividend_yield', num: true },
  { key: 'dps', label: '주당배당금', sort: 'dps', num: true },
  { key: 'revenue_growth', label: '매출 증가율', sort: 'revenue_growth', num: true, title: '최근 사업연도 매출의 전년 대비 증가율' },
  { key: 'op_profit_growth', label: '영업이익 증가율', sort: 'op_profit_growth', num: true, title: '최근 사업연도 영업이익의 전년 대비 증가율 (전년이 흑자일 때만)' },
  { key: 'dps_growth', label: '배당 증가율', sort: 'dps_growth', num: true, title: '최근 사업연도 주당배당금의 전년 대비 증가율' },
];

function _scIsNum(n) {
  return n !== null && n !== undefined && n !== '' && !Number.isNaN(Number(n));
}

function _scFmtNum(n, decimals = 2) {
  if (!_scIsNum(n)) return '-';
  return Number(n).toLocaleString(undefined, { maximumFractionDigits: decimals });
}

function _scFmtPct(n) {
  if (!_scIsNum(n)) return '-';
  return Number(n).toFixed(2) + '%';
}

function _scFmtMarketCap(n) {
  // 서버는 market_cap 을 '억원' 단위로 준다. 억 → 조/억 보기.
  if (!_scIsNum(n)) return '-';
  const v = Number(n);
  if (Math.abs(v) >= 10000) {
    return (v / 10000).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + '조';
  }
  return _scFmtNum(v, 0) + '억';
}

function _scFmtAmount(won) {
  // 재무제표 금액(원) → 조/억.
  if (!_scIsNum(won)) return '-';
  return _scFmtMarketCap(Number(won) / 1e8);
}

// 부호가 있는 증감률. 한국 시장 관행대로 증가=빨강(up), 감소=파랑(down).
function _scSignedPct(n, unit = '%') {
  if (!_scIsNum(n)) return '<span class="sc-muted">-</span>';
  const v = Number(n);
  const cls = v > 0 ? 'sc-up' : v < 0 ? 'sc-down' : '';
  const sign = v > 0 ? '+' : '';
  return `<span class="${cls}">${sign}${v.toFixed(1)}${unit}</span>`;
}

function _scFmtDate(iso) {
  return typeof iso === 'string' && iso.length >= 10 ? iso.slice(0, 10) : '';
}

async function loadScreener({ force = false } = {}) {
  const root = document.getElementById('screenerFilters');
  if (!root) return;
  if (_screenerLoaded && !force) return;
  try {
    const [spec, watch] = await Promise.all([
      apiFetchJson('/api/screener/spec', { errorMessage: '스크리너 설정을 불러오지 못했습니다.' }),
      apiFetchJson('/api/screener/watchlist', { fallback: null }),
    ]);
    _screenerSpec = spec;
    _screenerLoaded = true;
    _screenerSort = {
      by: spec.default_sort || 'market_cap',
      dir: spec.default_sort_dir || 'desc',
    };
    _scApplyWatchlist(watch);
    _renderScreenerCoverage();
    _renderScreenerConditions();
    _renderScreenerFilters();
    _renderScreenerWatchToggle();
    _wireScreenerControls();
    _scWatchHistoryBuild();
  } catch (err) {
    root.innerHTML = `<div class="screener-empty error">${escapeHtml(err.message)}</div>`;
  }
}

function _scApplyWatchlist(watch) {
  _screenerWatch.authenticated = Boolean(watch && watch.authenticated);
  _screenerWatch.codes = new Set((watch && Array.isArray(watch.codes)) ? watch.codes : []);
}

function _scHistory() {
  return (_screenerSpec && _screenerSpec.history) || {};
}

function _scHistoryReady() {
  return _scHistory().status === 'ready';
}

function _renderScreenerCoverage() {
  const el = document.getElementById('screenerCoverage');
  if (!el || !_screenerSpec) return;
  const c = _screenerSpec.coverage || {};
  const h = _scHistory();
  // finance-pi full-universe 스냅샷 전체가 검색 대상 — 개별 종목 분석 이력과 무관하다.
  const tiles = [
    { label: '검색 대상', value: _scFmtNum(c.universe || 0, 0), note: 'KOSPI·KOSDAQ 상장 종목' },
    { label: '재무 데이터', value: _scFmtNum(c.fundamentals || 0, 0), note: '없는 종목은 해당 조건에서 제외' },
  ];
  if (h.status === 'ready') {
    tiles.push({
      label: '연도별 실적',
      value: _scFmtNum(h.covered || 0, 0),
      note: h.latest_fiscal_year ? `최근 사업연도 ${h.latest_fiscal_year}년까지` : '사업연도별 재무',
    });
    tiles.push({
      label: '배당 이력',
      value: _scFmtNum(h.dividend_covered || 0, 0),
      note: '최근 사업연도 배당 지급 종목',
    });
  }
  const asOf = _scFmtDate(_screenerSpec.as_of);
  const built = _scFmtDate(h.built_at);
  const stamps = [
    asOf ? `시세 기준일 ${asOf}` : '',
    built ? `연도별 실적·배당 수집일 ${built}` : '',
  ].filter(Boolean).join(' · ');
  el.innerHTML = `
    <div class="screener-stats">
      ${tiles.map(t => `
        <div class="screener-stat">
          <span class="screener-stat-label">${escapeHtml(t.label)}</span>
          <strong class="screener-stat-value">${escapeHtml(t.value)}</strong>
          <span class="screener-stat-note">${escapeHtml(t.note)}</span>
        </div>
      `).join('')}
    </div>
    ${stamps ? `<div class="screener-coverage-hint">${escapeHtml(stamps)}</div>` : ''}
  `;
}

function _renderScreenerConditions() {
  const root = document.getElementById('screenerConditions');
  if (!root || !_screenerSpec) return;
  const conditions = Array.isArray(_screenerSpec.conditions) ? _screenerSpec.conditions : [];
  const h = _scHistory();
  const ready = h.status === 'ready';
  let notice = '';
  if (h.status === 'building') {
    const pct = _scIsNum(h.progress) ? ` (${Math.round(Number(h.progress) * 100)}%)` : '';
    notice = `<div class="screener-notice" role="status">연도별 실적·배당 데이터를 모으는 중입니다${pct}. 하루 한 번, 2~3분 걸립니다 — 끝나면 조건이 자동으로 켜집니다.</div>`;
  } else if (!ready) {
    notice = '<div class="screener-notice is-warning" role="status">연도별 실적·배당 데이터를 불러올 수 없어 히스토리 조건을 쓸 수 없습니다. 수치 조건은 그대로 쓸 수 있습니다.</div>';
  }
  if (!ready) _screenerConditions.clear();
  const groups = [];
  conditions.forEach(cond => {
    let group = groups.find(g => g.name === cond.group);
    if (!group) { group = { name: cond.group, items: [] }; groups.push(group); }
    group.items.push(cond);
  });
  root.innerHTML = notice + groups.map(group => `
    <div class="screener-cond-group">
      <div class="screener-group-label">${escapeHtml(group.name || '')}</div>
      <div class="screener-cond-list">
        ${group.items.map(cond => {
          const active = _screenerConditions.has(cond.key);
          return `
            <button type="button" class="screener-cond${active ? ' is-active' : ''}" data-cond="${escapeHtml(cond.key)}"
              aria-pressed="${active ? 'true' : 'false'}"${ready ? '' : ' disabled'}>
              <span class="screener-cond-label">${escapeHtml(cond.label)}</span>
              <span class="screener-cond-desc">${escapeHtml(cond.description || '')}</span>
            </button>
          `;
        }).join('')}
      </div>
    </div>
  `).join('');
  root.querySelectorAll('button[data-cond]').forEach(btn => {
    btn.addEventListener('click', () => {
      const key = btn.dataset.cond;
      if (_screenerConditions.has(key)) _screenerConditions.delete(key);
      else _screenerConditions.add(key);
      const active = _screenerConditions.has(key);
      btn.classList.toggle('is-active', active);
      btn.setAttribute('aria-pressed', active ? 'true' : 'false');
      // 프리셋은 입력이 없으니 누르는 즉시 실행한다. 마지막 조건을 끄면 안내로 되돌린다.
      _screenerPage = 0;
      if (_scHasCriteria()) _runScreener();
      else _scShowIdle();
    });
  });
}

function _renderScreenerFilters() {
  const root = document.getElementById('screenerFilters');
  if (!root || !_screenerSpec) return;
  const filters = _screenerSpec.filters || {};
  const ready = _scHistoryReady();
  const groups = [];
  Object.entries(filters).forEach(([key, spec]) => {
    const name = spec.group || '';
    let group = groups.find(g => g.name === name);
    if (!group) { group = { name, items: [] }; groups.push(group); }
    group.items.push([key, spec]);
  });
  // 다시 그릴 때(히스토리 준비 완료) 이미 넣은 값은 유지한다.
  const previous = _collectScreenerFilters();
  root.innerHTML = groups.map(group => `
    <div class="screener-filter-group">
      ${group.name ? `<div class="screener-group-label">${escapeHtml(group.name)}</div>` : ''}
      <div class="screener-filter-grid">
        ${group.items.map(([key, spec]) => {
          const unit = FILTER_UNITS[key] || '';
          const disabled = spec.needs_history && !ready ? ' disabled' : '';
          const kept = previous[key] || {};
          const value = op => (kept[op] === undefined ? '' : ` value="${escapeHtml(String(kept[op]))}"`);
          return `
            <div class="screener-filter-card" data-filter="${escapeHtml(key)}">
              <div class="screener-filter-label">${escapeHtml(spec.label || key)}</div>
              <div class="screener-filter-inputs">
                <input type="number" inputmode="decimal" class="screener-filter-min" data-key="${escapeHtml(key)}" data-op="min"
                  aria-label="${escapeHtml(spec.label || key)} 최소" placeholder="최소 ${escapeHtml(unit)}" step="any"${value('min')}${disabled}>
                <span class="screener-filter-sep">~</span>
                <input type="number" inputmode="decimal" class="screener-filter-max" data-key="${escapeHtml(key)}" data-op="max"
                  aria-label="${escapeHtml(spec.label || key)} 최대" placeholder="최대 ${escapeHtml(unit)}" step="any"${value('max')}${disabled}>
              </div>
            </div>
          `;
        }).join('')}
      </div>
    </div>
  `).join('');
  root.querySelectorAll('.screener-filter-inputs input').forEach(input => {
    input.addEventListener('keydown', event => {
      if (event.key !== 'Enter') return;
      event.preventDefault();
      _screenerPage = 0;
      _runScreener();
    });
  });
}

function _renderScreenerWatchToggle() {
  const btn = document.getElementById('screenerWatchOnlyBtn');
  if (!btn) return;
  const count = _screenerWatch.codes.size;
  btn.classList.toggle('is-active', _screenerWatchOnly);
  btn.setAttribute('aria-pressed', _screenerWatchOnly ? 'true' : 'false');
  btn.disabled = !_screenerWatch.authenticated;
  btn.title = _screenerWatch.authenticated
    ? '별표로 담은 관심종목만 봅니다. 조건을 함께 걸 수 있습니다.'
    : '관심종목은 로그인 후 사용할 수 있습니다.';
  btn.innerHTML = `<span class="sc-star-glyph" aria-hidden="true">${_screenerWatchOnly ? '★' : '☆'}</span> 관심종목만 보기`
    + (_screenerWatch.authenticated ? ` <span class="screener-watch-count">${count.toLocaleString()}</span>` : '');
}

function _wireScreenerControls() {
  const runBtn = document.getElementById('screenerRunBtn');
  const resetBtn = document.getElementById('screenerResetBtn');
  const watchBtn = document.getElementById('screenerWatchOnlyBtn');
  if (runBtn && !runBtn.dataset.wired) {
    runBtn.dataset.wired = '1';
    runBtn.addEventListener('click', () => { _screenerPage = 0; _runScreener(); });
  }
  if (resetBtn && !resetBtn.dataset.wired) {
    resetBtn.dataset.wired = '1';
    resetBtn.addEventListener('click', _resetScreenerFilters);
  }
  if (watchBtn && !watchBtn.dataset.wired) {
    watchBtn.dataset.wired = '1';
    watchBtn.addEventListener('click', () => {
      if (!_screenerWatch.authenticated) return;
      _screenerWatchOnly = !_screenerWatchOnly;
      _renderScreenerWatchToggle();
      _screenerPage = 0;
      if (_scHasCriteria()) _runScreener();
      else _scShowIdle();
    });
  }
}

// 히스토리 데이터를 처음 모으는 동안 spec 을 다시 물어, 준비되면 조건을 켠다.
function _scWatchHistoryBuild() {
  if (_screenerHistoryTimer) { clearTimeout(_screenerHistoryTimer); _screenerHistoryTimer = null; }
  if (_scHistory().status !== 'building') { _screenerHistoryPolls = 0; return; }
  if (_screenerHistoryPolls >= _SCREENER_HISTORY_MAX_POLLS) return;
  _screenerHistoryTimer = setTimeout(async () => {
    _screenerHistoryTimer = null;
    _screenerHistoryPolls += 1;
    const spec = await apiFetchJson('/api/screener/spec', { fallback: null });
    if (spec) {
      _screenerSpec = spec;
      _renderScreenerCoverage();
      _renderScreenerConditions();
      if (_scHistoryReady()) _renderScreenerFilters();
    }
    _scWatchHistoryBuild();
  }, _SCREENER_HISTORY_POLL_MS);
}

function _collectScreenerFilters() {
  const filters = {};
  document.querySelectorAll('#screenerFilters .screener-filter-inputs input').forEach(input => {
    const key = input.dataset.key;
    const op = input.dataset.op;
    const raw = input.value.trim();
    if (!key || !op || raw === '' || input.disabled) return;
    const val = Number(raw);
    if (Number.isNaN(val)) return;
    if (!filters[key]) filters[key] = {};
    filters[key][op] = val;
  });
  return filters;
}

function _scHasCriteria() {
  return _screenerWatchOnly || _screenerConditions.size > 0 || Object.keys(_collectScreenerFilters()).length > 0;
}

function _scShowIdle() {
  const results = document.getElementById('screenerResults');
  const status = document.getElementById('screenerStatus');
  const pager = document.getElementById('screenerPager');
  _screenerRunSeq += 1; // 진행 중인 실행의 응답은 버린다.
  if (results) results.innerHTML = '<div class="screener-empty">히스토리 조건을 고르거나 수치 조건을 넣고 \'스크린 실행\'을 누르세요.</div>';
  if (status) status.textContent = '';
  if (pager) pager.innerHTML = '';
}

function _resetScreenerFilters() {
  document.querySelectorAll('#screenerFilters .screener-filter-inputs input').forEach(input => { input.value = ''; });
  _screenerConditions.clear();
  document.querySelectorAll('#screenerConditions .screener-cond').forEach(btn => {
    btn.classList.remove('is-active');
    btn.setAttribute('aria-pressed', 'false');
  });
  _screenerWatchOnly = false;
  _renderScreenerWatchToggle();
  _screenerPage = 0;
  _screenerSort = {
    by: (_screenerSpec && _screenerSpec.default_sort) || 'market_cap',
    dir: (_screenerSpec && _screenerSpec.default_sort_dir) || 'desc',
  };
  _scShowIdle();
}

async function _runScreener() {
  const filters = _collectScreenerFilters();
  const results = document.getElementById('screenerResults');
  const status = document.getElementById('screenerStatus');
  const pager = document.getElementById('screenerPager');
  if (!results) return;
  // 재정렬·페이지 이동 때는 표를 지우지 않고 흐리게만 해 화면이 튀지 않게 한다.
  const table = results.querySelector('.screener-table-wrap');
  if (table) table.classList.add('is-loading');
  else results.innerHTML = '<div class="screener-empty">스크리닝 중...</div>';
  if (!table && status) status.textContent = '';
  if (!table && pager) pager.innerHTML = '';
  const params = new URLSearchParams({
    sort_by: _screenerSort.by,
    sort_dir: _screenerSort.dir,
    limit: String(_SCREENER_PAGE_SIZE),
    offset: String(_screenerPage * _SCREENER_PAGE_SIZE),
  });
  const body = { filters };
  if (_screenerConditions.size > 0) body.conditions = Array.from(_screenerConditions);
  if (_screenerWatchOnly) body.watchlist_only = true;
  const seq = ++_screenerRunSeq;
  try {
    const data = await apiFetchJson(`/api/screener/run?${params.toString()}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      errorMessage: '스크리닝에 실패했습니다.',
    });
    if (seq !== _screenerRunSeq) return;
    _renderScreenerResults(data);
  } catch (err) {
    if (seq !== _screenerRunSeq) return;
    results.innerHTML = `<div class="screener-empty error">${escapeHtml(err.message)}</div>`;
    if (status) status.textContent = '';
    if (pager) pager.innerHTML = '';
  }
}

function _scStatusText(data) {
  const total = data.total || 0;
  const parts = [`총 ${total.toLocaleString()}개 종목 일치`];
  const labels = (_screenerSpec && Array.isArray(_screenerSpec.conditions))
    ? (data.conditions || []).map(key => (_screenerSpec.conditions.find(c => c.key === key) || {}).label).filter(Boolean)
    : [];
  if (labels.length) parts.push(labels.join(' · '));
  if (data.watchlist_only) {
    parts.push(data.watchlist_missing > 0
      ? `관심종목만 (스크리너 대상 밖 ${data.watchlist_missing.toLocaleString()}개 제외)`
      : '관심종목만');
  }
  return parts.join(' — ');
}

function _scHeaderCell(col) {
  const cls = col.num ? ' class="num"' : '';
  if (!col.sort) return `<th scope="col"${cls}>${escapeHtml(col.label)}</th>`;
  const active = _screenerSort.by === col.sort;
  const dir = active ? _screenerSort.dir : '';
  const aria = active ? (dir === 'asc' ? 'ascending' : 'descending') : 'none';
  const arrow = active ? (dir === 'asc' ? '▲' : '▼') : '';
  const title = `${col.title ? col.title + ' — ' : ''}눌러서 정렬`;
  return `<th scope="col"${cls} aria-sort="${aria}">`
    + `<button type="button" class="sc-sort${active ? ' is-active' : ''}" data-sort="${escapeHtml(col.sort)}" title="${escapeHtml(title)}">`
    + `${escapeHtml(col.label)}<span class="sc-sort-arrow" aria-hidden="true">${arrow}</span></button></th>`;
}

// 종목명 아래에 붙는 추세 배지 — 히스토리 조건과 같은 계산값을 쓴다.
function _scBadges(r) {
  const badges = [];
  if (r.turnaround_op === true) badges.push(['is-turn', '영업 흑자전환']);
  else if (r.turnaround_net === true) badges.push(['is-turn', '순익 흑자전환']);
  if (Number(r.dps_growth_years) >= 1) badges.push(['is-div', `배당↑ ${Number(r.dps_growth_years)}년`]);
  if (Number(r.revenue_growth_years) >= 3) badges.push(['is-growth', `매출↑ ${Number(r.revenue_growth_years)}년`]);
  if (Number(r.profit_years) >= 5) badges.push(['is-steady', '5년 흑자']);
  return badges.map(([cls, text]) => `<span class="sc-badge ${cls}">${escapeHtml(text)}</span>`).join('');
}

function _scCell(col, r) {
  switch (col.key) {
    case 'stock_code':
      return `<td class="mono"><a href="/analysis?code=${encodeURIComponent(r.stock_code || '')}" class="screener-code-link" title="종목 분석 열기">${escapeHtml(r.stock_code || '')}</a></td>`;
    case 'corp_name': {
      const market = r.market ? `<span class="sc-market">${escapeHtml(r.market)}</span>` : '';
      const badges = _scBadges(r);
      return `<td class="sc-name"><span class="sc-name-main">${escapeHtml(r.corp_name || '')}${market}</span>${badges ? `<span class="sc-badges">${badges}</span>` : ''}</td>`;
    }
    case 'close_price': return `<td class="num">${_scFmtNum(r.close_price, 0)}</td>`;
    case 'market_cap': return `<td class="num">${_scFmtMarketCap(r.market_cap)}</td>`;
    case 'per': return `<td class="num">${_scFmtNum(r.per)}</td>`;
    case 'pbr': return `<td class="num">${_scFmtNum(r.pbr)}</td>`;
    case 'roe': {
      const delta = _scIsNum(r.roe_change) ? `<small class="sc-delta">${_scSignedPct(r.roe_change, '%p')}</small>` : '';
      return `<td class="num">${_scFmtPct(r.roe)}${delta}</td>`;
    }
    case 'operating_margin': return `<td class="num">${_scFmtPct(r.operating_margin)}</td>`;
    case 'debt_ratio': return `<td class="num">${_scFmtPct(r.debt_ratio)}</td>`;
    case 'dividend_yield': return `<td class="num">${_scFmtPct(r.dividend_yield)}</td>`;
    case 'dps': return `<td class="num">${_scFmtNum(r.dps, 0)}</td>`;
    case 'revenue_growth': return `<td class="num">${_scSignedPct(r.revenue_growth)}</td>`;
    case 'op_profit_growth': return `<td class="num">${_scSignedPct(r.op_profit_growth)}</td>`;
    case 'dps_growth': return `<td class="num">${_scSignedPct(r.dps_growth)}</td>`;
    default: return '<td></td>';
  }
}

function _scStarButton(code) {
  const on = _screenerWatch.codes.has(code);
  return `<button type="button" class="sc-star${on ? ' is-on' : ''}" data-star="${escapeHtml(code)}" aria-pressed="${on ? 'true' : 'false'}"`
    + ` aria-label="${on ? '관심종목 해제' : '관심종목에 담기'}" title="${on ? '관심종목 해제' : '관심종목에 담기'}">${on ? '★' : '☆'}</button>`;
}

// 최근 사업연도 실적 표(행 펼침). 연도는 왼쪽이 과거.
function _scHistoryTable(r) {
  const history = Array.isArray(r.history) ? r.history : [];
  if (history.length === 0) {
    return '<div class="sc-detail-empty">연도별 실적 데이터가 없습니다.</div>';
  }
  const signed = (value, fmt) => {
    if (!_scIsNum(value)) return '<span class="sc-muted">-</span>';
    return Number(value) < 0 ? `<span class="sc-neg">${fmt(value)}</span>` : fmt(value);
  };
  const lines = [
    ['매출', h => signed(h.revenue, _scFmtAmount)],
    ['영업이익', h => signed(h.operating_profit, _scFmtAmount)],
    ['순이익', h => signed(h.net_income, _scFmtAmount)],
    ['영업이익률', h => signed(h.operating_margin, _scFmtPct)],
    ['ROE', h => signed(h.roe, _scFmtPct)],
    ['부채비율', h => signed(h.debt_ratio, _scFmtPct)],
    ['주당배당금', h => (_scIsNum(h.dps) ? _scFmtNum(h.dps, 0) + '원' : '<span class="sc-muted">-</span>')],
  ];
  return `
    <table class="sc-history">
      <thead><tr><th scope="col">사업연도</th>${history.map(h => `<th scope="col" class="num">${escapeHtml(String(h.fy))}</th>`).join('')}</tr></thead>
      <tbody>
        ${lines.map(([label, cell]) => `<tr><th scope="row">${label}</th>${history.map(h => `<td class="num">${cell(h)}</td>`).join('')}</tr>`).join('')}
      </tbody>
    </table>
  `;
}

function _renderScreenerResults(data) {
  const results = document.getElementById('screenerResults');
  const status = document.getElementById('screenerStatus');
  const pager = document.getElementById('screenerPager');
  const rows = data.rows || [];
  const total = data.total || 0;
  if (status) status.textContent = _scStatusText(data);
  if (rows.length === 0) {
    const empty = data.watchlist_only && _screenerWatch.codes.size === 0
      ? '관심종목이 아직 없습니다. 결과 표의 ☆ 를 눌러 담아 보세요.'
      : '조건에 맞는 종목이 없습니다.';
    if (results) results.innerHTML = `<div class="screener-empty">${empty}</div>`;
    if (pager) pager.innerHTML = '';
    return;
  }
  const offset = data.offset || 0;
  const showingFrom = offset + 1;
  const showingTo = offset + rows.length;
  const colCount = SCREENER_COLUMNS.length + 1;
  if (results) {
    results.innerHTML = `
      <div class="screener-table-wrap">
        <table class="screener-table">
          <thead>
            <tr>
              <th scope="col" class="sc-col-actions"><span class="sr-only">관심종목·상세</span></th>
              ${SCREENER_COLUMNS.map(_scHeaderCell).join('')}
            </tr>
          </thead>
          <tbody>
            ${rows.map((r, index) => `
              <tr class="sc-row" data-code="${escapeHtml(r.stock_code || '')}">
                <td class="sc-col-actions">
                  ${_scStarButton(r.stock_code || '')}
                  <button type="button" class="sc-expand" data-expand="${index}" aria-expanded="false"
                    aria-label="${escapeHtml(r.corp_name || '')} 연도별 실적 펼치기" title="연도별 실적">▸</button>
                </td>
                ${SCREENER_COLUMNS.map(col => _scCell(col, r)).join('')}
              </tr>
              <tr class="sc-detail-row" data-detail="${index}" hidden>
                <td colspan="${colCount}">
                  <div class="sc-detail">
                    <div class="sc-detail-head">
                      <strong>${escapeHtml(r.corp_name || '')}</strong>
                      <span>최근 사업연도 연간 실적${r.history_fy ? ` (${escapeHtml(String(r.history_fy))}년까지)` : ''}</span>
                      <a href="/analysis?code=${encodeURIComponent(r.stock_code || '')}">종목 분석 열기 →</a>
                    </div>
                    ${_scHistoryTable(r)}
                  </div>
                </td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
    `;
    _wireScreenerTable(results);
  }
  _renderScreenerPager(pager, total, showingFrom, showingTo);
}

function _wireScreenerTable(results) {
  results.querySelectorAll('button[data-sort]').forEach(btn => {
    btn.addEventListener('click', () => {
      const key = btn.dataset.sort;
      const col = SCREENER_COLUMNS.find(c => c.sort === key);
      if (_screenerSort.by === key) {
        _screenerSort = { by: key, dir: _screenerSort.dir === 'asc' ? 'desc' : 'asc' };
      } else {
        _screenerSort = { by: key, dir: col && col.text ? 'asc' : 'desc' };
      }
      _screenerPage = 0;
      _runScreener();
    });
  });
  results.querySelectorAll('button[data-expand]').forEach(btn => {
    btn.addEventListener('click', () => {
      const detail = results.querySelector(`tr[data-detail="${btn.dataset.expand}"]`);
      if (!detail) return;
      const open = detail.hidden;
      detail.hidden = !open;
      btn.setAttribute('aria-expanded', open ? 'true' : 'false');
      btn.textContent = open ? '▾' : '▸';
      btn.closest('tr').classList.toggle('is-open', open);
    });
  });
  results.querySelectorAll('button[data-star]').forEach(btn => {
    btn.addEventListener('click', () => _toggleScreenerStar(btn));
  });
}

function _scPaintStar(btn, on) {
  btn.classList.toggle('is-on', on);
  btn.setAttribute('aria-pressed', on ? 'true' : 'false');
  btn.setAttribute('aria-label', on ? '관심종목 해제' : '관심종목에 담기');
  btn.title = on ? '관심종목 해제' : '관심종목에 담기';
  btn.textContent = on ? '★' : '☆';
}

async function _toggleScreenerStar(btn) {
  const code = btn.dataset.star;
  const status = document.getElementById('screenerStatus');
  if (!code || btn.disabled) return;
  if (!_screenerWatch.authenticated) {
    if (status) status.textContent = '관심종목은 로그인 후 사용할 수 있습니다.';
    return;
  }
  const next = !_screenerWatch.codes.has(code);
  // 낙관적 반영 — 실패하면 되돌린다.
  btn.disabled = true;
  _scPaintStar(btn, next);
  try {
    const data = await apiFetchJson(`/api/screener/watchlist/${encodeURIComponent(code)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ starred: next }),
      errorMessage: '관심종목을 저장하지 못했습니다.',
    });
    if (data && data.starred) _screenerWatch.codes.add(code);
    else _screenerWatch.codes.delete(code);
    _scPaintStar(btn, _screenerWatch.codes.has(code));
  } catch (err) {
    _scPaintStar(btn, _screenerWatch.codes.has(code));
    if (status) status.textContent = err.message;
  } finally {
    btn.disabled = false;
    _renderScreenerWatchToggle();
  }
}

function _renderScreenerPager(pager, total, showingFrom, showingTo) {
  if (!pager) return;
  const pageSize = _SCREENER_PAGE_SIZE;
  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const currentPage = _screenerPage;
  const parts = [];
  if (currentPage > 0) {
    parts.push(`<button class="sc-btn-ghost" data-page="0" aria-label="첫 페이지">«</button>`);
    parts.push(`<button class="sc-btn-ghost" data-page="${currentPage - 1}" aria-label="이전 페이지">‹</button>`);
  }
  parts.push(`<span class="screener-pager-info">${showingFrom.toLocaleString()}-${showingTo.toLocaleString()} / ${total.toLocaleString()} (페이지 ${currentPage + 1}/${totalPages})</span>`);
  if (currentPage < totalPages - 1) {
    parts.push(`<button class="sc-btn-ghost" data-page="${currentPage + 1}" aria-label="다음 페이지">›</button>`);
    parts.push(`<button class="sc-btn-ghost" data-page="${totalPages - 1}" aria-label="마지막 페이지">»</button>`);
  }
  pager.innerHTML = parts.join('');
  pager.querySelectorAll('button[data-page]').forEach(btn => {
    btn.addEventListener('click', () => {
      _screenerPage = Number(btn.dataset.page);
      _runScreener();
      // 페이지 전환 후 결과 상단으로.
      const top = document.getElementById('screenerStatus') || document.getElementById('screenerView');
      if (top) top.scrollIntoView({ behavior: 'smooth', block: 'start' });
    });
  });
}
