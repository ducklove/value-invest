// 투자 대가의 전략 — 대표 투자 철학 탐색·비교 + 성향 기반 참고용 배분 시뮬레이션.
// "도구" 허브(/labs 카드) 또는 /masters 직접 URL로 진입한다.
// 전략 데이터는 서버 카탈로그(/api/masters/strategies, data/investment_masters.json)가
// 단일 소스다 — 이 파일은 렌더링만 하고 전략 내용을 하드코딩하지 않는다.
// 모든 배분 표시는 교육·참고용이며, disclaimer 도 카탈로그에서 내려온 문구를 그대로 쓴다.

let _mastersLoaded = false;
let _mastersCatalog = null; // {disclaimer, asset_groups, asset_classes, profile_options, strategies}
let _mastersSelectedId = null;
let _mastersSimSeq = 0; // 연타 시 마지막 요청만 렌더

// 자산군(class)별 색 — 배분 막대·범례 공용. 카탈로그에 없는 자산은 회색 폴백.
const _MASTERS_ASSET_COLORS = {
  equity_kr: '#1d4ed8',
  equity_global: '#60a5fa',
  bond_long: '#047857',
  bond_mid: '#10b981',
  bond_short: '#94a3b8',
  gold: '#f59e0b',
  commodities: '#b45309',
  reits: '#8b5cf6',
};
const _MASTERS_ASSET_FALLBACK_COLOR = '#64748b';

function _mastersAssetColor(asset) {
  return _MASTERS_ASSET_COLORS[asset] || _MASTERS_ASSET_FALLBACK_COLOR;
}

function _mastersRiskDots(level) {
  const filled = Math.max(0, Math.min(5, Number(level) || 0));
  let html = '';
  for (let i = 1; i <= 5; i++) {
    html += `<span class="masters-dot${i <= filled ? ' on' : ''}"></span>`;
  }
  return `<span class="masters-dots" title="위험 수준 ${filled}/5">${html}</span>`;
}

function _mastersFitLabels(fitIds, optionKey) {
  const options = (_mastersCatalog?.profile_options?.[optionKey]) || [];
  return fitIds.map(id => {
    const opt = options.find(o => o.id === id);
    return opt ? opt.label : id;
  });
}

async function loadMasters({ force = false } = {}) {
  const root = document.getElementById('mastersCards');
  if (!root) return;
  if (_mastersLoaded && !force) return;
  try {
    _mastersCatalog = await apiFetchJson('/api/masters/strategies', {
      errorMessage: '전략 카탈로그를 불러오지 못했습니다.',
    });
    _mastersLoaded = true;
    const strategies = _mastersCatalog.strategies || [];
    _mastersSelectedId = strategies.length ? strategies[0].id : null;
    _renderMastersDisclaimer();
    _renderMastersCards();
    _renderMasterDetail();
    _renderMastersCompare();
    _renderMastersSimForm();
  } catch (err) {
    root.innerHTML = `<div class="masters-empty error">${escapeHtml(err.message)}</div>`;
  }
}

function _renderMastersDisclaimer() {
  const el = document.getElementById('mastersDisclaimer');
  if (!el || !_mastersCatalog) return;
  el.textContent = _mastersCatalog.disclaimer || '';
}

function _renderMastersCards() {
  const root = document.getElementById('mastersCards');
  if (!root || !_mastersCatalog) return;
  const strategies = _mastersCatalog.strategies || [];
  root.innerHTML = strategies.map(s => `
    <button type="button" class="masters-card${s.id === _mastersSelectedId ? ' active' : ''}" data-strategy="${escapeHtml(s.id)}">
      <div class="masters-card-head">
        <strong>${escapeHtml(s.master)}</strong>
        ${_mastersRiskDots(s.risk_level)}
      </div>
      <span class="masters-card-title">${escapeHtml(s.title)}</span>
      <p class="masters-card-tagline">"${escapeHtml(s.tagline)}"</p>
    </button>
  `).join('');
  root.querySelectorAll('button[data-strategy]').forEach(btn => {
    btn.addEventListener('click', () => {
      _mastersSelectedId = btn.dataset.strategy;
      root.querySelectorAll('button[data-strategy]').forEach(b =>
        b.classList.toggle('active', b.dataset.strategy === _mastersSelectedId));
      _renderMasterDetail();
      const detail = document.getElementById('mastersDetail');
      if (detail && typeof detail.scrollIntoView === 'function') {
        detail.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      }
    });
  });
}

// 배분 목록 → 100% 누적 가로 막대 + 범례. 카드 상세와 시뮬레이션 결과가 공유한다.
function _mastersAllocationBar(allocation) {
  const rows = (allocation || []).filter(r => Number(r.weight) > 0);
  if (!rows.length) return '';
  const assetsMeta = _mastersCatalog?.asset_classes || {};
  const segments = rows.map(r => {
    const label = r.label || (assetsMeta[r.asset] && assetsMeta[r.asset].label) || r.asset;
    return `<span class="masters-bar-seg" style="width:${Number(r.weight)}%;background:${_mastersAssetColor(r.asset)}" title="${escapeHtml(label)} ${Number(r.weight)}%"></span>`;
  }).join('');
  const legend = rows.map(r => {
    const label = r.label || (assetsMeta[r.asset] && assetsMeta[r.asset].label) || r.asset;
    const note = r.note ? ` <span class="masters-legend-note">— ${escapeHtml(r.note)}</span>` : '';
    return `<li><span class="masters-legend-swatch" style="background:${_mastersAssetColor(r.asset)}"></span>`
      + `<span class="masters-legend-label">${escapeHtml(label)}</span>`
      + `<strong class="masters-legend-weight">${Number(r.weight)}%</strong>${note}</li>`;
  }).join('');
  return `<div class="masters-bar" role="img" aria-label="자산배분 막대">${segments}</div><ul class="masters-legend">${legend}</ul>`;
}

function _renderMasterDetail() {
  const root = document.getElementById('mastersDetail');
  if (!root || !_mastersCatalog) return;
  const s = (_mastersCatalog.strategies || []).find(x => x.id === _mastersSelectedId);
  if (!s) { root.innerHTML = ''; return; }
  const fitRisk = _mastersFitLabels(s.fit?.risk || [], 'risk').join(' · ');
  const fitHorizon = _mastersFitLabels(s.fit?.horizon || [], 'horizon').join(' · ');
  const list = items => (items || []).map(t => `<li>${escapeHtml(t)}</li>`).join('');
  root.innerHTML = `
    <div class="masters-detail-card">
      <div class="masters-detail-head">
        <div>
          <h3>${escapeHtml(s.master)} — ${escapeHtml(s.title)}</h3>
          <p class="masters-detail-tagline">"${escapeHtml(s.tagline)}"</p>
        </div>
        <div class="masters-detail-meta">
          <span>위험 수준 ${_mastersRiskDots(s.risk_level)}</span>
          <span>관리 손 ${_mastersRiskDots(s.effort_level)}</span>
        </div>
      </div>
      <p class="masters-detail-summary">${escapeHtml(s.summary)}</p>
      <div class="masters-detail-grid">
        <section>
          <h4>핵심 원칙</h4>
          <ul class="masters-list">${list(s.principles)}</ul>
        </section>
        <section>
          <h4>장점</h4>
          <ul class="masters-list pros">${list(s.pros)}</ul>
          <h4>단점</h4>
          <ul class="masters-list cons">${list(s.cons)}</ul>
        </section>
      </div>
      <section>
        <h4>어떤 투자자에게 맞나</h4>
        <p>${escapeHtml(s.fit?.description || '')}</p>
        <p class="masters-fit-chips">
          <span class="masters-chip">성향: ${escapeHtml(fitRisk)}</span>
          <span class="masters-chip">기간: ${escapeHtml(fitHorizon)}</span>
        </p>
      </section>
      <section>
        <h4>예시 자산배분 <span class="masters-muted">(교육·참고용)</span></h4>
        ${_mastersAllocationBar(s.base_allocation)}
        <p class="masters-muted">${escapeHtml(s.allocation_basis || '')}</p>
      </section>
      <section>
        <h4>리밸런싱 아이디어 <span class="masters-muted">(${escapeHtml(s.rebalancing?.frequency || '')})</span></h4>
        <ul class="masters-list">${list(s.rebalancing?.ideas)}</ul>
      </section>
      <p class="masters-references">참고: ${escapeHtml((s.references || []).join(' · '))}</p>
    </div>
  `;
}

function _renderMastersCompare() {
  const root = document.getElementById('mastersCompare');
  if (!root || !_mastersCatalog) return;
  const strategies = _mastersCatalog.strategies || [];
  if (!strategies.length) { root.innerHTML = ''; return; }
  const headCells = strategies.map(s =>
    `<th>${escapeHtml(s.master)}<span class="masters-compare-sub">${escapeHtml(s.title)}</span></th>`).join('');
  const row = (label, cell) =>
    `<tr><th scope="row">${label}</th>${strategies.map(cell).join('')}</tr>`;
  const firstItem = items => escapeHtml((items && items[0]) || '');
  root.innerHTML = `
    <div class="masters-compare-wrap">
      <table class="masters-compare-table">
        <thead><tr><th></th>${headCells}</tr></thead>
        <tbody>
          ${row('핵심 한 줄', s => `<td>"${escapeHtml(s.tagline)}"</td>`)}
          ${row('위험 수준', s => `<td>${_mastersRiskDots(s.risk_level)}</td>`)}
          ${row('관리 손', s => `<td>${_mastersRiskDots(s.effort_level)}</td>`)}
          ${row('적합 성향', s => `<td>${escapeHtml(_mastersFitLabels(s.fit?.risk || [], 'risk').join(' · '))}</td>`)}
          ${row('투자 기간', s => `<td>${escapeHtml(_mastersFitLabels(s.fit?.horizon || [], 'horizon').join(' · '))}</td>`)}
          ${row('대표 장점', s => `<td>${firstItem(s.pros)}</td>`)}
          ${row('대표 단점', s => `<td>${firstItem(s.cons)}</td>`)}
          ${row('리밸런싱', s => `<td>${escapeHtml(s.rebalancing?.frequency || '')}</td>`)}
        </tbody>
      </table>
    </div>
  `;
}

function _renderMastersSimForm() {
  const root = document.getElementById('mastersSimForm');
  if (!root || !_mastersCatalog) return;
  const options = _mastersCatalog.profile_options || {};
  const groups = _mastersCatalog.asset_groups || {};
  const selectOptions = key => (options[key] || []).map((o, i) =>
    `<option value="${escapeHtml(o.id)}"${i === (key === 'risk' ? 1 : 2) ? ' selected' : ''}>${escapeHtml(o.label)}</option>`).join('');
  const groupChecks = Object.entries(groups).map(([id, g]) =>
    `<label class="masters-sim-check"><input type="checkbox" class="js-masters-group" value="${escapeHtml(id)}" checked> ${escapeHtml(g.label)}</label>`).join('');
  root.innerHTML = `
    <div class="masters-sim-row">
      <label>위험 성향
        <select id="mastersSimRisk">${selectOptions('risk')}</select>
      </label>
      <label>투자 기간
        <select id="mastersSimHorizon">${selectOptions('horizon')}</select>
      </label>
      <div class="masters-sim-groups">
        <span>선호 자산군</span>
        ${groupChecks}
      </div>
      <button class="bt-run" id="mastersSimRunBtn" type="button">시뮬레이션</button>
    </div>
    <p class="masters-muted">선택한 성향·기간·자산군에 맞춰 각 전략의 예시 배분을 조정해 보여줍니다. 결과는 참고용 시뮬레이션입니다.</p>
  `;
  const runBtn = document.getElementById('mastersSimRunBtn');
  if (runBtn && !runBtn.dataset.wired) {
    runBtn.dataset.wired = '1';
    runBtn.addEventListener('click', () => { _runMastersSimulation(); });
  }
}

async function _runMastersSimulation() {
  const results = document.getElementById('mastersSimResults');
  if (!results) return;
  const risk = document.getElementById('mastersSimRisk')?.value;
  const horizon = document.getElementById('mastersSimHorizon')?.value;
  const assetGroups = Array.from(document.querySelectorAll('#mastersSimForm .js-masters-group:checked'))
    .map(el => el.value);
  if (!assetGroups.length) {
    results.innerHTML = '<div class="masters-empty error">선호 자산군을 최소 1개 선택해 주세요.</div>';
    return;
  }
  const seq = ++_mastersSimSeq;
  results.innerHTML = '<div class="masters-empty">시뮬레이션 중...</div>';
  try {
    const data = await apiFetchJson('/api/masters/simulate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ profile: { risk, horizon, asset_groups: assetGroups } }),
      errorMessage: '시뮬레이션에 실패했습니다.',
    });
    if (seq !== _mastersSimSeq) return; // 더 최근 요청이 있음
    _renderMastersSimResults(data);
  } catch (err) {
    if (seq !== _mastersSimSeq) return;
    results.innerHTML = `<div class="masters-empty error">${escapeHtml(err.message)}</div>`;
  }
}

function _renderMastersSimResults(data) {
  const root = document.getElementById('mastersSimResults');
  if (!root) return;
  const items = data.results || [];
  const cards = items.map((r, idx) => {
    const badge = idx === 0 ? '<span class="masters-chip best">내 성향과 가장 근접</span>' : '';
    const reasons = (r.fit_reasons || []).map(t => `<li>${escapeHtml(t)}</li>`).join('');
    const adjustments = (r.adjustments || []).length
      ? `<ul class="masters-adjustments">${r.adjustments.map(t => `<li>${escapeHtml(t)}</li>`).join('')}</ul>`
      : '';
    const body = r.allocation
      ? _mastersAllocationBar(r.allocation) + adjustments
      : `<div class="masters-empty">${escapeHtml(r.note || '이 조합으로는 배분을 구성할 수 없습니다.')}</div>`;
    return `
      <div class="masters-sim-card">
        <div class="masters-sim-card-head">
          <strong>${escapeHtml(r.master)} — ${escapeHtml(r.title)}</strong>
          <span class="masters-fit-score" title="성향 부합도 (교육용 점수)">부합도 ${Number(r.fit_score) || 0}</span>
          ${badge}
        </div>
        ${body}
        <details class="masters-sim-why">
          <summary>왜 이 점수인가</summary>
          <ul class="masters-list">${reasons}</ul>
        </details>
      </div>
    `;
  }).join('');
  root.innerHTML = `
    <div class="masters-disclaimer">${escapeHtml(data.disclaimer || '')}</div>
    ${cards}
  `;
}
