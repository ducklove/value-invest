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

function _maFmtKrw(n) {
  if (n === null || n === undefined || Number.isNaN(Number(n))) return '-';
  return Math.round(Number(n)).toLocaleString('ko-KR') + '원';
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
    _renderMastersReviewControls();
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
      _mastersSelectMaster(btn.dataset.strategy);
      const detail = document.getElementById('mastersDetail');
      if (detail && typeof detail.scrollIntoView === 'function') {
        detail.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      }
    });
  });
}

// 카드 클릭·시뮬 폼의 대가 select 어느 쪽에서 골라도 선택 상태를 한 곳으로 동기화.
function _mastersSelectMaster(strategyId) {
  _mastersSelectedId = strategyId;
  document.querySelectorAll('#mastersCards button[data-strategy]').forEach(b =>
    b.classList.toggle('active', b.dataset.strategy === _mastersSelectedId));
  const select = document.getElementById('mastersSimStrategy');
  if (select && select.value !== strategyId) select.value = strategyId;
  _renderMasterDetail();
  _renderMastersReviewControls();
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
  const strategies = _mastersCatalog.strategies || [];
  const selectOptions = key => (options[key] || []).map((o, i) =>
    `<option value="${escapeHtml(o.id)}"${i === (key === 'risk' ? 1 : 2) ? ' selected' : ''}>${escapeHtml(o.label)}</option>`).join('');
  const strategyOptions = strategies.map(s =>
    `<option value="${escapeHtml(s.id)}"${s.id === _mastersSelectedId ? ' selected' : ''}>${escapeHtml(s.master)} — ${escapeHtml(s.title)}</option>`).join('');
  const groupChecks = Object.entries(groups).map(([id, g]) =>
    `<label class="masters-sim-check"><input type="checkbox" class="js-masters-group" value="${escapeHtml(id)}" checked> ${escapeHtml(g.label)}</label>`).join('');
  root.innerHTML = `
    <div class="masters-sim-row">
      <label>대가 선택
        <select id="mastersSimStrategy">${strategyOptions}</select>
      </label>
      <label>위험 성향
        <select id="mastersSimRisk">${selectOptions('risk')}</select>
      </label>
      <label>투자 기간
        <select id="mastersSimHorizon">${selectOptions('horizon')}</select>
      </label>
      <label>투자금액 (선택)
        <input type="number" id="mastersSimAmount" min="0" step="100000" placeholder="예: 10000000">
      </label>
      <div class="masters-sim-groups">
        <span>선호 자산군</span>
        ${groupChecks}
      </div>
      <button class="bt-run" id="mastersSimRunBtn" type="button">시뮬레이션</button>
    </div>
    <p class="masters-muted">선택한 대가의 예시 배분을 내 성향·기간·자산군에 맞춰 조정하고, 자산군별 대표 ETF 로 옮긴 참고용 포트폴리오를 보여줍니다. 금액을 입력하면 상품별 배정 금액과 대략 주수까지 계산합니다.</p>
  `;
  const runBtn = document.getElementById('mastersSimRunBtn');
  if (runBtn && !runBtn.dataset.wired) {
    runBtn.dataset.wired = '1';
    runBtn.addEventListener('click', () => { _runMastersSimulation(); });
  }
  const strategySelect = document.getElementById('mastersSimStrategy');
  if (strategySelect && !strategySelect.dataset.wired) {
    strategySelect.dataset.wired = '1';
    strategySelect.addEventListener('change', () => { _mastersSelectMaster(strategySelect.value); });
  }
}

async function _runMastersSimulation() {
  const results = document.getElementById('mastersSimResults');
  if (!results) return;
  const strategyId = document.getElementById('mastersSimStrategy')?.value;
  const risk = document.getElementById('mastersSimRisk')?.value;
  const horizon = document.getElementById('mastersSimHorizon')?.value;
  const amountRaw = document.getElementById('mastersSimAmount')?.value?.trim();
  const assetGroups = Array.from(document.querySelectorAll('#mastersSimForm .js-masters-group:checked'))
    .map(el => el.value);
  if (!assetGroups.length) {
    results.innerHTML = '<div class="masters-empty error">선호 자산군을 최소 1개 선택해 주세요.</div>';
    return;
  }
  const body = {
    strategy_id: strategyId,
    profile: { risk, horizon, asset_groups: assetGroups },
  };
  if (amountRaw) {
    const amount = Number(amountRaw);
    if (!Number.isFinite(amount) || amount <= 0) {
      results.innerHTML = '<div class="masters-empty error">투자금액은 양수로 입력해 주세요.</div>';
      return;
    }
    body.amount = amount;
  }
  const seq = ++_mastersSimSeq;
  results.innerHTML = '<div class="masters-empty">시뮬레이션 중...</div>';
  try {
    const data = await apiFetchJson('/api/masters/simulate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      errorMessage: '시뮬레이션에 실패했습니다.',
    });
    if (seq !== _mastersSimSeq) return; // 더 최근 요청이 있음
    _renderMastersSimResults(data);
  } catch (err) {
    if (seq !== _mastersSimSeq) return;
    results.innerHTML = `<div class="masters-empty error">${escapeHtml(err.message)}</div>`;
  }
}

// 상품 단위 포트폴리오 표 — 금액 입력 시 배정 금액/현재가/대략 주수 열이 추가된다.
function _mastersPortfolioTable(portfolio, amountSummary, quotesIncomplete) {
  const hasAmount = !!amountSummary;
  const headExtra = hasAmount ? '<th class="num">배정 금액</th><th class="num">현재가</th><th class="num">대략 주수</th>' : '';
  const rows = portfolio.map(r => {
    const inst = r.instrument || {};
    const note = inst.note ? `<div class="masters-inst-note">${escapeHtml(inst.note)}</div>` : '';
    const amountCells = hasAmount
      ? `<td class="num">${_maFmtKrw(r.amount)}</td>`
        + `<td class="num">${r.price != null ? _maFmtKrw(r.price) : '-'}</td>`
        + `<td class="num">${r.shares != null ? Number(r.shares).toLocaleString('ko-KR') + '주' : '-'}</td>`
      : '';
    return `
      <tr>
        <td>
          <span class="masters-inst-name">${escapeHtml(inst.name || '')}</span>
          <span class="masters-inst-code">${escapeHtml(inst.code || '')} · ${escapeHtml(inst.type || '')}</span>
          ${note}
        </td>
        <td>${escapeHtml(r.asset_label || '')}</td>
        <td class="num">${Number(r.weight)}%</td>
        ${amountCells}
      </tr>
    `;
  }).join('');
  let summaryHtml = '';
  if (hasAmount) {
    const residual = amountSummary.residual_cash != null
      ? `잔여 현금 ${_maFmtKrw(amountSummary.residual_cash)}`
      : '일부 상품 시세를 못 받아 잔여 현금은 계산하지 않았습니다';
    summaryHtml = `<p class="masters-muted">총 ${_maFmtKrw(amountSummary.total)} 중 약 ${_maFmtKrw(amountSummary.invested)} 배정 · ${residual}. 주수는 현재가 기준 단순 계산(수수료·호가 미반영)입니다.</p>`;
  }
  const staleNote = quotesIncomplete ? '<p class="masters-muted">일부 상품의 시세 조회에 실패해 주수 계산이 빠졌습니다.</p>' : '';
  return `
    <div class="masters-portfolio-wrap">
      <table class="masters-portfolio-table">
        <thead><tr><th>상품</th><th>자산군</th><th class="num">비중</th>${headExtra}</tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
    ${summaryHtml}${staleNote}
  `;
}

// ---- 대가의 시선 포트폴리오 진단 (LLM) ----

let _mastersReviewBusy = false;

function _mastersCurrentStrategy() {
  return (_mastersCatalog?.strategies || []).find(s => s.id === _mastersSelectedId) || null;
}

function _renderMastersReviewControls() {
  const root = document.getElementById('mastersReviewControls');
  if (!root || !_mastersCatalog) return;
  const loggedIn = typeof currentUser !== 'undefined' && !!currentUser;
  if (!loggedIn) {
    root.innerHTML = '<div class="masters-empty">로그인하면 내 포트폴리오를 선택한 대가의 관점으로 진단할 수 있습니다.</div>';
    return;
  }
  const strategy = _mastersCurrentStrategy();
  const master = strategy ? strategy.master : '';
  root.innerHTML = `
    <p class="masters-muted">위에서 선택한 대가(<strong>${escapeHtml(master)}</strong>)의 철학과 내 실제 보유 비중을 함께 LLM 에 보내 관점 진단을 생성합니다. 1~2분 걸릴 수 있고, 결과는 투자 조언이 아닌 참고용입니다.</p>
    <button class="bt-run" id="mastersReviewRunBtn" type="button"${_mastersReviewBusy ? ' disabled' : ''}>${escapeHtml(master)}의 시선으로 진단</button>
  `;
  const btn = document.getElementById('mastersReviewRunBtn');
  if (btn) btn.addEventListener('click', () => { _runMastersReview(); });
}

// marked+DOMPurify(전역 로드)가 있으면 마크다운 렌더, 없으면 이스케이프 텍스트 폴백.
function _mastersRenderMarkdown(md) {
  const text = String(md || '');
  if (typeof marked !== 'undefined' && typeof DOMPurify !== 'undefined') {
    try {
      return `<div class="masters-review-md">${DOMPurify.sanitize(marked.parse(text))}</div>`;
    } catch (e) { /* 폴백으로 */ }
  }
  return `<div class="masters-review-md masters-review-md-plain">${escapeHtml(text)}</div>`;
}

function _mastersGapTable(gap) {
  const rows = (gap || []).map(r => `
    <tr>
      <td>${escapeHtml(r.label)}</td>
      <td class="num">${Number(r.mine)}%</td>
      <td class="num">${Number(r.target)}%</td>
      <td class="num">${Number(r.diff) > 0 ? '+' : ''}${Number(r.diff)}%p</td>
    </tr>
  `).join('');
  return `
    <div class="masters-portfolio-wrap">
      <table class="masters-portfolio-table">
        <thead><tr><th>자산군</th><th class="num">내 비중</th><th class="num">대가 예시</th><th class="num">차이</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
    <p class="masters-muted">내 비중은 종목명 기반 자산군 근사치입니다 (시세 미확보 종목 제외).</p>
  `;
}

async function _runMastersReview() {
  const result = document.getElementById('mastersReviewResult');
  if (!result || _mastersReviewBusy) return;
  const strategy = _mastersCurrentStrategy();
  if (!strategy) return;
  _mastersReviewBusy = true;
  _renderMastersReviewControls();
  result.innerHTML = `<div class="masters-empty">${escapeHtml(strategy.master)}의 관점으로 진단 중... (최대 2분)</div>`;
  try {
    const data = await apiFetchJson('/api/masters/review', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ strategy_id: strategy.id }),
      timeoutMs: 180000,
      errorMessage: '진단 생성에 실패했습니다.',
    });
    _renderMastersReviewResult(data);
  } catch (err) {
    result.innerHTML = `<div class="masters-empty error">${escapeHtml(err.message)}</div>`;
  } finally {
    _mastersReviewBusy = false;
    _renderMastersReviewControls();
  }
}

function _renderMastersReviewResult(data) {
  const root = document.getElementById('mastersReviewResult');
  if (!root) return;
  const strategy = data.strategy || {};
  const b = data.breakdown || {};
  const chips = `
    <p class="masters-fit-chips">
      <span class="masters-chip">${Number(b.holdings_count) || 0}종목</span>
      <span class="masters-chip">상위3 집중도 ${Number(b.top3_weight) || 0}%</span>
      ${data.model ? `<span class="masters-chip">${escapeHtml(String(data.model))}</span>` : ''}
      ${data.truncated ? '<span class="masters-chip">출력 잘림 — 다시 실행 권장</span>' : ''}
    </p>
  `;
  root.innerHTML = `
    <div class="masters-disclaimer">${escapeHtml(data.disclaimer || '')}</div>
    <div class="masters-sim-card">
      <div class="masters-sim-card-head">
        <strong>${escapeHtml(strategy.master || '')} — ${escapeHtml(strategy.title || '')} 시선의 진단</strong>
      </div>
      ${chips}
      ${_mastersGapTable(data.gap)}
      ${_mastersRenderMarkdown(data.markdown)}
    </div>
  `;
}

function _renderMastersSimResults(data) {
  const root = document.getElementById('mastersSimResults');
  if (!root) return;
  const strategy = data.strategy || {};
  const reasons = (data.fit_reasons || []).map(t => `<li>${escapeHtml(t)}</li>`).join('');
  const adjustments = (data.adjustments || []).length
    ? `<ul class="masters-adjustments">${data.adjustments.map(t => `<li>${escapeHtml(t)}</li>`).join('')}</ul>`
    : '';
  const implementationNote = data.implementation_note
    ? `<p class="masters-impl-note">${escapeHtml(data.implementation_note)} `
      + `<a href="/screener" onclick="event.preventDefault(); switchView('screener');">밸류 스크리너 열기 →</a></p>`
    : '';
  const body = data.portfolio
    ? _mastersAllocationBar(data.allocation)
      + _mastersPortfolioTable(data.portfolio, data.amount, data.quotes_incomplete)
      + adjustments + implementationNote
    : `<div class="masters-empty">${escapeHtml(data.note || '이 조합으로는 포트폴리오를 구성할 수 없습니다.')}</div>`;
  root.innerHTML = `
    <div class="masters-disclaimer">${escapeHtml(data.disclaimer || '')}</div>
    <div class="masters-sim-card">
      <div class="masters-sim-card-head">
        <strong>${escapeHtml(strategy.master || '')} — ${escapeHtml(strategy.title || '')}</strong>
        <span class="masters-fit-score" title="성향 부합도 (교육용 점수)">부합도 ${Number(data.fit_score) || 0}</span>
      </div>
      ${body}
      <details class="masters-sim-why">
        <summary>부합도 근거</summary>
        <ul class="masters-list">${reasons}</ul>
      </details>
    </div>
  `;
}
