// 기회 보드(/opportunities): 형제 도구 신호를 같은 점수로 모은 보유 무관 순위.
//
// 순위 계산은 서버(services/ecosystem/signals.py)가 하고, 이 파일은 그대로 그린다. 입력 없이 동작하며
// 화면에서는 도구와 거래대금으로 보기만 거른다. 점수는 과거 분포나 종목군 안의 위치일 뿐이므로 수익을
// 뜻하는 문구를 쓰지 않는다(안내문은 index.html #opportunitiesView).
//
// data-feature="opportunities" 로 switchView 가 지연 로드한다. 전역 의존(utils.js): apiFetchJson,
// escapeHtml, fmtSignalMetric, fmtSignalLiquidity, signalToolLink. 종목 이동: switchView, analyzeStock.

const OB_BASIS_LABELS = { history3y: '자기 3년 분포', crossSection: '종목군 내 순위' };
const OB_HEADLINE_METRICS = 2;
let _obData = null;
let _obTool = 'all';
let _obMinLiquidity = 0;
let _obLoadSeq = 0;
let _obBound = false;

async function loadOpportunityBoard() {
  _obBindEvents();
  const seq = ++_obLoadSeq;
  if (!_obData) _obSetBoard('<p class="opportunity-empty">후보를 모으는 중입니다.</p>');
  let data = null;
  try {
    data = await apiFetchJson('/api/external/opportunities', { fallback: null, timeoutMs: 35_000 });
  } catch (e) {
    console.warn('opportunity board failed', e);
  }
  if (seq !== _obLoadSeq) return;  // 다시 연 화면의 응답만 그린다
  if (!data || !Array.isArray(data.rows)) {
    if (!_obData) {
      _obSetTools('');
      _obSetBoard('<p class="opportunity-empty">기회 보드를 불러오지 못했습니다. 잠시 후 이 화면을 다시 열어 주세요.</p>');
    }
    return;
  }
  _obData = data;
  if (_obTool !== 'all' && !data.rows.some((row) => row.primary.tool === _obTool)) _obTool = 'all';
  _obRender();
}

function _obSetBoard(html) {
  const el = document.getElementById('opportunityBoard');
  if (el) el.innerHTML = html;
}

function _obSetTools(html) {
  const el = document.getElementById('opportunityTools');
  if (el) el.innerHTML = html;
}

function _obBindEvents() {
  if (_obBound) return;
  _obBound = true;
  document.getElementById('opportunityFilters')?.addEventListener('click', (event) => {
    const button = event.target.closest('[data-ob-tool]');
    if (!button) return;
    _obTool = button.dataset.obTool;
    _obRender();
  });
  document.getElementById('opportunityMinLiquidity')?.addEventListener('change', (event) => {
    _obMinLiquidity = Number(event.target.value) || 0;
    _obRender();
  });
  document.getElementById('opportunityBoard')?.addEventListener('click', (event) => {
    const link = event.target.closest('[data-ob-stock]');
    if (!link) return;
    event.preventDefault();
    if (typeof switchView === 'function') switchView('analysis');
    if (typeof analyzeStock === 'function') analyzeStock(link.dataset.obStock);
  });
}

function _obRender() {
  _obSetTools(_obToolsHtml(_obData));
  _obRenderFilters(_obData);
  _obSetBoard(_obBoardHtml(_obData));
}

function _obToolsHtml(data) {
  const eligible = data.eligible || {};
  return (data.tools || []).map((tool) => {
    const count = Number(eligible[tool.tool]) || 0;
    const facts = [];
    if (tool.asOf) facts.push(`기준일 ${tool.asOf}`);
    if (tool.status !== 'failed') {
      facts.push(count > data.perTool ? `후보 ${count.toLocaleString('ko-KR')}개 중 상위 ${data.perTool}개` : `후보 ${count}개`);
    }
    const state = tool.status === 'failed' ? ' is-failed' : tool.warning ? ' is-warning' : '';
    return `<div class="opportunity-tool${state}">`
      + `<strong>${escapeHtml(tool.label)}</strong>`
      + `<span>${escapeHtml(facts.join(' · '))}</span>`
      + (tool.warning ? `<p>${escapeHtml(tool.warning)}</p>` : '')
      + '</div>';
  }).join('');
}

function _obRenderFilters(data) {
  const el = document.getElementById('opportunityFilters');
  if (!el) return;
  const present = new Set(data.rows.map((row) => row.primary.tool));
  const options = [{ tool: 'all', label: '전체' }, ...(data.tools || []).filter((tool) => present.has(tool.tool))];
  el.innerHTML = options.map(({ tool, label }) => {
    const active = _obTool === tool;
    return `<button type="button" class="opportunity-filter${active ? ' is-active' : ''}" data-ob-tool="${escapeHtml(tool)}"`
      + ` aria-pressed="${active}">${escapeHtml(label)}</button>`;
  }).join('');
}

function _obPassesLiquidity(row) {
  if (!_obMinLiquidity) return true;
  const value = row.liquidity && row.liquidity.value;
  return value !== null && value !== undefined && Number(value) >= _obMinLiquidity;
}

function _obBoardHtml(data) {
  if (!data.rows.length) {
    const allFailed = (data.tools || []).every((tool) => tool.status === 'failed');
    return allFailed
      ? '<p class="opportunity-empty">도구 데이터를 받지 못했습니다. 잠시 후 이 화면을 다시 열어 주세요.</p>'
      : `<p class="opportunity-empty">점수 ${escapeHtml(data.minScore)} 이상인 후보가 없습니다.</p>`;
  }
  const inTool = data.rows.filter((row) => _obTool === 'all' || row.primary.tool === _obTool);
  const visible = inTool.filter(_obPassesLiquidity);
  const hidden = inTool.length - visible.length;
  const footnote = hidden
    ? `<p class="opportunity-footnote">거래대금 조건으로 후보 ${hidden}개를 숨겼습니다. 거래대금을 확인하지 못한 후보도 포함됩니다.</p>`
    : '';
  if (!visible.length) return `<p class="opportunity-empty">이 조건에 맞는 후보가 없습니다.</p>${footnote}`;
  const held = new Set(data.heldCodes || []);
  return '<div class="opportunity-table-wrap"><table class="opportunity-table">'
    + '<thead><tr><th scope="col">순위</th><th scope="col">종목</th><th scope="col">점수</th>'
    + '<th scope="col">수치</th><th scope="col">거래대금</th><th scope="col">기준일</th></tr></thead>'
    + `<tbody>${visible.map((row) => _obRowHtml(row, held)).join('')}</tbody></table></div>${footnote}`;
}

function _obRowHtml(row, held) {
  const signal = row.primary;
  const code = String(row.code);
  const link = signalToolLink(signal.tool, code, signal.url);
  const name = escapeHtml(row.name || code);
  const heldBadge = held.has(code) ? '<span class="opportunity-held">보유</span>' : '';
  const pairName = signal.pairName && signal.pairName !== row.name
    ? `<small class="opportunity-pair">${escapeHtml(signal.pairName)}</small>` : '';
  const metrics = (signal.metrics || []).slice(0, OB_HEADLINE_METRICS).map((item) => (
    `<span class="opportunity-metric"><small>${escapeHtml(item.label)}</small>`
    + `<strong>${escapeHtml(fmtSignalMetric(item))}</strong></span>`
  )).join('');
  const liquidity = row.liquidity || {};
  const warnings = (signal.warnings || []).map((text) => `<p class="opportunity-warning">${escapeHtml(text)}</p>`).join('');
  return `<tr class="opportunity-row${signal.stale ? ' is-stale' : ''}">`
    + `<td class="opportunity-rank" data-label="순위">${escapeHtml(row.rank)}</td>`
    + '<td class="opportunity-stock" data-label="종목">'
    + `<a href="/analysis?code=${encodeURIComponent(code)}" data-ob-stock="${escapeHtml(code)}">${name}</a> ${heldBadge}`
    + `<small class="opportunity-code">${escapeHtml(code)}</small>${pairName}`
    + `<a class="opportunity-tool-link" href="${escapeHtml(link.href)}"${link.attrs} target="_blank" rel="noopener noreferrer">${escapeHtml(signal.label)} 도구 열기 ↗</a>`
    + `${_obContextHtml(row)}${warnings}</td>`
    + `<td class="opportunity-score" data-label="점수" title="${escapeHtml(signal.scoreNote || '')}">`
    + `<strong>${escapeHtml(Math.round(Number(signal.score)))}</strong><small>${escapeHtml(OB_BASIS_LABELS[signal.scoreBasis] || '')}</small></td>`
    + `<td class="opportunity-metrics" data-label="수치">${metrics}</td>`
    + `<td class="opportunity-liquidity${liquidity.low ? ' is-low' : ''}" data-label="거래대금">${escapeHtml(fmtSignalLiquidity(liquidity))}`
    + `${liquidity.asOf ? `<small>${escapeHtml(liquidity.asOf)}</small>` : ''}${liquidity.low ? '<small>1억 미만</small>' : ''}</td>`
    + `<td class="opportunity-asof" data-label="기준일">${escapeHtml(signal.asOf || '확인 불가')}`
    + `${signal.distributionEnd ? `<small>분포 ${escapeHtml(signal.distributionEnd)}까지</small>` : ''}</td>`
    + '</tr>';
}

// 같은 종목(우선주면 보통주 포함)의 다른 도구 신호. 순위에는 영향을 주지 않는 문맥이다.
function _obContextHtml(row) {
  const chips = (row.others || []).map((signal) => {
    const first = (signal.metrics || [])[0];
    if (!first || Number(first.value) === 0) return '';  // 자사주 0% 는 문맥이 아니다
    const owner = signal.code !== row.code ? ` title="${escapeHtml(signal.code)} 기준"` : '';
    return `<span class="opportunity-context"${owner}>${escapeHtml(signal.label)} ${escapeHtml(fmtSignalMetric(first))}</span>`;
  }).filter(Boolean);
  return chips.length ? `<span class="opportunity-contexts">${chips.join('')}</span>` : '';
}
