// 투자 논거와 반증 조건. 상태·수치는 저장된 연간 재무자료로 서버에서 계산한다.
let _pfTheses = [];
let _pfThesisSeq = 0;
let _pfThesisBusy = false;
const _pfThesisStatuses = { breached: '반증 조건 충족', due: '검토 기한 도래', unknown: '자료 확인 필요', monitoring: '조건 미충족', manual: '직접 점검', archived: '보관됨', changed: '수정됨 · 다시 조회 필요' };
const _pfThesisOps = { lt: '<', lte: '≤', gt: '>', gte: '≥' };

function _pfThesisLink(url, label) {
  try {
    const parsed = new URL(url);
    if (!['http:', 'https:'].includes(parsed.protocol)) return '';
    return `<a href="${escapeHtml(parsed.href)}" target="_blank" rel="noopener noreferrer">${escapeHtml(label)}</a>`;
  } catch { return ''; }
}

function _pfThesisCheckHtml(check) {
  const value = check.value === null || check.value === undefined ? '자료 없음' : `${Number(check.value).toLocaleString('ko-KR', { maximumFractionDigits: 2 })}%`;
  return `<p>${escapeHtml(check.metric_label || '')} · 현재 ${value}${check.threshold !== null && check.threshold !== undefined ? ` · 반증 기준 ${escapeHtml(_pfThesisOps[check.operator] || '')} ${Number(check.threshold)}%` : ''}</p>
    <p class="pf-chart-note">${check.year ? `${Number(check.year)}년 연간 재무` : '연간 재무자료 미확보'}${check.stale ? ' · 오래된 자료' : ''}${check.deadline ? ` · 검토 기한 ${escapeHtml(check.deadline)}` : ''}${check.changed_at ? ` · 상태 변경 ${escapeHtml(check.changed_at.slice(0, 16).replace('T', ' '))} UTC` : ''}</p>
    <div class="pf-thesis-links">${_pfThesisLink(check.evidence_url, '등록한 근거')}${_pfThesisLink(check.filing_url, `최근 공시 원문 ${check.filing_name || ''} (재무 지표 원문과 다를 수 있음)`)}</div>`;
}

function _pfThesisRender() {
  const content = document.getElementById('pfThesisContent');
  if (!content) return;
  content.innerHTML = _pfTheses.map(item => `<article class="pf-thesis-card ${item.check.status === 'breached' ? 'breached' : ''}">
    <div class="pf-nav-header"><a href="/analysis?code=${encodeURIComponent(item.stock_code)}" title="종목 분석에서 재무자료 확인"><strong>${escapeHtml(item.stock_name || item.stock_code)}</strong> <small>${escapeHtml(item.stock_code)}</small></a><span>${escapeHtml(_pfThesisStatuses[item.check.status] || item.check.status)}</span></div>
    <p class="pf-thesis-text">${escapeHtml(item.thesis)}</p><p class="pf-thesis-text"><strong>반증 조건</strong> ${escapeHtml(item.invalidation)}</p>
    ${!item.archived ? _pfThesisCheckHtml(item.check) : ''}
    <div class="pf-thesis-buttons"><button type="button" class="pf-mini-btn" onclick="pfEditThesis(${Number(item.id)})">수정</button><button type="button" class="pf-mini-btn" onclick="pfArchiveThesis(${Number(item.id)})">${item.archived ? '추적 재개' : '보관'}</button><button type="button" class="pf-mini-btn" onclick="pfThesisHistory(${Number(item.id)})">변경 이력</button></div><div id="pfThesisHistory${Number(item.id)}"></div>
    </article>`).join('') || '<p class="pf-risk-empty">투자 논거와 반증 조건을 등록하세요. 투자일지에서도 논거를 연결할 수 있습니다.</p>';
}

async function pfLoadTheses() {
  const content = document.getElementById('pfThesisContent');
  if (!content) return;
  const seq = ++_pfThesisSeq;
  content.textContent = '투자 논거를 점검하고 있습니다…';
  try {
    const data = await apiFetchJson('/api/portfolio/theses', { errorMessage: '투자 논거 조회 실패' });
    if (seq !== _pfThesisSeq) return;
    _pfTheses = data.items || [];
    _pfThesisRender();
  } catch (error) {
    if (seq === _pfThesisSeq) content.textContent = error?.status === 401 ? '로그인 후 이용할 수 있습니다.' : '투자 논거를 불러오지 못했습니다. 다시 점검해 주세요.';
  }
}

function pfEditThesis(id = null, seed = {}) {
  const form = document.getElementById('pfThesisForm');
  if (!form) return;
  const item = _pfTheses.find(row => row.id === id) || seed;
  form.reset();
  form.dataset.id = id || '';
  form.dataset.journalId = item.journal_entry_id || '';
  form.dataset.archived = item.archived ? '1' : '0';
  for (const key of ['stock_code', 'thesis', 'invalidation', 'metric', 'operator', 'threshold', 'deadline', 'evidence_url']) {
    if (item[key] !== null && item[key] !== undefined) form.elements[key].value = item[key];
  }
  document.getElementById('pfThesisEditor').open = true;
  form.elements.stock_code.focus();
}

function pfThesisFromJournal(id) {
  const all = [...(typeof _pfJournalEntries !== 'undefined' ? _pfJournalEntries || [] : []), ...(typeof _stockJournalEntries !== 'undefined' ? _stockJournalEntries || [] : [])];
  const entry = all.find(row => row.id === id);
  if (!entry) return;
  switchView('portfolio');
  if (document.body.classList.contains('pf-mobile-simple') && typeof pfToggleSimpleMode === 'function') pfToggleSimpleMode();
  pfSwitchTab('performance');
  pfEditThesis(null, { stock_code: entry.stock_code, thesis: entry.note, journal_entry_id: entry.id });
  document.getElementById('pfThesisWrap')?.scrollIntoView({ behavior: 'smooth' });
}

async function _pfSaveThesis(payload, id) {
  await apiFetchJson(`/api/portfolio/theses${id ? `/${id}` : ''}`, { method: id ? 'PUT' : 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload), errorMessage: '투자 논거 저장 실패' });
  await pfLoadTheses();
  if (typeof pfLoadActionBoard === 'function') void pfLoadActionBoard({ force: true });
}

async function pfSubmitThesis(event) {
  event.preventDefault();
  if (_pfThesisBusy) return;
  const form = event.target;
  const payload = Object.fromEntries(new FormData(form));
  payload.stock_code = payload.stock_code.trim().toUpperCase();
  payload.threshold = payload.metric === 'manual' || payload.threshold === '' ? null : Number(payload.threshold);
  payload.deadline = payload.deadline || null;
  payload.evidence_url = payload.evidence_url || null;
  payload.journal_entry_id = form.dataset.journalId ? Number(form.dataset.journalId) : null;
  payload.archived = form.dataset.archived === '1';
  _pfThesisBusy = true;
  const button = form.querySelector('button[type="submit"]');
  button.disabled = true;
  try {
    await _pfSaveThesis(payload, form.dataset.id);
    form.reset(); form.dataset.id = ''; form.dataset.journalId = ''; form.dataset.archived = '0';
    document.getElementById('pfThesisEditor').open = false;
  } catch (error) { reportApiError(error, '투자 논거 저장'); }
  finally { _pfThesisBusy = false; button.disabled = false; }
}

async function pfArchiveThesis(id) {
  const item = _pfTheses.find(row => row.id === id);
  if (!item || _pfThesisBusy) return;
  _pfThesisBusy = true;
  try { await _pfSaveThesis({ ...item, archived: !item.archived }, id); }
  catch (error) { reportApiError(error, '추적 상태 변경'); }
  finally { _pfThesisBusy = false; }
}

async function pfThesisHistory(id) {
  const content = document.getElementById(`pfThesisHistory${id}`);
  if (!content) return;
  content.textContent = '변경 이력을 불러오는 중입니다…';
  try {
    const data = await apiFetchJson(`/api/portfolio/theses/${id}/history`, { errorMessage: '변경 이력 조회 실패' });
    content.innerHTML = (data.items || []).map(check => `<div class="pf-thesis-history"><strong>${escapeHtml(_pfThesisStatuses[check.status] || check.status)}</strong><p class="pf-thesis-text">${escapeHtml(check.invalidation || '')}</p>${_pfThesisCheckHtml(check)}</div>`).join('') || '기록된 변경이 없습니다.';
  } catch { content.textContent = '변경 이력을 불러오지 못했습니다.'; }
}
