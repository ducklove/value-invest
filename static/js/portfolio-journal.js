// 투자 일지 — 매수/매도/메모 판단의 '이유'를 기록하고 결과와 함께 복기한다.
//
// GET/POST /api/portfolio/journal, PATCH/DELETE /api/portfolio/journal/{id}
// (routes/journal.py) 를 소비한다. 서버가 작성 시점의 현재가(price_at_entry)
// 와 목표가 스냅샷(target_price_at_entry)을 캡처하고, 목록 조회 때 현재가와
// 작성 후 수익률(since_entry_return_pct)을 계산해 내려준다.
//
// 두 표면(surface)에 산다:
// * 종목 분석 화면(#stockJournalSection) — 활성 종목의 기록 폼 + 타임라인.
//   analysis.js renderResult 가 loadStockJournal(stockCode) 를 호출한다.
// * 성과 탭 '투자 일지' 카드(#pfJournalWrap) — 전 종목 타임라인(읽기/복기).
//   pfSwitchTab(portfolio-performance.js)이 pfLoadJournalPanel() 로 lazy 로드.
//
// 규약: 로드는 silent(reportApiError silent + 표면 내 안내), 사용자 조작
// (기록/수정/삭제)은 토스트. note 는 escapeHtml 로만 렌더(원시 HTML 금지).
// 포맷터(fmtPct/returnClass/escapeHtml)는 portfolio-render.js / utils.js
// 공용 헬퍼를 재사용한다 — 여기서 중복 정의하지 않는다.

let _pfJournalEntries = null;      // 성과 탭 메모 — 전 종목 목록
let _pfJournalLoadSeq = 0;
let _stockJournalEntries = null;   // 분석 화면 메모 — 활성 종목 목록
let _stockJournalCode = null;
let _stockJournalLoadSeq = 0;
let _pfJournalEditing = null;      // 인라인 note 수정 중인 항목 id

const _PF_JOURNAL_TYPE_LABELS = { buy: '매수', sell: '매도', memo: '메모' };

function _pfJournalPanelEl() { return document.getElementById('pfJournalContent'); }
function _stockJournalListEl() { return document.getElementById('stockJournalList'); }

function _pfJournalMsg(el, message) {
  if (el) el.innerHTML = `<div class="pf-risk-empty">${escapeHtml(message)}</div>`;
}

// onclick 인라인 핸들러에 끼울 종목코드 — 서버 검증과 같은 문자 집합만 허용.
function _pfJournalSafeCode(code) {
  const c = String(code || '').toUpperCase();
  return /^[A-Z0-9._\-]{1,24}$/.test(c) ? c : '';
}

// 가격 표기 — 통화가 섞일 수 있어(국내 원/해외 달러) 통화 기호 없이 숫자만.
function _pfJournalNum(v) {
  if (v === null || v === undefined) return '-';
  const n = Number(v);
  if (!Number.isFinite(n)) return '-';
  return n.toLocaleString(undefined, { maximumFractionDigits: Number.isInteger(n) ? 0 : 2 });
}

// '2026-06-10T09:30:12.345' → '2026-06-10 09:30'
function _pfJournalDate(iso) {
  const s = String(iso || '');
  return s.length >= 16 ? `${s.slice(0, 10)} ${s.slice(11, 16)}` : s.slice(0, 10);
}

function _pfJournalBadge(entry) {
  const type = String(entry.entry_type || 'memo');
  const label = _PF_JOURNAL_TYPE_LABELS[type] || type;
  return `<span class="pf-journal-badge ${escapeHtml(type)}">${escapeHtml(label)}</span>`;
}

// 당시 가격 → 현재 가격 (작성 후 수익률 %) — 복기의 핵심 라인.
function _pfJournalPriceLine(entry) {
  if (entry.price_at_entry === null || entry.price_at_entry === undefined) {
    return '<span class="pf-journal-sub">당시 가격 미기록</span>';
  }
  const pct = entry.since_entry_return_pct;
  const pctHtml = (pct === null || pct === undefined)
    ? ''
    : ` <span class="${returnClass(pct)}">${fmtPct(pct)}</span>`;
  const current = (entry.current_price === null || entry.current_price === undefined)
    ? '-' : _pfJournalNum(entry.current_price);
  return `당시 <strong>${_pfJournalNum(entry.price_at_entry)}</strong>`
    + ` → 현재 <strong>${current}</strong>${pctHtml}`;
}

function _pfJournalMetaLine(entry) {
  const parts = [];
  if (entry.quantity !== null && entry.quantity !== undefined) {
    parts.push(`수량 ${_pfJournalNum(entry.quantity)}주`);
  }
  if (entry.target_price_at_entry !== null && entry.target_price_at_entry !== undefined) {
    parts.push(`당시 목표가 ${_pfJournalNum(entry.target_price_at_entry)}`);
  }
  return parts.length ? `<span class="pf-journal-sub">${parts.join(' · ')}</span>` : '';
}

// note 본문 — 수정 중이면 textarea 인라인 에디터로 교체.
function _pfJournalNoteHtml(entry) {
  const id = Number(entry.id);
  if (_pfJournalEditing === id) {
    return `<div class="pf-journal-edit">
      <textarea class="pf-journal-note-input" data-journal-edit-input="${id}" rows="3">${escapeHtml(entry.note)}</textarea>
      <div class="pf-journal-form-actions">
        <button class="pf-mini-btn" type="button" onclick="pfJournalSaveNote(${id})">저장</button>
        <button class="pf-mini-btn" type="button" onclick="pfJournalCancelEdit()">취소</button>
      </div>
    </div>`;
  }
  return `<div class="pf-journal-note">${escapeHtml(entry.note)}</div>`;
}

function _pfJournalCardHtml(entry, { showStock = false } = {}) {
  const id = Number(entry.id);
  const code = _pfJournalSafeCode(entry.stock_code);
  const stockHtml = showStock
    ? `<a href="#" class="pf-journal-stock" onclick="event.preventDefault(); switchView('analysis'); analyzeStock('${code}')">${escapeHtml(entry.stock_name || entry.stock_code)}</a>`
    : '';
  const edited = entry.updated_at && entry.updated_at !== entry.created_at
    ? ' <span class="pf-journal-sub">(수정됨)</span>' : '';
  return `<div class="pf-journal-card" data-journal-id="${id}">
    <div class="pf-journal-head">
      ${_pfJournalBadge(entry)}${stockHtml}
      <span class="pf-journal-date">${escapeHtml(_pfJournalDate(entry.created_at))}${edited}</span>
      <span class="pf-journal-actions">
        <button class="pf-journal-icon-btn" type="button" title="이유 수정" onclick="pfJournalEditNote(${id})">✏️</button>
        <button class="pf-journal-icon-btn" type="button" title="삭제" onclick="pfJournalDelete(${id})">🗑️</button>
      </span>
    </div>
    <div class="pf-journal-prices">${_pfJournalPriceLine(entry)} ${_pfJournalMetaLine(entry)}</div>
    ${_pfJournalNoteHtml(entry)}
  </div>`;
}

function _pfJournalTimelineHtml(entries, options) {
  return `<div class="pf-journal-list">${entries.map((e) => _pfJournalCardHtml(e, options)).join('')}</div>`;
}

// --- 성과 탭 카드(전 종목 타임라인) ----------------------------------------

function _pfJournalRenderPanel() {
  const el = _pfJournalPanelEl();
  if (!el || _pfJournalEntries === null) return;
  if (!_pfJournalEntries.length) {
    _pfJournalMsg(el, '기록된 투자 일지가 없습니다. 종목 분석 화면에서 매수·매도 판단의 이유를 남겨보세요.');
    return;
  }
  el.innerHTML = _pfJournalTimelineHtml(_pfJournalEntries, { showStock: true })
    + '<div class="pf-journal-foot">당시 가격 → 현재 가격 수익률로 과거 판단을 복기하세요. 새 기록은 종목 분석 화면에서 남길 수 있습니다.</div>';
}

// 성과 탭이 보일 때 호출(lazy). 메모가 있으면 그대로 그린다.
async function pfLoadJournalPanel({ force = false } = {}) {
  const el = _pfJournalPanelEl();
  if (!el) return;
  if (!force && _pfJournalEntries !== null) {
    _pfJournalRenderPanel();
    return;
  }
  const seq = ++_pfJournalLoadSeq;
  _pfJournalMsg(el, '투자 일지를 불러오는 중입니다...');
  try {
    const resp = await apiFetch('/api/portfolio/journal');
    if (resp.status === 401) {
      if (seq === _pfJournalLoadSeq) _pfJournalMsg(el, '로그인 후 이용할 수 있습니다.');
      return;
    }
    if (!resp.ok) throw new Error(`투자 일지 요청 실패 (${resp.status})`);
    const data = await resp.json();
    if (seq !== _pfJournalLoadSeq) return;
    _pfJournalEntries = Array.isArray(data.entries) ? data.entries : [];
    _pfJournalRenderPanel();
  } catch (e) {
    reportApiError(e, '투자 일지', { silent: true });
    if (seq === _pfJournalLoadSeq) {
      _pfJournalMsg(el, '투자 일지를 불러오지 못했습니다. 잠시 후 다시 시도해 주세요.');
    }
  }
}

// --- 종목 분석 화면(활성 종목 폼 + 타임라인) --------------------------------

function _stockJournalFormHtml() {
  return `<div class="pf-journal-form">
    <div class="pf-journal-form-row">
      <select id="stockJournalType" class="pf-modal-input">
        <option value="buy">매수</option>
        <option value="sell">매도</option>
        <option value="memo">메모</option>
      </select>
      <input id="stockJournalQty" class="pf-modal-input" type="number" min="0" step="any" placeholder="수량 (선택)">
      <button class="pf-mini-btn" type="button" onclick="stockJournalSubmit()">기록</button>
    </div>
    <textarea id="stockJournalNote" class="pf-journal-note-input" rows="2"
      placeholder="왜 이 결정을 내렸나요? 근거(밸류에이션·촉매·리스크)를 남겨두면 나중에 복기할 수 있습니다."></textarea>
    <div class="pf-journal-form-hint">저장 시점의 현재가와 목표가가 스냅샷으로 함께 기록됩니다.</div>
  </div>`;
}

function _stockJournalRenderList() {
  const el = _stockJournalListEl();
  if (!el || _stockJournalEntries === null) return;
  if (!_stockJournalEntries.length) {
    _pfJournalMsg(el, '아직 기록이 없습니다. 첫 판단 이유를 남겨보세요.');
    return;
  }
  el.innerHTML = _pfJournalTimelineHtml(_stockJournalEntries, { showStock: false });
}

// 분석 결과 렌더 때 호출 — 활성 종목의 일지 폼/타임라인을 채운다.
async function loadStockJournal(stockCode) {
  const formEl = document.getElementById('stockJournalForm');
  const listEl = _stockJournalListEl();
  if (!formEl || !listEl) return;
  _stockJournalCode = stockCode;
  _stockJournalEntries = null;
  const seq = ++_stockJournalLoadSeq;
  _pfJournalMsg(listEl, '투자 일지를 불러오는 중입니다...');
  try {
    const resp = await apiFetch(`/api/portfolio/journal?stock_code=${encodeURIComponent(stockCode)}`);
    if (resp.status === 401) {
      if (seq !== _stockJournalLoadSeq) return;
      formEl.innerHTML = '';
      _pfJournalMsg(listEl, '로그인하면 이 종목의 매수·매도 판단을 기록하고 복기할 수 있습니다.');
      return;
    }
    if (!resp.ok) throw new Error(`투자 일지 요청 실패 (${resp.status})`);
    const data = await resp.json();
    if (seq !== _stockJournalLoadSeq) return;
    formEl.innerHTML = _stockJournalFormHtml();
    _stockJournalEntries = Array.isArray(data.entries) ? data.entries : [];
    _stockJournalRenderList();
  } catch (e) {
    reportApiError(e, '투자 일지', { silent: true });
    if (seq === _stockJournalLoadSeq) {
      _pfJournalMsg(listEl, '투자 일지를 불러오지 못했습니다. 잠시 후 다시 시도해 주세요.');
    }
  }
}

// 분석 화면 폼 제출 — 서버가 가격/목표가 스냅샷을 캡처한다.
async function stockJournalSubmit() {
  const code = _stockJournalCode;
  if (!code) return;
  const note = (document.getElementById('stockJournalNote')?.value || '').trim();
  if (!note) { showToast('판단 이유를 입력해 주세요.'); return; }
  const entryType = document.getElementById('stockJournalType')?.value || 'memo';
  const qtyRaw = (document.getElementById('stockJournalQty')?.value || '').trim();
  const body = { stock_code: code, entry_type: entryType, note };
  if (qtyRaw) body.quantity = Number(qtyRaw);
  // 미보유 종목 기록 대비 표시명 — '삼성전자 (005930)' 의 앞부분.
  const nameText = document.getElementById('companyName')?.textContent || '';
  const stockName = nameText.split(' (')[0].trim();
  if (stockName) body.stock_name = stockName;
  try {
    const resp = await apiFetch('/api/portfolio/journal', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error(data.detail || `HTTP ${resp.status}`);
    const noteEl = document.getElementById('stockJournalNote');
    const qtyEl = document.getElementById('stockJournalQty');
    if (noteEl) noteEl.value = '';
    if (qtyEl) qtyEl.value = '';
    _pfJournalEntries = null; // 성과 탭 메모 무효화 — 다음 진입 때 재조회
    await loadStockJournal(code);
  } catch (e) {
    reportApiError(e, '투자 일지 기록');
  }
}

// --- note 인라인 수정 / 삭제 (두 표면 공용) ---------------------------------

function _pfJournalRerenderSurfaces() {
  _pfJournalRenderPanel();
  _stockJournalRenderList();
}

function _pfJournalApply(fn) {
  // 두 표면의 메모에 같은 변형을 적용 — 항목은 id 로 식별.
  if (Array.isArray(_pfJournalEntries)) _pfJournalEntries = fn(_pfJournalEntries);
  if (Array.isArray(_stockJournalEntries)) _stockJournalEntries = fn(_stockJournalEntries);
}

function pfJournalEditNote(entryId) {
  _pfJournalEditing = Number(entryId);
  _pfJournalRerenderSurfaces();
  document.querySelector(`[data-journal-edit-input="${_pfJournalEditing}"]`)?.focus();
}

function pfJournalCancelEdit() {
  _pfJournalEditing = null;
  _pfJournalRerenderSurfaces();
}

async function pfJournalSaveNote(entryId) {
  const id = Number(entryId);
  const input = document.querySelector(`[data-journal-edit-input="${id}"]`);
  const note = (input?.value || '').trim();
  if (!note) { showToast('판단 이유를 입력해 주세요.'); return; }
  try {
    const resp = await apiFetch(`/api/portfolio/journal/${id}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ note }),
    });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error(data.detail || `HTTP ${resp.status}`);
    _pfJournalApply((entries) => entries.map((e) => (
      Number(e.id) === id ? { ...e, note: data.note ?? note, updated_at: data.updated_at || e.updated_at } : e
    )));
    _pfJournalEditing = null;
    _pfJournalRerenderSurfaces();
  } catch (e) {
    reportApiError(e, '투자 일지 수정');
  }
}

async function pfJournalDelete(entryId) {
  const id = Number(entryId);
  if (!window.confirm('이 일지 항목을 삭제할까요? 복기 기록은 복구할 수 없습니다.')) return;
  try {
    const resp = await apiFetch(`/api/portfolio/journal/${id}`, { method: 'DELETE' });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error(data.detail || `HTTP ${resp.status}`);
    if (_pfJournalEditing === id) _pfJournalEditing = null;
    _pfJournalApply((entries) => entries.filter((e) => Number(e.id) !== id));
    _pfJournalRerenderSurfaces();
  } catch (e) {
    reportApiError(e, '투자 일지 삭제');
  }
}

if (typeof window !== 'undefined') {
  Object.assign(window, {
    pfLoadJournalPanel,
    loadStockJournal,
    stockJournalSubmit,
    pfJournalEditNote,
    pfJournalCancelEdit,
    pfJournalSaveNote,
    pfJournalDelete,
  });
}
