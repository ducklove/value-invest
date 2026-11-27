// 보유 종목 메모 빠른 입력 — 수정(✎) 모드에 들어가지 않고 메모만 고친다.
// 진입점: 표의 메모 칸, 종목명 옆 📝 배지(메모 컬럼을 숨겨도 보인다),
// 투자 인사이트 모달의 메모 섹션(모바일에서 행을 탭하면 열리는 창).
// 표 tbody 는 시세 tick 마다 innerHTML 로 다시 그려지므로, 편집기는 행 안이
// 아니라 body 에 붙는 팝오버로 띄워 입력 중인 값이 날아가지 않게 한다.
// 저장은 PUT /api/portfolio/{code}/metadata {memo} — 잔고·종목명은 건드리지 않는다.

const PF_MEMO_MAX_LEN = 500;
let _pfMemoPopover = null;

function pfMemoItem(stockCode) {
  return (PfStore.items || []).find(i => i.stock_code === stockCode) || null;
}

function pfCloseMemoEditor() {
  if (!_pfMemoPopover) return;
  const { el, onDocPointer, onReflow } = _pfMemoPopover;
  document.removeEventListener('pointerdown', onDocPointer, true);
  window.removeEventListener('resize', onReflow);
  window.removeEventListener('scroll', onReflow, true);
  el.remove();
  _pfMemoPopover = null;
}

// 같은 종목 메모 저장은 순서대로 보낸다 — 늦게 도착한 이전 응답이 최신 값을
// 덮어쓰지 않게(클라이언트 상태와 서버 최종값 모두).
const _pfMemoSaveChains = new Map();

function pfSaveHoldingMemo(stockCode, rawMemo, fallbackItem = null) {
  const prev = _pfMemoSaveChains.get(stockCode) || Promise.resolve();
  const next = prev.catch(() => {}).then(() => _pfSaveHoldingMemoNow(stockCode, rawMemo, fallbackItem));
  _pfMemoSaveChains.set(stockCode, next);
  const cleanup = () => { if (_pfMemoSaveChains.get(stockCode) === next) _pfMemoSaveChains.delete(stockCode); };
  next.then(cleanup, cleanup);
  return next;
}

// 분석 화면의 시세 tick 이 팝오버 기준(앵커) 버튼을 갈아엎지 않도록 열린 종목을 알려 준다.
function pfMemoEditorOpenFor(stockCode) {
  return !!(_pfMemoPopover && _pfMemoPopover.code === stockCode);
}

async function _pfSaveHoldingMemoNow(stockCode, rawMemo, fallbackItem = null) {
  const memo = String(rawMemo || '').trim().slice(0, PF_MEMO_MAX_LEN);
  const storeItem = pfMemoItem(stockCode);
  const item = storeItem || fallbackItem;
  if (item && memo === String(item.memo || '')) return item.memo || null;
  const data = await apiFetchJson(`/api/portfolio/${encodeURIComponent(stockCode)}/metadata`, {
    method: 'PUT',
    // 메모는 종목 단위 설정이다. 보고 있는 계좌에 없는 종목(종목분석의 합산 보유)도 저장되게.
    aggregate: !storeItem,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ memo }),
    errorMessage: '메모 저장에 실패했습니다.',
  });
  const saved = data && 'memo' in data ? (data.memo || null) : (memo || null);
  if (item) item.memo = saved;
  // 종목분석 "내 포지션" 바 등 다른 화면이 같은 메모를 다시 그리도록 알린다.
  document.dispatchEvent(new CustomEvent('pf:memo-saved', { detail: { code: stockCode, memo: saved } }));
  if (typeof renderPortfolio === 'function') renderPortfolio();
  showToast(saved ? '메모를 저장했습니다.' : '메모를 지웠습니다.', 'success');
  return saved;
}

function _pfPositionMemoPopover(el, anchor) {
  const rect = anchor.getBoundingClientRect();
  const width = Math.min(340, window.innerWidth - 24);
  el.style.width = `${width}px`;
  let left = rect.left;
  if (left + width > window.innerWidth - 12) left = window.innerWidth - 12 - width;
  left = Math.max(12, left);
  const height = el.offsetHeight || 180;
  let top = rect.bottom + 6;
  if (top + height > window.innerHeight - 12 && rect.top - height - 6 > 12) top = rect.top - height - 6;
  el.style.left = `${Math.round(left)}px`;
  el.style.top = `${Math.round(Math.max(12, top))}px`;
}

function _pfAutoGrowMemo(textarea) {
  textarea.style.height = 'auto';
  textarea.style.height = `${Math.min(textarea.scrollHeight, 220)}px`;
}

function pfOpenMemoEditor(stockCode, anchor, fallbackItem = null) {
  // fallbackItem: 포트폴리오를 아직 열지 않아 PfStore 가 비어 있을 때(종목분석 화면) 쓰는 보유 행.
  const item = pfMemoItem(stockCode) || fallbackItem;
  if (!item || !anchor) return;
  if (_pfMemoPopover && _pfMemoPopover.code === stockCode) { _pfMemoPopover.el.querySelector('textarea')?.focus(); return; }
  pfCloseMemoEditor();

  const el = document.createElement('div');
  el.className = 'pf-memo-popover';
  el.setAttribute('role', 'dialog');
  el.setAttribute('aria-label', `${item.stock_name || stockCode} 메모`);
  el.innerHTML = `
    <div class="pf-memo-popover-head"><strong>${escapeHtml(item.stock_name || stockCode)}</strong><span>메모</span></div>
    <textarea class="pf-memo-popover-input" maxlength="${PF_MEMO_MAX_LEN}" rows="3" placeholder="매수 이유, 확인할 것, 다음 행동…"></textarea>
    <div class="pf-memo-popover-foot">
      <span class="pf-memo-popover-hint">Enter 저장 · Shift+Enter 줄바꿈 · Esc 취소</span>
      <span class="pf-memo-popover-count"></span>
      <button type="button" class="pf-memo-popover-btn js-pf-memo-cancel">취소</button>
      <button type="button" class="pf-memo-popover-btn primary js-pf-memo-save">저장</button>
    </div>`;
  document.body.appendChild(el);
  const input = el.querySelector('textarea');
  const count = el.querySelector('.pf-memo-popover-count');
  const saveBtn = el.querySelector('.js-pf-memo-save');
  input.value = item.memo || '';
  const updateCount = () => { count.textContent = `${input.value.length}/${PF_MEMO_MAX_LEN}`; };
  updateCount();

  let saving = false;
  const save = async () => {
    if (saving) return;
    saving = true;
    saveBtn.disabled = true;
    input.disabled = true;
    try {
      await pfSaveHoldingMemo(stockCode, input.value, item);
      if (_pfMemoPopover && _pfMemoPopover.el === el) pfCloseMemoEditor();
    } catch (err) {
      saving = false;
      if (!el.isConnected) { reportApiError(err, '메모 저장'); return; }
      saveBtn.disabled = false;
      input.disabled = false;
      input.focus();
      reportApiError(err, '메모 저장');
    }
  };
  input.addEventListener('input', () => { updateCount(); _pfAutoGrowMemo(input); });
  input.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); pfCloseMemoEditor(); anchor.focus?.(); }
    else if (e.key === 'Enter' && !e.shiftKey && !e.isComposing) { e.preventDefault(); save(); }
  });
  saveBtn.addEventListener('click', save);
  el.querySelector('.js-pf-memo-cancel').addEventListener('click', () => pfCloseMemoEditor());

  // 바깥을 누르면 저장하고 닫는다 — 입력한 내용을 실수로 잃지 않게.
  const onDocPointer = (e) => {
    if (el.contains(e.target) || anchor.contains(e.target)) return;
    const unchanged = input.value.trim() === String(item.memo || '');
    if (unchanged) pfCloseMemoEditor(); else save();
  };
  // 표가 다시 그려져 anchor 가 분리되면 같은 행의 새 칸을 다시 찾는다.
  const onReflow = () => {
    let target = anchor;
    if (!target.isConnected) {
      const row = Array.from(document.querySelectorAll('#pfBody tr[data-code], #analysisPositionStrip[data-code]'))
        .find(r => r.dataset.code === stockCode);
      const candidates = row ? Array.from(row.querySelectorAll('.js-pf-memo-edit, .js-ap-memo')) : [];
      target = candidates.find(c => c.getClientRects().length) || candidates[0] || null;
      if (target) anchor = target;
    }
    if (target && target.isConnected) _pfPositionMemoPopover(el, target);
  };
  document.addEventListener('pointerdown', onDocPointer, true);
  window.addEventListener('resize', onReflow);
  window.addEventListener('scroll', onReflow, true);
  _pfMemoPopover = { el, code: stockCode, onDocPointer, onReflow };

  _pfAutoGrowMemo(input);
  _pfPositionMemoPopover(el, anchor);
  input.focus();
  input.setSelectionRange(input.value.length, input.value.length);
}

// 투자 인사이트 모달 안의 메모 섹션 — 행을 탭하면 이 모달이 열리므로
// 메모 컬럼이 없는 모바일에서도 바로 입력할 수 있다.
function pfRenderInsightMemo(stockCode) {
  const item = pfMemoItem(stockCode)
    || (typeof analysisPositionCachedItem === 'function' ? analysisPositionCachedItem(stockCode) : null);
  if (!item) return '';
  const safeCode = escapeHtml(stockCode);
  return `<section class="pf-insight-memo-panel" data-code="${safeCode}">
    <div class="pf-insight-tag-head">
      <div>
        <span class="pf-insight-section-kicker">Memo</span>
        <strong>메모</strong>
      </div>
      <span class="pf-insight-memo-status" aria-live="polite"></span>
    </div>
    <textarea class="pf-insight-memo-input js-pf-insight-memo" data-code="${safeCode}" maxlength="${PF_MEMO_MAX_LEN}" rows="2" placeholder="매수 이유, 확인할 것, 다음 행동… (입력 후 바깥을 누르면 저장)">${escapeHtml(item.memo || '')}</textarea>
  </section>`;
}

(function initPfMemoInsight() {
  // change 는 textarea 가 포커스를 잃을 때 한 번 — 모달 안 자동 저장.
  document.addEventListener('change', async (e) => {
    const input = e.target.closest?.('.js-pf-insight-memo');
    if (!input) return;
    const status = input.closest('.pf-insight-memo-panel')?.querySelector('.pf-insight-memo-status');
    if (status) status.textContent = '저장 중…';
    try {
      const code = input.dataset.code;
      const fallback = typeof analysisPositionCachedItem === 'function' ? analysisPositionCachedItem(code) : null;
      await pfSaveHoldingMemo(code, input.value, fallback);
      if (status) status.textContent = '저장됨';
    } catch (err) {
      if (status) status.textContent = '저장 실패';
      reportApiError(err, '메모 저장');
    }
  });
  document.addEventListener('keydown', (e) => {
    const input = e.target.closest?.('.js-pf-insight-memo');
    if (input && e.key === 'Enter' && (e.metaKey || e.ctrlKey)) { e.preventDefault(); input.blur(); }
  });
})();
