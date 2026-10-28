// 예수금 관측 구간의 사유. 사용자가 입력하는 동안 서버 갱신으로 DOM을 교체하지 않는다.
const PfCashReviews = { items: [], categories: [], generation: 0, loading: false, saving: false, autoShown: false, autoOpened: false, pending: true, offset: 0, dirty: false };
const _pfCashReviewEl = id => document.getElementById(id);
const _pfCashReviewMoney = value => Number(value).toLocaleString('ko-KR', { maximumFractionDigits: 6 });

function pfCashReviewDialog() {
  if (_pfCashReviewEl('pfCashReviewDialog')) return _pfCashReviewEl('pfCashReviewDialog');
  const dialog = document.createElement('dialog');
  dialog.id = 'pfCashReviewDialog';
  dialog.className = 'pf-trade-dialog pf-cash-review-dialog';
  dialog.setAttribute('aria-labelledby', 'pfCashReviewTitle');
  dialog.innerHTML = `<h2 id="pfCashReviewTitle">예수금 변동 사유</h2>
    <p>자동 연동 계좌에서 아직 설명되지 않은 예수금 변동입니다. 사유를 선택해 기록해 주세요.</p>
    <p class="pf-cash-review-help">확인된 매매·수입과 소액 변동은 자동 처리합니다. 최근 변동은 거래내역과 대조한 뒤 사유를 요청합니다. 소액 기준은 하루 합계 1,000원이며 예수금 규모에 따라 최대 10,000원까지 적용합니다. 조회 사이의 순변동이므로 여러 거래가 합쳐질 수 있습니다.</p>
    <div class="pf-account-actions"><button type="button" id="pfCashReviewMode">전체 이력 보기</button><button type="button" id="pfCashReviewReload">새로 조회</button><button type="button" id="pfCashReviewClose">나중에 입력</button></div>
    <p id="pfCashReviewStatus" role="status" aria-live="polite"></p><div id="pfCashReviewRows"></div>
    <div class="pf-account-actions"><button type="button" id="pfCashReviewPrev">이전</button><button type="button" id="pfCashReviewNext">다음</button></div>`;
  document.body.append(dialog);
  _pfCashReviewEl('pfCashReviewClose').onclick = () => { if (!PfCashReviews.saving) dialog.close(); };
  _pfCashReviewEl('pfCashReviewReload').onclick = () => pfLoadCashReviews({ render: true, replaceDraft: true });
  _pfCashReviewEl('pfCashReviewMode').onclick = () => {
    if (PfCashReviews.loading || PfCashReviews.saving || !pfCashReviewCanReplace()) return;
    PfCashReviews.dirty = false;
    PfCashReviews.autoOpened = false;
    PfCashReviews.pending = !PfCashReviews.pending;
    PfCashReviews.offset = 0;
    pfLoadCashReviews({ render: true });
  };
  _pfCashReviewEl('pfCashReviewPrev').onclick = () => pfCashReviewPage(-100);
  _pfCashReviewEl('pfCashReviewNext').onclick = () => pfCashReviewPage(100);
  _pfCashReviewEl('pfCashReviewRows').onsubmit = pfSaveCashReview;
  _pfCashReviewEl('pfCashReviewRows').addEventListener('input', () => { PfCashReviews.dirty = true; PfCashReviews.autoOpened = false; });
  _pfCashReviewEl('pfCashReviewRows').addEventListener('change', event => {
    PfCashReviews.dirty = true;
    PfCashReviews.autoOpened = false;
    const form = event.target.closest('form');
    if (form) form.elements.reason.required = ['other', 'transfer'].includes(form.elements.category.value);
  });
  dialog.addEventListener('cancel', event => { if (PfCashReviews.saving) event.preventDefault(); });
  dialog.addEventListener('close', () => { PfCashReviews.generation++; PfCashReviews.loading = false; PfCashReviews.autoOpened = false; });
  return dialog;
}

function pfCashReviewCanReplace() {
  return !PfCashReviews.dirty || confirm('저장하지 않은 사유가 있습니다. 입력을 버리고 새로 조회할까요?');
}

function pfCashReviewNotice(total) {
  let notice = _pfCashReviewEl('pfCashReviewNotice');
  if (!notice && total) {
    notice = document.createElement('button');
    notice.type = 'button'; notice.id = 'pfCashReviewNotice'; notice.className = 'pf-cash-review-notice';
    notice.onclick = () => pfOpenCashReviews();
    document.body.append(notice);
  }
  if (notice) { notice.hidden = !total; notice.textContent = `예수금 변동 사유 ${total}건 입력`; }
}

function pfRenderCashReviews(data) {
  PfCashReviews.items = data.items; PfCashReviews.categories = data.categories;
  PfCashReviews.dirty = false;
  _pfCashReviewEl('pfCashReviewMode').textContent = PfCashReviews.pending ? '전체 이력 보기' : '미입력만 보기';
  _pfCashReviewEl('pfCashReviewRows').innerHTML = data.items.map(row => {
    const amount = Number(row.unexplained);
    const automatic = amount === 0;
    const minor = row.auto_resolution === 'minor';
    const checking = row.auto_resolution === 'checking';
    const label = data.categories.find(item => item.id === row.category)?.label;
    const options = data.categories.filter(item => !item.direction || item.direction * amount > 0 || item.id === row.category);
    return `<form class="pf-cash-review-row" data-review="${Number(row.id)}">
      <h3>${escapeHtml(row.account_name)} · ${automatic ? '자동 확인' : checking ? '자동 대조 중' : minor ? '소액 자동처리' : amount > 0 ? '입금' : '출금'} ${_pfCashReviewMoney(Math.abs(amount))} ${escapeHtml(row.currency)}</h3>
      <p>${escapeHtml(row.from_at.replace('T', ' ').slice(0, 19))} ~ ${escapeHtml(row.to_at.replace('T', ' ').slice(0, 19))} (한국시간)</p>
      <p>예수금 순변동 ${_pfCashReviewMoney(row.amount)} ${escapeHtml(row.currency)}${Number(row.amount) !== amount ? ` · 확인된 금액 제외 후 ${_pfCashReviewMoney(amount)} ${escapeHtml(row.currency)}` : ''}</p>
      ${row.positions_changed ? '<p>보유 잔고도 변경됐습니다. 매매 결제·입출고·CMA 이동 여부를 함께 확인해 주세요.</p>' : ''}
      ${minor ? `<p>사유 입력이 필요하지 않습니다. 같은 날 미확인 변동의 절대금액 합계가 소액 기준(${_pfCashReviewMoney(row.minor_limit)} ${escapeHtml(row.currency)}) 이하입니다. 이자로 확정한 기록은 아닙니다.</p><details><summary>직접 사유 기록 (선택)</summary>` : ''}
      ${checking ? '<p>최근 변동을 거래내역과 대조하고 있습니다. 지금은 사유를 입력하지 않아도 됩니다.</p><details><summary>직접 사유 기록 (선택)</summary>' : ''}
      ${automatic ? '<p>수집된 거래내역으로 설명된 변동입니다.</p>' : `<fieldset><label>사유 선택 <select name="category" required><option value="">선택해 주세요</option>${options.map(item => `<option value="${escapeHtml(item.id)}" ${row.category === item.id ? 'selected' : ''}>${escapeHtml(item.label)}</option>`).join('')}</select></label>
      <label>상세 사유 <textarea name="reason" maxlength="500" rows="2" ${['other', 'transfer'].includes(row.category) ? 'required' : ''} placeholder="여러 사유가 섞였다면 금액과 내용을 함께 적어 주세요">${escapeHtml(row.reason || '')}</textarea></label>
      <button type="submit">${label ? '사유 수정' : '사유 저장'}</button></fieldset>`}${minor || checking ? '</details>' : ''}</form>`;
  }).join('') || `<p>${PfCashReviews.pending ? '사유를 입력할 예수금 변동이 없습니다.' : '기록된 예수금 변동이 없습니다.'}</p>`;
  _pfCashReviewEl('pfCashReviewPrev').disabled = !PfCashReviews.offset;
  _pfCashReviewEl('pfCashReviewNext').disabled = !data.has_more;
  _pfCashReviewEl('pfCashReviewStatus').textContent = `${PfCashReviews.pending ? '미입력' : '전체'} ${data.total}건`;
}

async function pfLoadCashReviews({ render = false, auto = false, replaceDraft = false } = {}) {
  if (PfCashReviews.loading || PfCashReviews.saving) return;
  if (render && replaceDraft && !pfCashReviewCanReplace()) return;
  if (render && replaceDraft) PfCashReviews.dirty = false;
  if (render && PfCashReviews.dirty && !replaceDraft) return;
  const generation = PfCashReviews.generation;
  const pending = render ? PfCashReviews.pending : true;
  const offset = render ? PfCashReviews.offset : 0;
  PfCashReviews.loading = true;
  try {
    const data = await apiFetchJson(`/api/portfolio/cash-reviews?pending=${pending}&offset=${offset}`, { errorMessage: '예수금 변동을 불러오지 못했습니다.' });
    if (generation !== PfCashReviews.generation) return;
    if (pending) pfCashReviewNotice(data.total);
    if (auto && data.total && !PfCashReviews.autoShown && !document.querySelector('dialog[open]')) {
      PfCashReviews.autoShown = true; PfCashReviews.pending = true; PfCashReviews.offset = 0;
      PfCashReviews.autoOpened = true;
      pfCashReviewDialog().showModal();
    }
    if (pending && !data.total && PfCashReviews.autoOpened && !PfCashReviews.dirty && !PfCashReviews.saving
        && _pfCashReviewEl('pfCashReviewDialog')?.open) {
      _pfCashReviewEl('pfCashReviewDialog').close();
      return;
    }
    if (_pfCashReviewEl('pfCashReviewDialog')?.open && !PfCashReviews.dirty && !PfCashReviews.saving
        && pending === PfCashReviews.pending && offset === PfCashReviews.offset) pfRenderCashReviews(data);
  } catch (error) {
    if (generation === PfCashReviews.generation && _pfCashReviewEl('pfCashReviewDialog')?.open) _pfCashReviewEl('pfCashReviewStatus').textContent = error.message;
  } finally { if (generation === PfCashReviews.generation) PfCashReviews.loading = false; }
}

async function pfOpenCashReviews() {
  const dialog = pfCashReviewDialog();
  PfCashReviews.autoShown = true;
  PfCashReviews.autoOpened = false;
  if (!dialog.open) {
    PfCashReviews.pending = true; PfCashReviews.offset = 0; PfCashReviews.dirty = false;
    dialog.showModal();
  }
  await pfLoadCashReviews({ render: true });
}

function pfCheckCashReviews() {
  if (typeof currentUser !== 'undefined' && !currentUser) return;
  if (_pfCashReviewEl('pfCashReviewDialog')?.open) {
    if (!PfCashReviews.dirty) pfLoadCashReviews({ render: true });
  } else pfLoadCashReviews({ auto: true });
}

function pfCashReviewPage(delta) {
  if (PfCashReviews.loading || PfCashReviews.saving || !pfCashReviewCanReplace()) return;
  PfCashReviews.dirty = false;
  PfCashReviews.offset = Math.max(0, PfCashReviews.offset + delta);
  pfLoadCashReviews({ render: true });
}

async function pfSaveCashReview(event) {
  event.preventDefault();
  if (PfCashReviews.saving) return;
  const form = event.target.closest('[data-review]');
  if (!form || !form.reportValidity()) return;
  const row = PfCashReviews.items.find(item => item.id === Number(form.dataset.review));
  if (!row) return;
  const payload = { revision: row.revision, category: form.elements.category.value, reason: form.elements.reason.value.trim() };
  // 저장 직전 시작한 조회 응답이 저장한 사유를 예전 값으로 되돌리지 않게 한다.
  const generation = ++PfCashReviews.generation;
  PfCashReviews.loading = false;
  PfCashReviews.saving = true;
  form.querySelector('fieldset').disabled = true;
  _pfCashReviewEl('pfCashReviewClose').disabled = true;
  try {
    await apiFetchJson(`/api/portfolio/cash-reviews/${row.id}`, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
    if (generation !== PfCashReviews.generation) return;
    row.revision++; row.category = payload.category; row.reason = payload.reason;
    form.querySelector('button').textContent = '사유 수정';
    // 다른 행의 초안은 유지한다. 목록 교체 없이 저장한 행만 갱신한다.
    PfCashReviews.dirty = [..._pfCashReviewEl('pfCashReviewRows').querySelectorAll('form[data-review]')].some(element => {
      const item = PfCashReviews.items.find(value => value.id === Number(element.dataset.review));
      return element.elements.category && (element.elements.category.value !== item.category || element.elements.reason.value.trim() !== item.reason);
    });
    _pfCashReviewEl('pfCashReviewStatus').textContent = '사유를 저장했습니다. 다른 항목의 입력은 유지했습니다.';
  } catch (error) {
    if (generation === PfCashReviews.generation) _pfCashReviewEl('pfCashReviewStatus').textContent = error.message;
  } finally {
    if (generation === PfCashReviews.generation) {
      PfCashReviews.saving = false; form.querySelector('fieldset').disabled = false;
      _pfCashReviewEl('pfCashReviewClose').disabled = false;
    }
  }
  if (generation === PfCashReviews.generation) await pfLoadCashReviews();
}

function pfResetCashReviews() {
  PfCashReviews.generation++; PfCashReviews.items = []; PfCashReviews.categories = [];
  PfCashReviews.loading = PfCashReviews.saving = PfCashReviews.autoShown = PfCashReviews.autoOpened = PfCashReviews.dirty = false;
  PfCashReviews.pending = true; PfCashReviews.offset = 0;
  _pfCashReviewEl('pfCashReviewDialog')?.remove(); _pfCashReviewEl('pfCashReviewNotice')?.remove();
}

document.addEventListener('DOMContentLoaded', () => {
  _pfCashReviewEl('pfCashReviewsOpen')?.addEventListener('click', () => pfOpenCashReviews());
});
