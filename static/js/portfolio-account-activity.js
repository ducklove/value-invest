// 편집 중인 사유는 서버의 실시간 갱신으로 덮어쓰지 않는다.
const PfActivity = { account: null, items: [], offset: 0, busy: false, generation: 0, dirty: new Set() };
const _pfActivityKinds = { dividend: '배당·분배금', interest: '이자', other_income: '기타 수입', transfer: '원금 입출금', internal: '계좌 내 자금 이동', trade: '매매·입출고', fee: '수수료·세금', review: '확인 필요' };
const _pfActivityEl = id => document.getElementById(id);
const _pfActivityMoney = value => value == null ? '확인 필요' : Number(value).toLocaleString('ko-KR', { maximumFractionDigits: 6 });

function pfActivityDialog() {
  if (_pfActivityEl('pfActivityDialog')) return _pfActivityEl('pfActivityDialog');
  const dialog = document.createElement('dialog');
  dialog.id = 'pfActivityDialog'; dialog.className = 'pf-trade-dialog pf-activity-dialog';
  dialog.setAttribute('aria-labelledby', 'pfActivityTitle');
  dialog.innerHTML = `<h2 id="pfActivityTitle">수입·입출금 내역</h2><button type="button" id="pfActivityClose">닫기</button>
    <p id="pfActivityHelp">NH에서 확인한 실제 수입과 입출금입니다. 사유·분류를 수정해도 현금을 다시 더하지 않습니다.</p>
    <p id="pfActivityPollingHelp">주문·체결은 통보 후 재조회하며, 배당·이자·입출금은 60초마다 확인합니다. 최초 가져오기의 과거 입출금은 시작 잔고에 포함된 내역으로 보존합니다.</p>
    <form id="pfActivityImport"><label>시작일 <input type="date" id="pfActivityStart" required></label><label>종료일 <input type="date" id="pfActivityEnd" required></label><button type="submit">기간 내역 가져오기</button></form>
    <p id="pfActivityStatus" role="status"></p><div id="pfActivityTotals"></div><div id="pfActivityRows"></div>
    <div class="pf-account-actions"><button type="button" id="pfActivityPrev">이전</button><button type="button" id="pfActivityNext">다음</button><button type="button" id="pfActivityReload">새로 조회</button></div>`;
  document.body.append(dialog);
  _pfActivityEl('pfActivityClose').onclick = () => dialog.close();
  _pfActivityEl('pfActivityReload').onclick = () => pfLoadActivity();
  _pfActivityEl('pfActivityPrev').onclick = () => { PfActivity.offset = Math.max(0, PfActivity.offset - 200); pfLoadActivity(); };
  _pfActivityEl('pfActivityNext').onclick = () => { PfActivity.offset += 200; pfLoadActivity(); };
  _pfActivityEl('pfActivityImport').onsubmit = pfImportActivity;
  _pfActivityEl('pfActivityRows').onsubmit = pfSaveActivity;
  _pfActivityEl('pfActivityRows').addEventListener('input', event => {
    const form = event.target.closest('[data-transaction]');
    if (form) PfActivity.dirty.add(Number(form.dataset.transaction));
  });
  _pfActivityEl('pfActivityRows').addEventListener('change', event => {
    if (event.target.name !== 'kind') return;
    const form = event.target.closest('[data-transaction]');
    form.querySelector('.pf-activity-income').hidden = !['dividend', 'interest', 'other_income'].includes(event.target.value);
  });
  dialog.addEventListener('close', () => { PfActivity.generation++; });
  return dialog;
}

async function pfOpenAccountActivity(account) {
  const dialog = pfActivityDialog();
  PfActivity.account = account; PfActivity.offset = 0;
  _pfActivityEl('pfActivityTitle').textContent = account.name + ' · 수입·입출금';
  _pfActivityEl('pfActivityHelp').textContent = account.broker === 'kis'
    ? '한국투자증권은 잔고 자동 동기화를 지원합니다. 배당·이자·입출금 거래내역 자동 수집은 아직 지원하지 않습니다. 기존에 보존한 내역이 있으면 아래에 표시합니다.'
    : 'NH에서 확인한 실제 수입과 입출금입니다. 사유·분류를 수정해도 현금을 다시 더하지 않습니다.';
  _pfActivityEl('pfActivityPollingHelp').hidden = account.broker === 'kis';
  const today = new Date().toLocaleDateString('sv-SE', { timeZone: 'Asia/Seoul' });
  _pfActivityEl('pfActivityStart').value = today.slice(0, 4) + '-01-01';
  _pfActivityEl('pfActivityEnd').value = today;
  _pfActivityEl('pfActivityImport').hidden = account.broker !== 'namuh' || account.connection?.environment === 'mock';
  dialog.showModal();
  await pfLoadActivity();
}

function pfActivityBase() { return `/api/portfolio/accounts/${encodeURIComponent(PfActivity.account.account_id)}/activity`; }

async function pfLoadActivity() {
  if (PfActivity.busy || !PfActivity.account) return;
  const generation = ++PfActivity.generation;
  _pfActivityEl('pfActivityStatus').textContent = '거래내역을 불러오는 중입니다…';
  try {
    const data = await apiFetchJson(`${pfActivityBase()}?offset=${PfActivity.offset}`);
    if (generation !== PfActivity.generation) return;
    PfActivity.items = data.items;
    PfActivity.dirty.clear();
    _pfActivityEl('pfActivityTotals').innerHTML = '<h3>가져온 기간의 수입 합계</h3>' + (data.totals.map(row => `<span>${escapeHtml(_pfActivityKinds[row.kind])} ${_pfActivityMoney(row.amount)} ${escapeHtml(row.currency)}</span>`).join(' · ') || '<p>수입 내역이 없습니다.</p>');
    _pfActivityEl('pfActivityRows').innerHTML = data.items.map(row => `<form class="pf-activity-row" data-transaction="${Number(row.id)}">
      <h3>${escapeHtml(row.date)} · ${escapeHtml(row.description || '거래내역')} · ${_pfActivityMoney(row.net_amount)} ${escapeHtml(row.currency)}</h3>
      <p>${escapeHtml(row.stock_name || row.stock_code || '')}${row.income_amount != null ? ` · 수입 ${_pfActivityMoney(row.income_amount)} ${escapeHtml(row.currency)}` : ''}${row.gross_amount != null ? ` · 세전 ${_pfActivityMoney(row.gross_amount)}, 세금 ${_pfActivityMoney(row.tax_amount)}, 수수료 ${_pfActivityMoney(row.fee_amount)}` : ''}</p>
      <label>분류 <select name="kind">${Object.entries(_pfActivityKinds).map(([value, label]) => `<option value="${value}" ${row.kind === value ? 'selected' : ''}>${label}</option>`).join('')}</select></label>
      <label class="pf-activity-income" ${['dividend', 'interest', 'other_income'].includes(row.kind) ? '' : 'hidden'}>세후 수입액 · 원금 제외 (${escapeHtml(row.currency)}) <input name="income_amount" type="number" step="any" value="${row.income_amount == null ? '' : Number(row.income_amount)}" placeholder="수입 분류 시 입력 · 취소는 음수"></label>
      <label>사유 <input name="reason" maxlength="500" value="${escapeHtml(row.reason)}" placeholder="예: 생활비 출금, 투자금 추가, 계좌 간 이체"></label>
      ${row.currency !== 'KRW' ? `<label>1 ${escapeHtml(row.currency)}당 원화 환율 <input name="fx_rate" type="number" min="0.000001" max="10000000" step="any" value="${row.fx_rate == null ? '' : Number(row.fx_rate)}" placeholder="원화 합산에 사용할 실제 환율"></label><small>환율 확인 전에는 원통화로 집계하며, 원화 수익 분해·입출금 계산에 포함하지 않습니다.</small>` : ''}
      ${row.baseline && row.kind === 'transfer' ? '<small>시작 잔고에 포함된 과거 입출금</small>' : ''}
      <button type="submit">사유·분류 저장</button></form>`).join('') || '<p>가져온 거래내역이 없습니다.</p>';
    _pfActivityEl('pfActivityPrev').disabled = PfActivity.offset === 0;
    _pfActivityEl('pfActivityNext').disabled = !data.has_more;
    _pfActivityEl('pfActivityStatus').textContent = data.state?.error || (data.state?.last_import_at ? `최근 확인 ${data.state.last_import_at.replace('T', ' ').slice(0, 19)} · 사유를 편집 중이면 새로 조회하기 전까지 입력을 유지합니다.` : '아직 거래내역을 가져오지 않았습니다.');
  } catch (error) { if (generation === PfActivity.generation) _pfActivityEl('pfActivityStatus').textContent = error.message; }
}

async function pfSaveActivity(event) {
  event.preventDefault();
  if (PfActivity.busy) return;
  const form = event.target.closest('[data-transaction]');
  if (!form) return;
  const row = PfActivity.items.find(item => item.id === Number(form.dataset.transaction));
  const values = new FormData(form);
  const payload = { revision: row.revision, kind: values.get('kind'), reason: values.get('reason') };
  if (values.get('fx_rate')) payload.fx_rate = Number(values.get('fx_rate'));
  if (values.get('income_amount')) payload.income_amount = Number(values.get('income_amount'));
  const button = form.querySelector('button');
  PfActivity.busy = button.disabled = true;
  const generation = PfActivity.generation;
  try {
    await apiFetchJson(`${pfActivityBase()}/${row.id}`, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
    if (generation !== PfActivity.generation) return;
    row.revision++; row.reason = payload.reason; row.kind = payload.kind;
    PfActivity.dirty.delete(row.id);
    _pfActivityEl('pfActivityStatus').textContent = '사유·분류를 저장했습니다. 다른 행의 입력은 유지했습니다. 합계는 새로 조회하면 갱신됩니다.';
  } catch (error) { if (generation === PfActivity.generation) _pfActivityEl('pfActivityStatus').textContent = error.message; }
  finally { PfActivity.busy = false; button.disabled = false; }
}

async function pfImportActivity(event) {
  event.preventDefault();
  if (PfActivity.busy) return;
  const start = _pfActivityEl('pfActivityStart').value, end = _pfActivityEl('pfActivityEnd').value;
  const button = event.target.querySelector('button');
  PfActivity.busy = button.disabled = true;
  const generation = PfActivity.generation;
  _pfActivityEl('pfActivityStatus').textContent = '거래내역과 잔고를 확인하고 있습니다…';
  let ok = false;
  try {
    await apiFetchJson(`${pfActivityBase()}/sync?start=${encodeURIComponent(start)}&end=${encodeURIComponent(end)}`, { method: 'POST', timeoutMs: 180000 });
    ok = generation === PfActivity.generation;
  } catch (error) { if (generation === PfActivity.generation) _pfActivityEl('pfActivityStatus').textContent = error.message; }
  finally { PfActivity.busy = false; button.disabled = false; }
  if (ok) { await pfLoadActivity(); await pfLoadAccounts(true); await loadPortfolio({ force: true }); }
}

function pfResetActivity() {
  PfActivity.generation++; PfActivity.account = null; PfActivity.items = []; PfActivity.dirty.clear();
  _pfActivityEl('pfActivityDialog')?.remove();
}

function pfActivityAccountsChanged(accounts) {
  if (!_pfActivityEl('pfActivityDialog')?.open || !accounts.some(row => row.account_id === PfActivity.account?.account_id)) return;
  if (PfActivity.busy) return;
  if (PfActivity.dirty.size) {
    _pfActivityEl('pfActivityStatus').textContent = '새 계좌 내역이 있습니다. 편집 중인 사유를 저장한 뒤 새로 조회해 주세요.';
    return;
  }
  pfLoadActivity();
}
