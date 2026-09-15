// 계좌 선택·잔고 관리·NH 조회 연동. 자격증명은 브라우저 저장소에 보관하지 않는다.
const PfAccounts = { rows: [], loaded: false, busy: false, focusCode: null, nhAccount: null, previewed: false, socket: null, retry: null };
const _pfAccountEl = id => document.getElementById(id);

async function pfLoadAccounts(force = false) {
  if (PfAccounts.loaded && !force) return PfAccounts.rows;
  const generation = PfAccounts.generation || 0;
  const rows = await apiFetchJson('/api/portfolio/accounts', { errorMessage: '계좌 목록을 불러오지 못했습니다.' });
  if ((PfAccounts.generation || 0) !== generation) return [];
  PfAccounts.rows = Array.isArray(rows) ? rows : [];
  PfAccounts.loaded = true;
  if (PfStore.accountId && !rows.some(row => row.account_id === PfStore.accountId)) PfStore.accountId = '';
  const select = _pfAccountEl('pfAccountSelect');
  if (select) {
    select.innerHTML = '<option value="">전체 계좌 · 합산</option>' + rows.map(row => `<option value="${escapeHtml(row.account_id)}">${escapeHtml(row.name)}${row.broker ? ' · NH 연동' : ''}</option>`).join('');
    select.value = PfStore.accountId || '';
  }
  const account = rows.find(row => row.account_id === PfStore.accountId);
  const state = _pfAccountEl('pfAccountState');
  if (state) state.textContent = account?.broker ? `NH 조회 연동 · ${account.connection?.last_sync_at ? '최근 동기화 ' + new Date(account.connection.last_sync_at).toLocaleString('ko-KR') : '첫 동기화 대기'}${account.connection?.sync_error ? ' · 동기화 확인 필요' : ''}` : account ? '이 계좌의 종목과 현금을 표시합니다.' : '모든 계좌의 동일 종목과 현금을 합산합니다.';
  if (state && account) state.textContent += ' NAV·기간 실적은 전체 계좌에서 확인하세요.';
  pfConnectNamuhQuotes();
  return rows;
}

async function pfSelectAccount(id) {
  if (PfStore.loading || PfStore.edit.savingCode) { _pfAccountEl('pfAccountSelect').value = PfStore.accountId || ''; return; }
  PfStore.accountId = id || '';
  PfStore.edit.code = null;
  PfStore.items = [];
  PfStore.manualOrder.pendingCodes = null;
  _pfAccountEl('pfAccountSelect').value = PfStore.accountId;
  await pfLoadAccounts(true);
  await loadPortfolio({ force: true });
}

function pfResetAccounts() {
  PfAccounts.generation = (PfAccounts.generation || 0) + 1;
  PfAccounts.rows = [];
  PfAccounts.loaded = false;
  PfAccounts.focusCode = PfAccounts.nhAccount = null;
  PfAccounts.previewed = false;
  PfStore.accountId = '';
  PfStore.items = [];
  try { localStorage.removeItem('valueInvestPortfolioSnapshot:v2'); } catch (_) { /* 저장소 사용 불가 */ }
  pfConnectNamuhQuotes();
  for (const id of ['pfNhDialog', 'pfAccountsDialog']) {
    const dialog = _pfAccountEl(id);
    if (dialog?.open) dialog.close();
  }
  for (const id of ['pfAccountsList', 'pfNhChoices', 'pfNhPreview', 'pfAccountState', 'pfNhQuoteState', 'pfBody', 'pfFoot', 'pfSummary']) {
    const element = _pfAccountEl(id);
    if (element) element.textContent = '';
  }
  const table = _pfAccountEl('pfTable');
  if (table) table.style.display = 'none';
}

function pfAccountNeedsSelection() {
  const current = PfAccounts.rows.find(row => row.account_id === PfStore.accountId);
  return current?.broker || (!PfStore.accountId && (PfAccounts.rows.length > 1 || PfAccounts.rows.some(row => row.broker)));
}

async function pfOpenAccountManager(code = null) {
  PfAccounts.focusCode = code;
  const dialog = _pfAccountEl('pfAccountsDialog');
  if (!dialog.open) dialog.showModal();
  _pfAccountEl('pfAccountsStatus').textContent = '';
  try { await pfLoadAccounts(true); pfRenderAccounts(); }
  catch (error) { _pfAccountEl('pfAccountsStatus').textContent = error.message; }
}

function pfRenderAccounts() {
  const item = PfStore.items.find(row => row.stock_code === PfAccounts.focusCode);
  _pfAccountEl('pfAccountsHelp').textContent = item ? `${item.stock_name} · 계좌를 선택하면 해당 계좌의 잔고를 관리할 수 있습니다.` : '계좌별로 잔고를 등록하고, 전체 계좌 보기에서 합산합니다. NH 연동은 조회 전용입니다.';
  _pfAccountEl('pfAccountsList').innerHTML = PfAccounts.rows.map(row => {
    const position = item?.account_positions?.find(p => p.account_id === row.account_id);
    const detail = position ? `${Number(position.quantity).toLocaleString('ko-KR')}주 · 평균 ${Number(position.avg_price).toLocaleString('ko-KR')} ${position.avg_price_currency}` : `잔고 ${row.holdings_count || 0}개`;
    const error = row.connection?.sync_error;
    return `<section class="pf-account-card" data-account="${escapeHtml(row.account_id)}"><h3>${escapeHtml(row.name)}</h3>
      <p>${escapeHtml(detail)}${row.broker ? ' · NH ' + escapeHtml(row.connection.account_mask) + (row.connection.environment === 'mock' ? ' · 모의계좌' : '') : ' · 수동 관리'}</p>
      ${error ? `<p class="pf-account-error">${escapeHtml(error)}</p>` : ''}
      <div class="pf-account-actions"><button type="button" data-account-action="view">이 계좌 보기</button><button type="button" data-account-action="rename">이름 수정</button>
      ${row.broker ? '<button type="button" data-account-action="sync">잔고 동기화</button><button type="button" data-account-action="disconnect">연결 해제</button>' : '<button type="button" data-account-action="connect">NH 계좌 연동</button>'}
      <button type="button" data-account-action="delete" ${row.holdings_count || row.broker || row.account_id.startsWith('default-') ? 'disabled' : ''}>계좌 삭제</button></div>
      ${row.broker ? '<small>종목과 현금은 증권사 잔고로 갱신됩니다. 연결 해제 시 현재 잔고를 수동 계좌로 보존합니다.</small>' : ''}</section>`;
  }).join('');
}

async function pfCreateAccount(event) {
  event.preventDefault();
  if (PfAccounts.busy) return;
  PfAccounts.busy = true;
  _pfAccountEl('pfAccountCreate').disabled = true;
  try {
    await apiFetchJson('/api/portfolio/accounts', { method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: _pfAccountEl('pfAccountName').value.trim(), type: _pfAccountEl('pfAccountType').value }) });
    _pfAccountEl('pfAccountName').value = '';
    await pfLoadAccounts(true); pfRenderAccounts();
    _pfAccountEl('pfAccountsStatus').textContent = '계좌를 만들었습니다. 이 계좌 보기에서 종목을 등록하거나 NH 계좌를 연결하세요.';
  } catch (error) { _pfAccountEl('pfAccountsStatus').textContent = error.message; }
  finally { PfAccounts.busy = false; _pfAccountEl('pfAccountCreate').disabled = false; }
}

async function pfAccountAction(event) {
  const button = event.target.closest('[data-account-action]');
  if (!button || PfAccounts.busy) return;
  const aid = button.closest('[data-account]').dataset.account;
  const row = PfAccounts.rows.find(account => account.account_id === aid);
  const action = button.dataset.accountAction;
  if (action === 'view') { _pfAccountEl('pfAccountsDialog').close(); await pfSelectAccount(aid); return; }
  if (action === 'connect') { pfOpenNhConnection(row); return; }
  let options = { method: 'DELETE' }, path = `/api/portfolio/accounts/${encodeURIComponent(aid)}`;
  if (action === 'rename') {
    const name = prompt('계좌 이름', row.name);
    if (!name?.trim() || name === row.name) return;
    options = { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name: name.trim() }) };
  } else if (action === 'sync') { path += '/namuh/sync'; options = { method: 'POST', timeoutMs: 120000 }; }
  else if (action === 'disconnect') {
    if (!confirm('NH 연결을 해제하고 현재 잔고를 수동 계좌로 보존할까요?')) return;
    path += '/namuh';
  } else if (!confirm('비어 있는 계좌를 삭제할까요?')) return;
  PfAccounts.busy = true; button.disabled = true;
  _pfAccountEl('pfAccountsStatus').textContent = '처리 중입니다…';
  try {
    await apiFetchJson(path, options);
    await pfLoadAccounts(true); pfRenderAccounts();
    await loadPortfolio({ force: true });
    _pfAccountEl('pfAccountsStatus').textContent = '반영했습니다.';
  } catch (error) { _pfAccountEl('pfAccountsStatus').textContent = error.message; }
  finally { PfAccounts.busy = false; button.disabled = false; }
}

function pfOpenNhConnection(account) {
  PfAccounts.nhAccount = account.account_id;
  PfAccounts.previewed = false;
  _pfAccountEl('pfNhForm').reset();
  _pfAccountEl('pfNhTitle').textContent = `${account.name} · NH 계좌 연동`;
  _pfAccountEl('pfNhChoices').innerHTML = '<option value="">앱키 확인 후 계좌를 선택하세요</option>';
  _pfAccountEl('pfNhPreview').textContent = '';
  _pfAccountEl('pfNhStatus').textContent = '';
  _pfAccountEl('pfNhSave').disabled = true;
  _pfAccountEl('pfNhPreviewButton').disabled = true;
  _pfAccountEl('pfNhDialog').showModal();
}

async function pfNhWork(action) {
  if (PfAccounts.busy) return;
  PfAccounts.busy = true;
  const fields = _pfAccountEl('pfNhFields'); fields.disabled = true;
  _pfAccountEl('pfNhClose').disabled = true;
  _pfAccountEl('pfNhStatus').textContent = '나무에서 조회하고 있습니다…';
  try {
    if (action === 'verify') {
      const data = await apiFetchJson('/api/portfolio/namuh/credentials', { method: 'POST', timeoutMs: 60000,
        headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ app_key: _pfAccountEl('pfNhKey').value.trim(), app_secret: _pfAccountEl('pfNhSecret').value.trim() }) });
      _pfAccountEl('pfNhKey').value = _pfAccountEl('pfNhSecret').value = '';
      _pfAccountEl('pfNhChoices').innerHTML = data.accounts.map(row => `<option value="${escapeHtml(row.selection)}">${escapeHtml(row.account_mask)} · ${row.environment === 'mock' ? '모의계좌' : '실계좌'}</option>`).join('');
      _pfAccountEl('pfNhPreviewButton').disabled = !data.accounts.length;
      _pfAccountEl('pfNhStatus').textContent = data.accounts.length ? '키를 확인했습니다. 연결할 계좌와 조회 범위를 선택해 주세요.' : '이 키에서 연결 가능한 계좌가 없습니다.';
      PfAccounts.previewed = false; _pfAccountEl('pfNhSave').disabled = true;
    } else {
      const payload = { selection: _pfAccountEl('pfNhChoices').value, include_overseas: _pfAccountEl('pfNhOverseas').checked };
      const base = `/api/portfolio/accounts/${encodeURIComponent(PfAccounts.nhAccount)}/namuh`;
      if (!payload.selection) throw new Error('연결할 계좌를 선택하세요.');
      if (action === 'save' && !PfAccounts.previewed) throw new Error('잔고 미리보기를 먼저 확인하세요.');
      const data = await apiFetchJson(base + (action === 'preview' ? '/preview' : ''), { method: 'POST', timeoutMs: 120000,
        headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
      if (action === 'preview') {
        _pfAccountEl('pfNhPreview').innerHTML = `<p>${data.items.length}개 잔고 · 현금 추가 차감 없이 초기 잔고로 가져옵니다.</p><table><thead><tr><th>종목</th><th>수량·잔액</th><th>통화</th></tr></thead><tbody>${data.items.map(item => `<tr><td>${escapeHtml(item.stock_name)}</td><td>${Number(item.quantity).toLocaleString('ko-KR')}</td><td>${escapeHtml(item.currency)}</td></tr>`).join('')}</tbody></table><p>원화는 D+2 예수금, 외화는 결제 후 예수금 기준입니다.</p>${data.items.some(item => item.stock_code === 'CMA_RP_KRW') ? '<p>CMA 원화RP는 현금과 구분하며, 수량·잔액에 증권사 조회 시점의 평가액(원)을 표시합니다.</p>' : ''}`;
        PfAccounts.previewed = true; _pfAccountEl('pfNhSave').disabled = false;
        _pfAccountEl('pfNhStatus').textContent = '미리보기를 확인한 뒤 잔고 가져오기를 누르세요.';
      } else {
        _pfAccountEl('pfNhDialog').close();
        await pfLoadAccounts(true); pfRenderAccounts(); await loadPortfolio({ force: true });
        _pfAccountEl('pfAccountsStatus').textContent = 'NH 계좌를 연결했습니다. 잔고는 5분마다 자동으로 확인합니다.';
      }
    }
  } catch (error) {
    _pfAccountEl('pfNhStatus').textContent = error.message;
    if (action === 'save') { await pfLoadAccounts(true).catch(() => {}); pfRenderAccounts(); }
  } finally { PfAccounts.busy = false; fields.disabled = false; _pfAccountEl('pfNhClose').disabled = false; }
}

function pfConnectNamuhQuotes() {
  const linked = PfAccounts.rows.some(row => row.broker === 'namuh');
  if (!linked) {
    clearTimeout(PfAccounts.retry); PfAccounts.retry = null;
    if (PfAccounts.socket) { PfAccounts.socket.onclose = null; PfAccounts.socket.close(); PfAccounts.socket = null; }
    return;
  }
  if (PfAccounts.socket) return;
  const socket = new WebSocket(`${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/ws/namuh`);
  PfAccounts.socket = socket;
  socket.onmessage = event => {
    try {
      const message = JSON.parse(event.data);
      if (message.type === 'quote') QuoteManager.onQuote?.(message.code, message);
      if (message.type === 'namuh_status') {
        const label = _pfAccountEl('pfNhQuoteState');
        if (label) label.textContent = message.state === 'live' ? 'NH 실시간 시세 수신' : message.state === 'subscribed' ? 'NH 시세 구독 · 체결 대기' : 'NH 시세 연결 확인 중';
      }
    } catch (_) { /* 잘못된 개별 메시지는 다음 메시지에 영향을 주지 않는다. */ }
  };
  socket.onclose = () => { PfAccounts.socket = null; PfAccounts.retry = setTimeout(pfConnectNamuhQuotes, 10000); };
}

document.addEventListener('DOMContentLoaded', () => {
  _pfAccountEl('pfAccountSelect')?.addEventListener('change', event => pfSelectAccount(event.target.value));
  _pfAccountEl('pfAccountsOpen')?.addEventListener('click', () => pfOpenAccountManager());
  _pfAccountEl('pfAccountsClose')?.addEventListener('click', () => _pfAccountEl('pfAccountsDialog').close());
  _pfAccountEl('pfAccountForm')?.addEventListener('submit', pfCreateAccount);
  _pfAccountEl('pfAccountsList')?.addEventListener('click', pfAccountAction);
  _pfAccountEl('pfNhClose')?.addEventListener('click', () => _pfAccountEl('pfNhDialog').close());
  _pfAccountEl('pfNhForm')?.addEventListener('submit', event => { event.preventDefault(); pfNhWork('verify'); });
  _pfAccountEl('pfNhPreviewButton')?.addEventListener('click', () => pfNhWork('preview'));
  _pfAccountEl('pfNhSave')?.addEventListener('click', () => pfNhWork('save'));
  for (const id of ['pfNhChoices', 'pfNhOverseas']) _pfAccountEl(id)?.addEventListener('change', () => { PfAccounts.previewed = false; _pfAccountEl('pfNhSave').disabled = true; _pfAccountEl('pfNhPreview').textContent = ''; });
  _pfAccountEl('pfNhDialog')?.addEventListener('cancel', event => { if (PfAccounts.busy) event.preventDefault(); });
  _pfAccountEl('pfNhDialog')?.addEventListener('close', () => { _pfAccountEl('pfNhKey').value = _pfAccountEl('pfNhSecret').value = ''; });
  document.addEventListener('click', event => {
    if (event.target.closest('.js-pf-account-detail')) { event.preventDefault(); pfOpenAccountManager(event.target.closest('tr[data-code]')?.dataset.code); return; }
    if (event.target.closest('.js-pf-trade,.js-pf-edit,.js-pf-delete,.js-pf-dividend-receipt,.js-pf-distribution,.js-pf-cashflow,#pfAddToggle') && pfAccountNeedsSelection()) {
      event.preventDefault(); event.stopImmediatePropagation();
      pfOpenAccountManager(event.target.closest('tr[data-code]')?.dataset.code);
    }
  }, true);
});
