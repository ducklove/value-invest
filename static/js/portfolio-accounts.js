// 계좌 선택·잔고 관리·증권사 조회 연동. 자격증명은 브라우저 저장소에 보관하지 않는다.
const PfAccounts = { rows: [], loaded: false, busy: false, focusCode: null, nhAccount: null, previewed: false, socket: null, retry: null, watchdog: null, lastMessageAt: 0 };
const _pfAccountEl = id => document.getElementById(id);
const pfBrokerDefinition = provider => PfAccounts.catalog?.find(item => item.id === provider);
const pfBrokerName = provider => pfBrokerDefinition(provider)?.name || provider || '증권사';

async function pfLoadBrokerCatalog() {
  if (PfAccounts.catalog) return;
  const catalog = await apiFetchJson('/api/portfolio/brokers', {errorMessage:'지원 증권사를 불러오지 못했습니다.'});
  if (!Array.isArray(catalog) || !catalog.length) throw new Error('지원 증권사를 확인하지 못했습니다. 다시 시도해 주세요.');
  PfAccounts.catalog = catalog;
}

async function pfLoadAccounts(force = false) {
  if (PfAccounts.loaded && !force) return PfAccounts.rows;
  const generation = PfAccounts.generation || 0;
  const [, rows] = await Promise.all([pfLoadBrokerCatalog(), apiFetchJson('/api/portfolio/accounts', { errorMessage: '계좌 목록을 불러오지 못했습니다.' })]);
  if ((PfAccounts.generation || 0) !== generation) return [];
  PfAccounts.rows = Array.isArray(rows) ? rows : [];
  PfAccounts.loaded = true;
  if (PfStore.accountId && !rows.some(row => row.account_id === PfStore.accountId)) PfStore.accountId = '';
  const select = _pfAccountEl('pfAccountSelect');
  if (select) {
    select.innerHTML = '<option value="">전체 계좌 · 합산</option>' + rows.map(row => `<option value="${escapeHtml(row.account_id)}">${escapeHtml(row.name)}${row.broker ? ' · ' + pfBrokerName(row.broker) + ' 연동' : ''}</option>`).join('');
    select.value = PfStore.accountId || '';
  }
  const account = rows.find(row => row.account_id === PfStore.accountId);
  const state = _pfAccountEl('pfAccountState');
  if (state) state.textContent = account?.broker ? `${pfBrokerName(account.broker)} 조회 연동 · ${account.connection?.last_sync_at ? '최근 동기화 ' + new Date(account.connection.last_sync_at).toLocaleString('ko-KR') : '첫 동기화 대기'}${account.connection?.sync_error ? ' · 동기화 확인 필요' : ''}` : account ? '이 계좌의 종목과 현금을 표시합니다.' : '모든 계좌의 동일 종목과 현금을 합산합니다.';
  if (state && account) state.textContent += ' NAV·기간 실적은 전체 계좌에서 확인하세요.';
  pfConnectNamuhQuotes();
  pfRenderDerivativeBalances();
  return rows;
}

function pfBrokerSnapshotHtml(snapshot) {
  if (!snapshot?.product?.endsWith('future')) return '';
  const fmt = value => value === null || value === undefined ? '미제공' : Number(value).toLocaleString('ko-KR', {maximumFractionDigits: 6});
  const positions = snapshot.positions || [];
  const labels = {dsg_csh:'예탁현금', dsg_sba_amt:'예탁대용', drn_pbl_amt:'출금가능액', fdv_dsg_amt:'예탁금', nxt_dd_dga_rnd:'익일 예탁잔액', fdv_brg_wtm:'위탁증거금', fdv_wrw_pbl_amt:'인출가능액'};
  return `<p>계좌 평가액 <strong>${fmt(snapshot.equity)}원</strong> · 평가손익 ${fmt(snapshot.pnl)}원</p>
    <p>${escapeHtml(snapshot.basis || '')} · 조회일 ${escapeHtml(snapshot.as_of_date || '')}${snapshot.synced_at ? ' · 갱신 ' + escapeHtml(new Date(snapshot.synced_at).toLocaleString('ko-KR')) : ''}</p>
    <p>${Object.entries(snapshot.details || {}).filter(([key]) => labels[key]).map(([key,value]) => `${labels[key]} ${fmt(value)}원`).join(' · ')}</p>
    <div class="pf-derivative-table-wrap"><table><thead><tr><th>계약</th><th>방향</th><th>계약 수</th><th>통화</th><th>평균가격</th><th>현재가격</th><th>평가손익</th></tr></thead><tbody>${positions.map(row => `<tr><td>${escapeHtml(row.name)}<br><small>${escapeHtml(row.code)}</small></td><td>${escapeHtml(row.side)}</td><td>${fmt(row.quantity)}</td><td>${escapeHtml(row.currency)}</td><td>${fmt(row.average_price)}</td><td>${fmt(row.current_price)}</td><td>${fmt(row.pnl)}</td></tr>`).join('')}</tbody></table></div>
    ${positions.length ? '' : '<p>보유 계약이 없습니다.</p>'}
    <p>합계에는 평가기준액(계좌 평가액 − 평가손익)과 평가손익을 한 번만 반영합니다. 위 예탁금·계약금액은 다시 더하지 않습니다. ${snapshot.product === 'gbfuture' ? '해외 증거금 조회에서 제공하지 않는 개별 가격·손익은 미제공으로 표시합니다.' : ''}</p>`;
}

function pfRenderDerivativeBalances() {
  const panel = _pfAccountEl('pfDerivativeBalances');
  if (!panel) return;
  const rows = PfAccounts.rows.filter(row => (!PfStore.accountId || row.account_id === PfStore.accountId) && row.broker_snapshot?.product?.endsWith('future'));
  panel.hidden = !rows.length;
  panel.innerHTML = rows.map(row => `<details class="pf-account-card" open><summary>${escapeHtml(row.name)} · ${row.broker_snapshot.product === 'krfuture' ? '국내' : '해외'}선물 잔고</summary>${row.connection?.sync_error ? `<p class="pf-account-error">동기화 실패 · 이전 잔고 표시: ${escapeHtml(row.connection.sync_error)}</p>` : ''}${!row.broker ? '<p>연결 해제 당시 잔고입니다. 자동 갱신되지 않습니다.</p>' : ''}${pfBrokerSnapshotHtml(row.broker_snapshot)}</details>`).join('');
}

async function pfSelectAccount(id) {
  if (PfStore.loading || PfStore.edit.savingCode) { _pfAccountEl('pfAccountSelect').value = PfStore.accountId || ''; return; }
  PfStore.accountId = id || '';
  PfStore.edit.code = null;
  PfStore.items = [];
  pfRenderDerivativeBalances();
  PfStore.manualOrder.pendingCodes = null;
  _pfAccountEl('pfAccountSelect').value = PfStore.accountId;
  await pfLoadAccounts(true);
  await loadPortfolio({ force: true });
}

function pfResetAccounts() {
  if (typeof pfResetActivity === 'function') pfResetActivity();
  clearTimeout(PfAccounts.refreshTimer); PfAccounts.refreshTimer = null;
  PfAccounts.generation = (PfAccounts.generation || 0) + 1;
  PfAccounts.rows = [];
  PfAccounts.loaded = false;
  PfAccounts.focusCode = PfAccounts.nhAccount = null;
  PfAccounts.previewed = false;
  PfStore.accountId = '';
  PfStore.items = [];
  pfRenderDerivativeBalances();
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
  _pfAccountEl('pfAccountsHelp').textContent = item ? `${item.stock_name} · 계좌를 선택하면 해당 계좌의 잔고를 관리할 수 있습니다.` : '계좌별로 잔고를 등록하고, 전체 계좌 보기에서 합산합니다. 증권사 연동은 조회 전용입니다.';
  _pfAccountEl('pfAccountsList').innerHTML = PfAccounts.rows.map(row => {
    const position = item?.account_positions?.find(p => p.account_id === row.account_id);
    const detail = position ? `${Number(position.quantity).toLocaleString('ko-KR')}주 · 평균 ${Number(position.avg_price).toLocaleString('ko-KR')} ${position.avg_price_currency}` : `잔고 ${row.holdings_count || 0}개`;
    const error = row.connection?.sync_error;
    return `<section class="pf-account-card" data-account="${escapeHtml(row.account_id)}"><h3>${escapeHtml(row.name)}</h3>
      <p>${escapeHtml(detail)}${row.broker ? ' · ' + pfBrokerName(row.broker) + ' ' + escapeHtml(row.connection.account_no) + (row.connection.environment === 'mock' ? ' · 모의계좌' : '') : ' · 수동 관리'}</p>
      ${error ? `<p class="pf-account-error">${escapeHtml(error)}</p>` : ''}
      ${row.connection?.activity_error ? `<p class="pf-account-error">수입·입출금 내역 확인 필요 · ${escapeHtml(row.connection.activity_error)}</p>` : ''}
      <div class="pf-account-actions"><button type="button" data-account-action="view">이 계좌 보기</button><button type="button" data-account-action="rename">이름 수정</button>
      ${row.broker ? '<button type="button" data-account-action="sync">잔고 동기화</button><button type="button" data-account-action="disconnect">연결 해제</button>' : '<button type="button" data-account-action="connect">증권사 계좌 연동</button>'}
      <button type="button" data-account-action="activity">수입·입출금 내역</button>
      <button type="button" data-account-action="delete" ${row.holdings_count || row.broker || row.account_id.startsWith('default-') ? 'disabled' : ''}>계좌 삭제</button></div>
      ${row.broker ? '<small>종목과 현금은 증권사 잔고로 갱신됩니다. 연결 해제 시 현재 잔고를 수동 계좌로 보존합니다.</small>' : ''}
      ${row.broker && !pfBrokerDefinition(row.broker)?.activity ? '<small>60초 자동 조회 · 배당·이자·입출금 내역 자동 수집은 미지원</small>' : ''}</section>`;
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
    _pfAccountEl('pfAccountsStatus').textContent = '계좌를 만들었습니다. 이 계좌 보기에서 종목을 등록하거나 증권사 계좌를 연결하세요.';
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
  if (action === 'activity') { await pfOpenAccountActivity(row); return; }
  let options = { method: 'DELETE' }, path = `/api/portfolio/accounts/${encodeURIComponent(aid)}`;
  if (action === 'rename') {
    const name = prompt('계좌 이름', row.name);
    if (!name?.trim() || name === row.name) return;
    options = { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name: name.trim() }) };
  } else if (action === 'sync') { path += `/${row.broker}/sync`; options = { method: 'POST', timeoutMs: 120000 }; }
  else if (action === 'disconnect') {
    if (!confirm(`${pfBrokerName(row.broker)} 연결을 해제하고 현재 잔고를 수동 계좌로 보존할까요?`)) return;
    path += `/${row.broker}`;
  } else if (!confirm('비어 있는 계좌를 삭제할까요?')) return;
  PfAccounts.busy = true; button.disabled = true;
  _pfAccountEl('pfAccountsStatus').textContent = '처리 중입니다…';
  try {
    const data = await apiFetchJson(path, options);
    await pfLoadAccounts(true); pfRenderAccounts();
    await loadPortfolio({ force: true });
    _pfAccountEl('pfAccountsStatus').textContent = data?.activity_error ? `잔고는 갱신했습니다. ${data.activity_error}` : '반영했습니다.';
  } catch (error) { _pfAccountEl('pfAccountsStatus').textContent = error.message; }
  finally { PfAccounts.busy = false; button.disabled = false; }
}

function pfOpenNhConnection(account, provider = 'namuh') {
  PfAccounts.nhAccount = account.account_id;
  PfAccounts.accountName = account.name;
  _pfAccountEl('pfBrokerProvider').innerHTML = PfAccounts.catalog.map(row => `<option value="${escapeHtml(row.id)}">${escapeHtml(row.name)}</option>`).join('');
  pfConfigureBroker(provider);
  _pfAccountEl('pfNhDialog').showModal();
}

function pfConfigureBroker(provider) {
  const definition = pfBrokerDefinition(provider);
  if (!definition) return;
  PfAccounts.provider = provider;
  PfAccounts.previewed = false;
  _pfAccountEl('pfNhForm').reset();
  _pfAccountEl('pfBrokerProvider').value = provider;
  _pfAccountEl('pfKisFields').hidden = !definition.account_input && !definition.hts_id && (!definition.select_environment || definition.environments.length < 2);
  _pfAccountEl('pfKisAccount').closest('label').hidden = !definition.account_input;
  _pfAccountEl('pfKisAccount').required = definition.account_input;
  _pfAccountEl('pfKisHts').closest('label').hidden = !definition.hts_id;
  _pfAccountEl('pfKisEnvironment').closest('label').hidden = !definition.select_environment;
  _pfAccountEl('pfKisEnvironment').innerHTML = definition.environments.map(env => `<option value="${env}">${env === 'mock' ? '모의계좌' : '실계좌'}</option>`).join('');
  _pfAccountEl('pfNhProduct').innerHTML = definition.products.map(product => `<option value="${escapeHtml(product.id)}">${escapeHtml(product.label)}</option>`).join('');
  _pfAccountEl('pfNhProduct').closest('label').hidden = definition.products.length < 2;
  _pfAccountEl('pfBrokerKeyLabel').textContent = `${definition.name} APP KEY`;
  _pfAccountEl('pfBrokerSecretLabel').textContent = `${definition.name} APP SECRET`;
  _pfAccountEl('pfNhClose').setAttribute('aria-label', `${definition.name} 연동 닫기`);
  _pfAccountEl('pfBrokerHelp').textContent = definition.help;
  _pfAccountEl('pfNhTitle').textContent = `${PfAccounts.accountName} · ${definition.name} 계좌 연동`;
  _pfAccountEl('pfNhChoices').innerHTML = '<option value="">앱키 확인 후 계좌를 선택하세요</option>';
  _pfAccountEl('pfNhPreview').textContent = '';
  _pfAccountEl('pfNhStatus').textContent = '';
  _pfAccountEl('pfNhSave').disabled = true;
  _pfAccountEl('pfNhPreviewButton').disabled = true;
  pfNhProductChanged();
}

function pfNhProductChanged() {
  const definition = pfBrokerDefinition(PfAccounts.provider || 'namuh');
  const product = definition?.products.find(item => item.id === _pfAccountEl('pfNhProduct').value);
  if (!product) return;
  const env = _pfAccountEl('pfKisEnvironment').value;
  const overseas = env === 'mock' ? product.overseas_mock : product.overseas_live;
  _pfAccountEl('pfNhOverseas').closest('label').hidden = !overseas;
  if (!overseas) _pfAccountEl('pfNhOverseas').checked = false;
  _pfAccountEl('pfNhProductHelp').textContent = product.help + (!definition.activity ? ' 배당·이자·입출금 내역 자동 수집은 미지원입니다.' : '');
}

async function pfNhWork(action) {
  if (PfAccounts.busy) return;
  PfAccounts.busy = true;
  const fields = _pfAccountEl('pfNhFields'); fields.disabled = true;
  _pfAccountEl('pfNhClose').disabled = true;
  const provider = PfAccounts.provider || 'namuh';
  const generation = PfAccounts.generation || 0;
  _pfAccountEl('pfNhStatus').textContent = `${pfBrokerName(provider)}에서 조회하고 있습니다…`;
  try {
    if (action === 'verify') {
      PfAccounts.previewed = false; _pfAccountEl('pfNhSave').disabled = true; _pfAccountEl('pfNhPreviewButton').disabled = true;
      _pfAccountEl('pfNhChoices').innerHTML = ''; _pfAccountEl('pfNhPreview').textContent = '';
      const credentials = { app_key: _pfAccountEl('pfNhKey').value.trim(), app_secret: _pfAccountEl('pfNhSecret').value.trim() };
      const definition = pfBrokerDefinition(provider);
      credentials.environment = _pfAccountEl('pfKisEnvironment').value;
      if (definition.account_input) credentials.account_no = _pfAccountEl('pfKisAccount').value.trim();
      if (definition.hts_id) credentials.hts_id = _pfAccountEl('pfKisHts').value.trim();
      const data = await apiFetchJson(`/api/portfolio/${provider}/credentials`, { method: 'POST', timeoutMs: 60000,
        headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(credentials) });
      if (generation !== (PfAccounts.generation || 0)) return;
      _pfAccountEl('pfNhKey').value = _pfAccountEl('pfNhSecret').value = '';
      _pfAccountEl('pfNhChoices').innerHTML = data.accounts.map(row => `<option value="${escapeHtml(row.selection)}">${escapeHtml(row.account_no)} · ${row.environment === 'mock' ? '모의계좌' : '실계좌'}</option>`).join('');
      _pfAccountEl('pfNhPreviewButton').disabled = !data.accounts.length;
      _pfAccountEl('pfNhStatus').textContent = data.accounts.length ? '키를 확인했습니다. 연결할 계좌와 조회 범위를 선택해 주세요.' : '이 키에서 연결 가능한 계좌가 없습니다.';
      PfAccounts.previewed = false; _pfAccountEl('pfNhSave').disabled = true;
    } else {
      const payload = { selection: _pfAccountEl('pfNhChoices').value, include_overseas: _pfAccountEl('pfNhOverseas').checked, product: _pfAccountEl('pfNhProduct').value };
      const base = `/api/portfolio/accounts/${encodeURIComponent(PfAccounts.nhAccount)}/${provider}`;
      if (!payload.selection) throw new Error('연결할 계좌를 선택하세요.');
      if (action === 'save' && !PfAccounts.previewed) throw new Error('잔고 미리보기를 먼저 확인하세요.');
      const data = await apiFetchJson(base + (action === 'preview' ? '/preview' : ''), { method: 'POST', timeoutMs: 120000,
        headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
      if (generation !== (PfAccounts.generation || 0)) return;
      if (action === 'preview') {
        _pfAccountEl('pfNhPreview').innerHTML = `<p>${data.items.length}개 잔고 · 현금 추가 차감 없이 초기 잔고로 가져옵니다.</p><table><thead><tr><th>종목</th><th>수량·잔액</th><th>통화</th></tr></thead><tbody>${data.items.map(item => `<tr><td>${escapeHtml(item.stock_name)}</td><td>${Number(item.quantity).toLocaleString('ko-KR')}</td><td>${escapeHtml(item.currency)}</td></tr>`).join('')}</tbody></table><p>${escapeHtml(pfBrokerDefinition(provider)?.products.find(item => item.id === payload.product)?.help || '')}</p>${data.items.some(item => item.stock_code === 'CMA_RP_KRW') ? '<p>CMA 원화RP는 현금과 구분하며, 수량·잔액에 증권사 조회 시점의 평가액(원)을 표시합니다.</p>' : ''}`;
        PfAccounts.previewed = true; _pfAccountEl('pfNhSave').disabled = false;
        if (data.balances?._excluded?.length) _pfAccountEl('pfNhPreview').insertAdjacentHTML('beforeend', `<p>${escapeHtml(data.balances._excluded_reason || '비상장·상장폐지')} ${data.balances._excluded.length}개 종목은 제외했습니다.</p>`);
        if (data.broker_snapshot?.product?.endsWith('future')) {
          _pfAccountEl('pfNhPreview').innerHTML = pfBrokerSnapshotHtml(data.broker_snapshot);
        }
        _pfAccountEl('pfNhStatus').textContent = '미리보기를 확인한 뒤 잔고 가져오기를 누르세요.';
      } else {
        _pfAccountEl('pfNhDialog').close();
        await pfLoadAccounts(true); pfRenderAccounts(); await loadPortfolio({ force: true });
        _pfAccountEl('pfAccountsStatus').textContent = data.activity_error ? `계좌 연결과 잔고 가져오기를 완료했습니다. ${data.activity_error}` : provider === 'kis' ? '한국투자증권 계좌를 연결했습니다. 잔고는 60초마다 갱신하며, HTS ID를 등록하면 체결 통보로도 갱신합니다.' : 'NH 계좌를 연결했습니다. 주문·체결 통보를 받으면 갱신하며, 수입·입출금은 60초마다 확인합니다.';
      }
    }
  } catch (error) {
    _pfAccountEl('pfNhStatus').textContent = error.message;
    if (action === 'save') { await pfLoadAccounts(true).catch(() => {}); pfRenderAccounts(); }
  } finally { PfAccounts.busy = false; fields.disabled = false; _pfAccountEl('pfNhClose').disabled = false; }
}

function pfCheckNamuhConnection() {
  if (!PfAccounts.socket || Date.now() - PfAccounts.lastMessageAt < 15_000) return;
  const old = PfAccounts.socket;
  old.onclose = null;
  old.close();
  PfAccounts.socket = null;
  clearInterval(PfAccounts.watchdog); PfAccounts.watchdog = null;
  QuoteManager.namuhUnavailable?.();
  pfConnectNamuhQuotes();
}

function pfRefreshChangedAccounts() {
  if (PfAccounts.refreshTimer) return;
  const generation = PfAccounts.generation || 0;
  const refresh = async () => {
    if (generation !== (PfAccounts.generation || 0)) return;
    if (PfStore.loading || PfStore.edit?.code || PfStore.edit?.savingCode || PfAccounts.busy
        || PfStore.manualOrder.draggingCode || PfStore.manualOrder.saveInFlight) {
      PfAccounts.refreshTimer = setTimeout(refresh, 1000); return;
    }
    PfAccounts.refreshTimer = null;
    try {
      await pfLoadAccounts(true);
      if (generation !== (PfAccounts.generation || 0)) return;
      if (_pfAccountEl('pfAccountsDialog')?.open) pfRenderAccounts();
      await loadPortfolio({ force: true });
    } catch (_) { /* 다음 서버 갱신 또는 수동 조회로 다시 확인 */ }
  };
  PfAccounts.refreshTimer = setTimeout(refresh, 250);
}

function pfConnectNamuhQuotes() {
  const linked = PfAccounts.rows.some(row => row.broker === 'namuh');
  QuoteManager.setNamuhLinked?.(linked);
  if (!PfAccounts.rows.some(row => row.broker)) {
    clearTimeout(PfAccounts.retry); PfAccounts.retry = null;
    clearInterval(PfAccounts.watchdog); PfAccounts.watchdog = null;
    if (PfAccounts.socket) { PfAccounts.socket.onclose = null; PfAccounts.socket.close(); PfAccounts.socket = null; }
    return;
  }
  if (PfAccounts.socket) return;
  const socket = new WebSocket(`${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/ws/broker-accounts`);
  PfAccounts.socket = socket;
  PfAccounts.lastMessageAt = Date.now();
  PfAccounts.watchdog = setInterval(pfCheckNamuhConnection, 10_000);
  socket.onmessage = event => {
    if (PfAccounts.socket !== socket) return;
    PfAccounts.lastMessageAt = Date.now();
    try {
      const message = JSON.parse(event.data);
      if (message.type === 'quote') {
        if (QuoteManager.onNamuhQuote) QuoteManager.onNamuhQuote(message.code, message);
        else QuoteManager.onQuote?.(message.code, message);
      }
      if (message.type === 'accounts_changed') {
        pfRefreshChangedAccounts();
        if (typeof pfActivityAccountsChanged === 'function') pfActivityAccountsChanged(message.accounts || []);
      }
      if (message.type === 'namuh_status') {
        QuoteManager._syncNamuhFallback?.();
        const label = _pfAccountEl('pfNhQuoteState');
        if (label) label.textContent = ({live: 'NH 실시간 시세 우선 사용', subscribed: 'NH 시세 구독 · 체결 대기',
          connecting: 'NH 시세 연결 중', degraded: 'NH 시세 연결 불안정 · 보조 시세 사용',
          waiting: 'NH 시세 연결 대기'}[message.state] || 'NH 시세 상태 확인 중')
          + (message.domestic?.rejected ? ' · 국내 시세 구독 권한·한도 확인 필요' : '')
          + (message.foreign?.rejected ? ' · 해외 실시간 권한·구독 한도 확인 필요, 보조 시세 사용' : '');
        if (label && message.notifications) label.textContent += message.notifications.state === 'subscribed'
          ? ' · 계좌 통보 연결 · 입출금 60초 확인' : message.notifications.state === 'degraded'
            ? ' · 계좌 통보 미연결 · 60초 조회 보완' : ' · 계좌 통보 연결 중 · 60초 조회 보완';
      }
      if (message.type === 'broker_account_status' || message.type === 'kis_account_status') {
        const state = _pfAccountEl('pfAccountState');
        const row = PfAccounts.rows.find(account => account.account_id === PfStore.accountId);
        if (state && row?.broker === (message.provider || 'kis')) {
          const suffix = message.state === 'subscribed' ? '체결 통보 연결 · 60초 조회 보완' : '60초 자동 조회 · 체결 통보 미연결';
          state.textContent = state.textContent.split(' | ')[0] + ' | ' + suffix;
        }
      }
    } catch (_) { /* 잘못된 개별 메시지는 다음 메시지에 영향을 주지 않는다. */ }
  };
  socket.onclose = () => {
    if (PfAccounts.socket !== socket) return;
    clearInterval(PfAccounts.watchdog); PfAccounts.watchdog = null;
    QuoteManager.namuhUnavailable?.(); PfAccounts.socket = null;
    PfAccounts.retry = setTimeout(pfConnectNamuhQuotes, 10000);
  };
}

document.addEventListener('DOMContentLoaded', () => {
  _pfAccountEl('pfAccountSelect')?.addEventListener('change', event => pfSelectAccount(event.target.value));
  _pfAccountEl('pfAccountsOpen')?.addEventListener('click', () => pfOpenAccountManager());
  _pfAccountEl('pfAccountsClose')?.addEventListener('click', () => _pfAccountEl('pfAccountsDialog').close());
  _pfAccountEl('pfAccountForm')?.addEventListener('submit', pfCreateAccount);
  _pfAccountEl('pfAccountsList')?.addEventListener('click', pfAccountAction);
  _pfAccountEl('pfNhClose')?.addEventListener('click', () => _pfAccountEl('pfNhDialog').close());
  _pfAccountEl('pfBrokerProvider')?.addEventListener('change', event => pfConfigureBroker(event.target.value));
  _pfAccountEl('pfNhForm')?.addEventListener('submit', event => { event.preventDefault(); pfNhWork('verify'); });
  _pfAccountEl('pfNhPreviewButton')?.addEventListener('click', () => pfNhWork('preview'));
  _pfAccountEl('pfNhSave')?.addEventListener('click', () => pfNhWork('save'));
  for (const id of ['pfNhChoices', 'pfNhOverseas', 'pfNhProduct']) _pfAccountEl(id)?.addEventListener('change', () => { pfNhProductChanged(); PfAccounts.previewed = false; _pfAccountEl('pfNhSave').disabled = true; _pfAccountEl('pfNhPreview').textContent = ''; });
  for (const id of ['pfKisAccount', 'pfKisEnvironment', 'pfKisHts']) _pfAccountEl(id)?.addEventListener('change', () => {
    pfNhProductChanged(); PfAccounts.previewed = false;
    _pfAccountEl('pfNhChoices').innerHTML = ''; _pfAccountEl('pfNhSave').disabled = true; _pfAccountEl('pfNhPreviewButton').disabled = true;
    _pfAccountEl('pfNhPreview').textContent = ''; _pfAccountEl('pfNhStatus').textContent = '변경한 계좌 정보로 앱키를 다시 확인해 주세요.';
  });
  _pfAccountEl('pfNhDialog')?.addEventListener('cancel', event => { if (PfAccounts.busy) event.preventDefault(); });
  _pfAccountEl('pfNhDialog')?.addEventListener('close', () => { for (const id of ['pfNhKey', 'pfNhSecret', 'pfKisAccount', 'pfKisHts']) _pfAccountEl(id).value = ''; });
  document.addEventListener('click', event => {
    if (event.target.closest('.js-pf-account-detail')) { event.preventDefault(); pfOpenAccountManager(event.target.closest('tr[data-code]')?.dataset.code); return; }
    if (event.target.closest('.js-pf-trade,.js-pf-delete,.js-pf-dividend-receipt,.js-pf-distribution,.js-pf-cashflow,#pfAddToggle') && pfAccountNeedsSelection()) {
      event.preventDefault(); event.stopImmediatePropagation();
      pfOpenAccountManager(event.target.closest('tr[data-code]')?.dataset.code);
    }
  }, true);
});
