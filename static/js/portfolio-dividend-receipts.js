// 배당 캘린더의 예상치로 입력을 준비하되, 수취 확인 후에만 현금과 원장을 저장한다.
const _pfDividend = { events: [], sourceKey: null, preview: null, payload: null, pending: null, busy: false, generation: 0, dirty: false };
const _pfDividendEl = id => document.getElementById(`pfDividend${id}`);
const _pfDividendFmt = value => Number(value).toLocaleString('ko-KR', { maximumFractionDigits: 8 });
const _pfDividendRates = { KR: 15.4, US: 15, CN: 14.4, HK: 15.4, OTHER: 0 };
const _pfDividendToday = () => new Date().toLocaleDateString('sv-SE', { timeZone: 'Asia/Seoul' });

function _pfDividendStorageKey() {
  const user = typeof currentUser === 'undefined' ? null : currentUser;
  return `value-invest:pending-dividend:${user?.google_sub || user?.email || 'session'}`;
}

function _pfDividendPersist(pending) {
  _pfDividend.pending = pending;
  try {
    if (pending) sessionStorage.setItem(_pfDividendStorageKey(), JSON.stringify(pending));
    else sessionStorage.removeItem(_pfDividendStorageKey());
  } catch { /* 저장소 접근이 차단돼도 현재 화면의 요청은 유지한다. */ }
}

function _pfDividendInvalidate() {
  if (_pfDividend.busy || _pfDividend.pending) return;
  _pfDividend.preview = null;
  _pfDividend.payload = null;
  _pfDividendEl('Preview').textContent = '';
  _pfDividendEl('Status').textContent = '';
  _pfDividendEl('Save').disabled = true;
  _pfDividendEl('Save').textContent = '배당금 수취 저장';
}

function _pfDividendCountry(code, currency) {
  if (/^[0-9][0-9A-Z]{5}$/.test(code)) return 'KR';
  if (/\.(SS|SZ)$/.test(code) || currency === 'CNY') return 'CN';
  if (/\.HK$/.test(code)) return 'HK';
  if (/^[A-Z]+([.-][A-Z]+)?$/.test(code) && ['USD', 'KRW'].includes(currency)) return 'US';
  return 'OTHER';
}

function _pfDividendSetStock(code, name, currency) {
  _pfDividendEl('Code').value = code;
  _pfDividendEl('Name').value = name;
  _pfDividendEl('Currency').value = currency || 'KRW';
  _pfDividendEl('Country').value = _pfDividendCountry(code, currency);
  _pfDividendEl('TaxRate').value = _pfDividendRates[_pfDividendEl('Country').value];
  _pfDividendEl('TaxAmount').value = '';
  const holding = PfStore.items.find(i => i.stock_code === code);
  _pfDividendEl('Holding').value = holding ? code : '';
  _pfDividendEl('Quantity').value = holding?.quantity > 0 ? holding.quantity : '';
  _pfDividendFx();
}

function _pfDividendFx() {
  const currency = _pfDividendEl('Currency').value;
  const foreign = currency !== 'KRW';
  _pfDividendEl('FxLabel').hidden = !foreign;
  _pfDividendEl('Fx').required = foreign;
  const reference = currency === 'USD' ? PfStore.currency?.fxRate : null;
  _pfDividendEl('Fx').value = reference > 0 ? reference : '';
  _pfDividendEl('FxText').textContent = `수취 환율: 1 ${currency} = ? KRW${reference > 0 ? ' · 현재 참고 환율, 실제 수취 환율 확인' : ''}`;
}

function _pfDividendAmounts() {
  const perShare = _pfDividendEl('Mode').value === 'shares';
  _pfDividendEl('PerShareLabel').hidden = !perShare;
  _pfDividendEl('QuantityLabel').hidden = !perShare;
  _pfDividendEl('GrossLabel').hidden = perShare;
  _pfDividendEl('PerShare').required = perShare;
  _pfDividendEl('Quantity').required = perShare;
  _pfDividendEl('Gross').required = !perShare;
  const currency = _pfDividendEl('Currency').value;
  const scale = ['KRW', 'JPY', 'VND'].includes(currency) ? 1 : 100;
  const input = perShare ? Number(_pfDividendEl('PerShare').value) * Number(_pfDividendEl('Quantity').value) : Number(_pfDividendEl('Gross').value);
  const gross = Math.round(input * scale) / scale;
  const tax = _pfDividendEl('TaxAmount').value === '' ? Math.floor(gross * Number(_pfDividendEl('TaxRate').value) / 100 * scale + 1e-8) / scale : Number(_pfDividendEl('TaxAmount').value);
  _pfDividendEl('Amounts').textContent = `세전 ${_pfDividendFmt(gross)} − 공제 ${_pfDividendFmt(tax)} = 세후 ${_pfDividendFmt(gross - tax)} ${currency}`;
}

function _pfDividendSelectSchedule() {
  const event = _pfDividend.events.find(ev => ev.source_key === _pfDividendEl('Schedule').value);
  _pfDividend.sourceKey = event?.source_key || null;
  _pfDividendInvalidate();
  if (!event) {
    _pfDividendEl('ScheduleNote').textContent = '스케줄 없이 실제 배당 입금 내역을 직접 입력할 수 있습니다.';
    return;
  }
  _pfDividendSetStock(event.stock_code, event.stock_name, event.currency || 'KRW');
  _pfDividendEl('Mode').value = 'shares';
  _pfDividendEl('PerShare').value = event.amount_per_share > 0 ? event.amount_per_share : '';
  _pfDividendEl('Date').value = event.type !== 'ex_date' && event.date <= _pfDividendToday() ? event.date : '';
  const base = Number(event.amount_per_share) * Number(event.shares);
  if (event.currency !== 'KRW' && base > 0 && event.expected_amount_krw > 0) {
    _pfDividendEl('Fx').value = Number((event.expected_amount_krw / base).toFixed(8));
    _pfDividendEl('FxText').textContent = `수취 환율: 1 ${event.currency} = ? KRW · 스케줄 참고 환율, 실제 수취 환율 확인`;
  }
  _pfDividendEl('ScheduleNote').textContent = event.type === 'ex_date'
    ? `${event.date}는 배당기준일입니다. 실제 수취일을 입력하세요. 수량은 현재 보유 수량이므로 배당 대상 수량을 확인하세요.`
    : `${event.date} 지급 예상 · 과거 실적 기반 주당 배당금과 현재 보유 수량을 채웠습니다. 실제 수취일·금액·배당 대상 수량을 확인하세요.`;
  _pfDividendAmounts();
}

function _pfDividendRead() {
  const perShare = _pfDividendEl('Mode').value === 'shares';
  return {
    stock_code: _pfDividendEl('Code').value.trim().toUpperCase(), stock_name: _pfDividendEl('Name').value.trim(),
    currency: _pfDividendEl('Currency').value, country: _pfDividendEl('Country').value,
    received_date: _pfDividendEl('Date').value,
    gross_amount: perShare ? null : _pfDividendEl('Gross').value,
    quantity: perShare ? _pfDividendEl('Quantity').value : null,
    amount_per_share: perShare ? _pfDividendEl('PerShare').value : null,
    tax_rate: _pfDividendEl('TaxRate').value, tax_amount: _pfDividendEl('TaxAmount').value || null,
    fx_rate: _pfDividendEl('FxLabel').hidden ? null : _pfDividendEl('Fx').value || null,
    source_key: _pfDividend.sourceKey, memo: _pfDividendEl('Memo').value.trim(),
  };
}

function _pfDividendRenderPreview(result) {
  const fmt = _pfDividendFmt;
  _pfDividendEl('Preview').innerHTML = `<strong>${escapeHtml(result.stock_name)} 배당금 수취</strong>
    <dl><dt>실제 수취일</dt><dd>${escapeHtml(result.received_date)}</dd>
    <dt>세전 배당금</dt><dd>${fmt(result.gross_amount)} ${escapeHtml(result.currency)}</dd>
    <dt>원천징수 (${fmt(result.tax_rate)}%)</dt><dd>${fmt(result.tax_amount)} ${escapeHtml(result.currency)}</dd>
    <dt>세후 입금액</dt><dd>+${fmt(result.net_amount)} ${escapeHtml(result.currency)}</dd>
    <dt>현금 잔고</dt><dd>${fmt(result.cash_before)} → ${fmt(result.cash_after)} ${escapeHtml(result.currency)}</dd>
    <dt>배당 수익 분류</dt><dd>${fmt(result.amount_krw)} KRW</dd></dl>
    <p class="pf-trade-help">${escapeHtml(result.applied_date)} 현재 현금과 배당 수익에 반영합니다. 보유 수량·평균 매입가·NAV 좌수는 그대로 유지합니다.</p>`;
}

async function pfPreviewDividendReceipt(event) {
  event?.preventDefault();
  if (_pfDividend.busy || _pfDividend.pending) return;
  _pfDividendInvalidate();
  _pfDividend.dirty = true;
  try {
    const payload = _pfDividendRead();
    _pfDividend.busy = true;
    _pfDividendEl('Fields').disabled = true;
    const result = await apiFetchJson('/api/portfolio/dividend-receipts/preview', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload), errorMessage: '배당 수취 내용을 확인하지 못했습니다.',
    });
    _pfDividend.preview = result;
    _pfDividend.payload = payload;
    _pfDividendRenderPreview(result);
    _pfDividendEl('Save').disabled = false;
  } catch (error) { _pfDividendEl('Status').textContent = error.message; }
  finally { _pfDividend.busy = false; _pfDividendEl('Fields').disabled = false; }
}

async function pfLoadDividendReceipts() {
  try {
    const rows = await apiFetchJson('/api/portfolio/dividend-receipts?limit=20', { errorMessage: '수취 내역을 불러오지 못했습니다.' });
    _pfDividendEl('History').innerHTML = rows.length ? rows.map(row => `<article><strong>${escapeHtml(row.stock_name)} · ${escapeHtml(row.received_date)}</strong>
      <p>세전 ${_pfDividendFmt(row.gross_amount)} − 공제 ${_pfDividendFmt(row.tax_amount)} = 세후 ${_pfDividendFmt(row.net_amount)} ${escapeHtml(row.currency)}</p>
      <small>${escapeHtml(new Date(row.created_at).toLocaleString('ko-KR'))}${row.memo ? ` · ${escapeHtml(row.memo)}` : ''}</small></article>`).join('') : '<p>아직 기록한 배당 수취가 없습니다.</p>';
  } catch (error) { _pfDividendEl('History').textContent = error.message; }
  try {
    const totals = await apiFetchJson('/api/portfolio/distributions/balances', { errorMessage: '배당 누적액을 불러오지 못했습니다.' });
    _pfDividendEl('Totals').textContent = `세후 배당 누계 · ${totals.length ? totals.map(row => `${row.currency}: 수취 ${_pfDividendFmt(row.net_amount)} / 분배 ${_pfDividendFmt(row.distributed_amount)} / 미분배 ${_pfDividendFmt(row.available_amount)}`).join(' · ') : '아직 수취 기록이 없습니다.'}`;
  } catch (error) { _pfDividendEl('Totals').textContent = error.message; }
}

async function pfSaveDividendReceipt() {
  if (_pfDividend.busy || (!_pfDividend.preview && !_pfDividend.pending)) return;
  if (!_pfDividend.pending) {
    if (JSON.stringify(_pfDividendRead()) !== JSON.stringify(_pfDividend.payload)) { _pfDividendInvalidate(); return; }
    _pfDividendPersist({ ..._pfDividend.payload, request_id: crypto.randomUUID(), expected_revision: _pfDividend.preview.revision });
  }
  _pfDividend.busy = true;
  _pfDividendEl('Fields').disabled = true;
  _pfDividendEl('Save').disabled = true;
  _pfDividendEl('Status').textContent = '배당 수취 내역을 저장하고 있습니다…';
  let saved;
  try {
    saved = await apiFetchJson('/api/portfolio/dividend-receipts', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(_pfDividend.pending), errorMessage: '저장 결과를 확인하지 못했습니다.',
    });
    if (!saved || !Number.isFinite(saved.net_amount) || !Number.isFinite(saved.cash_after)) throw new Error('저장 응답을 확인하지 못했습니다.');
  } catch (error) {
    saved = null;
    if ([400, 401, 403, 409, 422].includes(error.status)) {
      _pfDividendPersist(null);
      _pfDividendEl('Fields').disabled = false;
      _pfDividend.preview = null;
      _pfDividendEl('Preview').textContent = '';
      _pfDividendEl('Status').textContent = `${error.message} 수취 내용을 다시 확인해 주세요.`;
    } else {
      _pfDividendEl('Status').textContent = '응답을 확인하지 못했습니다. 같은 요청으로 저장 결과를 확인하면 중복 반영되지 않습니다.';
      _pfDividendEl('Save').textContent = '저장 결과 확인';
      _pfDividendEl('Save').disabled = false;
    }
  } finally { _pfDividend.busy = false; }
  if (!saved) return;
  _pfDividendPersist(null);
  _pfDividend.preview = null;
  _pfDividend.payload = null;
  _pfDividendEl('Fields').disabled = false;
  _pfDividendEl('Form').reset();
  _pfDividend.sourceKey = null;
  _pfDividendEl('Date').value = _pfDividendToday();
  _pfDividendFx();
  _pfDividendAmounts();
  _pfDividendEl('Save').textContent = '배당금 수취 저장';
  _pfDividendRenderPreview(saved);
  _pfDividendEl('Status').textContent = '세후 배당금을 현금과 배당 수익 내역에 저장했습니다.';
  if (saved.source_key) {
    const option = [..._pfDividendEl('Schedule').options].find(o => o.value === saved.source_key);
    if (option) { option.disabled = true; option.textContent += ' · 수취 완료'; }
  }
  PfStore.edit.code = null;
  await Promise.allSettled([loadPortfolio({ force: true }), pfLoadDividendReceipts()]);
  if (typeof pfRefreshTodayState === 'function') await Promise.allSettled([pfRefreshTodayState({ force: true, render: true })]);
  if (typeof pfLoadAttribution === 'function') await Promise.allSettled([pfLoadAttribution()]);
}

async function pfOpenDividendReceipt(sourceKey) {
  if (_pfDividend.busy) return;
  const generation = ++_pfDividend.generation;
  _pfDividendEl('Form').reset();
  _pfDividend.sourceKey = null;
  _pfDividend.preview = null;
  _pfDividend.payload = null;
  _pfDividend.dirty = false;
  _pfDividendEl('Fields').disabled = false;
  _pfDividendEl('Preview').textContent = '';
  _pfDividendEl('Status').textContent = '';
  _pfDividendEl('Date').max = _pfDividendToday();
  _pfDividendEl('Date').value = _pfDividendToday();
  _pfDividendEl('Holding').innerHTML = '<option value="">직접 입력 (매도한 종목 등)</option>' + PfStore.items.filter(i => !/^(CASH_|FUT_|SHORT_|IDX_|CRYPTO_|KRX_GOLD)/.test(i.stock_code)).map(i => `<option value="${escapeHtml(i.stock_code)}">${escapeHtml(i.stock_name)} (${escapeHtml(i.stock_code)})</option>`).join('');
  try { _pfDividend.pending = JSON.parse(sessionStorage.getItem(_pfDividendStorageKey()) || 'null'); } catch { /* 현재 화면의 요청 유지 */ }
  _pfDividendFx();
  if (_pfDividend.pending) {
    const p = _pfDividend.pending;
    _pfDividend.sourceKey = p.source_key;
    _pfDividendEl('Mode').value = p.gross_amount === null ? 'shares' : 'total';
    for (const [id, key] of [['Code', 'stock_code'], ['Name', 'stock_name'], ['Country', 'country'], ['Currency', 'currency'], ['Date', 'received_date'], ['Gross', 'gross_amount'], ['Quantity', 'quantity'], ['PerShare', 'amount_per_share'], ['TaxRate', 'tax_rate'], ['TaxAmount', 'tax_amount'], ['Fx', 'fx_rate'], ['Memo', 'memo']]) _pfDividendEl(id).value = p[key] ?? '';
    _pfDividendEl('FxLabel').hidden = p.currency === 'KRW';
    _pfDividendEl('Fields').disabled = true;
    _pfDividendEl('Save').disabled = false;
    _pfDividendEl('Save').textContent = '저장 결과 확인';
    _pfDividendEl('Status').textContent = '확인 중인 배당 수취가 있습니다. 같은 요청으로 저장 결과를 확인해 주세요.';
  } else _pfDividendInvalidate();
  _pfDividendAmounts();
  if (!_pfDividendEl('Dialog').open) _pfDividendEl('Dialog').showModal();
  pfLoadDividendReceipts();
  _pfDividendEl('Schedule').innerHTML = '<option value="">직접 입력</option>';
  _pfDividendEl('ScheduleNote').textContent = '배당 스케줄을 불러오고 있습니다…';
  try {
    const data = await apiFetchJson('/api/portfolio/dividend-receipts/candidates', { errorMessage: '배당 스케줄을 불러오지 못했습니다.' });
    if (generation !== _pfDividend.generation) return;
    _pfDividend.events = (data.events || []).slice().sort((a, b) => b.date.localeCompare(a.date));
    _pfDividendEl('Schedule').innerHTML += _pfDividend.events.map(ev => `<option value="${escapeHtml(ev.source_key)}"${ev.received ? ' disabled' : ''}>${escapeHtml(ev.date)} · ${escapeHtml(ev.stock_name)} · ${ev.type === 'ex_date' ? '배당기준일' : '예상 지급'}${ev.received ? ' · 수취 완료' : ''}</option>`).join('');
    if (!_pfDividend.dirty && !_pfDividend.pending && !_pfDividend.busy) {
      const selected = _pfDividend.events.find(ev => ev.source_key === sourceKey && !ev.received);
      if (selected) { _pfDividendEl('Schedule').value = sourceKey; _pfDividendSelectSchedule(); }
      else _pfDividendEl('ScheduleNote').textContent = '스케줄을 선택하면 주당 배당금·현재 보유 수량·기본 세율을 채웁니다. 배당기준일은 지급일이 아닙니다.';
    }
  } catch (error) { if (generation === _pfDividend.generation) _pfDividendEl('ScheduleNote').textContent = `${error.message} 직접 입력은 계속 사용할 수 있습니다.`; }
}

_pfDividendEl('Form').addEventListener('submit', pfPreviewDividendReceipt);
_pfDividendEl('Form').addEventListener('input', () => { _pfDividend.dirty = true; _pfDividendInvalidate(); _pfDividendAmounts(); });
_pfDividendEl('Form').addEventListener('change', () => { _pfDividend.dirty = true; _pfDividendInvalidate(); _pfDividendAmounts(); });
_pfDividendEl('Schedule').addEventListener('change', _pfDividendSelectSchedule);
_pfDividendEl('Holding').addEventListener('change', () => {
  _pfDividend.sourceKey = null;
  _pfDividendEl('Schedule').value = '';
  const item = PfStore.items.find(i => i.stock_code === _pfDividendEl('Holding').value);
  if (item) _pfDividendSetStock(item.stock_code, item.stock_name, item.currency);
  else { _pfDividendEl('Code').value = ''; _pfDividendEl('Name').value = ''; _pfDividendEl('Quantity').value = ''; }
  _pfDividendEl('PerShare').value = '';
  _pfDividendEl('ScheduleNote').textContent = '실제 수취한 배당금과 배당 대상 수량을 입력하세요.';
});
_pfDividendEl('Code').addEventListener('input', () => { _pfDividend.sourceKey = null; _pfDividendEl('Schedule').value = ''; _pfDividendEl('Holding').value = ''; });
_pfDividendEl('Country').addEventListener('change', () => { _pfDividendEl('TaxRate').value = _pfDividendRates[_pfDividendEl('Country').value]; _pfDividendEl('TaxAmount').value = ''; });
_pfDividendEl('Currency').addEventListener('change', () => { _pfDividendFx(); _pfDividendEl('TaxAmount').value = ''; });
_pfDividendEl('Save').addEventListener('click', pfSaveDividendReceipt);
_pfDividendEl('Close').addEventListener('click', () => _pfDividendEl('Dialog').close());
