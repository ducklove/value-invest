// 현금·현물의 매매 입력. 서버 미리보기 이후에만 저장하며 불명확한 응답은 같은 요청으로 재확인한다.
const _pfTrade = { selected: null, choices: [], preview: null, payload: null, pending: null, busy: false, search: 0 };
const _pfTradeEl = id => document.getElementById(`pfTrade${id}`);
const _pfTradeFmt = value => Number(value).toLocaleString('ko-KR', { maximumFractionDigits: 8 });

function _pfTradeStorageKey() {
  const user = typeof currentUser === 'undefined' ? null : currentUser;
  return `value-invest:pending-trade:${user?.google_sub || user?.email || 'session'}`;
}

function _pfTradePersist(pending) {
  _pfTrade.pending = pending;
  try {
    if (pending) sessionStorage.setItem(_pfTradeStorageKey(), JSON.stringify(pending));
    else sessionStorage.removeItem(_pfTradeStorageKey());
  } catch { /* 저장소가 차단된 브라우저에서도 현재 화면의 요청 번호는 보존한다. */ }
}

function _pfTradeOptions(items) {
  _pfTrade.choices = [...new Map(items.filter(i => !i.code.startsWith('CASH_')).map(i => [i.code, i])).values()];
  _pfTradeEl('Stocks').innerHTML = _pfTrade.choices.map(i => `<option value="${escapeHtml(`${i.name} (${i.code})`)}"></option>`).join('');
}

function _pfTradeInvalidate() {
  if (_pfTrade.pending || _pfTrade.busy) return;
  _pfTrade.preview = null;
  _pfTrade.payload = null;
  _pfTradeEl('Preview').textContent = '';
  _pfTradeEl('Status').textContent = '';
  _pfTradeEl('Save').disabled = true;
  _pfTradeEl('Save').textContent = '매매 기록 저장';
}

function _pfTradeCurrency() {
  const selected = _pfTrade.selected;
  const holding = PfStore.items.find(i => i.stock_code === selected?.code);
  const currency = _pfTradeEl('Currency').value;
  const needsFx = _pfTradeEl('Side').value === 'buy' && holding?.quantity > 0 && (holding.avg_price_currency || 'KRW') !== currency;
  _pfTradeEl('FxLabel').hidden = !needsFx;
  _pfTradeEl('Fx').required = needsFx;
  _pfTradeEl('FxText').textContent = needsFx ? `평균 매입가 환산: 1 ${currency} = ? ${holding.avg_price_currency || 'KRW'} (체결 환율)` : '매입가 환산 환율';
  const cash = PfStore.items.find(i => i.stock_code === `CASH_${currency}`);
  _pfTradeEl('Selected').textContent = selected
    ? `${selected.name} · ${selected.code} / 보유 ${_pfTradeFmt(holding?.quantity || 0)} / 현금 ${_pfTradeFmt(cash?.quantity || 0)} ${currency}`
    : '검색 결과에서 종목을 선택하세요.';
}

function _pfTradeSelect(item) {
  _pfTrade.selected = item;
  _pfTradeEl('Stock').value = `${item.name} (${item.code})`;
  const holding = PfStore.items.find(i => i.stock_code === item.code);
  _pfTradeEl('Currency').value = holding?.currency || item.currency || 'KRW';
  _pfTradeEl('Currency').disabled = Boolean(holding);
  _pfTradeTaxDefaults();
  _pfTradeCurrency();
}

function _pfTradeTaxDefaults() {
  const domesticSale = _pfTradeEl('Side').value === 'sell' && /^[0-9][0-9A-Z]{5}$/.test(_pfTrade.selected?.code || '') && _pfTradeEl('Currency').value === 'KRW';
  _pfTradeEl('TaxRate').value = domesticSale ? '0.2' : '0';
  _pfTradeEl('TaxAmount').value = '';
  _pfTradeCosts();
}

function _pfTradeCosts() {
  const currency = _pfTradeEl('Currency').value;
  const scale = ['KRW', 'JPY', 'VND'].includes(currency) ? 1 : 100;
  const gross = Math.round(Number(_pfTradeEl('Quantity').value) * Number(_pfTradeEl('Price').value) * scale) / scale;
  const tax = _pfTradeEl('TaxAmount').value === '' ? Math.floor((gross * Number(_pfTradeEl('TaxRate').value) / 100) * scale + 1e-8) / scale : Number(_pfTradeEl('TaxAmount').value);
  const fees = Number(_pfTradeEl('Fees').value);
  _pfTradeEl('Costs').textContent = `수수료 ${_pfTradeFmt(fees)} + 세금 ${_pfTradeFmt(tax)} = 합계 ${_pfTradeFmt(fees + tax)} ${currency}`;
}

async function _pfTradeSearch() {
  const query = _pfTradeEl('Stock').value.trim();
  const version = ++_pfTrade.search;
  const exact = _pfTrade.choices.find(i => [i.code, i.name, `${i.name} (${i.code})`].some(v => v.toLowerCase() === query.toLowerCase()));
  _pfTrade.selected = null;
  _pfTradeEl('Currency').disabled = false;
  if (exact) { _pfTradeSelect(exact); return; }
  _pfTradeCurrency();
  if (!query) return;
  await new Promise(resolve => setTimeout(resolve, 250));
  if (version !== _pfTrade.search) return;
  const [domestic, overseas, direct] = await Promise.all([
    apiFetchJson(`/api/search?q=${encodeURIComponent(query)}`, { fallback: [] }),
    pfIsForeignSearchQuery(query) ? apiFetchJson(`/api/portfolio/search-foreign?q=${encodeURIComponent(query)}&limit=8`, { fallback: [] }) : [],
    /^[0-9][0-9A-Z]{5}$/i.test(query) ? apiFetchJson(`/api/portfolio/resolve-name?code=${encodeURIComponent(query)}`, { fallback: {} }) : {},
  ]);
  if (version !== _pfTrade.search || !_pfTradeEl('Dialog').open) return;
  const items = [
    ...PfStore.items.map(i => ({ code: i.stock_code, name: i.stock_name, currency: i.currency })),
    ...(Array.isArray(domestic) ? domestic : []).map(i => ({ code: i.stock_code, name: i.corp_name, currency: 'KRW' })),
    ...(Array.isArray(overseas) ? overseas : []).map(pfForeignSearchItem).filter(Boolean),
    ...pfMatchedSpecialAssets(query).map(i => ({ code: i.code, name: i.name, currency: 'KRW' })),
    ...(direct?.stock_name ? [{ code: direct.stock_code, name: direct.stock_name, currency: 'KRW' }] : []),
  ];
  _pfTradeOptions(items);
  const found = _pfTrade.choices.find(i => i.code.toLowerCase() === query.toLowerCase() || i.name.toLowerCase() === query.toLowerCase());
  if (found) _pfTradeSelect(found);
}

function _pfTradeRead() {
  if (!_pfTrade.selected) throw new Error('검색 결과에서 종목을 먼저 선택해 주세요.');
  return {
    stock_code: _pfTrade.selected.code, stock_name: _pfTrade.selected.saveName || _pfTrade.selected.name,
    side: _pfTradeEl('Side').value, currency: _pfTradeEl('Currency').value,
    quantity: _pfTradeEl('Quantity').value, price: _pfTradeEl('Price').value,
    fees: _pfTradeEl('Fees').value || '0', memo: _pfTradeEl('Memo').value.trim(),
    tax_rate: _pfTradeEl('TaxRate').value || '0', tax_amount: _pfTradeEl('TaxAmount').value || null,
    cost_fx_rate: _pfTradeEl('FxLabel').hidden ? null : _pfTradeEl('Fx').value || null,
  };
}

function _pfTradeRenderPreview(result) {
  const number = _pfTradeFmt;
  _pfTradeEl('Preview').innerHTML = `<strong>${escapeHtml(result.stock_name)} ${result.side === 'buy' ? '매수' : '매도'}</strong>
    <dl><dt>보유 수량</dt><dd>${number(result.quantity_before)} → ${number(result.quantity_after)}</dd>
    <dt>현금 (${escapeHtml(result.currency)})</dt><dd>${number(result.cash_before)} → ${number(result.cash_after)}</dd>
    <dt>체결 금액 / 비용</dt><dd>${number(result.gross_amount)} / ${number(result.fees)} ${escapeHtml(result.currency)}</dd>
    <dt>수수료 / 세금</dt><dd>${number(result.commission ?? result.fees)} / ${number(result.tax_amount || 0)} ${escapeHtml(result.currency)}</dd>
    <dt>평균 매입가</dt><dd>${number(result.avg_price_after)} ${escapeHtml(result.avg_price_currency)}</dd></dl>
    <p class="pf-trade-help">${result.quantity_after === 0 ? '전량 매도하면 보유종목 목록에서 제외합니다.' : '매수 비용은 평균 매입가에 포함하고, 부분 매도는 기존 평균 매입가를 유지합니다.'}</p>`;
}

async function pfPreviewTrade(event) {
  event?.preventDefault();
  if (_pfTrade.busy || _pfTrade.pending) return;
  _pfTradeInvalidate();
  try {
    const payload = _pfTradeRead();
    _pfTrade.busy = true;
    _pfTradeEl('Fields').disabled = true;
    const result = await apiFetchJson('/api/portfolio/trades/preview', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload),
      errorMessage: '매매 내용을 확인하지 못했습니다.',
    });
    _pfTrade.preview = result;
    _pfTrade.payload = payload;
    _pfTradeRenderPreview(result);
    _pfTradeEl('Save').disabled = false;
  } catch (error) {
    _pfTradeEl('Status').textContent = error.message;
  } finally {
    _pfTrade.busy = false;
    _pfTradeEl('Fields').disabled = false;
  }
}

async function pfLoadTrades() {
  try {
    const rows = await apiFetchJson('/api/portfolio/trades?limit=20', { errorMessage: '매매 내역을 불러오지 못했습니다.' });
    _pfTradeEl('History').innerHTML = rows.length ? rows.map(row => `<article><strong>${escapeHtml(row.stock_name)} · ${row.side === 'buy' ? '매수' : '매도'}</strong>
      <p>${_pfTradeFmt(row.quantity)} × ${_pfTradeFmt(row.price)} ${escapeHtml(row.currency)} · 비용 ${_pfTradeFmt(row.fees)}${row.commission !== null && row.commission !== undefined ? ` (수수료 ${_pfTradeFmt(row.commission)} + 세금 ${_pfTradeFmt(row.tax_amount || 0)})` : ''}</p>
      <small>${escapeHtml(new Date(row.created_at).toLocaleString('ko-KR'))}${row.memo ? ` · ${escapeHtml(row.memo)}` : ''}</small></article>`).join('') : '<p>아직 기록한 매매가 없습니다.</p>';
  } catch (error) { _pfTradeEl('History').textContent = error.message; }
}

async function pfSaveTrade() {
  if (_pfTrade.busy || (!_pfTrade.preview && !_pfTrade.pending)) return;
  if (!_pfTrade.pending) {
    if (JSON.stringify(_pfTradeRead()) !== JSON.stringify(_pfTrade.payload)) { _pfTradeInvalidate(); return; }
    _pfTradePersist({ ..._pfTrade.payload, request_id: crypto.randomUUID(), expected_revision: _pfTrade.preview.revision });
  }
  _pfTrade.busy = true;
  _pfTradeEl('Fields').disabled = true;
  _pfTradeEl('Save').disabled = true;
  _pfTradeEl('Status').textContent = '매매 내역을 저장하고 있습니다…';
  let saved;
  try {
    saved = await apiFetchJson('/api/portfolio/trades', {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(_pfTrade.pending),
      errorMessage: '저장 결과를 확인하지 못했습니다.',
    });
    if (!saved || !Number.isFinite(saved.quantity_after) || !Number.isFinite(saved.cash_after)) throw new Error('저장 응답을 확인하지 못했습니다.');
  } catch (error) {
    saved = null;
    if ([400, 401, 403, 409, 422].includes(error.status)) {
      _pfTradePersist(null);
      _pfTradeEl('Fields').disabled = false;
      _pfTrade.preview = null;
      _pfTradeEl('Preview').textContent = '';
      _pfTradeEl('Status').textContent = `${error.message} 변경 내용을 다시 확인해 주세요.`;
    } else {
      _pfTradeEl('Status').textContent = '응답을 확인하지 못했습니다. 같은 요청으로 저장 결과를 확인하면 중복 반영되지 않습니다.';
      _pfTradeEl('Save').textContent = '저장 결과 확인';
      _pfTradeEl('Save').disabled = false;
    }
  } finally { _pfTrade.busy = false; }
  if (!saved) return;
  _pfTradePersist(null);
  _pfTradeEl('Fields').disabled = false;
  _pfTradeEl('Form').reset();
  _pfTradeEl('Currency').disabled = false;
  _pfTrade.selected = null;
  _pfTradeTaxDefaults();
  _pfTrade.preview = null;
  _pfTrade.payload = null;
  _pfTradeEl('FxLabel').hidden = true;
  _pfTradeEl('Fx').required = false;
  _pfTradeEl('Save').textContent = '매매 기록 저장';
  _pfTradeEl('Selected').textContent = '다음 매매를 기록하려면 종목을 선택하세요.';
  _pfTradeRenderPreview(saved);
  _pfTradeEl('Status').textContent = '매매 내역과 현금·보유 수량을 저장했습니다.';
  PfStore.edit.code = null;
  // 저장 응답과 화면 갱신 오류를 분리해, 완료된 거래를 재입력하게 만들지 않는다.
  await Promise.allSettled([loadPortfolio({ force: true }), pfLoadTrades()]);
  if (typeof pfRefreshTodayState === 'function') await Promise.allSettled([pfRefreshTodayState({ force: true, render: true })]);
}

function pfOpenTrade(code) {
  if (_pfTrade.busy) return;
  _pfTrade.search += 1;
  _pfTradeEl('Form').reset();
  _pfTrade.selected = null;
  _pfTradeEl('Fields').disabled = false;
  _pfTradeEl('Currency').disabled = false;
  _pfTradeEl('Preview').textContent = '';
  _pfTradeEl('Save').textContent = '매매 기록 저장';
  try { _pfTrade.pending = JSON.parse(sessionStorage.getItem(_pfTradeStorageKey()) || 'null'); } catch { /* 현재 화면의 요청 보존 */ }
  if (_pfTrade.pending) {
    const p = _pfTrade.pending;
    _pfTradeSelect({ code: p.stock_code, name: p.stock_name, currency: p.currency });
    for (const [id, key] of [['Side', 'side'], ['Currency', 'currency'], ['Quantity', 'quantity'], ['Price', 'price'], ['Fees', 'fees'], ['Memo', 'memo'], ['Fx', 'cost_fx_rate'], ['TaxRate', 'tax_rate'], ['TaxAmount', 'tax_amount']]) _pfTradeEl(id).value = p[key] ?? '';
    _pfTradeCosts();
    _pfTradeCurrency();
    _pfTradeEl('Fields').disabled = true;
    _pfTradeEl('Save').disabled = false;
    _pfTradeEl('Save').textContent = '저장 결과 확인';
    _pfTradeEl('Status').textContent = '확인 중인 매매가 있습니다. 저장 결과를 먼저 확인해 주세요.';
  } else {
    _pfTradeInvalidate();
    _pfTradeOptions(PfStore.items.map(i => ({ code: i.stock_code, name: i.stock_name, currency: i.currency })));
    const selected = _pfTrade.choices.find(i => i.code === code);
    if (selected) _pfTradeSelect(selected);
    else _pfTradeCurrency();
    _pfTradeCosts();
  }
  if (!_pfTradeEl('Dialog').open) _pfTradeEl('Dialog').showModal();
  pfLoadTrades();
}

_pfTradeEl('Form').addEventListener('submit', pfPreviewTrade);
_pfTradeEl('Form').addEventListener('input', () => { _pfTradeInvalidate(); _pfTradeCosts(); });
_pfTradeEl('Stock').addEventListener('input', () => _pfTradeSearch().catch(error => { _pfTradeEl('Status').textContent = error.message; }));
_pfTradeEl('Side').addEventListener('change', () => { _pfTradeInvalidate(); _pfTradeCurrency(); _pfTradeTaxDefaults(); });
_pfTradeEl('Currency').addEventListener('change', () => { _pfTradeInvalidate(); _pfTradeCurrency(); _pfTradeTaxDefaults(); });
_pfTradeEl('Save').addEventListener('click', pfSaveTrade);
_pfTradeEl('Close').addEventListener('click', () => _pfTradeEl('Dialog').close());
