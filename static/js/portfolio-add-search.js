// Portfolio add-box search helpers: special assets, direct foreign tickers,
// and fast foreign-search result normalization.

const PF_SPECIAL_ASSETS = [
  { code: 'KRX_GOLD', name: 'KRX 금현물', keywords: ['금', '금현물', 'krx금', 'krx_gold', 'gold'] },
  { code: 'CRYPTO_BTC', name: '비트코인', keywords: ['btc', '비트코인', 'bitcoin'] },
  { code: 'CRYPTO_ETH', name: '이더리움', keywords: ['eth', '이더리움', 'ethereum'] },
  { code: 'CRYPTO_USDT', name: '테더', keywords: ['usdt', '테더', 'tether'] },
  { code: 'CASH_KRW', name: '원화', keywords: ['krw', '원화', '현금', '원'] },
  { code: 'CASH_USD', name: '미국 달러', keywords: ['usd', '달러', '미국달러', 'dollar'] },
  { code: 'CASH_EUR', name: '유로', keywords: ['eur', '유로', 'euro'] },
  { code: 'CASH_JPY', name: '일본 엔', keywords: ['jpy', '엔', '일본엔', 'yen'] },
  { code: 'CASH_CNY', name: '중국 위안', keywords: ['cny', '위안', '중국위안', 'yuan'] },
  { code: 'CASH_HKD', name: '홍콩 달러', keywords: ['hkd', '홍콩달러'] },
  { code: 'CASH_GBP', name: '영국 파운드', keywords: ['gbp', '파운드', 'pound'] },
  { code: 'CASH_AUD', name: '호주 달러', keywords: ['aud', '호주달러'] },
  { code: 'CASH_CAD', name: '캐나다 달러', keywords: ['cad', '캐나다달러'] },
  { code: 'CASH_CHF', name: '스위스 프랑', keywords: ['chf', '프랑', '스위스프랑'] },
  { code: 'CASH_VND', name: '베트남 동', keywords: ['vnd', '베트남동', '동'] },
  { code: 'CASH_TWD', name: '대만 달러', keywords: ['twd', '대만달러'] },
];

function pfMatchedSpecialAssets(raw) {
  const qLower = String(raw || '').trim().toLowerCase();
  if (!qLower) return [];
  return PF_SPECIAL_ASSETS.filter(a =>
    a.code.toLowerCase() === qLower || a.keywords.some(k => qLower.includes(k))
  );
}

function pfIsForeignSearchQuery(raw) {
  const text = String(raw || '').trim();
  return /[A-Za-z]/.test(text) || /[.\-/]/.test(text);
}

function pfCanonicalDirectTicker(raw) {
  const text = String(raw || '').trim();
  if (!text || text.length > 24 || /\s/.test(text) || !/^[A-Za-z0-9][A-Za-z0-9./-]*$/.test(text)) return '';
  if (text !== text.toUpperCase() && !/[.\-/]/.test(text)) return '';
  let ticker = text.toUpperCase().replace(/\//g, '-');
  if (ticker.includes('.')) {
    const parts = ticker.split('.');
    const suffix = parts[parts.length - 1];
    const prefix = parts.slice(0, -1).join('.');
    if (suffix.length === 1 && /^[A-Z.]+$/.test(prefix)) ticker = `${prefix}-${suffix}`;
  }
  return ticker;
}

function pfInferTickerCurrency(ticker) {
  const code = String(ticker || '').trim().toUpperCase();
  if (code.endsWith('.T')) return 'JPY';
  if (code.endsWith('.HK')) return /^8\d{4}\.HK$/.test(code) ? 'CNY' : 'HKD';
  if (code.endsWith('.SS') || code.endsWith('.SZ')) return 'CNY';
  if (code.endsWith('.L')) return 'GBP';
  if (code.endsWith('.AX')) return 'AUD';
  if (code.endsWith('.TO')) return 'CAD';
  if (/\.(DE|F|PA|AS|MI|MC)$/.test(code)) return 'EUR';
  return 'USD';
}

async function pfFetchJson(path, fallback = [], options = {}) {
  try {
    return await apiFetchJson(path, { ...(options || {}), fallback });
  } catch {
    return fallback;
  }
}

function pfForeignSearchItem(item) {
  const code = String(item?.stock_code || item?.ticker || '').trim();
  if (!code) return null;
  const name = String(item?.stock_name || code).trim();
  const exchange = String(item?.exchange || '').trim();
  return {
    code,
    name: exchange ? `${name} · ${exchange}` : name,
    saveName: name,
    currency: String(item?.currency || '').trim(),
  };
}

function pfRenderAddDropdown(dropdown, items) {
  const deduped = [];
  const seen = new Set();
  for (const item of items) {
    if (!item || !item.code || seen.has(item.code)) continue;
    seen.add(item.code);
    deduped.push(item);
  }
  if (!deduped.length) { dropdown.classList.remove('show'); return; }
  dropdown.innerHTML = deduped.map(r =>
    `<div class="dropdown-item" data-code="${escapeHtml(r.code)}" data-name="${escapeHtml(r.saveName || r.name)}" data-currency="${escapeHtml(r.currency || '')}">${escapeHtml(r.name)} <span style="color:var(--text-secondary)">${escapeHtml(r.code)}</span></div>`
  ).join('');
  dropdown.classList.add('show');
  dropdown.querySelectorAll('.dropdown-item').forEach(el => {
    el.addEventListener('click', () => pfAddFromSearch(el.dataset.code, el.dataset.name, el.dataset.currency || ''));
  });
}

function pfApplySavedPortfolioItem(saved, fallbackCode, fallbackName, fallbackCurrency) {
  const stockCode = String(saved?.stock_code || fallbackCode || '').trim();
  if (!stockCode) return;
  const currency = String(saved?.currency || fallbackCurrency || 'KRW').trim().toUpperCase();
  const item = {
    stock_code: stockCode,
    stock_name: String(saved?.stock_name || fallbackName || stockCode).trim(),
    quantity: Number(saved?.quantity ?? 1),
    avg_price: Number(saved?.avg_price ?? 0),
    avg_price_currency: String(saved?.avg_price_currency || 'KRW').trim().toUpperCase(),
    avg_price_krw: Number(saved?.avg_price_krw ?? saved?.avg_price ?? 0),
    currency,
    group_name: saved?.group_name || null,
    benchmark_code: saved?.benchmark_code || null,
    created_at: saved?.created_at || new Date().toISOString(),
    target_price: saved?.target_price ?? null,
    target_price_disabled: saved?.target_price_disabled ?? 0,
    target_price_formula: saved?.target_price_formula ?? null,
    target_metrics: {},
    trailing_dps: null,
    tags: [],
    quote: stockCode === 'CASH_KRW' ? { price: 1, change: 0, change_pct: 0 } : {},
  };
  const idx = PfStore.items.findIndex(i => i.stock_code === stockCode);
  if (idx >= 0) {
    PfStore.items[idx] = { ...PfStore.items[idx], ...item };
  } else {
    PfStore.items = [item, ...PfStore.items];
  }
  if (typeof _savePortfolioSnapshot === 'function') _savePortfolioSnapshot(PfStore.items);
  renderPortfolio();
  if (typeof _updateQuoteSubscriptions === 'function') _updateQuoteSubscriptions();
}

// 초안은 입력 창 안에서만 유지하고, 확정 전에는 보유 목록과 캐시에 넣지 않는다.
const _pfRegistration = { item: null, saving: false };
const _pfRegisterEl = id => document.getElementById(`pfRegister${id}`);

function pfOpenInitialRegistration(item) {
  if (_pfRegistration.saving) return;
  _pfRegistration.item = item;
  _pfRegisterEl('Form').reset();
  _pfRegisterEl('Selected').textContent = `${item.name} · ${item.code} · ${item.currency}`;
  _pfRegisterEl('Name').value = item.name;
  _pfRegisterEl('QuantityLabel').textContent = item.code.startsWith('CASH_') ? `현금 잔고 (${item.currency})` : '보유 수량';
  _pfRegisterEl('Price').value = item.code === 'CASH_KRW' ? '1' : '0';
  const canChooseCurrency = pfCanEditAvgPriceCurrency(item.code);
  _pfRegisterEl('PriceCurrency').innerHTML = pfAvgPriceCurrencyOptions(canChooseCurrency ? item.currency : 'KRW');
  _pfRegisterEl('PriceCurrency').disabled = !canChooseCurrency;
  _pfRegisterEl('Status').textContent = '';
  if (!_pfRegisterEl('Dialog').open) _pfRegisterEl('Dialog').showModal();
  _pfRegisterEl('Quantity').focus();
}

async function pfSaveInitialRegistration(event) {
  event?.preventDefault();
  const item = _pfRegistration.item;
  if (!item || _pfRegistration.saving || !_pfRegisterEl('Form').reportValidity()) return;
  const quantity = Number(_pfRegisterEl('Quantity').value);
  const avgPrice = Number(_pfRegisterEl('Price').value);
  const name = _pfRegisterEl('Name').value.trim();
  if (!name || !Number.isFinite(quantity) || quantity === 0 || !Number.isFinite(avgPrice) || avgPrice < 0) {
    _pfRegisterEl('Status').textContent = '종목명, 0이 아닌 수량, 0 이상의 매입가를 입력해 주세요.';
    return;
  }
  if (PfStore.items.some(i => i.stock_code === item.code)) {
    _pfRegisterEl('Status').textContent = '이미 등록된 종목입니다. 창을 닫고 보유 수량을 확인해 주세요.';
    return;
  }
  const body = { stock_name: name, quantity, avg_price: avgPrice, currency: item.currency,
    avg_price_currency: _pfRegisterEl('PriceCurrency').value };
  if (_pfRegisterEl('Date').value) body.created_at = _pfRegisterEl('Date').value;
  _pfRegistration.saving = true;
  for (const id of ['Fields', 'Save', 'Close', 'Cancel']) _pfRegisterEl(id).disabled = true;
  _pfRegisterEl('Status').textContent = '초기 잔고를 등록하고 있습니다…';
  let saved;
  try {
    saved = await apiFetchJson(`/api/portfolio/${encodeURIComponent(item.code)}`, {
      method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
      errorMessage: '초기 등록에 실패했습니다.',
    });
  } catch (error) {
    _pfRegisterEl('Status').textContent = error.message;
    return;
  } finally {
    _pfRegistration.saving = false;
    for (const id of ['Fields', 'Save', 'Close', 'Cancel']) _pfRegisterEl(id).disabled = false;
  }
  _pfRegisterEl('Dialog').close();
  pfApplySavedPortfolioItem(saved, item.code, name, item.currency);
  showToast('초기 잔고를 등록했습니다.');
  loadPortfolio({ force: true }).catch(e => reportApiError(e, '포트폴리오 동기화', { silent: true }));
}

function pfInitInitialRegistration() {
  const dialog = _pfRegisterEl('Dialog');
  if (!dialog) return;
  _pfRegisterEl('Form').addEventListener('submit', pfSaveInitialRegistration);
  for (const id of ['Close', 'Cancel']) _pfRegisterEl(id).addEventListener('click', () => {
    if (!_pfRegistration.saving) dialog.close();
  });
  dialog.addEventListener('cancel', event => { if (_pfRegistration.saving) event.preventDefault(); });
  dialog.addEventListener('close', () => { _pfRegistration.item = null; });
}

const _pfRemoval = { item: null, busy: false };
const _pfRemoveEl = id => document.getElementById(`pfRemove${id}`);

function pfOpenHoldingRemoval(item) {
  if (_pfRemoval.busy) return;
  _pfRemoval.item = item;
  _pfRemoveEl('Selected').textContent = `${item.stock_name} (${item.stock_code}) · 보유 ${Number(item.quantity).toLocaleString('ko-KR')}`;
  const canSell = item.quantity > 0 && !/^(CASH_|FUT_|SHORT_|IDX_)/.test(item.stock_code)
    && !item.pair_long_code && !PfStore.items.some(i => i.pair_long_code === item.stock_code);
  _pfRemoveEl('Sell').hidden = _pfRemoveEl('SellHelp').hidden = !canSell;
  _pfRemoveEl('Status').textContent = '';
  if (!_pfRemoveEl('Dialog').open) _pfRemoveEl('Dialog').showModal();
}

async function pfSellHoldingRemoval() {
  const item = _pfRemoval.item;
  if (!item || _pfRemoval.busy) return;
  _pfRemoval.busy = true;
  _pfRemoveEl('Sell').disabled = _pfRemoveEl('Delete').disabled = true;
  try {
    await loadFeatureScripts('trades');
    if (!_pfRemoveEl('Dialog').open || _pfRemoval.item !== item) return;
    _pfRemoveEl('Dialog').close();
    pfOpenTrade(item.stock_code, null, { side: 'sell', quantity: item.quantity });
  } catch (error) { _pfRemoveEl('Status').textContent = error.message; }
  finally {
    _pfRemoval.busy = false;
    _pfRemoveEl('Sell').disabled = _pfRemoveEl('Delete').disabled = false;
  }
}

async function pfDeleteHoldingRegistration() {
  const item = _pfRemoval.item;
  if (!item || _pfRemoval.busy) return;
  _pfRemoval.busy = true;
  for (const id of ['Sell', 'Delete', 'Close']) _pfRemoveEl(id).disabled = true;
  _pfRemoveEl('Status').textContent = '등록 항목을 삭제하고 있습니다…';
  try {
    await apiFetchJson(`/api/portfolio/${encodeURIComponent(item.stock_code)}`, {
      method: 'DELETE', errorMessage: '등록 삭제에 실패했습니다.',
    });
  } catch (error) {
    _pfRemoveEl('Status').textContent = error.message;
    return;
  } finally {
    _pfRemoval.busy = false;
    for (const id of ['Sell', 'Delete', 'Close']) _pfRemoveEl(id).disabled = false;
  }
  _pfRemoveEl('Dialog').close();
  PfStore.items = PfStore.items.filter(i => i.stock_code !== item.stock_code);
  if (PfStore.edit.code === item.stock_code) PfStore.edit.code = null;
  renderPortfolio();
  loadPortfolio({ force: true }).catch(e => reportApiError(e, '포트폴리오 동기화', { silent: true }));
}

function pfInitHoldingRemoval() {
  const dialog = _pfRemoveEl('Dialog');
  if (!dialog) return;
  _pfRemoveEl('Sell').addEventListener('click', pfSellHoldingRemoval);
  _pfRemoveEl('Delete').addEventListener('click', pfDeleteHoldingRegistration);
  _pfRemoveEl('Close').addEventListener('click', () => dialog.close());
  dialog.addEventListener('cancel', event => { if (_pfRemoveEl('Close').disabled) event.preventDefault(); });
  dialog.addEventListener('close', () => { _pfRemoval.item = null; });
}

if (typeof window !== 'undefined') {
  Object.assign(window, {
    pfMatchedSpecialAssets,
    pfIsForeignSearchQuery,
    pfCanonicalDirectTicker,
    pfInferTickerCurrency,
    pfFetchJson,
    pfForeignSearchItem,
    pfRenderAddDropdown,
    pfApplySavedPortfolioItem,
  });
}
