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
  const code = String(ticker || '').toUpperCase();
  if (code.endsWith('.T')) return 'JPY';
  if (code.endsWith('.HK')) return 'HKD';
  if (code.endsWith('.SS') || code.endsWith('.SZ')) return 'CNY';
  if (code.endsWith('.L')) return 'GBP';
  if (code.endsWith('.AX')) return 'AUD';
  if (code.endsWith('.TO')) return 'CAD';
  if (/\.(DE|F|PA|AS|MI|MC)$/.test(code)) return 'EUR';
  return 'USD';
}

async function pfFetchJson(path, fallback = [], options = {}) {
  try {
    const resp = await apiFetch(path, options);
    if (!resp.ok) return fallback;
    return await resp.json();
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
