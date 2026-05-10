// Portfolio row actions: drag/drop, group/benchmark edits, CRUD, search, target/link actions.
// Split from static/js/portfolio.js to keep portfolio features maintainable.
async function pfDropRow(fromCode, toCode) {
  const fromIdx = portfolioItems.findIndex(i => i.stock_code === fromCode);
  const toIdx = portfolioItems.findIndex(i => i.stock_code === toCode);
  if (fromIdx < 0 || toIdx < 0) return;
  const next = portfolioItems.slice();
  const [moved] = next.splice(fromIdx, 1);
  next.splice(toIdx, 0, moved);
  portfolioItems = next;
  renderPortfolio();
  try {
    await apiFetch('/api/portfolio/order', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ stock_codes: next.map(i => i.stock_code) }),
    });
  } catch (e) {
    await loadPortfolio();
  }
}

async function pfChangeGroup(stockCode, groupName) {
  const item = portfolioItems.find(i => i.stock_code === stockCode);
  if (!item) return;
  try {
    const resp = await apiFetch(`/api/portfolio/${encodeURIComponent(stockCode)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        stock_name: item.stock_name,
        quantity: item.quantity,
        avg_price: item.avg_price,
        group_name: groupName,
      }),
    });
    if (!resp.ok) throw new Error('그룹 변경 실패');
    item.group_name = groupName;
    renderPortfolio();
  } catch (e) { showToast(e.message); }
}

function pfShowBenchmarkPicker(stockCode, td) {
  // Close any existing picker
  document.querySelectorAll('.pf-benchmark-picker').forEach(el => el.remove());
  if (pfSavingEditCode) return;
  const item = portfolioItems.find(i => i.stock_code === stockCode);
  if (!item) return;
  const picker = document.createElement('div');
  picker.className = 'pf-benchmark-picker';
  picker.dataset.code = stockCode;
  const presets = _BENCHMARK_PRESETS.map(p =>
    `<div class="pf-bm-option js-pf-bench-set${item.benchmark_code === p.code ? ' selected' : ''}" data-bench="${escapeHtml(p.code)}">${escapeHtml(p.name)}</div>`
  ).join('');
  picker.innerHTML = `
    ${presets}
    <div class="pf-bm-custom">
      <input class="pf-bm-input" placeholder="종목코드">
    </div>
    <div class="pf-bm-option pf-bm-reset js-pf-bench-set" data-bench="">기본값으로</div>
  `;
  td.style.position = 'relative';
  td.appendChild(picker);
  const input = picker.querySelector('.pf-bm-input');
  if (input) {
    input.focus();
    // Listener scoped to this picker instance — removed with the node when
    // picker is closed, so no global accumulation.
    input.addEventListener('keydown', (ev) => {
      if (ev.key === 'Enter') pfSetBenchmark(stockCode, input.value);
    });
  }
  // Close on outside click. Use { once: true } isn't safe because the
  // first click may be inside; instead remove explicitly when closed.
  setTimeout(() => {
    const close = (e) => {
      if (!picker.contains(e.target)) { picker.remove(); document.removeEventListener('click', close); }
    };
    document.addEventListener('click', close);
  }, 0);
}

async function pfSetBenchmark(stockCode, benchmarkCode) {
  document.querySelectorAll('.pf-benchmark-picker').forEach(el => el.remove());
  if (pfSavingEditCode) return;
  if (pfEditingCode !== stockCode) {
    showToast('벤치마크는 수정모드에서 변경할 수 있습니다.');
    return;
  }
  const item = portfolioItems.find(i => i.stock_code === stockCode);
  if (!item) return;
  try {
    const resp = await apiFetch(`/api/portfolio/${encodeURIComponent(stockCode)}/benchmark`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ benchmark_code: benchmarkCode || null }),
    });
    if (!resp.ok) throw new Error('벤치마크 변경 실패');
    const data = await resp.json();
    item.benchmark_code = data.effective_benchmark;
    if (data.benchmark_quote || data.benchmark_name) {
      pfBenchmarkQuotes[data.effective_benchmark] = {
        ...(pfBenchmarkQuotes[data.effective_benchmark] || {}),
        ...data.benchmark_quote,
        name: data.benchmark_name || data.effective_benchmark,
      };
    }
    renderPortfolio();
  } catch (e) { showToast(e.message); }
}

function startPortfolioEdit(stockCode) {
  if (pfSavingEditCode) return;
  pfEditingCode = stockCode;
  renderPortfolio();
  const priceInput = document.getElementById('pfEditPrice');
  if (priceInput) priceInput.focus();
}

function cancelPortfolioEdit() {
  if (pfSavingEditCode) return;
  pfEditingCode = null;
  renderPortfolio();
}

function _pfFindEditRow(stockCode, row) {
  if (row && row.dataset && row.dataset.code === stockCode) return row;
  const tbody = document.getElementById('pfBody');
  if (!tbody) return null;
  for (const tr of tbody.querySelectorAll('tr[data-code]')) {
    if (tr.dataset.code === stockCode) return tr;
  }
  return null;
}

function _pfSetEditSaving(stockCode, saving, row) {
  pfSavingEditCode = saving ? stockCode : null;
  const editRow = _pfFindEditRow(stockCode, row);
  if (!editRow) return;
  editRow.classList.toggle('pf-row-saving', !!saving);
  editRow.setAttribute('aria-busy', saving ? 'true' : 'false');
  editRow.querySelectorAll('input, select, button').forEach(el => {
    if (el.classList.contains('js-pf-cancel')) {
      el.disabled = !!saving;
      return;
    }
    if (el.classList.contains('js-pf-save')) {
      el.disabled = !!saving;
      el.innerHTML = saving
        ? '<span class="pf-save-spinner" aria-hidden="true"></span><span class="pf-save-label">저장중</span>'
        : '✓';
      el.title = saving ? '저장 중입니다' : '저장';
      return;
    }
    el.disabled = !!saving;
  });
}

async function savePortfolioEdit(stockCode, stockName, row) {
  if (pfSavingEditCode) return;
  const editRow = _pfFindEditRow(stockCode, row);
  const qtyEl = editRow?.querySelector('.js-pf-edit-qty') || document.getElementById('pfEditQty');
  const priceEl = editRow?.querySelector('.js-pf-edit-price') || document.getElementById('pfEditPrice');
  if (!editRow || !qtyEl || !priceEl) {
    showToast('편집 행을 찾지 못했습니다. 다시 수정해 주세요.');
    return;
  }
  const qty = Number(qtyEl.value);
  const price = Number(priceEl.value);
  if (!Number.isFinite(qty) || qty === 0 || !Number.isFinite(price) || price < 0) {
    showToast('수량과 매입가를 올바르게 입력해 주세요.');
    return;
  }
  // When called from the delegated handler the name is looked up locally
  // rather than smuggled through the DOM as a JS-string literal.
  if (stockName === undefined) {
    const existing = portfolioItems.find(i => i.stock_code === stockCode);
    stockName = existing ? existing.stock_name : '';
  }
  // 등록일자는 optional — 비워두면 서버가 기존 값 유지. Input[type=date]
  // 는 YYYY-MM-DD 또는 빈 문자열을 돌려주므로 그대로 전달.
  const createdAtEl = editRow.querySelector('.js-pf-edit-created-at') || document.getElementById('pfEditCreatedAt');
  const createdAt = createdAtEl ? createdAtEl.value.trim() : '';
  const body = { stock_name: stockName, quantity: qty, avg_price: price };
  if (createdAt) body.created_at = createdAt;
  // 목표가 input — 비워두면 자동 계산으로 되돌리고, 숫자 또는
  // BPS/EPS/DPS/보유지분/본주가격/매입가 기반 수식을 그대로 서버에 보낸다.
  const tgtEl = editRow.querySelector('.js-pf-edit-target') || document.getElementById('pfEditTarget');
  if (tgtEl) {
    const tgtRaw = tgtEl.value.trim();
    const numericTarget = tgtRaw.replace(/,/g, '');
    if (tgtRaw && /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/.test(numericTarget)) {
      body.target_price = Number(numericTarget);
    } else {
      body.target_price_formula = tgtRaw;
    }
  }
  _pfSetEditSaving(stockCode, true, editRow);
  try {
    const resp = await apiFetch(`/api/portfolio/${encodeURIComponent(stockCode)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!resp.ok) {
      const d = await resp.json().catch(() => ({}));
      throw new Error(d.detail || '저장 실패');
    }
    const data = await resp.json().catch(() => ({}));
    // Update local item without full reload
    const item = portfolioItems.find(i => i.stock_code === stockCode);
    if (item) {
      item.quantity = qty;
      item.avg_price = price;
      item.stock_name = stockName;
      // Server may have normalized or kept created_at — trust its echo.
      if (data.created_at) item.created_at = data.created_at;
      // target_price 도 server 응답을 trust — null/숫자 그대로.
      if ('target_price' in data) item.target_price = data.target_price;
      if ('target_price_disabled' in data) item.target_price_disabled = !!data.target_price_disabled;
      if ('target_price_formula' in data) item.target_price_formula = data.target_price_formula;
    }
    pfSavingEditCode = null;
    pfEditingCode = null;
    renderPortfolio();
  } catch (e) {
    _pfSetEditSaving(stockCode, false, editRow);
    showToast(e.message);
  }
}

// × 버튼 핸들러 — 목표가를 '명시적으로 비움' 상태로 만든다. DB 에
// target_price_disabled=1, target_price=NULL 을 저장. 자동 계산도
// bypass 되어 UI 는 '-' 로 고정. 다시 표시하려면 사용자가 직접 숫자를
// 입력하면 disabled 플래그가 자동 해제된다.
async function clearPortfolioTargetPrice(stockCode) {
  const item = portfolioItems.find(i => i.stock_code === stockCode);
  if (!item) return;
  // 이미 disabled 면 중복 요청 불필요.
  if (item.target_price_disabled) {
    const tgtEl = document.getElementById('pfEditTarget');
    if (tgtEl) tgtEl.value = '';
    return;
  }
  try {
    const resp = await apiFetch(`/api/portfolio/${encodeURIComponent(stockCode)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        stock_name: item.stock_name,
        quantity: item.quantity,
        avg_price: item.avg_price,
        target_price: null,
        target_price_formula: null,
        target_price_disabled: true,
      }),
    });
    if (!resp.ok) {
      const d = await resp.json().catch(() => ({}));
      throw new Error(d.detail || '목표가 초기화 실패');
    }
    const data = await resp.json().catch(() => ({}));
    if ('target_price' in data) item.target_price = data.target_price;
    else item.target_price = null;
    if ('target_price_disabled' in data) item.target_price_disabled = !!data.target_price_disabled;
    else item.target_price_disabled = 1;
    if ('target_price_formula' in data) item.target_price_formula = data.target_price_formula;
    else item.target_price_formula = null;
    // 편집 모드면 input 도 즉시 비우기.
    const tgtEl = document.getElementById('pfEditTarget');
    if (tgtEl) tgtEl.value = '';
    renderPortfolio();
    showToast('목표가를 비웠습니다. (- 로 표시)', 'success');
  } catch (e) {
    showToast(e.message);
  }
}

async function deletePortfolioItem(stockCode) {
  // Other destructive actions in this file (group delete, cashflow delete,
  // CSV replace) all confirm first; this one was the outlier, so a
  // misclick on the ✕ in a dense table silently wiped a holding. Look up
  // the display name so the operator sees which stock they're about to
  // remove, not just an opaque code.
  const item = portfolioItems.find(i => i.stock_code === stockCode);
  const displayName = item && item.stock_name
    ? `${item.stock_name} (${stockCode})`
    : stockCode;
  if (!confirm(`"${displayName}" 를 포트폴리오에서 삭제할까요?`)) return;
  try {
    const resp = await apiFetch(`/api/portfolio/${encodeURIComponent(stockCode)}`, { method: 'DELETE' });
    if (!resp.ok) {
      const data = await resp.json().catch(() => ({}));
      throw new Error(data.detail || '삭제 실패');
    }
    portfolioItems = portfolioItems.filter(i => i.stock_code !== stockCode);
    renderPortfolio();
    await loadPortfolio();
  } catch (e) { showToast(e.message); }
}

// Portfolio add - search
(function initPfSearch() {
  document.addEventListener('DOMContentLoaded', () => {
    const input = document.getElementById('pfAddInput');
    const dropdown = document.getElementById('pfDropdown');
    if (!input || !dropdown) return;

    input.addEventListener('input', () => {
      clearTimeout(pfSearchTimeout);
      const raw = input.value.trim();
      if (raw.length < 1) { dropdown.classList.remove('show'); return; }

      const q = raw.replace(/(우[A-Z0-9]?|우)$/, '').trim() || raw;
      const wantPref = q !== raw;

      pfSearchTimeout = setTimeout(async () => {
        try {
          // Special asset matching
          const specialAssets = [
            { code: 'KRX_GOLD', name: 'KRX 금현물', keywords: ['금', '금현물', 'krx금', 'krx_gold', 'gold'] },
            { code: 'CRYPTO_BTC', name: '비트코인', keywords: ['btc', '비트코인', 'bitcoin'] },
            { code: 'CRYPTO_ETH', name: '이더리움', keywords: ['eth', '이더리움', 'ethereum'] },
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
          const qLower = raw.toLowerCase();
          const matchedSpecial = specialAssets.filter(a => a.keywords.some(k => qLower.includes(k)) || a.code.toLowerCase() === qLower);

          const resp = await apiFetch(`/api/search?q=${encodeURIComponent(q)}`);
          const results = await resp.json();
          if (!results.length && !matchedSpecial.length) {
            // No domestic results — try as foreign ticker
            if (/^[A-Z0-9]/i.test(raw) && /[A-Z]/i.test(raw)) {
              const r2 = await apiFetch(`/api/portfolio/resolve-name?code=${encodeURIComponent(raw.trim())}`);
              const d = await r2.json();
              if (d.stock_name) {
                const resolvedCode = d.stock_code || raw.trim();
                dropdown.innerHTML = `<div class="dropdown-item" data-code="${resolvedCode}" data-name="${escapeHtml(d.stock_name)}">${escapeHtml(d.stock_name)} <span style="color:var(--text-secondary)">${resolvedCode}</span></div>`;
                dropdown.classList.add('show');
                dropdown.querySelectorAll('.dropdown-item').forEach(el => {
                  el.addEventListener('click', () => pfAddFromSearch(el.dataset.code, el.dataset.name));
                });
                return;
              }
            }
            dropdown.classList.remove('show'); return;
          }

          let items;
          if (wantPref) {
            // Resolve preferred stock names from backend
            const prefCodes = results.flatMap(r => {
              const base = r.stock_code;
              if (!base.endsWith('0')) return [];
              return ['5','7','8','9','K','L'].map(s => base.slice(0,-1) + s);
            });
            const resolved = await Promise.all(
              prefCodes.map(async c => {
                try {
                  const r2 = await apiFetch(`/api/portfolio/resolve-name?code=${c}`);
                  const d = await r2.json();
                  return d.stock_name ? { code: c, name: d.stock_name } : null;
                } catch { return null; }
              })
            );
            items = resolved.filter(Boolean);
          } else {
            items = results.map(r => ({ code: r.stock_code, name: r.corp_name }));
          }

          // Prepend matched special assets
          const specialItems = matchedSpecial.map(a => ({ code: a.code, name: a.name }));
          items = [...specialItems, ...items.filter(i => !specialItems.some(s => s.code === i.code))];
          if (!items.length) { dropdown.classList.remove('show'); return; }
          dropdown.innerHTML = items.map(r =>
            `<div class="dropdown-item" data-code="${r.code}" data-name="${escapeHtml(r.name)}">${escapeHtml(r.name)} <span style="color:var(--text-secondary)">${r.code}</span></div>`
          ).join('');
          dropdown.classList.add('show');
          dropdown.querySelectorAll('.dropdown-item').forEach(el => {
            el.addEventListener('click', () => pfAddFromSearch(el.dataset.code, el.dataset.name));
          });
        } catch (e) { console.warn(e); }
      }, 200);
    });

    // Submit the current input — shared by the Enter key and the
    // explicit "등록" button. Resolves the typed text to a canonical
    // stock_code via the backend (so typing "삼성전자" works just as
    // well as "005930") and then falls through to pfAddFromSearch.
    const submitAdd = async () => {
      dropdown.classList.remove('show');
      const q = input.value.trim();
      if (!q) return;
      const resp = await apiFetch(`/api/portfolio/resolve-name?code=${encodeURIComponent(q)}`);
      const data = await resp.json();
      const resolvedCode = data.stock_code || q;
      pfAddFromSearch(resolvedCode, data.stock_name || q);
    };

    input.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        e.preventDefault();
        submitAdd();
      }
    });

    const addBtn = document.getElementById('pfAddBtn');
    if (addBtn) {
      addBtn.addEventListener('click', submitAdd);
    }

    document.addEventListener('click', (e) => {
      if (!input.contains(e.target) && !dropdown.contains(e.target)) dropdown.classList.remove('show');
    });
  });
})();

async function pfAddFromSearch(code, name) {
  document.getElementById('pfDropdown').classList.remove('show');
  document.getElementById('pfAddInput').value = '';
  let resolvedCode = String(code || '').trim();
  let resolvedName = String(name || '').trim();
  try {
    const r = await apiFetch(`/api/portfolio/resolve-name?code=${encodeURIComponent(resolvedCode)}`);
    const d = await r.json();
    if (d.stock_code) resolvedCode = d.stock_code;
    if (d.stock_name) resolvedName = d.stock_name;
  } catch (e) {
    console.warn('portfolio code canonicalization failed', e);
  }
  const existing = portfolioItems.find(i => i.stock_code === resolvedCode);
  if (existing) {
    startPortfolioEdit(resolvedCode);
    return;
  }
  try {
    // Save the canonical code so aliases like KCC cannot create a foreign ticker row.
    const resp = await apiFetch(`/api/portfolio/${encodeURIComponent(resolvedCode)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ stock_name: resolvedName, quantity: 1, avg_price: 0 }),
    });
    if (!resp.ok) {
      const d = await resp.json().catch(() => ({}));
      throw new Error(d.detail || '추가 실패');
    }
    pfEditingCode = resolvedCode;
    await loadPortfolio();
  } catch (e) { showToast(e.message); }
}

let _PREFERRED_PAIR_BY_CODE = {};

function _normalizePortfolioCode(code) {
  return typeof code === 'string' ? code.trim().toUpperCase() : '';
}

function _isPreferredStock(code) {
  const normalized = _normalizePortfolioCode(code);
  return Boolean(_PREFERRED_PAIR_BY_CODE[normalized]) || /^[0-9]{5}[1-9A-Z]$/.test(normalized);
}

// 외부 quote 캐시 — 포트폴리오에 없지만 목표가 계산에 필요한 종목들
// (우선주의 보통주, 지주사의 자회사) quote 를 별도로 받아 보관. price
// 는 /api/asset-quotes 가 KRW 환산해서 돌려주므로 단위 일치.
let _EXTERNAL_QUOTE_CACHE = {};   // code → { price, ts }
const _EXTERNAL_QUOTE_TTL = 60 * 1000;
let _externalFetchInflight = false;

async function _ensureExternalQuotes(codes) {
  // 인자: 임의 종목 코드 리스트. 캐시에 없거나 stale 한 것만 fetch.
  // inflight guard 로 같은 렌더 사이클의 중복 호출 방지.
  if (_externalFetchInflight) return;
  const needed = new Set();
  const now = Date.now();
  for (const code of codes) {
    if (!code) continue;
    const cached = _EXTERNAL_QUOTE_CACHE[code];
    if (!cached || (now - cached.ts) > _EXTERNAL_QUOTE_TTL) {
      needed.add(code);
    }
  }
  if (!needed.size) return;
  _externalFetchInflight = true;
  try {
    const resp = await apiFetch('/api/asset-quotes', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ codes: [...needed] }),
    });
    if (!resp.ok) return;
    const data = await resp.json();
    for (const [code, q] of Object.entries(data)) {
      if (q && q.price != null) {
        _EXTERNAL_QUOTE_CACHE[code] = { price: Number(q.price), ts: Date.now() };
      }
    }
    // 새 quote 도착 → 의존 row (우선주 / 지주사) 의 목표가 다시 그리기
    if (typeof renderPortfolio === 'function') renderPortfolio();
  } catch (e) {
    console.warn('external quote fetch failed', e);
  } finally {
    _externalFetchInflight = false;
  }
}

function _preferredCommonPriceForItem(item, allItems) {
  const commonCode = _preferredCommonCodeFor(item.stock_code);
  const commonItem = allItems.find(i => i.stock_code === commonCode);
  const commonPrice = commonItem?.quote?.price;
  if (commonPrice != null) return Number(commonPrice);
  const cached = _EXTERNAL_QUOTE_CACHE[commonCode];
  return cached && cached.price != null ? Number(cached.price) : null;
}

function _holdingValueForItem(item, allItems) {
  const meta = _HOLDING_META[item.stock_code];
  if (!meta || !meta.totalShares) return null;
  const snapshotValue = Number(meta.holdingValuePerShare);
  const fallback = Number.isFinite(snapshotValue) && snapshotValue > 0 ? snapshotValue : null;
  let subTotal = 0;
  for (const sub of meta.subsidiaries || []) {
    const cached = _EXTERNAL_QUOTE_CACHE[sub.code];
    const inPort = allItems.find(i => i.stock_code === sub.code);
    const subPrice = inPort?.quote?.price ?? cached?.price;
    if (subPrice == null) return fallback;
    subTotal += Number(subPrice) * (sub.sharesHeld || 0);
  }
  const free = meta.totalShares - (meta.treasuryShares || 0);
  return free > 0 && subTotal > 0 ? subTotal / free : fallback;
}

function _targetFormulaUses(item, variableName) {
  return String(item?.target_price_formula || '').includes(variableName);
}

function _targetFormulaVariables(item, allItems) {
  const metrics = item.target_metrics || {};
  const numberOrNull = (v) => {
    if (v === null || v === undefined || v === '') return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  };
  return {
    BPS: numberOrNull(metrics.bps),
    EPS: numberOrNull(metrics.eps),
    DPS: numberOrNull(metrics.dps ?? item.trailingDps),
    '보유지분': _holdingValueForItem(item, allItems),
    '본주가격': _isPreferredStock(item.stock_code) ? _preferredCommonPriceForItem(item, allItems) : null,
    '매입가': numberOrNull(item.avgPrice ?? item.avg_price),
  };
}

function _evaluateTargetFormula(formula, variables) {
  const text = String(formula || '').trim();
  if (!text) return null;
  let pos = 0;
  const isChar = (ch) => typeof ch === 'string' && ch.length === 1;
  const isSpace = (ch) => isChar(ch) && /\s/.test(ch);
  const isDigit = (ch) => isChar(ch) && /[0-9]/.test(ch);
  const isIdent = (ch) => isChar(ch) && /[A-Za-z가-힣_]/.test(ch);
  const skip = () => { while (pos < text.length && isSpace(text[pos])) pos += 1; };
  const parseNumber = () => {
    const start = pos;
    while (pos < text.length && /[0-9.]/.test(text[pos])) pos += 1;
    const n = Number(text.slice(start, pos));
    if (!Number.isFinite(n)) throw new Error('bad number');
    return n;
  };
  const parseName = () => {
    const start = pos;
    while (pos < text.length && isIdent(text[pos])) pos += 1;
    const name = text.slice(start, pos);
    if (!Object.prototype.hasOwnProperty.call(variables, name)) throw new Error(`unknown ${name}`);
    const value = variables[name];
    if (!Number.isFinite(value)) throw new Error(`missing ${name}`);
    return value;
  };
  const factor = () => {
    skip();
    const ch = text[pos];
    if (ch === '+') { pos += 1; return factor(); }
    if (ch === '-') { pos += 1; return -factor(); }
    if (ch === '(') {
      pos += 1;
      const value = expr();
      skip();
      if (text[pos] !== ')') throw new Error('missing )');
      pos += 1;
      return value;
    }
    if (isDigit(ch) || ch === '.') return parseNumber();
    if (isIdent(ch)) return parseName();
    throw new Error('bad token');
  };
  const term = () => {
    let value = factor();
    while (true) {
      skip();
      const op = text[pos];
      if (op !== '*' && op !== '/') break;
      pos += 1;
      const right = factor();
      value = op === '*' ? value * right : value / right;
    }
    return value;
  };
  const expr = () => {
    let value = term();
    while (true) {
      skip();
      const op = text[pos];
      if (op !== '+' && op !== '-') break;
      pos += 1;
      const right = term();
      value = op === '+' ? value + right : value - right;
    }
    return value;
  };
  try {
    const value = expr();
    skip();
    return pos === text.length && Number.isFinite(value) && value >= 0 ? value : null;
  } catch (e) {
    return null;
  }
}

// 목표가 계산. 반환:
//   - null  → '-' 표시 (CASH_ 등 의미 없음, 또는 명시적 비움/수식 변수 부족)
//   - 숫자 → 그 값. 계산 출처는 _targetPriceSource 가 별도 알려줌.
function _computeTargetPrice(item, allItems) {
  const code = item.stock_code;
  if (code.startsWith('CASH_')) return null;
  if (item.target_price_disabled) return null;
  if (item.target_price_formula) {
    const formulaPrice = _evaluateTargetFormula(item.target_price_formula, _targetFormulaVariables(item, allItems));
    if (formulaPrice !== null) return formulaPrice;
    if (item.target_price != null) return Number(item.target_price);
    return null;
  }
  if (item.target_price != null) return Number(item.target_price);

  if (_isPreferredStock(code)) {
    return _preferredCommonPriceForItem(item, allItems) ?? item.avg_price * 1.3;
  }

  if (_HOLDING_META[code]) {
    return _holdingValueForItem(item, allItems) ?? item.avg_price * 1.3;
  }

  return item.avg_price * 1.3;
}

function _targetPriceSource(item) {
  const code = item.stock_code;
  if (code.startsWith('CASH_')) return 'cash';
  if (item.target_price_disabled) return 'disabled';
  if (item.target_price_formula) return 'formula';
  if (item.target_price != null) return 'manual';
  if (_isPreferredStock(code)) return 'preferred';
  if (_HOLDING_META[code]) return 'holding';
  return 'default';
}

function _targetPriceTooltip(item) {
  if (item.target_price_formula) return `목표가 수식: ${item.target_price_formula}`;
  if (item.target_price != null) return `직접 입력: ${Number(item.target_price).toLocaleString()}원`;
  const source = _targetPriceSource(item);
  if (source === 'preferred') return '자동 목표가: 본주가격';
  if (source === 'holding') return '자동 목표가: 보유지분';
  if (source === 'default') return '자동 목표가: 매입가 × 1.3';
  return '';
}

function _initPreferredPairsFromConfig() {
  const pairsByCode = getIntegrationConfig('preferredSpread').pairsByPreferredCode || {};
  _PREFERRED_PAIR_BY_CODE = pairsByCode && typeof pairsByCode === 'object' ? pairsByCode : {};
}

_initPreferredPairsFromConfig();

function _preferredCommonCodeFor(code) {
  const normalized = _normalizePortfolioCode(code);
  const pair = _PREFERRED_PAIR_BY_CODE[normalized];
  if (pair && pair.commonCode) return pair.commonCode;
  return normalized.slice(0, -1) + '0';
}

function _goldGapInfoForCode(code) {
  const config = getIntegrationConfig('goldGap');
  const assetByCode = config.assetByPortfolioCode || {};
  const fallbackAsset = {
    KRX_GOLD: 'gold',
    CRYPTO_BTC: 'bitcoin',
  }[code] || '';
  const asset = assetByCode[code] || fallbackAsset;
  if (!asset) return { asset: '', label: '', title: '' };
  const assetConfig = (config.assets && config.assets[asset]) || {};
  const latestGap = Number(assetConfig.latestGapPct);
  const hasLatestGap = Number.isFinite(latestGap);
  const label = hasLatestGap ? `Gap ${latestGap >= 0 ? '+' : ''}${latestGap.toFixed(1)}%` : 'Gap';
  const titleParts = [assetConfig.label || asset, 'gap dashboard'];
  if (hasLatestGap) titleParts.push(`latest ${latestGap.toFixed(2)}%`);
  if (assetConfig.latestDate) titleParts.push(assetConfig.latestDate);
  return { asset, label, title: titleParts.join(' · ') };
}

function _openGoldGapDashboard(asset) {
  openIntegration('goldGap', '', { asset });
}

function _isKoreanAnalysisCode(code) {
  return /^[0-9][0-9A-Z]{5}$/.test(_normalizePortfolioCode(code));
}

function _hasAssetInsight(code) {
  return Boolean(code);
}

function _analysisAction(stockCode, label = '분석 화면', hint = '재무/밸류에이션 분석') {
  return {
    id: `analysis-${stockCode}`,
    label,
    hint,
    run: () => {
      switchView('analysis');
      analyzeStock(stockCode);
    },
  };
}

function _naverFinanceAction(stockCode, label = '네이버 파이낸스') {
  const targetCode = _isPreferredStock(stockCode) ? _preferredCommonCodeFor(stockCode) : stockCode;
  return {
    id: `naver-finance-${targetCode}`,
    label,
    hint: `${targetCode} 네이버 금융`,
    run: () => {
      window.open(
        `https://finance.naver.com/item/main.naver?code=${encodeURIComponent(targetCode)}`,
        '_blank',
        'noopener,noreferrer',
      );
    },
  };
}

function _portfolioLinkActions(stockCode, options = {}) {
  const includeInsight = options.includeInsight !== false;
  const includeGoldGap = options.includeGoldGap !== false;
  const actions = [];
  if (_isKoreanAnalysisCode(stockCode)) {
    if (_isPreferredStock(stockCode)) {
      const commonCode = _preferredCommonCodeFor(stockCode);
      actions.push(_analysisAction(commonCode, `본주 분석 (${commonCode})`));
      actions.push(_naverFinanceAction(stockCode, '네이버 파이낸스 (본주)'));
      actions.push({
        id: 'preferred-spread',
        label: '우선주 괴리율',
        hint: '보통주 대비 스프레드',
        run: () => openIntegration('preferredSpread', '', { code: stockCode }),
      });
    } else {
      actions.push(_analysisAction(stockCode));
      actions.push(_naverFinanceAction(stockCode));
      if (_HOLDING_CODES.has(stockCode)) {
        actions.push({
          id: 'holding-value',
          label: '자회사 비율 추이',
          hint: 'Holding Value 대시보드',
          run: () => openIntegration('holdingValue', '', { code: stockCode }),
        });
      }
    }
  }
  if (includeInsight && _hasAssetInsight(stockCode)) {
    actions.push({
      id: 'insight',
      label: '투자 인사이트',
      hint: '가격 추세, 벤치마크, 시장 지표',
      run: () => pfOpenAssetInsight(stockCode),
    });
  }
  const goldGapInfo = _goldGapInfoForCode(stockCode);
  if (includeGoldGap && goldGapInfo.asset) {
    actions.push({
      id: 'gold-gap',
      label: goldGapInfo.label || 'Gap',
      hint: goldGapInfo.title || 'Gap 대시보드',
      run: () => _openGoldGapDashboard(goldGapInfo.asset),
    });
  }
  return actions;
}

function _runOrShowPortfolioLinks(stockCode, e) {
  const actions = _portfolioLinkActions(stockCode);
  if (actions.length === 0) return false;
  if (actions.length === 1) {
    actions[0].run();
    return true;
  }
  _showPortfolioLinkMenu(actions, e);
  return true;
}

function _positionPortfolioPopupMenu(menu, e) {
  const margin = 8;
  const anchor = e && e.target && e.target.closest
    ? (e.target.closest('.js-pf-analyze, .pf-stock-link, button, a') || e.target)
    : null;
  const rect = anchor && anchor.getBoundingClientRect
    ? anchor.getBoundingClientRect()
    : { left: 100, right: 100, top: 100, bottom: 100 };

  menu.style.visibility = 'hidden';
  menu.style.left = '0px';
  menu.style.top = '0px';
  menu.style.maxHeight = '';
  menu.style.overflowY = '';

  const viewportW = window.innerWidth || document.documentElement.clientWidth || 1024;
  const viewportH = window.innerHeight || document.documentElement.clientHeight || 768;
  const menuW = menu.offsetWidth || 240;
  const menuH = menu.scrollHeight || menu.offsetHeight || 120;
  const below = Math.max(0, viewportH - rect.bottom - margin);
  const above = Math.max(0, rect.top - margin);
  const openAbove = menuH > below && above > below;
  const maxViewportMenuH = Math.max(64, viewportH - margin * 2);
  const available = Math.min(maxViewportMenuH, Math.max(96, (openAbove ? above : below) - 4));
  const finalH = Math.min(menuH, available);

  if (menuH > available) {
    menu.style.maxHeight = `${available}px`;
    menu.style.overflowY = 'auto';
  }

  let left = rect.left;
  if (left + menuW > viewportW - margin) {
    left = Math.max(margin, viewportW - menuW - margin);
  }
  left = Math.max(margin, left);

  let top = openAbove ? rect.top - finalH - 4 : rect.bottom + 4;
  if (top + finalH > viewportH - margin) {
    top = Math.max(margin, viewportH - finalH - margin);
  }
  top = Math.max(margin, top);

  menu.classList.toggle('open-above', openAbove);
  menu.style.left = `${Math.round(left)}px`;
  menu.style.top = `${Math.round(top)}px`;
  menu.style.visibility = '';
}

function _showPortfolioLinkMenu(actions, e) {
  document.querySelectorAll('.pf-pref-menu').forEach(el => el.remove());
  const menu = document.createElement('div');
  menu.className = 'pf-pref-menu pf-link-menu';
  menu.innerHTML = actions.map((action, idx) => `
    <div class="pf-pref-item" data-action-idx="${idx}">
      <strong>${escapeHtml(action.label)}</strong>
      <span>${escapeHtml(action.hint || '')}</span>
    </div>
  `).join('');
  document.body.appendChild(menu);
  _positionPortfolioPopupMenu(menu, e);
  menu.querySelectorAll('[data-action-idx]').forEach(el => {
    el.addEventListener('click', () => {
      const action = actions[Number(el.dataset.actionIdx)];
      menu.remove();
      if (action) action.run();
    });
  });
  setTimeout(() => {
    document.addEventListener('click', function close(ev) {
      if (!menu.contains(ev.target)) { menu.remove(); document.removeEventListener('click', close); }
    });
  }, 0);
}
