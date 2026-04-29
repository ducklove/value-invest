// Portfolio group modal, market bar, CSV import/export, currency switching.
// Split from static/js/portfolio.js to keep portfolio features maintainable.
// --- Group management modal ---
function openGroupModal() {
  document.getElementById('pfGroupModal').style.display = 'flex';
  renderGroupModalBody();
}

function closeGroupModal() {
  document.getElementById('pfGroupModal').style.display = 'none';
}

const _PIE_COLORS = ['#4e79a7','#f28e2b','#e15759','#76b7b2','#59a14f','#edc948','#b07aa1','#ff9da7','#9c755f','#bab0ac'];

function _drawGroupPie(stats, grandMV) {
  const canvas = document.getElementById('pfGroupPie');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const dpr = window.devicePixelRatio || 1;
  const size = 180;
  canvas.width = size * dpr;
  canvas.height = size * dpr;
  canvas.style.width = size + 'px';
  canvas.style.height = size + 'px';
  ctx.scale(dpr, dpr);

  const cx = size / 2, cy = size / 2, r = 70;
  const slices = pfGroups.map((g, i) => {
    const s = stats[g.group_name] || { mv: 0 };
    return { name: g.group_name, value: s.mv, color: _PIE_COLORS[i % _PIE_COLORS.length] };
  }).filter(s => s.value > 0);

  if (!slices.length || grandMV <= 0) {
    ctx.fillStyle = 'var(--text-secondary)';
    ctx.font = '12px sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText('데이터 없음', cx, cy);
    return;
  }

  let angle = -Math.PI / 2;
  slices.forEach(s => {
    const sweep = (s.value / grandMV) * Math.PI * 2;
    ctx.beginPath();
    ctx.moveTo(cx, cy);
    ctx.arc(cx, cy, r, angle, angle + sweep);
    ctx.closePath();
    ctx.fillStyle = s.color;
    ctx.fill();
    // Label
    if (sweep > 0.15) {
      const mid = angle + sweep / 2;
      const lx = cx + Math.cos(mid) * (r * 0.6);
      const ly = cy + Math.sin(mid) * (r * 0.6);
      const pct = (s.value / grandMV * 100).toFixed(0) + '%';
      ctx.fillStyle = '#fff';
      ctx.font = 'bold 11px sans-serif';
      ctx.textAlign = 'center';
      ctx.textBaseline = 'middle';
      ctx.fillText(pct, lx, ly);
    }
    angle += sweep;
  });

  // Legend below
  const ly = size - 8;
  let lx = 8;
  ctx.font = '10px sans-serif';
  ctx.textBaseline = 'bottom';
  slices.forEach(s => {
    ctx.fillStyle = s.color;
    ctx.fillRect(lx, ly - 8, 8, 8);
    ctx.fillStyle = getComputedStyle(document.documentElement).getPropertyValue('--text').trim() || '#333';
    ctx.textAlign = 'left';
    const label = s.name.length > 4 ? s.name.slice(0, 4) + '..' : s.name;
    ctx.fillText(label, lx + 10, ly);
    lx += ctx.measureText(label).width + 18;
  });
}

function renderGroupModalBody() {
  const body = document.getElementById('pfGroupModalBody');
  // Compute per-group stats.
  // prevMV is derived from the LIVE quote (qty × (price - change)) rather
  // than from yesterday's snapshot, so composition drift (qty changes,
  // items added since the snapshot) cannot push group% outside the range
  // of its constituents. Missing-quote items are excluded from both sides.
  const stats = {};
  let grandMV = 0;
  portfolioItems.forEach(i => {
    const gn = pfGetGroup(i);
    if (!stats[gn]) stats[gn] = { cnt: 0, invested: 0, mv: 0, prevMV: 0 };
    const s = stats[gn];
    const q = i.quote || {};
    const price = q.price ?? null;
    const change = q.change ?? 0;
    const qty = i.quantity;
    const avgPrice = i.avg_price;
    s.cnt++;
    s.invested += qty * avgPrice;
    if (price !== null) {
      const mv = qty * price;
      s.mv += mv;
      s.prevMV += qty * (price - change);
      grandMV += mv;
    }
  });
  const defaultCount = pfGroups.filter(x => x.is_default).length;
  const rowsHtml = pfGroups.map((g, i) => {
    const s = stats[g.group_name] || { cnt: 0, invested: 0, mv: 0, prevMV: 0 };
    const weight = grandMV > 0 ? (s.mv / grandMV * 100) : 0;
    const returnPct = s.invested !== 0 ? ((s.mv - s.invested) / Math.abs(s.invested) * 100) : 0;
    const dailyPnl = s.prevMV !== 0 ? (s.mv - s.prevMV) : 0;
    const dailyPct = s.prevMV !== 0 ? (dailyPnl / Math.abs(s.prevMV) * 100) : 0;
    const canDelete = !g.is_default || defaultCount > 3;
    const delBtn = canDelete
      ? `<button class="pf-grp-del" data-grp-name="${escapeHtml(g.group_name)}" title="삭제">&times;</button>`
      : '';
    return `<tr class="pf-grp-tr" draggable="true" data-grp-idx="${i}">
      <td class="pf-grp-td-drag"><span class="pf-grp-drag" title="드래그하여 순서 변경">&#x2630;</span></td>
      <td class="pf-grp-td-name"><input class="pf-grp-name" value="${escapeHtml(g.group_name)}" data-orig="${escapeHtml(g.group_name)}" onblur="renameGroup(this)"></td>
      <td class="pf-grp-td-num">${s.cnt}</td>
      <td class="pf-grp-td-num">${weight.toFixed(1)}%</td>
      <td class="pf-grp-td-num">${fmtNum(Math.round(s.mv))}</td>
      <td class="pf-grp-td-num"><span class="${returnClass(returnPct)}">${fmtPct(returnPct)}</span></td>
      <td class="pf-grp-td-num"><span class="${returnClass(dailyPct)}">${fmtPct(dailyPct)}</span></td>
      <td class="pf-grp-td-num"><span class="${returnClass(dailyPnl)}">${fmtSignedKrw(dailyPnl)}</span></td>
      <td class="pf-grp-td-act">${delBtn}</td>
    </tr>`;
  }).join('');
  body.innerHTML = `<div class="pf-grp-layout">
    <div class="pf-grp-table-wrap"><table class="pf-grp-table">
      <thead><tr>
        <th></th><th>그룹명</th><th>종목</th><th>비중</th><th>평가금액</th><th>수익률</th><th>일간</th><th>일간수익</th><th></th>
      </tr></thead>
      <tbody>${rowsHtml}</tbody>
    </table></div>
    <div class="pf-grp-pie-wrap"><canvas id="pfGroupPie" width="180" height="180"></canvas></div>
  </div>`;
  // Draw pie chart
  _drawGroupPie(stats, grandMV);
  // Drag-and-drop for group reorder
  body.querySelectorAll('.pf-grp-tr[draggable]').forEach(row => {
    row.addEventListener('dragstart', e => {
      e.dataTransfer.effectAllowed = 'move';
      e.dataTransfer.setData('text/plain', row.dataset.grpIdx);
      row.classList.add('dragging');
    });
    row.addEventListener('dragend', () => row.classList.remove('dragging'));
    row.addEventListener('dragover', e => { e.preventDefault(); e.dataTransfer.dropEffect = 'move'; if (!row.classList.contains('dragging')) row.classList.add('drag-over'); });
    row.addEventListener('dragleave', () => row.classList.remove('drag-over'));
    row.addEventListener('drop', async e => {
      e.preventDefault();
      row.classList.remove('drag-over');
      const fromIdx = parseInt(e.dataTransfer.getData('text/plain'));
      const toIdx = parseInt(row.dataset.grpIdx);
      if (isNaN(fromIdx) || isNaN(toIdx) || fromIdx === toIdx) return;
      const [moved] = pfGroups.splice(fromIdx, 1);
      pfGroups.splice(toIdx, 0, moved);
      renderGroupModalBody();
      renderPortfolio();
      try {
        await apiFetch('/api/portfolio/groups-order', {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ group_names: pfGroups.map(g => g.group_name) }),
        });
      } catch (e) { console.warn(e); }
    });
  });
  // Delete buttons — read group name from data attribute
  body.querySelectorAll('.pf-grp-del[data-grp-name]').forEach(btn => {
    btn.addEventListener('click', () => deleteGroup(btn.dataset.grpName));
  });
}

async function addNewGroup() {
  const input = document.getElementById('pfNewGroupInput');
  const name = input.value.trim();
  if (!name) return;
  try {
    const resp = await apiFetch('/api/portfolio/groups', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name }),
    });
    if (!resp.ok) {
      const d = await resp.json().catch(() => ({}));
      throw new Error(d.detail || '추가 실패');
    }
    const result = await resp.json();
    pfGroups.push(result);
    input.value = '';
    renderGroupModalBody();
    renderPortfolio();
  } catch (e) { showToast(e.message); }
}

async function renameGroup(inputEl) {
  const orig = inputEl.dataset.orig;
  const newName = inputEl.value.trim();
  if (!newName || newName === orig) {
    inputEl.value = orig;
    return;
  }
  try {
    const resp = await apiFetch(`/api/portfolio/groups/${encodeURIComponent(orig)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ new_name: newName }),
    });
    if (!resp.ok) {
      const d = await resp.json().catch(() => ({}));
      throw new Error(d.detail || '변경 실패');
    }
    const g = pfGroups.find(g => g.group_name === orig);
    if (g) g.group_name = newName;
    portfolioItems.forEach(i => { if (i.group_name === orig) i.group_name = newName; });
    if (pfGroupFilter && pfGroupFilter.has(orig)) {
      pfGroupFilter.delete(orig);
      pfGroupFilter.add(newName);
    }
    inputEl.dataset.orig = newName;
    renderPortfolio();
  } catch (e) {
    showToast(e.message);
    inputEl.value = orig;
  }
}

async function deleteGroup(groupName) {
  const counts = {};
  portfolioItems.forEach(i => {
    const g = pfGetGroup(i);
    counts[g] = (counts[g] || 0) + 1;
  });
  const cnt = counts[groupName] || 0;
  if (cnt > 0 && !confirm(`"${groupName}" 그룹에 ${cnt}개 종목이 있습니다. 삭제하면 기본 그룹으로 이동합니다. 삭제할까요?`)) return;
  try {
    const resp = await apiFetch(`/api/portfolio/groups/${encodeURIComponent(groupName)}`, { method: 'DELETE' });
    if (!resp.ok) {
      const d = await resp.json().catch(() => ({}));
      throw new Error(d.detail || '삭제 실패');
    }
    pfGroups = pfGroups.filter(g => g.group_name !== groupName);
    if (pfGroupFilter) pfGroupFilter.delete(groupName);
    await loadPortfolio();
    renderGroupModalBody();
  } catch (e) { showToast(e.message); }
}

// --- Market Bar ---
const MB_DEFAULT_CODES = ['KOSPI', 'KOSDAQ', 'USD_KRW', 'CMDT_GC', 'NIGHT_FUTURES'];
const MB_MAX = 10;
const MB_LS_KEY = 'market_bar_codes';
let mbCodes = [];
let mbCatalog = {};
let mbLoaded = false;
let mbPickerOpen = false;
let mbDragFrom = -1;

function _mbGetCodes() {
  try { const v = JSON.parse(localStorage.getItem(MB_LS_KEY)); if (Array.isArray(v)) return v; } catch (e) { console.warn(e); }
  return null;
}
function _mbSaveCodes() {
  localStorage.setItem(MB_LS_KEY, JSON.stringify(mbCodes));
  if (currentUser) apiFetch('/api/settings/market-bar', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ codes: mbCodes }) }).catch(() => {});
}
async function _mbLoadCodes() {
  if (currentUser) {
    try {
      const resp = await apiFetch('/api/settings/market-bar');
      if (resp.ok) { const d = await resp.json(); if (d.codes) { mbCodes = d.codes; localStorage.setItem(MB_LS_KEY, JSON.stringify(mbCodes)); return; } }
    } catch (e) { console.warn(e); }
  }
  mbCodes = _mbGetCodes() || MB_DEFAULT_CODES.slice();
}
async function _mbLoadCatalog() {
  try {
    const resp = await apiFetch('/api/market-indicators');
    if (resp.ok) mbCatalog = await resp.json();
  } catch (e) { console.warn(e); }
}

function _mbIsHidden(code) {
  if (code === 'NIGHT_FUTURES') {
    const h = new Date().getHours();
    if (h >= 9 && h < 18) return true;
  }
  return false;
}

function _mbRenderBar(dataMap) {
  const bar = document.getElementById('marketBar');
  if (!bar) return;
  let html = '';
  mbCodes.forEach((code, idx) => {
    if (_mbIsHidden(code)) return;
    const cat = mbCatalog[code];
    const label = cat ? cat.label : code;
    const d = dataMap ? dataMap[code] : null;
    const r = idx;  // row index
    let valHtml = '-', chgHtml = '';
    if (d && d.value) {
      const rawPct = (d.change_pct || '').replace(/[-+%]/g, '');
      const isDown = d.direction === 'down';
      const cls = isDown ? 'mi-down' : (d.direction === 'up' ? 'mi-up' : '');
      const sign = isDown ? '-' : (d.direction === 'up' ? '+' : '');
      const chgVal = d.change ? `${sign}${d.change}` : '';
      const chgPct = rawPct ? `(${sign}${rawPct}%)` : '';
      valHtml = d.value;
      chgHtml = `<span class="${cls}">${chgVal} ${chgPct}</span>`;
    }
    html += `<span class="mi-label" draggable="true" data-idx="${r}">${escapeHtml(label)}</span>`;
    html += `<span class="mi-val" data-idx="${r}">${valHtml}</span>`;
    html += `<span class="mi-chg" data-idx="${r}">${chgHtml}</span>`;
    html += `<button class="mi-del" data-code="${code}" title="삭제">&times;</button>`;
  });
  if (mbCodes.length < MB_MAX) {
    html += `<div class="mi-add" id="mbAddBtn">+ 항목 추가</div>`;
  }
  bar.innerHTML = html;

  // Event: delete
  bar.querySelectorAll('.mi-del').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      mbCodes = mbCodes.filter(c => c !== btn.dataset.code);
      _mbSaveCodes();
      loadMarketSummary();
    });
  });

  // Event: drag reorder (on label spans)
  bar.querySelectorAll('.mi-label[draggable]').forEach(lbl => {
    lbl.addEventListener('dragstart', (e) => { mbDragFrom = parseInt(lbl.dataset.idx); e.dataTransfer.effectAllowed = 'move'; });
    lbl.addEventListener('dragend', () => { bar.querySelectorAll('.mi-drop-target').forEach(el => el.classList.remove('mi-drop-target')); });
  });
  bar.querySelectorAll('[data-idx]').forEach(cell => {
    cell.addEventListener('dragover', (e) => { e.preventDefault(); const r = parseInt(cell.dataset.idx); bar.querySelectorAll(`[data-idx="${r}"]`).forEach(c => c.classList.add('mi-drop-target')); });
    cell.addEventListener('dragleave', (e) => { const r = parseInt(cell.dataset.idx); bar.querySelectorAll(`[data-idx="${r}"]`).forEach(c => c.classList.remove('mi-drop-target')); });
    cell.addEventListener('drop', (e) => {
      e.preventDefault();
      bar.querySelectorAll('.mi-drop-target').forEach(el => el.classList.remove('mi-drop-target'));
      const to = parseInt(cell.dataset.idx);
      if (mbDragFrom !== to && mbDragFrom >= 0) {
        const [item] = mbCodes.splice(mbDragFrom, 1);
        mbCodes.splice(to, 0, item);
        _mbSaveCodes();
        loadMarketSummary();
      }
    });
  });

  // Event: row hover → show delete button
  bar.querySelectorAll('[data-idx]').forEach(cell => {
    cell.addEventListener('mouseenter', () => {
      const r = cell.dataset.idx;
      const dels = bar.querySelectorAll('.mi-del');
      dels[parseInt(r)]?.classList.add('visible');
    });
    cell.addEventListener('mouseleave', () => {
      bar.querySelectorAll('.mi-del.visible').forEach(d => d.classList.remove('visible'));
    });
  });
  bar.querySelectorAll('.mi-del').forEach(btn => {
    btn.addEventListener('mouseenter', () => btn.classList.add('visible'));
    btn.addEventListener('mouseleave', () => btn.classList.remove('visible'));
  });

  // Event: add button
  const addBtn = document.getElementById('mbAddBtn');
  if (addBtn) addBtn.addEventListener('click', () => _mbTogglePicker());

  if (mbLoaded) flashEl(bar);
  mbLoaded = true;
}

function _mbTogglePicker() {
  const existing = document.getElementById('mbPicker');
  if (existing) { existing.remove(); mbPickerOpen = false; return; }
  mbPickerOpen = true;

  const bar = document.getElementById('marketBar');
  const picker = document.createElement('div');
  picker.id = 'mbPicker';
  picker.className = 'mb-picker';

  const categories = {};
  for (const [code, info] of Object.entries(mbCatalog)) {
    const cat = info.category;
    if (!categories[cat]) categories[cat] = [];
    categories[cat].push({ code, label: info.label });
  }

  let html = '';
  for (const [cat, items] of Object.entries(categories)) {
    html += `<div class="mb-pick-cat">${escapeHtml(cat)}</div>`;
    items.forEach(item => {
      const disabled = mbCodes.includes(item.code);
      html += `<div class="mb-pick-item${disabled ? ' disabled' : ''}" data-code="${item.code}">${escapeHtml(item.label)}</div>`;
    });
  }
  picker.innerHTML = html;
  bar.appendChild(picker);

  picker.querySelectorAll('.mb-pick-item:not(.disabled)').forEach(el => {
    el.addEventListener('click', () => {
      mbCodes.push(el.dataset.code);
      _mbSaveCodes();
      picker.remove();
      mbPickerOpen = false;
      loadMarketSummary();
    });
  });

  // Close on outside click
  setTimeout(() => {
    const closeHandler = (e) => { if (!picker.contains(e.target) && e.target.id !== 'mbAddBtn') { picker.remove(); mbPickerOpen = false; document.removeEventListener('click', closeHandler); } };
    document.addEventListener('click', closeHandler);
  }, 0);
}

async function loadMarketSummary() {
  try {
    if (!mbCodes.length) await _mbLoadCodes();
    const resp = await apiFetch(`/api/market-summary?codes=${mbCodes.join(',')}`);
    if (!resp.ok) return;
    const dataMap = await resp.json();
    _mbRenderBar(dataMap);
  } catch (e) { console.warn(e); }
}

async function _pollBenchmarkQuotes() {
  try {
    const r = await apiFetch('/api/portfolio/benchmark-quotes');
    if (!r.ok) return;
    const fresh = await r.json();
    for (const [k, v] of Object.entries(fresh)) pfBenchmarkQuotes[k] = v;
    // 전체 재렌더 대신 벤치마크 셀만 업데이트 — 이래야 WS tick 으로 in-
    // place 갱신된 다른 셀들이 60초 polling 때마다 뒤집히지 않음.
    if (activeView === 'portfolio') {
      for (const k of Object.keys(fresh)) updatePortfolioBenchmarkCells(k);
    }
  } catch (e) { console.warn(e); }
}

function toggleCsvPanel() {
  const panel = document.getElementById('pfCsvPanel');
  panel.style.display = panel.style.display === 'none' ? 'block' : 'none';
}

async function submitCsv(mode) {
  const text = document.getElementById('pfCsvInput').value.trim();
  if (!text) { showToast('CSV 데이터를 입력해 주세요.'); return; }

  if (mode === 'replace' && !confirm('기존 포트폴리오를 모두 삭제하고 새로 등록합니다. 계속할까요?')) return;

  const lines = text.split('\n').map(l => l.trim()).filter(Boolean);
  const items = [];
  const errors = [];

  for (let i = 0; i < lines.length; i++) {
    const parts = lines[i].split(/[,\t]/).map(s => s.trim());
    if (parts.length < 3) { errors.push(`행 ${i+1}: 종목코드,매입가,수량 3개 필드가 필요합니다.`); continue; }
    const [code, priceStr, qtyStr] = parts;
    const price = Number(priceStr);
    const qty = parseFloat(qtyStr);
    if (!code) { errors.push(`행 ${i+1}: 종목코드가 비어 있습니다.`); continue; }
    if (isNaN(price) || price < 0) { errors.push(`행 ${i+1}: 매입가가 올바르지 않습니다.`); continue; }
    if (isNaN(qty) || qty === 0) { errors.push(`행 ${i+1}: 수량은 0이 아닌 값이어야 합니다.`); continue; }
    items.push({ stock_code: code, avg_price: price, quantity: qty });
  }

  if (errors.length) { showToast(errors.join('\n')); return; }
  if (!items.length) { showToast('등록할 종목이 없습니다.'); return; }

  const btns = document.querySelectorAll('.pf-csv-btn');
  btns.forEach(b => b.disabled = true);

  try {
    const resp = await apiFetch('/api/portfolio/bulk', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mode, items }),
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.detail || '등록 실패');
    document.getElementById('pfCsvInput').value = '';
    document.getElementById('pfCsvPanel').style.display = 'none';
    showToast(`${data.imported}개 종목이 ${mode === 'replace' ? '교체' : '추가'} 등록되었습니다.`, 'success');
    await loadPortfolio();
  } catch (e) {
    showToast(e.message);
  } finally {
    btns.forEach(b => b.disabled = false);
  }
}

// --- Currency conversion ---
function pfFx(krwValue) {
  if (pfCurrency === 'USD' && pfFxRate && pfFxRate > 0) return krwValue / pfFxRate;
  return krwValue;
}
function pfFxSymbol() { return pfCurrency === 'USD' ? '$' : ''; }
function pfFxUnit() { return pfCurrency === 'USD' ? 'M' : '억'; }
function pfFxDivisor() { return pfCurrency === 'USD' ? 1e6 : 1e8; }

async function _ensureFxRate() {
  if (pfFxRate) return;
  try {
    const resp = await apiFetch('/api/asset-quote/CASH_USD');
    if (resp.ok) {
      const d = await resp.json();
      if (d.price) pfFxRate = d.price; // KRW per 1 USD
    }
  } catch (e) { console.warn(e); }
}

function pfSetCurrency(currency) {
  pfCurrency = currency;
  document.querySelectorAll('.pf-currency-btn').forEach(b => b.classList.toggle('active', b.dataset.currency === currency));
  const refresh = () => {
    renderPortfolio();
    if (pfActiveTab === 'performance') loadPerformanceData();
  };
  if (currency === 'USD') {
    _ensureFxRate().then(refresh);
  } else {
    refresh();
  }
}
