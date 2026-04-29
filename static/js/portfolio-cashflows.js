// Portfolio cashflow table and cashflow mutations.
// Split from static/js/portfolio.js to keep portfolio features maintainable.
function renderCashflows(data, navData = _navChartData) {
  const tbody = document.getElementById('pfCfBody');
  if (!tbody) return;
  if (!data.length) {
    tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:var(--text-secondary);">입출금 내역이 없습니다.</td></tr>';
    return;
  }
  const fmtCfDecimal = (value) => {
    const n = Number(value);
    if (!Number.isFinite(n) || n === 0) return '-';
    return n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  };
  const fmtCfSignedDecimal = (value) => {
    const n = Number(value);
    if (!Number.isFinite(n) || n === 0) return '-';
    const sign = n > 0 ? '+' : '';
    return sign + n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  };
  const snapshotUnits = (snapshot) => {
    const units = Number(snapshot?.total_units);
    if (Number.isFinite(units) && units > 0) return units;
    const value = Number(snapshot?.total_value);
    const nav = Number(snapshot?.nav);
    return Number.isFinite(value) && Number.isFinite(nav) && nav > 0 ? value / nav : null;
  };
  const compareCashflows = (a, b) => {
    const dateCompare = String(a.date || '').localeCompare(String(b.date || ''));
    if (dateCompare !== 0) return dateCompare;
    const createdCompare = String(a.created_at || '').localeCompare(String(b.created_at || ''));
    if (createdCompare !== 0) return createdCompare;
    return Number(a.id || 0) - Number(b.id || 0);
  };
  const snapshots = (navData || [])
    .map(s => ({ date: String(s.date || ''), units: snapshotUnits(s) }))
    .filter(s => s.date && Number.isFinite(s.units))
    .sort((a, b) => a.date.localeCompare(b.date));
  const remainingUnitsById = new Map();
  let runningUnits = null;
  let snapshotIdx = -1;
  let activeSnapshotDate = '';
  [...data].sort(compareCashflows).forEach(cf => {
    const cfDate = String(cf.date || '');
    while (snapshotIdx + 1 < snapshots.length && snapshots[snapshotIdx + 1].date <= cfDate) {
      snapshotIdx += 1;
      runningUnits = snapshots[snapshotIdx].units;
      activeSnapshotDate = snapshots[snapshotIdx].date;
    }
    if (activeSnapshotDate !== cfDate) {
      if (runningUnits === null) runningUnits = 0;
      const delta = Number(cf.units_change);
      if (Number.isFinite(delta)) runningUnits += delta;
    }
    remainingUnitsById.set(String(cf.id), runningUnits);
  });
  tbody.innerHTML = data.map(cf => {
    const isDeposit = cf.type === 'deposit';
    return `<tr>
    <td>${escapeHtml(cf.date || '')}</td>
    <td><span class="pf-cf-type ${isDeposit ? 'deposit' : 'withdrawal'}">${isDeposit ? '입금' : '출금'}</span></td>
    <td class="pf-col-num pf-cf-amount ${isDeposit ? 'deposit' : 'withdrawal'}">${fmtNum(Math.round(cf.amount))}원</td>
    <td class="pf-col-num">${fmtCfDecimal(cf.nav_at_time)}</td>
    <td class="pf-col-num">${fmtCfSignedDecimal(cf.units_change)}</td>
    <td class="pf-col-num">${fmtCfDecimal(remainingUnitsById.get(String(cf.id)))}</td>
    <td title="${escapeHtml(cf.memo || '')}">${escapeHtml(cf.memo || '')}</td>
    <td><button class="pf-row-btn delete js-pf-cf-delete" data-cf-id="${cf.id}" aria-label="입출금 삭제" title="삭제">&times;</button></td>
  </tr>`;
  }).join('');
}

async function addCashflow() {
  const type = document.getElementById('pfCfType').value;
  const date = document.getElementById('pfCfDate').value;
  const amount = parseFloat(document.getElementById('pfCfAmount').value);
  const memo = document.getElementById('pfCfMemo').value.trim();
  if (!amount || amount <= 0) { showToast('금액을 입력해 주세요.'); return; }
  try {
    const resp = await apiFetch('/api/portfolio/cashflows', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type, date: date || undefined, amount, memo: memo || undefined }),
    });
    if (!resp.ok) { const d = await resp.json().catch(() => ({})); throw new Error(d.detail || '등록 실패'); }
    document.getElementById('pfCfAmount').value = '';
    document.getElementById('pfCfMemo').value = '';
    loadPerformanceData();
  } catch (e) { showToast(e.message); }
}

async function deleteCashflow(id) {
  if (!confirm('이 입출금 내역을 삭제할까요?')) return;
  try {
    await apiFetch(`/api/portfolio/cashflows/${id}`, { method: 'DELETE' });
    loadPerformanceData();
  } catch (e) { showToast(e.message); }
}
