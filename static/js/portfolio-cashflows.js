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
    const dateCompare = String(a.applied_snapshot_date || a.created_at || a.date || '').localeCompare(String(b.applied_snapshot_date || b.created_at || b.date || ''));
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
    const cfDate = String(cf.applied_snapshot_date || cf.date || '');
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
    remainingUnitsById.set(String(cf.id), cf.applied_snapshot_date ? runningUnits : null);
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
    <td>${cf.cancelled_at ? '취소됨' : cf.reversal_of_id != null ? '취소 거래' : `<button class="pf-row-btn delete js-pf-cf-delete" data-cf-id="${cf.id}" aria-label="입출금 취소" title="${cf.applied_snapshot_date ? '취소 거래로 되돌리기' : '삭제'}">&times;</button>`}</td>
  </tr>`;
  }).join('');
}

async function refreshPortfolioAfterCashflowMutation() {
  const tasks = [];
  if (typeof loadPortfolio === 'function') {
    tasks.push(loadPortfolio({ force: true }));
  }
  if (typeof pfRefreshTodayState === 'function') {
    tasks.push(pfRefreshTodayState({ force: true, render: false }));
  }
  await Promise.allSettled(tasks);
  if (typeof renderPortfolio === 'function') renderPortfolio();
  if (typeof loadPerformanceData === 'function') loadPerformanceData();
}

async function addCashflow() {
  const type = document.getElementById('pfCfType').value;
  const date = document.getElementById('pfCfDate').value;
  const amount = parseFloat(document.getElementById('pfCfAmount').value);
  const memo = document.getElementById('pfCfMemo').value.trim();
  if (!amount || amount <= 0) { showToast('금액을 입력해 주세요.'); return; }
  try {
    await apiFetchJson('/api/portfolio/cashflows', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type, date: date || undefined, amount, memo: memo || undefined }),
      errorMessage: '입출금을 등록하지 못했습니다.',
    });
    document.getElementById('pfCfAmount').value = '';
    document.getElementById('pfCfMemo').value = '';
    await refreshPortfolioAfterCashflowMutation();
  } catch (e) { reportApiError(e, '입출금 등록'); }
}

async function deleteCashflow(id) {
  if (!confirm('이 입출금을 취소할까요? 정산된 내역은 취소 거래를 남기고 현재 원화 잔고를 되돌립니다.')) return;
  try {
    await apiFetchJson(`/api/portfolio/cashflows/${id}`, {
      method: 'DELETE',
      errorMessage: '입출금을 삭제하지 못했습니다.',
    });
    await refreshPortfolioAfterCashflowMutation();
  } catch (e) { reportApiError(e, '입출금 삭제'); }
}
