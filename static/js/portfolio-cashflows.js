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
    <td><span class="pf-cf-type ${isDeposit ? 'deposit' : 'withdrawal'}">${isDeposit ? '입금' : cf.type === 'distribution' ? '분배금 출금' : '출금'}</span></td>
    <td class="pf-col-num pf-cf-amount ${isDeposit ? 'deposit' : 'withdrawal'}">${fmtNum(Math.round(cf.amount))}원${cf.type === 'distribution' && cf.currency && cf.currency !== 'KRW' ? `<small class="pf-cf-native">${Number(cf.native_amount).toLocaleString(undefined, { maximumFractionDigits: 2 })} ${escapeHtml(cf.currency)}</small>` : ''}</td>
    <td class="pf-col-num">${fmtCfDecimal(cf.nav_at_time)}</td>
    <td class="pf-col-num">${cf.type === 'distribution' ? '0.00' : fmtCfSignedDecimal(cf.units_change)}</td>
    <td class="pf-col-num">${fmtCfDecimal(remainingUnitsById.get(String(cf.id)))}</td>
    <td title="${escapeHtml(cf.memo || '')}">${escapeHtml(cf.memo || '')}</td>
    <td>${cf.type === 'distribution' ? '좌수 유지' : cf.cancelled_at ? '취소됨' : cf.reversal_of_id != null ? '취소 거래' : `<button class="pf-row-btn delete js-pf-cf-delete" data-cf-id="${cf.id}" aria-label="입출금 취소" title="${cf.applied_snapshot_date ? '취소 거래로 되돌리기' : '삭제'}">&times;</button>`}</td>
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
  if (typeof loadPerformanceData === 'function') await loadPerformanceData();
}

let _pfCashflowSaving = false;

function pfOpenCashflow() {
  if (_pfCashflowSaving) return;
  const date = document.getElementById('pfCfDate');
  const today = new Intl.DateTimeFormat('sv-SE', { timeZone: 'Asia/Seoul' }).format(new Date());
  date.max = today;
  if (!date.value) date.value = today;
  document.getElementById('pfCfStatus').textContent = '';
  const dialog = document.getElementById('pfCfDialog');
  if (!dialog.open) dialog.showModal();
  document.getElementById('pfCfAmount').focus();
}

async function addCashflow(event) {
  event?.preventDefault();
  if (_pfCashflowSaving || !document.getElementById('pfCfForm').reportValidity()) return;
  const type = document.getElementById('pfCfType').value;
  const date = document.getElementById('pfCfDate').value;
  const amount = parseFloat(document.getElementById('pfCfAmount').value);
  const memo = document.getElementById('pfCfMemo').value.trim();
  if (!Number.isFinite(amount) || amount <= 0) { showToast('금액을 입력해 주세요.'); return; }
  const fields = document.getElementById('pfCfFields');
  const save = document.getElementById('pfCfSave');
  const status = document.getElementById('pfCfStatus');
  _pfCashflowSaving = true;
  fields.disabled = save.disabled = true;
  status.textContent = '입출금을 등록하고 있습니다…';
  let saved = false;
  try {
    await apiFetchJson('/api/portfolio/cashflows', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type, date: date || undefined, amount, memo: memo || undefined }),
      errorMessage: '입출금을 등록하지 못했습니다.',
    });
    document.getElementById('pfCfAmount').value = '';
    document.getElementById('pfCfMemo').value = '';
    saved = true;
    document.getElementById('pfCfDialog').close();
    showToast('입출금을 등록했습니다. 심층 분석의 자금 입출금에서 내역을 확인할 수 있습니다.');
  } catch (e) {
    status.textContent = e.message || '입출금을 등록하지 못했습니다.';
    reportApiError(e, '입출금 등록');
  } finally {
    _pfCashflowSaving = false;
    fields.disabled = save.disabled = false;
  }
  if (saved) await refreshPortfolioAfterCashflowMutation();
}

document.getElementById('pfCfForm')?.addEventListener('submit', addCashflow);
document.getElementById('pfCfClose')?.addEventListener('click', () => {
  if (!_pfCashflowSaving) document.getElementById('pfCfDialog').close();
});
document.getElementById('pfCfDialog')?.addEventListener('cancel', event => {
  if (_pfCashflowSaving) event.preventDefault();
});

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
