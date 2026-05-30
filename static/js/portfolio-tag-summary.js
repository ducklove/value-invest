// Tag-level portfolio popup: list tagged holdings and compare daily weighted return.
function _pfNormalizeTagKey(tag) {
  return String(tag || '').trim().replace(/^#/, '').toLowerCase();
}

function _pfNumberOrNull(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function _pfTagSummaryRows() {
  return PfStore.items.map(item => {
    const q = item.quote || {};
    const price = quotePriceOrNull(q);
    const qty = _pfNumberOrNull(item.quantity) ?? 0;
    const rawChange = price !== null ? _pfNumberOrNull(q.change) : null;
    const change = rawChange ?? 0;
    const changePct = price !== null ? _pfNumberOrNull(q.change_pct) : null;
    const marketValue = price !== null ? qty * price : null;
    const dailyPnl = price !== null ? qty * change : null;
    const baseValue = marketValue !== null && dailyPnl !== null ? marketValue - dailyPnl : null;
    return {
      ...item,
      tags: pfGetTags(item),
      price,
      changePct,
      marketValue,
      dailyPnl,
      baseValue,
    };
  });
}

function _pfDailyWeightedStats(rows) {
  let marketValue = 0;
  let dailyPnl = 0;
  let baseValue = 0;
  let pricedCount = 0;
  for (const row of rows) {
    if (Number.isFinite(row.marketValue)) {
      marketValue += row.marketValue;
      pricedCount += 1;
    }
    if (Number.isFinite(row.dailyPnl)) dailyPnl += row.dailyPnl;
    if (Number.isFinite(row.baseValue)) baseValue += row.baseValue;
  }
  return {
    count: rows.length,
    pricedCount,
    marketValue,
    dailyPnl,
    baseValue,
    dailyReturnPct: baseValue > 0 ? dailyPnl / baseValue * 100 : null,
  };
}

function _pfTagSummaryCard(label, value, sub, cls = '') {
  return `
    <div class="pf-tag-summary-card">
      <div class="pf-tag-summary-label">${escapeHtml(label)}</div>
      <div class="pf-tag-summary-value ${cls}">${value}</div>
      ${sub ? `<div class="pf-tag-summary-sub">${sub}</div>` : ''}
    </div>`;
}

function _pfRenderTagSummaryRows(rows, tagStats) {
  if (!rows.length) {
    return '<div class="pf-tag-summary-empty">해당 태그가 붙은 종목이 없습니다.</div>';
  }
  const body = rows.map(row => {
    const rowWeight = tagStats.marketValue > 0 && Number.isFinite(row.marketValue)
      ? row.marketValue / tagStats.marketValue * 100
      : null;
    const dailyPnlText = row.dailyPnl !== null ? fmtSignedKrw(row.dailyPnl) : '-';
    const dailyPctText = row.changePct !== null ? fmtPct(row.changePct) : '-';
    return `
      <tr>
        <td>
          <strong>${escapeHtml(row.stock_name || row.stock_code || '-')}</strong>
          <span>${escapeHtml(row.stock_code || '')}</span>
        </td>
        <td class="${returnClass(row.changePct)}">${dailyPctText}</td>
        <td class="${returnClass(row.dailyPnl)}">${dailyPnlText}</td>
        <td>${row.marketValue !== null ? pfFmtPortfolioValue(row.marketValue) : '-'}</td>
        <td>${rowWeight !== null ? fmtPct(rowWeight, false) : '-'}</td>
      </tr>`;
  }).join('');
  return `
    <div class="pf-tag-summary-table-wrap">
      <table class="pf-tag-summary-table">
        <thead>
          <tr>
            <th>종목</th>
            <th>일간</th>
            <th>손익</th>
            <th>평가금액</th>
            <th>태그 비중</th>
          </tr>
        </thead>
        <tbody>${body}</tbody>
      </table>
    </div>`;
}

function pfCloseTagSummary() {
  const modal = document.getElementById('pfTagSummaryModal');
  if (modal) modal.remove();
}

function pfOpenTagSummary(tag) {
  const tagKey = _pfNormalizeTagKey(tag);
  if (!tagKey) return;
  const allRows = _pfTagSummaryRows();
  const taggedRows = allRows
    .filter(row => row.tags.some(t => _pfNormalizeTagKey(t) === tagKey))
    .sort((a, b) => Math.abs(b.marketValue || 0) - Math.abs(a.marketValue || 0));
  const displayTag = taggedRows
    .flatMap(row => row.tags)
    .find(t => _pfNormalizeTagKey(t) === tagKey) || tag;
  const allStats = _pfDailyWeightedStats(allRows);
  const tagStats = _pfDailyWeightedStats(taggedRows);
  const spreadPct = tagStats.dailyReturnPct !== null && allStats.dailyReturnPct !== null
    ? tagStats.dailyReturnPct - allStats.dailyReturnPct
    : null;
  const portfolioWeight = allStats.marketValue > 0
    ? tagStats.marketValue / allStats.marketValue * 100
    : null;

  pfCloseTagSummary();
  const overlay = document.createElement('div');
  overlay.className = 'pf-modal-overlay pf-tag-summary-overlay';
  overlay.id = 'pfTagSummaryModal';
  overlay.setAttribute('role', 'dialog');
  overlay.setAttribute('aria-modal', 'true');
  overlay.setAttribute('aria-labelledby', 'pfTagSummaryTitle');
  overlay.innerHTML = `
    <div class="pf-modal pf-tag-summary-modal">
      <div class="pf-modal-header">
        <h3 id="pfTagSummaryTitle">#${escapeHtml(displayTag)}</h3>
        <button type="button" class="pf-modal-close" onclick="pfCloseTagSummary()" aria-label="닫기">&times;</button>
      </div>
      <div class="pf-modal-body pf-tag-summary-body">
        <div class="pf-tag-summary-grid">
          ${_pfTagSummaryCard('태그 일간', fmtPct(tagStats.dailyReturnPct), fmtSignedKrw(tagStats.dailyPnl), returnClass(tagStats.dailyReturnPct))}
          ${_pfTagSummaryCard('전체 일간', fmtPct(allStats.dailyReturnPct), fmtSignedKrw(allStats.dailyPnl), returnClass(allStats.dailyReturnPct))}
          ${_pfTagSummaryCard('초과', fmtPct(spreadPct), 'vs 전체', returnClass(spreadPct))}
          ${_pfTagSummaryCard('전체 비중', portfolioWeight !== null ? fmtPct(portfolioWeight, false) : '-', `${tagStats.count.toLocaleString()}종목`, '')}
        </div>
        ${_pfRenderTagSummaryRows(taggedRows, tagStats)}
      </div>
    </div>`;
  overlay.addEventListener('click', e => {
    if (e.target === overlay) pfCloseTagSummary();
  });
  document.body.appendChild(overlay);
  overlay.querySelector('.pf-modal-close')?.focus();
}

(function initPfTagSummaryKeys() {
  document.addEventListener('keydown', e => {
    if (e.key === 'Escape' && document.getElementById('pfTagSummaryModal')) {
      pfCloseTagSummary();
    }
  });
})();
