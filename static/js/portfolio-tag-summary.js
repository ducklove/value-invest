// Tag-level portfolio popup: list tagged holdings and compare daily weighted return.
function _pfNormalizeTagKey(tag) {
  return String(tag || '').trim().replace(/^#/, '').toLowerCase();
}

function _pfNumberOrNull(value) {
  if (value == null || value === '') return null;
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

function _pfTagSummaryTileColor(changePct) {
  const value = _pfNumberOrNull(changePct);
  if (value === null) return 'rgba(148, 163, 184, 0.16)';
  const clamped = Math.max(-6, Math.min(6, value));
  const alpha = 0.10 + (Math.abs(clamped) / 6) * 0.30;
  if (clamped > 0) return `rgba(220, 38, 38, ${alpha.toFixed(3)})`;
  if (clamped < 0) return `rgba(37, 99, 235, ${alpha.toFixed(3)})`;
  return 'rgba(148, 163, 184, 0.12)';
}

function _pfTagSummaryTreemapLayout(items, x = 0, y = 0, width = 100, height = 100) {
  const total = items.reduce((sum, item) => sum + item.value, 0);
  if (!items.length || total <= 0 || width <= 0 || height <= 0) return [];
  if (items.length === 1) return [{ ...items[0], x, y, width, height }];

  const half = total / 2;
  let acc = 0;
  let split = 1;
  for (let i = 0; i < items.length - 1; i += 1) {
    const next = acc + items[i].value;
    if (i > 0 && Math.abs(half - acc) < Math.abs(half - next)) break;
    acc = next;
    split = i + 1;
  }

  const first = items.slice(0, split);
  const second = items.slice(split);
  const firstTotal = first.reduce((sum, item) => sum + item.value, 0);
  const ratio = firstTotal / total;

  if (width >= height) {
    const firstWidth = width * ratio;
    return [
      ..._pfTagSummaryTreemapLayout(first, x, y, firstWidth, height),
      ..._pfTagSummaryTreemapLayout(second, x + firstWidth, y, width - firstWidth, height),
    ];
  }
  const firstHeight = height * ratio;
  return [
    ..._pfTagSummaryTreemapLayout(first, x, y, width, firstHeight),
    ..._pfTagSummaryTreemapLayout(second, x, y + firstHeight, width, height - firstHeight),
  ];
}

function _pfTreemapTileStyle(tile) {
  return [
    `left:${tile.x.toFixed(4)}%`,
    `top:${tile.y.toFixed(4)}%`,
    `width:${tile.width.toFixed(4)}%`,
    `height:${tile.height.toFixed(4)}%`,
    `background:${_pfTagSummaryTileColor(tile.row.changePct)}`,
  ].join(';');
}

function _pfRenderTagSummaryTreemap(rows, tagStats, portfolioWeight, scopeLabel = '태그') {
  const pricedRows = rows
    .filter(row => Number.isFinite(row.marketValue) && row.marketValue > 0)
    .sort((a, b) => b.marketValue - a.marketValue);
  const tagTotal = tagStats.marketValue;
  const layout = _pfTagSummaryTreemapLayout(pricedRows.map(row => ({
    row,
    value: row.marketValue,
    pct: tagTotal > 0 ? row.marketValue / tagTotal * 100 : null,
  })));
  const treemapTiles = layout.map(tile => {
    const row = tile.row;
    const changeText = row.changePct !== null ? fmtPct(row.changePct) : '-';
    return `
      <div class="pf-tag-summary-treemap-tile" style="${_pfTreemapTileStyle(tile)}">
        <div class="pf-tag-summary-treemap-main">
          <strong>${escapeHtml(row.stock_name || row.stock_code || '-')}</strong>
          <span>${escapeHtml(row.stock_code || '')}</span>
        </div>
        <div class="pf-tag-summary-treemap-metrics">
          <span class="${returnClass(row.changePct)}">${changeText}</span>
          <strong>${tile.pct !== null ? fmtPct(tile.pct, false) : '-'}</strong>
        </div>
      </div>`;
  }).join('');

  return `
    <div class="pf-tag-summary-treemap-panel">
      <div class="pf-tag-summary-treemap-head">
        <div class="pf-tag-summary-section-title">${scopeLabel} 영역지도</div>
        <strong>${portfolioWeight !== null ? fmtPct(portfolioWeight, false) : '-'}</strong>
      </div>
      <div class="pf-tag-summary-treemap" role="img" aria-label="${scopeLabel} 평가금액 비중만큼 면적을 할당한 영역지도">
        ${treemapTiles || '<div class="pf-tag-summary-empty small">평가금액 데이터가 없습니다.</div>'}
      </div>
    </div>`;
}

function _pfRenderTagSummaryRows(rows, tagStats, scopeLabel = '태그') {
  if (!rows.length) {
    return `<div class="pf-tag-summary-empty">해당 ${scopeLabel}에 속한 종목이 없습니다.</div>`;
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
            <th>${scopeLabel} 비중</th>
          </tr>
        </thead>
        <tbody>${body}</tbody>
      </table>
    </div>`;
}

function _pfRenderTagSummaryComposition(rows, tagStats, portfolioWeight, scopeLabel = '태그') {
  if (!rows.length) return _pfRenderTagSummaryRows(rows, tagStats, scopeLabel);
  return `
    <div class="pf-tag-summary-composition">
      ${_pfRenderTagSummaryTreemap(rows, tagStats, portfolioWeight, scopeLabel)}
      <div class="pf-tag-summary-list-panel">
        <div class="pf-tag-summary-section-title">목록</div>
        ${_pfRenderTagSummaryRows(rows, tagStats, scopeLabel)}
      </div>
    </div>`;
}

function _pfGroupPopupHistory(rows, groupName) {
  const first = rows.find(row => row.group_name === groupName);
  if (!first) return [];
  const byDate = new Map();
  for (const row of rows) {
    if (row.date < first.date) continue;
    if (row.group_name === groupName) {
      byDate.set(row.date, row);
    } else if (!byDate.has(row.date)) {
      byDate.set(row.date, {
        date: row.date,
        market_value: 0,
        total_value: row.total_value,
        snapshot_total_value: row.snapshot_total_value,
      });
    }
  }
  return [...byDate.values()];
}

function pfCloseTagSummary() {
  const modal = document.getElementById('pfTagSummaryModal');
  if (modal) closeManagedModal(modal, { remove: true });
}

function _pfOpenPortfolioSliceSummary({
  displayName,
  title,
  scopeLabel,
  rows,
  trendPath,
  trendOptions,
  trendBasis,
}) {
  if (!displayName) return;
  const allRows = _pfTagSummaryRows();
  const selectedRows = rows(allRows)
    .sort((a, b) => Math.abs(b.marketValue || 0) - Math.abs(a.marketValue || 0));
  const allStats = _pfDailyWeightedStats(allRows);
  const sliceStats = _pfDailyWeightedStats(selectedRows);
  const spreadPct = sliceStats.dailyReturnPct !== null && allStats.dailyReturnPct !== null
    ? sliceStats.dailyReturnPct - allStats.dailyReturnPct
    : null;
  const portfolioWeight = allStats.pricedCount === allStats.count && allStats.marketValue > 0
    ? sliceStats.marketValue / allStats.marketValue * 100
    : null;
  const portfolioLabel = PfStore.accountId ? '계좌' : '전체';

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
        <h3 id="pfTagSummaryTitle">${escapeHtml(title)}</h3>
        <button type="button" class="pf-modal-close" onclick="pfCloseTagSummary()" aria-label="닫기">&times;</button>
      </div>
      <div class="pf-modal-body pf-tag-summary-body">
        <div class="pf-tag-summary-grid">
          ${_pfTagSummaryCard(`${scopeLabel} 일간`, fmtPct(sliceStats.dailyReturnPct), fmtSignedKrw(sliceStats.dailyPnl), returnClass(sliceStats.dailyReturnPct))}
          ${_pfTagSummaryCard(`${portfolioLabel} 일간`, fmtPct(allStats.dailyReturnPct), fmtSignedKrw(allStats.dailyPnl), returnClass(allStats.dailyReturnPct))}
          ${_pfTagSummaryCard('초과', fmtPct(spreadPct), `vs ${portfolioLabel}`, returnClass(spreadPct))}
          ${_pfTagSummaryCard(`${portfolioLabel} 비중`, portfolioWeight !== null ? fmtPct(portfolioWeight, false) : '-', pfFmtPortfolioValue(sliceStats.marketValue), '')}
        </div>
        ${_pfRenderTagSummaryComposition(selectedRows, sliceStats, portfolioWeight, scopeLabel)}
        ${_pfRenderPopupTrendShell('평가금액 합', trendBasis)}
      </div>
    </div>`;
  overlay.addEventListener('click', e => {
    if (e.target === overlay) pfCloseTagSummary();
  });
  document.body.appendChild(overlay);
  const trends = overlay.querySelector('[data-pf-popup-trends]');
  openManagedModal(overlay, {
    initialFocus: '.pf-modal-close',
    onEscape: pfCloseTagSummary,
    onClose: () => {
      _pfDisposePopupTrends(trends);
      overlay.remove();
    },
  });
  void _pfLoadPopupTrends(trends, trendPath, {
    valueTitle: '평가금액 합', scopeLabel, ...trendOptions,
  });
}

function pfOpenTagSummary(tag) {
  const tagKey = _pfNormalizeTagKey(tag);
  if (!tagKey) return;
  const allRows = _pfTagSummaryRows();
  const displayTag = allRows
    .filter(row => row.tags.some(t => _pfNormalizeTagKey(t) === tagKey))
    .flatMap(row => row.tags)
    .find(t => _pfNormalizeTagKey(t) === tagKey) || tag;
  _pfOpenPortfolioSliceSummary({
    displayName: displayTag,
    title: `#${displayTag}`,
    scopeLabel: '태그',
    rows: rows => rows.filter(row => row.tags.some(t => _pfNormalizeTagKey(t) === tagKey)),
    trendPath: `/api/portfolio/tag-history?tag=${encodeURIComponent(displayTag)}`,
    trendOptions: { valueKey: 'tag_value' },
    trendBasis: '전체 계좌 정산 · 현재 태그가 붙은 종목 기준',
  });
}

function pfOpenGroupSummary(groupName) {
  _pfOpenPortfolioSliceSummary({
    displayName: groupName,
    title: groupName,
    scopeLabel: '그룹',
    rows: rows => rows.filter(row => pfGetGroup(row) === groupName),
    trendPath: '/api/portfolio/group-weight-history',
    trendOptions: { selectRows: rows => _pfGroupPopupHistory(rows, groupName) },
    trendBasis: '전체 계좌 정산 · 날짜별 저장된 그룹 기준',
  });
}
