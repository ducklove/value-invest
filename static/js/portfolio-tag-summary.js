// Tag-level portfolio popup: list tagged holdings and compare daily weighted return.
let _pfTagSummaryLoadSeq = 0;
let _pfTagValueChartInstance = null;
let _pfTagWeightChartInstance = null;
let _pfTagValueChartResizeObserver = null;
let _pfTagWeightChartResizeObserver = null;

function _pfNormalizeTagKey(tag) {
  return String(tag || '').trim().replace(/^#/, '').toLowerCase();
}

function _pfNormalizeGroupKey(group) {
  return String(group || '').trim().toLowerCase();
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

function _pfRenderTagSummaryTrendShell() {
  return `
    <div class="pf-tag-summary-trends">
      <div class="pf-tag-summary-trend-panel">
        <div class="pf-tag-summary-trend-head">
          <strong>평가금액 합 추세</strong>
          <span id="pfTagValueRange"></span>
        </div>
        <div class="pf-tag-summary-chart" id="pfTagValueTrend"></div>
      </div>
      <div class="pf-tag-summary-trend-panel">
        <div class="pf-tag-summary-trend-head">
          <strong>포트폴리오 비중 추세</strong>
          <span id="pfTagWeightRange"></span>
        </div>
        <div class="pf-tag-summary-chart" id="pfTagWeightTrend"></div>
      </div>
    </div>`;
}

function _pfDisposeTagSummaryCharts() {
  if (_pfTagValueChartInstance) {
    _pfTagValueChartInstance.dispose();
    _pfTagValueChartInstance = null;
  }
  if (_pfTagWeightChartInstance) {
    _pfTagWeightChartInstance.dispose();
    _pfTagWeightChartInstance = null;
  }
  if (_pfTagValueChartResizeObserver) {
    _pfTagValueChartResizeObserver.disconnect();
    _pfTagValueChartResizeObserver = null;
  }
  if (_pfTagWeightChartResizeObserver) {
    _pfTagWeightChartResizeObserver.disconnect();
    _pfTagWeightChartResizeObserver = null;
  }
}

function _pfSetTagSummaryTrendMessage(message) {
  ['pfTagValueTrend', 'pfTagWeightTrend'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.innerHTML = `<div class="pf-chart-message">${escapeHtml(message)}</div>`;
  });
  ['pfTagValueRange', 'pfTagWeightRange'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.textContent = '';
  });
}

function _pfTagTrendValue(row) {
  const value = _pfNumberOrNull(row?.tag_value);
  if (value === null) return null;
  if (PfStore.currency.unit === 'USD') {
    const fx = _pfNumberOrNull(row?.fx_usdkrw);
    return fx && fx > 0 ? value / fx : pfFx(value);
  }
  return value;
}

function _pfUpdateTagTrendRangeLabel(elId, rows, startIdx, endIdx) {
  const el = document.getElementById(elId);
  if (!el) return;
  if (!rows?.length || !rows[startIdx] || !rows[endIdx]) {
    el.textContent = '';
    return;
  }
  const start = rows[startIdx].date;
  const end = rows[endIdx].date;
  const days = Math.max(0, Math.round((new Date(end) - new Date(start)) / 86400000)) + 1;
  el.textContent = `${start} ~ ${end} · ${days.toLocaleString()}일`;
}

async function _pfCreateTagTrendChart({
  containerId,
  rangeId,
  rows,
  values,
  title,
  color,
  axisFormatter,
  tooltipSuffix = '',
  yZero = false,
}) {
  const container = document.getElementById(containerId);
  if (!container) return null;
  if (!rows.length || !values.some(v => Number.isFinite(v))) {
    container.innerHTML = '<div class="pf-chart-message">일간 스냅샷 데이터가 없습니다.</div>';
    _pfUpdateTagTrendRangeLabel(rangeId, [], 0, 0);
    return null;
  }
  if (typeof PortfolioTrendChart === 'undefined') {
    container.innerHTML = '<div class="pf-chart-message">차트 렌더러를 불러오지 못했습니다.</div>';
    return null;
  }
  const waitForContainer = typeof _waitForChartContainer === 'function'
    ? _waitForChartContainer
    : async () => true;
  const ready = await waitForContainer(container);
  if (!ready && container.offsetParent === null) return null;

  const mobileChartMode = typeof _isMobileChartMode === 'function' && _isMobileChartMode();
  const labels = rows.map(row => row.date);
  const chart = PortfolioTrendChart.create(container, {
    grid: { left: containerId === 'pfTagValueTrend' ? 70 : 54, right: 12, top: 12, bottom: mobileChartMode ? 24 : 52 },
    dataZoom: mobileChartMode ? [] : [{ start: 0, end: 100 }],
    xAxis: { type: 'category', data: labels },
    yAxis: {
      type: 'value',
      min: yZero ? 0 : 'dataMin',
      axisLabel: { formatter: axisFormatter },
    },
    series: [{
      name: title,
      type: 'line',
      data: values.map(v => Number.isFinite(v) ? v : '-'),
      smooth: 0.3,
      symbol: values.length > 45 ? 'none' : 'circle',
      lineStyle: { color, width: 2 },
      itemStyle: { color },
      areaStyle: {},
      tooltipSuffix,
    }],
  });

  const fullWindow = typeof _chartZoomWindow === 'function'
    ? _chartZoomWindow(rows.length, 0, 100)
    : { startIdx: 0, endIdx: rows.length - 1 };
  if (typeof _applyVisibleYAxis === 'function') {
    _applyVisibleYAxis(chart, [values], fullWindow.startIdx, fullWindow.endIdx, yZero);
  }
  _pfUpdateTagTrendRangeLabel(rangeId, rows, fullWindow.startIdx, fullWindow.endIdx);

  if (typeof ResizeObserver !== 'undefined') {
    const ro = new ResizeObserver(() => {
      if (chart && chart.resize) chart.resize();
    });
    ro.observe(container);
    if (containerId === 'pfTagValueTrend') _pfTagValueChartResizeObserver = ro;
    else _pfTagWeightChartResizeObserver = ro;
  }
  if (typeof _kickChartResize === 'function') _kickChartResize(chart);

  if (chart && typeof chart.on === 'function') {
    let zoomTimer = null;
    chart.on('datazoom', () => {
      clearTimeout(zoomTimer);
      zoomTimer = setTimeout(() => {
        const dz = chart.getOption?.()?.dataZoom?.[0];
        const startPct = dz?.start ?? 0;
        const endPct = dz?.end ?? 100;
        const win = typeof _chartZoomWindow === 'function'
          ? _chartZoomWindow(rows.length, startPct, endPct)
          : { startIdx: 0, endIdx: rows.length - 1 };
        if (typeof _applyVisibleYAxis === 'function') {
          _applyVisibleYAxis(chart, [values], win.startIdx, win.endIdx, yZero);
        }
        _pfUpdateTagTrendRangeLabel(rangeId, rows, win.startIdx, win.endIdx);
      }, 80);
    });
  }
  return chart;
}

async function _pfRenderTagSummaryTrendCharts(rows) {
  _pfDisposeTagSummaryCharts();
  const cleanedRows = Array.isArray(rows)
    ? rows.filter(row => row && row.date)
    : [];
  if (!cleanedRows.length) {
    _pfSetTagSummaryTrendMessage('일간 스냅샷 데이터가 없습니다.');
    return;
  }

  const values = cleanedRows.map(_pfTagTrendValue);
  const weights = cleanedRows.map(row => _pfNumberOrNull(row.weight_pct));
  const firstValue = values.find(v => Number.isFinite(v));
  const lastValue = values.slice().reverse().find(v => Number.isFinite(v));
  const valuePct = firstValue && lastValue ? ((lastValue / firstValue) - 1) * 100 : 0;
  const latestWeight = weights.slice().reverse().find(v => Number.isFinite(v)) ?? 0;
  const valueColor = typeof returnToColor === 'function' ? returnToColor(valuePct, 20) : '#2563eb';
  const weightColor = typeof returnToColor === 'function' ? returnToColor(latestWeight, 30) : '#059669';
  const valueDiv = pfFxDivisor();
  const valueUnit = pfFxUnit();
  const valueSymbol = pfFxSymbol();

  _pfTagValueChartInstance = await _pfCreateTagTrendChart({
    containerId: 'pfTagValueTrend',
    rangeId: 'pfTagValueRange',
    rows: cleanedRows,
    values,
    title: '평가금액 합',
    color: valueColor,
    axisFormatter: v => `${valueSymbol}${(v / valueDiv).toFixed(PfStore.currency.unit === 'USD' ? 2 : 1)}${valueUnit}`,
    yZero: true,
  });
  _pfTagWeightChartInstance = await _pfCreateTagTrendChart({
    containerId: 'pfTagWeightTrend',
    rangeId: 'pfTagWeightRange',
    rows: cleanedRows,
    values: weights,
    title: '포트폴리오 비중',
    color: weightColor,
    axisFormatter: v => `${Number(v).toFixed(1)}%`,
    tooltipSuffix: '%',
    yZero: true,
  });
}

async function _pfLoadTagSummaryTrends(tag) {
  const loadSeq = ++_pfTagSummaryLoadSeq;
  _pfSetTagSummaryTrendMessage('태그 추세 데이터를 불러오는 중입니다...');
  try {
    const fetchJson = typeof _pfFetchJson === 'function'
      ? _pfFetchJson
      : async (path, label = 'Tag history') => apiFetchJson(path, { errorMessage: `${label} request failed` });
    const rows = await fetchJson(`/api/portfolio/tag-history?tag=${encodeURIComponent(tag)}`, 'Tag history');
    if (loadSeq !== _pfTagSummaryLoadSeq || !document.getElementById('pfTagSummaryModal')) return;
    await _pfRenderTagSummaryTrendCharts(rows);
  } catch (err) {
    if (loadSeq !== _pfTagSummaryLoadSeq) return;
    const message = err?.message || '태그 추세 데이터를 불러오지 못했습니다.';
    _pfSetTagSummaryTrendMessage(message);
    if (err?.status !== 401) console.warn(err);
  }
}

async function _pfLoadGroupSummaryTrends(groupName) {
  const loadSeq = ++_pfTagSummaryLoadSeq;
  const groupKey = _pfNormalizeGroupKey(groupName);
  _pfSetTagSummaryTrendMessage('그룹 추세 데이터를 불러오는 중입니다...');
  try {
    const fetchJson = typeof _pfFetchJson === 'function'
      ? _pfFetchJson
      : async (path, label = 'Group history') => apiFetchJson(path, { errorMessage: `${label} request failed` });
    const rows = await fetchJson('/api/portfolio/group-weight-history', 'Group weight history');
    if (loadSeq !== _pfTagSummaryLoadSeq || !document.getElementById('pfTagSummaryModal')) return;
    const groupRows = (Array.isArray(rows) ? rows : [])
      .filter(row => _pfNormalizeGroupKey(row?.group_name) === groupKey)
      .map(row => ({
        date: row.date,
        tag_value: row.market_value,
        weight_pct: row.weight_pct,
        fx_usdkrw: row.fx_usdkrw,
      }));
    await _pfRenderTagSummaryTrendCharts(groupRows);
  } catch (err) {
    if (loadSeq !== _pfTagSummaryLoadSeq) return;
    const message = err?.message || '그룹 추세 데이터를 불러오지 못했습니다.';
    _pfSetTagSummaryTrendMessage(message);
    if (err?.status !== 401) console.warn(err);
  }
}

function pfCloseTagSummary() {
  _pfTagSummaryLoadSeq += 1;
  _pfDisposeTagSummaryCharts();
  const modal = document.getElementById('pfTagSummaryModal');
  if (modal) closeManagedModal(modal, { remove: true });
}

function _pfOpenPortfolioSliceSummary({
  displayName,
  title,
  scopeLabel,
  rows,
  loadTrends,
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
  const portfolioWeight = allStats.marketValue > 0
    ? sliceStats.marketValue / allStats.marketValue * 100
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
        <h3 id="pfTagSummaryTitle">${escapeHtml(title)}</h3>
        <button type="button" class="pf-modal-close" onclick="pfCloseTagSummary()" aria-label="닫기">&times;</button>
      </div>
      <div class="pf-modal-body pf-tag-summary-body">
        <div class="pf-tag-summary-grid">
          ${_pfTagSummaryCard(`${scopeLabel} 일간`, fmtPct(sliceStats.dailyReturnPct), fmtSignedKrw(sliceStats.dailyPnl), returnClass(sliceStats.dailyReturnPct))}
          ${_pfTagSummaryCard('전체 일간', fmtPct(allStats.dailyReturnPct), fmtSignedKrw(allStats.dailyPnl), returnClass(allStats.dailyReturnPct))}
          ${_pfTagSummaryCard('초과', fmtPct(spreadPct), 'vs 전체', returnClass(spreadPct))}
          ${_pfTagSummaryCard('전체 비중', portfolioWeight !== null ? fmtPct(portfolioWeight, false) : '-', pfFmtPortfolioValue(sliceStats.marketValue), '')}
        </div>
        ${_pfRenderTagSummaryComposition(selectedRows, sliceStats, portfolioWeight, scopeLabel)}
        ${_pfRenderTagSummaryTrendShell()}
      </div>
    </div>`;
  overlay.addEventListener('click', e => {
    if (e.target === overlay) pfCloseTagSummary();
  });
  document.body.appendChild(overlay);
  openManagedModal(overlay, {
    initialFocus: '.pf-modal-close',
    onEscape: pfCloseTagSummary,
  });
  void loadTrends(displayName);
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
    loadTrends: _pfLoadTagSummaryTrends,
  });
}

function pfOpenGroupSummary(groupName) {
  const groupKey = _pfNormalizeGroupKey(groupName);
  if (!groupKey) return;
  const allRows = _pfTagSummaryRows();
  const displayGroup = allRows
    .map(row => pfGetGroup(row))
    .find(name => _pfNormalizeGroupKey(name) === groupKey) || groupName;
  _pfOpenPortfolioSliceSummary({
    displayName: displayGroup,
    title: displayGroup,
    scopeLabel: '그룹',
    rows: rows => rows.filter(row => _pfNormalizeGroupKey(pfGetGroup(row)) === groupKey),
    loadTrends: _pfLoadGroupSummaryTrends,
  });
}
