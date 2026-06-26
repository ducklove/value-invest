// Tag-level portfolio popup: list tagged holdings and compare daily weighted return.
let _pfTagSummaryLoadSeq = 0;
let _pfTagValueChartInstance = null;
let _pfTagWeightChartInstance = null;
let _pfTagValueChartResizeObserver = null;
let _pfTagWeightChartResizeObserver = null;

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

function _pfTagSummaryColor(index) {
  const palette = ['#2563eb', '#dc2626', '#059669', '#d97706', '#7c3aed', '#0891b2', '#be185d', '#4f46e5'];
  return palette[index % palette.length];
}

function _pfRenderTagSummaryPie(rows, tagStats, portfolioWeight) {
  const pricedRows = rows
    .filter(row => Number.isFinite(row.marketValue) && row.marketValue > 0)
    .sort((a, b) => b.marketValue - a.marketValue);
  const tagTotal = tagStats.marketValue;
  const hasData = tagTotal > 0 && pricedRows.length > 0;
  let cursor = 0;
  const stops = [];
  if (hasData) {
    pricedRows.forEach((row, idx) => {
      const pct = row.marketValue / tagTotal * 100;
      const next = idx === pricedRows.length - 1 ? 100 : Math.min(100, cursor + pct);
      stops.push(`${_pfTagSummaryColor(idx)} ${cursor.toFixed(2)}% ${next.toFixed(2)}%`);
      cursor = next;
    });
  }
  const pieBackground = hasData ? `conic-gradient(${stops.join(', ')})` : 'var(--border)';
  const legendRows = pricedRows.slice(0, 6).map((row, idx) => {
    const pct = tagTotal > 0 ? row.marketValue / tagTotal * 100 : null;
    return `
      <div class="pf-tag-summary-legend-row">
        <span class="pf-tag-summary-swatch" style="background:${_pfTagSummaryColor(idx)}"></span>
        <span class="pf-tag-summary-legend-name">${escapeHtml(row.stock_name || row.stock_code || '-')}</span>
        <strong>${pct !== null ? fmtPct(pct, false) : '-'}</strong>
      </div>`;
  }).join('');

  return `
    <div class="pf-tag-summary-pie-panel">
      <div class="pf-tag-summary-section-title">태그 비중</div>
      <div class="pf-tag-summary-pie-wrap">
        <div class="pf-tag-summary-pie" style="background:${pieBackground}" role="img" aria-label="태그 비중 파이 차트"></div>
        <div class="pf-tag-summary-pie-center">
          <strong>${portfolioWeight !== null ? fmtPct(portfolioWeight, false) : '-'}</strong>
          <span>포트폴리오</span>
        </div>
      </div>
      <div class="pf-tag-summary-legend">
        ${legendRows || '<div class="pf-tag-summary-empty small">평가금액 데이터가 없습니다.</div>'}
      </div>
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

function _pfRenderTagSummaryComposition(rows, tagStats, portfolioWeight) {
  if (!rows.length) return _pfRenderTagSummaryRows(rows, tagStats);
  return `
    <div class="pf-tag-summary-composition">
      ${_pfRenderTagSummaryPie(rows, tagStats, portfolioWeight)}
      <div class="pf-tag-summary-list-panel">
        <div class="pf-tag-summary-section-title">목록</div>
        ${_pfRenderTagSummaryRows(rows, tagStats)}
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
      : async path => {
          const resp = await apiFetch(path);
          if (!resp.ok) throw new Error(`Tag history request failed (${resp.status})`);
          return await resp.json();
        };
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

function pfCloseTagSummary() {
  _pfTagSummaryLoadSeq += 1;
  _pfDisposeTagSummaryCharts();
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
          ${_pfTagSummaryCard('전체 비중', portfolioWeight !== null ? fmtPct(portfolioWeight, false) : '-', pfFmtPortfolioValue(tagStats.marketValue), '')}
        </div>
        ${_pfRenderTagSummaryComposition(taggedRows, tagStats, portfolioWeight)}
        ${_pfRenderTagSummaryTrendShell()}
      </div>
    </div>`;
  overlay.addEventListener('click', e => {
    if (e.target === overlay) pfCloseTagSummary();
  });
  document.body.appendChild(overlay);
  overlay.querySelector('.pf-modal-close')?.focus();
  void _pfLoadTagSummaryTrends(displayTag);
}

(function initPfTagSummaryKeys() {
  document.addEventListener('keydown', e => {
    if (e.key === 'Escape' && document.getElementById('pfTagSummaryModal')) {
      pfCloseTagSummary();
    }
  });
})();
