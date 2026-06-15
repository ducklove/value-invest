// Group weight 100% stacked-area trend chart (성과 탭) + drill-down hooks
// into portfolio-group-composition.js. _GROUP_WEIGHT_COLORS is shared with
// the composition chart, so this file loads before it.
// Split from static/js/portfolio-trends.js to keep trend features maintainable.
let _groupWeightChartResizeObserver = null;
let _groupWeightChartRenderSeq = 0;
let _groupWeightChartInstance = null;
let _groupWeightChartData = [];
let _groupWeightSeriesForAxis = [];

const _GROUP_WEIGHT_COLORS = [
  '#2563eb', '#dc2626', '#16a34a', '#f59e0b', '#7c3aed', '#0891b2',
  '#db2777', '#65a30d', '#ea580c', '#475569', '#0f766e', '#9333ea',
];

function _prepareGroupWeightChartData(rows) {
  const cleanRows = (Array.isArray(rows) ? rows : [])
    .map(row => ({
      date: String(row.date || ''),
      group: String(row.group_name || '기타'),
      weight: Number(row.weight_pct),
      value: Number(row.market_value),
      stockCount: Number(row.stock_count),
    }))
    .filter(row => row.date && Number.isFinite(row.weight));
  const dates = Array.from(new Set(cleanRows.map(row => row.date))).sort();
  const latestDate = dates[dates.length - 1] || null;
  const latestWeights = {};
  const totalValues = {};
  cleanRows.forEach(row => {
    totalValues[row.group] = (totalValues[row.group] || 0) + Math.abs(Number.isFinite(row.value) ? row.value : 0);
    if (row.date === latestDate) latestWeights[row.group] = row.weight;
  });
  const groups = Array.from(new Set(cleanRows.map(row => row.group))).sort((a, b) => {
    const latestDiff = (latestWeights[b] ?? -Infinity) - (latestWeights[a] ?? -Infinity);
    if (latestDiff) return latestDiff;
    const valueDiff = (totalValues[b] || 0) - (totalValues[a] || 0);
    if (valueDiff) return valueDiff;
    return a.localeCompare(b);
  });
  const byDateGroup = {};
  cleanRows.forEach(row => {
    const key = `${row.date}::${row.group}`;
    byDateGroup[key] = row.weight;
  });
  const latest = cleanRows
    .filter(row => row.date === latestDate)
    .sort((a, b) => b.weight - a.weight);
  return { dates, groups, byDateGroup, latest };
}

async function renderGroupWeightChart(rows) {
  const container = document.getElementById('pfGroupWeightChart');
  if (!container) return;
  const renderSeq = ++_groupWeightChartRenderSeq;
  if (_groupWeightChartInstance) {
    _groupWeightChartInstance.dispose();
    _groupWeightChartInstance = null;
  }
  if (_groupWeightChartResizeObserver) {
    _groupWeightChartResizeObserver.disconnect();
    _groupWeightChartResizeObserver = null;
  }
  _groupWeightChartData = Array.isArray(rows) ? rows : [];
  _groupWeightSeriesForAxis = [];

  const statsEl = document.getElementById('pfGroupWeightStats');
  if (statsEl) statsEl.innerHTML = '';

  const containerReady = await _waitForChartContainer(container);
  if (!containerReady && container.offsetParent === null) return;
  if (renderSeq !== _groupWeightChartRenderSeq) return;

  const prepared = _prepareGroupWeightChartData(_groupWeightChartData);
  if (!prepared.dates.length || !prepared.groups.length) {
    container.innerHTML = '<div class="pf-chart-message">그룹 비중 스냅샷이 아직 없습니다.</div>';
    _updateChartRangeLabel('pfGroupWeightRange', [], 0, 0);
    return;
  }
  if (typeof PortfolioTrendChart === 'undefined') {
    container.innerHTML = '<div class="pf-chart-message">차트 렌더러를 불러오지 못했습니다.</div>';
    return;
  }

  const textColor = getComputedStyle(document.documentElement).getPropertyValue('--text-secondary').trim() || '#888';
  const gridColor = getComputedStyle(document.documentElement).getPropertyValue('--border').trim() || '#333';
  const mobileChartMode = _isMobileChartMode();
  const dateObjects = prepared.dates.map(date => ({ date }));
  const series = prepared.groups.map((group, idx) => {
    const color = _GROUP_WEIGHT_COLORS[idx % _GROUP_WEIGHT_COLORS.length];
    return {
      name: group,
      type: 'line',
      data: prepared.dates.map(date => {
        const value = prepared.byDateGroup[`${date}::${group}`];
        return Number.isFinite(value) ? Number(value.toFixed(2)) : 0;
      }),
      stack: 'groupWeight',
      smooth: 0.25,
      symbol: 'none',
      lineStyle: { color, width: prepared.groups.length > 8 ? 1 : 1.3 },
      itemStyle: { color },
      areaStyle: { opacity: 0.34 },
      tooltipSuffix: '%',
    };
  });

  _groupWeightChartInstance = PortfolioTrendChart.create(container, {
    legend: {
      data: prepared.groups,
      top: 0,
      right: 0,
      textStyle: { color: textColor, fontSize: 11 },
      itemWidth: 18,
      itemHeight: 2,
    },
    grid: { left: 52, right: 12, top: 28, bottom: mobileChartMode ? 24 : 56 },
    dataZoom: mobileChartMode ? [] : [{ start: 0, end: 100 }],
    xAxis: {
      type: 'category',
      data: prepared.dates,
      axisLine: { lineStyle: { color: gridColor } },
      axisLabel: { color: textColor, fontSize: 10 },
    },
    yAxis: {
      type: 'value',
      min: 0,
      max: 100,
      axisLabel: {
        color: textColor,
        fontSize: 10,
        formatter: v => `${Number(v).toFixed(0)}%`,
      },
      splitLine: { lineStyle: { color: gridColor, width: 0.5 } },
    },
    series,
  });
  _kickChartResize(_groupWeightChartInstance);

  _groupWeightSeriesForAxis = series.map(item => _chartDataToNumbers(item.data));
  const fullWindow = _chartZoomWindow(prepared.dates.length, 0, 100);
  _updateChartRangeLabel('pfGroupWeightRange', dateObjects, fullWindow.startIdx, fullWindow.endIdx);

  if (statsEl) {
    statsEl.innerHTML = prepared.latest.slice(0, 8).map(row => {
      const value = `${row.weight.toFixed(1)}%`;
      const count = Number.isFinite(row.stockCount) ? ` · ${row.stockCount.toLocaleString()}종목` : '';
      const title = Number.isFinite(row.value) ? ` title="${escapeHtml(fmtKrw(row.value))}"` : '';
      return `<div class="pf-nav-ret-card js-pf-group-weight-card" data-group="${escapeHtml(row.group)}"${title}><div class="pf-nav-ret-label">${escapeHtml(row.group)}${count}</div><div class="pf-nav-ret-value">${value}</div></div>`;
    }).join('');
    statsEl.querySelectorAll('.js-pf-group-weight-card').forEach(card => {
      card.addEventListener('click', () => {
        if (typeof pfShowGroupComposition === 'function') {
          pfShowGroupComposition(card.dataset.group || '');
        }
      });
    });
  }

  if (typeof ResizeObserver !== 'undefined' && _groupWeightChartInstance) {
    const ro = new ResizeObserver(() => {
      if (_groupWeightChartInstance && _groupWeightChartInstance.resize) {
        _groupWeightChartInstance.resize();
      }
    });
    ro.observe(container);
    _groupWeightChartResizeObserver = ro;
  }

  if (_groupWeightChartInstance && typeof _groupWeightChartInstance.on === 'function') {
    let _zoomTimer = null;
    _groupWeightChartInstance.on('datazoom', () => {
      clearTimeout(_zoomTimer);
      _zoomTimer = setTimeout(() => {
        const { startIdx, endIdx } = _chartWindowFromInstance(
          _groupWeightChartInstance,
          prepared.dates.length,
        );
        _updateChartRangeLabel('pfGroupWeightRange', dateObjects, startIdx, endIdx);
      }, 80);
    });
  }
}
