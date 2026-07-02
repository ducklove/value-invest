// Group drill-down chart for the portfolio performance tab.
let _groupCompositionChartInstance = null;
let _groupCompositionResizeObserver = null;
let _groupCompositionSelected = '';
let _groupCompositionRequestSeq = 0;
let _groupCompositionRenderSeq = 0;

const _GROUP_COMPOSITION_MAX_SERIES = 12;

function _disposeGroupCompositionChart() {
  if (_groupCompositionChartInstance) {
    _groupCompositionChartInstance.dispose();
    _groupCompositionChartInstance = null;
  }
  if (_groupCompositionResizeObserver) {
    _groupCompositionResizeObserver.disconnect();
    _groupCompositionResizeObserver = null;
  }
}

function pfHideGroupComposition() {
  _groupCompositionRequestSeq += 1;
  _disposeGroupCompositionChart();
  _groupCompositionSelected = '';
  document.querySelectorAll('.js-pf-group-weight-card').forEach(card => card.classList.remove('is-active'));
  const wrap = document.getElementById('pfGroupCompositionWrap');
  if (wrap) wrap.style.display = 'none';
  const range = document.getElementById('pfGroupCompositionRange');
  if (range) range.innerHTML = '';
  const stats = document.getElementById('pfGroupCompositionStats');
  if (stats) stats.innerHTML = '';
}

function _prepareGroupCompositionData(rows) {
  const raw = (Array.isArray(rows) ? rows : [])
    .map(row => ({
      date: String(row.date || ''),
      code: String(row.stock_code || ''),
      name: String(row.stock_name || row.stock_code || ''),
      weight: Number(row.weight_pct),
      value: Number(row.market_value),
    }))
    .filter(row => row.date && row.code && Number.isFinite(row.weight));
  const dates = Array.from(new Set(raw.map(row => row.date))).sort();
  const latestDate = dates[dates.length - 1] || null;
  const latestByCode = {};
  const totalValueByCode = {};
  const nameByCode = {};
  raw.forEach(row => {
    nameByCode[row.code] = row.name || row.code;
    totalValueByCode[row.code] = (totalValueByCode[row.code] || 0) + Math.abs(Number.isFinite(row.value) ? row.value : 0);
    if (row.date === latestDate) latestByCode[row.code] = row;
  });
  const codes = Array.from(new Set(raw.map(row => row.code))).sort((a, b) => {
    const latestDiff = (latestByCode[b]?.weight ?? -Infinity) - (latestByCode[a]?.weight ?? -Infinity);
    if (latestDiff) return latestDiff;
    const valueDiff = (totalValueByCode[b] || 0) - (totalValueByCode[a] || 0);
    if (valueDiff) return valueDiff;
    return (nameByCode[a] || a).localeCompare(nameByCode[b] || b);
  });
  const hasOther = codes.length > _GROUP_COMPOSITION_MAX_SERIES;
  const topCodes = new Set(hasOther ? codes.slice(0, _GROUP_COMPOSITION_MAX_SERIES - 1) : codes);
  const seriesItems = codes
    .filter(code => topCodes.has(code))
    .map(code => ({ key: code, label: nameByCode[code] || code, code }));
  if (hasOther) seriesItems.push({ key: '__OTHER__', label: '기타 종목', code: '' });

  const byDateKey = {};
  const latest = {};
  raw.forEach(row => {
    const key = topCodes.has(row.code) ? row.code : '__OTHER__';
    const mapKey = `${row.date}::${key}`;
    byDateKey[mapKey] = (byDateKey[mapKey] || 0) + row.weight;
    if (row.date === latestDate) {
      if (!latest[key]) latest[key] = { ...row, key, name: key === '__OTHER__' ? '기타 종목' : row.name };
      else {
        latest[key].weight += row.weight;
        latest[key].value += Number.isFinite(row.value) ? row.value : 0;
      }
    }
  });
  const latestRows = Object.values(latest).sort((a, b) => b.weight - a.weight);
  return { dates, seriesItems, byDateKey, latestRows };
}

async function pfShowGroupComposition(groupName) {
  groupName = String(groupName || '').trim();
  if (!groupName) return;
  const requestSeq = ++_groupCompositionRequestSeq;
  _groupCompositionSelected = groupName;
  document.querySelectorAll('.js-pf-group-weight-card').forEach(card => {
    card.classList.toggle('is-active', card.dataset.group === groupName);
  });

  const wrap = document.getElementById('pfGroupCompositionWrap');
  const title = document.getElementById('pfGroupCompositionTitle');
  const container = document.getElementById('pfGroupCompositionChart');
  if (!wrap || !container) return;
  wrap.style.display = 'block';
  if (title) title.textContent = `${groupName} 내 종목 비중 추이`;
  container.innerHTML = '<div class="pf-chart-message">종목 비중 데이터를 불러오는 중입니다...</div>';

  try {
    const rows = await apiFetchJson(`/api/portfolio/group-constituent-history?group=${encodeURIComponent(groupName)}`, {
      errorMessage: '종목 비중 데이터를 불러오지 못했습니다.',
    });
    if (requestSeq !== _groupCompositionRequestSeq || _groupCompositionSelected !== groupName) return;
    await renderGroupCompositionChart(groupName, rows);
  } catch (err) {
    if (requestSeq !== _groupCompositionRequestSeq || _groupCompositionSelected !== groupName) return;
    console.warn(err);
    container.innerHTML = `<div class="pf-chart-message">${escapeHtml(err?.message || '종목 비중 데이터를 불러오지 못했습니다.')}</div>`;
  }
}

async function renderGroupCompositionChart(groupName, rows) {
  const container = document.getElementById('pfGroupCompositionChart');
  if (!container) return;
  const renderSeq = ++_groupCompositionRenderSeq;
  _disposeGroupCompositionChart();
  const containerReady = await _waitForChartContainer(container);
  if (!containerReady && container.offsetParent === null) return;
  if (renderSeq !== _groupCompositionRenderSeq || _groupCompositionSelected !== groupName) return;

  const prepared = _prepareGroupCompositionData(rows);
  if (!prepared.dates.length || !prepared.seriesItems.length) {
    container.innerHTML = '<div class="pf-chart-message">이 그룹의 종목 비중 스냅샷이 아직 없습니다.</div>';
    _updateChartRangeLabel('pfGroupCompositionRange', [], 0, 0);
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
  const series = prepared.seriesItems.map((item, idx) => {
    const color = _GROUP_WEIGHT_COLORS[idx % _GROUP_WEIGHT_COLORS.length];
    return {
      name: item.label,
      type: 'line',
      data: prepared.dates.map(date => {
        const value = prepared.byDateKey[`${date}::${item.key}`];
        return Number.isFinite(value) ? Number(value.toFixed(2)) : 0;
      }),
      stack: 'groupComposition',
      smooth: 0.25,
      symbol: 'none',
      lineStyle: { color, width: prepared.seriesItems.length > 8 ? 1 : 1.3 },
      itemStyle: { color },
      areaStyle: { opacity: 0.34 },
      tooltipSuffix: '%',
    };
  });

  _groupCompositionChartInstance = PortfolioTrendChart.create(container, {
    legend: {
      data: prepared.seriesItems.map(item => item.label),
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
  _kickChartResize(_groupCompositionChartInstance);

  const fullWindow = _chartZoomWindow(prepared.dates.length, 0, 100);
  _updateChartRangeLabel('pfGroupCompositionRange', dateObjects, fullWindow.startIdx, fullWindow.endIdx);
  _renderGroupCompositionStats(prepared.latestRows);

  if (typeof ResizeObserver !== 'undefined') {
    const ro = new ResizeObserver(() => {
      if (_groupCompositionChartInstance && _groupCompositionChartInstance.resize) {
        _groupCompositionChartInstance.resize();
      }
    });
    ro.observe(container);
    _groupCompositionResizeObserver = ro;
  }
  if (_groupCompositionChartInstance && typeof _groupCompositionChartInstance.on === 'function') {
    let zoomTimer = null;
    _groupCompositionChartInstance.on('datazoom', () => {
      clearTimeout(zoomTimer);
      zoomTimer = setTimeout(() => {
        const { startIdx, endIdx } = _chartWindowFromInstance(_groupCompositionChartInstance, prepared.dates.length);
        _updateChartRangeLabel('pfGroupCompositionRange', dateObjects, startIdx, endIdx);
      }, 80);
    });
  }
}

function _renderGroupCompositionStats(rows) {
  const stats = document.getElementById('pfGroupCompositionStats');
  if (!stats) return;
  stats.innerHTML = rows.slice(0, 8).map(row => {
    const value = `${row.weight.toFixed(1)}%`;
    const title = Number.isFinite(row.value) ? ` title="${escapeHtml(fmtKrw(row.value))}"` : '';
    const code = row.code && row.key !== '__OTHER__' ? ` <span>${escapeHtml(row.code)}</span>` : '';
    return `<div class="pf-nav-ret-card"${title}><div class="pf-nav-ret-label">${escapeHtml(row.name)}${code}</div><div class="pf-nav-ret-value">${value}</div></div>`;
  }).join('');
}
