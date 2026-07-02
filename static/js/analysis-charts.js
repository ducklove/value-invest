// Analysis valuation charts: weekly/annual chart grid, target-price overlay,
// chart modal, and period switching (1Y/3Y/ALL).
// Split from static/js/analysis.js to keep analysis features maintainable.

function isPerChart(key) {
  return key === 'PER';
}

function normalizeChartValue(value) {
  return value === null || value === undefined ? null : Number(value);
}

function buildChartSeries(key, series) {
  const labelField = Object.prototype.hasOwnProperty.call(series[0], 'date') ? 'date' : 'year';
  const labels = series.map(item => item[labelField]);
  const rawValues = series.map(item => normalizeChartValue(item.value));
  let values = rawValues.slice();
  let note = '';
  let spanGaps = true;

  if (isPerChart(key)) {
    values = rawValues.map(value => {
      if (value === null || !Number.isFinite(value)) return null;
      if (value <= 0 || Math.abs(value) > PER_DISPLAY_MAX) return null;
      return value;
    });
    note = `표시 기준: 음수 PER와 ${PER_DISPLAY_MAX}배 초과 구간은 추세 왜곡을 막기 위해 제외합니다.`;
    spanGaps = false;
  }

  return { labelField, labels, values, rawValues, note, spanGaps };
}

function shouldUseZeroBaseline(values) {
  const validValues = (values || []).filter(value => value !== null && Number.isFinite(value));
  return validValues.length > 0 && validValues.every(value => value >= 0);
}

function formatWeeklyTickLabel(value) {
  if (typeof value !== 'string') return value;
  const match = value.match(/^(\d{4})-(\d{2})-\d{2}$/);
  if (!match) return value;
  return `${match[1].slice(-2)}.${match[2]}`;
}

let _lastWeeklyIndicators = null;

async function _overlayTargetPrices(reports) {
  _renderCoverage();  // 목표가 카드(최근 5개 평균, 3개월 이내)를 로드된 리포트로 갱신
  if (!_lastWeeklyIndicators) return;
  const priceSeries = _lastWeeklyIndicators['주가'] || [];
  if (priceSeries.length === 0) return;

  // Extract target prices with dates
  const targets = [];
  for (const r of (reports || [])) {
    const tp = r.target_price ? Number(String(r.target_price).replace(/,/g, '')) : null;
    const d = r.date;
    if (tp && tp > 0 && d) {
      const rec = (r.recommendation || '').toLowerCase();
      const isBuy = rec.includes('buy') || rec.includes('매수') || rec.includes('outperform');
      const url = r.pdf_url ? buildReportPdfUrl(r.pdf_url) : r.source_url;
      targets.push({ date: d, price: tp, firm: r.firm_short || r.firm || '', buy: isBuy, url });
    }
  }
  if (targets.length === 0) return;
  targets.sort((a, b) => a.date.localeCompare(b.date));

  // Determine date range from reports
  const minDate = targets[0].date;
  const maxDate = priceSeries[priceSeries.length - 1].date;

  // Filter price data to report date range
  const priceInRange = priceSeries.filter(p => p.date >= minDate && p.value != null);
  if (priceInRange.length === 0) return;

  const dates = priceInRange.map(p => p.date);
  const prices = priceInRange.map(p => p.value);

  // Build target price line (step-forward: hold last target until next)
  const targetLine = new Array(dates.length).fill(null);
  let tIdx = 0;
  let lastTarget = null;
  for (let i = 0; i < dates.length; i++) {
    while (tIdx < targets.length && targets[tIdx].date <= dates[i]) {
      lastTarget = targets[tIdx].price;
      tIdx++;
    }
    targetLine[i] = lastTarget;
  }

  // Build scatter data for individual report points
  const scatterData = [];
  for (const t of targets) {
    // Find nearest date in chart
    let bestIdx = -1, bestDiff = Infinity;
    for (let i = 0; i < dates.length; i++) {
      const diff = Math.abs(new Date(dates[i]) - new Date(t.date));
      if (diff < bestDiff) { bestDiff = diff; bestIdx = i; }
    }
    if (bestIdx >= 0 && bestDiff < 14 * 86400000) {
      scatterData.push({ value: [bestIdx, t.price], firm: t.firm, date: t.date, buy: t.buy, url: t.url });
    }
  }

  // Remove previous target chart
  const existingCard = document.getElementById('targetPriceChartCard');
  if (existingCard) { existingCard.remove(); }
  if (charts['_targetPrice']) { charts['_targetPrice'].dispose(); delete charts['_targetPrice']; }

  // Create new chart card at end of weekly grid
  const grid = document.getElementById('weeklyChartsGrid');
  if (!grid) return;
  // Ensure grid is visible
  if (grid.style.display === 'none') grid.style.display = 'grid';

  const card = document.createElement('div');
  card.className = 'chart-card';
  card.id = 'targetPriceChartCard';
  const chartDiv = document.createElement('div');
  chartDiv.className = 'chart-canvas-wrap';
  const innerDiv = document.createElement('div');
  innerDiv.id = 'targetPriceChartInner';
  innerDiv.style.cssText = 'width:100%;height:100%;';
  chartDiv.appendChild(innerDiv);
  card.innerHTML = `<h3>증권사 목표가</h3>`;
  card.appendChild(chartDiv);
  grid.appendChild(card);

  // 목표가 차트는 echarts 전용 — 모바일(uPlot)에서도 echarts 를 보장 로드한다.
  await loadEcharts();

  const textColor = getComputedStyle(document.documentElement).getPropertyValue('--text-secondary').trim() || '#888';
  const gridColor = getComputedStyle(document.documentElement).getPropertyValue('--border').trim() || '#ccc';
  const labels = dates.map(formatWeeklyTickLabel);

  const ec = echarts.init(innerDiv);
  ec.setOption({
    grid: { left: 55, right: 12, top: 28, bottom: 24 },
    legend: { show: true, top: 0, right: 0, textStyle: { color: textColor, fontSize: 11 } },
    xAxis: {
      type: 'category', data: labels,
      axisLine: { lineStyle: { color: gridColor } },
      axisLabel: { color: textColor, fontSize: 10 },
      splitLine: { show: false },
    },
    yAxis: {
      type: 'value', min: 0,
      axisLine: { show: false },
      axisLabel: { color: textColor, fontSize: 10 },
      splitLine: { lineStyle: { color: gridColor, width: 0.5 } },
    },
    tooltip: {
      trigger: 'axis',
      formatter(params) {
        const idx = params[0]?.dataIndex;
        let html = dates[idx] || '';
        for (const p of params) {
          if (p.seriesName === '주가') {
            html += `<br/><span style="color:${p.color}">● 주가: ${Number(p.value).toLocaleString()}원</span>`;
          } else if (p.seriesName === '목표가') {
            html += `<br/><span style="color:${p.color}">● 목표가: ${p.value == null || p.value === '-' ? '-' : Number(p.value).toLocaleString() + '원'}</span>`;
          } else if (p.seriesName === '리포트') {
            const d = p.data;
            const label = d.buy ? 'Buy' : 'Hold';
            const c = d.buy ? '#dc2626' : '#6b7280';
            html += `<br/><span style="color:${c}">◆ ${Number(d.value[1]).toLocaleString()}원 [${label}]</span> <span style="font-size:11px;color:#999">(${d.firm})</span>`;
          }
        }
        return html;
      },
    },
    series: [
      {
        name: '주가', type: 'line', data: prices,
        lineStyle: { color: '#3b82f6', width: 2 },
        itemStyle: { color: '#3b82f6' },
        symbol: 'none', smooth: 0.3,
        areaStyle: {
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: 'rgba(59,130,246,0.15)' },
            { offset: 1, color: 'rgba(59,130,246,0.0)' },
          ]),
        },
      },
      {
        name: '목표가', type: 'line',
        data: targetLine.map(v => v === null ? '-' : v),
        lineStyle: { color: '#f59e0b', width: 2, type: 'dashed' },
        itemStyle: { color: '#f59e0b' },
        symbol: 'none', step: 'end', connectNulls: false,
      },
      {
        name: '리포트', type: 'scatter',
        data: scatterData,
        symbol: 'diamond', symbolSize: 8,
        itemStyle: {
          color: (params) => params.data.buy ? '#dc2626' : '#6b7280',
          borderColor: (params) => params.data.buy ? '#b91c1c' : '#4b5563',
          borderWidth: 1,
        },
        z: 10,
      },
    ],
  });
  charts['_targetPrice'] = ec;

  // Click scatter → open report URL, block modal
  let _scatterClicked = false;
  ec.on('click', 'series.scatter', (params) => {
    _scatterClicked = true;
    const url = params.data?.url;
    if (url) window.open(url, '_blank', 'noopener');
  });
  card.addEventListener('click', () => {
    if (_scatterClicked) { _scatterClicked = false; return; }
    _openTargetPriceModal(dates, prices, targetLine, scatterData, labels);
  });
}

async function _openTargetPriceModal(dates, prices, targetLine, scatterData, labels) {
  const modal = document.getElementById('chartModal');
  const titleEl = document.getElementById('chartModalTitle');
  const canvas = document.getElementById('chartModalCanvas');
  titleEl.textContent = '증권사 목표가';
  openManagedModal(modal, {
    initialFocus: '.chart-modal-close',
    onEscape: closeChartModal,
  });

  if (_modalChart) { _modalChart.dispose(); _modalChart = null; }

  // 모바일(uPlot)에서도 모달 목표가 차트는 echarts 가 필요하므로 보장 로드.
  await loadEcharts();

  const textColor = getComputedStyle(document.documentElement).getPropertyValue('--text-secondary').trim() || '#888';
  const gridColor = getComputedStyle(document.documentElement).getPropertyValue('--border').trim() || '#ccc';

  const ec = echarts.init(canvas);
  ec.setOption({
    grid: { left: 60, right: 20, top: 28, bottom: 60 },
    legend: { show: true, top: 0, right: 0, textStyle: { color: textColor, fontSize: 11 } },
    xAxis: {
      type: 'category', data: labels,
      axisLine: { lineStyle: { color: gridColor } },
      axisLabel: { color: textColor, fontSize: 11 },
    },
    yAxis: {
      type: 'value', min: 0,
      axisLine: { show: false },
      axisLabel: { color: textColor, fontSize: 11 },
      splitLine: { lineStyle: { color: gridColor, width: 0.5 } },
    },
    dataZoom: [
      { type: 'slider', start: 0, end: 100, bottom: 8, height: 20, textStyle: { color: textColor, fontSize: 10 } },
      { type: 'inside' },
    ],
    tooltip: {
      trigger: 'axis',
      formatter(params) {
        const idx = params[0]?.dataIndex;
        let html = dates[idx] || '';
        for (const p of params) {
          if (p.seriesName === '주가') {
            html += `<br/><span style="color:${p.color}">● 주가: ${Number(p.value).toLocaleString()}원</span>`;
          } else if (p.seriesName === '목표가') {
            html += `<br/><span style="color:${p.color}">● 목표가: ${p.value == null || p.value === '-' ? '-' : Number(p.value).toLocaleString() + '원'}</span>`;
          } else if (p.seriesName === '리포트') {
            const d = p.data;
            const label = d.buy ? 'Buy' : 'Hold';
            const c = d.buy ? '#dc2626' : '#6b7280';
            html += `<br/><span style="color:${c}">◆ ${Number(d.value[1]).toLocaleString()}원 [${label}]</span> <span style="font-size:11px;color:#999">(${d.firm})</span>`;
          }
        }
        return html;
      },
    },
    series: [
      {
        name: '주가', type: 'line', data: prices,
        lineStyle: { color: '#3b82f6', width: 2 }, itemStyle: { color: '#3b82f6' },
        symbol: 'none', smooth: 0.3,
        areaStyle: { color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
          { offset: 0, color: 'rgba(59,130,246,0.15)' }, { offset: 1, color: 'rgba(59,130,246,0.0)' },
        ]) },
      },
      {
        name: '목표가', type: 'line',
        data: targetLine.map(v => v === null ? '-' : v),
        lineStyle: { color: '#f59e0b', width: 2, type: 'dashed' },
        itemStyle: { color: '#f59e0b' },
        symbol: 'none', step: 'end', connectNulls: false,
      },
      {
        name: '리포트', type: 'scatter',
        data: scatterData, symbol: 'diamond', symbolSize: 10,
        itemStyle: {
          color: (params) => params.data.buy ? '#dc2626' : '#6b7280',
          borderColor: (params) => params.data.buy ? '#b91c1c' : '#4b5563',
          borderWidth: 1,
        },
        z: 10,
      },
    ],
  });
  ec.on('click', 'series.scatter', (params) => {
    const url = params.data?.url;
    if (url) window.open(url, '_blank', 'noopener');
  });
  _modalChart = ec;
}

async function renderChartGrid(container, chartKeys, indicatorMap, gridColor, tickColor, prefix) {
  container.innerHTML = '';
  await loadChartLib();

  const _LEGACY_KEYS = { '시가총액': '시가총액 (억원)' };
  chartKeys.forEach((key, i) => {
    let series = indicatorMap[key] || [];
    if (series.length === 0 && _LEGACY_KEYS[key]) {
      const legacy = indicatorMap[_LEGACY_KEYS[key]] || [];
      if (legacy.length > 0) {
        series = legacy.map(d => ({ ...d, value: d.value != null ? d.value * 1e8 : null }));
      }
    }
    if (series.length === 0) return;

    const { labelField, labels, values, rawValues, note, spanGaps } = buildChartSeries(key, series);
    const zeroBaseline = shouldUseZeroBaseline(values);
    const color = CHART_COLORS[i % CHART_COLORS.length];
    const displayLabels = labelField === 'date' ? labels.map(formatWeeklyTickLabel) : labels;

    const card = document.createElement('div');
    card.className = 'chart-card';
    const chartDiv = document.createElement('div');
    chartDiv.className = 'chart-canvas-wrap';
    const innerDiv = document.createElement('div');
    innerDiv.id = `${prefix}-chart-${i}`;
    innerDiv.style.cssText = 'width:100%;height:100%;';
    chartDiv.appendChild(innerDiv);
    card.innerHTML = `<h3>${key}</h3>`;
    card.appendChild(chartDiv);
    if (note) {
      const noteEl = document.createElement('div');
      noteEl.className = 'chart-note';
      noteEl.textContent = note;
      card.appendChild(noteEl);
    }
    container.appendChild(card);

    const isKrwKey = /주가|시가총액|EPS|배당금/.test(key);
    const chartOpts = {
      labels: displayLabels,
      rawLabels: labels,
      values,
      color,
      yMin: zeroBaseline ? 0 : undefined,
      tooltipPrefix: `${key}: `,
      connectNulls: spanGaps,
      yFormatter: isKrwKey ? (v => fmtKrw(v)) : undefined,
      tooltipFormatter: isKrwKey ? (v => fmtKrw(v)) : undefined,
    };
    charts[key] = createLineChart(innerDiv, chartOpts);

    // 접근성(ST-05): echarts canvas 는 스크린 리더가 읽지 못하므로, 차트 컨테이너에
    // role=img + 요약 aria-label 을 붙이고 핵심 데이터(연도/값)를 숨김 표로 제공.
    // 보조기기 사용자가 "어떤 지표인지, 최근 값과 추세가 어떤지"를 인지 가능.
    const _latestIdx = values.length - 1;
    const _latestLabel = displayLabels[_latestIdx] || '';
    const _latestVal = rawValues[_latestIdx];
    const _valText = _latestVal == null ? '-' : (isKrwKey ? fmtKrw(_latestVal) : _latestVal);
    const _prevVal = rawValues[_latestIdx - 1];
    const _trend = (_latestVal == null || _prevVal == null || _prevVal === 0)
      ? ''
      : (_latestVal > _prevVal ? ' (전년 대비 상승)' : _latestVal < _prevVal ? ' (전년 대비 하락)' : ' (보합)');
    const _rows = displayLabels.map((lbl, idx) => {
      const v = rawValues[idx];
      return [lbl, v == null ? '-' : (isKrwKey ? fmtKrw(v) : v)];
    });
    describeChart(innerDiv, `${key} 차트: 최근 ${_latestLabel} ${_valText}${_trend}`, {
      caption: `${key} 연도별 추이`,
      headers: ['연도', key],
      rows: _rows,
    });

    // Click to open modal
    card.addEventListener('click', () => openChartModal(key, chartOpts));
  });
}

// --- Chart Modal ---
let _modalChart = null;

function openChartModal(title, opts) {
  const modal = document.getElementById('chartModal');
  const titleEl = document.getElementById('chartModalTitle');
  const canvas = document.getElementById('chartModalCanvas');
  titleEl.textContent = title;
  openManagedModal(modal, {
    initialFocus: '.chart-modal-close',
    onEscape: closeChartModal,
  });

  if (_modalChart) { _modalChart.dispose(); _modalChart = null; }

  const textColor = getComputedStyle(document.documentElement).getPropertyValue('--text-secondary').trim() || '#888';
  const gridColor = getComputedStyle(document.documentElement).getPropertyValue('--border').trim() || '#ccc';
  const color = opts.color || '#3b82f6';
  const labels = opts.labels || [];
  const rawLabels = opts.rawLabels || labels;
  const values = opts.values || [];

  const ec = echarts.init(canvas);
  ec.setOption({
    grid: { left: 60, right: 20, top: 20, bottom: 60 },
    xAxis: {
      type: 'category', data: labels,
      axisLine: { lineStyle: { color: gridColor } },
      axisLabel: { color: textColor, fontSize: 11 },
    },
    yAxis: {
      type: 'value', min: opts.yMin,
      axisLine: { show: false },
      axisLabel: { color: textColor, fontSize: 11 },
      splitLine: { lineStyle: { color: gridColor, width: 0.5 } },
    },
    tooltip: {
      trigger: 'axis',
      formatter(params) {
        const p = params[0];
        const idx = p.dataIndex;
        const dateLabel = rawLabels[idx] || labels[idx] || '';
        const val = p.value == null || p.value === '-' ? 'N/A' : `${opts.tooltipPrefix || ''}${Number(p.value).toLocaleString()}`;
        return `${dateLabel}<br/>${val}`;
      },
    },
    dataZoom: [
      { type: 'slider', start: 0, end: 100, bottom: 8, height: 20,
        textStyle: { color: textColor, fontSize: 10 } },
      { type: 'inside' },
    ],
    series: [{
      type: 'line',
      data: values.map(v => v === null ? '-' : v),
      smooth: 0.3, symbol: 'none',
      lineStyle: { color, width: 2 },
      itemStyle: { color },
      areaStyle: {
        color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
          { offset: 0, color: _hexToRgba(color, 0.25) },
          { offset: 1, color: _hexToRgba(color, 0.0) },
        ]),
      },
      connectNulls: opts.connectNulls || false,
    }],
  });
  _modalChart = ec;
}

function closeChartModal() {
  closeManagedModal('chartModal');
  if (_modalChart) { _modalChart.dispose(); _modalChart = null; }
}

// --- Period Switching (1Y / 3Y / ALL) ---
let _currentPeriod = 'all';
let _lastAnalysisData = null;
let _dailyCache = {}; // stock_code -> daily data

async function switchValuationPeriod(period) {
  _currentPeriod = period;
  document.querySelectorAll('.vp-btn').forEach(b => b.classList.toggle('active', b.dataset.period === period));

  if (!_lastAnalysisData) return;
  const stockCode = _lastAnalysisData.stock_code;
  const weeklyIndicators = _lastAnalysisData.weekly_indicators || {};

  if (period === '1y') {
    // Fetch daily data
    let daily = _dailyCache[stockCode];
    if (!daily) {
      try {
        daily = await apiFetchJson(`/api/analyze/${stockCode}/daily`);
        _dailyCache[stockCode] = daily;
      } catch (e) {
        console.error('Daily fetch failed', e);
        return;
      }
    }
    // Convert daily to weekly_indicators format
    const dailyIndicators = {};
    for (const key of WEEKLY_CHART_KEYS) {
      const fieldMap = { '주가': 'close_price', 'PER': 'per', 'PBR': 'pbr', '배당수익률 (%)': 'dividend_yield',
        '주당배당금 (원)': 'dividend_per_share',
        '시가총액': 'market_cap', 'EPS (원)': 'eps', 'ROE (%)': 'roe', '부채비율 (%)': 'debt_ratio', '영업이익률 (%)': 'operating_margin' };
      const field = fieldMap[key];
      if (field) {
        dailyIndicators[key] = daily.map(d => ({
          date: d.date,
          value: d[field],
        }));
      }
    }
    _renderValuationCharts(dailyIndicators);
  } else {
    // Filter weekly data by period
    let filtered = {};
    const cutoff = period === '3y' ? _dateDaysAgo(3 * 365) : null;
    for (const [key, series] of Object.entries(weeklyIndicators)) {
      filtered[key] = cutoff ? series.filter(d => d.date >= cutoff) : series;
    }
    _renderValuationCharts(filtered);
  }
}

function _dateDaysAgo(days) {
  // Use local date, not UTC: cutoff is compared against server-provided
  // dates in local calendar terms, so toISOString() would shift by a day
  // during early-morning hours in positive UTC offsets (e.g. KST 00–09).
  const d = new Date();
  d.setDate(d.getDate() - days);
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}

async function _renderValuationCharts(indicators) {
  const grid = document.getElementById('weeklyChartsGrid');
  if (!grid) return;

  // Destroy existing
  Object.entries(charts).forEach(([k, c]) => { if (k !== '_targetPrice') c.dispose(); });
  Object.keys(charts).forEach(k => { if (k !== '_targetPrice') delete charts[k]; });
  // Remove all cards except target price
  Array.from(grid.children).forEach(c => { if (c.id !== 'targetPriceChartCard') c.remove(); });

  const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
  const gridColor = isDark ? 'rgba(148,163,184,0.15)' : 'rgba(0,0,0,0.06)';
  const tickColor = isDark ? '#94a3b8' : '#666';

  const targetCard = document.getElementById('targetPriceChartCard');
  await renderChartGrid(grid, WEEKLY_CHART_KEYS, indicators, gridColor, tickColor, 'weekly');
  // Move target price chart to end
  if (targetCard) grid.appendChild(targetCard);

  // Re-overlay target prices
  if (allReports && allReports.length > 0) {
    _lastWeeklyIndicators = indicators;
    _overlayTargetPrices(allReports);
  }
}
