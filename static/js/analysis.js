// Analyze
const STEP_LABELS = {
  start: '분석 시작',
  financial_start: '재무제표 수집',
  financial_done: '재무제표 수집 완료',
  financial_error: '재무제표 조회 실패',
  market_start: '시장 데이터 계산',
  market_done: '시장 데이터 수집 완료',
  market_error: '시장 데이터 조회 실패',
  saving: '캐시 저장',
  analyzing: '지표 계산',
};

function getSeriesCoverage(series) {
  const years = (series || [])
    .filter(item => item && item.value !== null && item.value !== undefined)
    .map(item => item.year)
    .filter(year => Number.isFinite(year));
  if (years.length === 0) return '';
  const startYear = Math.min(...years);
  const endYear = Math.max(...years);
  return startYear === endYear ? String(startYear) : `${startYear}-${endYear}`;
}

function getWeeklyDateCoverage(series) {
  const dates = (series || [])
    .filter(item => item && item.value !== null && item.value !== undefined && item.date)
    .map(item => item.date)
    .sort();
  if (dates.length === 0) return null;
  return {
    startDate: dates[0],
    endDate: dates[dates.length - 1],
  };
}

function formatWeeklySectionTitle(weeklyIndicators) {
  const coverage = getWeeklyDateCoverage(weeklyIndicators?.['주가'] || weeklyIndicators?.['PER'] || []);
  if (!coverage) return '밸류에이션';
  return `밸류에이션 (${coverage.startDate} ~ ${coverage.endDate})`;
}

function getLatestIndicatorValue(series) {
  const entries = (series || []).filter(item => item && item.value !== null && item.value !== undefined && Number.isFinite(Number(item.value)));
  if (entries.length === 0) return null;
  return Number(entries[entries.length - 1].value);
}

// Walks backward through a series and returns the latest entry whose
// value is strictly positive. Used for trailing dividend so the card
// doesn't read 0% just because the current year hasn't had a payout
// recorded yet (e.g., it's January and last year's dividend was
// declared but no distribution yet).
function getLatestPositiveValue(series) {
  const arr = series || [];
  for (let i = arr.length - 1; i >= 0; i -= 1) {
    const v = Number(arr[i]?.value);
    if (Number.isFinite(v) && v > 0) return v;
  }
  return null;
}

function formatMetricNumber(value, suffix = '') {
  return value === null || value === undefined || !Number.isFinite(value)
    ? 'N/A'
    : `${value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}${suffix}`;
}

function getLatestDerivedBps(indicators) {
  const priceByYear = new Map((indicators['주가 (원)'] || []).map(item => [item.year, Number(item.value)]));
  const pbrSeries = indicators['PBR'] || [];

  for (let index = pbrSeries.length - 1; index >= 0; index -= 1) {
    const item = pbrSeries[index];
    const pbr = Number(item?.value);
    const price = priceByYear.get(item?.year);
    if (Number.isFinite(price) && Number.isFinite(pbr) && pbr > 0) {
      return price / pbr;
    }
  }

  return null;
}

function getCurrentValuationMetrics(indicators, quoteSnapshot) {
  const currentPrice = Number(quoteSnapshot?.price);
  if (!Number.isFinite(currentPrice) || currentPrice <= 0) {
    return { per: null, pbr: null, dividendYield: null };
  }

  const latestEps = getLatestIndicatorValue(indicators['EPS (원)']);
  // Use the most recent *positive* DPS so companies that pay dividends
  // don't read as 0% when the current year's payout hasn't been
  // recorded yet. Legitimate non-dividend-payers return null here
  // (all-zero series) and the card renders 'N/A'.
  const trailingDps = getLatestPositiveValue(indicators['주당배당금 (원)']);
  const latestBps = getLatestDerivedBps(indicators);

  return {
    per: latestEps && latestEps > 0 ? currentPrice / latestEps : null,
    pbr: latestBps && latestBps > 0 ? currentPrice / latestBps : null,
    dividendYield: trailingDps !== null ? (trailingDps / currentPrice) * 100 : null,
  };
}

// 베타는 별도 엔드포인트에서 비동기로 받아오며, 처음 렌더 시에는 '…' 로
// 플레이스홀더를 그렸다가 loadBeta 가 완료되면 해당 카드만 덮어쓴다.
let _currentBeta = null;   // {beta, sample_size, benchmark} 또는 null

function renderCurrentValuationSummary(indicators, quoteSnapshot) {
  const metrics = getCurrentValuationMetrics(indicators, quoteSnapshot);
  const betaVal = _currentBeta && _currentBeta.beta !== null && _currentBeta.beta !== undefined
    ? Number(_currentBeta.beta).toFixed(2)
    : (_currentBeta === null ? '…' : 'N/A');
  return [
    { label: 'PER', value: formatMetricNumber(metrics.per) },
    { label: 'PBR', value: formatMetricNumber(metrics.pbr) },
    { label: '배당수익률', value: formatMetricNumber(metrics.dividendYield, '%') },
    { label: '베타 (1Y)', value: betaVal, attr: 'data-beta="1"' },
  ].map(item => (
    `<div class="valuation-card" ${item.attr || ''}><span class="valuation-label">${item.label}</span><span class="valuation-value">${item.value}</span></div>`
  )).join('');
}

async function loadBeta(stockCode) {
  // 새 분석이 시작되면 이전 베타는 날려 플레이스홀더 '…' 로 표시되게.
  _currentBeta = null;
  try {
    const resp = await apiFetch(`/api/analyze/${encodeURIComponent(stockCode)}/beta`);
    if (!resp.ok) throw new Error('beta fetch failed');
    _currentBeta = await resp.json();
  } catch (e) {
    _currentBeta = { beta: null, sample_size: 0, benchmark: 'KOSPI' };
  }
  // 분석 종목이 바뀌지 않았다면 valuation card 만 재렌더.
  if (activeStockCode === stockCode) {
    const coverageNote = document.getElementById('coverageNote');
    if (coverageNote) {
      coverageNote.innerHTML = renderCurrentValuationSummary(
        activeIndicators || {},
        activeQuoteSnapshot || {},
      );
    }
  }
}

function renderQuoteSnapshot(quoteSnapshot, indicators = activeIndicators) {
  const quoteSummary = document.getElementById('quoteSummary');
  const quotePrice = document.getElementById('quotePrice');
  const quoteChange = document.getElementById('quoteChange');
  const quoteDate = document.getElementById('quoteDate');
  const coverageNote = document.getElementById('coverageNote');
  const quote = quoteSnapshot || {};

  if (quote.price !== null && quote.price !== undefined) {
    quoteSummary.style.display = 'flex';
    quotePrice.textContent = `${Number(quote.price).toLocaleString()}원`;
    const change = Number(quote.change || 0);
    const changePct = quote.change_pct;
    const changePrefix = change > 0 ? '+' : '';
    quoteChange.textContent = changePct !== null && changePct !== undefined
      ? `${changePrefix}${change.toLocaleString()}원 (${changePrefix}${Number(changePct).toLocaleString()}%)`
      : '변동 정보 없음';
    quoteChange.className = 'quote-change';
    quoteChange.classList.add(change > 0 ? 'up' : change < 0 ? 'down' : 'flat');
    quoteDate.textContent = quote.date ? `${quote.date} 기준` : '';
    // WS live dot next to date
    let dateDot = quoteDate.querySelector('.ws-live-dot');
    if (activeStockCode && QuoteManager.isLive(activeStockCode)) {
      if (!dateDot) { dateDot = document.createElement('span'); dateDot.className = 'ws-live-dot'; dateDot.title = '실시간'; quoteDate.appendChild(dateDot); }
    } else if (dateDot) { dateDot.remove(); }
  } else {
    quoteSummary.style.display = 'none';
    quotePrice.textContent = '';
    quoteChange.textContent = '';
    quoteDate.textContent = '';
  }

  coverageNote.innerHTML = renderCurrentValuationSummary(indicators || {}, quote);
}

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

  await loadChartLib();

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

function _openTargetPriceModal(dates, prices, targetLine, scatterData, labels) {
  const modal = document.getElementById('chartModal');
  const titleEl = document.getElementById('chartModalTitle');
  const canvas = document.getElementById('chartModalCanvas');
  titleEl.textContent = '증권사 목표가';
  modal.style.display = 'flex';
  document.body.style.overflow = 'hidden';

  if (_modalChart) { _modalChart.dispose(); _modalChart = null; }

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
  modal.style.display = 'flex';
  document.body.style.overflow = 'hidden';

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
  document.getElementById('chartModal').style.display = 'none';
  document.body.style.overflow = '';
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
        const resp = await apiFetch(`/api/analyze/${stockCode}/daily`);
        daily = await resp.json();
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

function resetProgress() {
  document.getElementById('progressBar').style.width = '0%';
  document.getElementById('progressSteps').innerHTML = '';
  document.getElementById('loadingDetail').textContent = '';
}

function addStep(text, cls) {
  const steps = document.getElementById('progressSteps');
  const div = document.createElement('div');
  div.className = cls || '';
  div.textContent = text;
  steps.appendChild(div);
  steps.scrollTop = steps.scrollHeight;
}

function markLastStepDone() {
  const steps = document.getElementById('progressSteps');
  const active = steps.querySelector('.active:last-child');
  if (active) { active.classList.remove('active'); active.classList.add('done'); active.textContent = '\u2713 ' + active.textContent; }
}

function cancelAnalysis() {
  if (currentAbortController) {
    currentAbortController.abort();
    currentAbortController = null;
  }
}

async function analyzeStock(stockCode) {
  try {
    requireApiConfiguration();
  } catch (error) {
    showToast(error.message);
    return;
  }

  // 이전 분석 진행중이면 취소
  if (currentAbortController) currentAbortController.abort();
  currentAbortController = new AbortController();
  const signal = currentAbortController.signal;

  const overlay = document.getElementById('loadingOverlay');
  const loadingText = document.getElementById('loadingText');
  const loadingDetail = document.getElementById('loadingDetail');
  const progressBar = document.getElementById('progressBar');
  const cancelBtn = document.getElementById('cancelBtn');

  overlay.classList.add('show');
  cancelBtn.style.display = 'inline-block';
  resetProgress();
  loadingText.textContent = '데이터를 분석하고 있습니다...';

  try {
    trackEvent('analysis_start', { stock_code: stockCode });
    const resp = await apiFetch(`/api/analyze/${stockCode}`, { signal });
    const contentType = resp.headers.get('content-type') || '';

    // 캐시 히트: 일반 JSON 응답
    if (contentType.includes('application/json')) {
      if (!resp.ok) {
        const err = await resp.json();
        showToast(err.detail || '분석 실패');
        return;
      }
      const data = await resp.json();
      renderResult(data);
      if (!currentUser) saveGuestRecent(data.stock_code, data.corp_name);
      if (activeTab === 'starred' && currentUser && !data.user_preference?.is_starred) {
        await autoStarCurrentStock();
      }
      loadRecentList();
      return;
    }

    // SSE 스트리밍 응답
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    let resultData = null;
    let lastDartStep = '';
    let eventType = '';

    // signal로 취소 시 reader도 정리
    signal.addEventListener('abort', () => { reader.cancel(); });

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      const lines = buffer.split('\n');
      buffer = lines.pop();

      for (const line of lines) {
        if (line.startsWith('event: ')) {
          eventType = line.slice(7).trim();
        } else if (line.startsWith('data: ')) {
          const payload = JSON.parse(line.slice(6));

          if (eventType === 'progress') {
            const step = payload.step;
            loadingText.textContent = payload.message;

            if (step === 'financial_start') {
              markLastStepDone();
              progressBar.style.width = '30%';
              addStep(payload.message, 'active');
            } else if (step === 'financial_done' || step === 'financial_error') {
              markLastStepDone();
              progressBar.style.width = '60%';
              addStep(payload.message, step === 'financial_done' ? 'done' : '');
            } else if (step === 'market_start') {
              markLastStepDone();
              progressBar.style.width = '65%';
              addStep(payload.message, 'active');
            } else if (step === 'market_done' || step === 'market_error') {
              markLastStepDone();
              progressBar.style.width = '85%';
              addStep(payload.message, step === 'market_done' ? 'done' : '');
            } else if (step === 'saving') {
              progressBar.style.width = '90%';
              addStep(payload.message, 'active');
            } else if (step === 'analyzing') {
              markLastStepDone();
              progressBar.style.width = '95%';
              addStep(payload.message, 'active');
            } else if (step === 'start') {
              addStep(payload.message, 'active');
            }
          } else if (eventType === 'result') {
            progressBar.style.width = '100%';
            markLastStepDone();
            addStep('분석 완료!', 'done');
            resultData = payload;
          } else if (eventType === 'error') {
            showToast(payload.message || '분석 실패');
          }
          eventType = '';
        }
      }
    }

    // 스트림 종료 후 버퍼에 남은 데이터 처리
    if (buffer.trim()) {
      const remainingLines = buffer.split('\n');
      for (const line of remainingLines) {
        if (line.startsWith('event: ')) {
          eventType = line.slice(7).trim();
        } else if (line.startsWith('data: ')) {
          const payload = JSON.parse(line.slice(6));
          if (eventType === 'result') {
            resultData = payload;
          }
          eventType = '';
        }
      }
    }

    if (resultData) {
      await new Promise(r => setTimeout(r, 300));
      renderResult(resultData);
      if (!currentUser) saveGuestRecent(resultData.stock_code, resultData.corp_name);
      if (activeTab === 'starred' && currentUser && !resultData.user_preference?.is_starred) {
        await autoStarCurrentStock();
      }
      loadRecentList();
    }
  } catch (e) {
    if (e.name === 'AbortError') {
      // 사용자가 취소함 - 무시
    } else {
      showToast('서버 오류: ' + e.message);
    }
  } finally {
    currentAbortController = null;
    overlay.classList.remove('show');
  }
}

async function renderResult(data) {
  _lastAnalysisData = data;
  _currentPeriod = 'all';
  _dailyCache = {};
  document.querySelectorAll('.vp-btn').forEach(b => b.classList.toggle('active', b.dataset.period === 'all'));

  // Company info
  const infoEl = document.getElementById('companyInfo');
  infoEl.style.display = 'block';
  activeStockCode = data.stock_code;
  activeIndicators = data.indicators || {};
  activeQuoteSnapshot = data.quote_snapshot || {};
  currentUserPreference = normalizeUserPreference(data.user_preference);
  document.getElementById('companyName').textContent = `${data.corp_name} (${data.stock_code})`;
  const cachedText = data.cached ? `캐시됨 (${new Date(data.analyzed_at).toLocaleDateString('ko-KR')})` : '신규 분석 완료';
  document.getElementById('companyMeta').textContent = cachedText;
  renderUserPreference();
  renderQuoteSnapshot(data.quote_snapshot || {}, activeIndicators);
  trackEvent('analysis_complete', { stock_code: data.stock_code, cached: String(Boolean(data.cached)) });

  // Hide empty state, show charts
  document.getElementById('emptyState').style.display = 'none';
  const weeklyTitle = document.getElementById('weeklySectionTitle');
  const weeklyGrid = document.getElementById('weeklyChartsGrid');
  const annualTitle = document.getElementById('annualSectionTitle');
  const grid = document.getElementById('chartsGrid');
  const hasWeeklyCharts = WEEKLY_CHART_KEYS.some(key => (data.weekly_indicators?.[key] || []).length > 0);
  _lastWeeklyIndicators = data.weekly_indicators || null;
  weeklyTitle.textContent = formatWeeklySectionTitle(data.weekly_indicators || {});
  const sectionRow = document.getElementById('weeklySectionRow');
  if (sectionRow) sectionRow.style.display = hasWeeklyCharts ? 'flex' : 'none';
  weeklyGrid.style.display = hasWeeklyCharts ? 'grid' : 'none';
  annualTitle.style.display = hasWeeklyCharts ? 'none' : 'block';
  grid.style.display = hasWeeklyCharts ? 'none' : 'grid';
  weeklyGrid.innerHTML = '';
  grid.innerHTML = '';

  // Destroy existing charts
  Object.values(charts).forEach(c => c.dispose());
  charts = {};

  const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
  const gridColor = isDark ? 'rgba(148,163,184,0.15)' : 'rgba(0,0,0,0.06)';
  const tickColor = isDark ? '#94a3b8' : '#666';

  if (hasWeeklyCharts) {
    await renderChartGrid(weeklyGrid, WEEKLY_CHART_KEYS, data.weekly_indicators || {}, gridColor, tickColor, 'weekly');
  } else {
    await renderChartGrid(grid, ANNUAL_CHART_KEYS, data.indicators || {}, gridColor, tickColor, 'annual');
  }

  // Load reports after charts are rendered (needs grid visible for target price chart)
  loadReports(data.stock_code);
  // Wiki is lazy — fire-and-forget, renders under the report table.
  loadWiki(data.stock_code);
  // DART filing review is cached-first and only spends AI tokens when
  // the user explicitly generates or refreshes it.
  loadFilingReview(data.stock_code);
  // 베타 — 1Y 일별 수익률 vs KOSPI. 비동기로 받아 valuation card 갱신.
  loadBeta(data.stock_code);
  _updateQuoteSubscriptions();
}

// Recent list
async function loadRecentList() {
  updateSidebarTabs();
  if (!hasApiConfiguration()) {
    document.getElementById('recentList').innerHTML = '<div style="color:var(--text-secondary);font-size:13px;">GitHub Pages에서는 API 서버 연결 후 최근 분석 목록을 불러옵니다.</div>';
    return;
  }

  if (recentListLoading) return;
  recentListLoading = true;

  try {
    const container = document.getElementById('recentList');
    const tab = currentUser ? activeTab : 'recent';

    if (currentUser) {
      const resp = await apiFetch(`/api/cache/list?include_quotes=true&tab=${tab}`);
      const data = await resp.json();
      recentListItems = Array.isArray(data) ? data.slice() : [];
    } else {
      recentListItems = getGuestRecent();
    }
    if (recentListItems.length === 0) {
      const emptyMsg = currentUser
        ? (tab === 'starred' ? '관심종목이 없습니다. 분석 화면에서 관심종목을 추가하세요.' : '최근 검색한 종목이 없습니다.')
        : '아직 분석한 종목이 없습니다.';
      container.innerHTML = `<div style="color:var(--text-secondary);font-size:13px;">${emptyMsg}</div>`;
      return;
    }
    container.innerHTML = '';
    recentListItems.forEach((item, index) => {
      const wrapper = document.createElement('div');
      wrapper.className = 'sidebar-item';
      wrapper.dataset.index = index;
      wrapper.dataset.code = item.stock_code;

      if (currentUser) {
        wrapper.draggable = true;
        wrapper.addEventListener('dragstart', (e) => {
          wrapper.classList.add('dragging');
          e.dataTransfer.effectAllowed = 'move';
          e.dataTransfer.setData('text/plain', String(index));
        });
        wrapper.addEventListener('dragend', () => {
          wrapper.classList.remove('dragging');
          container.querySelectorAll('.drag-over').forEach(el => el.classList.remove('drag-over'));
        });
        wrapper.addEventListener('dragover', (e) => {
          e.preventDefault();
          e.dataTransfer.dropEffect = 'move';
          const dragging = container.querySelector('.dragging');
          if (dragging !== wrapper) wrapper.classList.add('drag-over');
        });
        wrapper.addEventListener('dragleave', () => {
          wrapper.classList.remove('drag-over');
        });
        wrapper.addEventListener('drop', (e) => {
          e.preventDefault();
          wrapper.classList.remove('drag-over');
          const fromIndex = parseInt(e.dataTransfer.getData('text/plain'), 10);
          const toIndex = parseInt(wrapper.dataset.index, 10);
          if (fromIndex !== toIndex && !isNaN(fromIndex) && !isNaN(toIndex)) {
            dropRecentItem(fromIndex, toIndex);
          }
        });
      }

      const info = document.createElement('div');
      info.className = 'info';
      info.addEventListener('click', () => { switchView('analysis'); analyzeStock(item.stock_code); });

      const name = document.createElement('div');
      name.className = 'name';
      name.textContent = item.corp_name;
      if (QuoteManager.isLive(item.stock_code)) {
        const dot = document.createElement('span');
        dot.className = 'ws-live-dot';
        dot.title = '실시간';
        name.appendChild(dot);
      }
      const nameRow = document.createElement('div');
      nameRow.className = 'name-row';
      nameRow.appendChild(name);

      const badges = document.createElement('div');
      badges.className = 'badges';
      if (item.is_starred && activeTab !== 'starred') {
        const badge = document.createElement('span');
        badge.className = 'sidebar-badge star';
        badge.textContent = '관심';
        badges.appendChild(badge);
      }
      if (item.note) {
        const badge = document.createElement('span');
        badge.className = 'sidebar-badge note';
        badge.textContent = '메모';
        badges.appendChild(badge);
      }
      if (badges.childElementCount > 0) {
        nameRow.appendChild(badges);
      }

      const quote = item.quote_snapshot || {};
      const quotePrice = document.createElement('div');
      quotePrice.className = 'quote-price';
      const quoteChange = document.createElement('div');
      quoteChange.className = 'quote-change';

      if (quote.price !== null && quote.price !== undefined) {
        quotePrice.textContent = Number(quote.price).toLocaleString();
        const change = Number(quote.change || 0);
        const changePct = quote.change_pct;
        const changeClass = change > 0 ? 'up' : change < 0 ? 'down' : 'flat';
        quoteChange.classList.add(changeClass);
        if (changePct !== null && changePct !== undefined) {
          quoteChange.textContent = `${change > 0 ? '+' : ''}${Number(changePct).toFixed(2)}%`;
        }
      }

      info.append(nameRow, quotePrice, quoteChange);
      wrapper.appendChild(info);

      const button = document.createElement('button');
      button.className = 'delete-btn';
      button.title = activeTab === 'starred' ? '관심 해제' : '삭제';
      button.innerHTML = '&times;';
      button.addEventListener('click', (event) => {
        event.stopPropagation();
        if (currentUser) {
          deleteCache(item.stock_code);
        } else {
          removeGuestRecent(item.stock_code);
          loadRecentList();
        }
      });
      wrapper.appendChild(button);
      container.appendChild(wrapper);
    });
  } catch (e) { console.warn(e); } finally {
    recentListLoading = false;
    _updateQuoteSubscriptions();
  }
}

function refreshRecentList() {
  loadRecentList();
}

async function deleteCache(stockCode) {
  try {
    const resp = await apiFetch(`/api/cache/${stockCode}?tab=${activeTab}`, { method: 'DELETE' });
    if (!resp.ok) {
      const data = await resp.json().catch(() => ({}));
      throw new Error(data.detail || '삭제하지 못했습니다.');
    }
    loadRecentList();
  } catch (e) {
    showToast(e.message || '삭제하지 못했습니다.');
  }
}

async function saveRecentOrder(stockCodes) {
  const resp = await apiFetch('/api/cache/order', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ stock_codes: stockCodes, tab: activeTab }),
  });
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    throw new Error(data.detail || '순서를 저장하지 못했습니다.');
  }
}

async function moveRecentItem(index, delta) {
  if (!currentUser) return;
  const nextIndex = index + delta;
  if (nextIndex < 0 || nextIndex >= recentListItems.length) return;

  const nextItems = recentListItems.slice();
  const [moved] = nextItems.splice(index, 1);
  nextItems.splice(nextIndex, 0, moved);
  recentListItems = nextItems;

  try {
    await saveRecentOrder(nextItems.map(item => item.stock_code));
    await loadRecentList();
  } catch (error) {
    showToast(error.message || '순서를 저장하지 못했습니다.');
    await loadRecentList();
  }
}

async function dropRecentItem(fromIndex, toIndex) {
  if (!currentUser) return;
  const nextItems = recentListItems.slice();
  const [moved] = nextItems.splice(fromIndex, 1);
  nextItems.splice(toIndex, 0, moved);
  recentListItems = nextItems;

  try {
    await saveRecentOrder(nextItems.map(item => item.stock_code));
    await loadRecentList();
  } catch (error) {
    showToast(error.message || '순서를 저장하지 못했습니다.');
    await loadRecentList();
  }
}

// DART filing AI review
let _filingReviewLoadId = 0;
let _filingReviewStockCode = null;

function _setFilingReviewBusy(isBusy) {
  const generateBtn = document.getElementById('filingReviewGenerateBtn');
  const refreshBtn = document.getElementById('filingReviewRefreshBtn');
  if (generateBtn) {
    generateBtn.disabled = isBusy;
    generateBtn.textContent = isBusy ? '생성 중...' : 'AI 리뷰 생성';
  }
  if (refreshBtn) {
    refreshBtn.disabled = isBusy;
    refreshBtn.textContent = isBusy ? '생성 중...' : '새로 생성';
  }
}

function _filingReviewToneClass(tone) {
  const value = String(tone || 'neutral').toLowerCase();
  if (['good', 'watch', 'bad'].includes(value)) return value;
  return 'neutral';
}

function _renderFilingReviewCards(cards) {
  const container = document.getElementById('filingReviewCards');
  if (!container) return;
  const safeCards = Array.isArray(cards) ? cards.slice(0, 8) : [];
  container.innerHTML = safeCards.map(card => {
    const tone = _filingReviewToneClass(card.tone);
    return `<div class="filing-review-card ${tone}">
      <span>${escapeHtml(card.label || '체크')}</span>
      <strong>${escapeHtml(card.value || '-')}</strong>
      <p>${escapeHtml(card.detail || '')}</p>
    </div>`;
  }).join('');
}

function _renderFilingReviewDetails(review) {
  const details = document.getElementById('filingReviewDetails');
  const body = document.getElementById('filingReviewDetailsBody');
  if (!details || !body) return;
  const comparisons = review.comparison_reports || [];
  const comparisonRows = comparisons.length
    ? comparisons.map(item => {
      const href = safeExternalUrl(item.viewer_url || '');
      const link = href
        ? `<a href="${href}" target="_blank" rel="noopener noreferrer">${escapeHtml(item.report_name || item.rcept_no || '')}</a>`
        : escapeHtml(item.report_name || item.rcept_no || '');
      const suffix = item.error ? ` · ${escapeHtml(item.error)}` : '';
      return `<li>${link} <span>${escapeHtml(item.report_date || '')}${suffix}</span></li>`;
    }).join('')
    : '<li>비교 보고서 없음</li>';
  const model = review.model ? `모델 ${escapeHtml(review.model)}` : '';
  const tokenText = review.tokens_in || review.tokens_out
    ? `입력 ${Number(review.tokens_in || 0).toLocaleString()} / 출력 ${Number(review.tokens_out || 0).toLocaleString()} 토큰`
    : '';
  const sourceLimits = review.review?.source_limits
    ? `<p>${escapeHtml(review.review.source_limits)}</p>`
    : '';
  body.innerHTML = `
    <ul>${comparisonRows}</ul>
    ${sourceLimits}
    <div class="filing-review-runtime">${[model, tokenText, review.created_at ? `생성 ${new Date(review.created_at).toLocaleString('ko-KR')}` : ''].filter(Boolean).join(' · ')}</div>
  `;
  details.style.display = 'block';
}

function renderFilingReview(review, { cached = false } = {}) {
  const section = document.getElementById('filingReviewSection');
  const status = document.getElementById('filingReviewStatus');
  const meta = document.getElementById('filingReviewMeta');
  const body = document.getElementById('filingReviewBody');
  const source = document.getElementById('filingReviewSource');
  const generateBtn = document.getElementById('filingReviewGenerateBtn');
  const refreshBtn = document.getElementById('filingReviewRefreshBtn');
  if (!section || !status || !body) return;

  section.style.display = 'block';
  const title = review.report_name || '최근 정기보고서';
  const date = review.report_date || '';
  if (meta) meta.textContent = `${title}${date ? ` · ${date}` : ''}${cached ? ' · 캐시' : ''}`;
  if (status) status.textContent = '';
  if (source) {
    const href = safeExternalUrl(review.review?.viewer_url || review.viewer_url || `https://dart.fss.or.kr/dsaf001/main.do?rcpNo=${review.rcept_no || ''}`);
    source.href = href || '#';
    source.style.display = href ? 'inline-flex' : 'none';
  }
  if (generateBtn) generateBtn.style.display = 'none';
  if (refreshBtn) refreshBtn.style.display = currentUser ? 'inline-flex' : 'none';
  _renderFilingReviewCards(review.review?.cards || []);
  const md = review.review_md || review.review?.summary_md || '';
  body.innerHTML = md
    ? (typeof _renderSafeMarkdown === 'function' ? _renderSafeMarkdown(md) : escapeHtml(md))
    : '<p>생성된 리뷰 본문이 없습니다.</p>';
  _renderFilingReviewDetails(review);
}

function renderFilingReviewMissing(data) {
  const section = document.getElementById('filingReviewSection');
  const status = document.getElementById('filingReviewStatus');
  const meta = document.getElementById('filingReviewMeta');
  const body = document.getElementById('filingReviewBody');
  const cards = document.getElementById('filingReviewCards');
  const source = document.getElementById('filingReviewSource');
  const generateBtn = document.getElementById('filingReviewGenerateBtn');
  const refreshBtn = document.getElementById('filingReviewRefreshBtn');
  const details = document.getElementById('filingReviewDetails');
  if (!section || !status || !body) return;

  section.style.display = 'block';
  const report = data.latest_report || {};
  if (meta) meta.textContent = report.report_name ? `${report.report_name}${report.report_date ? ` · ${report.report_date}` : ''}` : '';
  if (source) {
    const href = safeExternalUrl(report.viewer_url || '');
    source.href = href || '#';
    source.style.display = href ? 'inline-flex' : 'none';
  }
  if (cards) cards.innerHTML = '';
  if (details) details.style.display = 'none';
  if (data.status === 'no_report') {
    status.textContent = '최근 DART 정기보고서를 찾지 못했습니다.';
    body.innerHTML = '';
    if (generateBtn) generateBtn.style.display = 'none';
    if (refreshBtn) refreshBtn.style.display = 'none';
    return;
  }
  status.textContent = currentUser
    ? '아직 생성된 AI 리뷰가 없습니다. 한 번 생성하면 이후에는 캐시로 빠르게 표시됩니다.'
    : '아직 생성된 AI 리뷰가 없습니다. 로그인하면 최근 공시 리뷰를 생성할 수 있습니다.';
  body.innerHTML = '';
  if (generateBtn) {
    generateBtn.style.display = currentUser ? 'inline-flex' : 'none';
    generateBtn.disabled = false;
    generateBtn.textContent = 'AI 리뷰 생성';
  }
  if (refreshBtn) refreshBtn.style.display = 'none';
}

async function loadFilingReview(stockCode) {
  const section = document.getElementById('filingReviewSection');
  const status = document.getElementById('filingReviewStatus');
  const body = document.getElementById('filingReviewBody');
  const cards = document.getElementById('filingReviewCards');
  const details = document.getElementById('filingReviewDetails');
  const loadId = ++_filingReviewLoadId;
  _filingReviewStockCode = stockCode;
  if (!section || !status) return;

  section.style.display = 'block';
  status.textContent = '최근 DART 공시를 확인하는 중...';
  if (body) body.innerHTML = '';
  if (cards) cards.innerHTML = '';
  if (details) details.style.display = 'none';

  if (!hasApiConfiguration()) {
    status.textContent = 'GitHub Pages에서는 API 서버 연결 후 DART 리뷰를 불러올 수 있습니다.';
    return;
  }

  try {
    const resp = await apiFetch(`/api/analysis/${encodeURIComponent(stockCode)}/filing-review`);
    if (loadId !== _filingReviewLoadId) return;
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error(data.detail || `HTTP ${resp.status}`);
    if (data.status === 'ready' && data.review) {
      renderFilingReview(data.review, { cached: true });
    } else {
      renderFilingReviewMissing(data);
    }
  } catch (err) {
    if (loadId !== _filingReviewLoadId) return;
    status.textContent = 'DART 리뷰 상태를 불러오지 못했습니다: ' + (err.message || err);
  }
}

async function generateFilingReview(force = false) {
  const stockCode = _filingReviewStockCode;
  const status = document.getElementById('filingReviewStatus');
  if (!stockCode || !status) return;
  if (!currentUser) {
    showToast && showToast('로그인이 필요합니다.');
    return;
  }
  _setFilingReviewBusy(true);
  status.textContent = force
    ? 'DART 원문을 다시 읽고 AI 리뷰를 새로 생성하는 중입니다...'
    : 'DART 원문을 읽고 AI 리뷰를 생성하는 중입니다...';
  try {
    const resp = await apiFetch(`/api/analysis/${encodeURIComponent(stockCode)}/filing-review`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ force }),
    });
    const data = await resp.json().catch(() => ({}));
    if (!resp.ok) throw new Error(data.detail || `HTTP ${resp.status}`);
    if (data.review) {
      renderFilingReview(data.review, { cached: Boolean(data.review.cached) });
      showToast && showToast('DART AI 리뷰가 준비됐습니다.');
    }
  } catch (err) {
    status.textContent = 'AI 리뷰 생성 실패: ' + (err.message || err);
  } finally {
    _setFilingReviewBusy(false);
  }
}

// Reports
let allReports = [];
let reportDisplayCount = 20;
let reportsRequestId = 0;

function getRecommBadge(recomm) {
  if (!recomm) return '';
  const r = recomm.toUpperCase();
  if (r.includes('BUY') || r.includes('매수') || r.includes('강력')) return `<span class="badge badge-buy">${recomm}</span>`;
  if (r.includes('SELL') || r.includes('매도') || r.includes('비중축소')) return `<span class="badge badge-sell">${recomm}</span>`;
  return `<span class="badge badge-hold">${recomm}</span>`;
}

function renderReportsTable(reports, limit) {
  const tbody = document.getElementById('reportsBody');
  const slice = reports.slice(0, limit);
  tbody.innerHTML = slice.map((r, idx) => {
    const safeTitle = escapeHtml(r.title);
    const safeSummary = r.summary ? `<div class="report-summary">${escapeHtml(r.summary)}</div>` : '';
    const safeAnalyst = escapeHtml(r.analyst);
    const safeFirm = escapeHtml(r.firm_short || r.firm);
    const safeDate = escapeHtml(r.date);
    const safeHref = safeExternalUrl(buildReportPdfUrl(r.pdf_url) || r.source_url);
    const titleLink = safeHref
      ? `<a href="${safeHref}" target="_blank" rel="noopener noreferrer">${safeTitle}</a>`
      : safeTitle;
    const safeReadHref = safeExternalUrl(r.source_url);
    // If we have an LLM summary for this exact PDF, offer a toggle just
    // before the 본문 페이지 link. Match is by pdf_url so the frontend
    // doesn't need to know about sha1.
    const wikiEntry = r.pdf_url ? _wikiByPdfUrl.get(r.pdf_url) : null;
    const summaryToggle = wikiEntry
      ? `<a href="#" class="report-summary-toggle" data-report-idx="${idx}">요약 내용 보기</a>`
      : '';
    const linkParts = [];
    if (summaryToggle) linkParts.push(summaryToggle);
    if (safeReadHref) linkParts.push(`<a href="${safeReadHref}" target="_blank" rel="noopener noreferrer">네이버 리서치 본문 페이지</a>`);
    const linkNote = linkParts.length
      ? `<div class="report-link-note">${linkParts.join('<span class="report-link-sep"> · </span>')}</div>`
      : '';
    const targetPrc = r.target_price ? Number(r.target_price.replace(/,/g, '')).toLocaleString() + '원' : '-';
    const mainRow = `<tr class="report-row" data-report-idx="${idx}">
      <td class="report-date">${safeDate}</td>
      <td class="report-firm">${safeFirm}</td>
      <td class="report-title">${titleLink}${safeSummary}${linkNote}<div style="font-size:11px;color:var(--text-secondary);margin-top:2px;">${safeAnalyst}</div></td>
      <td>${getRecommBadge(escapeHtml(r.recommendation))}</td>
      <td class="report-target">${escapeHtml(targetPrc)}</td>
    </tr>`;
    // Hidden companion row — populated on first click. Span all 5 cols.
    const summaryRow = wikiEntry
      ? `<tr class="report-summary-row" data-report-idx="${idx}" style="display:none;"><td colspan="5"><div class="report-summary-body" data-report-idx="${idx}"></div></td></tr>`
      : '';
    return mainRow + summaryRow;
  }).join('');

  // Wire up the toggles (event delegation would also work; direct binding
  // here is scoped to the rows we just rendered).
  tbody.querySelectorAll('.report-summary-toggle').forEach(a => {
    a.addEventListener('click', (e) => {
      e.preventDefault();
      const idx = a.dataset.reportIdx;
      const row = tbody.querySelector(`tr.report-summary-row[data-report-idx="${idx}"]`);
      if (!row) return;
      const body = row.querySelector('.report-summary-body');
      const report = slice[Number(idx)];
      const entry = report && report.pdf_url ? _wikiByPdfUrl.get(report.pdf_url) : null;
      if (!entry) return;
      const visible = row.style.display !== 'none';
      if (visible) {
        row.style.display = 'none';
        a.textContent = '요약 내용 보기';
      } else {
        // Render markdown lazily on first open; re-renders are cheap
        // but skipping avoids an allocation spike on bulk expand.
        if (!body.dataset.rendered) {
          const md = entry.summary_md || '';
          body.innerHTML = typeof _renderSafeMarkdown === 'function'
            ? _renderSafeMarkdown(md)
            : escapeHtml(md);
          body.dataset.rendered = '1';
        }
        row.style.display = '';
        a.textContent = '요약 접기';
      }
    });
  });

  const moreBtn = document.getElementById('reportsMore');
  moreBtn.style.display = reports.length > limit ? 'block' : 'none';
  moreBtn.textContent = `더 보기 (${limit}/${reports.length}건)`;
}

function showMoreReports() {
  reportDisplayCount += 20;
  renderReportsTable(allReports, reportDisplayCount);
}

async function loadReports(stockCode) {
  const section = document.getElementById('reportsSection');
  const loading = document.getElementById('reportsLoading');
  const table = document.getElementById('reportsTable');
  const countEl = document.getElementById('reportCount');
  const requestId = ++reportsRequestId;

  section.style.display = 'block';
  loading.style.display = 'block';
  loading.textContent = '최신 리포트를 불러오는 중...';
  table.style.display = 'none';
  countEl.textContent = '';
  allReports = [];
  reportDisplayCount = 20;

  if (!hasApiConfiguration()) {
    loading.style.display = 'block';
    loading.textContent = 'GitHub Pages에서는 API 서버 연결 후 리포트를 불러올 수 있습니다.';
    return;
  }

  const localCache = loadReportCache(stockCode);
  let latestReport = localCache?.latestReport || null;
  let renderedFromCache = false;

  if (Array.isArray(localCache?.reports) && localCache.reports.length > 0) {
    allReports = localCache.reports;
    countEl.textContent = `(최근 3년, ${allReports.length}건 · 캐시)`;
    table.style.display = 'table';
    loading.style.display = 'none';
    renderReportsTable(allReports, reportDisplayCount);
    _overlayTargetPrices(allReports);
    renderedFromCache = true;
  } else if (latestReport) {
    allReports = [latestReport];
    countEl.textContent = '(최신 1건 · 캐시)';
    table.style.display = 'table';
    loading.style.display = 'none';
    renderReportsTable(allReports, 1);
    renderedFromCache = true;
  }

  try {
    const latestResp = await apiFetch(`/api/reports/${stockCode}/latest?refresh=1`);
    const latestData = await latestResp.json();
    if (requestId !== reportsRequestId) return;
    const networkLatest = latestData.report || null;
    const latestChanged = latestData.changed ?? !sameReport(latestReport, networkLatest);

    if (networkLatest) {
      latestReport = networkLatest;
      saveReportCache(stockCode, {
        latestReport,
        reports: Array.isArray(localCache?.reports) ? localCache.reports : [],
      });
    }

    if (networkLatest && (!renderedFromCache || latestChanged)) {
      allReports = [latestReport];
      loading.style.display = 'none';
      countEl.textContent = latestChanged ? '(최신 1건 갱신됨)' : '(최신 1건 확인됨)';
      table.style.display = 'table';
      renderReportsTable(allReports, 1);
    }
  } catch (e) { console.warn(e); }

  try {
    const shouldRefreshFullReports =
      !Array.isArray(localCache?.reports) ||
      localCache.reports.length === 0 ||
      !sameReport(localCache.latestReport, latestReport);

    if (!shouldRefreshFullReports) {
      return;
    }

    const resp = await apiFetch(`/api/reports/${stockCode}`);
    const data = await resp.json();
    if (requestId !== reportsRequestId) return;

    allReports = data.reports || [];
    loading.style.display = 'none';

    if (allReports.length === 0) {
      if (latestReport) {
        countEl.textContent = '(최신 1건만 표시)';
        table.style.display = 'table';
        renderReportsTable([latestReport], 1);
      } else {
        loading.style.display = 'block';
        loading.textContent = '증권사 리포트가 없습니다.';
      }
      return;
    }

    countEl.textContent = `(최근 3년, ${allReports.length}건)`;
    table.style.display = 'table';
    renderReportsTable(allReports, reportDisplayCount);
    saveReportCache(stockCode, { latestReport: allReports[0] || latestReport, reports: allReports });
    _overlayTargetPrices(allReports);
  } catch (e) {
    if (requestId !== reportsRequestId) return;
    if (latestReport) {
      loading.style.display = 'none';
      countEl.textContent = '(최신 1건만 표시)';
      table.style.display = 'table';
      renderReportsTable([latestReport], 1);
    } else {
      loading.textContent = '리포트를 불러오지 못했습니다.';
    }
  }
}

// --- LLM wiki (per-stock report summaries) --------------------------------

let _wikiLoadId = 0;
// pdf_url → wiki entry, populated by loadWiki so renderReportsTable can
// attach '요약 내용 보기' inline to matching rows.
let _wikiByPdfUrl = new Map();

async function loadWiki(stockCode) {
  const countEl = document.getElementById('wikiCount');

  // Tag each in-flight load; a newer analyzeStock() call invalidates older
  // ones so we don't flicker stale wiki into a different stock's view.
  const myId = ++_wikiLoadId;

  _wikiByPdfUrl = new Map();
  if (countEl) countEl.textContent = '';
  // Set up Q&A binding for this stock; safe to call repeatedly.
  _setupWikiQa(stockCode);
  // Clear previous answer.
  const prevAns = document.getElementById('wikiQaAnswer');
  const prevMeta = document.getElementById('wikiQaMeta');
  if (prevAns) { prevAns.style.display = 'none'; prevAns.innerHTML = ''; }
  if (prevMeta) prevMeta.textContent = '';

  try {
    const resp = await apiFetch(`/api/analysis/${encodeURIComponent(stockCode)}/wiki?limit=100`);
    if (myId !== _wikiLoadId) return;
    if (!resp.ok) return;
    const data = await resp.json();
    if (myId !== _wikiLoadId) return;
    const entries = data.entries || [];
    entries.forEach(e => {
      if (e.pdf_url) _wikiByPdfUrl.set(e.pdf_url, e);
    });
    if (countEl) countEl.textContent = entries.length ? `· 요약 ${entries.length}건` : '';
    // If the reports table is already rendered, re-render so the toggles
    // attach to rows that just got summaries.
    if (typeof allReports !== 'undefined' && allReports.length) {
      renderReportsTable(allReports, reportDisplayCount);
    }
  } catch (err) {
    if (myId !== _wikiLoadId) return;
    console.warn('wiki load failed', err);
  }
}

let _wikiQaStockCode = null;

function _setupWikiQa(stockCode) {
  _wikiQaStockCode = stockCode;
  const btn = document.getElementById('wikiQaSubmit');
  if (btn && !btn.dataset.bound) {
    btn.addEventListener('click', () => askWikiQuestion());
    btn.dataset.bound = '1';
  }
  const input = document.getElementById('wikiQaInput');
  if (input && !input.dataset.bound) {
    input.addEventListener('keydown', e => {
      // Ctrl+Enter submits; plain Enter inserts newline.
      if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
        e.preventDefault();
        askWikiQuestion();
      }
    });
    input.dataset.bound = '1';
  }
  // Gate input on auth state.
  const hint = document.getElementById('wikiQaHint');
  const isLoggedIn = !!(typeof currentUser !== 'undefined' && currentUser);
  if (input) input.disabled = !isLoggedIn;
  if (btn) btn.disabled = !isLoggedIn;
  if (hint) hint.style.display = isLoggedIn ? 'none' : '';
}

async function askWikiQuestion() {
  const stockCode = _wikiQaStockCode;
  if (!stockCode) return;
  const input = document.getElementById('wikiQaInput');
  const btn = document.getElementById('wikiQaSubmit');
  const status = document.getElementById('wikiQaStatus');
  const answerEl = document.getElementById('wikiQaAnswer');
  const metaEl = document.getElementById('wikiQaMeta');
  if (!input || !btn) return;
  const q = (input.value || '').trim();
  if (!q) { showToast && showToast('질문을 입력해 주세요.'); return; }

  btn.disabled = true;
  btn.textContent = '생성 중...';
  status.textContent = '';
  answerEl.style.display = 'block';
  answerEl.textContent = '';
  metaEl.textContent = '';

  let mdText = '';
  try {
    const resp = await apiFetch(`/api/analysis/${encodeURIComponent(stockCode)}/ask`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ question: q }),
    });
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      throw new Error(err.detail || `HTTP ${resp.status}`);
    }
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      const lines = buf.split(/\r?\n/);
      buf = lines.pop();
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const d = JSON.parse(line.slice(6));
          if (d.content) {
            mdText += d.content;
            if (typeof _renderSafeMarkdown === 'function') {
              answerEl.innerHTML = _renderSafeMarkdown(mdText);
            } else {
              answerEl.textContent = mdText;
            }
          }
          if (d.done) {
            const srcN = (d.sources || []).length;
            const modelS = d.model ? ` · ${d.model}` : '';
            const costUsd = Number(d.cost || 0);
            const costS = costUsd ? ` · $${costUsd.toFixed(6)}` : '';
            metaEl.textContent = `참조 요약 ${srcN}건 · 입력 ${d.input_tokens || '?'} / 출력 ${d.output_tokens || '?'} 토큰${costS}${modelS}`;
          }
        } catch {}
      }
    }
    if (typeof _renderSafeMarkdown === 'function' && mdText) {
      answerEl.innerHTML = _renderSafeMarkdown(mdText);
    }
  } catch (e) {
    answerEl.textContent = '질문 처리 실패: ' + (e.message || e);
  }
  btn.disabled = false;
  btn.textContent = '질문';
}
