// Portfolio NAV/value trend charts and return cards.
// Split from static/js/portfolio.js to keep portfolio features maintainable.
let _navChartInstance = null;
// 탭 전환 등으로 container 크기가 늦게 확정될 때 ECharts 가 0 크기로
// init 되어 차트가 아예 안 보이는 증상 대응. treemap 과 동일 패턴.
let _navChartResizeObserver = null;
let _valueChartResizeObserver = null;
let _groupWeightChartResizeObserver = null;
let _navChartRenderSeq = 0;
let _valueChartRenderSeq = 0;
let _groupWeightChartRenderSeq = 0;
let _navChartData = [];  // cached for benchmark overlay
let _navChartSeriesForAxis = [];
let _navBenchSeriesForAxis = [];
let _valueChartData = [];
let _valueChartSeriesForAxis = [];
let _groupWeightChartInstance = null;
let _groupWeightChartData = [];
let _groupWeightSeriesForAxis = [];
let _benchCache = {};    // code -> [{date, close}]

const _BENCH_COLORS = { KOSPI: '#e74c3c', SP500: '#2563eb', GOLD: '#f59e0b' };
const _BENCH_LABELS = { KOSPI: '코스피', SP500: 'S&P 500', GOLD: '금' };
const _GROUP_WEIGHT_COLORS = [
  '#2563eb', '#dc2626', '#16a34a', '#f59e0b', '#7c3aed', '#0891b2',
  '#db2777', '#65a30d', '#ea580c', '#475569', '#0f766e', '#9333ea',
];

async function _waitForChartContainer(container) {
  for (let i = 0; i < 5; i++) {
    await new Promise(r => requestAnimationFrame(r));
    const rect = container.getBoundingClientRect();
    if (rect.width > 20 && rect.height > 20) return true;
  }
  return false;
}

function _kickChartResize(chart) {
  if (!chart || typeof chart.resize !== 'function') return;
  requestAnimationFrame(() => chart.resize());
  setTimeout(() => chart.resize(), 120);
}

function _getSelectedBenchmarks() {
  return Array.from(document.querySelectorAll('.pf-bench-chip input[value]:checked')).map(el => el.value);
}

function _isMobileChartMode() {
  return (typeof window !== 'undefined' && window.matchMedia?.('(max-width: 900px)')?.matches)
    || (typeof USE_UPLOT !== 'undefined' && USE_UPLOT);
}

async function onBenchToggle() {
  if (_isMobileChartMode()) return;
  const codes = _getSelectedBenchmarks();
  if (!_navChartData.length) return;
  // Preserve the current zoom window across re-render. renderNavChart()
  // disposes the chart instance and recreates it from scratch, which would
  // otherwise reset dataZoom to 0~100 — i.e. the user loses their 3M/6M/1Y
  // selection just for checking a benchmark box.
  let preservedZoom = null;
  let startIdx = 0;
  if (_navChartInstance) {
    try {
      const opt = _navChartInstance.getOption?.();
      const dz = opt?.dataZoom?.[0];
      if (dz && (dz.start != null || dz.end != null)) {
        preservedZoom = { start: dz.start ?? 0, end: dz.end ?? 100 };
        startIdx = _chartZoomWindow(_navChartData.length, preservedZoom.start, preservedZoom.end).startIdx;
      }
    } catch (_) { /* getOption can throw if chart is mid-dispose */ }
  }
  // Fetch any uncached benchmarks. Empty arrays are treated as uncached so a
  // transient Yahoo/backfill timeout doesn't permanently disable the chip.
  const startDate = _navChartData[startIdx]?.date || _navChartData[0].date;
  const toFetch = codes.filter(c => {
    const cached = _benchCache[c];
    return !Array.isArray(cached) || cached.length === 0 || String(cached[0]?.date || '') > startDate;
  });
  if (toFetch.length) {
    const results = await Promise.all(toFetch.map(c =>
      apiFetch(`/api/portfolio/benchmark-history?code=${encodeURIComponent(c)}&start=${encodeURIComponent(startDate)}`)
        .then(r => r.ok ? r.json() : [])
        // 실패 종목은 아래 failed 집계가 한 번에 토스트하므로 여기선 로그만.
        .catch(e => { reportApiError(e, '비교지수 히스토리', { silent: true }); return []; })
    ));
    const failed = [];
    toFetch.forEach((c, i) => {
      _benchCache[c] = Array.isArray(results[i]) ? results[i] : [];
      if (!_benchCache[c].length) failed.push(_BENCH_LABELS[c] || c);
    });
    if (failed.length && typeof showToast === 'function') {
      showToast(`비교지수 데이터를 아직 불러오지 못했습니다: ${failed.join(', ')}`);
    }
  }
  await renderNavChart(_navChartData);
  if (preservedZoom && _navChartInstance && typeof _navChartInstance.dispatchAction === 'function') {
    // dispatchAction fires the datazoom listener inside renderNavChart,
    // which re-scales benchmark series to match the restored window.
    _navChartInstance.dispatchAction({
      type: 'dataZoom',
      start: preservedZoom.start,
      end: preservedZoom.end,
    });
  }
}

// Per-benchmark raw ratio arrays (bench_close / bench_close[0]), computed once.
// On zoom, we multiply by navValues[zoomStartIdx] to scale into NAV space.
let _benchRatios = {};  // code -> { ratioByLabel: {date: ratio}, labels }

function _chartZoomWindow(length, startPct = 0, endPct = 100) {
  const last = Math.max(0, length - 1);
  return {
    startIdx: Math.max(0, Math.min(last, Math.round(startPct / 100 * last))),
    endIdx: Math.max(0, Math.min(last, Math.round(endPct / 100 * last))),
  };
}

function _chartWindowFromInstance(chart, length) {
  try {
    const dz = chart?.getOption?.()?.dataZoom?.[0];
    return _chartZoomWindow(length, dz?.start ?? 0, dz?.end ?? 100);
  } catch (_) {
    return _chartZoomWindow(length, 0, 100);
  }
}

function _chartDataToNumbers(values) {
  return (values || []).map(v => {
    if (v === null || v === undefined || v === '' || v === '-') return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  });
}

function _visibleChartValues(seriesList, startIdx, endIdx) {
  const values = [];
  for (const series of seriesList || []) {
    for (let i = startIdx; i <= endIdx; i++) {
      const raw = series?.[i];
      if (raw === null || raw === undefined || raw === '' || raw === '-') continue;
      const n = Number(raw);
      if (Number.isFinite(n)) values.push(n);
    }
  }
  return values;
}

function _axisRangeForVisibleSeries(seriesList, startIdx, endIdx, yZero) {
  const values = _visibleChartValues(seriesList, startIdx, endIdx);
  if (!values.length) {
    return { min: yZero ? 0 : 'dataMin', max: undefined };
  }
  const dataMin = Math.min(...values);
  let min = dataMin;
  let max = Math.max(...values);
  if (yZero) min = 0;
  if (min === max) {
    const pad = Math.max(Math.abs(max) * 0.02, 1);
    if (!yZero) min -= pad;
    max += pad;
  } else {
    const pad = (max - min) * 0.06;
    if (!yZero) min -= pad;
    max += pad;
  }
  if (!yZero && dataMin >= 0 && min < 0) min = 0;
  return { min, max };
}

function _applyVisibleYAxis(chart, seriesList, startIdx, endIdx, yZero) {
  if (!chart?.setOption) return;
  const axisRange = _axisRangeForVisibleSeries(seriesList, startIdx, endIdx, yZero);
  chart.setOption({ yAxis: axisRange });
  return axisRange;
}

function _updateChartRangeLabel(elId, data, startIdx, endIdx) {
  const el = document.getElementById(elId);
  if (!el) return;
  if (!data?.length || !data[startIdx] || !data[endIdx]) {
    el.innerHTML = '';
    return;
  }
  const start = data[startIdx].date;
  const end = data[endIdx].date;
  const days = Math.max(0, Math.round((new Date(end) - new Date(start)) / 86400000)) + 1;
  const points = Math.max(0, endIdx - startIdx + 1);
  el.innerHTML = `표시 기간 <strong>${escapeHtml(start)} ~ ${escapeHtml(end)}</strong><span>${days.toLocaleString()}일 · ${points.toLocaleString()}개 스냅샷</span>`;
}

function _computeReturnStats(pairs) {
  const n = pairs?.length || 0;
  if (n < 20) return { beta: null, rSquared: null };
  const meanNav = pairs.reduce((sum, p) => sum + p.nav, 0) / n;
  const meanBench = pairs.reduce((sum, p) => sum + p.bench, 0) / n;
  let cov = 0;
  let navVariance = 0;
  let benchVariance = 0;
  pairs.forEach(p => {
    const navDiff = p.nav - meanNav;
    const benchDiff = p.bench - meanBench;
    cov += navDiff * benchDiff;
    navVariance += navDiff * navDiff;
    benchVariance += benchDiff * benchDiff;
  });
  const beta = benchVariance > 0 ? cov / benchVariance : null;
  const rSquared = navVariance > 0 && benchVariance > 0 ? (cov * cov) / (navVariance * benchVariance) : null;
  return { beta, rSquared };
}

function _navBenchmarkBeta(labels, navValues, ratioMap, startIdx, endIdx) {
  const pairs = [];
  let prevNav = null;
  let prevBench = null;
  const last = Math.min(labels.length - 1, endIdx);
  for (let i = Math.max(0, startIdx); i <= last; i++) {
    const nav = Number(navValues[i]);
    const bench = Number(ratioMap?.[labels[i]]);
    if (!(nav > 0) || !(bench > 0)) continue;
    if (prevNav !== null && prevBench !== null) {
      pairs.push({ nav: nav / prevNav - 1, bench: bench / prevBench - 1 });
    }
    prevNav = nav;
    prevBench = bench;
  }
  return { ..._computeReturnStats(pairs), sampleSize: pairs.length };
}

function _formatBetaValue(beta) {
  return Number.isFinite(beta) ? beta.toFixed(2) : '-';
}

function _formatRSquaredValue(rSquared) {
  return Number.isFinite(rSquared) ? rSquared.toFixed(2) : '-';
}

function _updateNavBetaOverlay(labels, navValues, benchCodes, startIdx, endIdx) {
  const container = document.getElementById('pfNavChart');
  if (!container) return;
  const rows = (benchCodes || [])
    .filter(code => _benchRatios[code])
    .map(code => ({
      code,
      label: _BENCH_LABELS[code] || code,
      color: _BENCH_COLORS[code] || '#64748b',
      ..._navBenchmarkBeta(labels, navValues, _benchRatios[code], startIdx, endIdx),
    }));
  let overlay = container.querySelector('.pf-nav-beta-overlay');
  if (!rows.length) {
    if (overlay) overlay.remove();
    return;
  }
  if (!overlay) {
    overlay = document.createElement('div');
    overlay.className = 'pf-nav-beta-overlay';
    container.appendChild(overlay);
  }
  overlay.title = 'NAV beta vs selected benchmark, based on daily returns in the visible chart range';
  overlay.innerHTML = rows.map(row => {
    const beta = _formatBetaValue(row.beta);
    const rSquared = _formatRSquaredValue(row.rSquared);
    const hint = row.sampleSize < 20 ? ' title="Need at least 20 return points"' : '';
    return `<span class="pf-nav-beta-chip"${hint} style="--beta-color:${row.color}"><span class="pf-nav-beta-dot"></span>${escapeHtml(row.label)} \u03b2 ${beta} · R\u00b2 ${rSquared}</span>`;
  }).join('');
}

async function renderNavChart(data) {
  const container = document.getElementById('pfNavChart');
  if (!container) return;
  const renderSeq = ++_navChartRenderSeq;
  if (_navChartInstance) { _navChartInstance.dispose(); _navChartInstance = null; }
  if (_navChartResizeObserver) { _navChartResizeObserver.disconnect(); _navChartResizeObserver = null; }
  _navChartData = data || [];
  data = _navChartData;

  if (!data.length) {
    container.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text-secondary);font-size:14px;">스냅샷 데이터가 없습니다.</div>';
    _updateChartRangeLabel('pfNavRange', [], 0, 0);
    return;
  }
  if (typeof PortfolioTrendChart === 'undefined') {
    container.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text-secondary);font-size:14px;">차트 렌더러를 불러오지 못했습니다.</div>';
    return;
  }

  const navValues = data.map(d => {
    if (pfCurrency === 'USD' && d.fx_usdkrw && d.fx_usdkrw > 0) return d.nav / d.fx_usdkrw;
    return d.nav;
  });
  const labels = data.map(d => d.date);

  const last365 = data.slice(-365);
  const yoyPct = last365.length > 1
    ? ((navValues[navValues.length - 1] / navValues[navValues.length - last365.length]) - 1) * 100 : 0;
  const navColor = returnToColor(yoyPct);
  const mobileChartMode = _isMobileChartMode();

  // Precompute benchmark ratio maps (close / first_close for each date)
  const benchCodes = mobileChartMode ? [] : _getSelectedBenchmarks();
  _benchRatios = {};
  for (const code of benchCodes) {
    const raw = _benchCache[code] || [];
    if (!raw.length) continue;
    const firstClose = raw[0].close;
    if (!firstClose) continue;
    const ratioByLabel = {};
    raw.forEach(d => { ratioByLabel[d.date] = d.close / firstClose; });
    _benchRatios[code] = ratioByLabel;
  }

  // Build benchmark series scaled to the NAV value at the first visible
  // overlapping date. That gives the benchmark line the same left anchor as
  // NAV even when the benchmark feed starts a few dates after the portfolio.
  function buildBenchSeries(startIdx) {
    const series = [];
    for (const code of benchCodes) {
      const ratioMap = _benchRatios[code];
      if (!ratioMap) continue;
      // Find the ratio at the start index (first overlapping date from startIdx onward)
      let baseRatio = null;
      let anchorIdx = null;
      for (let i = startIdx; i < labels.length; i++) {
        if (ratioMap[labels[i]] != null && Number.isFinite(Number(navValues[i]))) {
          baseRatio = ratioMap[labels[i]];
          anchorIdx = i;
          break;
        }
      }
      if (!baseRatio || anchorIdx === null) continue;
      const navAtAnchor = navValues[anchorIdx];
      // Scale: benchNAV = navAtAnchor * (ratio / baseRatio)
      const vals = labels.map(lbl => {
        const r = ratioMap[lbl];
        return r != null ? navAtAnchor * (r / baseRatio) : null;
      });
      series.push({
        name: _BENCH_LABELS[code] || code,
        type: 'line',
        data: vals.map(v => v === null ? '-' : v),
        smooth: 0.3,
        symbol: 'none',
        lineStyle: { color: _BENCH_COLORS[code], width: 1.5, type: 'dashed' },
        itemStyle: { color: _BENCH_COLORS[code] },
        connectNulls: true,
      });
    }
    return series;
  }

  const hasBench = Object.keys(_benchRatios).length > 0;
  const legendData = ['NAV', ...benchCodes.filter(c => _benchRatios[c]).map(c => _BENCH_LABELS[c] || c)];
  const textColor = getComputedStyle(document.documentElement).getPropertyValue('--text-secondary').trim() || '#888';
  const gridColor = getComputedStyle(document.documentElement).getPropertyValue('--border').trim() || '#333';
  const yZero = mobileChartMode ? false : document.getElementById('pfNavYZero')?.checked;

  // Let the tab display style settle before the lightweight renderer
  // measures the canvas. This prevents zero-size first renders when users
  // switch tabs quickly or the browser has not completed layout yet.
  const containerReady = await _waitForChartContainer(container);
  if (!containerReady && container.offsetParent === null) return;
  if (renderSeq !== _navChartRenderSeq) return;

  const initBenchSeries = buildBenchSeries(0);
  const ec = PortfolioTrendChart.create(container, {
    legend: hasBench ? {
      data: legendData,
      top: 0, right: 0,
      textStyle: { color: textColor, fontSize: 11 },
      itemWidth: 18, itemHeight: 2,
    } : undefined,
    grid: { left: 55, right: 12, top: hasBench ? 28 : 10, bottom: mobileChartMode ? 24 : 56 },
    dataZoom: mobileChartMode ? [] : [{ start: 0, end: 100 }],
    xAxis: {
      type: 'category', data: labels,
      axisLine: { lineStyle: { color: gridColor } },
      axisLabel: { color: textColor, fontSize: 10 },
      splitLine: { show: false },
    },
    yAxis: {
      type: 'value',
      min: yZero ? 0 : 'dataMin',
      axisLine: { show: false },
      axisLabel: {
        color: textColor,
        fontSize: 10,
        formatter: v => Math.round(v).toLocaleString(),
      },
      splitLine: { lineStyle: { color: gridColor, width: 0.5 } },
    },
    tooltip: {
      trigger: 'axis',
      formatter(params) {
        let html = params[0] ? params[0].axisValueLabel : '';
        for (const p of params) {
          if (p.value == null || p.value === '-') continue;
          html += `<br/><span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${p.color};margin-right:4px;"></span>${p.seriesName}: ${Number(p.value).toFixed(2)}`;
        }
        return html;
      },
    },
    series: [
      {
        name: 'NAV',
        type: 'line',
        data: navValues.map(v => v === null ? '-' : v),
        smooth: 0.3,
        symbol: navValues.length > 30 ? 'none' : 'circle',
        symbolSize: navValues.length > 60 ? 0 : 4,
        lineStyle: { color: navColor, width: 2 },
        itemStyle: { color: navColor },
        areaStyle: {},
      },
      ...initBenchSeries,
    ],
  });

  // NAV and benchmarks share one Y-axis. Benchmarks are normalized to the
  // visible NAV start, then the common axis range includes every visible line.
  _navBenchSeriesForAxis = initBenchSeries.map(series => _chartDataToNumbers(series.data));
  _navChartSeriesForAxis = [navValues, ..._navBenchSeriesForAxis];
  const fullWindow = _chartZoomWindow(labels.length, 0, 100);
  _applyVisibleYAxis(ec, _navChartSeriesForAxis, fullWindow.startIdx, fullWindow.endIdx, !!yZero);
  _updateChartRangeLabel('pfNavRange', data, fullWindow.startIdx, fullWindow.endIdx);
  _updateNavBetaOverlay(labels, navValues, benchCodes, fullWindow.startIdx, fullWindow.endIdx);

  // On dataZoom change: (a) re-scale benchmark series to the new window,
  // (b) refresh the CAGR card so it reflects the visible period.
  //
  // Previously this listener only ran when hasBench was true — but the CAGR
  // card should react to zoom regardless of whether benchmarks are on, so
  // the listener is now installed unconditionally and internally guards the
  // benchmark-rescale path.
  {
    let _zoomTimer = null;
    ec.on('datazoom', () => {
      clearTimeout(_zoomTimer);
      _zoomTimer = setTimeout(() => {
        const opt = ec.getOption();
        const dz = opt?.dataZoom?.[0];
        const startPct = dz?.start ?? 0;
        const endPct = dz?.end ?? 100;
        const last = Math.max(0, labels.length - 1);
        const startIdx = Math.max(0, Math.min(last, Math.round(startPct / 100 * last)));
        const endIdx = Math.max(0, Math.min(last, Math.round(endPct / 100 * last)));

        if (hasBench) {
          const newBench = buildBenchSeries(startIdx);
          // Update only benchmark series (index 1+)
          const seriesUpdate = [{ data: navValues.map(v => v === null ? '-' : v) }, ...newBench];
          ec.setOption({ series: seriesUpdate });
          _navBenchSeriesForAxis = newBench.map(series => _chartDataToNumbers(series.data));
          _navChartSeriesForAxis = [navValues, ..._navBenchSeriesForAxis];
        } else {
          _navChartSeriesForAxis = [navValues];
          _navBenchSeriesForAxis = [];
        }

        const zoomYZero = !!document.getElementById('pfNavYZero')?.checked;
        _applyVisibleYAxis(ec, _navChartSeriesForAxis, startIdx, endIdx, zoomYZero);
        _updateChartRangeLabel('pfNavRange', data, startIdx, endIdx);
        _updateNavBetaOverlay(labels, navValues, benchCodes, startIdx, endIdx);
        _updateNavCagrCard(data, startIdx, endIdx);
      }, 80);
    });
  }

  _navChartInstance = ec;
  _kickChartResize(_navChartInstance);

  // 폰에서 탭 전환 직후 container 높이가 늦게 확정되는 경우가 있어
  // ResizeObserver 로 크기 확정 시점에 한 번 더 그린다.
  if (typeof ResizeObserver !== 'undefined') {
    const ro = new ResizeObserver(() => {
      if (_navChartInstance) _navChartInstance.resize();
    });
    ro.observe(container);
    _navChartResizeObserver = ro;
  }
}

function onNavYZeroToggle() {
  if (_isMobileChartMode()) return;
  if (_navChartInstance) {
    const yZero = document.getElementById('pfNavYZero')?.checked;
    if (typeof _navChartInstance.setOption !== 'function') {
      void renderNavChart(_navChartData);
      return;
    }
    const { startIdx, endIdx } = _chartWindowFromInstance(_navChartInstance, _navChartData.length);
    _applyVisibleYAxis(_navChartInstance, _navChartSeriesForAxis, startIdx, endIdx, !!yZero);
  }
}

function onValueYZeroToggle() {
  if (_isMobileChartMode()) return;
  if (_valueChartInstance) {
    const yZero = document.getElementById('pfValueYZero')?.checked;
    if (typeof _valueChartInstance.setOption !== 'function') {
      void renderValueChart(_valueChartData);
      return;
    }
    const { startIdx, endIdx } = _chartWindowFromInstance(_valueChartInstance, _valueChartData.length);
    _applyVisibleYAxis(_valueChartInstance, _valueChartSeriesForAxis, startIdx, endIdx, !!yZero);
  }
}

function _navZoomToDays(days) {
  if (_isMobileChartMode()) return;
  if (!_navChartInstance || !_navChartData.length || typeof _navChartInstance.dispatchAction !== 'function') return;
  const total = _navChartData.length;
  const startPct = Math.max(0, (1 - days / total) * 100);
  _navChartInstance.dispatchAction({ type: 'dataZoom', start: startPct, end: 100 });
}

function _valueZoomToDays(days) {
  if (_isMobileChartMode()) return;
  if (!_valueChartInstance || !_valueChartData.length || typeof _valueChartInstance.dispatchAction !== 'function') return;
  const total = _valueChartData.length;
  const startPct = Math.max(0, (1 - days / total) * 100);
  _valueChartInstance.dispatchAction({ type: 'dataZoom', start: startPct, end: 100 });
}

let _valueChartInstance = null;

async function renderValueChart(data) {
  const container = document.getElementById('pfValueChart');
  if (!container) return;
  const renderSeq = ++_valueChartRenderSeq;
  if (_valueChartInstance) { _valueChartInstance.dispose(); _valueChartInstance = null; }
  if (_valueChartResizeObserver) { _valueChartResizeObserver.disconnect(); _valueChartResizeObserver = null; }
  _valueChartData = data || [];
  _valueChartSeriesForAxis = [];

  const containerReady = await _waitForChartContainer(container);
  if (!containerReady && container.offsetParent === null) return;
  if (renderSeq !== _valueChartRenderSeq) return;

  // Stats cards
  const statsEl = document.getElementById('pfValueStats');
  if (statsEl) statsEl.innerHTML = '';

  if (!data.length) {
    container.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text-secondary);font-size:14px;">스냅샷 데이터가 없습니다.</div>';
    _updateChartRangeLabel('pfValueRange', [], 0, 0);
    return;
  }
  if (typeof PortfolioTrendChart === 'undefined') {
    container.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text-secondary);font-size:14px;">차트 렌더러를 불러오지 못했습니다.</div>';
    return;
  }

  // Convert values using per-day FX rate when available
  const fxValues = data.map(d => {
    if (pfCurrency === 'USD' && d.fx_usdkrw && d.fx_usdkrw > 0) return d.total_value / d.fx_usdkrw;
    if (pfCurrency === 'USD') return pfFx(d.total_value); // fallback to current rate
    return d.total_value;
  });

  // Color based on MoM
  const last30 = fxValues.slice(-30);
  const momPct = last30.length > 1
    ? ((fxValues[fxValues.length - 1] / last30[0]) - 1) * 100 : 0;
  const valColor = returnToColor(momPct, 10);

  const div = pfFxDivisor();
  const unit = pfFxUnit();
  const sym = pfFxSymbol();
  const mobileChartMode = _isMobileChartMode();

  const valYZero = mobileChartMode ? false : document.getElementById('pfValueYZero')?.checked;
  _valueChartInstance = PortfolioTrendChart.create(container, {
    grid: { left: 68, right: 12, top: 10, bottom: mobileChartMode ? 24 : 56 },
    dataZoom: mobileChartMode ? [] : [{ start: 0, end: 100 }],
    xAxis: { type: 'category', data: data.map(d => d.date) },
    yAxis: {
      type: 'value',
      min: valYZero ? 0 : 'dataMin',
      axisLabel: { formatter: v => sym + (v / div).toFixed(pfCurrency === 'USD' ? 2 : 0) + unit },
    },
    series: [{
      name: '평가금액',
      type: 'line',
      data: fxValues.map(v => Math.round(v)),
      smooth: 0.3,
      symbol: 'none',
      lineStyle: { color: valColor, width: 2 },
      itemStyle: { color: valColor },
      areaStyle: {},
    }],
  });
  _kickChartResize(_valueChartInstance);
  _valueChartSeriesForAxis = [fxValues.map(v => Math.round(v))];
  const fullWindow = _chartZoomWindow(data.length, 0, 100);
  _applyVisibleYAxis(_valueChartInstance, _valueChartSeriesForAxis, fullWindow.startIdx, fullWindow.endIdx, !!valYZero);
  _updateChartRangeLabel('pfValueRange', data, fullWindow.startIdx, fullWindow.endIdx);

  // NAV 차트와 동일한 ResizeObserver — 폰 탭 전환 시 init 타이밍 보정.
  if (typeof ResizeObserver !== 'undefined' && _valueChartInstance) {
    const ro = new ResizeObserver(() => {
      if (_valueChartInstance && _valueChartInstance.resize) _valueChartInstance.resize();
    });
    ro.observe(container);
    _valueChartResizeObserver = ro;
  }

  // Value stats cards — use FX-converted values
  if (statsEl) {
    const fxLast365 = fxValues.slice(-365);
    const min52 = Math.min(...fxLast365);
    const max52 = Math.max(...fxLast365);

    // YoY (using FX-adjusted values)
    const yoyPct = fxLast365.length > 1
      ? ((fxValues[fxValues.length - 1] / fxLast365[0]) - 1) * 100 : null;

    // CAGR using FX-adjusted values
    const valTotalDays = data.length > 1 ? (new Date(data[data.length - 1].date) - new Date(data[0].date)) / 86400000 : 0;
    const valTotalYears = valTotalDays / 365;
    const _latestFxVal = fxValues[fxValues.length - 1];
    const _firstFxVal = fxValues[0];
    const acctReturn = valTotalYears > 0 && _firstFxVal > 0
      ? ((_latestFxVal - _firstFxVal) / _firstFxVal * 100) / valTotalYears : null;

    const fmtVal = v => pfCurrency === 'USD' ? '$' + Number(v.toFixed(0)).toLocaleString() : fmtKrw(Math.round(v));
    const _periodPct = (days) => {
      if (fxValues.length < 2) return null;
      const slice = fxValues.slice(-days);
      if (slice.length < 2 || !(slice[0] > 0)) return null;
      return ((fxValues[fxValues.length - 1] / slice[0]) - 1) * 100;
    };
    const pct7 = _periodPct(7);
    const pct30 = _periodPct(30);
    const pct90 = _periodPct(90);
    const items = [
      { label: '전일 평가금액', val: fmtVal(_latestFxVal) },
      { label: '최근 7일', val: pct7 !== null ? fmtPct(pct7) : '-', cls: returnClass(pct7), days: 7 },
      { label: '최근 30일', val: pct30 !== null ? fmtPct(pct30) : '-', cls: returnClass(pct30), days: 30 },
      { label: '최근 90일', val: pct90 !== null ? fmtPct(pct90) : '-', cls: returnClass(pct90), days: 90 },
      { label: '52주 최저', val: fmtVal(min52), days: 365 },
      { label: '52주 최고', val: fmtVal(max52), days: 365 },
      { label: 'YoY', val: yoyPct !== null ? fmtPct(yoyPct) : '-', cls: returnClass(yoyPct), days: 365 },
      // role='cagr' is the hook _updateValueCagrCard() latches onto when
      // the 평가금액 chart dataZoom moves.
      { label: 'CAGR', val: acctReturn !== null ? fmtPct(acctReturn) : '-', cls: returnClass(acctReturn), role: 'cagr' },
    ];
    const displayItems = mobileChartMode ? items.filter(p => !p.days) : items;
    statsEl.innerHTML = displayItems.map(p => {
      const role = p.role ? ` data-role="${p.role}"` : '';
      const zoomable = p.days && !mobileChartMode ? ` js-pf-value-zoom" data-zoom-days="${p.days}" style="cursor:pointer;` : '';
      return `<div class="pf-nav-ret-card${zoomable}"${role}><div class="pf-nav-ret-label">${p.label}</div><div class="pf-nav-ret-value ${p.cls || ''}">${p.val}</div></div>`;
    }).join('');
  }

  // On dataZoom change, refresh the CAGR card for the visible window. The
  // uPlot mobile path doesn't expose `on` — skip there (no dataZoom UI).
  if (_valueChartInstance && typeof _valueChartInstance.on === 'function') {
    let _zoomTimer = null;
    _valueChartInstance.on('datazoom', () => {
      clearTimeout(_zoomTimer);
      _zoomTimer = setTimeout(() => {
        const opt = _valueChartInstance.getOption();
        const dz = opt?.dataZoom?.[0];
        const startPct = dz?.start ?? 0;
        const endPct = dz?.end ?? 100;
        const last = Math.max(0, data.length - 1);
        const startIdx = Math.max(0, Math.min(last, Math.round(startPct / 100 * last)));
        const endIdx = Math.max(0, Math.min(last, Math.round(endPct / 100 * last)));
        _applyVisibleYAxis(_valueChartInstance, _valueChartSeriesForAxis, startIdx, endIdx, !!document.getElementById('pfValueYZero')?.checked);
        _updateChartRangeLabel('pfValueRange', data, startIdx, endIdx);
        _updateValueCagrCard(data, fxValues, startIdx, endIdx);
      }, 80);
    });
  }
}

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
    container.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text-secondary);font-size:14px;">그룹 비중 스냅샷이 아직 없습니다.</div>';
    _updateChartRangeLabel('pfGroupWeightRange', [], 0, 0);
    return;
  }
  if (typeof PortfolioTrendChart === 'undefined') {
    container.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:var(--text-secondary);font-size:14px;">차트 렌더러를 불러오지 못했습니다.</div>';
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

// Update ONLY the CAGR card in #pfValueStats for the visible window
// [startIdx..endIdx]. Uses the closure-captured fxValues so the FX-adjusted
// math stays consistent with the snapshot renderValueChart computed at the
// top of the function.
function _updateValueCagrCard(data, fxValues, startIdx, endIdx) {
  const root = document.getElementById('pfValueStats');
  if (!root) return;
  const card = root.querySelector('[data-role="cagr"]');
  if (!card) return;
  const labelEl = card.querySelector('.pf-nav-ret-label');
  const valEl = card.querySelector('.pf-nav-ret-value');
  if (!labelEl || !valEl) return;

  const isFull = startIdx === 0 && endIdx === data.length - 1;
  labelEl.textContent = isFull ? 'CAGR' : 'CAGR (구간)';

  const first = fxValues[startIdx];
  const last = fxValues[endIdx];
  if (endIdx <= startIdx || first == null || last == null || !(first > 0)) {
    valEl.textContent = '-';
    valEl.className = 'pf-nav-ret-value';
    return;
  }
  const days = (new Date(data[endIdx].date) - new Date(data[startIdx].date)) / 86400000;
  const years = days / 365;
  if (!(years > 0)) {
    valEl.textContent = '-';
    valEl.className = 'pf-nav-ret-value';
    return;
  }
  const cagr = ((last - first) / first * 100) / years;
  valEl.textContent = fmtPct(cagr);
  valEl.className = 'pf-nav-ret-value ' + (returnClass(cagr) || '');
}

function renderNavReturns(data) {
  const el = document.getElementById('pfNavReturns');
  if (!el || !data.length) { if (el) el.innerHTML = ''; return; }
  const mobileChartMode = _isMobileChartMode();

  const _nav = d => {
    if (pfCurrency === 'USD' && d.fx_usdkrw && d.fx_usdkrw > 0) return d.nav / d.fx_usdkrw;
    return d.nav;
  };

  const latest = data[data.length - 1];
  const latestNav = _nav(latest);
  const firstNav = _nav(data[0]);

  // Period returns helper
  const _periodPct = (days) => {
    if (data.length < 2) return null;
    const slice = data.slice(-days);
    if (!slice.length) return null;
    const base = _nav(slice[0]);
    return base > 0 ? ((latestNav / base) - 1) * 100 : null;
  };

  const pct7 = _periodPct(7);
  const pct30 = _periodPct(30);
  const pct90 = _periodPct(90);

  // 52-week range
  const last365 = data.slice(-365);
  const navs52 = last365.map(d => _nav(d));
  const min52 = Math.min(...navs52);
  const max52 = Math.max(...navs52);

  // YoY
  const oneYearAgo = last365.length >= 252 ? last365[0] : (last365.length > 0 ? last365[0] : null);
  const yoyPct = oneYearAgo ? ((latestNav / _nav(oneYearAgo)) - 1) * 100 : null;

  // CAGR
  const totalDays = data.length > 1 ? (new Date(latest.date) - new Date(data[0].date)) / 86400000 : 0;
  const totalYears = totalDays / 365;
  const annualizedPct = totalYears > 0
    ? ((latestNav - firstNav) / firstNav * 100) / totalYears : null;

  const items = [
    { label: '전일 NAV', val: latestNav.toFixed(2) },
    { label: '최근 7일', val: pct7 !== null ? fmtPct(pct7) : '-', cls: returnClass(pct7), days: 7 },
    { label: '최근 30일', val: pct30 !== null ? fmtPct(pct30) : '-', cls: returnClass(pct30), days: 30 },
    { label: '최근 90일', val: pct90 !== null ? fmtPct(pct90) : '-', cls: returnClass(pct90), days: 90 },
    { label: '52주 최저', val: min52.toFixed(2), days: 365 },
    { label: '52주 최고', val: max52.toFixed(2), days: 365 },
    { label: 'YoY', val: yoyPct !== null ? fmtPct(yoyPct) : '-', cls: returnClass(yoyPct), days: 365 },
    // role='cagr' lets _updateNavCagrCard() find this specific card when
    // the NAV chart's dataZoom changes, so the value reflects the visible
    // window instead of the full-history snapshot.
    { label: 'CAGR', val: annualizedPct !== null ? fmtPct(annualizedPct) : '-', cls: returnClass(annualizedPct), role: 'cagr' },
  ];
  const displayItems = mobileChartMode ? items.filter(p => !p.days) : items;
  el.innerHTML = displayItems.map(p => {
    const zoomable = p.days && !mobileChartMode ? ` js-pf-nav-zoom" data-zoom-days="${p.days}" style="cursor:pointer;` : '';
    const role = p.role ? ` data-role="${p.role}"` : '';
    return `<div class="pf-nav-ret-card${zoomable}"${role}><div class="pf-nav-ret-label">${p.label}</div><div class="pf-nav-ret-value ${p.cls || ''}">${p.val}</div></div>`;
  }).join('');
}

// Update ONLY the CAGR card in #pfNavReturns to reflect the visible window
// [startIdx..endIdx] on the NAV chart. Called on every debounced dataZoom
// event. When the full range is selected the label stays plain "CAGR";
// when zoomed in it becomes "CAGR (구간)" so the user knows the value is
// no longer full-history.
function _updateNavCagrCard(data, startIdx, endIdx) {
  const root = document.getElementById('pfNavReturns');
  if (!root) return;
  const card = root.querySelector('[data-role="cagr"]');
  if (!card) return;
  const labelEl = card.querySelector('.pf-nav-ret-label');
  const valEl = card.querySelector('.pf-nav-ret-value');
  if (!labelEl || !valEl) return;

  const isFull = startIdx === 0 && endIdx === data.length - 1;
  labelEl.textContent = isFull ? 'CAGR' : 'CAGR (구간)';

  if (endIdx <= startIdx || !data[startIdx] || !data[endIdx]) {
    valEl.textContent = '-';
    valEl.className = 'pf-nav-ret-value';
    return;
  }
  // Same FX-aware accessor renderNavReturns uses — keep the two in sync so
  // switching the display currency is reflected in the zoomed CAGR too.
  const _nav = d => (pfCurrency === 'USD' && d.fx_usdkrw && d.fx_usdkrw > 0) ? d.nav / d.fx_usdkrw : d.nav;
  const firstNav = _nav(data[startIdx]);
  const lastNav = _nav(data[endIdx]);
  const days = (new Date(data[endIdx].date) - new Date(data[startIdx].date)) / 86400000;
  const years = days / 365;
  if (!(years > 0) || !(firstNav > 0)) {
    valEl.textContent = '-';
    valEl.className = 'pf-nav-ret-value';
    return;
  }
  // Matches renderNavReturns' "simple annualized" formula (not compound
  // CAGR) — keeps the zoomed value numerically comparable to the initial
  // full-range value on the same card.
  const cagr = ((lastNav - firstNav) / firstNav * 100) / years;
  valEl.textContent = fmtPct(cagr);
  valEl.className = 'pf-nav-ret-value ' + (returnClass(cagr) || '');
}
