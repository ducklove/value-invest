// Benchmark comparison overlay for the NAV trend chart: 비교지수 선택/캐시,
// NAV 스케일 정규화 입력 상태, beta/R² 통계와 차트 위 beta 오버레이.
// Split from static/js/portfolio-trends.js to keep trend features maintainable.
let _benchCache = {};    // code -> [{date, close}]

const _BENCH_COLORS = { KOSPI: '#e74c3c', SP500: '#2563eb', GOLD: '#f59e0b' };
const _BENCH_LABELS = { KOSPI: '코스피', SP500: 'S&P 500', GOLD: '금' };

function _getSelectedBenchmarks() {
  return Array.from(document.querySelectorAll('.pf-bench-chip input[value]:checked')).map(el => el.value);
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
      apiFetchJson(`/api/portfolio/benchmark-history?code=${encodeURIComponent(c)}&start=${encodeURIComponent(startDate)}`, { fallback: [] })
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
